# /// script
# requires-python = ">=3.12"
# dependencies = ["websockets>=14", "psycopg[binary]>=3.2", "python-dotenv>=1"]
# ///
"""Stream Bluesky events from Jetstream into Postgres.

Live:     uv run collector.py
Backfill: uv run collector.py --backfill-hours 24 --workers 6

Live mode resumes from the last stored event. Backfill splits the window into
one time slice per worker and replays them in parallel; events already stored
are skipped, so it is safe to re-run.
"""

import argparse
import asyncio
import json
import logging
import multiprocessing as mp
import time
from datetime import datetime, timedelta, timezone

import psycopg
import websockets
from dotenv import load_dotenv

load_dotenv()

JETSTREAM = (
    "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents"
)
COLLECTIONS = [
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
]
COLUMNS = "(seq, did, time, operation, collection, rkey, record)"
BATCH_SIZE = 5_000
FLUSH_SECONDS = 2.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq        bigint PRIMARY KEY,
    did        text NOT NULL,
    time       timestamptz NOT NULL,
    operation  text NOT NULL,
    collection text NOT NULL,
    rkey       text NOT NULL,
    record     jsonb
);
"""

GAP = timedelta(seconds=30)
RESUME = """
WITH slice AS (
    SELECT seq, time, lead(time) OVER (ORDER BY time) AS next
    FROM events
    WHERE time >= %(start)s AND time < %(end)s
)
SELECT seq FROM slice
WHERE (SELECT min(time) FROM slice) < %(start)s + %(gap)s
  AND (next IS NULL OR next - time > %(gap)s)
ORDER BY time
LIMIT 1
"""

log = logging.getLogger("collector")


def strip_nulls(value):
    # Postgres jsonb can't store \u0000, which turns up in some post text
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, dict):
        return {k: strip_nulls(v) for k, v in value.items()}
    if isinstance(value, list):
        return [strip_nulls(v) for v in value]
    return value


def dump_record(record: dict) -> str:
    dumped = json.dumps(record)
    # Only rebuild the record in the rare case that it contains a NUL
    return json.dumps(strip_nulls(record)) if "\\u0000" in dumped else dumped


def to_row(message: dict) -> tuple | None:
    payload = message.get("payload", {})
    if not payload.get("$type", "").endswith("#commit"):
        return None
    record = payload.get("record")
    return (
        payload["seq"],
        payload["did"],
        payload["time"],
        payload["operation"],
        payload["collection"],
        payload["rkey"],
        dump_record(record) if record is not None else None,
    )


def subscribe_url(cursor: int | None) -> str:
    params = [f"collections={c}" for c in COLLECTIONS] + ["kinds=commit"]
    if cursor is not None:
        params.append(f"cursor={cursor}")
    return f"{JETSTREAM}?{'&'.join(params)}"


async def store(conn: psycopg.AsyncConnection, rows: list[tuple]) -> None:
    # COPY into a staging table, then skip any events Jetstream resent
    async with conn.cursor() as cur:
        async with cur.copy(f"COPY staging {COLUMNS} FROM STDIN") as copy:
            for row in rows:
                await copy.write_row(row)
        await cur.execute(
            "INSERT INTO events SELECT * FROM staging ON CONFLICT (seq) DO NOTHING"
        )
    await conn.commit()


async def collect(
    name: str,
    cursor: int | None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> None:
    """Store events from `cursor` (a seq or unix-microsecond timestamp) until `end`."""
    # Connection details come from the standard PG* environment variables
    async with await psycopg.AsyncConnection.connect() as conn:
        await conn.execute(SCHEMA)
        await conn.execute(
            "CREATE TEMP TABLE staging (LIKE events) ON COMMIT DELETE ROWS"
        )
        await conn.commit()

        async with conn.cursor() as cur:
            if cursor is None:
                await cur.execute("SELECT max(seq) FROM events")
                (cursor,) = await cur.fetchone()
            elif start and end:
                # Resume a backfill slice from the end of the run of events already
                # stored from its start, i.e. just before the first gap
                await cur.execute(RESUME, {"start": start, "end": end, "gap": GAP})
                row = await cur.fetchone()
                if row:
                    cursor = row[0]

        total = 0
        started = time.monotonic()
        backoff = 1

        while True:
            try:
                log.info("%s: connecting (cursor=%s)", name, cursor)
                async with websockets.connect(
                    subscribe_url(cursor), max_size=2**22
                ) as ws:
                    backoff = 1
                    rows: list[tuple] = []
                    last_flush = time.monotonic()
                    done = False

                    async for raw in ws:
                        row = to_row(json.loads(raw))
                        if row:
                            if end and datetime.fromisoformat(row[2]) >= end:
                                done = True
                            else:
                                rows.append(row)

                        if (
                            done
                            or len(rows) >= BATCH_SIZE
                            or time.monotonic() - last_flush > FLUSH_SECONDS
                        ):
                            if rows:
                                await store(conn, rows)
                                cursor = rows[-1][0]
                                total += len(rows)
                                rate = total / (time.monotonic() - started)
                                log.info(
                                    "%s: %d events (%.0f/s), up to %s",
                                    name,
                                    total,
                                    rate,
                                    rows[-1][2][:19],
                                )
                                rows = []
                            last_flush = time.monotonic()
                        if done:
                            log.info("%s: finished", name)
                            return

            except (websockets.WebSocketException, OSError) as e:
                log.warning(
                    "%s: connection lost (%s), retrying in %ds", name, e, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)


def run_worker(
    name: str, cursor: int | None, start: datetime | None, end: datetime | None
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    try:
        asyncio.run(collect(name, cursor, start, end))
    except KeyboardInterrupt:
        pass


def backfill(hours: float, workers: int, end: datetime) -> None:
    start = end - timedelta(hours=hours)
    step = (end - start) / workers

    procs = []
    for i in range(workers):
        slice_start = start + step * i
        slice_end = slice_start + step
        cursor = int(slice_start.timestamp() * 1_000_000)
        name = f"w{i} {slice_start:%d %H:%M}-{slice_end:%H:%M}"
        procs.append(
            mp.Process(
                target=run_worker,
                args=(name, cursor, slice_start, slice_end),
                name=name,
            )
        )

    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()
        return

    failed = [p.name for p in procs if p.exitcode != 0]
    if failed:
        raise SystemExit(f"{len(failed)} worker(s) failed; re-run to resume")


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--backfill-hours", type=float, help="replay this many hours of history"
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="parallel connections for backfill"
    )
    parser.add_argument(
        "--end",
        type=parse_utc,
        default=datetime.now(timezone.utc),
        help="backfill up to this UTC time (default: now); pass the same value to resume a run",
    )
    args = parser.parse_args()

    if args.backfill_hours:
        backfill(args.backfill_hours, args.workers, args.end)
    else:
        run_worker("live", None, None, None)


if __name__ == "__main__":
    main()
