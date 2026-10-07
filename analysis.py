# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars>=2",
#     "connectorx>=0.4",
#     "sqlalchemy>=2",
#     "psycopg[binary]>=3.2",
#     "pyarrow>=17",
#     "python-dotenv>=1",
# ]
# ///
"""The code from "Postgres to DataFrame, Ten Years Later", in order.

uv run analysis.py export   # Postgres -> data/events/*.parquet
uv run analysis.py imports  # how many posts were backdated imports
uv run analysis.py query    # the SQL example over the Parquet files
uv run analysis.py sort     # the larger-than-memory sort
"""

import os
import sys
import time
from pathlib import Path
from urllib.parse import quote

import polars as pl
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(".env")


def database_uri() -> str:
    user = quote(os.environ["PGUSER"], safe="")
    password = quote(os.environ["PGPASSWORD"], safe="")
    return (
        f"postgresql://{user}:{password}"
        f"@{os.environ['PGHOST']}:{os.environ.get('PGPORT', '5432')}"
        f"/{os.environ['PGDATABASE']}"
    )


EXPORT_QUERY = """
    SELECT
        seq, did, time, operation, collection, rkey,
        record->>'createdAt'        AS created_at,
        record->'langs'->>0         AS lang,
        record->>'text'             AS text,
        record->'subject'->>'uri'   AS subject_uri
    FROM events
"""

LIKES_BY_LANGUAGE = """
    WITH posts AS (
        SELECT 'at://' || did || '/app.bsky.feed.post/' || rkey AS uri, did, lang
        FROM events
        WHERE collection = 'app.bsky.feed.post' AND operation = 'create'
          AND created_at > time - INTERVAL '1 hour'
    ),
    likes AS (
        SELECT subject_uri AS uri, COUNT(*) AS likes
        FROM events
        WHERE collection = 'app.bsky.feed.like' AND operation = 'create'
        GROUP BY subject_uri
    )
    SELECT
        l.language,
        COUNT(DISTINCT p.did) AS accounts,
        COUNT(*) AS posts,
        SUM(COALESCE(k.likes, 0)) / COUNT(*) AS likes_per_post
    FROM posts p
    JOIN languages l ON SPLIT_PART(p.lang, '-', 1) = l.code
    LEFT JOIN likes k ON p.uri = k.uri
    GROUP BY l.language
    HAVING COUNT(DISTINCT p.did) > 1000
    ORDER BY likes_per_post DESC
"""


IMPORTED_POSTS = """
    SELECT
        COUNT(*) AS posts,
        SUM(CASE WHEN created_at < time - INTERVAL '1 day' THEN 1 ELSE 0 END) AS backdated,
        ROUND(AVG(CASE WHEN created_at < time - INTERVAL '1 day' THEN 100.0 ELSE 0 END), 1) AS pct
    FROM events
    WHERE collection = 'app.bsky.feed.post' AND operation = 'create'
"""


def imports(source: str = "data/events/*.parquet") -> None:
    ctx = pl.SQLContext(events=pl.scan_parquet(source))
    print(ctx.execute(IMPORTED_POSTS).collect())


def read() -> None:
    counts = pl.read_database_uri(
        "SELECT collection, count(*) AS n FROM events GROUP BY collection",
        database_uri(),
    )
    print(counts)


def export(out: Path = Path("data/events"), limit: int | None = None) -> None:
    engine = create_engine(
        database_uri().replace("postgresql://", "postgresql+psycopg://")
    )
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("part-*.parquet"):
        old.unlink()
    query = EXPORT_QUERY + (f" LIMIT {limit}" if limit else "")

    with engine.connect() as conn:
        batches = pl.read_database(
            query,
            connection=conn.execution_options(stream_results=True),
            iter_batches=True,
            batch_size=500_000,
            schema_overrides={
                "created_at": pl.String,
                "lang": pl.String,
                "text": pl.String,
                "subject_uri": pl.String,
            },
        )
        for i, batch in enumerate(batches):
            batch = batch.with_columns(
                pl.col("time").dt.convert_time_zone("UTC"),
                pl.col("created_at").str.to_datetime(strict=False, time_zone="UTC"),
            )
            batch.write_parquet(out / f"part-{i:04d}.parquet")
            print(f"wrote part {i} ({len(batch):,} rows)")


def query(source: str = "data/events/*.parquet") -> None:
    ctx = pl.SQLContext(
        events=pl.scan_parquet(source),
        languages=pl.scan_csv("data/languages.csv"),
    )
    lf = ctx.execute(LIKES_BY_LANGUAGE)
    print(lf.explain())
    print(lf.collect_schema())
    start = time.perf_counter()
    result = lf.collect()
    print(result)
    print(f"collected in {time.perf_counter() - start:.1f}s")


def sort(source: str = "data/events/*.parquet") -> None:
    start = time.perf_counter()
    (
        pl.scan_parquet(source)
        .sort("text", nulls_last=True)
        .sink_parquet("data/sorted.parquet")
    )
    print(f"sorted in {time.perf_counter() - start:.1f}s")


if __name__ == "__main__":
    {"read": read, "export": export, "imports": imports, "query": query, "sort": sort}[
        sys.argv[1]
    ]()
