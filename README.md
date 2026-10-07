# polars-2-postgres

Companion code for the "Postgres to DataFrame, Ten Years Later" blog post. `collector.py` streams Bluesky posts, likes, reposts and follows from [Jetstream](https://bsky.network/docs/jetstream/) into Postgres.

## Run

1. Create a `.env` with the standard `PG*` variables (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`) and start Postgres:

   ```bash
   podman run -d --name bluesky-pg \
     -e POSTGRES_PASSWORD=jetstream -e POSTGRES_DB=bluesky \
     -p 5432:5432 -v bluesky-pg:/var/lib/postgresql/data \
     postgres:17
   ```

2. Start collecting:

   ```bash
   uv run collector.py
   ```

Stop with Ctrl+C. Restarting resumes from the last stored event; Jetstream may resend a few events, which are ignored.

## Check size

```bash
podman exec bluesky-pg psql -U postgres -d bluesky \
  -c "SELECT count(*), pg_size_pretty(pg_total_relation_size('events')) FROM events"
```

## Analysis

```bash
uv run analysis.py export   # Postgres -> data/events/*.parquet
uv run analysis.py imports  # share of backdated (imported) posts
uv run analysis.py query    # likes per post by language, via Polars SQL
```

Spill-to-disk test, sorting everything inside a container capped at 1.5 GB:

```bash
mkdir -p data/spill
podman run --rm --memory 1536m --memory-swap 1536m \
  -e POLARS_OOC_MEMORY_BUDGET_MB=500 -e POLARS_OOC_SPILL_DIR=/spill \
  -v "$PWD/data/spill:/spill" -v "$PWD/data:/data" -v "$PWD/spill_test.py:/spill_test.py" \
  python:3.12-slim-bookworm sh -c "pip install -q 'polars>=2' && python /spill_test.py"
```

## Note

`record` holds the full public record, including post text. Publish aggregates, not individual posts.
