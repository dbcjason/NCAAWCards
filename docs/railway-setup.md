# Railway Setup

This repo is set up to use Railway for payload generation without moving the website off Vercel.

## Recommended Railway services

Create these services from the `dbcjason/NCAAWCards` repo:

1. `ncaaw-worker`
2. `ncaaw-nightly-0`
3. `ncaaw-nightly-1`
4. `ncaaw-nightly-2`
5. `ncaaw-nightly-3`
6. `ncaaw-nightly-4`
7. `ncaaw-nightly-5`
8. `ncaaw-nightly-6`
9. `ncaaw-nightly-7`
10. `ncaaw-nightly-8`
11. `ncaaw-nightly-9`
12. `ncaaw-merge`

Use the repo root as the service root directory.

## Build command

Set this in Railway service settings:

```bash
python -m pip install --upgrade pip && python -m pip install pandas
```

## Start commands

### Sharded nightly cron services

Use:

```bash
python3 player_cards_pipeline/scripts/railway_run_payload_job.py
```

Suggested variables for each shard:

- `YEARS=2026`
- `MIN_GAMES=5`
- `INCREMENTAL=true`
- `CHUNK_COUNT=10`
- `CHUNK_INDEX=0` through `9`
- `CHECKPOINT_EVERY=25`
- `WRITE_SHARD_FILES=true`

Optional:

- `LIMIT=50` for smoke tests
- `TARGETS_FILE=/data/targets.json` for changed-player runs later

### Merge service

Use the same start command:

```bash
python3 player_cards_pipeline/scripts/railway_run_payload_job.py
```

Suggested variables:

- `YEARS=2026`
- `CHUNK_COUNT=10`
- `MERGE_SHARDS=true`

### Persistent warm worker

Use:

```bash
python3 player_cards_pipeline/scripts/railway_warm_worker.py
```

Suggested variables:

- `WARM_SEASON=2026`
- `HEARTBEAT_SECONDS=300`

## Volumes

Attach a Railway volume and mount it at:

```text
/data
```

Recommended later uses:

- shard checkpoint files
- sqlite card caches
- changed-player target files

## Notes

- Keep the frontend on Vercel.
- Use Railway for heavy payload generation.
- Use Supabase for metadata/indexing.
- Keep GitHub as payload storage for now.
