# videosaurous

Local catalog, downloader, and tracking system for **videsaur.com** — a Tamil/Telugu meme + reaction video site. Mirrors the public catalog, downloads every video to disk, computes integrity hashes, and tracks new/removed videos over time via incremental sync.

The name: **video + dinosaur** — a giant archive that keeps growing.

## What it does

- Walks the site's paginated API and caches every video record locally.
- Downloads every video file from CloudFront (resumable, polite, concurrent).
- Stores everything in a single SQLite database with full metadata, per-file SHA256 hashes, integrity status, and a per-video event log.
- Detects duplicates (same content uploaded under different IDs).
- Verifies file integrity (hash, size, ffprobe).
- Runs incremental syncs that download only new videos and mark videos that disappear from the site.

## Quick start

Requires Python 3.11+ and [`uv`](https://docs.astral.sh/uv/). All scripts are single-file with PEP 723 inline dependencies — `uv run` handles the env.

```bash
# 1. Fetch the full catalog metadata (~3 min, hits site 239 times, cached)
uv run fetch_catalog.py

# 2. Optional: verify every video URL is reachable (~3 min, HEAD requests only)
uv run survey.py

# 3. Download every video file (~30 min for ~8.5 GB at default concurrency=4)
uv run download_videos.py

# 4. Bootstrap the local database (hashes every file, ~30s)
uv run videsaur.py init
```

## Recurring use

```bash
# Sync against the live API — downloads only NEW videos, marks removed ones
uv run videsaur.py sync --concurrency 4

# Re-verify every file (hash + size + ffprobe). Slow, do monthly.
uv run videsaur.py verify --full

# Find byte-identical duplicates
uv run videsaur.py dedupe

# Overview
uv run videsaur.py stats

# Filter / search / inspect
uv run videsaur.py list --missing
uv run videsaur.py list --corrupt
uv run videsaur.py list --category Tamil
uv run videsaur.py search vadivelu
uv run videsaur.py show 8534
```

## Daily automation (cron)

```cron
0 3 * * * cd /path/to/videosaurous && /home/$USER/.local/bin/uv run videsaur.py sync >> sync.log 2>&1
```

## Architecture

| File | Role |
|------|------|
| `fetch_catalog.py` | Walks `/api/v1/video?page=N` with per-page disk cache. Polite delays. Writes `catalog.json`. |
| `survey.py` | Async HEAD against every video URL. Reports exact total bytes + dead URLs + rate-limit signal. |
| `download_videos.py` | Async downloader. Per-file `.part` + HTTP Range resume. Atomic rename. Configurable concurrency. |
| `videsaur.py` | Main CLI: `init`, `sync`, `verify`, `dedupe`, `stats`, `list`, `search`, `show`. SQLite-backed. |

## Database schema

Three tables in `videsaur.db`:

- **`videos`** — every video the API has ever returned. Full metadata mirror + local file state (`local_path`, `sha256`, `file_size`, `integrity_status`) + sync state (`first_seen_at`, `last_seen_at`, `removed_at`).
- **`sync_runs`** — one row per `sync` invocation, with counts of new/updated/removed/downloaded.
- **`events`** — per-video timeline: `added`, `updated`, `removed`, `re-added`, `downloaded`, `download_failed`, `corrupt`, `missing` — each with a JSON `details` payload.

`removed_at IS NULL` means the video is still live in the catalog. Removed videos retain their DB row (with a `removed_at` timestamp); files on disk are kept until manually deleted.

## Design notes

- **Origin vs CDN separation:** the catalog API (`videsaur.com`) is hit politely with delays between page requests. Video files come from AWS CloudFront, which tolerates much higher request rates (tested at concurrency=8 with zero throttling).
- **Resumable everything:** every phase can be Ctrl-C'd and rerun without losing work. `fetch_catalog` caches each page; `download_videos` resumes partial files via HTTP Range; `init` skips already-hashed files.
- **Idempotent sync:** running `sync` against an unchanged catalog produces zero downloads and zero events.
- **Forensic columns:** each `videos` row carries the full original API JSON in `raw_json`. If the upstream API schema ever changes, no data is lost.

## License

Private / personal archive. Respect videsaur.com's terms of service.
