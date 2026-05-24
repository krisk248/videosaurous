# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""
videsaur — local catalog + sync tool for videsaur.com videos.

A single SQLite database tracks every video the API has ever exposed,
its full metadata, local file state, SHA256 hash, and a per-video event log.
Designed to be run repeatedly: each `sync` only downloads what's new and
records what disappeared.

Commands:
  init              create DB, import catalog.json + videos/, hash all files
  sync              refetch API, diff against DB, download new, mark removed
  verify            re-hash every file, detect corruption + missing + orphans
  dedupe            find SHA256 duplicates across video IDs
  stats             counts, sizes, recent sync history
  list              list videos with filters (--missing, --removed, --category)
  search            substring search over name/description/keywords
  show <id>         dump full record + event history for one video

Run any of these via:  uv run videsaur.py <command> [...]
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx

# ---------------------------------------------------------------------------
# constants & helpers
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "videsaur.db"
VIDEOS_DIR = ROOT / "videos"
CATALOG_JSON = ROOT / "catalog.json"

API_BASE = "https://videsaur.com/api/v1/video"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

CLOUDFRONT_RE = re.compile(r"/videos/([^/]+)\.mp4$")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sanitize(name: str) -> str:
    name = re.sub(r"\s+", " ", name or "").strip()
    name = re.sub(r"[\\/*?:\"<>|\x00-\x1f]", "_", name)
    return name[:80] or "video"


def cloudfront_hash(url: str | None) -> str | None:
    if not url:
        return None
    m = CLOUDFRONT_RE.search(url)
    return m.group(1) if m else None


def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# ---------------------------------------------------------------------------
# database
# ---------------------------------------------------------------------------

SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS videos (
    id                  INTEGER PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    keywords            TEXT,
    remarks             TEXT,
    status              INTEGER,

    url                 TEXT,
    cloudfront_hash     TEXT,
    gif_url             TEXT,
    thumb_url           TEXT,

    duration            TEXT,
    views               INTEGER,
    registered_view     INTEGER,
    unregistered_view   INTEGER,
    likes               INTEGER,
    dislikes            INTEGER,
    download_count      INTEGER,

    uploader_id         INTEGER,
    uploader_name       TEXT,
    uploader_image      TEXT,

    categories_json     TEXT,
    tags_json           TEXT,
    language_json       TEXT,

    api_created_at      TEXT,
    api_updated_at      TEXT,
    api_deleted_at      TEXT,

    local_path          TEXT,
    file_size           INTEGER,
    sha256              TEXT,
    downloaded_at       TEXT,
    verified_at         TEXT,
    integrity_status    TEXT,

    first_seen_at       TEXT NOT NULL,
    last_seen_at        TEXT NOT NULL,
    removed_at          TEXT,

    raw_json            TEXT
);

CREATE INDEX IF NOT EXISTS idx_videos_sha256 ON videos(sha256);
CREATE INDEX IF NOT EXISTS idx_videos_removed ON videos(removed_at);
CREATE INDEX IF NOT EXISTS idx_videos_last_seen ON videos(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_videos_cfhash ON videos(cloudfront_hash);

CREATE TABLE IF NOT EXISTS sync_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    pages_fetched   INTEGER,
    api_total       INTEGER,
    new_count       INTEGER DEFAULT 0,
    updated_count   INTEGER DEFAULT 0,
    removed_count   INTEGER DEFAULT 0,
    download_ok     INTEGER DEFAULT 0,
    download_failed INTEGER DEFAULT 0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id    INTEGER NOT NULL,
    event_type  TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    details     TEXT,
    FOREIGN KEY (video_id) REFERENCES videos(id)
);

CREATE INDEX IF NOT EXISTS idx_events_video ON events(video_id);
CREATE INDEX IF NOT EXISTS idx_events_when ON events(occurred_at);
"""


def db_open() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def event(conn: sqlite3.Connection, video_id: int, event_type: str,
          details: dict | None = None) -> None:
    conn.execute(
        "INSERT INTO events(video_id, event_type, occurred_at, details) "
        "VALUES (?, ?, ?, ?)",
        (video_id, event_type, now_utc(),
         json.dumps(details, ensure_ascii=False) if details else None),
    )


# ---------------------------------------------------------------------------
# api -> row mapping
# ---------------------------------------------------------------------------

VIDEO_COLUMNS = [
    "id", "name", "description", "keywords", "remarks", "status",
    "url", "cloudfront_hash", "gif_url", "thumb_url",
    "duration", "views", "registered_view", "unregistered_view",
    "likes", "dislikes", "download_count",
    "uploader_id", "uploader_name", "uploader_image",
    "categories_json", "tags_json", "language_json",
    "api_created_at", "api_updated_at", "api_deleted_at",
    "raw_json",
]


def video_to_row(v: dict) -> dict:
    up = v.get("upload_by") or {}
    return {
        "id": v["id"],
        "name": v.get("name"),
        "description": v.get("description"),
        "keywords": v.get("keywords"),
        "remarks": v.get("remarks"),
        "status": v.get("status"),
        "url": v.get("url"),
        "cloudfront_hash": cloudfront_hash(v.get("url")),
        "gif_url": v.get("gif"),
        "thumb_url": v.get("image"),
        "duration": v.get("duration"),
        "views": v.get("views"),
        "registered_view": v.get("registered_view"),
        "unregistered_view": v.get("unregistered_view"),
        "likes": v.get("like"),
        "dislikes": v.get("dislike"),
        "download_count": v.get("download_count"),
        "uploader_id": up.get("id"),
        "uploader_name": up.get("name"),
        "uploader_image": up.get("image"),
        "categories_json": json.dumps(v.get("categories") or [], ensure_ascii=False),
        "tags_json": json.dumps(v.get("tags") or [], ensure_ascii=False),
        "language_json": json.dumps(v.get("language") or [], ensure_ascii=False),
        "api_created_at": v.get("created_at"),
        "api_updated_at": v.get("updated_at"),
        "api_deleted_at": v.get("deleted_at"),
        "raw_json": json.dumps(v, ensure_ascii=False),
    }


MEANINGFUL_FIELDS = ("name", "description", "url", "status", "api_deleted_at",
                     "uploader_id", "categories_json", "language_json")


def diff_row(old: sqlite3.Row, new: dict) -> dict:
    """Return {field: (old, new)} for meaningful differences."""
    changes = {}
    for f in MEANINGFUL_FIELDS:
        if (old[f] if old else None) != new.get(f):
            changes[f] = [old[f] if old else None, new.get(f)]
    return changes


def upsert_video(conn: sqlite3.Connection, row: dict,
                 first_seen: bool) -> None:
    now = now_utc()
    if first_seen:
        row["first_seen_at"] = now
    row["last_seen_at"] = now
    cols = VIDEO_COLUMNS + ["last_seen_at"]
    if first_seen:
        cols = cols + ["first_seen_at"]
    placeholders = ",".join(f":{c}" for c in cols)
    update_set = ",".join(f"{c}=excluded.{c}" for c in cols if c != "id")
    conn.execute(
        f"INSERT INTO videos ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {update_set}, removed_at=NULL",
        row,
    )


# ---------------------------------------------------------------------------
# integrity helpers
# ---------------------------------------------------------------------------

def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def ffprobe_ok(path: Path) -> tuple[bool, str | None]:
    """Return (is_valid, duration_seconds_str)."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode != 0:
            return False, None
        dur = r.stdout.strip()
        return bool(dur), dur or None
    except FileNotFoundError:
        return True, None       # ffprobe not installed — don't fail
    except subprocess.TimeoutExpired:
        return False, None


HAS_FFPROBE = shutil.which("ffprobe") is not None


# ---------------------------------------------------------------------------
# init command
# ---------------------------------------------------------------------------

def cmd_init(args: argparse.Namespace) -> int:
    conn = db_open()
    existing_ids = {r[0] for r in conn.execute("SELECT id FROM videos")}
    print(f"DB: {DB_PATH}  (existing rows: {len(existing_ids)})")

    if not CATALOG_JSON.exists():
        print("catalog.json missing — run fetch_catalog.py first", file=sys.stderr)
        return 1
    videos = json.loads(CATALOG_JSON.read_text())
    print(f"importing {len(videos)} videos from catalog.json")

    # index existing files by id (prefix of filename)
    file_by_id: dict[int, Path] = {}
    if VIDEOS_DIR.exists():
        for p in VIDEOS_DIR.iterdir():
            if p.suffix == ".mp4" and "_" in p.name:
                try:
                    vid = int(p.name.split("_", 1)[0])
                    file_by_id[vid] = p
                except ValueError:
                    pass
    print(f"found {len(file_by_id)} .mp4 files on disk")

    sync_id = conn.execute(
        "INSERT INTO sync_runs(started_at, notes) VALUES (?, ?)",
        (now_utc(), "init"),
    ).lastrowid

    new = 0
    for v in videos:
        row = video_to_row(v)
        first = v["id"] not in existing_ids
        upsert_video(conn, row, first_seen=first)
        if first:
            new += 1
            event(conn, v["id"], "added", {"source": "init"})
    conn.commit()
    print(f"  imported metadata: {new} new, {len(videos)-new} existing")

    # hash files in a thread pool
    to_hash = []
    for vid, path in file_by_id.items():
        row = conn.execute(
            "SELECT sha256, file_size, local_path FROM videos WHERE id=?",
            (vid,),
        ).fetchone()
        if row is None:
            continue   # orphan file, no metadata
        size = path.stat().st_size
        # if size + path already match and hash exists, skip
        if (row["sha256"] and row["file_size"] == size
                and row["local_path"] == str(path.relative_to(ROOT))):
            continue
        to_hash.append((vid, path, size))

    print(f"hashing {len(to_hash)} files (this is the slow part)...")
    started = time.time()
    hashed = 0
    with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as pool:
        futures = {pool.submit(sha256_of, p): (vid, p, sz)
                   for vid, p, sz in to_hash}
        for fut in futures:
            vid, p, sz = futures[fut]
            digest = fut.result()
            conn.execute(
                "UPDATE videos SET sha256=?, file_size=?, local_path=?, "
                "downloaded_at=COALESCE(downloaded_at, ?), "
                "integrity_status='ok', verified_at=? WHERE id=?",
                (digest, sz, str(p.relative_to(ROOT)),
                 now_utc(), now_utc(), vid),
            )
            hashed += 1
            if hashed % 200 == 0:
                conn.commit()
                rate = hashed / (time.time() - started)
                print(f"  hashed {hashed}/{len(to_hash)}  ({rate:.1f}/s)")
    conn.commit()
    print(f"  hashed total: {hashed} in {time.time()-started:.1f}s")

    # files on disk without metadata
    orphans = [(vid, p) for vid, p in file_by_id.items()
               if conn.execute("SELECT 1 FROM videos WHERE id=?",
                               (vid,)).fetchone() is None]
    if orphans:
        print(f"orphan files (file but no metadata): {len(orphans)}")
        for vid, p in orphans[:10]:
            print(f"  id={vid}  {p.name}")

    # metadata without files
    missing = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE local_path IS NULL "
        "AND removed_at IS NULL"
    ).fetchone()[0]
    print(f"metadata without file: {missing}")

    conn.execute(
        "UPDATE sync_runs SET finished_at=?, new_count=?, "
        "api_total=?, pages_fetched=0 WHERE id=?",
        (now_utc(), new, len(videos), sync_id),
    )
    conn.commit()
    conn.close()
    print("init done.")
    return 0


# ---------------------------------------------------------------------------
# verify command
# ---------------------------------------------------------------------------

def cmd_verify(args: argparse.Namespace) -> int:
    conn = db_open()
    rows = list(conn.execute(
        "SELECT id, name, sha256, file_size, local_path FROM videos "
        "WHERE local_path IS NOT NULL "
        + ("" if args.full else "AND (verified_at IS NULL OR ? = '1')"),
        () if args.full else (("1" if args.recheck else "0"),),
    ))
    print(f"verifying {len(rows)} files (full={args.full}, ffprobe={HAS_FFPROBE})")

    bad_hash = bad_size = missing = bad_ffprobe = ok = 0
    started = time.time()
    for i, r in enumerate(rows, 1):
        path = ROOT / r["local_path"]
        if not path.exists():
            conn.execute(
                "UPDATE videos SET integrity_status='missing', "
                "local_path=NULL, verified_at=? WHERE id=?",
                (now_utc(), r["id"]),
            )
            event(conn, r["id"], "missing", {"path": r["local_path"]})
            missing += 1
            continue
        size = path.stat().st_size
        if r["file_size"] is not None and size != r["file_size"]:
            conn.execute(
                "UPDATE videos SET integrity_status='size-mismatch', "
                "verified_at=? WHERE id=?",
                (now_utc(), r["id"]),
            )
            event(conn, r["id"], "corrupt",
                  {"reason": "size", "expected": r["file_size"], "got": size})
            bad_size += 1
            continue
        digest = sha256_of(path)
        if r["sha256"] and digest != r["sha256"]:
            conn.execute(
                "UPDATE videos SET integrity_status='hash-mismatch', "
                "verified_at=? WHERE id=?",
                (now_utc(), r["id"]),
            )
            event(conn, r["id"], "corrupt",
                  {"reason": "hash", "expected": r["sha256"], "got": digest})
            bad_hash += 1
            continue
        if HAS_FFPROBE:
            valid, dur = ffprobe_ok(path)
            if not valid:
                conn.execute(
                    "UPDATE videos SET integrity_status='ffprobe-fail', "
                    "verified_at=? WHERE id=?",
                    (now_utc(), r["id"]),
                )
                event(conn, r["id"], "corrupt", {"reason": "ffprobe"})
                bad_ffprobe += 1
                continue
            if dur and (not r["duration"] or r["duration"] != dur):
                conn.execute(
                    "UPDATE videos SET duration=?, integrity_status='ok', "
                    "verified_at=?, sha256=? WHERE id=?",
                    (dur, now_utc(), digest, r["id"]),
                )
        conn.execute(
            "UPDATE videos SET integrity_status='ok', verified_at=?, "
            "sha256=COALESCE(sha256, ?) WHERE id=?",
            (now_utc(), digest, r["id"]),
        )
        ok += 1
        if i % 500 == 0:
            conn.commit()
            print(f"  {i}/{len(rows)}  ok={ok} bad_hash={bad_hash} "
                  f"bad_size={bad_size} bad_ffprobe={bad_ffprobe} "
                  f"missing={missing}")
    conn.commit()
    print(f"\nverify done in {time.time()-started:.1f}s")
    print(f"  ok={ok}  hash-mismatch={bad_hash}  size-mismatch={bad_size}  "
          f"ffprobe-fail={bad_ffprobe}  missing={missing}")

    orphans = []
    if VIDEOS_DIR.exists():
        on_disk = {p.name.split("_", 1)[0]: p for p in VIDEOS_DIR.iterdir()
                   if p.suffix == ".mp4"}
        for prefix, p in on_disk.items():
            try:
                vid = int(prefix)
            except ValueError:
                continue
            if not conn.execute("SELECT 1 FROM videos WHERE id=?",
                                (vid,)).fetchone():
                orphans.append(p)
    if orphans:
        print(f"orphan files (no DB row): {len(orphans)}")
        for p in orphans[:10]:
            print(f"  {p.name}")
    conn.close()
    return 0


# ---------------------------------------------------------------------------
# dedupe command
# ---------------------------------------------------------------------------

def cmd_dedupe(args: argparse.Namespace) -> int:
    conn = db_open()
    groups = list(conn.execute(
        "SELECT sha256, COUNT(*) c, SUM(file_size) bytes, "
        "GROUP_CONCAT(id) ids "
        "FROM videos WHERE sha256 IS NOT NULL "
        "GROUP BY sha256 HAVING c > 1 ORDER BY c DESC, bytes DESC"
    ))
    if not groups:
        print("no duplicate files found.")
        return 0

    total_wasted = sum(g["bytes"] - g["bytes"] // g["c"] for g in groups)
    print(f"duplicate groups: {len(groups)}  "
          f"wasted: ~{fmt_bytes(total_wasted)}\n")
    for g in groups[: args.limit]:
        ids = sorted(int(x) for x in g["ids"].split(","))
        members = list(conn.execute(
            "SELECT id, name, local_path, first_seen_at FROM videos "
            "WHERE id IN (" + ",".join("?" * len(ids)) + ") "
            "ORDER BY first_seen_at",
            ids,
        ))
        print(f"sha256 {g['sha256'][:16]}…  ×{g['c']}  "
              f"{fmt_bytes(g['bytes'] // g['c'])}/each")
        for m in members:
            print(f"   id={m['id']:>5}  first_seen={m['first_seen_at']}  "
                  f"{(m['name'] or '')[:50]}")
        print()
    if len(groups) > args.limit:
        print(f"...and {len(groups) - args.limit} more groups "
              f"(use --limit N to see more)")
    conn.close()
    return 0


# ---------------------------------------------------------------------------
# sync command
# ---------------------------------------------------------------------------

async def fetch_all_pages(client: httpx.AsyncClient) -> list[dict]:
    r = await client.get(API_BASE, params={"page": 1}, timeout=30.0)
    r.raise_for_status()
    first = r.json()
    last_page = first["last_page"]
    print(f"  API says {first['total']} videos / {last_page} pages")

    pages: dict[int, dict] = {1: first}
    sem = asyncio.Semaphore(4)

    async def fetch_page(p: int) -> None:
        async with sem:
            for attempt in range(1, 6):
                try:
                    resp = await client.get(API_BASE, params={"page": p},
                                            timeout=30.0)
                    if resp.status_code == 429 or resp.status_code >= 500:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    resp.raise_for_status()
                    pages[p] = resp.json()
                    return
                except httpx.HTTPError:
                    await asyncio.sleep(2 ** attempt)
            raise RuntimeError(f"page {p} failed")

    await asyncio.gather(*[fetch_page(p) for p in range(2, last_page + 1)])
    all_videos = []
    for p in sorted(pages):
        all_videos.extend(pages[p]["data"])
    print(f"  fetched {len(pages)} pages, {len(all_videos)} video records")
    return all_videos


async def download_video(client: httpx.AsyncClient, v: dict,
                         sem: asyncio.Semaphore) -> tuple[bool, Path | None, int, str | None]:
    async with sem:
        url = v.get("url")
        if not url:
            return False, None, 0, None
        name = sanitize(v.get("name") or f"video_{v['id']}")
        final = VIDEOS_DIR / f"{v['id']:06d}_{name}.mp4"
        part = final.with_suffix(final.suffix + ".part")
        delay = 2.0
        for _ in range(5):
            try:
                headers = {}
                mode = "wb"
                if part.exists():
                    headers["Range"] = f"bytes={part.stat().st_size}-"
                    mode = "ab"
                async with client.stream("GET", url, headers=headers,
                                          timeout=60.0) as r:
                    if r.status_code == 416:
                        part.rename(final)
                        return True, final, final.stat().st_size, None
                    if r.status_code == 429 or r.status_code >= 500:
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue
                    r.raise_for_status()
                    h = hashlib.sha256()
                    if mode == "ab" and part.exists():
                        # restart hash since we can't rehash existing partial
                        h = None
                    with part.open(mode) as f:
                        async for chunk in r.aiter_bytes(64 * 1024):
                            f.write(chunk)
                            if h is not None:
                                h.update(chunk)
                part.rename(final)
                digest = h.hexdigest() if h is not None else sha256_of(final)
                return True, final, final.stat().st_size, digest
            except httpx.HTTPError:
                await asyncio.sleep(delay)
                delay *= 2
        return False, None, 0, None


async def sync_async(conn: sqlite3.Connection, concurrency: int) -> dict:
    async with httpx.AsyncClient(headers={"User-Agent": UA},
                                  http2=True, follow_redirects=True) as client:
        api_videos = await fetch_all_pages(client)
        api_ids = {v["id"] for v in api_videos}
        db_ids = {r[0] for r in conn.execute(
            "SELECT id FROM videos WHERE removed_at IS NULL"
        )}

        new_ids = api_ids - db_ids
        gone_ids = db_ids - api_ids

        print(f"  diff: +{len(new_ids)} new, -{len(gone_ids)} gone")

        # upsert all metadata
        updated = 0
        for v in api_videos:
            row = video_to_row(v)
            existing = conn.execute(
                "SELECT * FROM videos WHERE id=?", (v["id"],)
            ).fetchone()
            changes = diff_row(existing, row)
            first = existing is None
            upsert_video(conn, row, first_seen=first)
            if first:
                event(conn, v["id"], "added", {"source": "sync"})
            elif changes:
                event(conn, v["id"], "updated", changes)
                updated += 1
            elif existing["removed_at"]:
                event(conn, v["id"], "re-added", None)
        conn.commit()

        # mark removed
        for vid in gone_ids:
            conn.execute("UPDATE videos SET removed_at=? WHERE id=?",
                         (now_utc(), vid))
            event(conn, vid, "removed", None)
        conn.commit()

        # download new
        VIDEOS_DIR.mkdir(exist_ok=True)
        sem = asyncio.Semaphore(concurrency)
        to_dl = [v for v in api_videos if v["id"] in new_ids]
        print(f"  downloading {len(to_dl)} new videos at concurrency={concurrency}")
        ok = fail = 0
        if to_dl:
            tasks = [download_video(client, v, sem) for v in to_dl]
            for v, fut in zip(to_dl,
                              await asyncio.gather(*tasks, return_exceptions=True)):
                if isinstance(fut, Exception):
                    fail += 1
                    event(conn, v["id"], "download_failed", {"error": str(fut)})
                    continue
                success, path, size, digest = fut
                if success and path:
                    conn.execute(
                        "UPDATE videos SET local_path=?, file_size=?, "
                        "sha256=?, downloaded_at=?, verified_at=?, "
                        "integrity_status='ok' WHERE id=?",
                        (str(path.relative_to(ROOT)), size, digest,
                         now_utc(), now_utc(), v["id"]),
                    )
                    event(conn, v["id"], "downloaded",
                          {"size": size, "sha256": digest})
                    ok += 1
                    print(f"    + id={v['id']:>5}  {fmt_bytes(size)}  "
                          f"{(v.get('name') or '')[:45]}")
                else:
                    fail += 1
                    event(conn, v["id"], "download_failed", None)
            conn.commit()

        return {
            "api_total": len(api_videos),
            "new": len(new_ids),
            "removed": len(gone_ids),
            "updated": updated,
            "download_ok": ok,
            "download_failed": fail,
        }


def cmd_sync(args: argparse.Namespace) -> int:
    conn = db_open()
    sync_id = conn.execute(
        "INSERT INTO sync_runs(started_at, notes) VALUES (?, ?)",
        (now_utc(), "sync"),
    ).lastrowid
    conn.commit()
    print(f"sync run #{sync_id} starting at {now_utc()}")
    try:
        result = asyncio.run(sync_async(conn, args.concurrency))
        conn.execute(
            "UPDATE sync_runs SET finished_at=?, api_total=?, new_count=?, "
            "updated_count=?, removed_count=?, download_ok=?, download_failed=? "
            "WHERE id=?",
            (now_utc(), result["api_total"], result["new"],
             result["updated"], result["removed"],
             result["download_ok"], result["download_failed"], sync_id),
        )
        conn.commit()
        print(f"sync done: +{result['new']} new, ~{result['updated']} updated, "
              f"-{result['removed']} removed, "
              f"download ok={result['download_ok']} fail={result['download_failed']}")
        return 0 if result["download_failed"] == 0 else 2
    except Exception as e:
        conn.execute("UPDATE sync_runs SET finished_at=?, notes=? WHERE id=?",
                     (now_utc(), f"error: {e}", sync_id))
        conn.commit()
        print(f"sync failed: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# stats / list / search / show
# ---------------------------------------------------------------------------

def cmd_stats(args: argparse.Namespace) -> int:
    conn = db_open()
    total = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    active = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE removed_at IS NULL"
    ).fetchone()[0]
    removed = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE removed_at IS NOT NULL"
    ).fetchone()[0]
    downloaded = conn.execute(
        "SELECT COUNT(*), SUM(file_size) FROM videos WHERE local_path IS NOT NULL"
    ).fetchone()
    missing = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE local_path IS NULL AND removed_at IS NULL"
    ).fetchone()[0]
    bad = conn.execute(
        "SELECT integrity_status, COUNT(*) FROM videos "
        "WHERE integrity_status NOT IN ('ok') AND integrity_status IS NOT NULL "
        "GROUP BY integrity_status"
    ).fetchall()
    dupes = conn.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM videos WHERE sha256 IS NOT NULL "
        "GROUP BY sha256 HAVING COUNT(*) > 1)"
    ).fetchone()[0]

    print(f"videos total       : {total}")
    print(f"  active           : {active}")
    print(f"  removed          : {removed}")
    print(f"downloaded         : {downloaded[0]} ({fmt_bytes(downloaded[1] or 0)})")
    print(f"missing locally    : {missing}")
    print(f"duplicate sha256s  : {dupes}")
    for status, c in bad:
        print(f"  integrity {status}: {c}")

    print("\nrecent sync runs:")
    for r in conn.execute(
        "SELECT id, started_at, finished_at, new_count, removed_count, "
        "download_ok, download_failed FROM sync_runs "
        "ORDER BY id DESC LIMIT 5"
    ):
        print(f"  #{r['id']:>3}  {r['started_at']}  new={r['new_count']}  "
              f"rm={r['removed_count']}  dl_ok={r['download_ok']}  "
              f"dl_fail={r['download_failed']}")

    print("\ntop categories:")
    cats: dict[str, int] = {}
    for r in conn.execute(
        "SELECT categories_json FROM videos WHERE removed_at IS NULL"
    ):
        for c in json.loads(r["categories_json"] or "[]"):
            cats[c["name"]] = cats.get(c["name"], 0) + 1
    for name, count in sorted(cats.items(), key=lambda x: -x[1])[:10]:
        print(f"  {count:>5}  {name}")

    conn.close()
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    conn = db_open()
    where = []
    params: list = []
    if args.missing:
        where.append("local_path IS NULL AND removed_at IS NULL")
    if args.removed:
        where.append("removed_at IS NOT NULL")
    if args.corrupt:
        where.append("integrity_status NOT IN ('ok') AND integrity_status IS NOT NULL")
    if args.category:
        where.append("categories_json LIKE ?")
        params.append(f"%{args.category}%")
    sql = "SELECT id, name, local_path, integrity_status, file_size FROM videos"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(args.limit)
    for r in conn.execute(sql, params):
        size = fmt_bytes(r["file_size"]) if r["file_size"] else "—"
        print(f"  id={r['id']:>5}  {size:>10}  "
              f"{r['integrity_status'] or '—':<14}  "
              f"{(r['name'] or '')[:60]}")
    conn.close()
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    conn = db_open()
    q = f"%{args.query}%"
    rows = list(conn.execute(
        "SELECT id, name, local_path FROM videos "
        "WHERE name LIKE ? OR description LIKE ? OR keywords LIKE ? "
        "ORDER BY id DESC LIMIT ?",
        (q, q, q, args.limit),
    ))
    print(f"{len(rows)} match(es) for {args.query!r}")
    for r in rows:
        print(f"  id={r['id']:>5}  {(r['name'] or '')[:70]}")
        if r["local_path"]:
            print(f"        {r['local_path']}")
    conn.close()
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    conn = db_open()
    r = conn.execute("SELECT * FROM videos WHERE id=?",
                     (args.id,)).fetchone()
    if not r:
        print(f"no video with id={args.id}")
        return 1
    for k in r.keys():
        v = r[k]
        if v is None:
            continue
        if k == "raw_json":
            continue
        print(f"  {k:<20} {v}")
    print("\nevents:")
    for e in conn.execute(
        "SELECT * FROM events WHERE video_id=? ORDER BY id", (args.id,)
    ):
        print(f"  {e['occurred_at']}  {e['event_type']:<18}  "
              f"{e['details'] or ''}")
    conn.close()
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="videsaur")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="bootstrap DB from catalog.json + videos/")
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("sync", help="refetch API, diff, download new videos")
    sp.add_argument("--concurrency", type=int, default=4)
    sp.set_defaults(func=cmd_sync)

    sp = sub.add_parser("verify", help="re-hash every file, detect corruption")
    sp.add_argument("--full", action="store_true",
                    help="re-verify even files already verified")
    sp.add_argument("--recheck", action="store_true")
    sp.set_defaults(func=cmd_verify)

    sp = sub.add_parser("dedupe", help="find SHA256-identical files")
    sp.add_argument("--limit", type=int, default=20)
    sp.set_defaults(func=cmd_dedupe)

    sp = sub.add_parser("stats", help="overview of catalog + downloads")
    sp.set_defaults(func=cmd_stats)

    sp = sub.add_parser("list", help="list videos with filters")
    sp.add_argument("--missing", action="store_true")
    sp.add_argument("--removed", action="store_true")
    sp.add_argument("--corrupt", action="store_true")
    sp.add_argument("--category", type=str)
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("search", help="substring search on name/description")
    sp.add_argument("query", type=str)
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_search)

    sp = sub.add_parser("show", help="dump one video + its event history")
    sp.add_argument("id", type=int)
    sp.set_defaults(func=cmd_show)

    return p


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
