# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""
Resumable, concurrent bulk downloader for videsaur.com videos.

Reads catalog.json, downloads each video to videos/<id>_<safe-name>.mp4.

Survey proved CloudFront is fine with concurrency=8 — default here is 4
(comfortable margin, ~30 min total) but you can change it.

Features:
  - configurable concurrency (--concurrency N)
  - resume from interrupted downloads via .part + HTTP Range
  - skip files that already exist with matching server size
  - exponential backoff on 429/5xx, honors Retry-After
  - light per-request jitter to avoid synchronized bursts
  - live progress with running MB total

Run:
  uv run download_videos.py                    # default concurrency=4
  uv run download_videos.py --concurrency 8    # faster
  uv run download_videos.py --concurrency 1    # slowest, most polite
  uv run download_videos.py --start 500 --limit 100
  uv run download_videos.py --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import sys
import time
from pathlib import Path

import httpx

CATALOG = Path("catalog.json")
OUT_DIR = Path("videos")
LOG = Path("download.log")

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

MAX_RETRIES = 5
CHUNK = 64 * 1024
JITTER = (0.1, 0.4)   # tiny per-task jitter; concurrency does the heavy lifting


def sanitize(name: str) -> str:
    name = re.sub(r"\s+", " ", name or "").strip()
    name = re.sub(r"[\\/*?:\"<>|\x00-\x1f]", "_", name)
    return name[:80] or "video"


_log_lock = asyncio.Lock()


async def log(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    async with _log_lock:
        print(line, flush=True)
        with LOG.open("a") as f:
            f.write(line + "\n")


class Progress:
    def __init__(self, total: int) -> None:
        self.total = total
        self.done = 0
        self.ok = 0
        self.skip = 0
        self.fail = 0
        self.bytes = 0
        self.start = time.time()
        self.lock = asyncio.Lock()

    async def tick(self, status: str, size: int = 0) -> None:
        async with self.lock:
            self.done += 1
            if status == "ok":
                self.ok += 1
                self.bytes += size
            elif status == "skip":
                self.skip += 1
                self.bytes += size
            else:
                self.fail += 1


async def download_one(client: httpx.AsyncClient, sem: asyncio.Semaphore,
                       v: dict, prog: Progress) -> None:
    async with sem:
        await asyncio.sleep(random.uniform(*JITTER))
        vid = v["id"]
        url = v.get("url")
        name = sanitize(v.get("name") or f"video_{vid}")
        final = OUT_DIR / f"{vid:06d}_{name}.mp4"
        part = final.with_suffix(final.suffix + ".part")

        if not url:
            await prog.tick("fail")
            await log(f"id={vid:>5}  FAIL  no-url")
            return

        # skip if already complete
        if final.exists():
            try:
                head = await client.head(url, timeout=15.0)
                remote = int(head.headers.get("Content-Length", -1))
                if head.status_code == 200 and remote == final.stat().st_size:
                    await prog.tick("skip", remote)
                    await log(f"id={vid:>5}  skip  {remote/1024:>8.1f} KB  "
                              f"[{prog.done}/{prog.total}]")
                    return
                await log(f"id={vid:>5}  size mismatch, redownloading")
                final.unlink()
            except httpx.HTTPError:
                await prog.tick("skip", final.stat().st_size)
                return

        delay = 2.0
        for attempt in range(1, MAX_RETRIES + 1):
            headers = {}
            mode = "wb"
            if part.exists():
                headers["Range"] = f"bytes={part.stat().st_size}-"
                mode = "ab"
            try:
                async with client.stream("GET", url, headers=headers,
                                          timeout=60.0) as r:
                    if r.status_code == 416:
                        part.rename(final)
                        await prog.tick("ok", final.stat().st_size)
                        await log(f"id={vid:>5}  ok-resumed-complete")
                        return
                    if r.status_code == 429 or r.status_code >= 500:
                        wait = float(r.headers.get("Retry-After", delay))
                        await log(f"id={vid:>5}  HTTP {r.status_code} "
                                  f"sleeping {wait:.1f}s")
                        await asyncio.sleep(wait)
                        delay *= 2
                        continue
                    r.raise_for_status()
                    with part.open(mode) as f:
                        async for chunk in r.aiter_bytes(CHUNK):
                            f.write(chunk)

                part.rename(final)
                size = final.stat().st_size
                await prog.tick("ok", size)
                rate = prog.bytes / max(time.time() - prog.start, 0.1)
                await log(f"id={vid:>5}  ok    {size/1024:>8.1f} KB  "
                          f"[{prog.done}/{prog.total}]  "
                          f"total={prog.bytes/1024**2:.1f} MB  "
                          f"{rate/1024**2:.2f} MB/s  | "
                          f"{name[:45]}")
                return
            except httpx.HTTPError as e:
                await log(f"id={vid:>5}  attempt {attempt} error: {e}")
                await asyncio.sleep(delay)
                delay *= 2

        await prog.tick("fail")
        await log(f"id={vid:>5}  FAIL after {MAX_RETRIES} attempts")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not CATALOG.exists():
        print("catalog.json missing — run fetch_catalog.py first", file=sys.stderr)
        return 1
    OUT_DIR.mkdir(exist_ok=True)

    videos = json.loads(CATALOG.read_text())
    videos.sort(key=lambda v: v["id"])
    work = videos[args.start: (args.start + args.limit) if args.limit else None]

    print(f"plan: {len(work)} videos, concurrency={args.concurrency}, "
          f"out={OUT_DIR}/")
    if args.dry_run:
        for v in work[:5]:
            print(f"  would fetch {v['id']}  {(v.get('name') or '')[:50]}  {v.get('url')}")
        print("  ...")
        return 0

    prog = Progress(len(work))
    sem = asyncio.Semaphore(args.concurrency)
    limits = httpx.Limits(max_connections=args.concurrency * 2,
                          max_keepalive_connections=args.concurrency * 2)
    async with httpx.AsyncClient(headers={"User-Agent": UA},
                                  http2=True, follow_redirects=True,
                                  limits=limits) as client:
        await asyncio.gather(*[
            download_one(client, sem, v, prog) for v in work
        ])

    elapsed = time.time() - prog.start
    print(f"\nDONE  ok={prog.ok}  skip={prog.skip}  fail={prog.fail}  "
          f"in {elapsed/60:.1f} min  "
          f"total={prog.bytes/1024**3:.2f} GB  "
          f"avg={prog.bytes/max(elapsed,1)/1024**2:.2f} MB/s")
    return 0 if prog.fail == 0 else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
