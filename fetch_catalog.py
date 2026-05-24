# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""
Fetch the full videsaur.com video catalog.

Walks /api/v1/video?page=N from 1..last_page, saves all video records to
catalog.json. Each per-page response is also cached in pages/page_N.json
so a crash mid-walk doesn't lose work.

Run:  uv run fetch_catalog.py
"""
from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path

import httpx

API = "https://videsaur.com/api/v1/video"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
}
PAGES_DIR = Path("pages")
CATALOG = Path("catalog.json")
PAGE_DELAY = (0.8, 1.6)        # polite gap between page fetches
MAX_RETRIES = 5


def fetch_page(client: httpx.Client, page: int) -> dict:
    cache = PAGES_DIR / f"page_{page:03d}.json"
    if cache.exists():
        return json.loads(cache.read_text())

    delay = 2.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.get(API, params={"page": page}, timeout=30.0)
            if r.status_code == 429 or r.status_code >= 500:
                wait = float(r.headers.get("Retry-After", delay))
                print(f"  page {page}: {r.status_code}, sleeping {wait:.1f}s")
                time.sleep(wait)
                delay *= 2
                continue
            r.raise_for_status()
            data = r.json()
            cache.write_text(json.dumps(data))
            return data
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            print(f"  page {page} attempt {attempt}: {e}")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Gave up on page {page}")


def main() -> int:
    if CATALOG.exists():
        existing = json.loads(CATALOG.read_text())
        print(f"catalog.json already exists with {len(existing)} videos — nothing to do.")
        print(f"  (delete catalog.json if you want to rebuild from cached pages/)")
        return 0

    PAGES_DIR.mkdir(exist_ok=True)
    cached = sorted(PAGES_DIR.glob("page_*.json"))
    if cached:
        print(f"resuming: {len(cached)} pages already cached in {PAGES_DIR}/")

    def show(seen: int, total_count: int, v: dict) -> None:
        name = (v.get("name") or "").replace("\n", " ")[:55]
        dur = v.get("duration") or "?"
        views = v.get("views", 0)
        url = v.get("url") or "<no-url>"
        print(f"  [{seen:>5}/{total_count}] id={v['id']:>5}  dur={dur:<5}  "
              f"views={views:<5}  {name}\n        {url}", flush=True)

    with httpx.Client(headers=HEADERS, http2=True, follow_redirects=True) as client:
        first = fetch_page(client, 1)
        last_page = first["last_page"]
        total = first["total"]
        per_page = first["per_page"]
        print(f"\nCatalog: {total} videos, {last_page} pages, {per_page}/page\n")

        all_videos: list[dict] = []
        print(f"--- page 1/{last_page} ---")
        for v in first["data"]:
            all_videos.append(v)
            show(len(all_videos), total, v)

        for page in range(2, last_page + 1):
            time.sleep(random.uniform(*PAGE_DELAY))
            print(f"\n--- page {page}/{last_page} ---")
            data = fetch_page(client, page)
            for v in data["data"]:
                all_videos.append(v)
                show(len(all_videos), total, v)

    # de-dup by id, just in case
    by_id = {v["id"]: v for v in all_videos}
    videos = list(by_id.values())
    CATALOG.write_text(json.dumps(videos, indent=2, ensure_ascii=False))
    print(f"\nWrote {CATALOG} with {len(videos)} unique videos "
          f"(API said {total}).")
    if len(videos) != total:
        print(f"  NOTE: count mismatch — API total={total}, got={len(videos)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
