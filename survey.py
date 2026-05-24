# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""
Pre-flight survey of every CloudFront video URL in catalog.json.

Sends a HEAD to each URL (concurrent, polite), records status + size,
prints live progress, then writes survey.json with everything.

Tells you:
  - exact total bytes (and GB)
  - status code distribution (any 403/404/429?)
  - any dead URLs to skip during real download
  - whether CloudFront is throttling us at HEAD speed

Run:  uv run survey.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

CATALOG = Path("catalog.json")
OUT = Path("survey.json")
CONCURRENCY = 8
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


async def head_one(client: httpx.AsyncClient, sem: asyncio.Semaphore,
                   v: dict, idx: int, total: int, results: list,
                   counts: Counter) -> None:
    async with sem:
        url = v.get("url")
        if not url:
            results.append({"id": v["id"], "status": "no-url", "size": 0})
            counts["no-url"] += 1
            print(f"  [{idx:>5}/{total}] id={v['id']:>5}  NO-URL", flush=True)
            return
        try:
            r = await client.head(url, timeout=20.0)
            size = int(r.headers.get("Content-Length", 0))
            results.append({"id": v["id"], "status": r.status_code,
                            "size": size, "url": url})
            counts[r.status_code] += 1
            name = (v.get("name") or "")[:40].replace("\n", " ")
            print(f"  [{idx:>5}/{total}] id={v['id']:>5}  "
                  f"{r.status_code}  {size/1024:>8.1f} KB  {name}",
                  flush=True)
        except httpx.HTTPError as e:
            results.append({"id": v["id"], "status": f"err:{type(e).__name__}",
                            "size": 0, "url": url})
            counts["error"] += 1
            print(f"  [{idx:>5}/{total}] id={v['id']:>5}  ERR  {e}", flush=True)


async def main() -> int:
    if not CATALOG.exists():
        print("catalog.json missing — run fetch_catalog.py first", file=sys.stderr)
        return 1

    videos = json.loads(CATALOG.read_text())
    print(f"Surveying {len(videos)} videos with concurrency={CONCURRENCY}\n")

    results: list[dict] = []
    counts: Counter = Counter()
    sem = asyncio.Semaphore(CONCURRENCY)
    start = time.time()

    async with httpx.AsyncClient(headers={"User-Agent": UA},
                                  http2=True, follow_redirects=True) as client:
        tasks = [
            head_one(client, sem, v, i, len(videos), results, counts)
            for i, v in enumerate(videos, 1)
        ]
        await asyncio.gather(*tasks)

    elapsed = time.time() - start
    OUT.write_text(json.dumps(results, indent=2))

    total_bytes = sum(r["size"] for r in results)
    ok = [r for r in results if r["status"] == 200]
    print("\n" + "=" * 60)
    print(f"Survey done in {elapsed:.1f}s")
    print(f"Status code distribution: {dict(counts)}")
    print(f"OK (200): {len(ok)} / {len(results)}")
    print(f"Total size: {total_bytes:,} bytes "
          f"= {total_bytes / 1024**2:.1f} MB "
          f"= {total_bytes / 1024**3:.2f} GB")
    if ok:
        avg = total_bytes / len(ok)
        print(f"Avg per video: {avg / 1024:.1f} KB")
        for tag, conc, gap in [("polite", 1, 2.5), ("normal", 4, 1.0), ("fast", 8, 0.5)]:
            est = (gap * len(ok) / conc) / 60
            print(f"  est download time @ concurrency={conc}, gap={gap}s : "
                  f"{est:.1f} min ({est/60:.1f} h)")
    bad = [r for r in results if r["status"] != 200]
    if bad:
        print(f"\nNon-200 entries ({len(bad)}):")
        for r in bad[:20]:
            print(f"  id={r['id']}  status={r['status']}")
        if len(bad) > 20:
            print(f"  ... and {len(bad) - 20} more (see survey.json)")
    print(f"\nWrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
