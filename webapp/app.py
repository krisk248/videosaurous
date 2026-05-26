"""
videosaurous web UI — a meme soundboard backed by videsaur.db.

Endpoints:
  GET /              full page (header + sidebar + grid)
  GET /grid          just the grid HTML — returned to HTMX for live updates
  GET /video/{id}    just the player modal HTML — returned to HTMX on tile click
  GET /media/{id}    streams the actual .mp4 (with Range support for seeking)
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Resolve paths absolutely so the app works both inside Docker and via
# `uvicorn` on the host machine.
WEBAPP_DIR = Path(__file__).resolve().parent
APP_ROOT = WEBAPP_DIR.parent
DB_PATH = APP_ROOT / "videsaur.db"
VIDEOS_DIR = APP_ROOT / "videos"

PAGE_SIZE_DEFAULT = 48   # tiles per page (user can override via ?per_page=)
PAGE_SIZE_MAX = 10000    # safety cap — "all" maps to this

app = FastAPI(title="videosaurous")
templates = Jinja2Templates(directory=str(WEBAPP_DIR / "templates"))
app.mount("/static",
          StaticFiles(directory=str(WEBAPP_DIR / "static")),
          name="static")


def db() -> sqlite3.Connection:
    """Open a read-only-friendly connection. Row factory gives dict-like rows."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def build_where(q, category, language, prefix=""):
    """Builds the shared WHERE-clause + params used in many queries.
    `prefix` lets the same logic work for joined queries (e.g. 'v.')."""
    p = prefix
    clauses = [f"{p}removed_at IS NULL", f"{p}local_path IS NOT NULL"]
    params: list = []
    if q:
        clauses.append(f"({p}name LIKE ? OR {p}description LIKE ? OR {p}keywords LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like, like])
    if category:
        clauses.append(f"{p}categories_json LIKE ?")
        params.append(f'%"{category}"%')
    if language:
        clauses.append(f"{p}language_json LIKE ?")
        params.append(f'%"{language}"%')
    return " AND ".join(clauses), params


def fetch_videos(
    q: Optional[str],
    category: Optional[str],
    language: Optional[str],
    page: int,
    per_page: int,
) -> tuple[list[dict], int]:
    """Returns (videos_for_this_page, total_matching_count).
    Joins against a duplicate-hash subquery so each row knows if its SHA256
    is shared with other videos (dup_count > 1)."""
    where_sql, params = build_where(q, category, language, prefix="v.")

    with db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM videos v WHERE {where_sql}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""SELECT v.id, v.name, v.local_path, v.thumb_url, v.gif_url,
                       v.views, v.categories_json, v.language_json,
                       COALESCE(d.cnt, 1) AS dup_count
                FROM videos v
                LEFT JOIN (
                    SELECT sha256, COUNT(*) AS cnt FROM videos
                    WHERE sha256 IS NOT NULL
                    GROUP BY sha256
                    HAVING COUNT(*) > 1
                ) d ON v.sha256 = d.sha256
                WHERE {where_sql}
                ORDER BY v.id DESC
                LIMIT ? OFFSET ?""",
            params + [per_page, page * per_page],
        ).fetchall()

    videos = []
    for r in rows:
        d = dict(r)
        d["categories"] = [c["name"]
                           for c in json.loads(d.pop("categories_json") or "[]")]
        d["languages"] = [l["language"]
                          for l in json.loads(d.pop("language_json") or "[]")]
        videos.append(d)
    return videos, total


def clamp_per_page(per_page: Optional[int]) -> int:
    """Validate the per_page query param — default if missing, cap at max."""
    if not per_page or per_page < 1:
        return PAGE_SIZE_DEFAULT
    return min(per_page, PAGE_SIZE_MAX)


def aggregate_facets() -> tuple[list, list]:
    """Count videos per category and per language. ~50ms for 7,631 rows."""
    cats: dict[str, int] = {}
    langs: dict[str, int] = {}
    with db() as conn:
        for r in conn.execute(
            "SELECT categories_json, language_json FROM videos "
            "WHERE removed_at IS NULL"
        ):
            for c in json.loads(r["categories_json"] or "[]"):
                cats[c["name"]] = cats.get(c["name"], 0) + 1
            for l in json.loads(r["language_json"] or "[]"):
                langs[l["language"]] = langs.get(l["language"], 0) + 1
    return (
        sorted(cats.items(), key=lambda x: -x[1]),
        sorted(langs.items(), key=lambda x: -x[1]),
    )


@app.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    q: Optional[str] = None,
    category: Optional[str] = None,
    language: Optional[str] = None,
    page: int = 0,
    per_page: Optional[int] = None,
):
    per_page = clamp_per_page(per_page)
    videos, total = fetch_videos(q, category, language, page, per_page)
    categories, languages = aggregate_facets()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "videos": videos,
        "total": total,
        "page": page,
        "per_page": per_page,
        "q": q or "",
        "category": category or "",
        "language": language or "",
        "categories": categories,
        "languages": languages,
    })


@app.get("/grid", response_class=HTMLResponse)
def grid(
    request: Request,
    q: Optional[str] = None,
    category: Optional[str] = None,
    language: Optional[str] = None,
    page: int = 0,
    per_page: Optional[int] = None,
):
    per_page = clamp_per_page(per_page)
    videos, total = fetch_videos(q, category, language, page, per_page)
    return templates.TemplateResponse("_grid.html", {
        "request": request,
        "videos": videos,
        "total": total,
        "page": page,
        "per_page": per_page,
        "q": q or "",
        "category": category or "",
        "language": language or "",
    })


@app.get("/video/{video_id}", response_class=HTMLResponse)
def video_modal(
    request: Request,
    video_id: int,
    q: Optional[str] = None,
    category: Optional[str] = None,
    language: Optional[str] = None,
):
    """Renders the modal player. Computes prev_id / next_id within the
    current filter set so arrow keys / buttons can walk to neighbours."""
    where_sql, params = build_where(q, category, language, prefix="")

    with db() as conn:
        row = conn.execute(
            "SELECT id, name, description, local_path, views, sha256, "
            "       categories_json, language_json "
            "FROM videos WHERE id = ?",
            (video_id,),
        ).fetchone()
        if not row:
            return HTMLResponse("not found", status_code=404)

        # We sort the grid by id DESC, so "next" visually = lower id,
        # "prev" visually = higher id.
        next_row = conn.execute(
            f"SELECT id FROM videos WHERE {where_sql} AND id < ? "
            f"ORDER BY id DESC LIMIT 1",
            params + [video_id],
        ).fetchone()
        prev_row = conn.execute(
            f"SELECT id FROM videos WHERE {where_sql} AND id > ? "
            f"ORDER BY id ASC LIMIT 1",
            params + [video_id],
        ).fetchone()

        # Dup count for the badge inside the modal
        dup_count = conn.execute(
            "SELECT COUNT(*) FROM videos WHERE sha256 = ? AND sha256 IS NOT NULL",
            (row["sha256"],),
        ).fetchone()[0] if row["sha256"] else 1

    v = dict(row)
    v["categories"] = [c["name"]
                       for c in json.loads(v.pop("categories_json") or "[]")]
    v["languages"] = [l["language"]
                      for l in json.loads(v.pop("language_json") or "[]")]
    v["dup_count"] = dup_count

    return templates.TemplateResponse("_player.html", {
        "request": request,
        "video": v,
        "prev_id": prev_row["id"] if prev_row else None,
        "next_id": next_row["id"] if next_row else None,
        "q": q or "",
        "category": category or "",
        "language": language or "",
    })


@app.get("/media/{video_id}")
def media(video_id: int):
    """Stream the actual mp4 file. Starlette's FileResponse handles HTTP Range
    requests automatically, so the browser can seek inside the video."""
    with db() as conn:
        row = conn.execute(
            "SELECT local_path FROM videos WHERE id = ?", (video_id,)
        ).fetchone()
    if not row or not row["local_path"]:
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(str(APP_ROOT / row["local_path"]), media_type="video/mp4")


@app.get("/healthz")
def health():
    """Trivial endpoint to confirm Docker is up."""
    return {"ok": True, "db": DB_PATH.exists(), "videos_dir": VIDEOS_DIR.exists()}
