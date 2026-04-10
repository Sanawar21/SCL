import json
from pathlib import Path

from flask import Blueprint, current_app, abort, jsonify, render_template

from app.session_files import RESERVED_PUBLIC_SLUGS, resolve_named_directory, resolve_session_file

viewer_bp = Blueprint("viewer", __name__)


def _published_session_dir() -> Path:
    return resolve_named_directory(current_app, "PUBLISHED_SESSION_DIR", "published_sessions")


def _published_sessions():
    published_dir = _published_session_dir()
    sessions = []
    for file_path in sorted(published_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        slug = file_path.stem
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue

        sessions.append(
            {
                "slug": slug,
                "name": payload.get("session_name") or slug,
                "published_at": payload.get("saved_at"),
                "url": f"/{slug}",
            }
        )
    return sessions


@viewer_bp.get("/")
def home():
    return render_template("viewer/home.html", published_sessions=_published_sessions())


@viewer_bp.get("/viewer/live")
def live_view():
    auction_service = current_app.extensions["auction_service"]
    return render_template("viewer/live.html", state=auction_service.get_state())


@viewer_bp.get("/<slug>")
def published_view(slug):
    slug = slug.lower()
    if "." in slug:
        abort(404)
    if slug in RESERVED_PUBLIC_SLUGS:
        abort(404)

    try:
        file_path = resolve_session_file(_published_session_dir(), f"{slug}.json")
    except ValueError:
        abort(404)
    if not file_path.exists():
        abort(404)

    payload = json.loads(file_path.read_text(encoding="utf-8"))
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        abort(404)

    auction_service = current_app.extensions["auction_service"]
    state = auction_service.build_state_from_tables(tables, bid_limit=None)
    state["published_session_name"] = payload.get("session_name") or slug
    state["published_slug"] = slug
    state["published_at"] = payload.get("saved_at")
    return render_template("viewer/published.html", state=state)


@viewer_bp.get("/api/state")
def api_state():
    return jsonify(current_app.extensions["auction_service"].get_state())
