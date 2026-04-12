from flask import Flask
from flask_socketio import SocketIO
from pathlib import Path
import os
import shutil
import json
from datetime import datetime
from tinydb import TinyDB

from app.config import Config
from app.db import LockedTinyDB, SeasonStoreManager
from app.services.auth_service import AuthService
from app.services.auction_service import AuctionService
from app.services.fantasy_service import FantasyService
from app.services.scorer_service import ScorerService

socketio = SocketIO(async_mode="threading")


def _resolve_path(app: Flask, configured_path: str) -> Path:
    path_obj = Path(configured_path)
    if path_obj.is_absolute():
        return path_obj
    return Path(app.root_path).parent / path_obj


def _migrate_legacy_published_sessions(app: Flask):
    published_dir = _resolve_path(app, app.config["PUBLISHED_SESSION_DIR"])
    if not published_dir.exists():
        return

    season_store_manager = app.extensions["season_store_manager"]

    for file_path in published_dir.glob("*.json"):
        slug = file_path.stem.lower()
        if season_store_manager.has_season(slug):
            continue

        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue

        tables = payload.get("tables")
        if not isinstance(tables, dict):
            continue

        season_tables = {
            table_name: rows
            for table_name, rows in tables.items()
            if table_name != "bids"
        }

        season_store = season_store_manager.get_store(slug, create=True)
        season_store.import_tables(season_tables)

        with season_store.write() as db:
            meta_table = db.table("season_meta")
            season_meta = {
                "slug": slug,
                "name": payload.get("session_name") or slug,
                "published": bool(payload.get("published", True)),
                "published_file": file_path.name,
                "published_at": payload.get("saved_at") or datetime.utcnow().isoformat(),
                "created_at": datetime.utcnow().isoformat(),
                "submissions_open": False,
            }
            if meta_table.get(doc_id=1):
                meta_table.update(season_meta, doc_ids=[1])
            else:
                meta_table.insert(season_meta)


def _migrate_legacy_session_snapshots(app: Flask):
    snapshots_dir = _resolve_path(app, app.config["SNAPSHOT_DIR"])
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    # Migrate legacy snapshots from sessions/*.json into snapshot folder.
    sessions_dir = _resolve_path(app, app.config["SESSION_DIR"])
    if sessions_dir.exists():
        for file_path in sessions_dir.glob("*.json"):
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue

            tables = payload.get("tables")
            if not isinstance(tables, dict):
                continue

            slug = file_path.stem.lower()
            target_path = snapshots_dir / f"{slug}.json"
            if target_path.exists():
                continue

            snapshot_payload = {
                "slug": slug,
                "file": target_path.name,
                "session_name": payload.get("session_name") or slug,
                "saved_at": payload.get("saved_at") or datetime.utcnow().isoformat(),
                "tables": tables,
                "legacy_source": str(file_path),
            }
            target_path.write_text(json.dumps(snapshot_payload, indent=2), encoding="utf-8")

    # Migrate legacy snapshot DB rows (single-file model) into snapshot folder.
    legacy_snapshot_db_path = _resolve_path(app, app.config["LEGACY_SNAPSHOT_DB_PATH"])
    if legacy_snapshot_db_path.exists():
        try:
            legacy_db = TinyDB(str(legacy_snapshot_db_path))
            rows = legacy_db.table("auction_snapshots").all()
            legacy_db.close()
        except Exception:  # noqa: BLE001
            rows = []

        for row in rows:
            slug = (row.get("slug") or "").strip().lower()
            if not slug:
                continue
            target_path = snapshots_dir / f"{slug}.json"
            if target_path.exists():
                continue
            target_path.write_text(json.dumps(row, indent=2), encoding="utf-8")


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    legacy_db_path = _resolve_path(app, app.config["DB_PATH"])
    configured_auction_path = _resolve_path(app, app.config["AUCTION_DB_PATH"])
    configured_auth_path = _resolve_path(app, app.config["AUTH_DB_PATH"])

    auction_db_env_override = os.environ.get("SCL_AUCTION_DB_PATH")
    if not configured_auction_path.exists() and legacy_db_path.exists() and not auction_db_env_override:
        configured_auction_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(str(legacy_db_path), str(configured_auction_path))

    auction_db_path = str(configured_auction_path)

    auth_store = LockedTinyDB(str(configured_auth_path))
    auction_store = LockedTinyDB(auction_db_path)
    season_store_manager = SeasonStoreManager(app.config["SEASON_DB_DIR"], app.root_path)
    auth_service = AuthService(auth_store)
    auction_service = AuctionService(auction_store)
    fantasy_service = FantasyService(auction_store, app.config["PUBLISHED_SESSION_DIR"], season_store_manager)
    scorer_service = ScorerService(
        season_store_manager,
        auction_service,
        app.root_path,
        app.config["SCORER_CONFIG_PATH"],
    )

    auth_service.seed_admin_if_missing()
    auction_service.bootstrap_defaults()

    app.extensions["auth_store"] = auth_store
    app.extensions["auction_store"] = auction_store
    app.extensions["season_store_manager"] = season_store_manager
    app.extensions["auth_service"] = auth_service
    app.extensions["auction_service"] = auction_service
    app.extensions["fantasy_service"] = fantasy_service
    app.extensions["scorer_service"] = scorer_service

    _migrate_legacy_published_sessions(app)
    _migrate_legacy_session_snapshots(app)

    from app.routes.admin import admin_bp, unified_admin_bp
    from app.routes.fantasy import fantasy_bp
    from app.routes.landing import landing_bp
    from app.routes.manager import manager_bp
    from app.routes.viewer import viewer_bp

    app.register_blueprint(landing_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(unified_admin_bp)
    app.register_blueprint(fantasy_bp)
    app.register_blueprint(manager_bp)
    app.register_blueprint(viewer_bp)

    socketio.init_app(app)

    return app
