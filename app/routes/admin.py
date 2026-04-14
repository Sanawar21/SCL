import json
import secrets
import sqlite3
import re
from datetime import datetime
from pathlib import Path

from flask import Blueprint, current_app, flash, jsonify, redirect, render_template, request, session, url_for

from app import socketio
from app.authz import login_required
from app.db import Query
from app.session_files import RESERVED_PUBLIC_SLUGS, resolve_named_directory, resolve_session_file, slugify_session_name
from app.rules import (
    PHASE_A_BREAK,
    PHASE_A_P,
    PHASE_A_SG,
    PHASE_B,
    PHASE_COMPLETE,
    PHASE_SETUP,
    ROLE_ADMIN,
    ROLE_MANAGER,
    TIER_BASE_PRICE,
    TIER_CREDIT_COST,
)

admin_bp = Blueprint("admin", __name__, url_prefix="/auction/admin")
unified_admin_bp = Blueprint("unified_admin", __name__)

SPECIALITIES = {"ALL_ROUNDER", "BATTER", "BOWLER"}


def _safe_json_loads(raw_value):
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except Exception:  # noqa: BLE001
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _safe_json_dumps(payload):
    return json.dumps(payload or {}, ensure_ascii=True)


def _resolve_db_path(config_value: str) -> Path:
    configured = Path(config_value)
    if configured.is_absolute():
        return configured
    return (Path(current_app.root_path).parent / configured).resolve()


def _connect_sqlite():
    db_path = _resolve_db_path(current_app.config["AUCTION_DB_PATH"])
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _default_admin_setup_state():
    return {
        "sellable_player_ids": [],
        "teams": [],
    }


def _normalize_admin_setup_state(raw_state):
    state = dict(raw_state) if isinstance(raw_state, dict) else {}

    sellable_raw = state.get("sellable_player_ids")
    if not isinstance(sellable_raw, list):
        sellable_raw = []

    sellable_ids = []
    seen_sellable = set()
    for player_id in sellable_raw:
        safe_player_id = str(player_id or "").strip()
        if not safe_player_id or safe_player_id in seen_sellable:
            continue
        seen_sellable.add(safe_player_id)
        sellable_ids.append(safe_player_id)

    teams_raw = state.get("teams")
    if not isinstance(teams_raw, list):
        teams_raw = []

    teams = []
    seen_team_ids = set()
    for team in teams_raw:
        if not isinstance(team, dict):
            continue
        team_id = str(team.get("id") or "").strip()
        team_name = str(team.get("name") or "").strip()
        if not team_id or not team_name or team_id in seen_team_ids:
            continue
        seen_team_ids.add(team_id)
        teams.append(
            {
                "id": team_id,
                "name": team_name,
                "selected": bool(team.get("selected", True)),
                "manager_player_id": str(team.get("manager_player_id") or "").strip() or None,
            }
        )

    return {
        "sellable_player_ids": sellable_ids,
        "teams": teams,
    }


def _extract_admin_setup_state(metadata_json):
    metadata = _safe_json_loads(metadata_json)
    raw_state = metadata.get("admin_setup")
    if not isinstance(raw_state, dict):
        return _default_admin_setup_state()
    return _normalize_admin_setup_state(raw_state)


def _save_admin_setup_state(conn: sqlite3.Connection, auction_id: str, setup_state: dict):
    row = conn.execute(
        "SELECT metadata_json FROM auctions WHERE id = ?",
        (auction_id,),
    ).fetchone()
    if not row:
        raise ValueError("Setup auction not found")

    metadata = _safe_json_loads(row["metadata_json"])
    metadata["admin_setup"] = _normalize_admin_setup_state(setup_state)
    conn.execute(
        "UPDATE auctions SET metadata_json = ?, updated_at = ? WHERE id = ?",
        (_safe_json_dumps(metadata), datetime.utcnow().isoformat(), auction_id),
    )


def _slugify_lifecycle_value(raw_value: str, fallback: str):
    slug = re.sub(r"[^a-z0-9]+", "-", (raw_value or "").strip().lower()).strip("-")
    return slug or fallback


def _slugify_username(value: str):
    base = re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())
    return base or "manager"


def _ensure_unique_manager_username(base: str, auth_service, used_usernames: set[str]):
    safe_base = _slugify_username(base)
    candidate = safe_base
    suffix = 1
    while candidate in used_usernames:
        suffix += 1
        candidate = f"{safe_base}{suffix}"
    used_usernames.add(candidate)
    return candidate


def _select_players_by_ids(conn: sqlite3.Connection, player_ids: list[str]):
    safe_ids = [str(player_id or "").strip() for player_id in player_ids if str(player_id or "").strip()]
    if not safe_ids:
        return {}

    placeholders = ",".join(["?" for _ in safe_ids])
    rows = conn.execute(
        f"""
        SELECT id, display_name, tier, speciality
        FROM players
        WHERE id IN ({placeholders})
        """,
        safe_ids,
    ).fetchall()
    return {row["id"]: row for row in rows}


def _next_season_slug(conn: sqlite3.Connection):
    rows = conn.execute("SELECT slug FROM seasons").fetchall()
    max_num = 0
    for row in rows:
        slug = (row["slug"] or "").strip().lower()
        match = re.fullmatch(r"season-(\d+)", slug)
        if match:
            max_num = max(max_num, int(match.group(1)))
    return f"season-{max_num + 1}"


def _find_lifecycle_auction(conn: sqlite3.Connection, season_slug: str):
    safe_slug = (season_slug or "").strip().lower()
    if not safe_slug:
        return None

    return conn.execute(
        """
        SELECT s.id AS season_id,
               s.slug AS season_slug,
               s.name AS season_name,
               a.id AS auction_id,
               a.status,
               a.metadata_json,
               a.updated_at
        FROM auctions a
        JOIN seasons s ON s.id = a.season_id
        WHERE s.slug = ?
          AND a.source_path = 'admin-lifecycle'
        ORDER BY COALESCE(a.updated_at, '') DESC, a.id DESC
        LIMIT 1
        """,
        (safe_slug,),
    ).fetchone()


def _list_lifecycle_seasons(conn: sqlite3.Connection):
    rows = conn.execute(
        """
        SELECT s.slug,
               s.name,
               s.created_at,
               a.id AS auction_id,
               a.status,
               a.metadata_json,
               a.updated_at,
               (
                 SELECT COUNT(*)
                 FROM auction_snapshots snap
                 WHERE snap.auction_id = a.id
                   AND snap.snapshot_type = 'admin-setup'
               ) AS snapshot_count
        FROM auctions a
        JOIN seasons s ON s.id = a.season_id
        WHERE a.source_path = 'admin-lifecycle'
        ORDER BY COALESCE(s.created_at, '') DESC, s.slug DESC
        """
    ).fetchall()

    seasons = []
    for row in rows:
        setup = _extract_admin_setup_state(row["metadata_json"])
        selected_teams = [team for team in setup["teams"] if team.get("selected")]
        manager_assigned = [team for team in selected_teams if team.get("manager_player_id")]
        seasons.append(
            {
                "slug": row["slug"],
                "name": row["name"],
                "auction_id": row["auction_id"],
                "status": row["status"],
                "updated_at": row["updated_at"],
                "snapshot_count": int(row["snapshot_count"] or 0),
                "sellable_count": len(setup["sellable_player_ids"]),
                "team_count": len(selected_teams),
                "manager_assigned_count": len(manager_assigned),
            }
        )
    return seasons


def _list_master_players(conn: sqlite3.Connection):
    return [
        {
            "id": row["id"],
            "name": row["display_name"] or row["id"],
            "tier": (row["tier"] or "").strip().lower(),
            "speciality": (row["speciality"] or "ALL_ROUNDER").strip() or "ALL_ROUNDER",
        }
        for row in conn.execute(
            """
            SELECT id, display_name, tier, speciality
            FROM players
            ORDER BY LOWER(COALESCE(display_name, id)) ASC
            """
        ).fetchall()
    ]


def _list_database_teams(conn: sqlite3.Connection):
    return [
        {
            "id": row["id"],
            "name": row["name"] or row["id"],
        }
        for row in conn.execute(
            """
            SELECT id, name
            FROM teams
            ORDER BY LOWER(COALESCE(name, id)) ASC
            """
        ).fetchall()
    ]


def _build_lifecycle_context(selected_setup_slug: str):
    safe_selected_slug = (selected_setup_slug or "").strip().lower()
    with _connect_sqlite() as conn:
        seasons = _list_lifecycle_seasons(conn)
        if not safe_selected_slug and seasons:
            safe_selected_slug = seasons[0]["slug"]

        master_players = _list_master_players(conn)
        database_teams = _list_database_teams(conn)

        selected = None
        snapshots = []
        if safe_selected_slug:
            auction_row = _find_lifecycle_auction(conn, safe_selected_slug)
            if auction_row:
                setup = _extract_admin_setup_state(auction_row["metadata_json"])
                sellable_set = set(setup["sellable_player_ids"])
                manager_candidates = [
                    player
                    for player in master_players
                    if player["id"] not in sellable_set
                ]

                snapshots = [
                    {
                        "id": row["id"],
                        "name": row["snapshot_name"],
                        "created_at": row["created_at"],
                        "source_path": row["source_path"],
                    }
                    for row in conn.execute(
                        """
                        SELECT id, snapshot_name, created_at, source_path
                        FROM auction_snapshots
                        WHERE auction_id = ?
                          AND snapshot_type = 'admin-setup'
                        ORDER BY COALESCE(created_at, '') DESC, id DESC
                        """,
                        (auction_row["auction_id"],),
                    ).fetchall()
                ]

                selected = {
                    "slug": auction_row["season_slug"],
                    "name": auction_row["season_name"],
                    "auction_id": auction_row["auction_id"],
                    "status": auction_row["status"],
                    "sellable_player_ids": setup["sellable_player_ids"],
                    "teams": setup["teams"],
                    "setup_team_ids": [team.get("id") for team in setup["teams"] if team.get("id")],
                    "manager_candidates": manager_candidates,
                }

    return {
        "seasons": seasons,
        "selected_slug": safe_selected_slug,
        "selected": selected,
        "master_players": master_players,
        "database_teams": database_teams,
        "snapshots": snapshots,
    }


def _redirect_lifecycle(selected_slug: str = ""):
    safe_slug = (selected_slug or "").strip().lower()
    if safe_slug:
        return redirect(url_for("unified_admin.dashboard", tab="lifecycle", setup_season=safe_slug))
    return redirect(url_for("unified_admin.dashboard", tab="lifecycle"))


def _build_unified_admin_context():
    auction_service = current_app.extensions["auction_service"]
    fantasy_service = current_app.extensions["fantasy_service"]
    scorer_service = current_app.extensions["scorer_service"]

    seasons = fantasy_service.list_fantasy_seasons()
    published_sessions = fantasy_service.list_published_sessions()
    scorer_config = scorer_service.load_config()
    season_slug = (request.args.get("season") or "").strip().lower()
    setup_season_slug = (request.args.get("setup_season") or "").strip().lower()
    active_tab = (request.args.get("tab") or "auction").strip().lower()
    if active_tab not in {"auction", "fantasy", "scorer", "lifecycle"}:
        active_tab = "auction"

    selected = None
    entries = []
    if season_slug:
        selected = fantasy_service.get_season(season_slug)
        if selected:
            entries = fantasy_service.get_entries_for_season(season_slug)

    return {
        "state": auction_service.get_state(),
        "active_tab": active_tab,
        "seasons": seasons,
        "published_sessions": published_sessions,
        "selected_season": selected,
        "entries": entries,
        "scorer_config": scorer_config,
        "scorer_available_seasons": scorer_service.list_seasons(),
        "scorer_download_filename": scorer_service.download_filename(scorer_config),
        "scorer_download_url": url_for("landing.scorer_download"),
        "lifecycle": _build_lifecycle_context(setup_season_slug),
    }


def _ensure_setup_phase():
    state = current_app.extensions["auction_service"].get_state()
    if state.get("phase") != PHASE_SETUP:
        raise ValueError("This action is only allowed during setup phase")


def _normalize_speciality(raw_value: str) -> str:
    value = (raw_value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if value not in SPECIALITIES:
        raise ValueError("Speciality must be one of: ALL_ROUNDER, BATTER, BOWLER")
    return value


def _published_session_dir() -> Path:
    return resolve_named_directory(current_app, "PUBLISHED_SESSION_DIR", "published_sessions")


def _snapshot_dir() -> Path:
    return resolve_named_directory(current_app, "SNAPSHOT_DIR", "data/auction_snapshots")


def _slugify_session_name(name: str) -> str:
    return slugify_session_name(name)


def _resolve_published_file(filename: str) -> Path:
    return resolve_session_file(_published_session_dir(), filename)


def _resolve_snapshot_file(filename: str) -> Path:
    return resolve_session_file(_snapshot_dir(), filename)


@admin_bp.get("/login")
def admin_login_page():
    return redirect(url_for("unified_admin.admin_login_page"))


@admin_bp.post("/login")
def admin_login():
    auth_service = current_app.extensions["auth_service"]
    user = auth_service.login(request.form.get("username", ""), request.form.get("password", ""))
    if not user or user["role"] != ROLE_ADMIN:
        flash("Invalid admin credentials", "error")
        return redirect(url_for("unified_admin.admin_login_page"))
    session["user"] = user
    return redirect(url_for("unified_admin.dashboard", tab="auction"))


@admin_bp.get("/logout")
def admin_logout():
    return redirect(url_for("unified_admin.admin_logout"))


@admin_bp.get("/dashboard")
@login_required(role=ROLE_ADMIN)
def dashboard():
    return redirect(url_for("unified_admin.dashboard", tab="auction"))


@unified_admin_bp.get("/admin/login", endpoint="admin_login_page")
def admin_login_page_unified():
    return render_template("admin/unified_login.html")


@unified_admin_bp.post("/admin/login", endpoint="admin_login")
def admin_login_unified():
    auth_service = current_app.extensions["auth_service"]
    user = auth_service.login(request.form.get("username", ""), request.form.get("password", ""))
    if not user or user["role"] != ROLE_ADMIN:
        flash("Invalid admin credentials", "error")
        return redirect(url_for("unified_admin.admin_login_page"))
    session["user"] = user
    return redirect(url_for("unified_admin.dashboard", tab="auction"))


@unified_admin_bp.get("/admin/logout", endpoint="admin_logout")
def admin_logout_unified():
    session.clear()
    return redirect(url_for("viewer.live_view"))


@unified_admin_bp.get("/admin", endpoint="dashboard")
def dashboard_unified():
    user = session.get("user")
    if not user:
        return redirect(url_for("unified_admin.admin_login_page"))
    if user.get("role") != ROLE_ADMIN:
        return redirect(url_for("viewer.home"))
    return render_template("admin/unified_dashboard.html", **_build_unified_admin_context())


def _require_unified_admin_user():
    user = session.get("user")
    if not user:
        return redirect(url_for("unified_admin.admin_login_page"))
    if user.get("role") != ROLE_ADMIN:
        return redirect(url_for("viewer.home"))
    return None


@unified_admin_bp.post("/admin/scorer", endpoint="scorer_save")
def scorer_save():
    user = session.get("user")
    if not user:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if user.get("role") != ROLE_ADMIN:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    scorer_service = current_app.extensions["scorer_service"]
    try:
        config = scorer_service.save_config(
            {
                "title": request.form.get("title", "").strip(),
                "version": request.form.get("version", "").strip(),
                "season_slug": request.form.get("season_slug", "").strip().lower(),
                "max_overs": request.form.get("max_overs", "").strip(),
            }
        )
        return jsonify(
            {
                "ok": True,
                "config": config,
                "download_filename": scorer_service.download_filename(config),
                "download_url": url_for("landing.scorer_download"),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@unified_admin_bp.post("/admin/lifecycle/create-season", endpoint="lifecycle_create_season")
def lifecycle_create_season():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_name = (request.form.get("season_name") or "").strip()

    try:
        with _connect_sqlite() as conn:
            existing = conn.execute(
                """
                SELECT s.slug
                FROM auctions a
                JOIN seasons s ON s.id = a.season_id
                WHERE a.source_path = 'admin-lifecycle'
                ORDER BY COALESCE(a.updated_at, '') DESC, a.id DESC
                LIMIT 1
                """
            ).fetchone()
            if existing:
                flash("Lifecycle setup already exists. Reusing current setup season.", "success")
                return _redirect_lifecycle(existing["slug"])

            slug = "season-1"
            season_id = "season-1"
            display_name = season_name or "Season 1"
            auction_id = "auction::season-1::admin-setup"
            now = datetime.utcnow().isoformat()

            season_row = conn.execute(
                "SELECT id FROM seasons WHERE id = ? OR slug = ? LIMIT 1",
                (season_id, slug),
            ).fetchone()
            if not season_row:
                conn.execute(
                    """
                    INSERT INTO seasons (id, slug, name, created_at, published_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        season_id,
                        slug,
                        display_name,
                        now,
                        None,
                        _safe_json_dumps({"created_by": "admin-lifecycle"}),
                    ),
                )
            elif season_name:
                conn.execute(
                    "UPDATE seasons SET name = ? WHERE id = ?",
                    (display_name, season_id),
                )

            conn.execute(
                """
                INSERT INTO auctions
                (id, season_id, name, mode, source_path, status, phase, current_player_id, started_at, ended_at, created_at, updated_at, saved_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    auction_id,
                    season_id,
                    f"{display_name} Setup",
                    "live",
                    "admin-lifecycle",
                    "draft",
                    PHASE_SETUP,
                    None,
                    None,
                    None,
                    now,
                    now,
                    None,
                    _safe_json_dumps({"admin_setup": _default_admin_setup_state()}),
                ),
            )

            conn.commit()

        flash(f"Created lifecycle setup for {display_name}", "success")
        return _redirect_lifecycle(slug)
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")
        return _redirect_lifecycle()


@unified_admin_bp.post("/admin/lifecycle/add-player", endpoint="lifecycle_add_player")
def lifecycle_add_player():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_slug = (request.form.get("season_slug") or "").strip().lower()
    name = (request.form.get("name") or "").strip()
    tier = (request.form.get("tier") or "").strip().lower()

    try:
        speciality = _normalize_speciality(request.form.get("speciality", ""))
        if not name:
            raise ValueError("Player name is required")
        if tier not in {"silver", "gold", "platinum"}:
            raise ValueError("Tier must be silver, gold, or platinum")

        with _connect_sqlite() as conn:
            conn.execute(
                """
                INSERT INTO players
                (id, canonical_name, display_name, speciality, tier, is_manager, manager_username, created_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    secrets.token_hex(8),
                    name.lower(),
                    name,
                    speciality,
                    tier,
                    0,
                    None,
                    datetime.utcnow().isoformat(),
                    _safe_json_dumps({"created_by": "admin-lifecycle"}),
                ),
            )
            conn.commit()

        flash("Player added to database", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return _redirect_lifecycle(season_slug)


@unified_admin_bp.post("/admin/lifecycle/add-team", endpoint="lifecycle_add_team")
def lifecycle_add_team():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_slug = (request.form.get("season_slug") or "").strip().lower()
    team_name = (request.form.get("team_name") or "").strip()

    try:
        if not season_slug:
            raise ValueError("Select a season first")
        if not team_name:
            raise ValueError("Team name is required")

        with _connect_sqlite() as conn:
            auction_row = _find_lifecycle_auction(conn, season_slug)
            if not auction_row:
                raise ValueError("Lifecycle setup season not found")

            setup_state = _extract_admin_setup_state(auction_row["metadata_json"])
            if any((team.get("name") or "").strip().lower() == team_name.lower() for team in setup_state["teams"]):
                raise ValueError("Team name already exists in this setup")

            setup_state["teams"].append(
                {
                    "id": f"team-{secrets.token_hex(6)}",
                    "name": team_name,
                    "selected": True,
                    "manager_player_id": None,
                }
            )
            _save_admin_setup_state(conn, auction_row["auction_id"], setup_state)
            conn.commit()

        flash("Team added to setup", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return _redirect_lifecycle(season_slug)


@unified_admin_bp.post("/admin/lifecycle/add-existing-team", endpoint="lifecycle_add_existing_team")
def lifecycle_add_existing_team():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_slug = (request.form.get("season_slug") or "").strip().lower()
    team_id = (request.form.get("team_id") or "").strip()

    try:
        if not season_slug:
            raise ValueError("Select a season first")
        if not team_id:
            raise ValueError("Select an existing team")

        with _connect_sqlite() as conn:
            auction_row = _find_lifecycle_auction(conn, season_slug)
            if not auction_row:
                raise ValueError("Lifecycle setup season not found")

            existing_team = conn.execute(
                "SELECT id, name, manager_player_id FROM teams WHERE id = ? LIMIT 1",
                (team_id,),
            ).fetchone()
            if not existing_team:
                raise ValueError("Selected team does not exist in database")

            setup_state = _extract_admin_setup_state(auction_row["metadata_json"])
            if any((team.get("id") or "").strip() == team_id for team in setup_state["teams"]):
                raise ValueError("Team already added to this setup")

            sellable_set = set(setup_state.get("sellable_player_ids") or [])
            manager_player_id = (existing_team["manager_player_id"] or "").strip() or None
            if manager_player_id and manager_player_id in sellable_set:
                manager_player_id = None

            setup_state["teams"].append(
                {
                    "id": existing_team["id"],
                    "name": existing_team["name"] or existing_team["id"],
                    "selected": True,
                    "manager_player_id": manager_player_id,
                }
            )
            _save_admin_setup_state(conn, auction_row["auction_id"], setup_state)
            conn.commit()

        flash("Existing team added to setup", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return _redirect_lifecycle(season_slug)


@unified_admin_bp.post("/admin/lifecycle/save-players", endpoint="lifecycle_save_players")
def lifecycle_save_players():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_slug = (request.form.get("season_slug") or "").strip().lower()

    try:
        if not season_slug:
            raise ValueError("Select a season first")

        sellable_ids = [
            str(player_id or "").strip()
            for player_id in request.form.getlist("sellable_player_ids")
            if str(player_id or "").strip()
        ]

        with _connect_sqlite() as conn:
            auction_row = _find_lifecycle_auction(conn, season_slug)
            if not auction_row:
                raise ValueError("Lifecycle setup season not found")

            valid_ids = {
                row["id"]
                for row in conn.execute(
                    "SELECT id FROM players"
                ).fetchall()
            }
            unknown = [player_id for player_id in sellable_ids if player_id not in valid_ids]
            if unknown:
                raise ValueError("Selected sellable list contains invalid players")

            setup_state = _extract_admin_setup_state(auction_row["metadata_json"])
            setup_state["sellable_player_ids"] = sellable_ids

            # Clear manager assignments that now conflict with sellable players.
            sellable_set = set(sellable_ids)
            for team in setup_state["teams"]:
                manager_player_id = (team.get("manager_player_id") or "").strip()
                if manager_player_id and manager_player_id in sellable_set:
                    team["manager_player_id"] = None

            _save_admin_setup_state(conn, auction_row["auction_id"], setup_state)
            conn.commit()

        flash("Sellable player list saved", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return _redirect_lifecycle(season_slug)


@unified_admin_bp.post("/admin/lifecycle/save-teams", endpoint="lifecycle_save_teams")
def lifecycle_save_teams():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_slug = (request.form.get("season_slug") or "").strip().lower()

    try:
        if not season_slug:
            raise ValueError("Select a season first")

        selected_team_ids = {
            str(team_id or "").strip()
            for team_id in request.form.getlist("selected_team_ids")
            if str(team_id or "").strip()
        }

        with _connect_sqlite() as conn:
            auction_row = _find_lifecycle_auction(conn, season_slug)
            if not auction_row:
                raise ValueError("Lifecycle setup season not found")

            setup_state = _extract_admin_setup_state(auction_row["metadata_json"])
            sellable_set = set(setup_state.get("sellable_player_ids") or [])

            valid_manager_ids = {
                row["id"]
                for row in conn.execute(
                    "SELECT id FROM players"
                ).fetchall()
            }

            assigned_manager_ids = set()
            for team in setup_state["teams"]:
                team_id = (team.get("id") or "").strip()
                if not team_id:
                    continue

                team["selected"] = team_id in selected_team_ids
                manager_key = f"manager_player_id__{team_id}"
                requested_manager = (request.form.get(manager_key) or "").strip() or None

                if not team["selected"]:
                    team["manager_player_id"] = None
                    continue

                if not requested_manager:
                    team["manager_player_id"] = None
                    continue

                if requested_manager not in valid_manager_ids:
                    raise ValueError("Invalid manager selection")
                if requested_manager in sellable_set:
                    raise ValueError("Managers must be selected from players not in the auction")
                if requested_manager in assigned_manager_ids:
                    raise ValueError("A manager can only be assigned to one team")

                assigned_manager_ids.add(requested_manager)
                team["manager_player_id"] = requested_manager

            _save_admin_setup_state(conn, auction_row["auction_id"], setup_state)
            conn.commit()

        flash("Team participation and manager assignments saved", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return _redirect_lifecycle(season_slug)


@unified_admin_bp.post("/admin/lifecycle/save-snapshot", endpoint="lifecycle_save_snapshot")
def lifecycle_save_snapshot():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_slug = (request.form.get("season_slug") or "").strip().lower()
    snapshot_name = (request.form.get("snapshot_name") or "").strip()
    overwrite = (request.form.get("overwrite") or "").strip().lower() in {"1", "true", "yes", "on"}

    try:
        if not season_slug:
            raise ValueError("Select a season first")
        if not snapshot_name:
            raise ValueError("Snapshot name is required")

        with _connect_sqlite() as conn:
            auction_row = _find_lifecycle_auction(conn, season_slug)
            if not auction_row:
                raise ValueError("Lifecycle setup season not found")

            setup_state = _extract_admin_setup_state(auction_row["metadata_json"])
            snapshot_slug = _slugify_lifecycle_value(snapshot_name, "snapshot")
            source_path = f"admin-setup/{snapshot_slug}"
            now = datetime.utcnow().isoformat()

            existing = conn.execute(
                """
                SELECT id
                FROM auction_snapshots
                WHERE auction_id = ?
                  AND snapshot_type = 'admin-setup'
                  AND source_path = ?
                LIMIT 1
                """,
                (auction_row["auction_id"], source_path),
            ).fetchone()

            state_json = _safe_json_dumps(
                {
                    "season_slug": season_slug,
                    "admin_setup": setup_state,
                }
            )

            if existing and not overwrite:
                raise ValueError("A snapshot with this name already exists")

            if existing:
                conn.execute(
                    """
                    UPDATE auction_snapshots
                    SET snapshot_name = ?,
                        created_at = ?,
                        state_json = ?,
                        metadata_json = ?
                    WHERE id = ?
                    """,
                    (
                        snapshot_name,
                        now,
                        state_json,
                        _safe_json_dumps({"source": "admin-lifecycle"}),
                        existing["id"],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO auction_snapshots
                    (id, auction_id, snapshot_name, snapshot_type, source_path, created_at, restored_at, state_json, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"snapshot::{auction_row['auction_id']}::{snapshot_slug}",
                        auction_row["auction_id"],
                        snapshot_name,
                        "admin-setup",
                        source_path,
                        now,
                        None,
                        state_json,
                        _safe_json_dumps({"source": "admin-lifecycle"}),
                    ),
                )

            conn.commit()

        flash("Setup snapshot saved to database", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return _redirect_lifecycle(season_slug)


@unified_admin_bp.post("/admin/lifecycle/load-snapshot", endpoint="lifecycle_load_snapshot")
def lifecycle_load_snapshot():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_slug = (request.form.get("season_slug") or "").strip().lower()
    snapshot_id = (request.form.get("snapshot_id") or "").strip()

    try:
        if not season_slug:
            raise ValueError("Select a season first")
        if not snapshot_id:
            raise ValueError("Snapshot selection is required")

        with _connect_sqlite() as conn:
            auction_row = _find_lifecycle_auction(conn, season_slug)
            if not auction_row:
                raise ValueError("Lifecycle setup season not found")

            snapshot = conn.execute(
                """
                SELECT id, state_json
                FROM auction_snapshots
                WHERE id = ?
                  AND auction_id = ?
                  AND snapshot_type = 'admin-setup'
                LIMIT 1
                """,
                (snapshot_id, auction_row["auction_id"]),
            ).fetchone()
            if not snapshot:
                raise ValueError("Snapshot not found")

            snapshot_payload = _safe_json_loads(snapshot["state_json"])
            setup_state = _normalize_admin_setup_state(snapshot_payload.get("admin_setup"))
            _save_admin_setup_state(conn, auction_row["auction_id"], setup_state)
            conn.execute(
                "UPDATE auction_snapshots SET restored_at = ? WHERE id = ?",
                (datetime.utcnow().isoformat(), snapshot["id"]),
            )
            conn.commit()

        flash("Setup snapshot loaded", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return _redirect_lifecycle(season_slug)


@unified_admin_bp.post("/admin/lifecycle/activate", endpoint="lifecycle_activate")
def lifecycle_activate():
    guard = _require_unified_admin_user()
    if guard:
        return guard

    season_slug = (request.form.get("season_slug") or "").strip().lower()

    try:
        if not season_slug:
            raise ValueError("Select a lifecycle season first")

        auth_service = current_app.extensions["auth_service"]
        auction_service = current_app.extensions["auction_service"]
        auction_store = current_app.extensions["auction_store"]

        with _connect_sqlite() as conn:
            auction_row = _find_lifecycle_auction(conn, season_slug)
            if not auction_row:
                raise ValueError("Lifecycle setup season not found")

            setup_state = _extract_admin_setup_state(auction_row["metadata_json"])
            sellable_player_ids = list(setup_state.get("sellable_player_ids") or [])
            selected_teams = [team for team in setup_state.get("teams") or [] if team.get("selected")]

            if not sellable_player_ids:
                raise ValueError("No sellable players selected")
            if not selected_teams:
                raise ValueError("No participating teams selected")

            sellable_players = _select_players_by_ids(conn, sellable_player_ids)
            if len(sellable_players) != len(set(sellable_player_ids)):
                raise ValueError("One or more sellable players are missing in database")

            manager_player_ids = []
            for team in selected_teams:
                manager_player_id = str(team.get("manager_player_id") or "").strip()
                if not manager_player_id:
                    raise ValueError(f"Team '{team.get('name') or team.get('id')}' has no manager assigned")
                if manager_player_id in sellable_players:
                    raise ValueError(f"Team '{team.get('name') or team.get('id')}' manager is marked sellable")
                manager_player_ids.append(manager_player_id)

            manager_players = _select_players_by_ids(conn, manager_player_ids)
            if len(manager_players) != len(set(manager_player_ids)):
                raise ValueError("One or more assigned managers are missing in database")

            team_ids = [str(team.get("id") or "").strip() for team in selected_teams if str(team.get("id") or "").strip()]
            database_teams = {}
            if team_ids:
                placeholders = ",".join(["?" for _ in team_ids])
                rows = conn.execute(
                    f"SELECT id, name, manager_username FROM teams WHERE id IN ({placeholders})",
                    team_ids,
                ).fetchall()
                database_teams = {row["id"]: row for row in rows}

            runtime_players = []
            for player_id in sellable_player_ids:
                row = sellable_players.get(player_id)
                tier = (row["tier"] or "silver").strip().lower()
                speciality = (row["speciality"] or "ALL_ROUNDER").strip() or "ALL_ROUNDER"
                runtime_players.append(
                    {
                        "id": row["id"],
                        "name": row["display_name"] or row["id"],
                        "tier": tier,
                        "speciality": speciality,
                        "base_price": TIER_BASE_PRICE.get(tier, 100),
                        "status": "unsold",
                        "sold_to": None,
                        "sold_price": 0,
                        "phase_sold": None,
                        "credits": TIER_CREDIT_COST.get(tier, 1),
                        "current_bid": 0,
                        "current_bidder_team_id": None,
                        "nominated_phase_a": False,
                    }
                )

            runtime_users = []
            runtime_teams = []
            used_usernames = set()
            created_credentials = []

            for team in selected_teams:
                team_id = str(team.get("id") or "").strip()
                team_name = str(team.get("name") or team_id).strip() or team_id
                manager_player_id = str(team.get("manager_player_id") or "").strip()
                manager_row = manager_players[manager_player_id]

                manager_name = (manager_row["display_name"] or manager_player_id).strip() or manager_player_id
                manager_tier = (manager_row["tier"] or "silver").strip().lower()
                manager_speciality = (manager_row["speciality"] or "ALL_ROUNDER").strip() or "ALL_ROUNDER"

                preferred_username = ""
                db_team = database_teams.get(team_id)
                if db_team:
                    preferred_username = (db_team["manager_username"] or "").strip()

                # Reuse an existing canonical manager username for this display name.
                existing_by_name = conn.execute(
                    """
                    SELECT username
                    FROM users
                    WHERE LOWER(role) = 'manager'
                      AND LOWER(COALESCE(display_name, '')) = LOWER(?)
                      AND password_hash IS NOT NULL
                      AND LENGTH(TRIM(password_hash)) > 0
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (manager_name,),
                ).fetchone()
                if existing_by_name:
                    preferred_username = (existing_by_name["username"] or "").strip() or preferred_username

                preferred_username = preferred_username or _slugify_username(manager_name)
                username = _ensure_unique_manager_username(preferred_username, auth_service, used_usernames)

                if not auth_service.get_user(username):
                    auth_service.create_manager_credentials(username=username, display_name=manager_name)
                    created_credentials.append(username)

                runtime_users.append(
                    {
                        "username": username,
                        "role": ROLE_MANAGER,
                        "display_name": manager_name,
                        "speciality": manager_speciality,
                        "team_id": team_id,
                    }
                )

                runtime_teams.append(
                    {
                        "id": team_id,
                        "name": team_name,
                        "manager_username": username,
                        "manager_tier": manager_tier,
                        "players": [],
                        "bench": [],
                        "spent": 0,
                        "purse_remaining": None,
                        "credits_remaining": None,
                    }
                )

            runtime_tables = {
                "meta": [
                    {
                        "phase": PHASE_SETUP,
                        "created_at": datetime.utcnow().isoformat(),
                        "current_player_id": None,
                        "nomination_history": [],
                    }
                ],
                "teams": runtime_teams,
                "users": runtime_users,
                "players": runtime_players,
                "bids": [],
                "trade_requests": [],
            }

            auction_store.import_tables(runtime_tables)
            auth_service.seed_admin_if_missing()
            auction_service.bootstrap_defaults()
            auction_service.setup_team_budgets()

            # Track activation on lifecycle auction metadata for auditability.
            lifecycle_meta = _safe_json_loads(auction_row["metadata_json"])
            lifecycle_meta["activated_to_live"] = {
                "activated_at": datetime.utcnow().isoformat(),
                "player_count": len(runtime_players),
                "team_count": len(runtime_teams),
            }
            conn.execute(
                "UPDATE auctions SET status = ?, updated_at = ?, metadata_json = ? WHERE id = ?",
                ("active", datetime.utcnow().isoformat(), _safe_json_dumps(lifecycle_meta), auction_row["auction_id"]),
            )
            conn.commit()

        socketio.emit("state_update", auction_service.get_state())

        if created_credentials:
            flash(
                "Activated setup to live auction. New manager usernames created: "
                + ", ".join(created_credentials)
                + ". Temporary password for these managers is password123.",
                "success",
            )
        else:
            flash("Activated setup to live auction.", "success")
        return redirect(url_for("unified_admin.dashboard", tab="auction"))
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")
        return _redirect_lifecycle(season_slug)


@admin_bp.post("/create-manager")
@login_required(role=ROLE_ADMIN)
def create_manager():
    auth_service = current_app.extensions["auth_service"]
    auth_created_for = None
    try:
        _ensure_setup_phase()
        username = request.form.get("username", "").strip()
        display_name = request.form.get("display_name", "").strip()
        team_name = request.form.get("team_name", "").strip()
        manager_tier = request.form.get("manager_tier", "silver").strip().lower()
        speciality = _normalize_speciality(request.form.get("speciality", ""))
        credentials_result = auth_service.create_manager_credentials(username=username, display_name=display_name)
        auth_created_for = username

        team_id = secrets.token_hex(8)
        auction_store = current_app.extensions["auction_store"]
        with auction_store.write() as db:
            auction_users = db.table("users")
            teams = db.table("teams")

            if auction_users.get(lambda u: u.get("username") == username):
                raise ValueError("Username already exists")

            auction_users.insert(
                {
                    "username": username,
                    "role": ROLE_MANAGER,
                    "display_name": display_name,
                    "speciality": speciality,
                    "team_id": team_id,
                }
            )

            teams.insert(
                {
                    "id": team_id,
                    "name": team_name,
                    "manager_username": username,
                    "manager_tier": manager_tier,
                    "players": [],
                    "bench": [],
                    "spent": 0,
                    "purse_remaining": None,
                    "credits_remaining": None,
                }
            )

        current_app.extensions["auction_service"].setup_team_budgets()
        socketio.emit("state_update", current_app.extensions["auction_service"].get_state())
        return jsonify({"ok": True, "team_id": team_id, **credentials_result})
    except Exception as exc:  # noqa: BLE001
        if auth_created_for:
            try:
                auth_service.delete_user(auth_created_for)
            except Exception:  # noqa: BLE001
                pass
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/add-player")
@login_required(role=ROLE_ADMIN)
def add_player():
    tier = request.form.get("tier", "silver").strip().lower()
    name = request.form.get("name", "").strip()
    speciality = request.form.get("speciality", "")
    auction_service = current_app.extensions["auction_service"]
    store = current_app.extensions["auction_store"]

    from app.rules import TIER_BASE_PRICE, TIER_CREDIT_COST

    try:
        _ensure_setup_phase()
        normalized_speciality = _normalize_speciality(speciality)
        with store.write() as db:
            db.table("players").insert(
                {
                    "id": __import__("secrets").token_hex(8),
                    "name": name,
                    "tier": tier,
                    "base_price": TIER_BASE_PRICE[tier],
                    "status": "unsold",
                    "sold_to": None,
                    "sold_price": 0,
                    "phase_sold": None,
                    "credits": TIER_CREDIT_COST[tier],
                    "current_bid": 0,
                    "current_bidder_team_id": None,
                    "nominated_phase_a": False,
                    "speciality": normalized_speciality,
                }
            )
        socketio.emit("state_update", auction_service.get_state())
        return redirect(url_for("admin.dashboard"))
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")
        return redirect(url_for("admin.dashboard"))


@admin_bp.post("/update-player")
@login_required(role=ROLE_ADMIN)
def update_player():
    player_id = request.form.get("player_id", "").strip()
    name = request.form.get("name", "").strip()
    tier = request.form.get("tier", "").strip().lower()
    speciality = request.form.get("speciality", "")

    if not player_id:
        return jsonify({"ok": False, "error": "Missing player id"}), 400
    if not name:
        return jsonify({"ok": False, "error": "Player name is required"}), 400

    from app.rules import TIER_BASE_PRICE, TIER_CREDIT_COST

    if tier not in TIER_BASE_PRICE:
        return jsonify({"ok": False, "error": "Invalid tier"}), 400

    try:
        _ensure_setup_phase()
        normalized_speciality = _normalize_speciality(speciality)
        Player = Query()
        store = current_app.extensions["auction_store"]
        auction_service = current_app.extensions["auction_service"]

        with store.write() as db:
            players = db.table("players")
            if not players.get(Player.id == player_id):
                return jsonify({"ok": False, "error": "Player not found"}), 404

            players.update(
                {
                    "name": name,
                    "tier": tier,
                    "base_price": TIER_BASE_PRICE[tier],
                    "credits": TIER_CREDIT_COST[tier],
                    "speciality": normalized_speciality,
                },
                Player.id == player_id,
            )

        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/delete-player")
@login_required(role=ROLE_ADMIN)
def delete_player():
    player_id = request.form.get("player_id", "").strip()
    if not player_id:
        return jsonify({"ok": False, "error": "Missing player id"}), 400

    try:
        _ensure_setup_phase()
        Player = Query()
        Bid = Query()
        Team = Query()
        store = current_app.extensions["auction_store"]
        auction_service = current_app.extensions["auction_service"]

        with store.write() as db:
            players = db.table("players")
            player = players.get(Player.id == player_id)
            if not player:
                return jsonify({"ok": False, "error": "Player not found"}), 404

            db.table("bids").remove(Bid.player_id == player_id)

            teams = db.table("teams")
            for team in teams.all():
                updated_players = [pid for pid in team.get("players", []) if pid != player_id]
                updated_bench = [pid for pid in team.get("bench", []) if pid != player_id]
                if updated_players != team.get("players", []) or updated_bench != team.get("bench", []):
                    teams.update(
                        {"players": updated_players, "bench": updated_bench},
                        Team.id == team["id"],
                    )

            meta = db.table("meta").get(doc_id=1) or {}
            if meta.get("current_player_id") == player_id:
                db.table("meta").update({"current_player_id": None}, doc_ids=[1])

            players.remove(Player.id == player_id)

        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/update-manager")
@login_required(role=ROLE_ADMIN)
def update_manager():
    manager_username = request.form.get("manager_username", "").strip()
    username = request.form.get("username", "").strip()
    display_name = request.form.get("display_name", "").strip()
    speciality = request.form.get("speciality", "")

    if not manager_username:
        return jsonify({"ok": False, "error": "Missing manager username"}), 400
    if not username:
        return jsonify({"ok": False, "error": "Username is required"}), 400
    if not display_name:
        return jsonify({"ok": False, "error": "Display name is required"}), 400

    try:
        _ensure_setup_phase()
        normalized_speciality = _normalize_speciality(speciality)
        auth_service = current_app.extensions["auth_service"]
        auth_service.assert_username_available(username, except_username=manager_username)
        User = Query()
        Team = Query()
        store = current_app.extensions["auction_store"]
        auction_service = current_app.extensions["auction_service"]

        with store.write() as db:
            users = db.table("users")
            teams = db.table("teams")

            manager = users.get(User.username == manager_username)
            if not manager or manager.get("role") != "manager":
                return jsonify({"ok": False, "error": "Manager not found"}), 404

            username_taken = users.get((User.username == username) & (User.username != manager_username))
            if username_taken:
                return jsonify({"ok": False, "error": "Username already exists"}), 400

            users.update(
                {
                    "username": username,
                    "display_name": display_name,
                    "speciality": normalized_speciality,
                },
                User.username == manager_username,
            )

            teams.update(
                {"manager_username": username},
                Team.manager_username == manager_username,
            )

        auth_service.update_user(current_username=manager_username, new_username=username, display_name=display_name)

        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/delete-manager")
@login_required(role=ROLE_ADMIN)
def delete_manager():
    manager_username = request.form.get("manager_username", "").strip()
    if not manager_username:
        return jsonify({"ok": False, "error": "Missing manager username"}), 400

    try:
        _ensure_setup_phase()
        User = Query()
        Team = Query()
        Player = Query()
        store = current_app.extensions["auction_store"]
        auction_service = current_app.extensions["auction_service"]

        with store.write() as db:
            users = db.table("users")
            teams = db.table("teams")
            players = db.table("players")

            manager = users.get(User.username == manager_username)
            if not manager or manager.get("role") != "manager":
                return jsonify({"ok": False, "error": "Manager not found"}), 404

            team_id = manager.get("team_id")
            team = teams.get(Team.id == team_id) if team_id else None

            if team:
                for pid in team.get("players", []) + team.get("bench", []):
                    players.update(
                        {
                            "status": "unsold",
                            "sold_to": None,
                            "sold_price": 0,
                            "phase_sold": None,
                            "current_bid": 0,
                            "current_bidder_team_id": None,
                            "nominated_phase_a": False,
                        },
                        Player.id == pid,
                    )
                teams.remove(Team.id == team_id)

            users.remove(User.username == manager_username)

        auth_service = current_app.extensions["auth_service"]
        auth_service.delete_user(manager_username)

        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/update-team")
@login_required(role=ROLE_ADMIN)
def update_team():
    team_id = request.form.get("team_id", "").strip()
    team_name = request.form.get("team_name", "").strip()
    manager_tier = request.form.get("manager_tier", "").strip().lower()

    if not team_id:
        return jsonify({"ok": False, "error": "Missing team id"}), 400
    if not team_name:
        return jsonify({"ok": False, "error": "Team name is required"}), 400
    if manager_tier not in {"silver", "gold", "platinum"}:
        return jsonify({"ok": False, "error": "Invalid manager tier"}), 400

    try:
        _ensure_setup_phase()
        Team = Query()
        store = current_app.extensions["auction_store"]
        auction_service = current_app.extensions["auction_service"]

        with store.write() as db:
            teams = db.table("teams")
            if not teams.get(Team.id == team_id):
                return jsonify({"ok": False, "error": "Team not found"}), 404

            teams.update(
                {
                    "name": team_name,
                    "manager_tier": manager_tier,
                },
                Team.id == team_id,
            )

        auction_service.setup_team_budgets()
        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/delete-team")
@login_required(role=ROLE_ADMIN)
def delete_team():
    team_id = request.form.get("team_id", "").strip()
    if not team_id:
        return jsonify({"ok": False, "error": "Missing team id"}), 400

    try:
        _ensure_setup_phase()
        Team = Query()
        User = Query()
        Player = Query()
        Bid = Query()
        store = current_app.extensions["auction_store"]
        auction_service = current_app.extensions["auction_service"]

        with store.write() as db:
            teams = db.table("teams")
            users = db.table("users")
            players = db.table("players")
            bids = db.table("bids")

            team = teams.get(Team.id == team_id)
            if not team:
                return jsonify({"ok": False, "error": "Team not found"}), 404

            for pid in team.get("players", []) + team.get("bench", []):
                players.update(
                    {
                        "status": "unsold",
                        "sold_to": None,
                        "sold_price": 0,
                        "phase_sold": None,
                        "current_bid": 0,
                        "current_bidder_team_id": None,
                        "nominated_phase_a": False,
                    },
                    Player.id == pid,
                )

            bids.remove(Bid.team_id == team_id)
            linked_users = users.search(User.team_id == team_id)
            users.remove(User.team_id == team_id)
            teams.remove(Team.id == team_id)

        auth_service = current_app.extensions["auth_service"]
        for linked_user in linked_users:
            username = (linked_user or {}).get("username")
            if username:
                auth_service.delete_user(username)

        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/set-phase")
@login_required(role=ROLE_ADMIN)
def set_phase():
    phase = request.form.get("phase")
    if phase not in {PHASE_A_SG, PHASE_A_BREAK, PHASE_A_P, PHASE_B}:
        return jsonify({"ok": False, "error": "Invalid phase"}), 400
    auction_service = current_app.extensions["auction_service"]
    state = auction_service.get_state()
    if phase == PHASE_B and not state.get("phase_b_readiness", {}).get("can_enter_phase_b", False):
        readiness = state.get("phase_b_readiness", {})
        return jsonify(
            {
                "ok": False,
                "error": (
                    "Phase B cannot start until unsold players are greater than the number "
                    "needed to fill incomplete teams"
                ),
                "phase_b_readiness": readiness,
            }
        ), 400
    auction_service.set_phase(phase)
    socketio.emit("state_update", auction_service.get_state())
    return jsonify({"ok": True})


@admin_bp.post("/nominate-next")
@login_required(role=ROLE_ADMIN)
def nominate_next():
    auction_service = current_app.extensions["auction_service"]
    sold_result = None
    state = auction_service.get_state()
    previous_player_id = state.get("current_player", {}).get("id") if state.get("current_player") else None

    # One-click flow: close current lot first, then nominate next lot.
    if state.get("current_player"):
        sold_result = auction_service.close_current_player()

    player = auction_service.nominate_next_player(previous_player_id=previous_player_id)
    socketio.emit("state_update", auction_service.get_state())

    if not player and not sold_result:
        return jsonify({"ok": False, "error": "No player available for this phase"}), 400

    return jsonify({"ok": True, "sold_result": sold_result, "player": player})


@admin_bp.post("/previous-player")
@login_required(role=ROLE_ADMIN)
def previous_player():
    auction_service = current_app.extensions["auction_service"]
    try:
        player = auction_service.previous_player()
        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True, "player": player})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/close-current")
@login_required(role=ROLE_ADMIN)
def close_current():
    auction_service = current_app.extensions["auction_service"]
    try:
        result = auction_service.close_current_player()
        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True, "result": result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/delete-bid")
@login_required(role=ROLE_ADMIN)
def delete_bid():
    auction_service = current_app.extensions["auction_service"]
    bid_id = request.form.get("bid_id", "").strip()
    if not bid_id:
        return jsonify({"ok": False, "error": "Missing bid id"}), 400
    try:
        result = auction_service.delete_bid(bid_id)
        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True, "result": result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/complete-draft")
@login_required(role=ROLE_ADMIN)
def complete_draft():
    auction_service = current_app.extensions["auction_service"]
    state = auction_service.get_state()
    if state.get("phase") != PHASE_B:
        return jsonify({"ok": False, "error": "Complete Draft + Penalties is only allowed during Phase B"}), 400
    auction_service.complete_phase_b_with_penalties()
    socketio.emit("state_update", auction_service.get_state())
    return jsonify({"ok": True})


@admin_bp.get("/session/list")
@login_required(role=ROLE_ADMIN)
def list_sessions():
    snapshots = []
    for file_path in _snapshot_dir().glob("*.json"):
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        snapshots.append(
            {
                "file": file_path.name,
                "label": payload.get("session_name") or file_path.stem,
                "saved_at": payload.get("saved_at") or datetime.utcfromtimestamp(file_path.stat().st_mtime).isoformat(),
            }
        )

    sessions = sorted(snapshots, key=lambda item: item.get("saved_at") or "", reverse=True)
    return jsonify({"ok": True, "sessions": sessions})


@admin_bp.post("/session/save")
@login_required(role=ROLE_ADMIN)
def save_session():
    requested_name = request.form.get("session_name", "").strip()
    overwrite = request.form.get("overwrite", "").strip().lower() in {"1", "true", "yes", "on"}
    slug = _slugify_session_name(requested_name)
    auction_store = current_app.extensions["auction_store"]
    payload = {
        "session_name": requested_name or slug,
        "saved_at": datetime.utcnow().isoformat(),
        "tables": auction_store.export_tables(),
    }
    snapshot_payload = {
        "slug": slug,
        "file": f"{slug}.json",
        "session_name": payload["session_name"],
        "saved_at": payload["saved_at"],
        "tables": payload["tables"],
    }

    snapshot_file = _resolve_snapshot_file(snapshot_payload["file"])
    existed = snapshot_file.exists()
    if existed and not overwrite:
        return jsonify({"ok": False, "error": "A session with this name already exists"}), 400

    snapshot_file.write_text(json.dumps(snapshot_payload, indent=2), encoding="utf-8")
    return jsonify({"ok": True, "file": snapshot_payload["file"], "overwritten": existed})


@admin_bp.post("/publish-session")
@login_required(role=ROLE_ADMIN)
def publish_session():
    requested_name = request.form.get("session_name", "").strip()
    requested_suffix = request.form.get("session_link_suffix", "").strip()
    overwrite = request.form.get("overwrite", "").strip().lower() in {"1", "true", "yes", "on"}
    slug = _slugify_session_name(requested_suffix or requested_name)
    if slug in RESERVED_PUBLIC_SLUGS:
        return jsonify({"ok": False, "error": "That name is reserved"}), 400
    file_path = _resolve_published_file(f"{slug}.json")
    existed = file_path.exists()
    if existed and not overwrite:
        return jsonify({"ok": False, "error": "A published session with this name already exists"}), 400

    auction_service = current_app.extensions["auction_service"]
    state = auction_service.get_state()
    if state.get("phase") != PHASE_COMPLETE:
        return jsonify({"ok": False, "error": "Publishing is only allowed after the auction is complete"}), 400

    store = current_app.extensions["auction_store"]
    payload = {
        "session_name": requested_name or slug,
        "session_link_suffix": slug,
        "saved_at": datetime.utcnow().isoformat(),
        "published": True,
        "tables": store.export_tables(),
    }
    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    season_store_manager = current_app.extensions["season_store_manager"]
    season_store_existed = season_store_manager.has_season(slug)
    season_store = season_store_manager.get_store(slug, create=True)

    season_tables_payload = {
        table_name: rows
        for table_name, rows in (payload["tables"] or {}).items()
        if table_name != "bids"
    }

    preserved_tables = {}
    preserved_meta = {}
    if season_store_existed:
        existing_tables = season_store.export_tables()
        for table_name, rows in existing_tables.items():
            if table_name in {"meta", "teams", "users", "players", "bids"}:
                continue
            preserved_tables[table_name] = rows
        season_meta_rows = existing_tables.get("season_meta") or []
        if season_meta_rows:
            preserved_meta = dict(season_meta_rows[0])

    season_store.import_tables(season_tables_payload)

    with season_store.write() as db:
        for table_name, rows in preserved_tables.items():
            table = db.table(table_name)
            if rows:
                table.insert_multiple(rows)

        season_meta = {
            "slug": slug,
            "name": requested_name or slug,
            "published": True,
            "published_file": file_path.name,
            "published_at": payload["saved_at"],
            "created_at": preserved_meta.get("created_at") or datetime.utcnow().isoformat(),
            "submissions_open": bool(preserved_meta.get("submissions_open", False)),
        }

        meta_table = db.table("season_meta")
        if meta_table.get(doc_id=1):
            meta_table.update(season_meta, doc_ids=[1])
        else:
            meta_table.insert(season_meta)

    return jsonify(
        {
            "ok": True,
            "file": file_path.name,
            "overwritten": existed,
            "public_path": url_for("viewer.published_view", slug=slug),
        }
    )


@admin_bp.post("/session/load")
@login_required(role=ROLE_ADMIN)
def load_session():
    filename = request.form.get("session_file", "").strip()
    if not filename:
        return jsonify({"ok": False, "error": "Session file is required"}), 400

    try:
        snapshot_file = _resolve_snapshot_file(filename)
        if not snapshot_file.exists():
            return jsonify({"ok": False, "error": "Session not found"}), 404

        payload = json.loads(snapshot_file.read_text(encoding="utf-8"))

        tables = payload.get("tables")
        if not isinstance(tables, dict):
            return jsonify({"ok": False, "error": "Invalid session file format"}), 400

        store = current_app.extensions["auction_store"]
        auth_service = current_app.extensions["auth_service"]
        auction_service = current_app.extensions["auction_service"]

        store.import_tables(tables)
        auth_service.seed_admin_if_missing()
        auction_service.bootstrap_defaults()

        socketio.emit("state_update", auction_service.get_state())
        loaded_file = payload.get("file") or snapshot_file.name
        return jsonify({"ok": True, "loaded": loaded_file})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 500
