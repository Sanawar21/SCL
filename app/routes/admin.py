import json
import secrets
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, current_app, flash, jsonify, redirect, render_template, request, session, url_for
from tinydb import Query

from app import socketio
from app.authz import login_required
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
)

admin_bp = Blueprint("admin", __name__, url_prefix="/auction/admin")
unified_admin_bp = Blueprint("unified_admin", __name__)

SPECIALITIES = {"ALL_ROUNDER", "BATTER", "BOWLER"}


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _finance_seasons():
    season_store_manager = current_app.extensions["season_store_manager"]
    fantasy_service = current_app.extensions["fantasy_service"]

    seasons = []
    for slug in season_store_manager.list_slugs():
        safe_slug = (slug or "").strip().lower()
        if not safe_slug:
            continue
        season_info = fantasy_service.get_season(safe_slug) if hasattr(fantasy_service, "get_season") else None
        seasons.append(
            {
                "slug": safe_slug,
                "name": (season_info or {}).get("name") or safe_slug,
            }
        )

    seasons.sort(key=lambda item: item.get("slug") or "")
    return seasons


def _finance_team_rows(season_slug: str):
    safe_slug = (season_slug or "").strip().lower()
    if not safe_slug:
        return []

    season_store_manager = current_app.extensions["season_store_manager"]
    if not season_store_manager.has_season(safe_slug):
        return []

    store = season_store_manager.get_store(safe_slug, create=False)
    with store.read() as db:
        teams = db.table("teams").all()

    rows = []
    for team in teams:
        team_id = (team.get("id") or "").strip()
        if not team_id:
            continue
        rows.append(
            {
                "id": team_id,
                "name": (team.get("name") or team_id).strip() or team_id,
                "purse_remaining": _safe_int(team.get("purse_remaining"), 0),
                "credits_remaining": _safe_int(team.get("credits_remaining"), 0),
                "active_count": len(team.get("players") or []),
                "bench_count": len(team.get("bench") or []),
            }
        )

    rows.sort(key=lambda item: (item.get("name") or "").lower())
    return rows


def _finance_log_transaction(db, payload: dict):
    tx_table = db.table("finance_transactions")
    tx_table.insert(
        {
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            **payload,
        }
    )


def _build_unified_admin_context():
    auction_service = current_app.extensions["auction_service"]
    auction_store = current_app.extensions["auction_store"]
    fantasy_service = current_app.extensions["fantasy_service"]
    scorer_service = current_app.extensions["scorer_service"]

    seasons = fantasy_service.list_fantasy_seasons()
    published_sessions = fantasy_service.list_published_sessions()
    scorer_config = scorer_service.load_config()
    season_slug = (request.args.get("season") or "").strip().lower()
    active_tab = (request.args.get("tab") or "auction").strip().lower()
    if active_tab not in {"auction", "fantasy", "scorer", "stats", "finances"}:
        active_tab = "auction"

    selected = None
    entries = []
    if season_slug:
        selected = fantasy_service.get_season(season_slug)
        if selected:
            entries = fantasy_service.get_entries_for_season(season_slug)

    finance_seasons = _finance_seasons()
    requested_finance_season = (request.args.get("fin_season") or "").strip().lower()
    valid_finance_slugs = {(season.get("slug") or "").strip().lower() for season in finance_seasons}
    if requested_finance_season in valid_finance_slugs:
        finance_selected_season = requested_finance_season
    else:
        finance_selected_season = (finance_seasons[0].get("slug") if finance_seasons else "")

    finance_season_team_options = {
        (season.get("slug") or "").strip().lower(): _finance_team_rows((season.get("slug") or "").strip().lower())
        for season in finance_seasons
        if (season.get("slug") or "").strip().lower()
    }

    finance_selected_team_rows = finance_season_team_options.get(finance_selected_season, [])

    available_manager_players = []
    team_manager_options = {}
    with auction_store.read() as db:
        teams = db.table("teams").all()
        players = db.table("players").all()

        assigned_manager_ids = {
            (team.get("manager_player_id") or "").strip()
            for team in teams
            if (team.get("manager_player_id") or "").strip()
        }
        players_by_id = {
            (player.get("id") or "").strip(): player
            for player in players
            if (player.get("id") or "").strip()
        }

        for player in players:
            player_id = (player.get("id") or "").strip()
            if not player_id or player_id in assigned_manager_ids:
                continue
            if (player.get("status") or "").strip().lower() != "unsold":
                continue

            available_manager_players.append(
                {
                    "id": player_id,
                    "name": player.get("name") or "Unknown",
                    "tier": (player.get("tier") or "").strip().lower(),
                    "speciality": (player.get("speciality") or "-").strip() or "-",
                }
            )

        # Team manager reassignment options = available pool + current manager player.
        for team in teams:
            team_id = (team.get("id") or "").strip()
            if not team_id:
                continue

            options = [dict(item) for item in available_manager_players]
            current_manager_player_id = (team.get("manager_player_id") or "").strip()
            if current_manager_player_id and current_manager_player_id in players_by_id:
                current_player = players_by_id[current_manager_player_id]
                if not any(item["id"] == current_manager_player_id for item in options):
                    options.append(
                        {
                            "id": current_manager_player_id,
                            "name": current_player.get("name") or "Unknown",
                            "tier": (current_player.get("tier") or "").strip().lower(),
                            "speciality": (current_player.get("speciality") or "-").strip() or "-",
                        }
                    )

            options.sort(key=lambda item: item["name"].lower())
            team_manager_options[team_id] = options

    available_manager_players.sort(key=lambda item: item["name"].lower())

    return {
        "state": auction_service.get_state(),
        "active_tab": active_tab,
        "seasons": seasons,
        "published_sessions": published_sessions,
        "selected_season": selected,
        "entries": entries,
        "available_manager_players": available_manager_players,
        "team_manager_options": team_manager_options,
        "scorer_config": scorer_config,
        "scorer_available_seasons": scorer_service.list_seasons(),
        "scorer_download_filename": scorer_service.download_filename(scorer_config),
        "scorer_download_url": url_for("landing.scorer_download"),
        "scorer_recent_imports": scorer_service.list_recent_imports(limit=12),
        "scorer_team_global_stats": scorer_service.list_global_team_stats(limit=200),
        "scorer_player_global_stats": scorer_service.list_global_player_stats(limit=500),
        "scorer_match_seasons": scorer_service.list_match_seasons(),
        "scorer_match_registry": scorer_service.list_match_registry(limit=500),
        "scorer_season_team_options": scorer_service.list_season_team_options(),
        "finance_seasons": finance_seasons,
        "finance_selected_season": finance_selected_season,
        "finance_season_team_options": finance_season_team_options,
        "finance_selected_team_rows": finance_selected_team_rows,
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


@unified_admin_bp.post("/admin/scorer/import", endpoint="scorer_import")
def scorer_import():
    user = session.get("user")
    if not user:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if user.get("role") != ROLE_ADMIN:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    season_slug = (request.form.get("season_slug") or "").strip().lower()
    if not season_slug:
        return jsonify({"ok": False, "error": "Season is required"}), 400

    uploaded_files = [item for item in request.files.getlist("match_csvs") if item and (item.filename or "").strip()]
    if not uploaded_files:
        return jsonify({"ok": False, "error": "Select one or more CSV files"}), 400

    scorer_service = current_app.extensions["scorer_service"]
    confirm_overwrite = (request.form.get("confirm_overwrite") or "").strip().lower() in {"1", "true", "yes", "on"}
    include_in_fantasy_points_raw = request.form.get("include_in_fantasy_points")
    if include_in_fantasy_points_raw is None:
        include_in_fantasy_points = True
    else:
        include_in_fantasy_points = (include_in_fantasy_points_raw or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    imports = []
    errors = []
    duplicates = []
    latest_summary = {}
    for upload in uploaded_files:
        try:
            summary = scorer_service.import_match_csv(
                file_storage=upload,
                season_slug=season_slug,
                match_id_override=(request.form.get("match_id_override") or "").strip(),
                venue_override=(request.form.get("venue_override") or "").strip(),
                match_date=(request.form.get("match_date") or "").strip(),
                uploaded_by=(user.get("username") or "admin"),
                confirm_overwrite=confirm_overwrite,
                include_in_fantasy_points=include_in_fantasy_points,
            )
            imports.append(summary)
            latest_summary = summary
        except Exception as exc:  # noqa: BLE001
            if getattr(exc, "requires_confirmation", False):
                duplicates.append(
                    {
                        "file": upload.filename or "unknown.csv",
                        "season_slug": getattr(exc, "season_slug", season_slug),
                        "match_id": getattr(exc, "match_id", ""),
                        "error": str(exc),
                    }
                )
            errors.append({"file": upload.filename or "unknown.csv", "error": str(exc)})

    if not imports:
        if duplicates:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Overwrite confirmation required for one or more duplicate match IDs",
                        "errors": errors,
                        "confirmation_required": True,
                        "duplicates": duplicates,
                    }
                ),
                409,
            )
        return jsonify({"ok": False, "error": "Unable to import scorer CSV files", "errors": errors}), 400

    return jsonify(
        {
            "ok": True,
            "imports": imports,
            "errors": errors,
            "confirmation_required": bool(duplicates),
            "duplicates": duplicates,
            "summary": latest_summary,
            "recent_imports": scorer_service.list_recent_imports(limit=12),
        }
    )


@unified_admin_bp.post("/admin/scorer/matches", endpoint="scorer_match_upsert")
def scorer_match_upsert():
    user = session.get("user")
    if not user:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if user.get("role") != ROLE_ADMIN:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    scorer_service = current_app.extensions["scorer_service"]
    walkover = (request.form.get("walkover") or "").strip().lower() in {"1", "true", "yes", "on"}
    try:
        row = scorer_service.upsert_match_registry_entry(
            season_slug=(request.form.get("season_slug") or "").strip().lower(),
            match_id=(request.form.get("match_id") or "").strip(),
            team_a_global_id=(request.form.get("team_a_global_id") or "").strip(),
            team_b_global_id=(request.form.get("team_b_global_id") or "").strip(),
            match_number=(request.form.get("match_number") or "").strip(),
            match_title=(request.form.get("match_title") or "").strip(),
            walkover=walkover,
            walkover_winner_global_id=(request.form.get("walkover_winner_global_id") or "").strip(),
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400

    return jsonify(
        {
            "ok": True,
            "row": row,
            "matches": scorer_service.list_match_registry(limit=500),
        }
    )


@unified_admin_bp.post("/admin/scorer/matches/delete", endpoint="scorer_match_delete")
def scorer_match_delete():
    user = session.get("user")
    if not user:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if user.get("role") != ROLE_ADMIN:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    scorer_service = current_app.extensions["scorer_service"]
    try:
        summary = scorer_service.delete_match_registry_entry(
            season_slug=(request.form.get("season_slug") or "").strip().lower(),
            match_id=(request.form.get("match_id") or "").strip(),
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400

    if not summary.get("removed"):
        return jsonify({"ok": False, "error": "Match not found", "summary": summary}), 404

    return jsonify(
        {
            "ok": True,
            "summary": summary,
            "matches": scorer_service.list_match_registry(limit=500),
        }
    )


@unified_admin_bp.post("/admin/scorer/import/undo", endpoint="scorer_import_undo")
def scorer_import_undo():
    user = session.get("user")
    if not user:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if user.get("role") != ROLE_ADMIN:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    match_key = (request.form.get("match_key") or "").strip()
    if not match_key:
        return jsonify({"ok": False, "error": "Match key is required"}), 400

    scorer_service = current_app.extensions["scorer_service"]
    summary = scorer_service.undo_imported_match(match_key=match_key)
    if not summary.get("removed"):
        return jsonify({"ok": False, "error": "Match import not found", "summary": summary}), 404

    return jsonify(
        {
            "ok": True,
            "summary": summary,
            "recent_imports": scorer_service.list_recent_imports(limit=12),
        }
    )


@unified_admin_bp.post("/admin/finances/adjust", endpoint="finances_adjust")
def finances_adjust():
    user = session.get("user")
    if not user:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if user.get("role") != ROLE_ADMIN:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    season_slug = (request.form.get("season_slug") or "").strip().lower()
    team_id = (request.form.get("team_id") or "").strip()
    operation = (request.form.get("operation") or "").strip().lower()
    amount = _safe_int((request.form.get("amount") or "").strip(), -1)
    comment = (request.form.get("comment") or "").strip()

    if not season_slug:
        return jsonify({"ok": False, "error": "Season is required"}), 400
    if not team_id:
        return jsonify({"ok": False, "error": "Team is required"}), 400
    if operation not in {"add", "remove"}:
        return jsonify({"ok": False, "error": "Operation must be add or remove"}), 400
    if amount <= 0:
        return jsonify({"ok": False, "error": "Amount must be a positive integer"}), 400
    if not comment:
        return jsonify({"ok": False, "error": "Comment is required"}), 400

    season_store_manager = current_app.extensions["season_store_manager"]
    if not season_store_manager.has_season(season_slug):
        return jsonify({"ok": False, "error": "Season not found"}), 404

    store = season_store_manager.get_store(season_slug, create=False)
    Team = Query()

    with store.write() as db:
        teams = db.table("teams")
        team = teams.get(Team.id == team_id)
        if not team:
            return jsonify({"ok": False, "error": "Team not found"}), 404

        team_name = (team.get("name") or team_id).strip() or team_id
        current_purse = _safe_int(team.get("purse_remaining"), 0)
        delta = amount if operation == "add" else -amount
        next_purse = current_purse + delta

        teams.update({"purse_remaining": next_purse}, Team.id == team_id)
        _finance_log_transaction(
            db,
            {
                "type": "adjust",
                "season_slug": season_slug,
                "created_by": (user.get("username") or "admin").strip() or "admin",
                "operation": operation,
                "amount": amount,
                "comment": comment,
                "team_id": team_id,
                "team_name": team_name,
                "before_purse": current_purse,
                "after_purse": next_purse,
            },
        )

    return jsonify(
        {
            "ok": True,
            "season_slug": season_slug,
            "team_id": team_id,
            "operation": operation,
            "amount": amount,
            "team_rows": _finance_team_rows(season_slug),
        }
    )


@unified_admin_bp.post("/admin/finances/transfer", endpoint="finances_transfer")
def finances_transfer():
    user = session.get("user")
    if not user:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if user.get("role") != ROLE_ADMIN:
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    season_slug = (request.form.get("season_slug") or "").strip().lower()
    from_team_id = (request.form.get("from_team_id") or "").strip()
    to_team_id = (request.form.get("to_team_id") or "").strip()
    amount = _safe_int((request.form.get("amount") or "").strip(), -1)
    comment = (request.form.get("comment") or "").strip()

    if not season_slug:
        return jsonify({"ok": False, "error": "Season is required"}), 400
    if not from_team_id or not to_team_id:
        return jsonify({"ok": False, "error": "Both source and target teams are required"}), 400
    if from_team_id == to_team_id:
        return jsonify({"ok": False, "error": "Source and target teams must be different"}), 400
    if amount <= 0:
        return jsonify({"ok": False, "error": "Amount must be a positive integer"}), 400
    if not comment:
        return jsonify({"ok": False, "error": "Comment is required"}), 400

    season_store_manager = current_app.extensions["season_store_manager"]
    if not season_store_manager.has_season(season_slug):
        return jsonify({"ok": False, "error": "Season not found"}), 404

    store = season_store_manager.get_store(season_slug, create=False)
    Team = Query()

    with store.write() as db:
        teams = db.table("teams")
        from_team = teams.get(Team.id == from_team_id)
        to_team = teams.get(Team.id == to_team_id)
        if not from_team or not to_team:
            return jsonify({"ok": False, "error": "One or more teams were not found"}), 404

        from_team_name = (from_team.get("name") or from_team_id).strip() or from_team_id
        to_team_name = (to_team.get("name") or to_team_id).strip() or to_team_id
        from_purse = _safe_int(from_team.get("purse_remaining"), 0)
        to_purse = _safe_int(to_team.get("purse_remaining"), 0)

        from_after = from_purse - amount
        to_after = to_purse + amount

        teams.update({"purse_remaining": from_after}, Team.id == from_team_id)
        teams.update({"purse_remaining": to_after}, Team.id == to_team_id)
        _finance_log_transaction(
            db,
            {
                "type": "transfer",
                "season_slug": season_slug,
                "created_by": (user.get("username") or "admin").strip() or "admin",
                "amount": amount,
                "comment": comment,
                "from_team_id": from_team_id,
                "from_team_name": from_team_name,
                "to_team_id": to_team_id,
                "to_team_name": to_team_name,
                "from_before_purse": from_purse,
                "from_after_purse": from_after,
                "to_before_purse": to_purse,
                "to_after_purse": to_after,
            },
        )

    return jsonify(
        {
            "ok": True,
            "season_slug": season_slug,
            "from_team_id": from_team_id,
            "to_team_id": to_team_id,
            "amount": amount,
            "team_rows": _finance_team_rows(season_slug),
        }
    )


@admin_bp.post("/create-manager")
@login_required(role=ROLE_ADMIN)
def create_manager():
    auth_service = current_app.extensions["auth_service"]
    auth_created_for = None
    try:
        _ensure_setup_phase()
        team_name = request.form.get("team_name", "").strip()
        manager_player_id = request.form.get("manager_player_id", "").strip()

        if not team_name:
            raise ValueError("Team name is required")
        if not manager_player_id:
            raise ValueError("Select a manager player")

        credentials_result = auth_service.create_team_credentials(team_name=team_name, display_name=team_name)
        username = credentials_result["username"]
        auth_created_for = username

        team_id = secrets.token_hex(8)
        auction_store = current_app.extensions["auction_store"]
        with auction_store.write() as db:
            auction_users = db.table("users")
            teams = db.table("teams")
            players = db.table("players")
            Team = Query()
            Player = Query()

            if auction_users.get(lambda u: u.get("username") == username):
                raise ValueError("Username already exists")

            if teams.get(Team.name == team_name):
                raise ValueError("Team name already exists")

            manager_player = players.get(Player.id == manager_player_id)
            if not manager_player:
                raise ValueError("Selected manager player not found")

            if teams.get(Team.manager_player_id == manager_player_id):
                raise ValueError("Selected manager player is already assigned to a team")

            if (manager_player.get("status") or "").strip().lower() != "unsold":
                raise ValueError("Selected manager player is not available")

            manager_tier = (manager_player.get("tier") or "silver").strip().lower()
            manager_speciality = (manager_player.get("speciality") or "ALL_ROUNDER").strip().upper()

            auction_users.insert(
                {
                    "username": username,
                    "role": ROLE_MANAGER,
                    "display_name": team_name,
                    "speciality": manager_speciality,
                    "team_id": team_id,
                }
            )

            teams.insert(
                {
                    "id": team_id,
                    "name": team_name,
                    "is_active": True,
                    "manager_username": username,
                    "manager_tier": manager_tier,
                    "manager_player_id": manager_player_id,
                    "players": [],
                    "bench": [],
                    "spent": 0,
                    "purse_remaining": None,
                    "credits_remaining": None,
                }
            )

            players.update(
                {
                    "status": "sold",
                    "sold_to": team_id,
                    "sold_price": 0,
                    "phase_sold": None,
                    "current_bid": 0,
                    "current_bidder_team_id": None,
                    "manager_team_id": team_id,
                },
                Player.id == manager_player_id,
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


@admin_bp.post("/update-team")
@login_required(role=ROLE_ADMIN)
def update_team():
    team_id = request.form.get("team_id", "").strip()
    team_name = request.form.get("team_name", "").strip()
    manager_player_id = request.form.get("manager_player_id", "").strip()

    if not team_id:
        return jsonify({"ok": False, "error": "Missing team id"}), 400
    if not team_name:
        return jsonify({"ok": False, "error": "Team name is required"}), 400

    try:
        _ensure_setup_phase()
        Team = Query()
        Player = Query()
        User = Query()
        store = current_app.extensions["auction_store"]
        auction_service = current_app.extensions["auction_service"]
        auth_service = current_app.extensions["auth_service"]

        linked_username = None
        with store.write() as db:
            teams = db.table("teams")
            players = db.table("players")
            users = db.table("users")
            if not teams.get(Team.id == team_id):
                return jsonify({"ok": False, "error": "Team not found"}), 404

            team = teams.get(Team.id == team_id)
            previous_manager_player_id = (team.get("manager_player_id") or "").strip()
            selected_manager_player_id = manager_player_id or previous_manager_player_id
            if not selected_manager_player_id:
                return jsonify({"ok": False, "error": "Manager player is required"}), 400

            selected_manager = players.get(Player.id == selected_manager_player_id)
            if not selected_manager:
                return jsonify({"ok": False, "error": "Selected manager player not found"}), 404

            if selected_manager_player_id != previous_manager_player_id:
                conflict = teams.get((Team.manager_player_id == selected_manager_player_id) & (Team.id != team_id))
                if conflict:
                    return jsonify({"ok": False, "error": "Selected manager player already belongs to another team"}), 400

                if (selected_manager.get("status") or "").strip().lower() != "unsold":
                    return jsonify({"ok": False, "error": "Selected manager player is not available"}), 400

                if previous_manager_player_id:
                    players.update(
                        {
                            "status": "unsold",
                            "sold_to": None,
                            "sold_price": 0,
                            "phase_sold": None,
                            "current_bid": 0,
                            "current_bidder_team_id": None,
                            "manager_team_id": None,
                        },
                        Player.id == previous_manager_player_id,
                    )

                players.update(
                    {
                        "status": "sold",
                        "sold_to": team_id,
                        "sold_price": 0,
                        "phase_sold": None,
                        "current_bid": 0,
                        "current_bidder_team_id": None,
                        "manager_team_id": team_id,
                    },
                    Player.id == selected_manager_player_id,
                )

            manager_tier = (selected_manager.get("tier") or "silver").strip().lower()
            manager_speciality = (selected_manager.get("speciality") or "ALL_ROUNDER").strip().upper()

            teams.update(
                {
                    "name": team_name,
                    "manager_tier": manager_tier,
                    "manager_player_id": selected_manager_player_id,
                },
                Team.id == team_id,
            )

            linked_user = users.get(User.team_id == team_id)
            if linked_user:
                linked_username = (linked_user.get("username") or "").strip()
                users.update(
                    {
                        "display_name": team_name,
                        "speciality": manager_speciality,
                    },
                    User.team_id == team_id,
                )

        if linked_username:
            auth_service.update_user(
                current_username=linked_username,
                new_username=linked_username,
                display_name=team_name,
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

            manager_player_id = (team.get("manager_player_id") or "").strip()
            all_team_player_ids = team.get("players", []) + team.get("bench", [])
            if manager_player_id:
                all_team_player_ids.append(manager_player_id)

            for pid in all_team_player_ids:
                players.update(
                    {
                        "status": "unsold",
                        "sold_to": None,
                        "sold_price": 0,
                        "phase_sold": None,
                        "current_bid": 0,
                        "current_bidder_team_id": None,
                        "nominated_phase_a": False,
                        "manager_team_id": None,
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


@admin_bp.post("/set-team-participation")
@login_required(role=ROLE_ADMIN)
def set_team_participation():
    team_id = request.form.get("team_id", "").strip()
    desired_state = (request.form.get("is_active", "").strip().lower() in {"1", "true", "yes", "on"})

    if not team_id:
        return jsonify({"ok": False, "error": "Missing team id"}), 400

    try:
        _ensure_setup_phase()
        Team = Query()
        store = current_app.extensions["auction_store"]
        auction_service = current_app.extensions["auction_service"]

        with store.write() as db:
            teams = db.table("teams")
            team = teams.get(Team.id == team_id)
            if not team:
                return jsonify({"ok": False, "error": "Team not found"}), 404

            teams.update({"is_active": desired_state}, Team.id == team_id)

        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True, "team_id": team_id, "is_active": desired_state})
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
    saved_at = datetime.utcnow().isoformat()
    raw_tables = store.export_tables()

    global_sync = {}
    global_league_service = current_app.extensions.get("global_league_service")
    if global_league_service:
        synced_tables, global_sync = global_league_service.apply_global_ids(
            slug,
            raw_tables,
            published_at=saved_at,
        )
    else:
        synced_tables = raw_tables

    payload = {
        "session_name": requested_name or slug,
        "session_link_suffix": slug,
        "saved_at": saved_at,
        "published": True,
        "tables": synced_tables,
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
            "global_sync": global_sync,
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
