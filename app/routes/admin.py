import json
from datetime import datetime
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
)

admin_bp = Blueprint("admin", __name__, url_prefix="/auction/admin")


def _ensure_setup_phase():
    state = current_app.extensions["auction_service"].get_state()
    if state.get("phase") != PHASE_SETUP:
        raise ValueError("This action is only allowed during setup phase")


def _session_dir() -> Path:
    return resolve_named_directory(current_app, "SESSION_DIR", "sessions")


def _published_session_dir() -> Path:
    return resolve_named_directory(current_app, "PUBLISHED_SESSION_DIR", "published_sessions")


def _slugify_session_name(name: str) -> str:
    return slugify_session_name(name)


def _resolve_session_file(filename: str) -> Path:
    return resolve_session_file(_session_dir(), filename)


def _resolve_published_file(filename: str) -> Path:
    return resolve_session_file(_published_session_dir(), filename)


@admin_bp.get("/login")
def admin_login_page():
    return render_template("admin/login.html")


@admin_bp.post("/login")
def admin_login():
    auth_service = current_app.extensions["auth_service"]
    user = auth_service.login(request.form.get("username", ""), request.form.get("password", ""))
    if not user or user["role"] != ROLE_ADMIN:
        flash("Invalid admin credentials", "error")
        return redirect(url_for("admin.admin_login_page"))
    session["user"] = user
    return redirect(url_for("admin.dashboard"))


@admin_bp.get("/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("viewer.live_view"))


@admin_bp.get("/dashboard")
@login_required(role=ROLE_ADMIN)
def dashboard():
    auction_service = current_app.extensions["auction_service"]
    return render_template("admin/dashboard.html", state=auction_service.get_state())


@admin_bp.post("/create-manager")
@login_required(role=ROLE_ADMIN)
def create_manager():
    auth_service = current_app.extensions["auth_service"]
    try:
        result = auth_service.create_manager(
            username=request.form.get("username", "").strip(),
            display_name=request.form.get("display_name", "").strip(),
            team_name=request.form.get("team_name", "").strip(),
            manager_tier=request.form.get("manager_tier", "silver").strip().lower(),
        )
        current_app.extensions["auction_service"].setup_team_budgets()
        socketio.emit("state_update", current_app.extensions["auction_service"].get_state())
        return jsonify({"ok": True, **result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@admin_bp.post("/add-player")
@login_required(role=ROLE_ADMIN)
def add_player():
    tier = request.form.get("tier", "silver").strip().lower()
    name = request.form.get("name", "").strip()
    auction_service = current_app.extensions["auction_service"]
    store = current_app.extensions["store"]

    from app.rules import TIER_BASE_PRICE, TIER_CREDIT_COST

    try:
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

    if not player_id:
        return jsonify({"ok": False, "error": "Missing player id"}), 400
    if not name:
        return jsonify({"ok": False, "error": "Player name is required"}), 400

    from app.rules import TIER_BASE_PRICE, TIER_CREDIT_COST

    if tier not in TIER_BASE_PRICE:
        return jsonify({"ok": False, "error": "Invalid tier"}), 400

    try:
        _ensure_setup_phase()
        Player = Query()
        store = current_app.extensions["store"]
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
        store = current_app.extensions["store"]
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

    if not manager_username:
        return jsonify({"ok": False, "error": "Missing manager username"}), 400
    if not username:
        return jsonify({"ok": False, "error": "Username is required"}), 400
    if not display_name:
        return jsonify({"ok": False, "error": "Display name is required"}), 400

    try:
        _ensure_setup_phase()
        User = Query()
        Team = Query()
        store = current_app.extensions["store"]
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
                },
                User.username == manager_username,
            )

            teams.update(
                {"manager_username": username},
                Team.manager_username == manager_username,
            )

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
        store = current_app.extensions["store"]
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
        store = current_app.extensions["store"]
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
        store = current_app.extensions["store"]
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
            users.remove(User.team_id == team_id)
            teams.remove(Team.id == team_id)

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
    session_files = sorted(
        _session_dir().glob("*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    sessions = []
    for session_file in session_files:
        saved_at = datetime.utcfromtimestamp(session_file.stat().st_mtime).isoformat()
        sessions.append(
            {
                "file": session_file.name,
                "label": session_file.stem,
                "saved_at": saved_at,
            }
        )
    return jsonify({"ok": True, "sessions": sessions})


@admin_bp.post("/session/save")
@login_required(role=ROLE_ADMIN)
def save_session():
    requested_name = request.form.get("session_name", "").strip()
    overwrite = request.form.get("overwrite", "").strip().lower() in {"1", "true", "yes", "on"}
    slug = _slugify_session_name(requested_name)
    file_path = _resolve_session_file(f"{slug}.json")
    existed = file_path.exists()
    if existed and not overwrite:
        return jsonify({"ok": False, "error": "A session with this name already exists"}), 400

    store = current_app.extensions["store"]
    payload = {
        "session_name": requested_name or slug,
        "saved_at": datetime.utcnow().isoformat(),
        "tables": store.export_tables(),
    }
    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return jsonify({"ok": True, "file": file_path.name, "overwritten": existed})


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

    store = current_app.extensions["store"]
    payload = {
        "session_name": requested_name or slug,
        "session_link_suffix": slug,
        "saved_at": datetime.utcnow().isoformat(),
        "published": True,
        "tables": store.export_tables(),
    }
    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
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
        file_path = _resolve_session_file(filename)
        if not file_path.exists():
            return jsonify({"ok": False, "error": "Session not found"}), 404

        payload = json.loads(file_path.read_text(encoding="utf-8"))
        tables = payload.get("tables")
        if not isinstance(tables, dict):
            return jsonify({"ok": False, "error": "Invalid session file format"}), 400

        store = current_app.extensions["store"]
        auth_service = current_app.extensions["auth_service"]
        auction_service = current_app.extensions["auction_service"]

        store.import_tables(tables)
        auth_service.seed_admin_if_missing()
        auction_service.bootstrap_defaults()

        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True, "loaded": filename})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 500
