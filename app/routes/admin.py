from flask import Blueprint, current_app, flash, jsonify, redirect, render_template, request, session, url_for

from app import socketio
from app.authz import login_required
from app.rules import (
    PHASE_A_BREAK,
    PHASE_A_P,
    PHASE_A_SG,
    PHASE_B,
    ROLE_ADMIN,
)

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


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


@admin_bp.post("/set-phase")
@login_required(role=ROLE_ADMIN)
def set_phase():
    phase = request.form.get("phase")
    if phase not in {PHASE_A_SG, PHASE_A_BREAK, PHASE_A_P, PHASE_B}:
        return jsonify({"ok": False, "error": "Invalid phase"}), 400
    auction_service = current_app.extensions["auction_service"]
    auction_service.set_phase(phase)
    socketio.emit("state_update", auction_service.get_state())
    return jsonify({"ok": True})


@admin_bp.post("/nominate-next")
@login_required(role=ROLE_ADMIN)
def nominate_next():
    auction_service = current_app.extensions["auction_service"]
    sold_result = None
    state = auction_service.get_state()

    # One-click flow: close current lot first, then nominate next lot.
    if state.get("current_player"):
        sold_result = auction_service.close_current_player()

    player = auction_service.nominate_next_player()
    socketio.emit("state_update", auction_service.get_state())

    if not player and not sold_result:
        return jsonify({"ok": False, "error": "No player available for this phase"}), 400

    return jsonify({"ok": True, "sold_result": sold_result, "player": player})


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


@admin_bp.post("/complete-draft")
@login_required(role=ROLE_ADMIN)
def complete_draft():
    auction_service = current_app.extensions["auction_service"]
    auction_service.complete_phase_b_with_penalties()
    socketio.emit("state_update", auction_service.get_state())
    return jsonify({"ok": True})
