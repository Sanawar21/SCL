from flask import Blueprint, current_app, flash, jsonify, redirect, render_template, request, session, url_for

from app import socketio
from app.authz import login_required
from app.rules import ROLE_MANAGER

manager_bp = Blueprint("manager", __name__, url_prefix="/manager")


@manager_bp.get("/login")
def login_page():
    return render_template("manager/login.html")


@manager_bp.post("/login")
def login():
    auth_service = current_app.extensions["auth_service"]
    user = auth_service.login(request.form.get("username", ""), request.form.get("password", ""))
    if not user or user["role"] != ROLE_MANAGER:
        flash("Invalid manager credentials", "error")
        return redirect(url_for("manager.login_page"))
    session["user"] = user
    return redirect(url_for("manager.dashboard"))


@manager_bp.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("viewer.live_view"))


@manager_bp.get("/dashboard")
@login_required(role=ROLE_MANAGER)
def dashboard():
    auction_service = current_app.extensions["auction_service"]
    team = auction_service.get_team_by_username(session["user"]["username"])
    return render_template("manager/dashboard.html", state=auction_service.get_state(), my_team=team)


@manager_bp.post("/bid")
@login_required(role=ROLE_MANAGER)
def bid():
    auction_service = current_app.extensions["auction_service"]
    team = auction_service.get_team_by_username(session["user"]["username"])
    try:
        player = auction_service.place_bid(team["id"], int(request.form.get("amount", 0)))
        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True, "player": player})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@manager_bp.post("/pass")
@login_required(role=ROLE_MANAGER)
def pass_turn():
    auction_service = current_app.extensions["auction_service"]
    team = auction_service.get_team_by_username(session["user"]["username"])
    try:
        result = auction_service.pass_current(team["id"])
        socketio.emit("state_update", auction_service.get_state())
        return jsonify(result)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@manager_bp.get("/state")
@login_required(role=ROLE_MANAGER)
def manager_state():
    auction_service = current_app.extensions["auction_service"]
    team = auction_service.get_team_by_username(session["user"]["username"])
    state = auction_service.get_state()
    state["my_team"] = team
    return jsonify(state)


@manager_bp.post("/trade")
@login_required(role=ROLE_MANAGER)
def trade():
    auction_service = current_app.extensions["auction_service"]
    my_team = auction_service.get_team_by_username(session["user"]["username"])
    to_team_id = request.form.get("to_team_id", "").strip()
    offered_player_id = request.form.get("offered_player_id", "").strip()
    requested_player_id = request.form.get("requested_player_id", "").strip() or None

    try:
        result = auction_service.trade_players(
            from_team_id=my_team["id"],
            to_team_id=to_team_id,
            offered_player_id=offered_player_id,
            requested_player_id=requested_player_id,
        )
        socketio.emit("state_update", auction_service.get_state())
        return jsonify({"ok": True, "trade": result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400
