from flask import Blueprint, current_app, flash, jsonify, redirect, render_template, request, session, url_for
from datetime import datetime

from app.authz import login_required
from app.rules import ROLE_ADMIN

fantasy_bp = Blueprint("fantasy", __name__, url_prefix="/fantasy")


def _fantasy_service():
    return current_app.extensions["fantasy_service"]


@fantasy_bp.get("/")
def fantasy_home():
    fantasy_service = _fantasy_service()
    seasons = fantasy_service.list_fantasy_seasons()
    return render_template("fantasy/index.html", seasons=seasons)


@fantasy_bp.get("/points-system")
def fantasy_points_system():
    return render_template("fantasy/points_system.html")


@fantasy_bp.get("/<season_slug>")
def fantasy_season(season_slug):
    fantasy_service = _fantasy_service()
    season = fantasy_service.get_season(season_slug)
    if not season:
        flash("Fantasy season not found", "error")
        return redirect(url_for("fantasy.fantasy_home"))

    rankings = fantasy_service.get_rankings(season_slug)

    def format_submitted_at(raw_value: str) -> str:
        if not raw_value:
            return "-"
        try:
            parsed = datetime.fromisoformat(raw_value)
        except ValueError:
            return raw_value
        return parsed.strftime("%d %b %Y, %I:%M %p")

    return render_template(
        "fantasy/season.html",
        season=season,
        entries=rankings["entries"],
        team_rankings=rankings["team_rankings"],
        player_rankings=rankings["player_rankings"],
        format_submitted_at=format_submitted_at,
    )


@fantasy_bp.get("/<season_slug>/submit")
def fantasy_submit_page(season_slug):
    fantasy_service = _fantasy_service()
    season = fantasy_service.get_season(season_slug)
    if not season:
        flash("Fantasy season not found", "error")
        return redirect(url_for("fantasy.fantasy_home"))

    players = fantasy_service.get_season_players(season_slug)
    entrant_names = fantasy_service.get_eligible_entrant_names(season_slug)
    return render_template(
        "fantasy/submit.html",
        season=season,
        players=players,
        entrant_names=entrant_names,
    )


@fantasy_bp.post("/submit/<season_slug>")
def fantasy_submit_form(season_slug):
    fantasy_service = _fantasy_service()
    try:
        fantasy_service.submit_entry(
            season_slug=season_slug,
            entrant_name=request.form.get("entrant_name", ""),
            player_ids=request.form.getlist("player_ids"),
        )
        flash("Fantasy team submitted successfully", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")
        return redirect(url_for("fantasy.fantasy_submit_page", season_slug=season_slug))
    return redirect(url_for("fantasy.fantasy_season", season_slug=season_slug))


@fantasy_bp.post("/api/submit/<season_slug>")
def fantasy_submit_api(season_slug):
    fantasy_service = _fantasy_service()
    payload = request.get_json(silent=True) or {}
    try:
        entry = fantasy_service.submit_entry(
            season_slug=season_slug,
            entrant_name=payload.get("entrant_name", ""),
            player_ids=payload.get("player_ids") or [],
        )
        return jsonify({"ok": True, "entry": entry})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@fantasy_bp.get("/admin/login")
def fantasy_admin_login_page():
    return redirect(url_for("unified_admin.admin_login_page"))


@fantasy_bp.post("/admin/login")
def fantasy_admin_login():
    auth_service = current_app.extensions["auth_service"]
    user = auth_service.login(request.form.get("username", ""), request.form.get("password", ""))
    if not user or user.get("role") != ROLE_ADMIN:
        flash("Invalid admin credentials", "error")
        return redirect(url_for("unified_admin.admin_login_page"))
    session["user"] = user
    return redirect(url_for("unified_admin.dashboard", tab="fantasy"))


@fantasy_bp.get("/admin/logout")
def fantasy_admin_logout():
    session.clear()
    return redirect(url_for("fantasy.fantasy_home"))


@fantasy_bp.get("/admin/dashboard")
@login_required(role=ROLE_ADMIN)
def fantasy_admin_dashboard():
    season_slug = (request.args.get("season") or "").strip().lower()
    if season_slug:
        return redirect(url_for("unified_admin.dashboard", tab="fantasy", season=season_slug))
    return redirect(url_for("unified_admin.dashboard", tab="fantasy"))


@fantasy_bp.post("/admin/create-season")
@login_required(role=ROLE_ADMIN)
def fantasy_admin_create_season():
    fantasy_service = _fantasy_service()
    published_slug = request.form.get("published_slug", "")
    season_name = request.form.get("season_name", "")

    try:
        season = fantasy_service.create_fantasy_season(published_slug=published_slug, name=season_name)
        flash("Fantasy season created", "success")
        return redirect(url_for("unified_admin.dashboard", tab="fantasy", season=season["slug"]))
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")
        return redirect(url_for("unified_admin.dashboard", tab="fantasy"))


@fantasy_bp.post("/admin/toggle-submissions")
@login_required(role=ROLE_ADMIN)
def fantasy_admin_toggle_submissions():
    fantasy_service = _fantasy_service()
    season_slug = request.form.get("season_slug", "")
    desired_state = (request.form.get("desired_state", "open") or "").strip().lower()
    is_open = desired_state == "open"

    try:
        fantasy_service.set_submissions_open(season_slug, is_open=is_open)
        flash("Submission state updated", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return redirect(url_for("unified_admin.dashboard", tab="fantasy", season=season_slug))


@fantasy_bp.post("/admin/delete-entry")
@login_required(role=ROLE_ADMIN)
def fantasy_admin_delete_entry():
    fantasy_service = _fantasy_service()
    entry_id = request.form.get("entry_id", "")
    season_slug = request.form.get("season_slug", "")

    try:
        fantasy_service.delete_entry(season_slug, entry_id)
        flash("Fantasy entry deleted", "success")
    except Exception as exc:  # noqa: BLE001
        flash(str(exc), "error")

    return redirect(url_for("unified_admin.dashboard", tab="fantasy", season=season_slug))
