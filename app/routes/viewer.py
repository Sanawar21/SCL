from flask import Blueprint, current_app, jsonify, render_template

viewer_bp = Blueprint("viewer", __name__)


@viewer_bp.get("/")
def home():
    return render_template("viewer/home.html")


@viewer_bp.get("/viewer/live")
def live_view():
    auction_service = current_app.extensions["auction_service"]
    return render_template("viewer/live.html", state=auction_service.get_state())


@viewer_bp.get("/api/state")
def api_state():
    return jsonify(current_app.extensions["auction_service"].get_state())
