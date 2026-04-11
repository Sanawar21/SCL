from flask import Flask
from flask_socketio import SocketIO

from app.config import Config
from app.db import LockedTinyDB
from app.services.auth_service import AuthService
from app.services.auction_service import AuctionService
from app.services.fantasy_service import FantasyService

socketio = SocketIO(async_mode="threading")


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    store = LockedTinyDB(app.config["DB_PATH"])
    auth_service = AuthService(store)
    auction_service = AuctionService(store)
    fantasy_service = FantasyService(store, app.config["PUBLISHED_SESSION_DIR"])

    auth_service.seed_admin_if_missing()
    auction_service.bootstrap_defaults()

    app.extensions["store"] = store
    app.extensions["auth_service"] = auth_service
    app.extensions["auction_service"] = auction_service
    app.extensions["fantasy_service"] = fantasy_service

    from app.routes.admin import admin_bp
    from app.routes.fantasy import fantasy_bp
    from app.routes.manager import manager_bp
    from app.routes.viewer import viewer_bp

    app.register_blueprint(admin_bp)
    app.register_blueprint(fantasy_bp)
    app.register_blueprint(manager_bp)
    app.register_blueprint(viewer_bp)

    socketio.init_app(app)

    return app
