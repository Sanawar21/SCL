from flask import Flask
from flask_socketio import SocketIO

from app.config import Config
from app.db import LockedTinyDB
from app.services.auth_service import AuthService
from app.services.auction_service import AuctionService

socketio = SocketIO(async_mode="threading")


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    store = LockedTinyDB(app.config["DB_PATH"])
    auth_service = AuthService(store)
    auction_service = AuctionService(store)

    auth_service.seed_admin_if_missing()
    auction_service.bootstrap_defaults()

    app.extensions["store"] = store
    app.extensions["auth_service"] = auth_service
    app.extensions["auction_service"] = auction_service

    from app.routes.admin import admin_bp
    from app.routes.manager import manager_bp
    from app.routes.viewer import viewer_bp

    app.register_blueprint(admin_bp)
    app.register_blueprint(manager_bp)
    app.register_blueprint(viewer_bp)

    socketio.init_app(app)

    return app
