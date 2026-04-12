import os


class Config:
    SECRET_KEY = os.environ.get("SCL_SECRET_KEY", "scl-dev-secret")
    # Legacy setting retained for backward compatibility.
    DB_PATH = os.environ.get("SCL_DB_PATH", "scl_db.json")
    AUTH_DB_PATH = os.environ.get("SCL_AUTH_DB_PATH", "data/global_auth_db.json")
    AUCTION_DB_PATH = os.environ.get("SCL_AUCTION_DB_PATH", "data/auction_live_db.json")
    SNAPSHOT_DIR = os.environ.get("SCL_SNAPSHOT_DIR", "data/auction_snapshots")
    LEGACY_SNAPSHOT_DB_PATH = os.environ.get("SCL_SNAPSHOT_DB_PATH", "data/auction_snapshots_db.json")
    SESSION_DIR = os.environ.get("SCL_SESSION_DIR", "sessions")
    PUBLISHED_SESSION_DIR = os.environ.get("SCL_PUBLISHED_SESSION_DIR", "published_sessions")
    SEASON_DB_DIR = os.environ.get("SCL_SEASON_DB_DIR", "data/season_dbs")
    SCORER_CONFIG_PATH = os.environ.get("SCL_SCORER_CONFIG_PATH", "data/scorer_config.json")
