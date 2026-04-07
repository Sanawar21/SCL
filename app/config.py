import os


class Config:
    SECRET_KEY = os.environ.get("SCL_SECRET_KEY", "scl-dev-secret")
    DB_PATH = os.environ.get("SCL_DB_PATH", "scl_db.json")
