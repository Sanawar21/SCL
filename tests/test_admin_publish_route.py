import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

if "flask_socketio" not in sys.modules:
    stub = types.ModuleType("flask_socketio")

    class _SocketIOStub:
        def __init__(self, *args, **kwargs):
            pass

        def init_app(self, *args, **kwargs):
            pass

        def emit(self, *args, **kwargs):
            pass

    stub.SocketIO = _SocketIOStub
    sys.modules["flask_socketio"] = stub

from app import create_app
from app.config import Config
from app.rules import PHASE_COMPLETE


class AdminPublishRouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = Path(self.temp_dir.name)

        self.paths = {
            "auction": root / "data" / "auction_live_db.json",
            "auth": root / "data" / "global_auth_db.json",
            "global": root / "data" / "global_league_db.json",
            "snapshots": root / "data" / "auction_snapshots",
            "published": root / "published_sessions",
            "season": root / "data" / "season_dbs",
            "sessions": root / "sessions",
            "legacy_snapshot": root / "data" / "auction_snapshots_db.json",
            "scorer": root / "data" / "scorer_config.json",
        }

        self.paths["auction"].parent.mkdir(parents=True, exist_ok=True)
        self.paths["auth"].parent.mkdir(parents=True, exist_ok=True)
        self.paths["global"].parent.mkdir(parents=True, exist_ok=True)
        self.paths["snapshots"].mkdir(parents=True, exist_ok=True)
        self.paths["published"].mkdir(parents=True, exist_ok=True)
        self.paths["season"].mkdir(parents=True, exist_ok=True)
        self.paths["sessions"].mkdir(parents=True, exist_ok=True)

        self._config_keys = [
            "AUCTION_DB_PATH",
            "AUTH_DB_PATH",
            "GLOBAL_LEAGUE_DB_PATH",
            "SNAPSHOT_DIR",
            "PUBLISHED_SESSION_DIR",
            "SEASON_DB_DIR",
            "SESSION_DIR",
            "LEGACY_SNAPSHOT_DB_PATH",
            "SCORER_CONFIG_PATH",
            "SECRET_KEY",
        ]
        self._old_config = {key: getattr(Config, key) for key in self._config_keys}

        Config.AUCTION_DB_PATH = str(self.paths["auction"])
        Config.AUTH_DB_PATH = str(self.paths["auth"])
        Config.GLOBAL_LEAGUE_DB_PATH = str(self.paths["global"])
        Config.SNAPSHOT_DIR = str(self.paths["snapshots"])
        Config.PUBLISHED_SESSION_DIR = str(self.paths["published"])
        Config.SEASON_DB_DIR = str(self.paths["season"])
        Config.SESSION_DIR = str(self.paths["sessions"])
        Config.LEGACY_SNAPSHOT_DB_PATH = str(self.paths["legacy_snapshot"])
        Config.SCORER_CONFIG_PATH = str(self.paths["scorer"])
        Config.SECRET_KEY = "test-secret"

        self.app = create_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

        with self.app.extensions["auction_store"].write() as db:
            db.table("players").truncate()
            db.table("teams").truncate()
            db.table("users").truncate()
            db.table("bids").truncate()
            db.table("meta").update({"phase": PHASE_COMPLETE, "current_player_id": None}, doc_ids=[1])

            db.table("players").insert_multiple(
                [
                    {
                        "id": "p-mgr-1",
                        "name": "Manager One",
                        "tier": "gold",
                        "base_price": 800,
                        "status": "sold",
                        "sold_to": "t1",
                        "sold_price": 0,
                        "phase_sold": None,
                        "credits": 2,
                        "current_bid": 0,
                        "current_bidder_team_id": None,
                        "nominated_phase_a": False,
                        "speciality": "ALL_ROUNDER",
                        "manager_team_id": "t1",
                    },
                    {
                        "id": "p-1",
                        "name": "Player One",
                        "tier": "silver",
                        "base_price": 400,
                        "status": "sold",
                        "sold_to": "t1",
                        "sold_price": 400,
                        "phase_sold": "phase_a_silver_gold",
                        "credits": 1,
                        "current_bid": 400,
                        "current_bidder_team_id": "t1",
                        "nominated_phase_a": True,
                        "speciality": "BATTER",
                    },
                ]
            )
            db.table("teams").insert(
                {
                    "id": "t1",
                    "name": "Alpha Team",
                    "manager_username": "alpha-team",
                    "manager_tier": "gold",
                    "manager_player_id": "p-mgr-1",
                    "players": ["p-1"],
                    "bench": [],
                    "spent": 400,
                    "purse_remaining": 1600,
                    "credits_remaining": 7,
                }
            )
            db.table("users").insert(
                {
                    "username": "alpha-team",
                    "role": "manager",
                    "display_name": "Alpha Team",
                    "speciality": "ALL_ROUNDER",
                    "team_id": "t1",
                }
            )

        with self.client.session_transaction() as sess:
            sess["user"] = {"username": "admin", "role": "admin", "display_name": "Administrator"}

    def tearDown(self):
        for ext_name in ("auction_store", "auth_store", "global_league_store"):
            store = self.app.extensions.get(ext_name)
            if store and getattr(store, "db", None):
                store.db.close()

        season_store_manager = self.app.extensions.get("season_store_manager")
        if season_store_manager:
            for store in season_store_manager._stores.values():
                if store and getattr(store, "db", None):
                    store.db.close()

        for key, old_value in self._old_config.items():
            setattr(Config, key, old_value)
        self.temp_dir.cleanup()

    def test_publish_session_returns_global_sync_and_writes_global_ids(self):
        response = self.client.post(
            "/auction/admin/publish-session",
            data={
                "session_name": "Season Test",
                "session_link_suffix": "season-test",
                "overwrite": "true",
            },
        )

        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        body = response.get_json()
        self.assertTrue(body["ok"])

        sync = body.get("global_sync") or {}
        self.assertEqual(sync.get("players_linked"), 2)
        self.assertEqual(sync.get("teams_linked"), 1)

        published_dir = Path(self.app.config["PUBLISHED_SESSION_DIR"])
        published_file = published_dir / body["file"]
        self.assertTrue(published_file.exists())

        payload = json.loads(published_file.read_text(encoding="utf-8"))
        team_row = payload["tables"]["teams"][0]
        self.assertEqual(team_row.get("manager_player_id"), "p-mgr-1")
        self.assertTrue((team_row.get("global_team_id") or "").strip())

        global_tables = self.app.extensions["global_league_store"].export_tables()
        self.assertGreaterEqual(len(global_tables.get("global_players", [])), 2)
        self.assertGreaterEqual(len(global_tables.get("global_teams", [])), 1)


if __name__ == "__main__":
    unittest.main()
