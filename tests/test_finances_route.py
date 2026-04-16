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

    stub.SocketIO = _SocketIOStub
    sys.modules["flask_socketio"] = stub

from app import create_app
from app.config import Config


class FinancesRouteTests(unittest.TestCase):
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

    def _seed_season_teams(self):
        manager = self.app.extensions["season_store_manager"]

        season1_store = manager.get_store("season-1", create=True)
        with season1_store.write() as db:
            db.table("season_meta").upsert(
                {
                    "slug": "season-1",
                    "name": "Season 1",
                    "published_at": 100,
                },
                lambda row: True,
            )
            db.table("teams").insert_multiple(
                [
                    {
                        "id": "t1",
                        "name": "Lions",
                        "purse_remaining": 110,
                        "credits_remaining": 4,
                        "players": [{"id": "p1"}, {"id": "p2"}],
                        "bench": [{"id": "b1"}],
                    },
                    {
                        "id": "t2",
                        "name": "Tigers",
                        "purse_remaining": 95,
                        "credits_remaining": 6,
                        "players": [{"id": "p3"}],
                        "bench": [],
                    },
                ]
            )
            db.table("finance_transactions").insert_multiple(
                [
                    {
                        "created_at": "2026-04-16T10:00:00Z",
                        "season_slug": "season-1",
                        "type": "adjust",
                        "operation": "add",
                        "team_id": "t1",
                        "team_name": "Lions",
                        "amount": 20,
                        "comment": "Bonus for fair play",
                        "created_by": "admin",
                    },
                    {
                        "created_at": "2026-04-16T10:10:00Z",
                        "season_slug": "season-1",
                        "type": "transfer",
                        "from_team_id": "t1",
                        "from_team_name": "Lions",
                        "to_team_id": "t2",
                        "to_team_name": "Tigers",
                        "amount": 15,
                        "comment": "Trade correction",
                        "created_by": "admin",
                    },
                ]
            )

        season2_store = manager.get_store("season-2", create=True)
        with season2_store.write() as db:
            db.table("season_meta").upsert(
                {
                    "slug": "season-2",
                    "name": "Season 2",
                    "published_at": 200,
                },
                lambda row: True,
            )
            db.table("teams").insert(
                {
                    "id": "t9",
                    "name": "Sharks",
                    "purse_remaining": 130,
                    "credits_remaining": 5,
                    "players": [],
                    "bench": [],
                }
            )

    def test_finances_route_shows_selected_season_budget_board(self):
        self._seed_season_teams()

        response = self.client.get("/finances/season-1")
        self.assertEqual(response.status_code, 200)

        html = response.get_data(as_text=True)
        self.assertIn("Budget Board: season-1", html)
        self.assertIn("Lions", html)
        self.assertIn("Tigers", html)
        self.assertNotIn("Sharks", html)
        self.assertIn(">110<", html)
        self.assertIn(">95<", html)
        self.assertIn("Transactions: season-1", html)
        self.assertIn("Add", html)
        self.assertIn("Transfer", html)
        self.assertIn("Lions to Tigers", html)
        self.assertIn("Bonus for fair play", html)
        self.assertIn("Trade correction", html)
        self.assertNotIn("Credits Remaining", html)
        self.assertNotIn("Active Players", html)
        self.assertNotIn("Bench Players", html)

    def test_finances_home_defaults_to_latest_published_season(self):
        self._seed_season_teams()

        response = self.client.get("/finances")
        self.assertEqual(response.status_code, 200)

        html = response.get_data(as_text=True)
        self.assertIn("Budget Board: season-2", html)
        self.assertIn("Sharks", html)
        self.assertNotIn("Lions", html)


if __name__ == "__main__":
    unittest.main()
