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


class LeagueTableRouteTests(unittest.TestCase):
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

    def test_table_ranks_by_points_then_nrr_for_selected_season(self):
        rows = [
            {
                "season_slug": "season-1",
                "team_id": "team-a",
                "team_name": "Team A",
                "result": "win",
                "wins": 1,
                "losses": 0,
                "ties": 0,
                "no_results": 0,
                "runs_scored": 30,
                "balls_faced": 18,
                "runs_conceded": 24,
                "balls_bowled": 18,
            },
            {
                "season_slug": "season-1",
                "team_id": "team-b",
                "team_name": "Team B",
                "result": "loss",
                "wins": 0,
                "losses": 1,
                "ties": 0,
                "no_results": 0,
                "runs_scored": 24,
                "balls_faced": 18,
                "runs_conceded": 30,
                "balls_bowled": 18,
            },
            {
                "season_slug": "season-1",
                "team_id": "team-b",
                "team_name": "Team B",
                "result": "win",
                "wins": 1,
                "losses": 0,
                "ties": 0,
                "no_results": 0,
                "runs_scored": 36,
                "balls_faced": 18,
                "runs_conceded": 20,
                "balls_bowled": 18,
            },
            {
                "season_slug": "season-1",
                "team_id": "team-c",
                "team_name": "Team C",
                "result": "loss",
                "wins": 0,
                "losses": 1,
                "ties": 0,
                "no_results": 0,
                "runs_scored": 20,
                "balls_faced": 18,
                "runs_conceded": 36,
                "balls_bowled": 18,
            },
            {
                "season_slug": "season-1",
                "team_id": "team-a",
                "team_name": "Team A",
                "result": "tie",
                "wins": 0,
                "losses": 0,
                "ties": 1,
                "no_results": 0,
                "runs_scored": 24,
                "balls_faced": 18,
                "runs_conceded": 24,
                "balls_bowled": 18,
            },
            {
                "season_slug": "season-1",
                "team_id": "team-c",
                "team_name": "Team C",
                "result": "tie",
                "wins": 0,
                "losses": 0,
                "ties": 1,
                "no_results": 0,
                "runs_scored": 24,
                "balls_faced": 18,
                "runs_conceded": 24,
                "balls_bowled": 18,
            },
            {
                "season_slug": "season-1",
                "team_id": "team-d",
                "team_name": "Team D",
                "result": "win",
                "wins": 1,
                "losses": 0,
                "ties": 0,
                "no_results": 0,
                "runs_scored": 18,
                "balls_faced": 12,
                "runs_conceded": 12,
                "balls_bowled": 12,
            },
            {
                "season_slug": "season-1",
                "team_id": "team-d",
                "team_name": "Team D",
                "result": "tie",
                "wins": 0,
                "losses": 0,
                "ties": 1,
                "no_results": 0,
                "runs_scored": 12,
                "balls_faced": 12,
                "runs_conceded": 12,
                "balls_bowled": 12,
            },
            {
                "season_slug": "season-2",
                "team_id": "team-z",
                "team_name": "Team Z",
                "result": "win",
                "wins": 1,
                "losses": 0,
                "ties": 0,
                "no_results": 0,
                "runs_scored": 100,
                "balls_faced": 30,
                "runs_conceded": 1,
                "balls_bowled": 30,
            },
        ]

        with self.app.extensions["global_league_store"].write() as db:
            db.table("scorer_team_match_stats").insert_multiple(rows)

        response = self.client.get("/table/season-1")
        self.assertEqual(response.status_code, 200)

        html = response.get_data(as_text=True)
        self.assertIn("League Table: season-1", html)
        self.assertIn("Team D", html)
        self.assertIn("Team A", html)
        self.assertIn("Team B", html)
        self.assertIn("Team C", html)
        self.assertNotIn("Team Z", html)

        self.assertLess(html.find("Team D"), html.find("Team A"))
        self.assertLess(html.find("Team A"), html.find("Team B"))
        self.assertLess(html.find("Team B"), html.find("Team C"))


if __name__ == "__main__":
    unittest.main()
