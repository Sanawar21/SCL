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


class LeaderboardRouteTests(unittest.TestCase):
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

    def test_leaderboard_route_renders_global_and_season_stats(self):
        team_alpha = "team-alpha-01"
        team_beta = "team-beta-01"
        alice = "player-alice-01"
        bob = "player-bob-01"
        cara = "player-cara-01"

        with self.app.extensions["global_league_store"].write() as db:
            db.table("scorer_match_stats").insert_multiple(
                [
                    {"match_key": "season-1:m1", "season_slug": "season-1", "match_id": "M1", "include_in_fantasy_points": True},
                    {"match_key": "season-1:m2", "season_slug": "season-1", "match_id": "M2", "include_in_fantasy_points": True},
                    {"match_key": "season-2:m1", "season_slug": "season-2", "match_id": "M1", "include_in_fantasy_points": True},
                ]
            )

            db.table("scorer_player_match_stats").insert_multiple(
                [
                    {
                        "match_key": "season-1:m1",
                        "season_slug": "season-1",
                        "player_id": alice,
                        "player_name": "Alice",
                        "runs": 60,
                        "balls_faced": 20,
                        "dismissed": 1,
                        "fours": 6,
                        "sixes": 4,
                        "wickets": 2,
                        "balls_bowled": 12,
                        "runs_conceded": 18,
                        "fantasy_score": 85,
                    },
                    {
                        "match_key": "season-1:m2",
                        "season_slug": "season-1",
                        "player_id": bob,
                        "player_name": "Bob",
                        "runs": 45,
                        "balls_faced": 15,
                        "dismissed": 1,
                        "fours": 5,
                        "sixes": 1,
                        "wickets": 4,
                        "balls_bowled": 12,
                        "runs_conceded": 24,
                        "fantasy_score": 78,
                    },
                    {
                        "match_key": "season-1:m2",
                        "season_slug": "season-1",
                        "player_id": cara,
                        "player_name": "Cara",
                        "runs": 10,
                        "balls_faced": 5,
                        "dismissed": 1,
                        "fours": 1,
                        "sixes": 0,
                        "wickets": 5,
                        "balls_bowled": 18,
                        "runs_conceded": 30,
                        "fantasy_score": 66,
                    },
                    {
                        "match_key": "season-2:m1",
                        "season_slug": "season-2",
                        "player_id": alice,
                        "player_name": "Alice",
                        "runs": 20,
                        "balls_faced": 10,
                        "dismissed": 1,
                        "fours": 2,
                        "sixes": 1,
                        "wickets": 0,
                        "balls_bowled": 6,
                        "runs_conceded": 15,
                        "fantasy_score": 20,
                    },
                ]
            )

            db.table("scorer_team_match_stats").insert_multiple(
                [
                    {
                        "match_key": "season-1:m1",
                        "season_slug": "season-1",
                        "team_id": team_alpha,
                        "team_name": "Alpha XI",
                        "result": "win",
                        "wins": 1,
                        "losses": 0,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 55,
                        "balls_faced": 18,
                        "runs_conceded": 40,
                        "balls_bowled": 18,
                        "fours": 7,
                        "sixes": 4,
                        "wickets_taken": 2,
                        "fantasy_points": 130,
                    },
                    {
                        "match_key": "season-1:m1",
                        "season_slug": "season-1",
                        "team_id": team_beta,
                        "team_name": "Beta XI",
                        "result": "loss",
                        "wins": 0,
                        "losses": 1,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 40,
                        "balls_faced": 18,
                        "runs_conceded": 55,
                        "balls_bowled": 18,
                        "fours": 3,
                        "sixes": 2,
                        "wickets_taken": 1,
                        "fantasy_points": 80,
                    },
                    {
                        "match_key": "season-1:m2",
                        "season_slug": "season-1",
                        "team_id": team_alpha,
                        "team_name": "Alpha XI",
                        "result": "win",
                        "wins": 1,
                        "losses": 0,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 50,
                        "balls_faced": 18,
                        "runs_conceded": 45,
                        "balls_bowled": 18,
                        "fours": 5,
                        "sixes": 3,
                        "wickets_taken": 3,
                        "fantasy_points": 120,
                    },
                    {
                        "match_key": "season-1:m2",
                        "season_slug": "season-1",
                        "team_id": team_beta,
                        "team_name": "Beta XI",
                        "result": "loss",
                        "wins": 0,
                        "losses": 1,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 45,
                        "balls_faced": 18,
                        "runs_conceded": 50,
                        "balls_bowled": 18,
                        "fours": 4,
                        "sixes": 2,
                        "wickets_taken": 1,
                        "fantasy_points": 82,
                    },
                    {
                        "match_key": "season-2:m1",
                        "season_slug": "season-2",
                        "team_id": team_beta,
                        "team_name": "Beta XI",
                        "result": "win",
                        "wins": 1,
                        "losses": 0,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 35,
                        "balls_faced": 18,
                        "runs_conceded": 30,
                        "balls_bowled": 18,
                        "fours": 4,
                        "sixes": 1,
                        "wickets_taken": 2,
                        "fantasy_points": 90,
                    },
                ]
            )

            db.table("scorer_player_global_stats").insert_multiple(
                [
                    {
                        "player_id": alice,
                        "player_name": "Alice",
                        "matches": 4,
                        "runs": 110,
                        "balls_faced": 30,
                        "dismissed": 2,
                        "fours": 8,
                        "sixes": 5,
                        "wickets": 2,
                        "balls_bowled": 18,
                        "runs_conceded": 33,
                        "fantasy_score": 105,
                    },
                    {
                        "player_id": bob,
                        "player_name": "Bob",
                        "matches": 2,
                        "runs": 45,
                        "balls_faced": 15,
                        "dismissed": 1,
                        "fours": 5,
                        "sixes": 1,
                        "wickets": 8,
                        "balls_bowled": 24,
                        "runs_conceded": 40,
                        "fantasy_score": 88,
                    },
                    {
                        "player_id": cara,
                        "player_name": "Cara",
                        "matches": 2,
                        "runs": 10,
                        "balls_faced": 5,
                        "dismissed": 1,
                        "fours": 1,
                        "sixes": 0,
                        "wickets": 6,
                        "balls_bowled": 24,
                        "runs_conceded": 38,
                        "fantasy_score": 70,
                    },
                ]
            )

            db.table("scorer_team_global_stats").insert_multiple(
                [
                    {
                        "team_id": team_alpha,
                        "team_name": "Alpha XI",
                        "matches": 2,
                        "wins": 2,
                        "losses": 0,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 105,
                        "balls_faced": 36,
                        "runs_conceded": 85,
                        "balls_bowled": 36,
                        "fours": 12,
                        "sixes": 7,
                        "wickets_taken": 5,
                        "fantasy_points": 250,
                        "net_run_rate": 3.33,
                    },
                    {
                        "team_id": team_beta,
                        "team_name": "Beta XI",
                        "matches": 3,
                        "wins": 1,
                        "losses": 2,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 120,
                        "balls_faced": 54,
                        "runs_conceded": 135,
                        "balls_bowled": 54,
                        "fours": 11,
                        "sixes": 5,
                        "wickets_taken": 4,
                        "fantasy_points": 252,
                        "net_run_rate": -1.67,
                    },
                ]
            )

        response = self.client.get("/leaderboard/season-1")
        self.assertEqual(response.status_code, 200)

        html = response.get_data(as_text=True)
        self.assertIn("Season Leaderboards: season-1", html)
        self.assertIn("Most Sixes", html)
        self.assertIn("Most Runs", html)
        self.assertIn("Most Fours", html)
        self.assertIn("Most Boundaries", html)
        self.assertIn("Highest Strike Rate (Min 5 Balls)", html)
        self.assertIn("Best Economy (Min 5 Balls Bowled)", html)
        self.assertIn("Most Wickets", html)
        self.assertIn("Most Fantasy Points", html)
        self.assertIn("Best Fantasy Team", html)
        self.assertIn("Highest Batting Avg (Min 5 Balls)", html)
        self.assertNotIn("Global Leaderboards", html)
        self.assertNotIn("Most Trophies (League Winners)", html)
        self.assertNotIn("Season Champion (Table)", html)

        self.assertIn("Alice", html)
        self.assertIn("Bob", html)
        self.assertIn("Alpha XI", html)
        self.assertIn("Beta XI", html)

        scorer_service = self.app.extensions["scorer_service"]
        alice_slug = scorer_service.player_profile_slug(alice, "Alice")
        alpha_slug = scorer_service.team_profile_slug(team_alpha, "Alpha XI")
        self.assertIn(f"/players/{alice_slug}", html)
        self.assertIn(f"/teams/{alpha_slug}", html)

        home_response = self.client.get("/leaderboard?scope=global")
        self.assertEqual(home_response.status_code, 200)
        home_html = home_response.get_data(as_text=True)
        self.assertIn("Global Leaderboards", home_html)
        self.assertIn("Best Economy (Min 5 Balls Bowled)", home_html)
        self.assertNotIn("Season Leaderboards:", home_html)
        self.assertNotIn("Most Trophies (League Winners)", home_html)
        self.assertNotIn("Season Champion (Table)", home_html)


if __name__ == "__main__":
    unittest.main()
