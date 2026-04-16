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


class TeamAndPlayerRoutesTests(unittest.TestCase):
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

    def test_teams_and_player_profiles_show_global_and_split_stats(self):
        team_alpha_id = "team-alpha-12345678"
        team_beta_id = "team-beta-12345678"
        alice_id = "player-alice-12345678"
        bob_id = "player-bob-12345678"
        cara_id = "player-cara-12345678"

        with self.app.extensions["global_league_store"].write() as db:
            db.table("global_teams").insert_multiple(
                [
                    {"id": team_alpha_id, "name": "Alpha XI"},
                    {"id": team_beta_id, "name": "Beta XI"},
                ]
            )

            db.table("global_players").insert_multiple(
                [
                    {"id": alice_id, "name": "Alice", "tier": "platinum", "speciality": "ALL_ROUNDER"},
                    {"id": bob_id, "name": "Bob", "tier": "gold", "speciality": "BATTER"},
                    {"id": cara_id, "name": "Cara", "tier": "silver", "speciality": "BOWLER"},
                ]
            )

            db.table("season_team_links").insert_multiple(
                [
                    {
                        "season_slug": "season-1",
                        "global_team_id": team_alpha_id,
                        "team_name": "Alpha XI",
                        "manager_global_player_id": alice_id,
                    },
                    {
                        "season_slug": "season-2",
                        "global_team_id": team_alpha_id,
                        "team_name": "Alpha XI",
                        "manager_global_player_id": alice_id,
                    },
                ]
            )

            db.table("season_team_rosters").insert_multiple(
                [
                    {
                        "season_slug": "season-1",
                        "global_team_id": team_alpha_id,
                        "global_player_ids": [alice_id, bob_id],
                    },
                    {
                        "season_slug": "season-2",
                        "global_team_id": team_alpha_id,
                        "global_player_ids": [alice_id, cara_id],
                    },
                ]
            )

            db.table("scorer_match_stats").insert_multiple(
                [
                    {"match_key": "season-1:m1", "season_slug": "season-1", "include_in_fantasy_points": True},
                    {"match_key": "season-2:m1", "season_slug": "season-2", "include_in_fantasy_points": False},
                ]
            )

            db.table("scorer_team_match_stats").insert_multiple(
                [
                    {
                        "match_key": "season-1:m1",
                        "season_slug": "season-1",
                        "team_id": team_alpha_id,
                        "team_name": "Alpha XI",
                        "result": "win",
                        "wins": 1,
                        "losses": 0,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 42,
                        "balls_faced": 24,
                        "runs_conceded": 30,
                        "balls_bowled": 24,
                        "wickets_taken": 3,
                        "wickets_lost": 2,
                        "fantasy_points": 120,
                    },
                    {
                        "match_key": "season-2:m1",
                        "season_slug": "season-2",
                        "team_id": team_alpha_id,
                        "team_name": "Alpha XI",
                        "result": "loss",
                        "wins": 0,
                        "losses": 1,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 21,
                        "balls_faced": 18,
                        "runs_conceded": 24,
                        "balls_bowled": 18,
                        "wickets_taken": 1,
                        "wickets_lost": 4,
                        "fantasy_points": 80,
                    },
                ]
            )

            db.table("scorer_team_global_stats").insert_multiple(
                [
                    {
                        "team_id": team_alpha_id,
                        "team_name": "Alpha XI",
                        "matches": 2,
                        "wins": 1,
                        "losses": 1,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 63,
                        "runs_conceded": 54,
                        "net_run_rate": 0.5,
                        "fantasy_points": 120,
                    },
                    {
                        "team_id": team_beta_id,
                        "team_name": "Beta XI",
                        "matches": 0,
                        "wins": 0,
                        "losses": 0,
                        "ties": 0,
                        "no_results": 0,
                        "runs_scored": 0,
                        "runs_conceded": 0,
                        "net_run_rate": 0.0,
                        "fantasy_points": 0,
                    },
                ]
            )

            db.table("scorer_player_match_stats").insert_multiple(
                [
                    {
                        "match_key": "season-1:m1",
                        "season_slug": "season-1",
                        "team_id": team_alpha_id,
                        "team_name": "Alpha XI",
                        "player_id": alice_id,
                        "player_name": "Alice",
                        "runs": 24,
                        "balls_faced": 12,
                        "dismissed": 1,
                        "wickets": 2,
                        "balls_bowled": 6,
                        "runs_conceded": 11,
                        "fantasy_score": 44,
                    },
                    {
                        "match_key": "season-2:m1",
                        "season_slug": "season-2",
                        "team_id": team_alpha_id,
                        "team_name": "Alpha XI",
                        "player_id": alice_id,
                        "player_name": "Alice",
                        "runs": 5,
                        "balls_faced": 4,
                        "dismissed": 1,
                        "wickets": 0,
                        "balls_bowled": 6,
                        "runs_conceded": 18,
                        "fantasy_score": 30,
                    },
                    {
                        "match_key": "season-1:m1",
                        "season_slug": "season-1",
                        "team_id": team_beta_id,
                        "team_name": "Beta XI",
                        "player_id": alice_id,
                        "player_name": "Alice",
                        "runs": 9,
                        "balls_faced": 8,
                        "dismissed": 1,
                        "wickets": 1,
                        "balls_bowled": 0,
                        "runs_conceded": 0,
                        "fantasy_score": 20,
                    },
                ]
            )

            db.table("scorer_player_global_stats").insert(
                {
                    "player_id": alice_id,
                    "player_name": "Alice",
                    "role": "ALL_ROUNDER",
                    "tier": "platinum",
                    "matches": 3,
                    "runs": 38,
                    "wickets": 3,
                    "balls_faced": 24,
                    "balls_bowled": 12,
                    "strike_rate": 158.33,
                    "batting_average": 12.67,
                    "economy": 14.5,
                    "fantasy_score": 64,
                    "fantasy_average": 21,
                }
            )

        scorer_service = self.app.extensions["scorer_service"]
        team_slug = scorer_service.team_profile_slug(team_alpha_id, "Alpha XI")
        player_slug = scorer_service.player_profile_slug(alice_id, "Alice")

        teams_response = self.client.get("/teams")
        self.assertEqual(teams_response.status_code, 200)
        teams_html = teams_response.get_data(as_text=True)
        self.assertIn("Alpha XI", teams_html)
        self.assertIn("Beta XI", teams_html)
        self.assertIn(f"/teams/{team_slug}", teams_html)

        team_response = self.client.get(f"/teams/{team_slug}")
        self.assertEqual(team_response.status_code, 200)
        team_html = team_response.get_data(as_text=True)
        self.assertIn("Global Stats", team_html)
        self.assertIn("Season Wise Stats", team_html)
        self.assertIn("season-1", team_html)
        self.assertIn("season-2", team_html)
        self.assertIn("Season Squads", team_html)
        self.assertIn("Alice", team_html)
        self.assertIn("Bob", team_html)
        self.assertIn("Cara", team_html)

        player_response = self.client.get(f"/players/{player_slug}")
        self.assertEqual(player_response.status_code, 200)
        player_html = player_response.get_data(as_text=True)
        self.assertIn("Global Stats", player_html)
        self.assertIn("Team Wise Stats", player_html)
        self.assertIn("Alpha XI", player_html)
        self.assertIn("Beta XI", player_html)
        self.assertIn("Season Wise Stats", player_html)


if __name__ == "__main__":
    unittest.main()
