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

    stub.SocketIO = _SocketIOStub
    sys.modules["flask_socketio"] = stub

from app.db import LockedTinyDB, SeasonStoreManager
from app.services.fantasy_service import FantasyService


class FantasyServiceRankingsTests(unittest.TestCase):
    def test_get_rankings_uses_only_requested_season_points(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            global_store = LockedTinyDB(str(temp_path / "global_league.json"))
            season_manager = SeasonStoreManager(str(temp_path / "season_dbs"), str(temp_path / "app_root"))
            published_dir = temp_path / "published_sessions"
            published_dir.mkdir(parents=True, exist_ok=True)

            service = FantasyService(global_store, str(published_dir), season_manager)

            season_store = season_manager.get_store("season-1", create=True)
            with season_store.write() as db:
                db.table("teams").insert_multiple(
                    [
                        {"id": "t-local-1", "name": "Naan CC"},
                        {"id": "t-local-2", "name": "Pandiya Associates"},
                    ]
                )
                db.table("players").insert_multiple(
                    [
                        {"id": "p-local-1", "name": "Azen", "tier": "platinum", "speciality": "ALL_ROUNDER", "sold_to": "t-local-1"},
                        {"id": "p-local-2", "name": "Qambar", "tier": "platinum", "speciality": "ALL_ROUNDER", "sold_to": "t-local-1"},
                        {"id": "p-local-3", "name": "Owais", "tier": "platinum", "speciality": "BATTER", "sold_to": "t-local-2"},
                    ]
                )
                db.table("fantasy_entries").insert_multiple(
                    [
                        {
                            "id": "entry-high",
                            "entrant_name": "Entrant A",
                            "entrant_key": "player:entrant a",
                            "created_at": "2026-04-16T01:00:00",
                            "picks": [{"player_id": "p-local-1"}, {"player_id": "p-local-3"}],
                        },
                        {
                            "id": "entry-low",
                            "entrant_name": "Entrant B",
                            "entrant_key": "player:entrant b",
                            "created_at": "2026-04-16T02:00:00",
                            "picks": [{"player_id": "p-local-2"}],
                        },
                        {
                            "id": "entry-low-2",
                            "entrant_name": "Entrant C",
                            "entrant_key": "player:entrant c",
                            "created_at": "2026-04-16T03:00:00",
                            "picks": [{"player_id": "p-local-2"}],
                        },
                    ]
                )

            with global_store.write() as db:
                db.table("season_player_links").insert_multiple(
                    [
                        {"season_slug": "season-1", "local_player_id": "p-local-1", "global_player_id": "g-1"},
                        {"season_slug": "season-1", "local_player_id": "p-local-2", "global_player_id": "g-2"},
                        {"season_slug": "season-1", "local_player_id": "p-local-3", "global_player_id": "g-3"},
                    ]
                )
                db.table("season_team_links").insert_multiple(
                    [
                        {"season_slug": "season-1", "local_team_id": "t-local-1", "global_team_id": "tg-1"},
                        {"season_slug": "season-1", "local_team_id": "t-local-2", "global_team_id": "tg-2"},
                    ]
                )
                db.table("scorer_player_match_stats").insert_multiple(
                    [
                        {"season_slug": "season-1", "player_id": "g-1", "fantasy_score": 40},
                        {"season_slug": "season-1", "player_id": "g-3", "fantasy_score": 15},
                        {"season_slug": "season-2", "player_id": "g-2", "fantasy_score": 70},
                    ]
                )
                db.table("scorer_team_match_stats").insert_multiple(
                    [
                        {"season_slug": "season-1", "team_id": "tg-1", "team_name": "Naan CC", "fantasy_points": 50},
                        {"season_slug": "season-1", "team_id": "tg-2", "team_name": "Pandiya Associates", "fantasy_points": 120},
                        {"season_slug": "season-2", "team_id": "tg-1", "team_name": "Naan CC", "fantasy_points": 999},
                    ]
                )

            rankings = service.get_rankings("season-1")

            self.assertEqual(rankings["entries"][0]["id"], "entry-high")
            self.assertEqual(rankings["entries"][0]["pts"], 55)
            self.assertEqual(rankings["entries"][1]["id"], "entry-low")
            self.assertEqual(rankings["entries"][1]["pts"], 0)
            self.assertEqual(rankings["entries"][2]["id"], "entry-low-2")
            self.assertEqual(rankings["entries"][2]["pts"], 0)

            players_by_id = {row["player_id"]: row for row in rankings["player_rankings"]}
            self.assertEqual(players_by_id["p-local-1"]["pts"], 40)
            self.assertEqual(players_by_id["p-local-2"]["pts"], 0)
            self.assertEqual(players_by_id["p-local-3"]["pts"], 15)
            self.assertEqual(rankings["player_rankings"][0]["player_id"], "p-local-1")
            self.assertEqual(rankings["player_rankings"][1]["player_id"], "p-local-3")
            self.assertEqual(rankings["player_rankings"][2]["player_id"], "p-local-2")

            teams_by_name = {row["team_name"]: row for row in rankings["team_rankings"]}
            self.assertEqual(teams_by_name["Naan CC"]["pts"], 50)
            self.assertEqual(teams_by_name["Pandiya Associates"]["pts"], 120)
            self.assertEqual(rankings["team_rankings"][0]["team_name"], "Pandiya Associates")
            self.assertEqual(rankings["team_rankings"][1]["team_name"], "Naan CC")

            season_store.db.close()
            global_store.db.close()


if __name__ == "__main__":
    unittest.main()
