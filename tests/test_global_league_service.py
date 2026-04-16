import unittest
import sys
import types

if "flask_socketio" not in sys.modules:
    stub = types.ModuleType("flask_socketio")

    class _SocketIOStub:
        def __init__(self, *args, **kwargs):
            pass

    stub.SocketIO = _SocketIOStub
    sys.modules["flask_socketio"] = stub

from app.services.global_league_service import GlobalLeagueService


class GlobalLeagueServiceTests(unittest.TestCase):
    def test_manager_backfill_uses_manager_username_as_player_name(self):
        service = GlobalLeagueService(store=None)
        tables = {
            "players": [],
            "teams": [
                {
                    "id": "t1",
                    "name": "Team One",
                    "manager_username": "hassan",
                    "manager_tier": "platinum",
                }
            ],
            "users": [
                {
                    "username": "hassan",
                    "display_name": "Team One",
                    "speciality": "BATTER",
                }
            ],
        }

        patched, changed = service._ensure_manager_players_in_tables(tables)

        self.assertTrue(changed)
        self.assertEqual(patched["players"][0]["name"], "hassan")

    def test_rewrite_fantasy_entries_converts_legacy_manager_ids(self):
        service = GlobalLeagueService(store=None)
        tables = {
            "players": [
                {"id": "p1", "name": "Alice", "tier": "gold", "speciality": "BATTER"},
                {"id": "p2", "name": "Bob", "tier": "silver", "speciality": "ALL_ROUNDER"},
            ],
            "teams": [
                {"id": "t1", "name": "Team One", "manager_username": "Bob", "manager_player_id": "p2", "manager_tier": "silver"}
            ],
            "fantasy_entries": [
                {
                    "id": "e1",
                    "entrant_name": "Bob",
                    "entrant_key": "manager:bob",
                    "picks": [
                        {"player_id": "manager::bob", "player_name": "Bob", "tier": "silver", "credits": 1},
                        {"player_id": "p1", "player_name": "Alice", "tier": "gold", "credits": 2},
                    ],
                }
            ],
        }

        patched, rewritten = service._rewrite_fantasy_entries(tables)

        self.assertEqual(rewritten, 1)
        entry = patched["fantasy_entries"][0]
        self.assertEqual(entry["entrant_key"], "player:bob")
        pick_ids = [pick["player_id"] for pick in entry["picks"]]
        self.assertIn("p2", pick_ids)
        self.assertNotIn("manager::bob", pick_ids)
        self.assertEqual(entry["team_signature"], "p1|p2")

    def test_manager_backfill_repairs_existing_manager_player_name(self):
        service = GlobalLeagueService(store=None)
        tables = {
            "players": [
                {
                    "id": "mp1",
                    "name": "MHK Royales",
                    "tier": "platinum",
                    "status": "sold",
                    "sold_to": "t1",
                    "manager_team_id": "t1",
                }
            ],
            "teams": [
                {
                    "id": "t1",
                    "name": "MHK Royales",
                    "manager_username": "Hassan",
                    "manager_player_id": "mp1",
                    "manager_tier": "platinum",
                }
            ],
            "users": [
                {
                    "username": "Hassan",
                    "display_name": "MHK Royales",
                    "speciality": "BATTER",
                }
            ],
        }

        patched, changed = service._ensure_manager_players_in_tables(tables)

        self.assertTrue(changed)
        self.assertEqual(patched["players"][0]["name"], "Hassan")


if __name__ == "__main__":
    unittest.main()
