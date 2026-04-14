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


if __name__ == "__main__":
    unittest.main()
