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

from scripts.migrate_global_links import _rewrite_team_usernames_in_tables


class MigrateGlobalLinksTests(unittest.TestCase):
    def test_rewrite_team_usernames_updates_users_and_teams(self):
        tables = {
            "users": [
                {"username": "Hassan", "role": "manager", "display_name": "Hassan", "team_id": "t1"},
                {"username": "Hashir", "role": "manager", "display_name": "Hashir", "team_id": "t2"},
                {"username": "admin", "role": "admin", "display_name": "Administrator"},
            ],
            "teams": [
                {"id": "t1", "name": "MHK Royales", "manager_username": "Hassan"},
                {"id": "t2", "name": "Naan CC", "manager_username": "Hashir"},
            ],
        }

        patched, rename_map, renamed_count = _rewrite_team_usernames_in_tables(tables)

        self.assertEqual(rename_map.get("Hassan"), "mhk-royales")
        self.assertEqual(rename_map.get("Hashir"), "naan-cc")
        self.assertGreater(renamed_count, 0)

        users = {row["team_id"]: row for row in patched["users"] if row.get("role") == "manager"}
        self.assertEqual(users["t1"]["username"], "mhk-royales")
        self.assertEqual(users["t2"]["username"], "naan-cc")
        self.assertEqual(users["t1"]["display_name"], "MHK Royales")
        self.assertEqual(users["t2"]["display_name"], "Naan CC")

        teams = {row["id"]: row for row in patched["teams"]}
        self.assertEqual(teams["t1"]["manager_username"], "mhk-royales")
        self.assertEqual(teams["t2"]["manager_username"], "naan-cc")


if __name__ == "__main__":
    unittest.main()
