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


class AdminSeasonFinancesRouteTests(unittest.TestCase):
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

    def _seed_season(self, slug: str, teams: list[dict]):
        season_store_manager = self.app.extensions["season_store_manager"]
        store = season_store_manager.get_store(slug, create=True)
        with store.write() as db:
            db.table("teams").truncate()
            if teams:
                db.table("teams").insert_multiple(teams)

    def _season_team(self, slug: str, team_id: str):
        season_store_manager = self.app.extensions["season_store_manager"]
        store = season_store_manager.get_store(slug, create=False)
        with store.read() as db:
            rows = db.table("teams").search(lambda row: (row.get("id") or "").strip() == team_id)
        return rows[0] if rows else {}

    def _season_transactions(self, slug: str):
        season_store_manager = self.app.extensions["season_store_manager"]
        store = season_store_manager.get_store(slug, create=False)
        with store.read() as db:
            rows = db.table("finance_transactions").all()
        return rows

    def test_finance_add_remove_is_season_specific(self):
        self._seed_season(
            "season-1",
            [
                {
                    "id": "team-a",
                    "name": "Team A",
                    "purse_remaining": 100,
                    "credits_remaining": 8,
                    "players": [],
                    "bench": [],
                },
                {
                    "id": "team-b",
                    "name": "Team B",
                    "purse_remaining": 50,
                    "credits_remaining": 8,
                    "players": [],
                    "bench": [],
                },
            ],
        )
        self._seed_season(
            "season-2",
            [
                {
                    "id": "team-a",
                    "name": "Team A",
                    "purse_remaining": 200,
                    "credits_remaining": 8,
                    "players": [],
                    "bench": [],
                }
            ],
        )

        add_response = self.client.post(
            "/admin/finances/adjust",
            data={
                "season_slug": "season-1",
                "team_id": "team-a",
                "operation": "add",
                "amount": "30",
                "comment": "Prize bonus",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(add_response.status_code, 200, add_response.get_data(as_text=True))

        remove_response = self.client.post(
            "/admin/finances/adjust",
            data={
                "season_slug": "season-1",
                "team_id": "team-a",
                "operation": "remove",
                "amount": "20",
                "comment": "Penalty",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(remove_response.status_code, 200, remove_response.get_data(as_text=True))

        season1_team_a = self._season_team("season-1", "team-a")
        season2_team_a = self._season_team("season-2", "team-a")
        self.assertEqual(int(season1_team_a.get("purse_remaining") or 0), 110)
        self.assertEqual(int(season2_team_a.get("purse_remaining") or 0), 200)

        transactions = self._season_transactions("season-1")
        self.assertEqual(len(transactions), 2)
        operations = sorted((row.get("operation") or "") for row in transactions)
        self.assertEqual(operations, ["add", "remove"])
        comments = sorted((row.get("comment") or "") for row in transactions)
        self.assertEqual(comments, ["Penalty", "Prize bonus"])

        negative_remove_response = self.client.post(
            "/admin/finances/adjust",
            data={
                "season_slug": "season-1",
                "team_id": "team-b",
                "operation": "remove",
                "amount": "999",
                "comment": "Emergency deduction",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(negative_remove_response.status_code, 200, negative_remove_response.get_data(as_text=True))

        season1_team_b = self._season_team("season-1", "team-b")
        self.assertEqual(int(season1_team_b.get("purse_remaining") or 0), -949)

    def test_finance_transfer_between_teams(self):
        self._seed_season(
            "season-1",
            [
                {
                    "id": "team-a",
                    "name": "Team A",
                    "purse_remaining": 100,
                    "credits_remaining": 8,
                    "players": [],
                    "bench": [],
                },
                {
                    "id": "team-b",
                    "name": "Team B",
                    "purse_remaining": 20,
                    "credits_remaining": 8,
                    "players": [],
                    "bench": [],
                },
            ],
        )

        transfer_response = self.client.post(
            "/admin/finances/transfer",
            data={
                "season_slug": "season-1",
                "from_team_id": "team-a",
                "to_team_id": "team-b",
                "amount": "40",
                "comment": "Trade settlement",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(transfer_response.status_code, 200, transfer_response.get_data(as_text=True))

        team_a = self._season_team("season-1", "team-a")
        team_b = self._season_team("season-1", "team-b")
        self.assertEqual(int(team_a.get("purse_remaining") or 0), 60)
        self.assertEqual(int(team_b.get("purse_remaining") or 0), 60)

        transactions = self._season_transactions("season-1")
        self.assertEqual(len(transactions), 1)
        self.assertEqual((transactions[0].get("type") or "").strip(), "transfer")
        self.assertEqual((transactions[0].get("from_team_id") or "").strip(), "team-a")
        self.assertEqual((transactions[0].get("to_team_id") or "").strip(), "team-b")
        self.assertEqual(int(transactions[0].get("amount") or 0), 40)
        self.assertEqual((transactions[0].get("comment") or "").strip(), "Trade settlement")

    def test_finance_comment_is_required(self):
        self._seed_season(
            "season-1",
            [
                {
                    "id": "team-a",
                    "name": "Team A",
                    "purse_remaining": 100,
                    "credits_remaining": 8,
                    "players": [],
                    "bench": [],
                },
                {
                    "id": "team-b",
                    "name": "Team B",
                    "purse_remaining": 20,
                    "credits_remaining": 8,
                    "players": [],
                    "bench": [],
                },
            ],
        )

        adjust_missing_comment = self.client.post(
            "/admin/finances/adjust",
            data={
                "season_slug": "season-1",
                "team_id": "team-a",
                "operation": "add",
                "amount": "10",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(adjust_missing_comment.status_code, 400, adjust_missing_comment.get_data(as_text=True))

        transfer_missing_comment = self.client.post(
            "/admin/finances/transfer",
            data={
                "season_slug": "season-1",
                "from_team_id": "team-a",
                "to_team_id": "team-b",
                "amount": "10",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(transfer_missing_comment.status_code, 400, transfer_missing_comment.get_data(as_text=True))

        same_team_response = self.client.post(
            "/admin/finances/transfer",
            data={
                "season_slug": "season-1",
                "from_team_id": "team-a",
                "to_team_id": "team-a",
                "amount": "1",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(same_team_response.status_code, 400, same_team_response.get_data(as_text=True))

        negative_transfer = self.client.post(
            "/admin/finances/transfer",
            data={
                "season_slug": "season-1",
                "from_team_id": "team-b",
                "to_team_id": "team-a",
                "amount": "999",
                "comment": "Carry-forward allocation",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(negative_transfer.status_code, 200, negative_transfer.get_data(as_text=True))

        team_a_after_negative_transfer = self._season_team("season-1", "team-a")
        team_b_after_negative_transfer = self._season_team("season-1", "team-b")
        self.assertEqual(int(team_a_after_negative_transfer.get("purse_remaining") or 0), 1099)
        self.assertEqual(int(team_b_after_negative_transfer.get("purse_remaining") or 0), -979)


if __name__ == "__main__":
    unittest.main()
