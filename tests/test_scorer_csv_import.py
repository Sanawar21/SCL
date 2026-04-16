import io
import sys
import tempfile
import types
import unittest
from decimal import Decimal, ROUND_HALF_UP
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


class ScorerCsvImportRouteTests(unittest.TestCase):
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

    @staticmethod
    def _sample_csv_payload(match_id: str = "M99") -> str:
        return "\n".join(
            [
                '"Match ID","Match","Venue","Scorer Version","Substitutions Applied","Substitution Details","Innings Order","Batting Team","Batting Team ID","Batting Manager ID","Over Number","Ball Number","Valid Ball?","Batter","Batter ID","Batter Order","Non Strike Batter","Non Strike Batter ID","Bowler","Bowler ID","Bowling Team","Bowling Team ID","Bowling Manager ID","Runs Bat","Runs Extra","Extras Type","Dismissed Batter","Dismissed Batter ID","Progressive Runs","Progressive Wickets","Match Toss","Match Result"',
                f'"{match_id}","Alpha XI vs Beta XI","Old Venue","1.1.0","0","None","1","Alpha XI","t-local-1","m-local-1","0","1","Yes","Alice","p-local-a","1","Bob","p-local-b","Carl","p-local-c","Beta XI","t-local-2","m-local-2","4","0","None","None","","4","0","Alpha XI","Alpha XI won"',
                f'"{match_id}","Alpha XI vs Beta XI","Old Venue","1.1.0","0","None","1","Alpha XI","t-local-1","m-local-1","0","2","Yes","Alice","p-local-a","1","Bob","p-local-b","Carl","p-local-c","Beta XI","t-local-2","m-local-2","0","0","None","Alice","p-local-a","4","1","Alpha XI","Alpha XI won"',
                f'"{match_id}","Alpha XI vs Beta XI","Old Venue","1.1.0","0","None","2","Beta XI","t-local-2","m-local-2","0","1","Yes","Carl","p-local-c","1","Dana","p-local-d","Bob","p-local-b","Alpha XI","t-local-1","m-local-1","1","0","None","None","","1","0","Alpha XI","Alpha XI won"',
                f'"{match_id}","Alpha XI vs Beta XI","Old Venue","1.1.0","0","None","2","Beta XI","t-local-2","m-local-2","0","2","Yes","Dana","p-local-d","2","Carl","p-local-c","Bob","p-local-b","Alpha XI","t-local-1","m-local-1","0","0","None","Dana","p-local-d","1","1","Alpha XI","Alpha XI won"',
                "",
                '"Substitution Log"',
                '"Step","Playing Team","Player Out","Player In","From Team"',
                '"0","None","None","None","None"',
            ]
        )

    def _register_match(self, match_id: str, season_slug: str = "season-1"):
        response = self.client.post(
            "/admin/scorer/matches",
            data={
                "season_slug": season_slug,
                "match_id": match_id,
                "team_a_global_id": "team-alpha",
                "team_b_global_id": "team-beta",
                "match_number": f"Match {match_id}",
                "match_title": f"{match_id} Title",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))

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

    def test_import_scorer_csv_computes_global_stats(self):
        self._register_match("M99-OVR")
        csv_payload = self._sample_csv_payload(match_id="M99")

        response = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "match_id_override": "M99-OVR",
                "venue_override": "National Ground",
                "match_date": "2026-04-15",
                "match_csvs": (io.BytesIO(csv_payload.encode("utf-8")), "match_M99.csv"),
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        body = response.get_json()
        self.assertTrue(body.get("ok"), body)
        self.assertEqual(len(body.get("imports", [])), 1)

        global_tables = self.app.extensions["global_league_store"].export_tables()

        match_rows = global_tables.get("scorer_match_stats", [])
        self.assertEqual(len(match_rows), 1)
        self.assertEqual(match_rows[0].get("match_id"), "M99-OVR")
        self.assertEqual(match_rows[0].get("venue"), "National Ground")

        team_match_rows = global_tables.get("scorer_team_match_stats", [])
        player_match_rows = global_tables.get("scorer_player_match_stats", [])
        self.assertGreaterEqual(len(team_match_rows), 2)
        self.assertGreaterEqual(len(player_match_rows), 3)

        player_global_rows = global_tables.get("scorer_player_global_stats", [])
        team_global_rows = global_tables.get("scorer_team_global_stats", [])
        self.assertGreaterEqual(len(player_global_rows), 3)
        self.assertGreaterEqual(len(team_global_rows), 2)

        fantasy_present = any(abs(float(row.get("fantasy_score") or 0.0)) > 0 for row in player_match_rows)
        self.assertTrue(fantasy_present)

        bonus_applied = any(
            int(row.get("fantasy_score") or 0)
            == int(
                Decimal(
                    str(
                        float(row.get("fantasy_bat_points") or 0.0)
                        + float(row.get("fantasy_bowl_points") or 0.0)
                        + 25.0
                    )
                ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            )
            for row in player_match_rows
        )
        self.assertTrue(bonus_applied)

        season_files = list(Path(self.paths["season"]).glob("*.json"))
        self.assertEqual(season_files, [])

        stats_page = self.client.get("/admin?tab=stats")
        self.assertEqual(stats_page.status_code, 200)
        self.assertIn("Global Team Stats", stats_page.get_data(as_text=True))

    def test_import_can_exclude_match_from_fantasy_aggregates(self):
        self._register_match("M66")
        csv_payload = self._sample_csv_payload(match_id="M66")

        response = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "include_in_fantasy_points": "false",
                "match_csvs": (io.BytesIO(csv_payload.encode("utf-8")), "match_M66.csv"),
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        body = response.get_json()
        self.assertTrue(body.get("ok"), body)
        self.assertEqual((body.get("summary") or {}).get("include_in_fantasy_points"), False)

        global_tables = self.app.extensions["global_league_store"].export_tables()

        match_rows = [row for row in global_tables.get("scorer_match_stats", []) if row.get("match_id") == "M66"]
        self.assertEqual(len(match_rows), 1)
        self.assertEqual(match_rows[0].get("include_in_fantasy_points"), False)

        player_match_rows = [row for row in global_tables.get("scorer_player_match_stats", []) if row.get("match_id") == "M66"]
        self.assertTrue(any(abs(float(row.get("fantasy_score") or 0.0)) > 0 for row in player_match_rows))

        team_global_rows = global_tables.get("scorer_team_global_stats", [])
        player_global_rows = global_tables.get("scorer_player_global_stats", [])
        self.assertTrue(any(int(row.get("runs_scored") or 0) > 0 for row in team_global_rows))
        self.assertTrue(all(abs(float(row.get("fantasy_points") or 0.0)) == 0.0 for row in team_global_rows))
        self.assertTrue(all(abs(float(row.get("fantasy_score") or 0.0)) == 0.0 for row in player_global_rows))

    def test_substitution_log_suppresses_fantasy_for_subbed_in_players_only(self):
        self._register_match("M70")
        csv_payload = "\n".join(
            [
                '"Match ID","Match","Venue","Scorer Version","Substitutions Applied","Substitution Details","Innings Order","Batting Team","Batting Team ID","Batting Manager ID","Over Number","Ball Number","Valid Ball?","Batter","Batter ID","Batter Order","Non Strike Batter","Non Strike Batter ID","Bowler","Bowler ID","Bowling Team","Bowling Team ID","Bowling Manager ID","Runs Bat","Runs Extra","Extras Type","Dismissed Batter","Dismissed Batter ID","Progressive Runs","Progressive Wickets","Match Toss","Match Result"',
                '"M70","Alpha XI vs Beta XI","Old Venue","1.1.0","1","None","1","Alpha XI","t-local-1","m-local-1","0","1","Yes","Evan","p-local-e","1","Bob","p-local-b","Carl","p-local-c","Beta XI","t-local-2","m-local-2","4","0","None","None","","4","0","Alpha XI","Alpha XI won"',
                '"M70","Alpha XI vs Beta XI","Old Venue","1.1.0","1","None","1","Alpha XI","t-local-1","m-local-1","0","2","Yes","Evan","p-local-e","1","Bob","p-local-b","Carl","p-local-c","Beta XI","t-local-2","m-local-2","1","0","None","None","","5","0","Alpha XI","Alpha XI won"',
                '"M70","Alpha XI vs Beta XI","Old Venue","1.1.0","1","None","2","Beta XI","t-local-2","m-local-2","0","1","Yes","Carl","p-local-c","1","Dana","p-local-d","Bob","p-local-b","Alpha XI","t-local-1","m-local-1","1","0","None","None","","1","0","Alpha XI","Alpha XI won"',
                '',
                '"Substitution Log"',
                '"Step","Playing Team","Player Out","Player In","From Team"',
                '"1","Alpha XI","Alice","Evan","None"',
            ]
        )

        response = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "match_csvs": (io.BytesIO(csv_payload.encode("utf-8")), "match_M70.csv"),
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        body = response.get_json()
        self.assertTrue(body.get("ok"), body)

        global_tables = self.app.extensions["global_league_store"].export_tables()
        player_match_rows = [row for row in global_tables.get("scorer_player_match_stats", []) if row.get("match_id") == "M70"]

        evan_row = next((row for row in player_match_rows if (row.get("player_name") or "") == "Evan"), None)
        self.assertIsNotNone(evan_row)
        self.assertGreater(int(evan_row.get("runs") or 0), 0)
        self.assertEqual(int(evan_row.get("fantasy_score") or 0), 0)

        non_subbed_rows = [row for row in player_match_rows if (row.get("player_name") or "") != "Evan"]
        self.assertTrue(any(abs(int(row.get("fantasy_score") or 0)) > 0 for row in non_subbed_rows))

    def test_duplicate_match_requires_confirmation_before_overwrite(self):
        self._register_match("M44")
        csv_payload = self._sample_csv_payload(match_id="M44")

        first_response = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "match_csvs": (io.BytesIO(csv_payload.encode("utf-8")), "match_M44.csv"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(first_response.status_code, 200, first_response.get_data(as_text=True))

        duplicate_response = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "match_csvs": (io.BytesIO(csv_payload.encode("utf-8")), "match_M44.csv"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(duplicate_response.status_code, 409, duplicate_response.get_data(as_text=True))
        duplicate_body = duplicate_response.get_json()
        self.assertFalse(duplicate_body.get("ok"), duplicate_body)
        self.assertTrue(duplicate_body.get("confirmation_required"), duplicate_body)
        self.assertEqual(len(duplicate_body.get("duplicates", [])), 1)

        overwrite_response = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "confirm_overwrite": "true",
                "match_csvs": (io.BytesIO(csv_payload.encode("utf-8")), "match_M44.csv"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(overwrite_response.status_code, 200, overwrite_response.get_data(as_text=True))

        global_tables = self.app.extensions["global_league_store"].export_tables()
        match_rows = [row for row in global_tables.get("scorer_match_stats", []) if row.get("match_id") == "M44"]
        self.assertEqual(len(match_rows), 1)

    def test_undo_imported_match_removes_rows_and_rebuilds_aggregates(self):
        self._register_match("M55")
        csv_payload = self._sample_csv_payload(match_id="M55")

        import_response = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "match_csvs": (io.BytesIO(csv_payload.encode("utf-8")), "match_M55.csv"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(import_response.status_code, 200, import_response.get_data(as_text=True))
        import_body = import_response.get_json()
        self.assertTrue(import_body.get("ok"), import_body)
        match_key = import_body.get("imports", [{}])[0].get("match_key")
        self.assertTrue(match_key)

        undo_response = self.client.post(
            "/admin/scorer/import/undo",
            data={"match_key": match_key},
            content_type="multipart/form-data",
        )
        self.assertEqual(undo_response.status_code, 200, undo_response.get_data(as_text=True))
        undo_body = undo_response.get_json()
        self.assertTrue(undo_body.get("ok"), undo_body)
        self.assertTrue((undo_body.get("summary") or {}).get("removed"), undo_body)

        global_tables = self.app.extensions["global_league_store"].export_tables()
        remaining_matches = [row for row in global_tables.get("scorer_match_stats", []) if row.get("match_key") == match_key]
        remaining_team_rows = [
            row for row in global_tables.get("scorer_team_match_stats", []) if row.get("match_key") == match_key
        ]
        remaining_player_rows = [
            row for row in global_tables.get("scorer_player_match_stats", []) if row.get("match_key") == match_key
        ]

        self.assertEqual(len(remaining_matches), 0)
        self.assertEqual(len(remaining_team_rows), 0)
        self.assertEqual(len(remaining_player_rows), 0)
        self.assertEqual(global_tables.get("scorer_team_global_stats", []), [])
        self.assertEqual(global_tables.get("scorer_player_global_stats", []), [])

        undo_missing_response = self.client.post(
            "/admin/scorer/import/undo",
            data={"match_key": match_key},
            content_type="multipart/form-data",
        )
        self.assertEqual(undo_missing_response.status_code, 404, undo_missing_response.get_data(as_text=True))

    def test_matches_registry_controls_upload_and_walkover(self):
        save_match_response = self.client.post(
            "/admin/scorer/matches",
            data={
                "season_slug": "season-1",
                "match_id": "M77",
                "team_a_global_id": "team-alpha",
                "team_b_global_id": "team-beta",
                "match_number": "Match 77",
                "match_title": "Qualifier",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(save_match_response.status_code, 200, save_match_response.get_data(as_text=True))

        csv_payload = self._sample_csv_payload(match_id="M88")
        unknown_match_upload = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "match_csvs": (io.BytesIO(csv_payload.encode("utf-8")), "match_M88.csv"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(unknown_match_upload.status_code, 400, unknown_match_upload.get_data(as_text=True))

        known_match_upload = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "match_csvs": (io.BytesIO(self._sample_csv_payload(match_id="M77").encode("utf-8")), "match_M77.csv"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(known_match_upload.status_code, 200, known_match_upload.get_data(as_text=True))

        matches_page = self.client.get("/matches/season-1")
        self.assertEqual(matches_page.status_code, 200)
        matches_html = matches_page.get_data(as_text=True)
        self.assertIn("M77", matches_html)
        self.assertIn("Uploaded", matches_html)

        summary_page = self.client.get("/matches/season-1/M77")
        self.assertEqual(summary_page.status_code, 200)
        summary_html = summary_page.get_data(as_text=True)
        self.assertIn("Match Innings", summary_html)
        self.assertIn("Fantasy Points Leaderboard", summary_html)

        walkover_response = self.client.post(
            "/admin/scorer/matches",
            data={
                "season_slug": "season-1",
                "match_id": "M78",
                "team_a_global_id": "team-gamma",
                "team_b_global_id": "team-delta",
                "match_number": "Match 78",
                "walkover": "true",
                "walkover_winner_global_id": "team-gamma",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(walkover_response.status_code, 200, walkover_response.get_data(as_text=True))

        global_tables = self.app.extensions["global_league_store"].export_tables()
        walkover_match_rows = [row for row in global_tables.get("scorer_match_stats", []) if row.get("match_id") == "M78"]
        walkover_team_rows = [row for row in global_tables.get("scorer_team_match_stats", []) if row.get("match_id") == "M78"]
        self.assertEqual(len(walkover_match_rows), 1)
        self.assertEqual((walkover_match_rows[0].get("source_type") or ""), "walkover")
        self.assertEqual(len(walkover_team_rows), 2)

        walkover_upload = self.client.post(
            "/admin/scorer/import",
            data={
                "season_slug": "season-1",
                "match_csvs": (io.BytesIO(self._sample_csv_payload(match_id="M78").encode("utf-8")), "match_M78.csv"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(walkover_upload.status_code, 400, walkover_upload.get_data(as_text=True))

        walkover_summary = self.client.get("/matches/season-1/M78")
        self.assertEqual(walkover_summary.status_code, 200)
        self.assertIn("Walkover", walkover_summary.get_data(as_text=True))

        delete_walkover_match = self.client.post(
            "/admin/scorer/matches/delete",
            data={
                "season_slug": "season-1",
                "match_id": "M78",
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(delete_walkover_match.status_code, 200, delete_walkover_match.get_data(as_text=True))

        tables_after_delete = self.app.extensions["global_league_store"].export_tables()
        self.assertEqual(
            [row for row in tables_after_delete.get("scorer_match_stats", []) if row.get("match_id") == "M78"],
            [],
        )
        self.assertEqual(
            [row for row in tables_after_delete.get("scorer_team_match_stats", []) if row.get("match_id") == "M78"],
            [],
        )


if __name__ == "__main__":
    unittest.main()
