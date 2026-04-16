import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.migrate_matches import migrate_matches


class MigrateMatchesTests(unittest.TestCase):
    def test_migrate_matches_remaps_manager_ids_and_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_data = root / "data-backup-4-prod" / "season_dbs"
            target_data = root / "data" / "season_dbs"
            source_matches = root / "matches"
            target_matches = root / "matches-migrated"

            source_data.mkdir(parents=True, exist_ok=True)
            target_data.mkdir(parents=True, exist_ok=True)
            source_matches.mkdir(parents=True, exist_ok=True)

            source_payload = {
                "teams": {
                    "1": {
                        "id": "team-1",
                        "name": "MHK Royales",
                        "manager_username": "Hassan",
                    }
                },
                "players": {
                    "1": {
                        "id": "player-1",
                        "name": "Talha",
                        "tier": "gold",
                    }
                },
                "users": {
                    "1": {
                        "username": "Hassan",
                        "role": "manager",
                        "team_id": "team-1",
                    }
                },
            }

            target_payload = {
                "teams": {
                    "1": {
                        "id": "team-1",
                        "name": "MHK Royales",
                        "manager_username": "Hassan",
                        "manager_player_id": "manager-player-1",
                    }
                },
                "players": {
                    "1": {
                        "id": "player-1",
                        "name": "Talha",
                        "tier": "gold",
                    },
                    "2": {
                        "id": "manager-player-1",
                        "name": "Hassan",
                        "tier": "platinum",
                    },
                },
            }

            (source_data / "season-1.json").write_text(json.dumps(source_payload), encoding="utf-8")
            (target_data / "season-1.json").write_text(json.dumps(target_payload), encoding="utf-8")

            header = [
                "Match ID",
                "Batting Team ID",
                "Batting Manager ID",
                "Batter",
                "Batter ID",
                "Non Strike Batter",
                "Non Strike Batter ID",
                "Bowler",
                "Bowler ID",
                "Bowling Team ID",
                "Bowling Manager ID",
                "Dismissed Batter",
                "Dismissed Batter ID",
            ]

            rows = [
                [
                    "M1",
                    "team-1",
                    "team-1",
                    "Hassan",
                    "team-1",
                    "Talha",
                    "player-1",
                    "Hassan",
                    "team-1",
                    "team-1",
                    "team-1",
                    "Hassan",
                    "team-1",
                ],
                [],
                ["Substitution Log"],
                ["Step", "Playing Team", "Player Out", "Player In", "From Team"],
                ["0", "None", "None", "None", "None"],
            ]

            with (source_matches / "match_M1.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(header)
                writer.writerows(rows)

            summary = migrate_matches(
                source_matches_dir=source_matches,
                target_matches_dir=target_matches,
                source_data_root=root / "data-backup-4-prod",
                target_data_root=root / "data",
                season_file="season-1.json",
            )

            self.assertEqual(summary["files"], 1)
            self.assertGreater(summary["changed_cells"], 0)

            with (target_matches / "match_M1.csv").open("r", encoding="utf-8", newline="") as handle:
                migrated_rows = list(csv.reader(handle))

            data_row = migrated_rows[1]
            by_col = {col: data_row[idx] for idx, col in enumerate(header)}

            self.assertEqual(by_col["Batting Team ID"], "team-1")
            self.assertEqual(by_col["Bowling Team ID"], "team-1")
            self.assertEqual(by_col["Batting Manager ID"], "manager-player-1")
            self.assertEqual(by_col["Bowling Manager ID"], "manager-player-1")
            self.assertEqual(by_col["Batter ID"], "manager-player-1")
            self.assertEqual(by_col["Bowler ID"], "manager-player-1")
            self.assertEqual(by_col["Dismissed Batter ID"], "manager-player-1")
            self.assertEqual(by_col["Batter"], "Hassan")
            self.assertEqual(by_col["Bowler"], "Hassan")
            self.assertEqual(by_col["Dismissed Batter"], "Hassan")


if __name__ == "__main__":
    unittest.main()