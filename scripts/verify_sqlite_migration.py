#!/usr/bin/env python3
"""Verify JSON -> SQLite migration completeness for SCL data."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def iter_rows(table_obj: Any):
    if isinstance(table_obj, dict):
        for key in sorted(table_obj.keys(), key=lambda k: (len(str(k)), str(k))):
            row = table_obj.get(key)
            if isinstance(row, dict):
                yield str(key), row
    elif isinstance(table_obj, list):
        for idx, row in enumerate(table_obj, start=1):
            if isinstance(row, dict):
                yield str(idx), row


def extract_tables(payload: dict[str, Any]) -> dict[str, Any]:
    tables = payload.get("tables")
    if isinstance(tables, dict):
        return tables
    return payload


def slug_from_name(name: str) -> str:
    safe = (name or "").strip().lower()
    out = []
    for ch in safe:
        if ch.isalnum() or ch in {"-", "_"}:
            out.append(ch)
        elif ch in {" ", "."}:
            out.append("-")
    slug = "".join(out).strip("-")
    return slug or "unknown"


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


class Verifier:
    def __init__(self, data_dir: Path, db_path: Path):
        self.data_dir = data_dir
        self.db_path = db_path

    def run(self) -> int:
        if not self.data_dir.exists() or not self.data_dir.is_dir():
            print(f"ERROR: data dir not found: {self.data_dir}")
            return 2
        if not self.db_path.exists():
            print(f"ERROR: sqlite db not found: {self.db_path}")
            return 2

        docs = self._load_docs()
        final_draft_bids_by_slug = self._load_final_draft_bids_by_slug()
        checks: list[CheckResult] = []

        with sqlite3.connect(self.db_path) as conn:
            checks.extend(self._check_source_capture(conn, docs))
            checks.extend(self._check_modeled_counts(conn, docs, final_draft_bids_by_slug))
            checks.extend(self._check_manager_integrity(conn))

        failed = [c for c in checks if not c.passed]
        for check in checks:
            prefix = "PASS" if check.passed else "FAIL"
            print(f"[{prefix}] {check.name}: {check.detail}")

        print()
        print(f"Checks: {len(checks)}, Passed: {len(checks) - len(failed)}, Failed: {len(failed)}")
        return 1 if failed else 0

    def _load_docs(self) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
        docs = []
        for file_path in sorted(self.data_dir.rglob("*.json")):
            rel = file_path.relative_to(self.data_dir).as_posix()
            if rel.startswith("auction_snapshots/"):
                continue
            if rel == "auction_live_db.json":
                continue
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                docs.append((rel, payload, extract_tables(payload)))
        return docs

    def _load_final_draft_bids_by_slug(self) -> dict[str, int]:
        out: dict[str, int] = {}
        snapshots_dir = self.data_dir / "auction_snapshots"
        if not snapshots_dir.exists():
            return out

        for file_path in sorted(snapshots_dir.glob("*-final-draft.json")):
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(payload, dict):
                continue

            tables = extract_tables(payload)
            bid_table = tables.get("bids")
            if bid_table is None:
                continue

            slug = file_path.stem
            if slug.endswith("-final-draft"):
                slug = slug[: -len("-final-draft")]
            season_slug = slug_from_name(slug)
            out[season_slug] = sum(1 for _ in iter_rows(bid_table))

        return out

    def _check_source_capture(
        self,
        conn: sqlite3.Connection,
        docs: list[tuple[str, dict[str, Any], dict[str, Any]]],
    ) -> list[CheckResult]:
        checks: list[CheckResult] = []

        expected_docs = len(docs)
        actual_docs = conn.execute("SELECT COUNT(*) FROM source_documents").fetchone()[0]
        checks.append(
            CheckResult(
                "source_documents count",
                actual_docs == expected_docs,
                f"expected={expected_docs}, actual={actual_docs}",
            )
        )

        expected_rows = 0
        expected_paths = set()
        for rel, _payload, tables in docs:
            expected_paths.add(rel)
            for _table_name, table_obj in tables.items():
                expected_rows += sum(1 for _ in iter_rows(table_obj))

        actual_rows = conn.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0]
        checks.append(
            CheckResult(
                "source_rows count",
                actual_rows == expected_rows,
                f"expected={expected_rows}, actual={actual_rows}",
            )
        )

        db_paths = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT source_path FROM source_documents"
            ).fetchall()
        }
        missing = sorted(expected_paths - db_paths)
        extra = sorted(db_paths - expected_paths)
        checks.append(
            CheckResult(
                "source document paths",
                (not missing) and (not extra),
                f"missing={len(missing)}, extra={len(extra)}",
            )
        )

        return checks

    def _check_modeled_counts(
        self,
        conn: sqlite3.Connection,
        docs: list[tuple[str, dict[str, Any], dict[str, Any]]],
        final_draft_bids_by_slug: dict[str, int],
    ) -> list[CheckResult]:
        checks: list[CheckResult] = []

        player_ids = set()
        team_ids = set()
        bid_count = 0
        fantasy_team_count = 0
        fantasy_pick_count = 0
        roster_count = 0

        for _rel, _payload, tables in docs:
            for _k, player in iter_rows(tables.get("players", {})):
                pid = (player.get("id") or "").strip()
                if pid:
                    player_ids.add(pid)

            for _k, team in iter_rows(tables.get("teams", {})):
                tid = (team.get("id") or "").strip()
                if tid:
                    team_ids.add(tid)
                    manager_username = (team.get("manager_username") or "").strip().lower()
                    manager_pid = f"manager::{manager_username}" if manager_username else f"manager::team-{tid}"
                    player_ids.add(manager_pid)

                    roster_count += len(team.get("players", []) or [])
                    roster_count += len(team.get("bench", []) or [])
                    roster_count += 1

            doc_bids = sum(1 for _ in iter_rows(tables.get("bids", {})))
            if doc_bids:
                bid_count += doc_bids
            elif _rel.startswith("season_dbs/"):
                season_slug = slug_from_name(Path(_rel).stem)
                bid_count += final_draft_bids_by_slug.get(season_slug, 0)

            for _k, entry in iter_rows(tables.get("fantasy_entries", {})):
                fantasy_team_count += 1
                picks = entry.get("picks") or []
                fantasy_pick_count += len(picks)
                for pick in picks:
                    pid = pick.get("player_id")
                    if isinstance(pid, str) and pid.startswith("manager::"):
                        player_ids.add(pid)

        actual_players = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        actual_teams = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
        actual_bids = conn.execute("SELECT COUNT(*) FROM bids").fetchone()[0]
        actual_fteams = conn.execute("SELECT COUNT(*) FROM fantasy_teams").fetchone()[0]
        actual_fpicks = conn.execute("SELECT COUNT(*) FROM fantasy_team_picks").fetchone()[0]
        actual_rosters = conn.execute("SELECT COUNT(*) FROM team_rosters").fetchone()[0]

        checks.append(
            CheckResult(
                "players count",
                actual_players == len(player_ids),
                f"expected={len(player_ids)}, actual={actual_players}",
            )
        )
        checks.append(
            CheckResult(
                "teams count",
                actual_teams == len(team_ids),
                f"expected={len(team_ids)}, actual={actual_teams}",
            )
        )
        checks.append(
            CheckResult(
                "bids count",
                actual_bids == bid_count,
                f"expected={bid_count}, actual={actual_bids}",
            )
        )
        checks.append(
            CheckResult(
                "fantasy teams count",
                actual_fteams == fantasy_team_count,
                f"expected={fantasy_team_count}, actual={actual_fteams}",
            )
        )
        checks.append(
            CheckResult(
                "fantasy picks count",
                actual_fpicks == fantasy_pick_count,
                f"expected={fantasy_pick_count}, actual={actual_fpicks}",
            )
        )
        checks.append(
            CheckResult(
                "team rosters count",
                actual_rosters == roster_count,
                f"expected={roster_count}, actual={actual_rosters}",
            )
        )

        return checks

    def _check_manager_integrity(self, conn: sqlite3.Connection) -> list[CheckResult]:
        checks: list[CheckResult] = []

        missing_manager_players = conn.execute(
            """
            SELECT COUNT(*)
            FROM teams t
            LEFT JOIN players p ON p.id = t.manager_player_id
            WHERE p.id IS NULL
            """
        ).fetchone()[0]
        checks.append(
            CheckResult(
                "teams.manager_player_id references players",
                missing_manager_players == 0,
                f"missing_references={missing_manager_players}",
            )
        )

        non_manager_flag_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM teams t
            JOIN players p ON p.id = t.manager_player_id
            WHERE p.is_manager != 1
            """
        ).fetchone()[0]
        checks.append(
            CheckResult(
                "manager players flagged is_manager=1",
                non_manager_flag_count == 0,
                f"invalid_rows={non_manager_flag_count}",
            )
        )

        missing_manager_roster = conn.execute(
            """
            SELECT COUNT(*)
            FROM teams t
            LEFT JOIN team_rosters r
              ON r.team_id = t.id
             AND r.player_id = t.manager_player_id
             AND r.roster_role = 'manager'
            WHERE r.team_id IS NULL
            """
        ).fetchone()[0]
        checks.append(
            CheckResult(
                "manager included in team_rosters",
                missing_manager_roster == 0,
                f"missing_rows={missing_manager_roster}",
            )
        )

        return checks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify SQLite migration completeness")
    parser.add_argument("--data-dir", default="data", help="JSON data directory")
    parser.add_argument("--db-path", default="data/scl.sqlite3", help="SQLite database path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    verifier = Verifier(
        data_dir=Path(args.data_dir).resolve(),
        db_path=Path(args.db_path).resolve(),
    )
    return verifier.run()


if __name__ == "__main__":
    sys.exit(main())
