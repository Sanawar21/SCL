import argparse
import csv
import json
from pathlib import Path


MANAGER_ID_COLUMNS = (
    "Batting Manager ID",
    "Bowling Manager ID",
)

PLAYER_ID_NAME_COLUMNS = (
    ("Batter ID", "Batter"),
    ("Non Strike Batter ID", "Non Strike Batter"),
    ("Bowler ID", "Bowler"),
    ("Dismissed Batter ID", "Dismissed Batter"),
)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _norm(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _table_values(payload: dict, key: str):
    table = payload.get(key)
    if isinstance(table, dict):
        return [row for row in table.values() if isinstance(row, dict)]
    if isinstance(table, list):
        return [row for row in table if isinstance(row, dict)]
    return []


def _build_identity_mapping(source_season: Path, target_season: Path):
    src = _load_json(source_season)
    dst = _load_json(target_season)

    src_teams = _table_values(src, "teams")
    src_users = _table_values(src, "users")
    src_players = _table_values(src, "players")

    dst_teams = _table_values(dst, "teams")
    dst_players = _table_values(dst, "players")

    src_team_by_id = {
        (row.get("id") or "").strip(): row
        for row in src_teams
        if (row.get("id") or "").strip()
    }
    src_team_by_name = {
        _norm(row.get("name") or ""): row
        for row in src_teams
        if _norm(row.get("name") or "")
    }

    src_manager_username_by_team = {
        (row.get("id") or "").strip(): (row.get("manager_username") or "").strip()
        for row in src_teams
        if (row.get("id") or "").strip()
    }

    for user in src_users:
        if (user.get("role") or "").strip().lower() != "manager":
            continue
        team_id = (user.get("team_id") or "").strip()
        if not team_id:
            continue
        src_manager_username_by_team.setdefault(team_id, (user.get("username") or "").strip())

    dst_team_by_id = {
        (row.get("id") or "").strip(): row
        for row in dst_teams
        if (row.get("id") or "").strip()
    }
    dst_team_by_name = {
        _norm(row.get("name") or ""): row
        for row in dst_teams
        if _norm(row.get("name") or "")
    }

    dst_player_by_id = {
        (row.get("id") or "").strip(): row
        for row in dst_players
        if (row.get("id") or "").strip()
    }

    source_to_target_id = {}

    # Non-manager players typically preserve ids; copy straight-through when present.
    for src_player in src_players:
        player_id = (src_player.get("id") or "").strip()
        if player_id and player_id in dst_player_by_id:
            source_to_target_id[player_id] = player_id

    team_to_manager_player_id = {}

    for team_id, src_team in src_team_by_id.items():
        dst_team = dst_team_by_id.get(team_id)
        if not dst_team:
            team_name = _norm(src_team.get("name") or "")
            dst_team = dst_team_by_name.get(team_name)
        if not dst_team:
            continue

        manager_player_id = (dst_team.get("manager_player_id") or "").strip()
        if not manager_player_id:
            continue

        team_to_manager_player_id[team_id] = manager_player_id

        # Legacy scorer rows stored manager identity as team id.
        source_to_target_id[team_id] = manager_player_id

        src_manager_player_id = (src_team.get("manager_player_id") or "").strip()
        if src_manager_player_id:
            source_to_target_id[src_manager_player_id] = manager_player_id

        manager_username = src_manager_username_by_team.get(team_id, "")
        if manager_username:
            source_to_target_id[f"manager::{_norm(manager_username)}"] = manager_player_id

    target_player_name_by_id = {
        player_id: (row.get("name") or "").strip()
        for player_id, row in dst_player_by_id.items()
    }

    return {
        "source_to_target_id": source_to_target_id,
        "team_to_manager_player_id": team_to_manager_player_id,
        "target_player_name_by_id": target_player_name_by_id,
    }


def _collect_mapping(source_data_root: Path, target_data_root: Path, season_file: str):
    source_season = source_data_root / "season_dbs" / season_file
    target_season = target_data_root / "season_dbs" / season_file

    if not source_season.exists():
        raise FileNotFoundError(f"Source season file not found: {source_season}")
    if not target_season.exists():
        raise FileNotFoundError(f"Target season file not found: {target_season}")

    return _build_identity_mapping(source_season, target_season)


def _patch_delivery_row(row: list[str], index: dict, mapping: dict):
    if not row:
        return row

    source_to_target_id = mapping["source_to_target_id"]
    target_player_name_by_id = mapping["target_player_name_by_id"]

    out = list(row)

    for col in MANAGER_ID_COLUMNS:
        idx = index.get(col)
        if idx is None or idx >= len(out):
            continue
        safe_old = (out[idx] or "").strip()
        if not safe_old:
            continue
        out[idx] = source_to_target_id.get(safe_old, safe_old)

    for id_col, name_col in PLAYER_ID_NAME_COLUMNS:
        id_idx = index.get(id_col)
        name_idx = index.get(name_col)
        if id_idx is None or id_idx >= len(out):
            continue
        safe_old_id = (out[id_idx] or "").strip()
        if not safe_old_id:
            continue
        safe_new_id = source_to_target_id.get(safe_old_id, safe_old_id)
        out[id_idx] = safe_new_id

        if name_idx is not None and name_idx < len(out):
            target_name = target_player_name_by_id.get(safe_new_id)
            if target_name:
                out[name_idx] = target_name

    return out


def migrate_match_file(source_csv: Path, target_csv: Path, mapping: dict):
    with source_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    if not rows:
        target_csv.parent.mkdir(parents=True, exist_ok=True)
        with target_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerows(rows)
        return {"rows": 0, "changed_cells": 0}

    header = rows[0]
    index = {column: idx for idx, column in enumerate(header)}

    out_rows = [header]
    in_substitution_section = False
    delivery_rows = 0
    changed_cells = 0

    for row in rows[1:]:
        if row and (row[0] or "").strip() == "Substitution Log":
            in_substitution_section = True
            out_rows.append(row)
            continue

        if in_substitution_section:
            out_rows.append(row)
            continue

        patched = _patch_delivery_row(row, index, mapping)
        delivery_rows += 1 if row else 0

        if row and patched:
            compare_len = min(len(row), len(patched))
            for idx in range(compare_len):
                if row[idx] != patched[idx]:
                    changed_cells += 1

        out_rows.append(patched)

    target_csv.parent.mkdir(parents=True, exist_ok=True)
    with target_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(out_rows)

    return {
        "rows": delivery_rows,
        "changed_cells": changed_cells,
    }


def migrate_matches(
    source_matches_dir: Path,
    target_matches_dir: Path,
    source_data_root: Path,
    target_data_root: Path,
    season_file: str,
):
    mapping = _collect_mapping(source_data_root, target_data_root, season_file)

    files = sorted(source_matches_dir.glob("*.csv"))
    summary = {
        "files": 0,
        "rows": 0,
        "changed_cells": 0,
        "output_dir": str(target_matches_dir),
    }

    for source_csv in files:
        target_csv = target_matches_dir / source_csv.name
        file_summary = migrate_match_file(source_csv, target_csv, mapping)
        summary["files"] += 1
        summary["rows"] += file_summary["rows"]
        summary["changed_cells"] += file_summary["changed_cells"]

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Migrate legacy match CSV ids from backup DB identity model to current data identity model."
    )
    parser.add_argument(
        "--source-matches",
        default="matches",
        help="Directory containing source match CSV files.",
    )
    parser.add_argument(
        "--target-matches",
        default="matches-migrated",
        help="Directory where migrated match CSV files will be written.",
    )
    parser.add_argument(
        "--source-data-root",
        default="data-backup-4-prod",
        help="Source data root used when matches were originally scored.",
    )
    parser.add_argument(
        "--target-data-root",
        default="data",
        help="Target data root containing current canonical identities.",
    )
    parser.add_argument(
        "--season-file",
        default="season-1.json",
        help="Season DB filename used to build id mapping (under season_dbs).",
    )
    args = parser.parse_args()

    summary = migrate_matches(
        source_matches_dir=Path(args.source_matches),
        target_matches_dir=Path(args.target_matches),
        source_data_root=Path(args.source_data_root),
        target_data_root=Path(args.target_data_root),
        season_file=args.season_file,
    )

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()