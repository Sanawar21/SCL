import argparse
import json
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import LockedTinyDB
from app.services.global_league_service import GlobalLeagueService
from werkzeug.security import generate_password_hash


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload):
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _team_username_base(team_name: str):
    base = re.sub(r"[^a-z0-9]+", "-", (team_name or "").strip().lower())
    base = base.strip("-")
    return base or "team"


def _rewrite_team_usernames_in_tables(tables: dict):
    if not isinstance(tables, dict):
        return tables, {}, 0

    users = list(tables.get("users", []))
    teams = list(tables.get("teams", []))
    if not users or not teams:
        return tables, {}, 0

    manager_users_by_team_id = {
        (user.get("team_id") or "").strip(): user
        for user in users
        if (user.get("role") or "").strip().lower() == "manager" and (user.get("team_id") or "").strip()
    }
    manager_users_by_username = {
        (user.get("username") or "").strip(): user
        for user in users
        if (user.get("role") or "").strip().lower() == "manager" and (user.get("username") or "").strip()
    }

    taken = {
        (user.get("username") or "").strip()
        for user in users
        if (user.get("username") or "").strip()
    }

    rename_map = {}
    for team in sorted(teams, key=lambda item: ((item.get("name") or "").lower(), (item.get("id") or ""))):
        team_id = (team.get("id") or "").strip()
        if not team_id:
            continue

        current_username = (team.get("manager_username") or "").strip()
        linked_manager_user = manager_users_by_team_id.get(team_id)
        if not current_username and linked_manager_user:
            current_username = (linked_manager_user.get("username") or "").strip()
        if not current_username:
            continue

        base = _team_username_base(team.get("name") or "")
        candidate = base
        suffix = 2

        while candidate in (taken - {current_username}) or candidate in rename_map.values():
            candidate = f"{base}-{suffix}"
            suffix += 1

        rename_map[current_username] = candidate
        taken.add(candidate)

    renamed_count = 0
    patched_teams = []
    team_name_by_id = {}
    for team in teams:
        patched_team = dict(team)
        team_id = (patched_team.get("id") or "").strip()
        team_name_by_id[team_id] = (patched_team.get("name") or "").strip()

        old_username = (patched_team.get("manager_username") or "").strip()
        new_username = rename_map.get(old_username)
        if old_username and new_username and new_username != old_username:
            patched_team["manager_username"] = new_username
            renamed_count += 1

        patched_teams.append(patched_team)

    patched_users = []
    for user in users:
        patched_user = dict(user)
        old_username = (patched_user.get("username") or "").strip()
        new_username = rename_map.get(old_username)
        if old_username and new_username and new_username != old_username:
            patched_user["username"] = new_username
            renamed_count += 1

        if (patched_user.get("role") or "").strip().lower() == "manager":
            team_id = (patched_user.get("team_id") or "").strip()
            team_name = team_name_by_id.get(team_id)
            if team_name:
                patched_user["display_name"] = team_name

        patched_users.append(patched_user)

    patched_tables = dict(tables)
    patched_tables["users"] = patched_users
    patched_tables["teams"] = patched_teams
    return patched_tables, rename_map, renamed_count


def _process_tinydb(
    path: Path,
    season_slug: str,
    service: GlobalLeagueService,
    apply_changes: bool,
    rewrite_team_usernames: bool,
):
    db = LockedTinyDB(str(path))
    tables = db.export_tables()
    if rewrite_team_usernames:
        rewritten_tables, rename_map, renamed_count = _rewrite_team_usernames_in_tables(tables)
    else:
        rewritten_tables = tables
        rename_map = {}
        renamed_count = 0

    patched_tables, summary = service.apply_global_ids(
        season_slug=season_slug,
        tables=rewritten_tables,
        published_at=datetime.now(timezone.utc).isoformat(),
    )

    changed = tables != patched_tables
    if apply_changes and changed:
        db.import_tables(patched_tables)

    return {
        "file": str(path),
        "season_slug": season_slug,
        "changed": changed,
        "team_username_renames": rename_map,
        "team_username_renamed_count": renamed_count,
        "summary": summary,
    }


def _process_payload_file(
    path: Path,
    slug: str,
    service: GlobalLeagueService,
    apply_changes: bool,
    rewrite_team_usernames: bool,
):
    payload = _load_json(path)
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        return {
            "file": str(path),
            "season_slug": slug,
            "changed": False,
            "skipped": True,
            "reason": "missing tables payload",
        }

    if rewrite_team_usernames:
        rewritten_tables, rename_map, renamed_count = _rewrite_team_usernames_in_tables(tables)
    else:
        rewritten_tables = tables
        rename_map = {}
        renamed_count = 0

    patched_tables, summary = service.apply_global_ids(
        season_slug=slug,
        tables=rewritten_tables,
        published_at=(payload.get("saved_at") or datetime.now(timezone.utc).isoformat()),
    )

    changed = tables != patched_tables
    if apply_changes and changed:
        payload["tables"] = patched_tables
        _write_json(path, payload)

    return {
        "file": str(path),
        "season_slug": slug,
        "changed": changed,
        "team_username_renames": rename_map,
        "team_username_renamed_count": renamed_count,
        "summary": summary,
    }


def _sync_global_auth(auth_path: Path, live_tables: dict, live_rename_map: dict, apply_changes: bool):
    if not auth_path.exists() or not isinstance(live_tables, dict):
        return {
            "file": str(auth_path),
            "changed": False,
            "manager_auth_renamed": 0,
            "manager_auth_created": 0,
            "skipped": True,
            "reason": "missing auth file or live tables",
        }

    payload = _load_json(auth_path)
    auth_users_table = payload.get("auth_users") if isinstance(payload.get("auth_users"), dict) else {}
    auth_users = {str(key): dict(value) for key, value in auth_users_table.items() if isinstance(value, dict)}

    live_users = list(live_tables.get("users", []))
    live_teams = list(live_tables.get("teams", []))

    team_name_by_id = {
        (team.get("id") or "").strip(): (team.get("name") or "").strip()
        for team in live_teams
        if (team.get("id") or "").strip()
    }

    manager_live_users = [
        user
        for user in live_users
        if (user.get("role") or "").strip().lower() == "manager"
    ]

    manager_auth_renamed = 0
    manager_auth_created = 0
    changed = False

    username_to_key = {
        (row.get("username") or "").strip(): key
        for key, row in auth_users.items()
        if (row.get("username") or "").strip()
    }

    max_doc_id = 0
    for key in auth_users.keys():
        if key.isdigit():
            max_doc_id = max(max_doc_id, int(key))

    # First, rename existing auth usernames using live table rename map.
    for old_username, new_username in (live_rename_map or {}).items():
        safe_old = (old_username or "").strip()
        safe_new = (new_username or "").strip()
        if not safe_old or not safe_new or safe_old == safe_new:
            continue

        old_key = username_to_key.get(safe_old)
        if not old_key:
            continue

        if safe_new in username_to_key:
            # Already present; keep old row untouched to avoid accidental overwrite.
            continue

        row = auth_users[old_key]
        row["username"] = safe_new
        username_to_key.pop(safe_old, None)
        username_to_key[safe_new] = old_key
        manager_auth_renamed += 1
        changed = True

    # Ensure every live manager account exists in global auth and has team display name.
    for user in manager_live_users:
        username = (user.get("username") or "").strip()
        if not username:
            continue

        team_name = team_name_by_id.get((user.get("team_id") or "").strip()) or (user.get("display_name") or username)
        existing_key = username_to_key.get(username)
        if existing_key:
            row = auth_users[existing_key]
            if row.get("display_name") != team_name:
                row["display_name"] = team_name
                changed = True
            if (row.get("role") or "").strip().lower() != "manager":
                row["role"] = "manager"
                changed = True
            continue

        max_doc_id += 1
        auth_users[str(max_doc_id)] = {
            "username": username,
            "password_hash": user.get("password_hash") or generate_password_hash("password123"),
            "role": "manager",
            "display_name": team_name,
        }
        username_to_key[username] = str(max_doc_id)
        manager_auth_created += 1
        changed = True

    # Keep legacy mirror table `users` aligned when present.
    if isinstance(payload.get("users"), dict):
        payload["users"] = {
            key: dict(value)
            for key, value in auth_users.items()
        }

    payload["auth_users"] = {
        key: value
        for key, value in sorted(auth_users.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 0)
    }

    if apply_changes and changed:
        _write_json(auth_path, payload)

    return {
        "file": str(auth_path),
        "changed": changed,
        "manager_auth_renamed": manager_auth_renamed,
        "manager_auth_created": manager_auth_created,
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill global player/team IDs across SCL JSON databases.")
    parser.add_argument("--workspace", default=".", help="Workspace root path")
    parser.add_argument("--apply", action="store_true", help="Apply writes. Default is dry-run")
    parser.add_argument(
        "--global-db",
        default="data/global_league_db.json",
        help="Global league database path relative to workspace unless absolute",
    )
    parser.add_argument(
        "--include-unpublished",
        action="store_true",
        help="Also process the live auction DB. Default processes season and published data only.",
    )
    parser.add_argument(
        "--include-snapshots",
        action="store_true",
        help="Include snapshot payload files under data/auction_snapshots (off by default).",
    )
    parser.add_argument(
        "--reset-global",
        action="store_true",
        help="Clear global league DB before processing (apply mode only).",
    )
    parser.add_argument(
        "--sync-auth",
        action="store_true",
        help="Sync global auth manager accounts from live auction manager users (requires --include-unpublished).",
    )
    parser.add_argument(
        "--rewrite-team-usernames",
        action="store_true",
        help="Rewrite manager/team usernames to team-name slugs before global linkage (off by default).",
    )
    args = parser.parse_args()

    workspace = Path(args.workspace).resolve()
    global_db_path = Path(args.global_db)
    if not global_db_path.is_absolute():
        global_db_path = workspace / global_db_path

    if args.reset_global and not args.apply:
        raise ValueError("--reset-global requires --apply")
    if args.sync_auth and not args.include_unpublished:
        raise ValueError("--sync-auth requires --include-unpublished")

    if args.apply:
        global_store_path = global_db_path
    else:
        temp_dir = Path(tempfile.mkdtemp(prefix="scl-global-dryrun-"))
        global_store_path = temp_dir / "global_league_db.json"
        if global_db_path.exists():
            shutil.copyfile(str(global_db_path), str(global_store_path))

    global_store = LockedTinyDB(str(global_store_path))

    if args.apply and args.reset_global:
        global_store.import_tables({})

    service = GlobalLeagueService(global_store)

    results = []

    if args.include_unpublished:
        live_db = workspace / "data" / "auction_live_db.json"
        if live_db.exists():
            results.append(
                _process_tinydb(
                    live_db,
                    "live",
                    service,
                    args.apply,
                    args.rewrite_team_usernames,
                )
            )

    season_dir = workspace / "data" / "season_dbs"
    for path in sorted(season_dir.glob("*.json")):
        results.append(
            _process_tinydb(
                path,
                path.stem.lower(),
                service,
                args.apply,
                args.rewrite_team_usernames,
            )
        )

    if args.include_snapshots:
        snapshot_dir = workspace / "data" / "auction_snapshots"
        for path in sorted(snapshot_dir.glob("*.json")):
            slug = path.stem.lower()
            try:
                payload = _load_json(path)
                slug = (payload.get("slug") or slug).strip().lower() or slug
            except Exception:  # noqa: BLE001
                pass
            results.append(
                _process_payload_file(
                    path,
                    slug,
                    service,
                    args.apply,
                    args.rewrite_team_usernames,
                )
            )

    published_dir = workspace / "published_sessions"
    if published_dir.exists():
        for path in sorted(published_dir.glob("*.json")):
            results.append(
                _process_payload_file(
                    path,
                    path.stem.lower(),
                    service,
                    args.apply,
                    args.rewrite_team_usernames,
                )
            )

    auth_sync_result = None
    if args.sync_auth:
        live_db = workspace / "data" / "auction_live_db.json"
        if live_db.exists():
            live_tables = LockedTinyDB(str(live_db)).export_tables()
            auth_path = workspace / "data" / "global_auth_db.json"
            live_rename_map = {}
            for item in results:
                if item.get("season_slug") == "live":
                    live_rename_map = dict(item.get("team_username_renames") or {})
                    break

            auth_sync_result = _sync_global_auth(auth_path, live_tables, live_rename_map, args.apply)

    changed_count = sum(1 for item in results if item.get("changed"))
    skipped_count = sum(1 for item in results if item.get("skipped"))

    output = {
        "mode": "apply" if args.apply else "dry-run",
        "workspace": str(workspace),
        "global_db": str(global_db_path),
        "global_db_runtime": str(global_store_path),
        "include_unpublished": bool(args.include_unpublished),
        "include_snapshots": bool(args.include_snapshots),
        "rewrite_team_usernames": bool(args.rewrite_team_usernames),
        "reset_global": bool(args.reset_global),
        "sync_auth": bool(args.sync_auth),
        "files_processed": len(results),
        "files_changed": changed_count,
        "files_skipped": skipped_count,
        "results": results,
    }

    if auth_sync_result is not None:
        output["auth_sync"] = auth_sync_result

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
