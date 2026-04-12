import json
import secrets
from collections import Counter
from datetime import datetime
from pathlib import Path

from tinydb import Query

from app.rules import TIER_CREDIT_COST, TOTAL_CREDITS
from app.session_files import resolve_session_file


class FantasyService:
    def __init__(self, global_store, published_dir: str, season_store_manager):
        self.global_store = global_store
        self.published_dir = Path(published_dir)
        self.season_store_manager = season_store_manager

    def _load_published_payload(self, slug: str):
        safe_slug = (slug or "").strip().lower()
        if not safe_slug:
            raise ValueError("Missing season slug")
        file_path = resolve_session_file(self.published_dir, f"{safe_slug}.json")
        if not file_path.exists():
            raise ValueError("Published season does not exist")
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload.get("tables"), dict):
            raise ValueError("Published season is invalid")
        return payload

    def _ensure_season_store(self, slug: str):
        safe_slug = (slug or "").strip().lower()
        if self.season_store_manager.has_season(safe_slug):
            return self.season_store_manager.get_store(safe_slug, create=False)

        payload = self._load_published_payload(safe_slug)
        store = self.season_store_manager.get_store(safe_slug, create=True)
        tables_payload = {
            table_name: rows
            for table_name, rows in (payload.get("tables") or {}).items()
            if table_name != "bids"
        }
        store.import_tables(tables_payload)

        with store.write() as db:
            meta_table = db.table("season_meta")
            meta_payload = {
                "slug": safe_slug,
                "name": payload.get("session_name") or safe_slug,
                "published": bool(payload.get("published", True)),
                "published_file": f"{safe_slug}.json",
                "published_at": payload.get("saved_at") or datetime.utcnow().isoformat(),
                "created_at": datetime.utcnow().isoformat(),
                "submissions_open": False,
            }
            if meta_table.get(doc_id=1):
                meta_table.update(meta_payload, doc_ids=[1])
            else:
                meta_table.insert(meta_payload)

        return store

    def _get_store_if_exists(self, slug: str):
        safe_slug = (slug or "").strip().lower()
        if not self.season_store_manager.has_season(safe_slug):
            return None
        return self.season_store_manager.get_store(safe_slug, create=False)

    def _load_season_tables(self, slug: str):
        store = self._get_store_if_exists(slug)
        if store:
            tables = store.export_tables()
            if isinstance(tables, dict) and tables.get("players") and tables.get("teams"):
                return tables

        payload = self._load_published_payload(slug)
        return payload.get("tables") or {}

    def _get_season_meta(self, slug: str):
        store = self._get_store_if_exists(slug)
        if not store:
            return None
        with store.read() as db:
            return db.table("season_meta").get(doc_id=1)

    def _get_enabled_season_store(self, slug: str):
        safe_slug = (slug or "").strip().lower()
        store = self._get_store_if_exists(safe_slug)
        if not store:
            raise ValueError("Fantasy season not found")

        return store

    def list_published_sessions(self):
        sessions_by_slug = {}

        for season_slug in self.season_store_manager.list_slugs():
            meta = self._get_season_meta(season_slug) or {}
            sessions_by_slug[season_slug] = {
                "slug": season_slug,
                "name": meta.get("name") or season_slug,
                "published_at": meta.get("published_at"),
            }

        if self.published_dir.exists():
            for file_path in sorted(self.published_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
                slug = file_path.stem
                if slug in sessions_by_slug:
                    continue
                try:
                    payload = json.loads(file_path.read_text(encoding="utf-8"))
                except Exception:  # noqa: BLE001
                    continue
                sessions_by_slug[slug] = {
                    "slug": slug,
                    "name": payload.get("session_name") or slug,
                    "published_at": payload.get("saved_at"),
                }

        sessions = list(sessions_by_slug.values())
        sessions.sort(key=lambda item: item.get("published_at") or "", reverse=True)
        return sessions

    def list_fantasy_seasons(self):
        seasons = []
        for season_slug in self.season_store_manager.list_slugs():
            store = self._get_store_if_exists(season_slug)
            if not store:
                continue

            with store.read() as db:
                meta = db.table("season_meta").get(doc_id=1) or {}
                entry_count = len(db.table("fantasy_entries"))

            seasons.append(
                {
                    "id": meta.get("slug") or season_slug,
                    "slug": season_slug,
                    "name": meta.get("name") or season_slug,
                    "published_slug": season_slug,
                    "submissions_open": bool(meta.get("submissions_open", False)),
                    "created_at": meta.get("created_at") or "",
                    "entry_count": entry_count,
                }
            )

        seasons.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return seasons

    def get_season(self, slug: str):
        safe_slug = (slug or "").strip().lower()
        store = self._get_store_if_exists(safe_slug)
        if not store:
            return None

        with store.read() as db:
            meta = db.table("season_meta").get(doc_id=1) or {}

        with store.read() as db:
            entry_count = len(db.table("fantasy_entries"))

        return {
            "id": meta.get("slug") or safe_slug,
            "slug": safe_slug,
            "name": meta.get("name") or safe_slug,
            "published_slug": safe_slug,
            "submissions_open": bool(meta.get("submissions_open", False)),
            "created_at": meta.get("created_at") or "",
            "entry_count": entry_count,
        }

    def create_fantasy_season(self, published_slug: str, name: str = ""):
        safe_slug = (published_slug or "").strip().lower()
        if not safe_slug:
            raise ValueError("Published season slug is required")

        store = self._ensure_season_store(safe_slug)
        published_payload = None
        display_name = (name or "").strip()

        if not display_name:
            try:
                published_payload = self._load_published_payload(safe_slug)
            except Exception:  # noqa: BLE001
                published_payload = None

        with store.write() as db:
            meta_table = db.table("season_meta")
            existing_meta = meta_table.get(doc_id=1) or {}

            final_name = (
                display_name
                or existing_meta.get("name")
                or (published_payload or {}).get("session_name")
                or safe_slug
            )

            season_meta = {
                "slug": safe_slug,
                "name": final_name,
                "published": True,
                "published_file": existing_meta.get("published_file") or f"{safe_slug}.json",
                "published_at": existing_meta.get("published_at") or (published_payload or {}).get("saved_at") or datetime.utcnow().isoformat(),
                "created_at": existing_meta.get("created_at") or datetime.utcnow().isoformat(),
                "submissions_open": True,
            }

            if meta_table.get(doc_id=1):
                meta_table.update(season_meta, doc_ids=[1])
            else:
                meta_table.insert(season_meta)

        return {
            "id": safe_slug,
            "slug": safe_slug,
            "name": final_name,
            "published_slug": safe_slug,
            "submissions_open": True,
            "created_at": season_meta["created_at"],
            "entry_count": 0,
        }

    def set_submissions_open(self, season_slug: str, is_open: bool):
        store = self._get_enabled_season_store(season_slug)

        with store.write() as db:
            meta_table = db.table("season_meta")
            meta = meta_table.get(doc_id=1)
            if not meta:
                raise ValueError("Fantasy season not found")
            meta_table.update({"submissions_open": bool(is_open)}, doc_ids=[1])

    def delete_entry(self, season_slug: str, entry_id: str):
        safe_entry_id = (entry_id or "").strip()
        if not safe_entry_id:
            raise ValueError("Missing entry id")

        store = self._get_enabled_season_store(season_slug)
        with store.write() as db:
            Entry = Query()
            entries = db.table("fantasy_entries")
            if not entries.get(Entry.id == safe_entry_id):
                raise ValueError("Fantasy entry not found")
            entries.remove(Entry.id == safe_entry_id)

    def _season_players(self, season_slug: str):
        tables = self._load_season_tables(season_slug)

        users_by_username = {
            (user.get("username") or "").strip(): user
            for user in tables.get("users", [])
            if (user.get("username") or "").strip()
        }

        teams_by_id = {
            team.get("id"): team.get("name")
            for team in tables.get("teams", [])
            if team.get("id")
        }

        players = []
        for player in tables.get("players", []):
            tier = (player.get("tier") or "").strip().lower()
            players.append(
                {
                    "id": player.get("id"),
                    "name": player.get("name") or "Unknown",
                    "tier": tier,
                    "credits": TIER_CREDIT_COST.get(tier, 0),
                    "auction_team_id": player.get("sold_to"),
                    "auction_team_name": teams_by_id.get(player.get("sold_to")) or "-",
                    "selection_type": "player",
                }
            )

        for team in tables.get("teams", []):
            manager_username = (team.get("manager_username") or "").strip()
            if not manager_username:
                continue

            manager_user = users_by_username.get(manager_username, {})
            manager_name = ((manager_user.get("display_name") or "").strip() or manager_username)

            manager_tier = (team.get("manager_tier") or "").strip().lower()
            manager_credits = TIER_CREDIT_COST.get(manager_tier, 0)

            players.append(
                {
                    "id": f"manager::{manager_username.lower()}",
                    "name": manager_name,
                    "tier": manager_tier,
                    "credits": manager_credits,
                    "auction_team_id": team.get("id"),
                    "auction_team_name": team.get("name") or "-",
                    "selection_type": "manager",
                }
            )

        players.sort(
            key=lambda item: (
                0 if item.get("selection_type") == "player" else 1,
                (item.get("name") or "").lower(),
            )
        )
        return players

    def get_season_players(self, season_slug: str):
        return self._season_players(season_slug)

    @staticmethod
    def _normalize(value: str):
        return " ".join((value or "").strip().lower().split())

    @staticmethod
    def _team_signature(player_ids):
        normalized_ids = sorted((player_id or "").strip() for player_id in (player_ids or []) if (player_id or "").strip())
        return "|".join(normalized_ids)

    def _entry_team_signature(self, entry: dict) -> str:
        signature = (entry or {}).get("team_signature")
        if signature:
            return str(signature)
        picks = entry.get("picks") or []
        pick_ids = [pick.get("player_id") for pick in picks if pick.get("player_id")]
        return self._team_signature(pick_ids)

    def _eligible_lookup(self, season_slug: str):
        tables = self._load_season_tables(season_slug)
        lookup = {}

        for user in tables.get("users", []):
            if user.get("role") != "manager":
                continue
            username = (user.get("username") or "").strip()
            display_name = (user.get("display_name") or username).strip()
            if not username:
                continue

            canonical = f"manager:{self._normalize(username)}"
            for alias in {username, display_name}:
                normalized_alias = self._normalize(alias)
                if normalized_alias:
                    lookup[normalized_alias] = {
                        "entrant_key": canonical,
                        "entrant_display_name": display_name,
                    }

        for player in tables.get("players", []):
            name = (player.get("name") or "").strip()
            if not name:
                continue
            normalized_name = self._normalize(name)
            if not normalized_name:
                continue
            lookup[normalized_name] = {
                "entrant_key": f"player:{normalized_name}",
                "entrant_display_name": name,
            }

        return lookup

    def get_eligible_entrant_names(self, season_slug: str):
        lookup = self._eligible_lookup(season_slug)
        submitted_keys = set()
        submitted_names = set()

        store = self._get_store_if_exists(season_slug)
        if store:
            with store.read() as db:
                entries = db.table("fantasy_entries").all()
            for entry in entries:
                entrant_key = (entry.get("entrant_key") or "").strip()
                if entrant_key:
                    submitted_keys.add(entrant_key)
                normalized_existing_name = self._normalize(entry.get("entrant_name") or "")
                if normalized_existing_name:
                    submitted_names.add(normalized_existing_name)

        seen = set()
        names = []
        for item in lookup.values():
            entrant_key = (item.get("entrant_key") or "").strip()
            display_name = (item.get("entrant_display_name") or "").strip()
            normalized_display_name = self._normalize(display_name)

            if entrant_key and entrant_key in submitted_keys:
                continue
            if normalized_display_name and normalized_display_name in submitted_names:
                continue

            key = self._normalize(display_name)
            if not key or key in seen:
                continue
            seen.add(key)
            names.append(display_name)
        return sorted(names, key=lambda value: value.lower())

    def submit_entry(self, season_slug: str, entrant_name: str, player_ids):
        season = self.get_season(season_slug)
        if not season:
            raise ValueError("Fantasy season not found")
        if not season.get("submissions_open"):
            raise ValueError("Submissions are closed for this fantasy season")

        normalized_name = self._normalize(entrant_name)
        if not normalized_name:
            raise ValueError("Entrant name is required")

        eligible_lookup = self._eligible_lookup(season_slug)
        entrant = eligible_lookup.get(normalized_name)
        if not entrant:
            raise ValueError("Only league players/managers can submit fantasy teams")

        unique_player_ids = []
        seen = set()
        for player_id in player_ids or []:
            safe_id = (player_id or "").strip()
            if not safe_id or safe_id in seen:
                continue
            seen.add(safe_id)
            unique_player_ids.append(safe_id)

        if not unique_player_ids:
            raise ValueError("Select at least one player")
        if len(unique_player_ids) != 4:
            raise ValueError("Fantasy team must contain exactly 4 selections")

        team_signature = self._team_signature(unique_player_ids)

        players = self._season_players(season_slug)
        players_by_id = {player["id"]: player for player in players if player.get("id")}

        picks = []
        total_credits = 0
        for player_id in unique_player_ids:
            player = players_by_id.get(player_id)
            if not player:
                raise ValueError("One or more selected players are invalid")
            picks.append(
                {
                    "player_id": player["id"],
                    "player_name": player["name"],
                    "tier": player["tier"],
                    "credits": player["credits"],
                }
            )
            total_credits += int(player["credits"])

        if total_credits > TOTAL_CREDITS:
            raise ValueError(f"Total credits exceed {TOTAL_CREDITS}")

        entry = {
            "id": secrets.token_hex(8),
            "season_slug": (season_slug or "").strip().lower(),
            "entrant_name": entrant["entrant_display_name"],
            "entrant_key": entrant["entrant_key"],
            "team_signature": team_signature,
            "picks": picks,
            "total_credits": total_credits,
            "created_at": datetime.utcnow().isoformat(),
        }

        store = self._get_enabled_season_store(season_slug)
        with store.write() as db:
            entries = db.table("fantasy_entries")
            existing_entries = entries.all()
            normalized_entrant_name = self._normalize(entry["entrant_name"])

            conflicting_entry = None
            for existing in existing_entries:
                existing_signature = self._entry_team_signature(existing)
                if existing_signature and not (existing.get("team_signature") or "").strip():
                    doc_id = getattr(existing, "doc_id", None)
                    if doc_id is not None:
                        entries.update({"team_signature": existing_signature}, doc_ids=[doc_id])

                if existing_signature == team_signature:
                    conflicting_entry = existing
                    break

            if conflicting_entry:
                conflict_name = (conflicting_entry.get("entrant_name") or "another entrant").strip() or "another entrant"
                raise ValueError(f"Your team conflicts with {conflict_name}'s squad")

            duplicate = next(
                (
                    existing
                    for existing in existing_entries
                    if (existing.get("entrant_key") or "").strip() == entry["entrant_key"]
                    or self._normalize(existing.get("entrant_name") or "") == normalized_entrant_name
                ),
                None,
            )
            if duplicate:
                raise ValueError("You have already submitted a fantasy team for this season")

            entries.insert(entry)

        return entry

    def get_entries_for_season(self, season_slug: str):
        store = self._get_enabled_season_store(season_slug)
        with store.read() as db:
            entries = db.table("fantasy_entries").all()
        return sorted(entries, key=lambda item: item.get("created_at") or "", reverse=True)

    def get_rankings(self, season_slug: str):
        players = self._season_players(season_slug)
        players_by_id = {player["id"]: player for player in players if player.get("id")}
        entries = self.get_entries_for_season(season_slug)

        pick_counter = Counter()
        for entry in entries:
            for pick in entry.get("picks", []):
                player_id = pick.get("player_id")
                if player_id:
                    pick_counter[player_id] += 1

        player_rankings = []
        for player in players:
            player_rankings.append(
                {
                    "player_id": player["id"],
                    "player_name": player["name"],
                    "tier": player["tier"],
                    "credits": player["credits"],
                    "auction_team_name": player["auction_team_name"],
                    "pick_count": pick_counter.get(player["id"], 0),
                }
            )

        player_rankings.sort(key=lambda item: (-item["pick_count"], item["player_name"].lower()))

        team_scores = Counter()
        for player_id, count in pick_counter.items():
            team_name = players_by_id.get(player_id, {}).get("auction_team_name") or "Unassigned"
            team_scores[team_name] += count

        team_rankings = [{"team_name": team_name, "pick_points": points} for team_name, points in team_scores.items()]
        team_rankings.sort(key=lambda item: (-item["pick_points"], item["team_name"].lower()))

        return {
            "entries": entries,
            "player_rankings": player_rankings,
            "team_rankings": team_rankings,
        }
