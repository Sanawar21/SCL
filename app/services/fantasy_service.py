import json
import secrets
from collections import Counter
from datetime import datetime
from pathlib import Path

from tinydb import Query

from app.rules import TIER_CREDIT_COST, TOTAL_CREDITS
from app.session_files import resolve_session_file


class FantasyService:
    def __init__(self, store, published_dir: str):
        self.store = store
        self.published_dir = Path(published_dir)

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

    def list_published_sessions(self):
        sessions = []
        if not self.published_dir.exists():
            return sessions

        for file_path in sorted(self.published_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            sessions.append(
                {
                    "slug": file_path.stem,
                    "name": payload.get("session_name") or file_path.stem,
                    "published_at": payload.get("saved_at"),
                }
            )
        return sessions

    def list_fantasy_seasons(self):
        with self.store.read() as db:
            seasons = sorted(
                db.table("fantasy_seasons").all(),
                key=lambda item: item.get("created_at") or "",
                reverse=True,
            )
            entries = db.table("fantasy_entries").all()

        entry_counts = Counter((entry.get("season_slug") or "") for entry in entries)
        for season in seasons:
            season["entry_count"] = entry_counts.get(season.get("slug"), 0)
        return seasons

    def get_season(self, slug: str):
        safe_slug = (slug or "").strip().lower()
        with self.store.read() as db:
            Season = Query()
            return db.table("fantasy_seasons").get(Season.slug == safe_slug)

    def create_fantasy_season(self, published_slug: str, name: str = ""):
        payload = self._load_published_payload(published_slug)
        slug = (published_slug or "").strip().lower()
        display_name = (name or "").strip() or payload.get("session_name") or slug

        with self.store.write() as db:
            seasons = db.table("fantasy_seasons")
            Season = Query()
            if seasons.get(Season.slug == slug):
                raise ValueError("Fantasy season already exists")

            season = {
                "id": secrets.token_hex(8),
                "slug": slug,
                "name": display_name,
                "published_slug": slug,
                "submissions_open": True,
                "created_at": datetime.utcnow().isoformat(),
            }
            seasons.insert(season)
            return season

    def set_submissions_open(self, season_slug: str, is_open: bool):
        safe_slug = (season_slug or "").strip().lower()
        with self.store.write() as db:
            Season = Query()
            seasons = db.table("fantasy_seasons")
            if not seasons.get(Season.slug == safe_slug):
                raise ValueError("Fantasy season not found")
            seasons.update({"submissions_open": bool(is_open)}, Season.slug == safe_slug)

    def delete_entry(self, entry_id: str):
        safe_entry_id = (entry_id or "").strip()
        if not safe_entry_id:
            raise ValueError("Missing entry id")

        with self.store.write() as db:
            Entry = Query()
            entries = db.table("fantasy_entries")
            if not entries.get(Entry.id == safe_entry_id):
                raise ValueError("Fantasy entry not found")
            entries.remove(Entry.id == safe_entry_id)

    def _season_players(self, season_slug: str):
        payload = self._load_published_payload(season_slug)
        tables = payload["tables"]

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
            manager_name = (
                (manager_user.get("display_name") or "").strip()
                or manager_username
            )

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

    def _eligible_lookup(self, season_slug: str):
        payload = self._load_published_payload(season_slug)
        tables = payload["tables"]
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
        seen = set()
        names = []
        for item in lookup.values():
            display_name = (item.get("entrant_display_name") or "").strip()
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
            "picks": picks,
            "total_credits": total_credits,
            "created_at": datetime.utcnow().isoformat(),
        }

        with self.store.write() as db:
            entries = db.table("fantasy_entries")
            Entry = Query()
            existing = entries.get(
                (Entry.season_slug == entry["season_slug"])
                & (Entry.entrant_key == entry["entrant_key"])
            )
            if existing:
                raise ValueError("You have already submitted a fantasy team for this season")

            entries.insert(entry)

        return entry

    def get_entries_for_season(self, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        with self.store.read() as db:
            Entry = Query()
            entries = db.table("fantasy_entries").search(Entry.season_slug == safe_slug)
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

        team_rankings = [
            {"team_name": team_name, "pick_points": points}
            for team_name, points in team_scores.items()
        ]
        team_rankings.sort(key=lambda item: (-item["pick_points"], item["team_name"].lower()))

        return {
            "entries": entries,
            "player_rankings": player_rankings,
            "team_rankings": team_rankings,
        }
