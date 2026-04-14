import secrets
from datetime import datetime, timezone


TIER_CREDIT_COST = {
    "platinum": 3,
    "gold": 2,
    "silver": 1,
}

TIER_BASE_PRICE = {
    "platinum": 1500,
    "gold": 800,
    "silver": 400,
}


class GlobalLeagueService:
    """Maintains global cross-season player/team identities and season linkage."""

    def __init__(self, store):
        self.store = store

    @staticmethod
    def _normalize_name(value: str) -> str:
        return " ".join((value or "").strip().lower().split())

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _ensure_global_player(self, db, player: dict):
        players = db.table("global_players")
        safe_name = (player.get("name") or "").strip()
        safe_tier = (player.get("tier") or "").strip().lower()
        safe_speciality = (player.get("speciality") or "ALL_ROUNDER").strip().upper()
        normalized_name = self._normalize_name(safe_name)

        existing = players.get(
            lambda row: self._normalize_name(row.get("name", "")) == normalized_name
            and (row.get("speciality") or "ALL_ROUNDER").strip().upper() == safe_speciality
        )

        now = self._now()
        if existing:
            players.update(
                {
                    "name": safe_name or existing.get("name"),
                    "tier": safe_tier or existing.get("tier") or "silver",
                    "speciality": safe_speciality,
                    "updated_at": now,
                },
                doc_ids=[existing.doc_id],
            )
            updated = dict(existing)
            updated.update(
                {
                    "name": safe_name or existing.get("name"),
                    "tier": safe_tier or existing.get("tier") or "silver",
                    "speciality": safe_speciality,
                    "updated_at": now,
                }
            )
            return updated, False

        global_player = {
            "id": secrets.token_hex(8),
            "name": safe_name,
            "tier": safe_tier or "silver",
            "speciality": safe_speciality,
            "created_at": now,
            "updated_at": now,
        }
        players.insert(global_player)
        return global_player, True

    def _ensure_global_team(self, db, team: dict):
        teams = db.table("global_teams")
        safe_name = (team.get("name") or "").strip()
        normalized_name = self._normalize_name(safe_name)

        existing = teams.get(lambda row: self._normalize_name(row.get("name", "")) == normalized_name)

        now = self._now()
        if existing:
            teams.update(
                {
                    "name": safe_name or existing.get("name"),
                    "updated_at": now,
                },
                doc_ids=[existing.doc_id],
            )
            updated = dict(existing)
            updated.update(
                {
                    "name": safe_name or existing.get("name"),
                    "updated_at": now,
                }
            )
            return updated, False

        global_team = {
            "id": secrets.token_hex(8),
            "name": safe_name,
            "created_at": now,
            "updated_at": now,
        }
        teams.insert(global_team)
        return global_team, True

    def _ensure_manager_players_in_tables(self, tables: dict):
        teams_in = list(tables.get("teams", []))
        players_in = list(tables.get("players", []))
        users_in = list(tables.get("users", []))

        users_by_username = {
            (user.get("username") or "").strip().lower(): user
            for user in users_in
            if (user.get("username") or "").strip()
        }

        players_by_id = {
            (player.get("id") or "").strip(): player
            for player in players_in
            if (player.get("id") or "").strip()
        }
        players_by_name = {}
        for player in players_in:
            normalized = self._normalize_name(player.get("name", ""))
            if normalized and normalized not in players_by_name:
                players_by_name[normalized] = player

        changed = False
        for team in teams_in:
            team_id = (team.get("id") or "").strip()
            if not team_id:
                continue

            manager_player_id = (team.get("manager_player_id") or "").strip()
            if manager_player_id and manager_player_id in players_by_id:
                manager_player = players_by_id[manager_player_id]
                team["manager_tier"] = (manager_player.get("tier") or team.get("manager_tier") or "silver").strip().lower()
                continue

            manager_username = (team.get("manager_username") or "").strip()
            manager_user = users_by_username.get(manager_username.lower(), {}) if manager_username else {}
            manager_name = (manager_user.get("display_name") or manager_username or "Manager").strip()
            manager_speciality = (manager_user.get("speciality") or "ALL_ROUNDER").strip().upper()

            existing_player = players_by_name.get(self._normalize_name(manager_name))
            if not existing_player and manager_username:
                existing_player = players_by_name.get(self._normalize_name(manager_username))

            if existing_player:
                manager_player_id = (existing_player.get("id") or "").strip()
            else:
                manager_tier = (team.get("manager_tier") or "silver").strip().lower() or "silver"
                manager_player_id = secrets.token_hex(8)
                new_player = {
                    "id": manager_player_id,
                    "name": manager_name,
                    "tier": manager_tier,
                    "base_price": TIER_BASE_PRICE.get(manager_tier, 400),
                    "status": "sold",
                    "sold_to": team_id,
                    "sold_price": 0,
                    "phase_sold": None,
                    "credits": TIER_CREDIT_COST.get(manager_tier, 1),
                    "current_bid": 0,
                    "current_bidder_team_id": None,
                    "nominated_phase_a": False,
                    "speciality": manager_speciality,
                    "manager_team_id": team_id,
                }
                players_in.append(new_player)
                players_by_id[manager_player_id] = new_player
                players_by_name[self._normalize_name(manager_name)] = new_player
                changed = True

            if manager_player_id:
                team["manager_player_id"] = manager_player_id
                manager_player = players_by_id.get(manager_player_id)
                if manager_player:
                    team["manager_tier"] = (manager_player.get("tier") or team.get("manager_tier") or "silver").strip().lower()
                    if (manager_player.get("status") or "").strip().lower() == "unsold":
                        manager_player["status"] = "sold"
                    if not manager_player.get("sold_to"):
                        manager_player["sold_to"] = team_id
                    manager_player.setdefault("sold_price", 0)
                    manager_player.setdefault("phase_sold", None)
                    manager_player.setdefault("current_bid", 0)
                    manager_player.setdefault("current_bidder_team_id", None)
                    manager_player["manager_team_id"] = team_id
                changed = True

        if changed:
            tables = dict(tables)
            tables["players"] = players_in
            tables["teams"] = teams_in
        return tables, changed

    def _rewrite_fantasy_entries(self, tables: dict):
        fantasy_entries = list(tables.get("fantasy_entries", []))
        if not fantasy_entries:
            return tables, 0

        teams = list(tables.get("teams", []))
        players = list(tables.get("players", []))

        manager_alias_to_player_id = {}
        for team in teams:
            manager_username = self._normalize_name(team.get("manager_username") or "")
            manager_player_id = (team.get("manager_player_id") or "").strip()
            if manager_username and manager_player_id:
                manager_alias_to_player_id[manager_username] = manager_player_id

        players_by_id = {
            (player.get("id") or "").strip(): player
            for player in players
            if (player.get("id") or "").strip()
        }
        players_by_name = {
            self._normalize_name(player.get("name") or ""): (player.get("id") or "").strip()
            for player in players
            if self._normalize_name(player.get("name") or "")
        }

        rewritten_entries = 0
        patched_entries = []
        for entry in fantasy_entries:
            patched_entry = dict(entry)
            changed = False

            entrant_key = (patched_entry.get("entrant_key") or "").strip()
            if entrant_key.startswith("manager:"):
                alias = self._normalize_name(entrant_key.split(":", 1)[1])
                manager_player_id = manager_alias_to_player_id.get(alias)
                if not manager_player_id:
                    manager_player_id = players_by_name.get(alias)
                if manager_player_id:
                    player_name = (players_by_id.get(manager_player_id, {}) or {}).get("name") or patched_entry.get("entrant_name") or alias
                    patched_entry["entrant_key"] = f"player:{self._normalize_name(player_name)}"
                    patched_entry["entrant_name"] = player_name
                    changed = True

            picks = []
            for pick in patched_entry.get("picks", []) or []:
                patched_pick = dict(pick)
                player_id = (patched_pick.get("player_id") or "").strip()
                if player_id.startswith("manager::"):
                    alias = self._normalize_name(player_id.split("::", 1)[1])
                    manager_player_id = manager_alias_to_player_id.get(alias)
                    if not manager_player_id:
                        manager_player_id = players_by_name.get(alias)
                    if not manager_player_id:
                        manager_player_id = players_by_name.get(self._normalize_name(patched_pick.get("player_name") or ""))

                    if manager_player_id and manager_player_id in players_by_id:
                        manager_player = players_by_id[manager_player_id]
                        patched_pick["player_id"] = manager_player_id
                        patched_pick["player_name"] = manager_player.get("name") or patched_pick.get("player_name")
                        patched_pick["tier"] = manager_player.get("tier") or patched_pick.get("tier")
                        patched_pick["credits"] = TIER_CREDIT_COST.get((manager_player.get("tier") or "").strip().lower(), patched_pick.get("credits", 0))
                        changed = True
                picks.append(patched_pick)

            if changed:
                patched_entry["picks"] = picks
                patched_entry["team_signature"] = "|".join(sorted(
                    (pick.get("player_id") or "").strip()
                    for pick in picks
                    if (pick.get("player_id") or "").strip()
                ))
                rewritten_entries += 1

            patched_entries.append(patched_entry)

        if rewritten_entries:
            tables = dict(tables)
            tables["fantasy_entries"] = patched_entries
        return tables, rewritten_entries

    def apply_global_ids(self, season_slug: str, tables: dict, published_at: str | None = None):
        """Returns tables patched with global ids and persists season linkage rows."""
        if not isinstance(tables, dict):
            raise ValueError("Invalid tables payload")

        tables, manager_backfill_changed = self._ensure_manager_players_in_tables(tables)
        tables, fantasy_entries_rewritten = self._rewrite_fantasy_entries(tables)

        safe_slug = (season_slug or "").strip().lower()
        players_in = list(tables.get("players", []))
        teams_in = list(tables.get("teams", []))

        player_global_map = {}
        team_global_map = {}
        manager_player_map = {}

        created_global_players = 0
        created_global_teams = 0

        players_by_id = {
            (player.get("id") or "").strip(): player
            for player in players_in
            if (player.get("id") or "").strip()
        }

        with self.store.write() as db:
            season_player_links = db.table("season_player_links")
            season_team_links = db.table("season_team_links")
            season_team_rosters = db.table("season_team_rosters")

            for player in players_in:
                local_player_id = (player.get("id") or "").strip()
                if not local_player_id:
                    continue

                global_player, created = self._ensure_global_player(db, player)
                created_global_players += 1 if created else 0
                player_global_map[local_player_id] = global_player["id"]

                row = {
                    "season_slug": safe_slug,
                    "local_player_id": local_player_id,
                    "global_player_id": global_player["id"],
                    "player_name": player.get("name") or "",
                    "tier": (player.get("tier") or "").strip().lower(),
                    "speciality": (player.get("speciality") or "ALL_ROUNDER").strip().upper(),
                    "published_at": published_at,
                    "updated_at": self._now(),
                }
                existing = season_player_links.get(
                    lambda item: item.get("season_slug") == safe_slug
                    and item.get("local_player_id") == local_player_id
                )
                if existing:
                    season_player_links.update(row, doc_ids=[existing.doc_id])
                else:
                    season_player_links.insert(row)

            # Keep manager linking stable even before manager_player_id is fully rolled out.
            players_by_name = {}
            for player in players_in:
                key = self._normalize_name(player.get("name", ""))
                if key and key not in players_by_name:
                    players_by_name[key] = (player.get("id") or "").strip()

            for team in teams_in:
                local_team_id = (team.get("id") or "").strip()
                if not local_team_id:
                    continue

                global_team, created = self._ensure_global_team(db, team)
                created_global_teams += 1 if created else 0
                team_global_map[local_team_id] = global_team["id"]

                manager_player_id = (team.get("manager_player_id") or "").strip()
                if not manager_player_id:
                    manager_username = self._normalize_name(team.get("manager_username") or "")
                    manager_player_id = players_by_name.get(manager_username, "")
                if manager_player_id:
                    manager_player_map[local_team_id] = manager_player_id

                manager_global_player_id = player_global_map.get(manager_player_id)

                row = {
                    "season_slug": safe_slug,
                    "local_team_id": local_team_id,
                    "global_team_id": global_team["id"],
                    "team_name": team.get("name") or "",
                    "manager_player_id": manager_player_id or None,
                    "manager_global_player_id": manager_global_player_id,
                    "published_at": published_at,
                    "updated_at": self._now(),
                }
                existing = season_team_links.get(
                    lambda item: item.get("season_slug") == safe_slug and item.get("local_team_id") == local_team_id
                )
                if existing:
                    season_team_links.update(row, doc_ids=[existing.doc_id])
                else:
                    season_team_links.insert(row)

                local_roster_ids = []
                for key in ("players", "bench"):
                    for player_id in team.get(key, []) or []:
                        safe_id = (player_id or "").strip()
                        if safe_id and safe_id not in local_roster_ids:
                            local_roster_ids.append(safe_id)
                if manager_player_id and manager_player_id not in local_roster_ids:
                    local_roster_ids.append(manager_player_id)

                global_roster_ids = [
                    player_global_map[player_id]
                    for player_id in local_roster_ids
                    if player_id in player_global_map
                ]

                roster_row = {
                    "season_slug": safe_slug,
                    "local_team_id": local_team_id,
                    "global_team_id": global_team["id"],
                    "local_player_ids": local_roster_ids,
                    "global_player_ids": global_roster_ids,
                    "updated_at": self._now(),
                }
                existing_roster = season_team_rosters.get(
                    lambda item: item.get("season_slug") == safe_slug and item.get("local_team_id") == local_team_id
                )
                if existing_roster:
                    season_team_rosters.update(roster_row, doc_ids=[existing_roster.doc_id])
                else:
                    season_team_rosters.insert(roster_row)

        patched_players = []
        for player in players_in:
            patched = dict(player)
            local_player_id = (player.get("id") or "").strip()
            if local_player_id and local_player_id in player_global_map:
                patched["global_player_id"] = player_global_map[local_player_id]
            patched_players.append(patched)

        patched_teams = []
        for team in teams_in:
            patched = dict(team)
            local_team_id = (team.get("id") or "").strip()
            if local_team_id and local_team_id in team_global_map:
                patched["global_team_id"] = team_global_map[local_team_id]

            manager_player_id = (patched.get("manager_player_id") or "").strip()
            if not manager_player_id:
                manager_player_id = manager_player_map.get(local_team_id, "")
                if manager_player_id:
                    patched["manager_player_id"] = manager_player_id

            if manager_player_id:
                patched["manager_global_player_id"] = player_global_map.get(manager_player_id)
                manager_player = players_by_id.get(manager_player_id)
                if manager_player:
                    patched["manager_tier"] = (manager_player.get("tier") or patched.get("manager_tier") or "silver").strip().lower()

            patched_teams.append(patched)

        patched_tables = dict(tables)
        patched_tables["players"] = patched_players
        patched_tables["teams"] = patched_teams

        summary = {
            "season_slug": safe_slug,
            "players_linked": len(player_global_map),
            "teams_linked": len(team_global_map),
            "created_global_players": created_global_players,
            "created_global_teams": created_global_teams,
            "manager_backfill_changed": manager_backfill_changed,
            "fantasy_entries_rewritten": fantasy_entries_rewritten,
        }
        return patched_tables, summary
