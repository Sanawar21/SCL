import secrets
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

from app.rules import TIER_CREDIT_COST, TOTAL_CREDITS


class FantasyService:
    def __init__(self, global_store, published_dir: str, season_store_manager):
        self.global_store = global_store
        self.published_dir = Path(published_dir)
        self.season_store_manager = season_store_manager
        self._db_path = getattr(getattr(global_store, "db", None), "path", None)

    def _connect(self):
        if not self._db_path:
            raise RuntimeError("Fantasy database path is not configured")
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _parse_metadata(raw_value):
        if not raw_value:
            return {}
        try:
            import json

            parsed = json.loads(raw_value)
        except Exception:  # noqa: BLE001
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _dump_metadata(payload):
        import json

        return json.dumps(payload or {}, ensure_ascii=True)

    def _season_row(self, conn: sqlite3.Connection, slug: str):
        safe_slug = (slug or "").strip().lower()
        if not safe_slug:
            return None
        return conn.execute(
            """
            SELECT id, slug, name, created_at, published_at, metadata_json
            FROM seasons
            WHERE slug = ? OR id = ?
            LIMIT 1
            """,
            (safe_slug, safe_slug),
        ).fetchone()

    def _season_metadata(self, season_row):
        return self._parse_metadata(season_row["metadata_json"]) if season_row else {}

    def _published_auction(self, conn: sqlite3.Connection, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        return conn.execute(
            """
            SELECT a.id,
                   a.season_id,
                   a.saved_at,
                   a.created_at,
                   s.slug,
                   s.name,
                   s.published_at,
                   s.metadata_json AS season_metadata_json
            FROM auctions a
            JOIN seasons s ON s.id = a.season_id
            WHERE a.status = 'published'
              AND (s.slug = ? OR s.id = ?)
            ORDER BY COALESCE(a.saved_at, '') DESC, COALESCE(a.created_at, '') DESC, a.id DESC
            LIMIT 1
            """,
            (safe_slug, safe_slug),
        ).fetchone()

    def _require_published_auction(self, conn: sqlite3.Connection, season_slug: str):
        row = self._published_auction(conn, season_slug)
        if not row:
            raise ValueError("Published season does not exist")
        return row

    def _entry_count(self, conn: sqlite3.Connection, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM fantasy_teams WHERE season_id = ?",
            (safe_slug,),
        ).fetchone()
        return int(row["count"] if row else 0)

    def _season_payload(self, conn: sqlite3.Connection, season_row):
        if not season_row:
            return None
        slug = (season_row["slug"] or season_row["id"] or "").strip().lower()
        metadata = self._season_metadata(season_row)
        entry_count = self._entry_count(conn, slug)
        return {
            "id": slug,
            "slug": slug,
            "name": (season_row["name"] or slug),
            "published_slug": slug,
            "submissions_open": bool(metadata.get("submissions_open", False)),
            "created_at": metadata.get("fantasy_created_at") or season_row["created_at"] or "",
            "entry_count": entry_count,
        }

    def list_published_sessions(self):
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT s.slug,
                       s.name,
                       a.saved_at,
                       s.published_at,
                       a.id AS auction_id
                FROM seasons s
                JOIN auctions a ON a.season_id = s.id
                WHERE a.status = 'published'
                ORDER BY COALESCE(a.saved_at, s.published_at, '') DESC, a.id DESC
                """
            ).fetchall()

        sessions = []
        seen = set()
        for row in rows:
            slug = (row["slug"] or "").strip().lower()
            if not slug or slug in seen:
                continue
            seen.add(slug)
            sessions.append(
                {
                    "slug": slug,
                    "name": (row["name"] or slug),
                    "published_at": row["saved_at"] or row["published_at"],
                }
            )
        return sessions

    def list_fantasy_seasons(self):
        published = self.list_published_sessions()
        with self._connect() as conn:
            seasons = []
            for item in published:
                season_row = self._season_row(conn, item["slug"])
                payload = self._season_payload(conn, season_row)
                if payload:
                    seasons.append(payload)
        seasons.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return seasons

    def get_season(self, slug: str):
        safe_slug = (slug or "").strip().lower()
        with self._connect() as conn:
            if not self._published_auction(conn, safe_slug):
                return None
            season_row = self._season_row(conn, safe_slug)
            return self._season_payload(conn, season_row)

    def create_fantasy_season(self, published_slug: str, name: str = ""):
        safe_slug = (published_slug or "").strip().lower()
        if not safe_slug:
            raise ValueError("Published season slug is required")

        display_name = (name or "").strip()

        with self._connect() as conn:
            published = self._require_published_auction(conn, safe_slug)
            season_row = self._season_row(conn, safe_slug)
            if not season_row:
                raise ValueError("Published season does not exist")

            metadata = self._season_metadata(season_row)
            metadata["submissions_open"] = True
            metadata.setdefault("fantasy_created_at", datetime.utcnow().isoformat())

            final_name = display_name or season_row["name"] or safe_slug

            conn.execute(
                """
                UPDATE seasons
                SET name = ?,
                    metadata_json = ?
                WHERE id = ?
                """,
                (final_name, self._dump_metadata(metadata), season_row["id"]),
            )
            conn.commit()

            return {
                "id": safe_slug,
                "slug": safe_slug,
                "name": final_name,
                "published_slug": safe_slug,
                "submissions_open": True,
                "created_at": metadata.get("fantasy_created_at") or season_row["created_at"] or datetime.utcnow().isoformat(),
                "entry_count": self._entry_count(conn, published["season_id"]),
            }

    def set_submissions_open(self, season_slug: str, is_open: bool):
        safe_slug = (season_slug or "").strip().lower()
        with self._connect() as conn:
            if not self._published_auction(conn, safe_slug):
                raise ValueError("Fantasy season not found")
            season_row = self._season_row(conn, safe_slug)
            if not season_row:
                raise ValueError("Fantasy season not found")
            metadata = self._season_metadata(season_row)
            metadata["submissions_open"] = bool(is_open)
            metadata.setdefault("fantasy_created_at", datetime.utcnow().isoformat())
            conn.execute(
                """
                UPDATE seasons
                SET metadata_json = ?
                WHERE id = ?
                """,
                (self._dump_metadata(metadata), season_row["id"]),
            )
            conn.commit()

    def delete_entry(self, season_slug: str, entry_id: str):
        safe_slug = (season_slug or "").strip().lower()
        safe_entry_id = (entry_id or "").strip()
        if not safe_entry_id:
            raise ValueError("Missing entry id")

        with self._connect() as conn:
            if not self._published_auction(conn, safe_slug):
                raise ValueError("Fantasy season not found")

            existing = conn.execute(
                """
                SELECT id
                FROM fantasy_teams
                WHERE id = ? AND season_id = ?
                LIMIT 1
                """,
                (safe_entry_id, safe_slug),
            ).fetchone()
            if not existing:
                raise ValueError("Fantasy entry not found")

            conn.execute("DELETE FROM fantasy_team_picks WHERE fantasy_team_id = ?", (safe_entry_id,))
            conn.execute("DELETE FROM fantasy_teams WHERE id = ?", (safe_entry_id,))
            conn.commit()

    def _season_players(self, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        with self._connect() as conn:
            auction = self._require_published_auction(conn, safe_slug)
            auction_id = auction["id"]

            player_rows = conn.execute(
                """
                SELECT ap.player_id,
                       ap.sold_to_team_id,
                       p.display_name,
                       p.tier,
                       p.speciality,
                       t.name AS team_name
                FROM auction_players ap
                JOIN players p ON p.id = ap.player_id
                LEFT JOIN teams t ON t.id = ap.sold_to_team_id
                WHERE ap.auction_id = ?
                  AND COALESCE(p.is_manager, 0) = 0
                ORDER BY LOWER(COALESCE(p.display_name, '')) ASC
                """,
                (auction_id,),
            ).fetchall()

            manager_rows = conn.execute(
                """
                SELECT t.id,
                       t.name,
                       t.manager_username,
                       t.manager_tier,
                       mp.display_name AS manager_name,
                       mp.speciality AS manager_speciality
                FROM auction_teams at
                JOIN teams t ON t.id = at.team_id
                LEFT JOIN players mp ON mp.id = t.manager_player_id
                WHERE at.auction_id = ?
                ORDER BY LOWER(COALESCE(t.name, '')) ASC
                """,
                (auction_id,),
            ).fetchall()

        players = []
        for row in player_rows:
            tier = (row["tier"] or "").strip().lower()
            players.append(
                {
                    "id": row["player_id"],
                    "name": row["display_name"] or "Unknown",
                    "tier": tier,
                    "speciality": (row["speciality"] or "-").strip() or "-",
                    "credits": TIER_CREDIT_COST.get(tier, 0),
                    "auction_team_id": row["sold_to_team_id"],
                    "auction_team_name": row["team_name"] or "-",
                }
            )

        for row in manager_rows:
            manager_username = (row["manager_username"] or "").strip()
            if not manager_username:
                continue

            manager_tier = (row["manager_tier"] or "").strip().lower()
            players.append(
                {
                    "id": f"manager::{manager_username.lower()}",
                    "name": (row["manager_name"] or manager_username),
                    "tier": manager_tier,
                    "speciality": (row["manager_speciality"] or "-").strip() or "-",
                    "credits": TIER_CREDIT_COST.get(manager_tier, 0),
                    "auction_team_id": row["id"],
                    "auction_team_name": row["name"] or "-",
                }
            )

        players.sort(key=lambda item: (item.get("name") or "").lower())
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

    @staticmethod
    def _manager_username_from_player_id(player_id: str):
        safe_player_id = (player_id or "").strip().lower()
        if safe_player_id.startswith("manager::"):
            return safe_player_id.split("::", 1)[1].strip()
        return ""

    def _entry_team_signature(self, entry: dict) -> str:
        signature = (entry or {}).get("team_signature")
        if signature:
            return str(signature)
        picks = entry.get("picks") or []
        pick_ids = [pick.get("player_id") for pick in picks if pick.get("player_id")]
        return self._team_signature(pick_ids)

    def _eligible_lookup(self, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        lookup = {}

        season_players = self._season_players(safe_slug)

        for entrant in season_players:
            entrant_id = (entrant.get("id") or "").strip()
            entrant_name = (entrant.get("name") or "").strip()
            if not entrant_name:
                continue

            manager_username = self._manager_username_from_player_id(entrant_id)
            if manager_username:
                canonical = f"manager:{self._normalize(manager_username)}"
                for alias in {manager_username, entrant_name}:
                    normalized_alias = self._normalize(alias)
                    if normalized_alias:
                        lookup[normalized_alias] = {
                            "entrant_key": canonical,
                            "entrant_display_name": entrant_name,
                        }
                continue

            normalized_name = self._normalize(entrant_name)
            if not normalized_name:
                continue
            lookup[normalized_name] = {
                "entrant_key": f"player:{normalized_name}",
                "entrant_display_name": entrant_name,
            }

        return lookup

    def get_eligible_entrant_names(self, season_slug: str):
        lookup = self._eligible_lookup(season_slug)
        submitted_keys = set()
        submitted_names = set()

        for entry in self.get_entries_for_season(season_slug):
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
        safe_slug = (season_slug or "").strip().lower()
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
            raise ValueError("Only league entrants can submit fantasy teams")

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

        players = self._season_players(safe_slug)
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
            "season_slug": safe_slug,
            "entrant_name": entrant["entrant_display_name"],
            "entrant_key": entrant["entrant_key"],
            "team_signature": team_signature,
            "picks": picks,
            "total_credits": total_credits,
            "created_at": datetime.utcnow().isoformat(),
        }

        with self._connect() as conn:
            if not self._published_auction(conn, safe_slug):
                raise ValueError("Fantasy season not found")

            existing_entries = conn.execute(
                """
                SELECT id, entrant_name, entrant_key, team_signature
                FROM fantasy_teams
                WHERE season_id = ?
                ORDER BY created_at DESC, id DESC
                """,
                (safe_slug,),
            ).fetchall()

            normalized_entrant_name = self._normalize(entry["entrant_name"])

            for existing in existing_entries:
                existing_signature = (existing["team_signature"] or "").strip()
                if existing_signature == team_signature:
                    conflict_name = (existing["entrant_name"] or "another entrant").strip() or "another entrant"
                    raise ValueError(f"Your team conflicts with {conflict_name}'s squad")

                same_entrant = (existing["entrant_key"] or "").strip() == entry["entrant_key"]
                same_name = self._normalize(existing["entrant_name"] or "") == normalized_entrant_name
                if same_entrant or same_name:
                    raise ValueError("You have already submitted a fantasy team for this season")

            conn.execute(
                """
                INSERT INTO fantasy_teams
                (id, season_id, entrant_name, entrant_key, total_credits, team_signature, created_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry["id"],
                    safe_slug,
                    entry["entrant_name"],
                    entry["entrant_key"],
                    entry["total_credits"],
                    entry["team_signature"],
                    entry["created_at"],
                    self._dump_metadata({"season_slug": safe_slug}),
                ),
            )

            for index, pick in enumerate(picks, start=1):
                conn.execute(
                    """
                    INSERT INTO fantasy_team_picks
                    (fantasy_team_id, pick_index, player_id, player_name, tier, credits, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry["id"],
                        index,
                        pick.get("player_id"),
                        pick.get("player_name"),
                        pick.get("tier"),
                        int(pick.get("credits") or 0),
                        self._dump_metadata({}),
                    ),
                )

            conn.commit()

        return entry

    def get_entries_for_season(self, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        with self._connect() as conn:
            if not self._published_auction(conn, safe_slug):
                return []

            team_rows = conn.execute(
                """
                SELECT id, entrant_name, entrant_key, team_signature, total_credits, created_at
                FROM fantasy_teams
                WHERE season_id = ?
                ORDER BY COALESCE(created_at, '') DESC, id DESC
                """,
                (safe_slug,),
            ).fetchall()

            entries = []
            for row in team_rows:
                pick_rows = conn.execute(
                    """
                    SELECT player_id, player_name, tier, credits
                    FROM fantasy_team_picks
                    WHERE fantasy_team_id = ?
                    ORDER BY pick_index ASC
                    """,
                    (row["id"],),
                ).fetchall()
                picks = [
                    {
                        "player_id": pick["player_id"],
                        "player_name": pick["player_name"],
                        "tier": pick["tier"],
                        "credits": int(pick["credits"] or 0),
                    }
                    for pick in pick_rows
                ]

                entries.append(
                    {
                        "id": row["id"],
                        "season_slug": safe_slug,
                        "entrant_name": row["entrant_name"],
                        "entrant_key": row["entrant_key"],
                        "team_signature": row["team_signature"],
                        "picks": picks,
                        "total_credits": int(row["total_credits"] or 0),
                        "created_at": row["created_at"],
                    }
                )
            return entries

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
                    "speciality": player.get("speciality") or "-",
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
