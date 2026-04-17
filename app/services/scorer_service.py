import csv
import io
import json
import re
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone
from pathlib import Path


class MatchOverwriteConfirmationRequired(ValueError):
    def __init__(self, season_slug: str, match_id: str):
        self.season_slug = (season_slug or "").strip().lower()
        self.match_id = (match_id or "").strip()
        self.requires_confirmation = True
        super().__init__(
            f"Match {self.match_id or '-'} already exists for season {self.season_slug or '-'}. "
            "Confirm overwrite to replace existing match stats."
        )


class ScorerService:
    DEFAULT_CONFIG = {
        "title": "SCL Scorer",
        "version": "1.0.0",
        "season_slug": "",
        "max_overs": 5,
    }

    CSV_REQUIRED_COLUMNS = (
        "Match ID",
        "Match",
        "Venue",
        "Innings Order",
        "Batting Team",
        "Batting Team ID",
        "Batting Manager ID",
        "Over Number",
        "Ball Number",
        "Valid Ball?",
        "Batter",
        "Batter ID",
        "Non Strike Batter",
        "Non Strike Batter ID",
        "Bowler",
        "Bowler ID",
        "Bowling Team",
        "Bowling Team ID",
        "Bowling Manager ID",
        "Runs Bat",
        "Runs Extra",
        "Extras Type",
        "Dismissed Batter",
        "Dismissed Batter ID",
        "Progressive Runs",
        "Progressive Wickets",
        "Match Toss",
        "Match Result",
    )

    FANTASY_TIERS = {
        "S": {"value": 1, "reward": 1.1, "penalty": 0.9},
        "G": {"value": 2, "reward": 1.0, "penalty": 1.0},
        "P": {"value": 3, "reward": 0.9, "penalty": 1.1},
    }
    FANTASY_BAT_POINTS = {0: -3, 1: 0, 2: +1, 3: +2, 4: +4, 6: +6, "OUT": -7}
    FANTASY_BOWL_POINTS = {0: +3, 1: +1, 2: 0, 3: -2, 4: -4, 6: -5, "WICKET": +8}
    FANTASY_MATCH_BONUS_POINTS = 25.0

    # Fallback role/tier map carried from scoreCard.py for unknown identities.
    FANTASY_PLAYER_ROLES = {
        "ahmad": "BATTER",
        "qambar": "ALL_ROUNDER",
        "osama": "BOWLER",
        "talha": "BOWLER",
        "hashir": "BATTER",
        "mashaal": "ALL_ROUNDER",
        "yousuf": "BOWLER",
        "azen": "ALL_ROUNDER",
        "moiz": "ALL_ROUNDER",
        "sanawar": "BOWLER",
        "asad": "BOWLER",
        "baloch": "BATTER",
        "hassan": "ALL_ROUNDER",
        "owais": "BATTER",
        "umar": "BOWLER",
        "anas": "BOWLER",
        "hassin": "BOWLER",
    }
    FANTASY_PLAYER_TIERS = {
        "ahmad": "G",
        "qambar": "P",
        "osama": "G",
        "talha": "G",
        "hashir": "S",
        "mashaal": "G",
        "yousuf": "S",
        "azen": "P",
        "moiz": "G",
        "sanawar": "G",
        "asad": "S",
        "baloch": "S",
        "hassan": "P",
        "owais": "P",
        "umar": "G",
        "anas": "S",
        "hassin": "S",
    }

    TIER_TO_FANTASY_CODE = {
        "silver": "S",
        "gold": "G",
        "platinum": "P",
        "s": "S",
        "g": "G",
        "p": "P",
    }

    FANTASY_CODE_TO_TIER = {
        "S": "silver",
        "G": "gold",
        "P": "platinum",
    }

    def __init__(self, season_store_manager, auction_service, app_root: str, config_path: str, global_league_service=None):
        self.season_store_manager = season_store_manager
        self.auction_service = auction_service
        self.global_league_service = global_league_service

        app_root_path = Path(app_root)
        self.workspace_root = app_root_path.parent
        self.template_path = self.workspace_root / "scorer.html"

        config_file = Path(config_path)
        self.config_path = config_file if config_file.is_absolute() else (self.workspace_root / config_file)
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

    def _normalize_config(self, payload):
        config = dict(self.DEFAULT_CONFIG)
        if isinstance(payload, dict):
            config.update(payload)

        config["title"] = str(config.get("title") or self.DEFAULT_CONFIG["title"]).strip() or self.DEFAULT_CONFIG["title"]
        config["version"] = str(config.get("version") or self.DEFAULT_CONFIG["version"]).strip() or self.DEFAULT_CONFIG["version"]
        config["season_slug"] = str(config.get("season_slug") or "").strip().lower()

        try:
            max_overs = int(config.get("max_overs", self.DEFAULT_CONFIG["max_overs"]))
        except (TypeError, ValueError):
            max_overs = self.DEFAULT_CONFIG["max_overs"]
        config["max_overs"] = max(1, max_overs)
        return config

    @staticmethod
    def _sanitize_filename_fragment(value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip())
        cleaned = cleaned.strip(".-_")
        return cleaned or "latest"

    def _matches_archive_dir(self) -> Path:
        target = self.config_path.parent / "matches"
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _save_imported_csv(self, season_slug: str, match_id: str, rows: list[dict]):
        if not rows:
            return

        safe_season = self._sanitize_filename_fragment((season_slug or "").strip().lower())
        safe_match_id = self._sanitize_filename_fragment((match_id or "").strip())
        file_path = self._matches_archive_dir() / f"{safe_season}-{safe_match_id}.csv"

        fieldnames = list(rows[0].keys())
        with file_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({name: row.get(name, "") for name in fieldnames})

    def load_config(self):
        if not self.config_path.exists():
            return self._normalize_config({})

        try:
            payload = json.loads(self.config_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            payload = {}
        return self._normalize_config(payload)

    def save_config(self, payload):
        config = self._normalize_config(payload)
        self.config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
        return config

    def list_seasons(self):
        seasons = []
        for slug in self.season_store_manager.list_slugs():
            meta = self._season_meta(slug)
            seasons.append(
                {
                    "slug": slug,
                    "name": meta.get("name") or slug,
                    "published_at": meta.get("published_at") or "",
                }
            )
        seasons.sort(key=lambda item: item.get("published_at") or "", reverse=True)
        return seasons

    def _season_meta(self, slug: str):
        if not self.season_store_manager.has_season(slug):
            return {}
        store = self.season_store_manager.get_store(slug, create=False)
        with store.read() as db:
            return db.table("season_meta").get(doc_id=1) or {}

    def _default_season_slug(self):
        seasons = self.season_store_manager.list_slugs()
        return seasons[0] if seasons else ""

    def _load_tables(self, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        if safe_slug and self.season_store_manager.has_season(safe_slug):
            store = self.season_store_manager.get_store(safe_slug, create=False)
            tables = store.export_tables()
            meta = self._season_meta(safe_slug)
            return tables, meta

        tables = self.auction_service.store.export_tables()
        with self.auction_service.store.read() as db:
            meta = db.table("meta").get(doc_id=1) or {}
        return tables, meta

    def _global_tables(self):
        if not self.global_league_service:
            return {}

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return {}

        try:
            tables = store.export_tables()
        except Exception:  # noqa: BLE001
            return {}

        return tables if isinstance(tables, dict) else {}

    @staticmethod
    def _normalized_key(name: str, speciality: str = ""):
        name_key = " ".join((name or "").strip().lower().split())
        speciality_key = (speciality or "").strip().upper()
        return f"{name_key}|{speciality_key}" if name_key else ""

    def _build_teams(self, tables, season_slug: str = ""):
        teams = list(tables.get("teams", [])) if isinstance(tables, dict) else []
        global_tables = self._global_tables()
        users_by_username = {
            (user.get("username") or "").strip(): user
            for user in (tables.get("users", []) if isinstance(tables, dict) else [])
            if (user.get("username") or "").strip()
        }
        players_by_id = {
            (player.get("id") or "").strip(): player
            for player in (tables.get("players", []) if isinstance(tables, dict) else [])
            if (player.get("id") or "").strip()
        }

        player_global_by_local = {}
        team_global_by_local = {}
        manager_global_by_team_local = {}
        if season_slug and isinstance(global_tables, dict):
            for row in global_tables.get("season_player_links", []):
                if (row.get("season_slug") or "").strip().lower() != season_slug:
                    continue
                local_player_id = (row.get("local_player_id") or "").strip()
                global_player_id = (row.get("global_player_id") or "").strip()
                if local_player_id and global_player_id:
                    player_global_by_local[local_player_id] = global_player_id

            for row in global_tables.get("season_team_links", []):
                if (row.get("season_slug") or "").strip().lower() != season_slug:
                    continue
                local_team_id = (row.get("local_team_id") or "").strip()
                global_team_id = (row.get("global_team_id") or "").strip()
                if local_team_id and global_team_id:
                    team_global_by_local[local_team_id] = global_team_id
                manager_global_player_id = (row.get("manager_global_player_id") or "").strip()
                if local_team_id and manager_global_player_id:
                    manager_global_by_team_local[local_team_id] = manager_global_player_id

        global_players = list(global_tables.get("global_players", [])) if isinstance(global_tables, dict) else []
        global_players_by_key = {}
        global_players_by_name = {}
        for player in global_players:
            player_id = (player.get("id") or "").strip()
            if not player_id:
                continue
            key = self._normalized_key(player.get("name") or "", player.get("speciality") or "")
            if key and key not in global_players_by_key:
                global_players_by_key[key] = player_id
            name_key = (player.get("name") or "").strip().lower()
            if name_key and name_key not in global_players_by_name:
                global_players_by_name[name_key] = player_id

        global_teams = list(global_tables.get("global_teams", [])) if isinstance(global_tables, dict) else []
        global_teams_by_name = {}
        for team in global_teams:
            team_id = (team.get("id") or "").strip()
            team_name = (team.get("name") or "").strip().lower()
            if team_id and team_name and team_name not in global_teams_by_name:
                global_teams_by_name[team_name] = team_id

        roster_teams = []
        for team in teams:
            team_id = (team.get("id") or "").strip()
            team_name = (team.get("name") or team_id or "Team").strip()
            manager_username = (team.get("manager_username") or "").strip()
            manager_user = users_by_username.get(manager_username, {})
            manager_player_id = (team.get("manager_player_id") or "").strip()
            manager_player = players_by_id.get(manager_player_id) if manager_player_id else None
            global_team_id = (
                (team.get("global_team_id") or "").strip()
                or team_global_by_local.get(team_id, "")
                or global_teams_by_name.get(team_name.lower(), "")
                or team_id
            )

            manager_global_player_id = (
                (team.get("manager_global_player_id") or "").strip()
                or manager_global_by_team_local.get(team_id, "")
            )

            roster_ids = []
            for field_name in ("players", "bench"):
                for player_id in team.get(field_name, []) or []:
                    safe_player_id = (player_id or "").strip()
                    if safe_player_id and safe_player_id not in roster_ids:
                        roster_ids.append(safe_player_id)

            roster = []
            for player_id in roster_ids:
                player = players_by_id.get(player_id)
                if not player:
                    continue
                global_player_id = (
                    (player.get("global_player_id") or "").strip()
                    or player_global_by_local.get(player_id, "")
                    or global_players_by_key.get(self._normalized_key(player.get("name") or "", player.get("speciality") or ""), "")
                    or global_players_by_name.get((player.get("name") or "").strip().lower(), "")
                    or player_id
                )
                roster.append(
                    {
                        "id": global_player_id,
                        "local_id": player.get("id"),
                        "name": player.get("name") or "Unknown",
                        "tier": (player.get("tier") or "").strip().lower(),
                        "speciality": (player.get("speciality") or "-").strip() or "-",
                    }
                )

            if manager_player:
                manager_global_player_id = manager_global_player_id or (
                    (manager_player.get("global_player_id") or "").strip()
                    or player_global_by_local.get(manager_player_id, "")
                    or global_players_by_key.get(self._normalized_key(manager_player.get("name") or "", manager_player.get("speciality") or ""), "")
                    or global_players_by_name.get((manager_player.get("name") or "").strip().lower(), "")
                    or manager_player_id
                )
                roster.append(
                    {
                        "id": manager_global_player_id,
                        "local_id": manager_player.get("id"),
                        "name": manager_player.get("name") or manager_user.get("display_name") or manager_username or team_name,
                        "tier": (manager_player.get("tier") or team.get("manager_tier") or "").strip().lower(),
                        "speciality": (manager_player.get("speciality") or manager_user.get("speciality") or "-").strip() or "-",
                    }
                )
            elif manager_username:
                manager_global_player_id = manager_global_player_id or global_players_by_name.get(manager_username.lower(), "") or team_id
                roster.append(
                    {
                        "id": manager_global_player_id,
                        "local_id": team_id,
                        "name": manager_user.get("display_name") or manager_username or team_name,
                        "tier": (team.get("manager_tier") or "").strip().lower(),
                        "speciality": (manager_user.get("speciality") or team.get("manager_speciality") or "-").strip() or "-",
                    }
                )

            roster.sort(key=lambda item: item.get("name", "").lower())
            roster_teams.append(
                {
                    "id": global_team_id,
                    "local_id": team_id,
                    "name": team_name,
                    "manager_id": manager_global_player_id or manager_player_id or team_id,
                    "manager_player_id": manager_player_id,
                    "manager_global_player_id": manager_global_player_id or "",
                    "manager_username": manager_username,
                    "manager_name": (
                        manager_player.get("name")
                        if manager_player
                        else (manager_user.get("display_name") or manager_username or team_name)
                    ),
                    "manager_tier": (team.get("manager_tier") or "").strip().lower(),
                    "players": roster,
                }
            )

        return roster_teams

    def build_context(self):
        config = self.load_config()
        season_slug = config.get("season_slug") or self._default_season_slug()
        tables, source_meta = self._load_tables(season_slug)
        teams = self._build_teams(tables, season_slug=season_slug)

        season_slug = season_slug or source_meta.get("slug") or ""
        season_name = source_meta.get("name") or season_slug or config["title"]

        payload = {
            "title": config["title"],
            "version": config["version"],
            "max_overs": config["max_overs"],
            "season": {
                "slug": season_slug,
                "name": season_name,
            },
            "teams": teams,
        }

        return {
            "scorer_config": config,
            "scorer_payload": payload,
            "scorer_available_seasons": self.list_seasons(),
            "scorer_download_filename": self.download_filename(config),
            "scorer_download_url": "/scorer/download",
        }

    def download_filename(self, config=None):
        active_config = self._normalize_config(config or self.load_config())
        version_fragment = self._sanitize_filename_fragment(active_config.get("version"))
        return f"scorer-v{version_fragment}.html"

    def template_source(self):
        return self.template_path.read_text(encoding="utf-8")

    @staticmethod
    def _now_iso():
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _norm(value: str):
        return " ".join((value or "").strip().lower().split())

    @staticmethod
    def _safe_int(value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _round_nearest_int(value, default=0):
        try:
            return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        except (TypeError, ValueError, ArithmeticError):
            return default

    @staticmethod
    def _overs_string(valid_balls: int):
        safe_balls = max(0, int(valid_balls or 0))
        return f"{safe_balls // 6}.{safe_balls % 6}"

    @staticmethod
    def _speciality_to_role(speciality: str):
        value = (speciality or "").strip().upper()
        if value in {"BATTER", "BOWLER", "ALL_ROUNDER"}:
            return value
        return "ALL_ROUNDER"

    @classmethod
    def _tier_to_fantasy_code(cls, tier: str, fallback_name: str = ""):
        code = cls.TIER_TO_FANTASY_CODE.get((tier or "").strip().lower())
        if code:
            return code

        fallback_key = cls._norm(fallback_name)
        if fallback_key in cls.FANTASY_PLAYER_TIERS:
            return cls.FANTASY_PLAYER_TIERS[fallback_key]

        return "G"

    @classmethod
    def _fantasy_code_to_tier(cls, code: str):
        return cls.FANTASY_CODE_TO_TIER.get((code or "").strip().upper(), "gold")

    def list_recent_imports(self, limit=10):
        if not self.global_league_service:
            return []

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return []

        with store.read() as db:
            rows = db.table("scorer_match_stats").all()

        rows.sort(key=lambda item: item.get("uploaded_at") or "", reverse=True)
        return rows[: max(1, int(limit or 10))]

    def list_global_team_stats(self, limit=200):
        if not self.global_league_service:
            return []

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return []

        with store.read() as db:
            rows = db.table("scorer_team_global_stats").all()

        rows.sort(
            key=lambda item: (
                self._safe_int(item.get("matches"), 0),
                float(item.get("fantasy_points") or 0.0),
                float(item.get("net_run_rate") or 0.0),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit or 200))]

    def list_global_player_stats(self, limit=500):
        if not self.global_league_service:
            return []

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return []

        with store.read() as db:
            rows = db.table("scorer_player_global_stats").all()

        rows.sort(
            key=lambda item: (
                self._safe_int(item.get("matches"), 0),
                float(item.get("fantasy_score") or 0.0),
                self._safe_int(item.get("runs"), 0),
                self._safe_int(item.get("wickets"), 0),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit or 500))]

    @staticmethod
    def _slugify_fragment(value: str) -> str:
        text = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower())
        text = text.strip("-")
        return text or "item"

    @classmethod
    def team_profile_slug(cls, team_id: str, team_name: str) -> str:
        safe_id = (team_id or "").strip().lower()
        suffix = safe_id[:8] if safe_id else "team"
        return f"{cls._slugify_fragment(team_name)}-{suffix}"

    @classmethod
    def player_profile_slug(cls, player_id: str, player_name: str) -> str:
        safe_id = (player_id or "").strip().lower()
        suffix = safe_id[:8] if safe_id else "player"
        return f"{cls._slugify_fragment(player_name)}-{suffix}"

    def _resolve_slug_to_id(self, slug: str, rows: list, id_key: str, name_key: str, slug_builder):
        safe_slug = (slug or "").strip().lower()
        if not safe_slug:
            return ""

        candidates = []
        ids_by_prefix = []
        base_name_slug = safe_slug
        slug_suffix = ""
        if "-" in safe_slug:
            base_name_slug, slug_suffix = safe_slug.rsplit("-", 1)

        for row in rows or []:
            row_id = (row.get(id_key) or "").strip()
            if not row_id:
                continue
            row_name = (row.get(name_key) or row_id).strip()
            full_slug = slug_builder(row_id, row_name)
            if full_slug == safe_slug:
                return row_id
            candidates.append((row_id, row_name))
            if slug_suffix and row_id.lower().startswith(slug_suffix):
                ids_by_prefix.append(row_id)

        if len(ids_by_prefix) == 1:
            return ids_by_prefix[0]

        if base_name_slug:
            matching_ids = [row_id for row_id, row_name in candidates if self._slugify_fragment(row_name) == base_name_slug]
            if len(matching_ids) == 1:
                return matching_ids[0]

        if len(candidates) == 1:
            return candidates[0][0]

        return ""

    @staticmethod
    def _season_sort_key(slug: str):
        safe_slug = (slug or "").strip().lower()
        if not safe_slug:
            return (10**9, "")
        match = re.search(r"(\d+)$", safe_slug)
        if match:
            return (int(match.group(1)), safe_slug)
        return (10**9 - 1, safe_slug)

    def list_global_teams_overview(self, limit=500):
        if not self.global_league_service:
            return []

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return []

        with store.read() as db:
            global_team_rows = db.table("global_teams").all()
            global_stats_rows = db.table("scorer_team_global_stats").all()
            season_team_link_rows = db.table("season_team_links").all()
            team_match_rows = db.table("scorer_team_match_stats").all()

        stats_by_team_id = {
            (row.get("team_id") or "").strip(): row
            for row in global_stats_rows
            if (row.get("team_id") or "").strip()
        }

        team_rows = {}
        for row in global_team_rows:
            team_id = (row.get("id") or "").strip()
            if not team_id:
                continue
            team_rows[team_id] = {
                "id": team_id,
                "name": (row.get("name") or team_id).strip(),
            }

        for row in global_stats_rows:
            team_id = (row.get("team_id") or "").strip()
            if not team_id:
                continue
            if team_id not in team_rows:
                team_rows[team_id] = {
                    "id": team_id,
                    "name": (row.get("team_name") or team_id).strip(),
                }

        seasons_by_team = {}
        for row in season_team_link_rows:
            team_id = (row.get("global_team_id") or "").strip()
            season_slug = (row.get("season_slug") or "").strip().lower()
            if not team_id or not season_slug:
                continue
            seasons_by_team.setdefault(team_id, set()).add(season_slug)

        for row in team_match_rows:
            team_id = (row.get("team_id") or "").strip()
            season_slug = (row.get("season_slug") or "").strip().lower()
            if not team_id or not season_slug:
                continue
            seasons_by_team.setdefault(team_id, set()).add(season_slug)

        overview_rows = []
        for team_id, team in team_rows.items():
            stat = stats_by_team_id.get(team_id, {})
            wins = self._safe_int(stat.get("wins"), 0)
            ties = self._safe_int(stat.get("ties"), 0)
            nrr_value = self._safe_float(stat.get("net_run_rate"), 0.0)
            season_slugs = sorted(list(seasons_by_team.get(team_id, set())), key=self._season_sort_key)
            overview_rows.append(
                {
                    "team_id": team_id,
                    "team_name": team.get("name") or team_id,
                    "team_slug": self.team_profile_slug(team_id, team.get("name") or team_id),
                    "matches": self._safe_int(stat.get("matches"), 0),
                    "wins": wins,
                    "losses": self._safe_int(stat.get("losses"), 0),
                    "ties": ties,
                    "no_results": self._safe_int(stat.get("no_results"), 0),
                    "points": (wins * 2) + ties,
                    "runs_scored": self._safe_int(stat.get("runs_scored"), 0),
                    "runs_conceded": self._safe_int(stat.get("runs_conceded"), 0),
                    "fantasy_points": self._safe_int(stat.get("fantasy_points"), 0),
                    "net_run_rate": nrr_value,
                    "nrr_display": f"{nrr_value:.2f}",
                    "seasons": season_slugs,
                    "seasons_count": len(season_slugs),
                }
            )

        overview_rows.sort(
            key=lambda item: (
                -self._safe_int(item.get("matches"), 0),
                -self._safe_int(item.get("points"), 0),
                -self._safe_float(item.get("net_run_rate"), 0.0),
                (item.get("team_name") or "").lower(),
            )
        )
        return overview_rows[: max(1, int(limit or 500))]

    def _team_season_stats(self, team_id: str):
        if not self.global_league_service:
            return []

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return []

        safe_team_id = (team_id or "").strip()
        if not safe_team_id:
            return []

        with store.read() as db:
            match_rows = db.table("scorer_match_stats").all()
            team_rows = db.table("scorer_team_match_stats").all()

        include_fantasy_by_match = {
            (row.get("match_key") or "").strip(): bool(row.get("include_in_fantasy_points", True))
            for row in match_rows
            if (row.get("match_key") or "").strip()
        }

        grouped = {}
        for row in team_rows:
            if (row.get("team_id") or "").strip() != safe_team_id:
                continue

            season_slug = (row.get("season_slug") or "").strip().lower()
            if not season_slug:
                continue

            if season_slug not in grouped:
                grouped[season_slug] = {
                    "season_slug": season_slug,
                    "matches": 0,
                    "wins": 0,
                    "losses": 0,
                    "ties": 0,
                    "no_results": 0,
                    "points": 0,
                    "runs_scored": 0,
                    "balls_faced": 0,
                    "runs_conceded": 0,
                    "balls_bowled": 0,
                    "wickets_taken": 0,
                    "wickets_lost": 0,
                    "fantasy_points": 0,
                    "run_rate_for": 0.0,
                    "run_rate_against": 0.0,
                    "net_run_rate": 0.0,
                    "nrr_display": "0.00",
                }

            item = grouped[season_slug]
            wins = self._safe_int(row.get("wins"), 0)
            losses = self._safe_int(row.get("losses"), 0)
            ties = self._safe_int(row.get("ties"), 0)
            no_results = self._safe_int(row.get("no_results"), 0)
            if wins == losses == ties == no_results == 0:
                result_value = (row.get("result") or "").strip().lower()
                if result_value == "win":
                    wins = 1
                elif result_value == "loss":
                    losses = 1
                elif result_value in {"tie", "draw"}:
                    ties = 1
                elif result_value in {"no_result", "nr"}:
                    no_results = 1

            item["matches"] += max(1, wins + losses + ties + no_results)
            item["wins"] += wins
            item["losses"] += losses
            item["ties"] += ties
            item["no_results"] += no_results
            item["points"] += (wins * 2) + ties
            item["runs_scored"] += self._safe_int(row.get("runs_scored"), 0)
            item["balls_faced"] += self._safe_int(row.get("balls_faced"), 0)
            item["runs_conceded"] += self._safe_int(row.get("runs_conceded"), 0)
            item["balls_bowled"] += self._safe_int(row.get("balls_bowled"), 0)
            item["wickets_taken"] += self._safe_int(row.get("wickets_taken"), 0)
            item["wickets_lost"] += self._safe_int(row.get("wickets_lost"), 0)

            match_key = (row.get("match_key") or "").strip()
            if include_fantasy_by_match.get(match_key, True):
                item["fantasy_points"] += self._safe_int(row.get("fantasy_points"), 0)

        season_rows = list(grouped.values())
        for row in season_rows:
            balls_for = self._safe_int(row.get("balls_faced"), 0)
            balls_against = self._safe_int(row.get("balls_bowled"), 0)
            runs_for = self._safe_int(row.get("runs_scored"), 0)
            runs_against = self._safe_int(row.get("runs_conceded"), 0)
            row["run_rate_for"] = (runs_for * 6.0 / float(balls_for)) if balls_for else 0.0
            row["run_rate_against"] = (runs_against * 6.0 / float(balls_against)) if balls_against else 0.0
            row["net_run_rate"] = row["run_rate_for"] - row["run_rate_against"]
            row["nrr_display"] = f"{row['net_run_rate']:.2f}"

        season_rows.sort(key=lambda item: self._season_sort_key(item.get("season_slug") or ""))
        return season_rows

    def _team_squads_by_season(self, team_id: str):
        safe_team_id = (team_id or "").strip()
        if not safe_team_id:
            return []

        global_tables = self._global_tables()
        if not isinstance(global_tables, dict):
            return []

        global_players = {
            (row.get("id") or "").strip(): row
            for row in global_tables.get("global_players", [])
            if (row.get("id") or "").strip()
        }

        season_team_links = [
            row
            for row in global_tables.get("season_team_links", [])
            if (row.get("global_team_id") or "").strip() == safe_team_id
        ]

        roster_by_season = {}
        for row in global_tables.get("season_team_rosters", []):
            if (row.get("global_team_id") or "").strip() != safe_team_id:
                continue
            season_slug = (row.get("season_slug") or "").strip().lower()
            if season_slug:
                roster_by_season[season_slug] = row

        squad_rows = []
        for row in season_team_links:
            season_slug = (row.get("season_slug") or "").strip().lower()
            if not season_slug:
                continue

            roster_row = roster_by_season.get(season_slug, {})
            global_player_ids = [
                (item or "").strip()
                for item in (roster_row.get("global_player_ids") or [])
                if (item or "").strip()
            ]

            manager_id = (row.get("manager_global_player_id") or "").strip()
            if manager_id and manager_id not in global_player_ids:
                global_player_ids.append(manager_id)

            players = []
            seen = set()
            for player_id in global_player_ids:
                if not player_id or player_id in seen:
                    continue
                seen.add(player_id)
                player = global_players.get(player_id, {})
                player_name = (player.get("name") or player_id).strip()
                players.append(
                    {
                        "player_id": player_id,
                        "player_name": player_name,
                        "tier": (player.get("tier") or "").strip().lower(),
                        "speciality": (player.get("speciality") or "").strip().upper() or "-",
                        "player_slug": self.player_profile_slug(player_id, player_name),
                        "is_manager": bool(manager_id and player_id == manager_id),
                    }
                )

            players.sort(key=lambda item: (item.get("player_name") or "").lower())
            manager_name = (global_players.get(manager_id, {}).get("name") if manager_id else "") or "-"
            manager_player_slug = self.player_profile_slug(manager_id, manager_name) if manager_id else ""
            squad_rows.append(
                {
                    "season_slug": season_slug,
                    "team_name": (row.get("team_name") or safe_team_id).strip(),
                    "manager_global_player_id": manager_id,
                    "manager_name": manager_name,
                    "manager_player_slug": manager_player_slug,
                    "players": players,
                }
            )

        squad_rows.sort(key=lambda item: self._season_sort_key(item.get("season_slug") or ""))
        return squad_rows

    def get_team_profile(self, team_slug: str):
        if not self.global_league_service:
            return {}

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return {}

        with store.read() as db:
            global_team_rows = db.table("global_teams").all()
            global_stat_rows = db.table("scorer_team_global_stats").all()

        team_rows = []
        for row in global_team_rows:
            team_id = (row.get("id") or "").strip()
            if team_id:
                team_rows.append({"team_id": team_id, "team_name": (row.get("name") or team_id).strip()})
        for row in global_stat_rows:
            team_id = (row.get("team_id") or "").strip()
            if team_id:
                team_rows.append({"team_id": team_id, "team_name": (row.get("team_name") or team_id).strip()})

        team_id = self._resolve_slug_to_id(
            team_slug,
            rows=team_rows,
            id_key="team_id",
            name_key="team_name",
            slug_builder=self.team_profile_slug,
        )
        if not team_id:
            return {}

        team_name = ""
        for row in team_rows:
            if (row.get("team_id") or "").strip() == team_id:
                team_name = (row.get("team_name") or "").strip() or team_name

        global_stats_row = {}
        for row in global_stat_rows:
            if (row.get("team_id") or "").strip() == team_id:
                global_stats_row = row
                break

        wins = self._safe_int(global_stats_row.get("wins"), 0)
        ties = self._safe_int(global_stats_row.get("ties"), 0)
        nrr_value = self._safe_float(global_stats_row.get("net_run_rate"), 0.0)
        global_stats = {
            "matches": self._safe_int(global_stats_row.get("matches"), 0),
            "wins": wins,
            "losses": self._safe_int(global_stats_row.get("losses"), 0),
            "ties": ties,
            "no_results": self._safe_int(global_stats_row.get("no_results"), 0),
            "points": (wins * 2) + ties,
            "runs_scored": self._safe_int(global_stats_row.get("runs_scored"), 0),
            "balls_faced": self._safe_int(global_stats_row.get("balls_faced"), 0),
            "runs_conceded": self._safe_int(global_stats_row.get("runs_conceded"), 0),
            "balls_bowled": self._safe_int(global_stats_row.get("balls_bowled"), 0),
            "wickets_taken": self._safe_int(global_stats_row.get("wickets_taken"), 0),
            "wickets_lost": self._safe_int(global_stats_row.get("wickets_lost"), 0),
            "fantasy_points": self._safe_int(global_stats_row.get("fantasy_points"), 0),
            "run_rate_for": self._safe_float(global_stats_row.get("run_rate_for"), 0.0),
            "run_rate_against": self._safe_float(global_stats_row.get("run_rate_against"), 0.0),
            "net_run_rate": nrr_value,
            "nrr_display": f"{nrr_value:.2f}",
        }

        return {
            "team_id": team_id,
            "team_name": team_name or team_id,
            "team_slug": self.team_profile_slug(team_id, team_name or team_id),
            "global_stats": global_stats,
            "season_stats": self._team_season_stats(team_id),
            "squads_by_season": self._team_squads_by_season(team_id),
        }

    def get_player_profile(self, player_slug: str):
        if not self.global_league_service:
            return {}

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return {}

        with store.read() as db:
            global_player_rows = db.table("global_players").all()
            global_stat_rows = db.table("scorer_player_global_stats").all()
            match_rows = db.table("scorer_match_stats").all()
            player_match_rows = db.table("scorer_player_match_stats").all()

        player_rows = []
        for row in global_player_rows:
            player_id = (row.get("id") or "").strip()
            if player_id:
                player_rows.append({"player_id": player_id, "player_name": (row.get("name") or player_id).strip()})
        for row in global_stat_rows:
            player_id = (row.get("player_id") or "").strip()
            if player_id:
                player_rows.append({"player_id": player_id, "player_name": (row.get("player_name") or player_id).strip()})

        player_id = self._resolve_slug_to_id(
            player_slug,
            rows=player_rows,
            id_key="player_id",
            name_key="player_name",
            slug_builder=self.player_profile_slug,
        )
        if not player_id:
            return {}

        player_meta = {}
        for row in global_player_rows:
            if (row.get("id") or "").strip() == player_id:
                player_meta = row
                break

        global_stats_row = {}
        for row in global_stat_rows:
            if (row.get("player_id") or "").strip() == player_id:
                global_stats_row = row
                break

        include_fantasy_by_match = {
            (row.get("match_key") or "").strip(): bool(row.get("include_in_fantasy_points", True))
            for row in match_rows
            if (row.get("match_key") or "").strip()
        }

        teamwise = {}
        for row in player_match_rows:
            if (row.get("player_id") or "").strip() != player_id:
                continue

            team_id = (row.get("team_id") or "").strip() or "unknown-team"
            if team_id not in teamwise:
                teamwise[team_id] = {
                    "team_id": team_id,
                    "team_name": (row.get("team_name") or team_id).strip() or team_id,
                    "matches": 0,
                    "runs": 0,
                    "balls_faced": 0,
                    "dismissed": 0,
                    "wickets": 0,
                    "balls_bowled": 0,
                    "runs_conceded": 0,
                    "fantasy_score": 0,
                    "strike_rate": 0.0,
                    "economy": 0.0,
                    "team_slug": self.team_profile_slug(team_id, (row.get("team_name") or team_id).strip() or team_id),
                }

            item = teamwise[team_id]
            item["team_name"] = (row.get("team_name") or item.get("team_name") or team_id).strip() or team_id
            item["matches"] += 1
            item["runs"] += self._safe_int(row.get("runs"), 0)
            item["balls_faced"] += self._safe_int(row.get("balls_faced"), 0)
            item["dismissed"] += self._safe_int(row.get("dismissed"), 0)
            item["wickets"] += self._safe_int(row.get("wickets"), 0)
            item["balls_bowled"] += self._safe_int(row.get("balls_bowled"), 0)
            item["runs_conceded"] += self._safe_int(row.get("runs_conceded"), 0)
            match_key = (row.get("match_key") or "").strip()
            if include_fantasy_by_match.get(match_key, True):
                item["fantasy_score"] += self._safe_int(row.get("fantasy_score"), 0)

        teamwise_rows = list(teamwise.values())
        for row in teamwise_rows:
            balls_faced = self._safe_int(row.get("balls_faced"), 0)
            balls_bowled = self._safe_int(row.get("balls_bowled"), 0)
            runs = self._safe_int(row.get("runs"), 0)
            runs_conceded = self._safe_int(row.get("runs_conceded"), 0)
            row["strike_rate"] = round((runs * 100.0 / balls_faced), 2) if balls_faced else 0.0
            row["economy"] = round((runs_conceded * 6.0 / balls_bowled), 2) if balls_bowled else 0.0

        teamwise_rows.sort(
            key=lambda item: (
                -self._safe_int(item.get("matches"), 0),
                -self._safe_int(item.get("runs"), 0),
                -self._safe_int(item.get("wickets"), 0),
                (item.get("team_name") or "").lower(),
            )
        )

        seasonwise = {}
        for row in player_match_rows:
            if (row.get("player_id") or "").strip() != player_id:
                continue
            season_slug = (row.get("season_slug") or "").strip().lower()
            if not season_slug:
                continue
            if season_slug not in seasonwise:
                seasonwise[season_slug] = {
                    "season_slug": season_slug,
                    "matches": 0,
                    "runs": 0,
                    "wickets": 0,
                    "fantasy_score": 0,
                }
            item = seasonwise[season_slug]
            item["matches"] += 1
            item["runs"] += self._safe_int(row.get("runs"), 0)
            item["wickets"] += self._safe_int(row.get("wickets"), 0)
            match_key = (row.get("match_key") or "").strip()
            if include_fantasy_by_match.get(match_key, True):
                item["fantasy_score"] += self._safe_int(row.get("fantasy_score"), 0)

        seasonwise_rows = list(seasonwise.values())
        seasonwise_rows.sort(key=lambda item: self._season_sort_key(item.get("season_slug") or ""))

        player_name = (player_meta.get("name") or global_stats_row.get("player_name") or player_id).strip() or player_id
        global_stats = {
            "matches": self._safe_int(global_stats_row.get("matches"), 0),
            "runs": self._safe_int(global_stats_row.get("runs"), 0),
            "wickets": self._safe_int(global_stats_row.get("wickets"), 0),
            "balls_faced": self._safe_int(global_stats_row.get("balls_faced"), 0),
            "balls_bowled": self._safe_int(global_stats_row.get("balls_bowled"), 0),
            "strike_rate": self._safe_float(global_stats_row.get("strike_rate"), 0.0),
            "batting_average": self._safe_float(global_stats_row.get("batting_average"), 0.0),
            "economy": self._safe_float(global_stats_row.get("economy"), 0.0),
            "fantasy_score": self._safe_int(global_stats_row.get("fantasy_score"), 0),
            "fantasy_average": self._safe_int(global_stats_row.get("fantasy_average"), 0),
            "role": (global_stats_row.get("role") or player_meta.get("speciality") or "ALL_ROUNDER").strip().upper(),
            "tier": (global_stats_row.get("tier") or player_meta.get("tier") or "").strip().lower(),
        }

        return {
            "player_id": player_id,
            "player_name": player_name,
            "player_slug": self.player_profile_slug(player_id, player_name),
            "global_stats": global_stats,
            "team_wise_stats": teamwise_rows,
            "season_wise_stats": seasonwise_rows,
        }

    def _match_key(self, season_slug: str, match_id: str):
        season_key = (season_slug or "global").strip().lower() or "global"
        safe_match_key = re.sub(r"[^a-z0-9_-]+", "-", (match_id or "").strip().lower()) or "match"
        return f"{season_key}:{safe_match_key}"

    @staticmethod
    def _match_number_sort_value(match_number: str):
        text = str(match_number or "").strip()
        if not text:
            return (10**9, "")
        match = re.search(r"\d+", text)
        if match:
            return (int(match.group(0)), text.lower())
        return (10**9 - 1, text.lower())

    def _enrich_match_registry_rows(self, rows):
        if not self.global_league_service:
            return []

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return []

        with store.read() as db:
            uploaded_rows = db.table("scorer_match_stats").all()

        uploaded_by_season_match = {}
        for row in uploaded_rows:
            season_slug = (row.get("season_slug") or "").strip().lower()
            match_id = (row.get("match_id") or "").strip()
            if not season_slug or not match_id:
                continue
            uploaded_by_season_match[(season_slug, match_id)] = row

        enriched = []
        for row in rows or []:
            season_slug = (row.get("season_slug") or "").strip().lower()
            match_id = (row.get("match_id") or "").strip()
            if not season_slug or not match_id:
                continue

            uploaded_row = uploaded_by_season_match.get((season_slug, match_id))
            item = dict(row)
            item["season_slug"] = season_slug
            item["match_id"] = match_id
            item["match_key"] = item.get("match_key") or self._match_key(season_slug, match_id)
            item["team_a_global_id"] = (item.get("team_a_global_id") or "").strip()
            item["team_b_global_id"] = (item.get("team_b_global_id") or "").strip()
            item["team_a_name"] = (item.get("team_a_name") or item.get("team_a_global_id") or "").strip()
            item["team_b_name"] = (item.get("team_b_name") or item.get("team_b_global_id") or "").strip()
            item["walkover"] = bool(item.get("walkover", False))
            item["walkover_winner_global_id"] = (item.get("walkover_winner_global_id") or "").strip()
            item["walkover_winner_name"] = (item.get("walkover_winner_name") or item.get("walkover_winner") or "").strip()
            item["between"] = (
                (item.get("between") or "").strip()
                or f"{item.get('team_a_name') or '-'} vs {item.get('team_b_name') or '-'}"
            )
            item["has_uploaded_data"] = uploaded_row is not None
            item["uploaded_at"] = (uploaded_row or {}).get("uploaded_at") or ""
            item["uploaded_match_key"] = (uploaded_row or {}).get("match_key") or item["match_key"]
            enriched.append(item)

        enriched.sort(
            key=lambda item: (
                item.get("season_slug") or "",
                self._match_number_sort_value(item.get("match_number")),
                item.get("match_id") or "",
            )
        )
        return enriched

    def list_match_registry(self, season_slug: str = "", limit=500):
        if not self.global_league_service:
            return []

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return []

        safe_season_slug = (season_slug or "").strip().lower()
        with store.read() as db:
            rows = db.table("scorer_match_registry").all()

        if safe_season_slug:
            rows = [row for row in rows if (row.get("season_slug") or "").strip().lower() == safe_season_slug]

        enriched = self._enrich_match_registry_rows(rows)
        return enriched[: max(1, int(limit or 500))]

    def list_match_seasons(self):
        known = {}
        for season in self.list_seasons():
            slug = (season.get("slug") or "").strip().lower()
            if not slug:
                continue
            known[slug] = {
                "slug": slug,
                "name": season.get("name") or slug,
                "published_at": season.get("published_at") or "",
            }

        for row in self.list_match_registry(limit=5000):
            slug = (row.get("season_slug") or "").strip().lower()
            if not slug or slug in known:
                continue
            known[slug] = {
                "slug": slug,
                "name": slug,
                "published_at": "",
            }

        if self.global_league_service and getattr(self.global_league_service, "store", None):
            with self.global_league_service.store.read() as db:
                match_rows = db.table("scorer_match_stats").all()
                team_rows = db.table("scorer_team_match_stats").all()

            for row in list(match_rows) + list(team_rows):
                slug = (row.get("season_slug") or "").strip().lower()
                if not slug or slug in known:
                    continue
                known[slug] = {
                    "slug": slug,
                    "name": slug,
                    "published_at": "",
                }

        seasons = list(known.values())
        seasons.sort(key=lambda item: item.get("published_at") or "", reverse=True)
        if not any(item.get("published_at") for item in seasons):
            seasons.sort(key=lambda item: item.get("slug") or "")
        return seasons

    def list_season_finances(self, season_slug: str):
        safe_season_slug = (season_slug or "").strip().lower()
        if not safe_season_slug:
            return []

        if not self.season_store_manager.has_season(safe_season_slug):
            return []

        store = self.season_store_manager.get_store(safe_season_slug, create=False)
        with store.read() as db:
            teams = db.table("teams").all()

        rows = []
        for team in teams:
            team_id = (team.get("id") or "").strip()
            if not team_id:
                continue

            rows.append(
                {
                    "team_id": team_id,
                    "team_name": (team.get("name") or team_id).strip() or team_id,
                    "purse_remaining": self._safe_int(team.get("purse_remaining"), 0),
                    "credits_remaining": self._safe_int(team.get("credits_remaining"), 0),
                    "active_count": len(team.get("players") or []),
                    "bench_count": len(team.get("bench") or []),
                }
            )

        rows.sort(key=lambda item: (item.get("team_name") or "").lower())
        return rows

    def list_season_finance_transactions(self, season_slug: str, limit=200):
        safe_season_slug = (season_slug or "").strip().lower()
        if not safe_season_slug:
            return []

        if not self.season_store_manager.has_season(safe_season_slug):
            return []

        store = self.season_store_manager.get_store(safe_season_slug, create=False)
        with store.read() as db:
            rows = db.table("finance_transactions").all()

        normalized = []
        for row in rows:
            row_season = (row.get("season_slug") or safe_season_slug).strip().lower()
            if row_season != safe_season_slug:
                continue

            tx_type = (row.get("type") or "").strip().lower()
            amount = self._safe_int(row.get("amount"), 0)
            created_at = (row.get("created_at") or "").strip()
            created_by = (row.get("created_by") or "system").strip() or "system"
            comment = (row.get("comment") or "").strip()

            if tx_type == "transfer":
                summary = (
                    f"{(row.get('from_team_name') or row.get('from_team_id') or '-').strip()} "
                    f"to {(row.get('to_team_name') or row.get('to_team_id') or '-').strip()}"
                )
                tx_label = "Transfer"
            elif tx_type == "player_transfer":
                player_name = (row.get("player_name") or row.get("player_id") or "-").strip() or "-"
                from_team_name = (row.get("from_team_name") or row.get("from_team_id") or "-").strip() or "-"
                to_team_name = (row.get("to_team_name") or row.get("to_team_id") or "-").strip() or "-"
                summary = f"{player_name}: {from_team_name} to {to_team_name}"
                tx_label = "Player Transfer"
            else:
                operation = (row.get("operation") or "adjust").strip().lower()
                tx_label = "Add" if operation == "add" else "Remove" if operation == "remove" else "Adjust"
                summary = (row.get("team_name") or row.get("team_id") or "-").strip() or "-"

            normalized.append(
                {
                    "created_at": created_at,
                    "created_by": created_by,
                    "type": tx_type or "adjust",
                    "label": tx_label,
                    "summary": summary,
                    "amount": amount,
                    "comment": comment,
                }
            )

        normalized.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return normalized[: max(1, self._safe_int(limit, 200))]

    def list_season_league_table(self, season_slug: str):
        safe_season_slug = (season_slug or "").strip().lower()
        if not safe_season_slug:
            return []

        if not self.global_league_service:
            return []

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return []

        team_name_by_id = {
            (team.get("id") or "").strip(): (team.get("name") or team.get("id") or "Team").strip()
            for team in self._season_team_options(safe_season_slug)
            if (team.get("id") or "").strip()
        }

        standings_by_team = {}

        def ensure_row(team_id: str, fallback_name: str = ""):
            safe_team_id = (team_id or "").strip()
            if not safe_team_id:
                return None
            if safe_team_id not in standings_by_team:
                team_name = team_name_by_id.get(safe_team_id) or (fallback_name or safe_team_id).strip() or safe_team_id
                standings_by_team[safe_team_id] = {
                    "team_id": safe_team_id,
                    "team_name": team_name,
                    "played": 0,
                    "wins": 0,
                    "draws": 0,
                    "losses": 0,
                    "no_results": 0,
                    "points": 0,
                    "runs_for": 0,
                    "balls_for": 0,
                    "runs_against": 0,
                    "balls_against": 0,
                    "run_rate_for": 0.0,
                    "run_rate_against": 0.0,
                    "nrr": 0.0,
                    "nrr_display": "0.00",
                }
            return standings_by_team[safe_team_id]

        for team_id, team_name in team_name_by_id.items():
            ensure_row(team_id, team_name)

        with store.read() as db:
            rows = db.table("scorer_team_match_stats").all()

        season_rows = [
            row
            for row in rows
            if (row.get("season_slug") or "").strip().lower() == safe_season_slug
        ]

        for row in season_rows:
            team_id = (row.get("team_id") or "").strip()
            team_name = (row.get("team_name") or team_id or "Team").strip()
            entry = ensure_row(team_id, team_name)
            if not entry:
                continue

            result_value = (row.get("result") or "").strip().lower()
            wins = self._safe_int(row.get("wins"), 0)
            losses = self._safe_int(row.get("losses"), 0)
            ties = self._safe_int(row.get("ties"), 0)
            no_results = self._safe_int(row.get("no_results"), 0)

            if wins == losses == ties == no_results == 0:
                if result_value == "win":
                    wins = 1
                elif result_value in {"tie", "draw"}:
                    ties = 1
                elif result_value in {"no_result", "nr"}:
                    no_results = 1
                elif result_value == "loss":
                    losses = 1

            played = wins + losses + ties + no_results
            if played <= 0:
                played = 1

            entry["played"] += played
            entry["wins"] += wins
            entry["draws"] += ties
            entry["losses"] += losses
            entry["no_results"] += no_results
            entry["points"] += (wins * 2) + ties

            entry["runs_for"] += self._safe_int(row.get("runs_scored"), 0)
            entry["balls_for"] += self._safe_int(row.get("balls_faced"), 0)
            entry["runs_against"] += self._safe_int(row.get("runs_conceded"), 0)
            entry["balls_against"] += self._safe_int(row.get("balls_bowled"), 0)

        standings = list(standings_by_team.values())
        for entry in standings:
            if entry["balls_for"] > 0:
                entry["run_rate_for"] = (entry["runs_for"] * 6.0) / float(entry["balls_for"])
            else:
                entry["run_rate_for"] = 0.0

            if entry["balls_against"] > 0:
                entry["run_rate_against"] = (entry["runs_against"] * 6.0) / float(entry["balls_against"])
            else:
                entry["run_rate_against"] = 0.0

            entry["nrr"] = entry["run_rate_for"] - entry["run_rate_against"]
            entry["nrr_display"] = f"{entry['nrr']:.2f}"

        standings.sort(
            key=lambda item: (
                -self._safe_int(item.get("points"), 0),
                -self._safe_float(item.get("nrr"), 0.0),
                -self._safe_int(item.get("wins"), 0),
                (item.get("team_name") or "").lower(),
            )
        )

        for idx, item in enumerate(standings, start=1):
            item["rank"] = idx

        return standings

    def _season_team_options(self, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        if not safe_slug:
            return []

        tables, _ = self._load_tables(safe_slug)
        teams = self._build_teams(tables, season_slug=safe_slug)
        options = []
        seen = set()
        for team in teams:
            team_id = (team.get("id") or "").strip()
            team_name = (team.get("name") or team_id).strip()
            if not team_id or team_id in seen:
                continue
            seen.add(team_id)
            options.append({"id": team_id, "name": team_name})

        if not options:
            global_tables = self._global_tables()
            for row in global_tables.get("global_teams", []) if isinstance(global_tables, dict) else []:
                team_id = (row.get("id") or "").strip()
                team_name = (row.get("name") or team_id).strip()
                if not team_id or team_id in seen:
                    continue
                seen.add(team_id)
                options.append({"id": team_id, "name": team_name})

        options.sort(key=lambda item: (item.get("name") or "").lower())
        return options

    def list_season_team_options(self):
        mapping = {}
        for season in self.list_match_seasons():
            slug = (season.get("slug") or "").strip().lower()
            if not slug:
                continue
            mapping[slug] = self._season_team_options(slug)
        return mapping

    @staticmethod
    def _team_name_by_id(team_options, team_id: str):
        safe_team_id = (team_id or "").strip()
        for item in team_options or []:
            if (item.get("id") or "").strip() == safe_team_id:
                return (item.get("name") or safe_team_id).strip()
        return safe_team_id

    def _remove_match_rows(self, db, match_key: str):
        safe_match_key = (match_key or "").strip()
        if not safe_match_key:
            return {"team_rows": 0, "player_rows": 0}

        match_table = db.table("scorer_match_stats")
        team_table = db.table("scorer_team_match_stats")
        player_table = db.table("scorer_player_match_stats")

        existing = match_table.get(lambda row: (row.get("match_key") or "").strip() == safe_match_key)
        if existing:
            match_table.remove(doc_ids=[existing.doc_id])

        team_rows = team_table.search(lambda row: (row.get("match_key") or "").strip() == safe_match_key)
        player_rows = player_table.search(lambda row: (row.get("match_key") or "").strip() == safe_match_key)
        team_table.remove(lambda row: (row.get("match_key") or "").strip() == safe_match_key)
        player_table.remove(lambda row: (row.get("match_key") or "").strip() == safe_match_key)

        return {
            "team_rows": len(team_rows),
            "player_rows": len(player_rows),
        }

    def _upsert_walkover_stats(self, db, registry_entry: dict):
        season_slug = (registry_entry.get("season_slug") or "").strip().lower()
        match_id = (registry_entry.get("match_id") or "").strip()
        match_key = (registry_entry.get("match_key") or self._match_key(season_slug, match_id)).strip()
        team_a_id = (registry_entry.get("team_a_global_id") or "").strip()
        team_b_id = (registry_entry.get("team_b_global_id") or "").strip()
        team_a_name = (registry_entry.get("team_a_name") or team_a_id).strip()
        team_b_name = (registry_entry.get("team_b_name") or team_b_id).strip()
        winner_id = (registry_entry.get("walkover_winner_global_id") or "").strip()
        winner_name = (registry_entry.get("walkover_winner_name") or winner_id).strip()

        if not all([season_slug, match_id, team_a_id, team_b_id, winner_id]):
            raise ValueError("Walkover match requires season, match id, both teams, and winner")
        if winner_id not in {team_a_id, team_b_id}:
            raise ValueError("Walkover winner must be one of the selected teams")

        loser_id = team_b_id if winner_id == team_a_id else team_a_id
        loser_name = team_b_name if winner_id == team_a_id else team_a_name

        removed = self._remove_match_rows(db, match_key)
        _ = removed

        now = self._now_iso()
        match_row = {
            "match_key": match_key,
            "season_slug": season_slug,
            "match_id": match_id,
            "match": (registry_entry.get("match_title") or registry_entry.get("between") or match_id),
            "venue": "Walkover",
            "match_date": "",
            "result": f"{winner_name} won by walkover",
            "toss": "",
            "winner_team_id": winner_id,
            "scorer_version": "walkover",
            "delivery_rows": 0,
            "team_rows": 2,
            "player_rows": 0,
            "source_file": "walkover",
            "source_type": "walkover",
            "uploaded_by": "admin",
            "uploaded_at": now,
            "is_walkover": True,
            "include_in_fantasy_points": False,
        }

        team_rows = [
            {
                "match_key": match_key,
                "season_slug": season_slug,
                "match_id": match_id,
                "match": match_row["match"],
                "team_id": winner_id,
                "team_name": winner_name,
                "runs_scored": 0,
                "balls_faced": 0,
                "wickets_lost": 0,
                "fours": 0,
                "sixes": 0,
                "wides_faced": 0,
                "noballs_faced": 0,
                "runs_conceded": 0,
                "balls_bowled": 0,
                "wickets_taken": 0,
                "wides_bowled": 0,
                "noballs_bowled": 0,
                "fantasy_points": 0,
                "result": "win",
                "wins": 1,
                "losses": 0,
                "ties": 0,
                "no_results": 0,
                "overs_faced": "0.0",
                "overs_bowled": "0.0",
                "run_rate_for": 0.0,
                "run_rate_against": 0.0,
                "net_run_rate": 0.0,
                "updated_at": now,
                "uploaded_at": now,
                "is_walkover": True,
            },
            {
                "match_key": match_key,
                "season_slug": season_slug,
                "match_id": match_id,
                "match": match_row["match"],
                "team_id": loser_id,
                "team_name": loser_name,
                "runs_scored": 0,
                "balls_faced": 0,
                "wickets_lost": 0,
                "fours": 0,
                "sixes": 0,
                "wides_faced": 0,
                "noballs_faced": 0,
                "runs_conceded": 0,
                "balls_bowled": 0,
                "wickets_taken": 0,
                "wides_bowled": 0,
                "noballs_bowled": 0,
                "fantasy_points": 0,
                "result": "loss",
                "wins": 0,
                "losses": 1,
                "ties": 0,
                "no_results": 0,
                "overs_faced": "0.0",
                "overs_bowled": "0.0",
                "run_rate_for": 0.0,
                "run_rate_against": 0.0,
                "net_run_rate": 0.0,
                "updated_at": now,
                "uploaded_at": now,
                "is_walkover": True,
            },
        ]

        db.table("scorer_match_stats").insert(match_row)
        db.table("scorer_team_match_stats").insert_multiple(team_rows)

    def _sync_walkover_stats(self, db, registry_entry: dict, previous_registry_entry: dict | None = None):
        is_walkover = bool((registry_entry or {}).get("walkover", False))
        match_key = (registry_entry.get("match_key") or "").strip()
        if not match_key:
            return

        match_row = db.table("scorer_match_stats").get(lambda row: (row.get("match_key") or "").strip() == match_key)
        is_existing_walkover = bool(match_row and (match_row.get("source_type") or "") == "walkover")

        if is_walkover:
            self._upsert_walkover_stats(db, registry_entry)
            self._rebuild_global_aggregates(db)
            return

        if is_existing_walkover or bool((previous_registry_entry or {}).get("walkover", False)):
            self._remove_match_rows(db, match_key)
            self._rebuild_global_aggregates(db)

    def upsert_match_registry_entry(
        self,
        season_slug: str,
        match_id: str,
        team_a_global_id: str,
        team_b_global_id: str,
        match_number: str,
        match_title: str = "",
        walkover: bool = False,
        walkover_winner_global_id: str = "",
    ):
        if not self.global_league_service or not getattr(self.global_league_service, "store", None):
            raise ValueError("Global league store is not configured")

        safe_season_slug = (season_slug or "").strip().lower()
        safe_match_id = (match_id or "").strip()
        safe_team_a_id = (team_a_global_id or "").strip()
        safe_team_b_id = (team_b_global_id or "").strip()
        safe_match_number = (match_number or "").strip()
        safe_match_title = (match_title or "").strip()
        safe_walkover_winner_id = (walkover_winner_global_id or "").strip()

        if not safe_season_slug:
            raise ValueError("Season is required")
        if not safe_match_id:
            raise ValueError("Match ID is required")
        if not safe_team_a_id or not safe_team_b_id:
            raise ValueError("Both teams are required")
        if safe_team_a_id == safe_team_b_id:
            raise ValueError("Select two different teams")
        if not safe_match_number:
            raise ValueError("Match # is required")
        if walkover and not safe_walkover_winner_id:
            raise ValueError("Walkover winner is required when walkover is enabled")
        if walkover and safe_walkover_winner_id not in {safe_team_a_id, safe_team_b_id}:
            raise ValueError("Walkover winner must be one of the selected teams")

        team_options = self._season_team_options(safe_season_slug)
        team_a_name = self._team_name_by_id(team_options, safe_team_a_id)
        team_b_name = self._team_name_by_id(team_options, safe_team_b_id)
        walkover_winner_name = self._team_name_by_id(team_options, safe_walkover_winner_id) if walkover else ""
        between = f"{team_a_name} vs {team_b_name}"

        now = self._now_iso()
        payload = {
            "season_slug": safe_season_slug,
            "match_id": safe_match_id,
            "match_key": self._match_key(safe_season_slug, safe_match_id),
            "between": between,
            "team_a_global_id": safe_team_a_id,
            "team_b_global_id": safe_team_b_id,
            "team_a_name": team_a_name,
            "team_b_name": team_b_name,
            "match_number": safe_match_number,
            "match_title": safe_match_title,
            "walkover": bool(walkover),
            "walkover_winner_global_id": safe_walkover_winner_id if walkover else "",
            "walkover_winner_name": walkover_winner_name,
            "updated_at": now,
        }

        with self.global_league_service.store.write() as db:
            table = db.table("scorer_match_registry")
            existing = table.get(
                lambda row: (row.get("season_slug") or "").strip().lower() == safe_season_slug
                and (row.get("match_id") or "").strip() == safe_match_id
            )
            previous = dict(existing) if existing else None
            if existing:
                created_at = (existing.get("created_at") or "").strip() or now
                table.update({**payload, "created_at": created_at}, doc_ids=[existing.doc_id])
            else:
                table.insert({**payload, "created_at": now})

            self._sync_walkover_stats(db, {**payload, "created_at": (existing.get("created_at") if existing else now) or now}, previous)

        return self.get_match_registry_entry(safe_season_slug, safe_match_id)

    def delete_match_registry_entry(self, season_slug: str, match_id: str):
        if not self.global_league_service or not getattr(self.global_league_service, "store", None):
            raise ValueError("Global league store is not configured")

        safe_season_slug = (season_slug or "").strip().lower()
        safe_match_id = (match_id or "").strip()
        if not safe_season_slug or not safe_match_id:
            raise ValueError("Season and match ID are required")

        with self.global_league_service.store.write() as db:
            table = db.table("scorer_match_registry")
            existing = table.get(
                lambda row: (row.get("season_slug") or "").strip().lower() == safe_season_slug
                and (row.get("match_id") or "").strip() == safe_match_id
            )
            if not existing:
                return {"ok": False, "removed": False, "reason": "not-found"}

            match_key = (existing.get("match_key") or self._match_key(safe_season_slug, safe_match_id)).strip()
            uploaded_match = db.table("scorer_match_stats").get(
                lambda row: (row.get("match_key") or "").strip() == match_key
            )

            if uploaded_match and (uploaded_match.get("source_type") or "") != "walkover":
                raise ValueError("Cannot delete a match that has uploaded CSV data. Undo the import first.")

            table.remove(doc_ids=[existing.doc_id])

            restored = False
            restored_rows = {"team_rows": 0, "player_rows": 0}
            if uploaded_match and (uploaded_match.get("source_type") or "") == "walkover":
                restored_rows = self._remove_match_rows(db, match_key)
                self._rebuild_global_aggregates(db)
                restored = True

        return {
            "ok": True,
            "removed": True,
            "season_slug": safe_season_slug,
            "match_id": safe_match_id,
            "restored_walkover_stats": restored,
            "restored_team_rows": restored_rows.get("team_rows", 0),
            "restored_player_rows": restored_rows.get("player_rows", 0),
        }

    def get_match_registry_entry(self, season_slug: str, match_id: str):
        safe_season_slug = (season_slug or "").strip().lower()
        safe_match_id = (match_id or "").strip()
        if not safe_season_slug or not safe_match_id:
            return None

        for row in self.list_match_registry(season_slug=safe_season_slug, limit=5000):
            if (row.get("match_id") or "").strip() == safe_match_id:
                return row
        return None

    def get_match_summary(self, season_slug: str, match_id: str):
        safe_season_slug = (season_slug or "").strip().lower()
        safe_match_id = (match_id or "").strip()
        if not safe_season_slug or not safe_match_id:
            return None

        registry_entry = self.get_match_registry_entry(safe_season_slug, safe_match_id)
        if not self.global_league_service or not getattr(self.global_league_service, "store", None):
            return None

        with self.global_league_service.store.read() as db:
            match_row = db.table("scorer_match_stats").get(
                lambda row: (row.get("season_slug") or "").strip().lower() == safe_season_slug
                and (row.get("match_id") or "").strip() == safe_match_id
            )

            team_rows = []
            player_rows = []
            if match_row:
                match_key = (match_row.get("match_key") or "").strip()
                if match_key:
                    team_rows = db.table("scorer_team_match_stats").search(
                        lambda row: (row.get("match_key") or "").strip() == match_key
                    )
                    player_rows = db.table("scorer_player_match_stats").search(
                        lambda row: (row.get("match_key") or "").strip() == match_key
                    )

        if not registry_entry and not match_row:
            return None

        # Keep a fantasy-sorted view while preserving innings-like team ordering from persisted rows.
        fantasy_rows = sorted(
            list(player_rows),
            key=lambda item: (
                self._safe_int(item.get("fantasy_score"), 0),
                self._safe_int(item.get("runs"), 0),
                self._safe_int(item.get("wickets"), 0),
            ),
            reverse=True,
        )

        team_sections = []
        for team in team_rows:
            team_id = (team.get("team_id") or "").strip()
            team_name = (team.get("team_name") or team_id or "Team").strip()

            batting_rows = [
                row
                for row in player_rows
                if (row.get("team_id") or "").strip() == team_id and (self._safe_int(row.get("innings_batted"), 0) > 0)
            ]
            batting_rows.sort(
                key=lambda row: (
                    self._safe_int(row.get("runs"), 0),
                    self._safe_int(row.get("balls_faced"), 0),
                    (row.get("player_name") or "").lower(),
                ),
                reverse=True,
            )

            for batter in batting_rows:
                batter["status"] = "out" if self._safe_int(batter.get("dismissed"), 0) > 0 else "not out"
                batter["strike_rate_display"] = (
                    f"{float(batter.get('strike_rate') or 0.0):.1f}" if self._safe_int(batter.get("balls_faced"), 0) > 0 else "-"
                )

            bowling_rows = [
                row
                for row in player_rows
                if (row.get("team_id") or "").strip() != team_id and self._safe_int(row.get("balls_bowled"), 0) > 0
            ]
            bowling_rows.sort(
                key=lambda row: (
                    self._safe_int(row.get("wickets"), 0),
                    self._safe_int(row.get("balls_bowled"), 0),
                    -(float(row.get("economy") or 0.0)),
                ),
                reverse=True,
            )

            for bowler in bowling_rows:
                bowler["overs_display"] = self._overs_string(self._safe_int(bowler.get("balls_bowled"), 0))
                bowler["economy_display"] = (
                    f"{float(bowler.get('economy') or 0.0):.2f}" if self._safe_int(bowler.get("balls_bowled"), 0) > 0 else "-"
                )

            extras = self._safe_int(team.get("wides_faced"), 0) + self._safe_int(team.get("noballs_faced"), 0)
            total_str = (
                f"{self._safe_int(team.get('runs_scored'), 0)}/{self._safe_int(team.get('wickets_lost'), 0)} "
                f"({team.get('overs_faced') or self._overs_string(self._safe_int(team.get('balls_faced'), 0))} Ov)"
            )

            team_sections.append(
                {
                    "team": team,
                    "team_id": team_id,
                    "team_name": team_name,
                    "extras": extras,
                    "total": total_str,
                    "batting_rows": batting_rows,
                    "bowling_rows": bowling_rows,
                    "fall_of_wickets": [],
                    "fall_of_wickets_available": False,
                }
            )

        between = (registry_entry or {}).get("between") or ""
        if not between and len(team_rows) >= 2:
            between = f"{team_rows[0].get('team_name') or '-'} vs {team_rows[1].get('team_name') or '-'}"

        return {
            "season_slug": safe_season_slug,
            "match_id": safe_match_id,
            "match_key": (match_row or {}).get("match_key") or (registry_entry or {}).get("match_key") or self._match_key(safe_season_slug, safe_match_id),
            "between": between,
            "match_number": (registry_entry or {}).get("match_number") or "",
            "match_title": (registry_entry or {}).get("match_title") or (match_row or {}).get("match") or "",
            "walkover": bool((registry_entry or {}).get("walkover", False)),
            "walkover_winner": (registry_entry or {}).get("walkover_winner_name") or "",
            "walkover_winner_global_id": (registry_entry or {}).get("walkover_winner_global_id") or "",
            "has_uploaded_data": bool(match_row),
            "match_row": match_row or {},
            "team_rows": team_rows,
            "player_rows": player_rows,
            "team_sections": team_sections,
            "fantasy_leaderboard": fantasy_rows,
            "registry": registry_entry or {},
        }

    def _match_exists(self, season_slug: str, match_id: str):
        safe_season_slug = (season_slug or "").strip().lower()
        safe_match_id = (match_id or "").strip()
        if not safe_season_slug or not safe_match_id:
            return False

        if not self.global_league_service:
            return False

        store = getattr(self.global_league_service, "store", None)
        if not store:
            return False

        with store.read() as db:
            existing = db.table("scorer_match_stats").get(
                lambda row: (row.get("season_slug") or "").strip().lower() == safe_season_slug
                and (row.get("match_id") or "").strip() == safe_match_id
            )

        return existing is not None

    def import_match_csv(
        self,
        file_storage,
        season_slug: str,
        match_id_override: str = "",
        venue_override: str = "",
        match_date: str = "",
        uploaded_by: str = "admin",
        confirm_overwrite: bool = False,
        include_in_fantasy_points: bool = True,
    ):
        if not self.global_league_service or not getattr(self.global_league_service, "store", None):
            raise ValueError("Global league store is not configured")

        safe_season_slug = (season_slug or "").strip().lower()
        if not safe_season_slug:
            raise ValueError("Season is required")

        file_name = (getattr(file_storage, "filename", "") or "match.csv").strip() or "match.csv"
        payload = file_storage.read()
        if not payload:
            raise ValueError(f"{file_name}: empty file")

        if isinstance(payload, str):
            text = payload
        else:
            try:
                text = payload.decode("utf-8-sig")
            except UnicodeDecodeError:
                text = payload.decode("utf-8")

        rows, substitution_log_player_ins = self._parse_match_csv_rows(text, file_name)
        safe_match_override = (match_id_override or "").strip()
        safe_venue_override = (venue_override or "").strip()
        for row in rows:
            if safe_match_override:
                row["Match ID"] = safe_match_override
            if safe_venue_override:
                row["Venue"] = safe_venue_override

        normalized_rows = self._normalize_rows_to_global_ids(rows, safe_season_slug)

        effective_match_id = (safe_match_override or normalized_rows[0].get("Match ID") or "").strip()
        if not effective_match_id:
            raise ValueError("Match ID is required")

        registry_entry = self.get_match_registry_entry(safe_season_slug, effective_match_id)
        if not registry_entry:
            raise ValueError(
                f"Match ID {effective_match_id} is not configured for season {safe_season_slug}. "
                "Add it from Admin > Scorer > Manage Season Matches."
            )

        if registry_entry.get("walkover"):
            raise ValueError(
                f"Match ID {effective_match_id} is declared as a walkover. CSV upload is not allowed for walkover matches."
            )

        if registry_entry.get("has_uploaded_data") and not confirm_overwrite:
            raise MatchOverwriteConfirmationRequired(safe_season_slug, effective_match_id)

        self._save_imported_csv(safe_season_slug, effective_match_id, rows)

        derived = self._derive_match_stats(
            normalized_rows,
            season_slug=safe_season_slug,
            source_file=file_name,
            uploaded_by=(uploaded_by or "admin"),
            match_date=(match_date or "").strip(),
            include_in_fantasy_points=bool(include_in_fantasy_points),
            substitution_log_player_ins=substitution_log_player_ins,
        )
        return self._persist_match_stats(derived)

    def undo_imported_match(self, match_key: str):
        safe_match_key = (match_key or "").strip()
        if not safe_match_key:
            raise ValueError("Match key is required")

        if not self.global_league_service or not getattr(self.global_league_service, "store", None):
            raise ValueError("Global league store is not configured")

        with self.global_league_service.store.write() as db:
            match_table = db.table("scorer_match_stats")
            team_table = db.table("scorer_team_match_stats")
            player_table = db.table("scorer_player_match_stats")

            existing_match = match_table.get(lambda row: (row.get("match_key") or "").strip() == safe_match_key)
            if not existing_match:
                return {
                    "ok": False,
                    "removed": False,
                    "match_key": safe_match_key,
                }

            removed_team_rows = len(team_table.search(lambda row: (row.get("match_key") or "").strip() == safe_match_key))
            removed_player_rows = len(
                player_table.search(lambda row: (row.get("match_key") or "").strip() == safe_match_key)
            )

            match_table.remove(doc_ids=[existing_match.doc_id])
            team_table.remove(lambda row: (row.get("match_key") or "").strip() == safe_match_key)
            player_table.remove(lambda row: (row.get("match_key") or "").strip() == safe_match_key)

            self._rebuild_global_aggregates(db)

        return {
            "ok": True,
            "removed": True,
            "match_key": safe_match_key,
            "match_id": existing_match.get("match_id") or "",
            "season_slug": existing_match.get("season_slug") or "",
            "removed_team_rows": removed_team_rows,
            "removed_player_rows": removed_player_rows,
        }

    def _parse_match_csv_rows(self, text: str, file_name: str):
        reader = csv.reader(io.StringIO(text))
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError(f"{file_name}: invalid CSV header") from exc

        if not header:
            raise ValueError(f"{file_name}: invalid CSV header")

        missing = [column for column in self.CSV_REQUIRED_COLUMNS if column not in header]
        if missing:
            raise ValueError(f"{file_name}: missing columns: {', '.join(missing)}")

        width = len(header)

        def to_row(values):
            padded = list(values[:width]) + [""] * max(0, width - len(values))
            return {header[idx]: (padded[idx] or "").strip() for idx in range(width)}

        rows = []
        substitution_log_player_ins = set()
        in_substitution_log = False

        for values in reader:
            if not values or not any((cell or "").strip() for cell in values):
                continue

            first_cell = (values[0] or "").strip()
            if first_cell == "Substitution Log":
                in_substitution_log = True
                continue

            if in_substitution_log:
                # Format in exported scorer CSV:
                # "Step","Playing Team","Player Out","Player In","From Team"
                # Skip the header row and collect valid Player In values.
                if first_cell.lower() == "step":
                    continue
                player_in = (values[3] if len(values) > 3 else "").strip()
                if player_in and player_in.lower() != "none":
                    substitution_log_player_ins.add(player_in)
                continue

            row = to_row(values)
            match_id = (row.get("Match ID") or "").strip()
            if not match_id:
                continue
            rows.append(row)

        if not rows:
            raise ValueError(f"{file_name}: no delivery rows found")

        return rows, substitution_log_player_ins

    def _build_global_identity_maps(self, season_slug: str):
        safe_season_slug = (season_slug or "").strip().lower()
        global_tables = self._global_tables()

        global_players = list(global_tables.get("global_players", [])) if isinstance(global_tables, dict) else []
        global_teams = list(global_tables.get("global_teams", [])) if isinstance(global_tables, dict) else []
        season_player_links = list(global_tables.get("season_player_links", [])) if isinstance(global_tables, dict) else []
        season_team_links = list(global_tables.get("season_team_links", [])) if isinstance(global_tables, dict) else []

        players_by_id = {}
        players_by_name = {}
        players_by_key = {}
        for row in global_players:
            player_id = (row.get("id") or "").strip()
            if not player_id:
                continue
            players_by_id[player_id] = row

            normalized_name = self._norm(row.get("name") or "")
            if normalized_name and normalized_name not in players_by_name:
                players_by_name[normalized_name] = player_id

            speciality = (row.get("speciality") or "ALL_ROUNDER").strip().upper()
            normalized_key = f"{normalized_name}|{speciality}" if normalized_name else ""
            if normalized_key and normalized_key not in players_by_key:
                players_by_key[normalized_key] = player_id

        teams_by_id = {}
        teams_by_name = {}
        for row in global_teams:
            team_id = (row.get("id") or "").strip()
            if not team_id:
                continue
            teams_by_id[team_id] = row
            team_name_key = self._norm(row.get("name") or "")
            if team_name_key and team_name_key not in teams_by_name:
                teams_by_name[team_name_key] = team_id

        player_global_by_local = {}
        for row in season_player_links:
            if (row.get("season_slug") or "").strip().lower() != safe_season_slug:
                continue
            local_player_id = (row.get("local_player_id") or "").strip()
            global_player_id = (row.get("global_player_id") or "").strip()
            if local_player_id and global_player_id:
                player_global_by_local[local_player_id] = global_player_id

        team_global_by_local = {}
        manager_global_by_local_manager_id = {}
        manager_global_by_team_local = {}
        manager_global_by_team_global = {}
        for row in season_team_links:
            if (row.get("season_slug") or "").strip().lower() != safe_season_slug:
                continue

            local_team_id = (row.get("local_team_id") or "").strip()
            global_team_id = (row.get("global_team_id") or "").strip()
            manager_player_id = (row.get("manager_player_id") or "").strip()
            manager_global_player_id = (row.get("manager_global_player_id") or "").strip()

            if local_team_id and global_team_id:
                team_global_by_local[local_team_id] = global_team_id

            if manager_player_id and manager_global_player_id:
                manager_global_by_local_manager_id[manager_player_id] = manager_global_player_id

            if local_team_id and manager_global_player_id:
                manager_global_by_team_local[local_team_id] = manager_global_player_id

            if global_team_id and manager_global_player_id:
                manager_global_by_team_global[global_team_id] = manager_global_player_id

        return {
            "safe_season_slug": safe_season_slug,
            "players_by_id": players_by_id,
            "players_by_name": players_by_name,
            "players_by_key": players_by_key,
            "teams_by_id": teams_by_id,
            "teams_by_name": teams_by_name,
            "player_global_by_local": player_global_by_local,
            "team_global_by_local": team_global_by_local,
            "manager_global_by_local_manager_id": manager_global_by_local_manager_id,
            "manager_global_by_team_local": manager_global_by_team_local,
            "manager_global_by_team_global": manager_global_by_team_global,
        }

    def _upsert_season_player_link(self, local_player_id: str, global_player_id: str, player_name: str, season_slug: str):
        if not local_player_id or not global_player_id or not season_slug:
            return
        store = getattr(self.global_league_service, "store", None)
        if not store:
            return

        with store.write() as db:
            links = db.table("season_player_links")
            existing = links.get(
                lambda row: row.get("season_slug") == season_slug and row.get("local_player_id") == local_player_id
            )
            payload = {
                "season_slug": season_slug,
                "local_player_id": local_player_id,
                "global_player_id": global_player_id,
                "player_name": player_name,
                "updated_at": self._now_iso(),
            }
            if existing:
                links.update(payload, doc_ids=[existing.doc_id])
            else:
                links.insert(payload)

    def _upsert_season_team_link(self, local_team_id: str, global_team_id: str, team_name: str, season_slug: str):
        if not local_team_id or not global_team_id or not season_slug:
            return
        store = getattr(self.global_league_service, "store", None)
        if not store:
            return

        with store.write() as db:
            links = db.table("season_team_links")
            existing = links.get(
                lambda row: row.get("season_slug") == season_slug and row.get("local_team_id") == local_team_id
            )
            payload = {
                "season_slug": season_slug,
                "local_team_id": local_team_id,
                "global_team_id": global_team_id,
                "team_name": team_name,
                "updated_at": self._now_iso(),
            }
            if existing:
                links.update(payload, doc_ids=[existing.doc_id])
            else:
                links.insert(payload)

    def _resolve_team_id(self, raw_team_id: str, team_name: str, season_slug: str, maps: dict):
        safe_team_id = (raw_team_id or "").strip()
        safe_team_name = (team_name or "").strip()

        if safe_team_id and safe_team_id in maps["teams_by_id"]:
            return safe_team_id

        if safe_team_id and safe_team_id in maps["team_global_by_local"]:
            return maps["team_global_by_local"][safe_team_id]

        normalized_name = self._norm(safe_team_name)
        if normalized_name and normalized_name in maps["teams_by_name"]:
            resolved = maps["teams_by_name"][normalized_name]
            if safe_team_id and safe_team_id not in maps["team_global_by_local"]:
                maps["team_global_by_local"][safe_team_id] = resolved
                self._upsert_season_team_link(safe_team_id, resolved, safe_team_name, season_slug)
            return resolved

        if not self.global_league_service:
            return safe_team_id

        with self.global_league_service.store.write() as db:
            global_team, _ = self.global_league_service._ensure_global_team(
                db,
                {
                    "name": safe_team_name or safe_team_id or "Unknown Team",
                },
            )

        resolved_team_id = (global_team.get("id") or "").strip()
        if not resolved_team_id:
            return safe_team_id

        maps["teams_by_id"][resolved_team_id] = global_team
        if normalized_name:
            maps["teams_by_name"][normalized_name] = resolved_team_id
        if safe_team_id:
            maps["team_global_by_local"][safe_team_id] = resolved_team_id
            self._upsert_season_team_link(safe_team_id, resolved_team_id, safe_team_name, season_slug)

        return resolved_team_id

    def _resolve_player_id(self, raw_player_id: str, player_name: str, season_slug: str, maps: dict, role_hint: str = "ALL_ROUNDER"):
        safe_player_id = (raw_player_id or "").strip()
        safe_player_name = (player_name or "").strip()

        if safe_player_id and safe_player_id in maps["players_by_id"]:
            return safe_player_id

        if safe_player_id and safe_player_id in maps["player_global_by_local"]:
            return maps["player_global_by_local"][safe_player_id]

        if safe_player_id and safe_player_id in maps["manager_global_by_local_manager_id"]:
            return maps["manager_global_by_local_manager_id"][safe_player_id]

        normalized_name = self._norm(safe_player_name)
        if normalized_name and normalized_name in maps["players_by_name"]:
            resolved = maps["players_by_name"][normalized_name]
            if safe_player_id and safe_player_id not in maps["player_global_by_local"]:
                maps["player_global_by_local"][safe_player_id] = resolved
                self._upsert_season_player_link(safe_player_id, resolved, safe_player_name, season_slug)
            return resolved

        if not self.global_league_service:
            return safe_player_id

        fallback_code = self.FANTASY_PLAYER_TIERS.get(normalized_name, "G")
        inferred_tier = self._fantasy_code_to_tier(fallback_code)
        inferred_speciality = self._speciality_to_role(role_hint)
        if normalized_name in self.FANTASY_PLAYER_ROLES:
            inferred_speciality = self.FANTASY_PLAYER_ROLES[normalized_name]

        with self.global_league_service.store.write() as db:
            global_player, _ = self.global_league_service._ensure_global_player(
                db,
                {
                    "name": safe_player_name or safe_player_id or "Unknown",
                    "tier": inferred_tier,
                    "speciality": inferred_speciality,
                },
            )

        resolved_player_id = (global_player.get("id") or "").strip()
        if not resolved_player_id:
            return safe_player_id

        maps["players_by_id"][resolved_player_id] = global_player
        if normalized_name:
            maps["players_by_name"][normalized_name] = resolved_player_id

        speciality = (global_player.get("speciality") or "ALL_ROUNDER").strip().upper()
        player_key = f"{normalized_name}|{speciality}" if normalized_name else ""
        if player_key:
            maps["players_by_key"][player_key] = resolved_player_id

        if safe_player_id:
            maps["player_global_by_local"][safe_player_id] = resolved_player_id
            self._upsert_season_player_link(safe_player_id, resolved_player_id, safe_player_name, season_slug)

        return resolved_player_id

    def _resolve_manager_id(self, raw_manager_id: str, raw_team_id: str, resolved_team_id: str, maps: dict):
        safe_manager_id = (raw_manager_id or "").strip()
        safe_raw_team_id = (raw_team_id or "").strip()
        safe_resolved_team_id = (resolved_team_id or "").strip()

        if safe_manager_id and safe_manager_id in maps["players_by_id"]:
            return safe_manager_id

        if safe_manager_id and safe_manager_id in maps["manager_global_by_local_manager_id"]:
            return maps["manager_global_by_local_manager_id"][safe_manager_id]

        if safe_manager_id and safe_manager_id in maps["player_global_by_local"]:
            return maps["player_global_by_local"][safe_manager_id]

        if safe_manager_id and safe_manager_id in maps["manager_global_by_team_local"]:
            return maps["manager_global_by_team_local"][safe_manager_id]

        if safe_raw_team_id and safe_raw_team_id in maps["manager_global_by_team_local"]:
            return maps["manager_global_by_team_local"][safe_raw_team_id]

        if safe_resolved_team_id and safe_resolved_team_id in maps["manager_global_by_team_global"]:
            return maps["manager_global_by_team_global"][safe_resolved_team_id]

        return safe_manager_id

    def _normalize_rows_to_global_ids(self, rows, season_slug: str):
        maps = self._build_global_identity_maps(season_slug)
        normalized_rows = []

        for row in rows:
            patched = dict(row)

            raw_batting_team_id = (row.get("Batting Team ID") or "").strip()
            raw_bowling_team_id = (row.get("Bowling Team ID") or "").strip()

            batting_team_id = self._resolve_team_id(
                raw_batting_team_id,
                row.get("Batting Team") or "",
                season_slug,
                maps,
            )
            bowling_team_id = self._resolve_team_id(
                raw_bowling_team_id,
                row.get("Bowling Team") or "",
                season_slug,
                maps,
            )

            batter_id = self._resolve_player_id(
                row.get("Batter ID") or "",
                row.get("Batter") or "",
                season_slug,
                maps,
                role_hint="BATTER",
            )
            non_striker_id = self._resolve_player_id(
                row.get("Non Strike Batter ID") or "",
                row.get("Non Strike Batter") or "",
                season_slug,
                maps,
                role_hint="BATTER",
            )
            bowler_id = self._resolve_player_id(
                row.get("Bowler ID") or "",
                row.get("Bowler") or "",
                season_slug,
                maps,
                role_hint="BOWLER",
            )

            dismissed_batter = (row.get("Dismissed Batter") or "").strip()
            dismissed_batter_id = (row.get("Dismissed Batter ID") or "").strip()
            if dismissed_batter and dismissed_batter != "None":
                dismissed_batter_id = self._resolve_player_id(
                    dismissed_batter_id,
                    dismissed_batter,
                    season_slug,
                    maps,
                    role_hint="BATTER",
                )

            batting_manager_id = self._resolve_manager_id(
                row.get("Batting Manager ID") or "",
                raw_batting_team_id,
                batting_team_id,
                maps,
            )
            bowling_manager_id = self._resolve_manager_id(
                row.get("Bowling Manager ID") or "",
                raw_bowling_team_id,
                bowling_team_id,
                maps,
            )

            patched["Batting Team ID"] = batting_team_id or raw_batting_team_id
            patched["Bowling Team ID"] = bowling_team_id or raw_bowling_team_id
            patched["Batter ID"] = batter_id or (row.get("Batter ID") or "")
            patched["Non Strike Batter ID"] = non_striker_id or (row.get("Non Strike Batter ID") or "")
            patched["Bowler ID"] = bowler_id or (row.get("Bowler ID") or "")
            patched["Dismissed Batter ID"] = dismissed_batter_id or (row.get("Dismissed Batter ID") or "")
            patched["Batting Manager ID"] = batting_manager_id or (row.get("Batting Manager ID") or "")
            patched["Bowling Manager ID"] = bowling_manager_id or (row.get("Bowling Manager ID") or "")

            if batting_team_id in maps["teams_by_id"]:
                patched["Batting Team"] = maps["teams_by_id"][batting_team_id].get("name") or patched.get("Batting Team")
            if bowling_team_id in maps["teams_by_id"]:
                patched["Bowling Team"] = maps["teams_by_id"][bowling_team_id].get("name") or patched.get("Bowling Team")

            if patched["Batter ID"] in maps["players_by_id"]:
                patched["Batter"] = maps["players_by_id"][patched["Batter ID"]].get("name") or patched.get("Batter")
            if patched["Non Strike Batter ID"] in maps["players_by_id"]:
                patched["Non Strike Batter"] = (
                    maps["players_by_id"][patched["Non Strike Batter ID"]].get("name") or patched.get("Non Strike Batter")
                )
            if patched["Bowler ID"] in maps["players_by_id"]:
                patched["Bowler"] = maps["players_by_id"][patched["Bowler ID"]].get("name") or patched.get("Bowler")
            if patched["Dismissed Batter ID"] in maps["players_by_id"]:
                patched["Dismissed Batter"] = (
                    maps["players_by_id"][patched["Dismissed Batter ID"]].get("name") or patched.get("Dismissed Batter")
                )

            normalized_rows.append(patched)

        return normalized_rows

    def _build_match_outcome(self, team_name_by_id: dict, match_result: str):
        result_text = (match_result or "").strip()
        normalized_result = self._norm(result_text)

        outcome = {team_id: "no_result" for team_id in team_name_by_id.keys()}
        winner_id = ""

        if "won" in normalized_result:
            for team_id, team_name in team_name_by_id.items():
                if self._norm(team_name) and self._norm(team_name) in normalized_result:
                    winner_id = team_id
                    break
            if winner_id:
                for team_id in outcome.keys():
                    outcome[team_id] = "win" if team_id == winner_id else "loss"
        elif "tied" in normalized_result:
            for team_id in outcome.keys():
                outcome[team_id] = "tie"

        return outcome, winner_id

    def _get_player_metadata(self, season_slug: str):
        maps = self._build_global_identity_maps(season_slug)
        return maps.get("players_by_id", {})

    def _derive_match_stats(
        self,
        rows,
        season_slug: str,
        source_file: str,
        uploaded_by: str,
        match_date: str = "",
        include_in_fantasy_points: bool = True,
        substitution_log_player_ins=None,
    ):
        first = rows[0]
        match_id = (first.get("Match ID") or "").strip() or "unknown"
        match_name = (first.get("Match") or "").strip()
        venue = (first.get("Venue") or "").strip()
        match_result = (first.get("Match Result") or "").strip()
        match_toss = (first.get("Match Toss") or "").strip()
        scorer_version = (first.get("Scorer Version") or "").strip()

        match_key = self._match_key(season_slug, match_id)

        player_master = self._get_player_metadata(season_slug)

        team_rows = {}
        player_rows = {}

        def ensure_team(team_id: str, team_name: str):
            safe_team_id = (team_id or "").strip() or self._norm(team_name) or "unknown-team"
            safe_team_name = (team_name or "").strip() or safe_team_id
            if safe_team_id not in team_rows:
                team_rows[safe_team_id] = {
                    "match_key": match_key,
                    "season_slug": season_slug,
                    "match_id": match_id,
                    "match": match_name,
                    "team_id": safe_team_id,
                    "team_name": safe_team_name,
                    "runs_scored": 0,
                    "balls_faced": 0,
                    "wickets_lost": 0,
                    "fours": 0,
                    "sixes": 0,
                    "wides_faced": 0,
                    "noballs_faced": 0,
                    "runs_conceded": 0,
                    "balls_bowled": 0,
                    "wickets_taken": 0,
                    "wides_bowled": 0,
                    "noballs_bowled": 0,
                    "fantasy_points": 0,
                    "updated_at": self._now_iso(),
                }
            elif safe_team_name and not team_rows[safe_team_id].get("team_name"):
                team_rows[safe_team_id]["team_name"] = safe_team_name
            return team_rows[safe_team_id]

        def ensure_player(player_id: str, player_name: str, team_id: str, team_name: str, role_hint: str):
            safe_player_id = (player_id or "").strip()
            if not safe_player_id:
                return None

            global_row = player_master.get(safe_player_id, {})
            safe_player_name = (player_name or "").strip() or (global_row.get("name") or safe_player_id)
            tier = (global_row.get("tier") or "").strip().lower()
            role = self._speciality_to_role(global_row.get("speciality") or role_hint)
            if not tier:
                tier = self._fantasy_code_to_tier(self._tier_to_fantasy_code("", safe_player_name))

            if safe_player_id not in player_rows:
                player_rows[safe_player_id] = {
                    "match_key": match_key,
                    "season_slug": season_slug,
                    "match_id": match_id,
                    "match": match_name,
                    "player_id": safe_player_id,
                    "player_name": safe_player_name,
                    "team_id": (team_id or "").strip(),
                    "team_name": (team_name or "").strip(),
                    "role": role,
                    "tier": tier,
                    "matches": 1,
                    "innings_batted": 0,
                    "not_out": 0,
                    "dismissed": 0,
                    "runs": 0,
                    "balls_faced": 0,
                    "fours": 0,
                    "sixes": 0,
                    "innings_bowled": 0,
                    "balls_bowled": 0,
                    "runs_conceded": 0,
                    "wickets": 0,
                    "wides": 0,
                    "noballs": 0,
                    "strike_rate": 0.0,
                    "economy": 0.0,
                    "fantasy_score": 0,
                    "fantasy_bat_points": 0.0,
                    "fantasy_bowl_points": 0.0,
                    "updated_at": self._now_iso(),
                }

            player = player_rows[safe_player_id]
            if (team_id or "").strip() and not player.get("team_id"):
                player["team_id"] = (team_id or "").strip()
                player["team_name"] = (team_name or "").strip()
            return player

        for row in rows:
            batting_team_id = (row.get("Batting Team ID") or "").strip()
            bowling_team_id = (row.get("Bowling Team ID") or "").strip()
            batting_team_name = (row.get("Batting Team") or "").strip()
            bowling_team_name = (row.get("Bowling Team") or "").strip()

            batting_team = ensure_team(batting_team_id, batting_team_name)
            bowling_team = ensure_team(bowling_team_id, bowling_team_name)

            batter_id = (row.get("Batter ID") or "").strip()
            non_striker_id = (row.get("Non Strike Batter ID") or "").strip()
            bowler_id = (row.get("Bowler ID") or "").strip()
            dismissed_batter = (row.get("Dismissed Batter") or "").strip()
            dismissed_batter_id = (row.get("Dismissed Batter ID") or "").strip()

            batter = ensure_player(batter_id, row.get("Batter") or "", batting_team_id, batting_team_name, "BATTER")
            non_striker = ensure_player(
                non_striker_id,
                row.get("Non Strike Batter") or "",
                batting_team_id,
                batting_team_name,
                "BATTER",
            )
            bowler = ensure_player(bowler_id, row.get("Bowler") or "", bowling_team_id, bowling_team_name, "BOWLER")

            if batter:
                batter["innings_batted"] = 1
            if non_striker:
                non_striker["innings_batted"] = 1
            if bowler:
                bowler["innings_bowled"] = 1

            runs_bat = self._safe_int(row.get("Runs Bat"), 0)
            runs_extra = self._safe_int(row.get("Runs Extra"), 0)
            total_runs = runs_bat + runs_extra
            extras_type = (row.get("Extras Type") or "None").strip() or "None"
            valid_ball = (row.get("Valid Ball?") or "").strip() == "Yes"
            is_wicket = bool(dismissed_batter and dismissed_batter != "None")

            batting_team["runs_scored"] += total_runs
            if valid_ball:
                batting_team["balls_faced"] += 1
            if extras_type == "Wide":
                batting_team["wides_faced"] += runs_extra
            if extras_type == "No Ball":
                batting_team["noballs_faced"] += runs_extra
            if is_wicket:
                batting_team["wickets_lost"] += 1

            bowling_team["runs_conceded"] += total_runs
            if valid_ball:
                bowling_team["balls_bowled"] += 1
            if extras_type == "Wide":
                bowling_team["wides_bowled"] += runs_extra
            if extras_type == "No Ball":
                bowling_team["noballs_bowled"] += runs_extra
            if is_wicket:
                bowling_team["wickets_taken"] += 1

            if batter:
                if extras_type != "Wide":
                    batter["balls_faced"] += 1
                batter["runs"] += runs_bat
                if runs_bat == 4:
                    batter["fours"] += 1
                    batting_team["fours"] += 1
                if runs_bat == 6:
                    batter["sixes"] += 1
                    batting_team["sixes"] += 1

            if bowler:
                if valid_ball:
                    bowler["balls_bowled"] += 1
                bowler["runs_conceded"] += total_runs
                if extras_type == "Wide":
                    bowler["wides"] += runs_extra
                if extras_type == "No Ball":
                    bowler["noballs"] += runs_extra
                if is_wicket:
                    bowler["wickets"] += 1

            if is_wicket and dismissed_batter_id:
                dismissed_player = ensure_player(
                    dismissed_batter_id,
                    dismissed_batter,
                    batting_team_id,
                    batting_team_name,
                    "BATTER",
                )
                if dismissed_player:
                    dismissed_player["innings_batted"] = 1
                    dismissed_player["dismissed"] = 1

        # Build fantasy scores using the same logic as scoreCard.py.
        fantasy_scores = self._calculate_fantasy_scores(
            rows,
            player_rows,
            substitution_log_player_ins=substitution_log_player_ins,
        )
        for player_id, fantasy in fantasy_scores.items():
            if player_id not in player_rows:
                continue
            # Actual fantasy points include a flat per-match bonus over calculated score.
            if bool(fantasy.get("is_substitute", False)):
                actual_score = 0
            else:
                actual_score = self._round_nearest_int(float(fantasy.get("score", 0.0)) + self.FANTASY_MATCH_BONUS_POINTS, 0)
            player_rows[player_id]["fantasy_score"] = actual_score
            player_rows[player_id]["fantasy_bat_points"] = fantasy.get("bat_pts", 0.0)
            player_rows[player_id]["fantasy_bowl_points"] = fantasy.get("bowl_pts", 0.0)

            team_id = (player_rows[player_id].get("team_id") or "").strip()
            if team_id in team_rows:
                team_rows[team_id]["fantasy_points"] += actual_score

        for player in player_rows.values():
            if player["innings_batted"] and not player["dismissed"]:
                player["not_out"] = 1
            if player["balls_faced"] > 0:
                player["strike_rate"] = round(player["runs"] * 100.0 / player["balls_faced"], 2)
            if player["balls_bowled"] > 0:
                overs = player["balls_bowled"] / 6.0
                player["economy"] = round(player["runs_conceded"] / overs, 2)

        outcome_by_team, winner_team_id = self._build_match_outcome(
            {team_id: item.get("team_name") for team_id, item in team_rows.items()},
            match_result,
        )

        for team_id, team in team_rows.items():
            outcome = outcome_by_team.get(team_id, "no_result")
            team["result"] = outcome
            team["wins"] = 1 if outcome == "win" else 0
            team["losses"] = 1 if outcome == "loss" else 0
            team["ties"] = 1 if outcome == "tie" else 0
            team["no_results"] = 1 if outcome == "no_result" else 0
            team["overs_faced"] = self._overs_string(team["balls_faced"])
            team["overs_bowled"] = self._overs_string(team["balls_bowled"])
            team["run_rate_for"] = round((team["runs_scored"] * 6.0 / team["balls_faced"]), 2) if team["balls_faced"] else 0.0
            team["run_rate_against"] = (
                round((team["runs_conceded"] * 6.0 / team["balls_bowled"]), 2) if team["balls_bowled"] else 0.0
            )
            team["net_run_rate"] = round(team["run_rate_for"] - team["run_rate_against"], 2)

        uploaded_at = self._now_iso()
        match_row = {
            "match_key": match_key,
            "season_slug": season_slug,
            "match_id": match_id,
            "match": match_name,
            "venue": venue,
            "match_date": match_date,
            "result": match_result,
            "toss": match_toss,
            "winner_team_id": winner_team_id,
            "scorer_version": scorer_version,
            "delivery_rows": len(rows),
            "team_rows": len(team_rows),
            "player_rows": len(player_rows),
            "source_file": source_file,
            "uploaded_by": uploaded_by,
            "uploaded_at": uploaded_at,
            "include_in_fantasy_points": bool(include_in_fantasy_points),
        }

        for team in team_rows.values():
            team["uploaded_at"] = uploaded_at
        for player in player_rows.values():
            player["uploaded_at"] = uploaded_at

        return {
            "match_key": match_key,
            "match_row": match_row,
            "team_rows": list(team_rows.values()),
            "player_rows": list(player_rows.values()),
        }

    def _fantasy_tier_for_player(self, player_id: str, player_name: str, player_meta: dict):
        meta = player_meta.get(player_id, {}) if isinstance(player_meta, dict) else {}
        tier = (meta.get("tier") or "").strip().lower()
        return self._tier_to_fantasy_code(tier, player_name)

    def _fantasy_role_for_player(self, player_id: str, player_name: str, player_meta: dict):
        meta = player_meta.get(player_id, {}) if isinstance(player_meta, dict) else {}
        role = self._speciality_to_role(meta.get("role") or "")
        if role != "ALL_ROUNDER":
            return role

        speciality = self._speciality_to_role(meta.get("speciality") or "")
        if speciality != "ALL_ROUNDER":
            return speciality

        fallback = self.FANTASY_PLAYER_ROLES.get(self._norm(player_name), "ALL_ROUNDER")
        return self._speciality_to_role(fallback)

    def _fantasy_matchup_multiplier(self, bat_tier: str, bowl_tier: str, role: str, base_points: float):
        bat_val = self.FANTASY_TIERS[bat_tier]["value"]
        bowl_val = self.FANTASY_TIERS[bowl_tier]["value"]
        diff = abs(bat_val - bowl_val)
        if diff == 0:
            return 1.0

        upset_mult = 1.15 if diff == 1 else 1.65
        expected_mult = 1.0 / upset_mult
        is_positive = base_points > 0
        batter_won = is_positive if role == "BATTER" else not is_positive

        if batter_won:
            return upset_mult if bat_val < bowl_val else expected_mult
        return upset_mult if bowl_val < bat_val else expected_mult

    def _calculate_fantasy_scores(self, rows, player_rows: dict, substitution_log_player_ins=None):
        players = {}
        substitute_players = set()
        observed_ids_by_name = {}

        substitution_log_player_ins = substitution_log_player_ins or set()

        for row in rows:
            batter_name = (row.get("Batter") or "").strip()
            bowler_name = (row.get("Bowler") or "").strip()
            dismissed_name = (row.get("Dismissed Batter") or "").strip()
            batter_id = (row.get("Batter ID") or "").strip()
            bowler_id = (row.get("Bowler ID") or "").strip()
            dismissed_id = (row.get("Dismissed Batter ID") or "").strip()

            if batter_name and batter_id:
                observed_ids_by_name[self._norm(batter_name)] = batter_id
            if bowler_name and bowler_id:
                observed_ids_by_name[self._norm(bowler_name)] = bowler_id
            if dismissed_name and dismissed_name != "None" and dismissed_id:
                observed_ids_by_name[self._norm(dismissed_name)] = dismissed_id

        for row in rows:
            substitutions = (row.get("Substitution Details") or "").strip()
            if not substitutions or substitutions == "None":
                continue
            for entry in substitutions.split("|"):
                if "->" not in entry:
                    continue
                incoming_name = entry.split("->", 1)[1].split("(", 1)[0].strip()
                incoming_id = observed_ids_by_name.get(self._norm(incoming_name), "")
                if incoming_id:
                    substitute_players.add(incoming_id)

        # Also honor substitutions reported only in the CSV's trailing Substitution Log section.
        for incoming_name in substitution_log_player_ins:
            incoming_id = observed_ids_by_name.get(self._norm(incoming_name), "")
            if incoming_id:
                substitute_players.add(incoming_id)

        for row in rows:
            batter_id = (row.get("Batter ID") or "").strip()
            bowler_id = (row.get("Bowler ID") or "").strip()
            dismissed_id = (row.get("Dismissed Batter ID") or "").strip()

            runs_bat = self._safe_int(row.get("Runs Bat"), 0)
            runs_extra = self._safe_int(row.get("Runs Extra"), 0)
            extras_type = (row.get("Extras Type") or "None").strip() or "None"
            is_wicket = bool(dismissed_id and (row.get("Dismissed Batter") or "").strip() != "None")
            is_striker_out = dismissed_id == batter_id
            is_valid_ball = (row.get("Valid Ball?") or "").strip() == "Yes"
            is_wide = extras_type == "Wide"
            is_no_ball = extras_type == "No Ball"

            init_ids = [player_id for player_id in [batter_id, bowler_id] if player_id]
            if is_wicket and dismissed_id and dismissed_id not in init_ids:
                init_ids.append(dismissed_id)

            for player_id in init_ids:
                if player_id in players:
                    continue

                row_meta = player_rows.get(player_id, {})
                player_name = row_meta.get("player_name") or ""
                players[player_id] = {
                    "role": self._fantasy_role_for_player(player_id, player_name, {player_id: row_meta}),
                    "batting_points": 0.0,
                    "bowling_points": 0.0,
                    "balls_faced": 0,
                    "balls_bowled": 0,
                    "team": (row.get("Batting Team ID") or "").strip() if player_id == batter_id else (row.get("Bowling Team ID") or "").strip(),
                }

            if not batter_id or not bowler_id:
                continue

            batter_name = (row.get("Batter") or "").strip()
            bowler_name = (row.get("Bowler") or "").strip()
            bat_tier = self._fantasy_tier_for_player(batter_id, batter_name, player_rows)
            bowl_tier = self._fantasy_tier_for_player(bowler_id, bowler_name, player_rows)

            # Batting points (striker)
            if not is_wide:
                players[batter_id]["balls_faced"] += 1
                bat_role = players[batter_id]["role"]
                bat_base = self.FANTASY_BAT_POINTS["OUT"] if is_striker_out else self.FANTASY_BAT_POINTS.get(runs_bat, 0)
                bat_role_mult = 1.2 if bat_role == "BOWLER" else 1.0
                matchup = self._fantasy_matchup_multiplier(bat_tier, bowl_tier, "BATTER", bat_base)
                if bat_base >= 0:
                    points = bat_base * self.FANTASY_TIERS[bat_tier]["reward"] * matchup * bat_role_mult
                else:
                    points = bat_base * self.FANTASY_TIERS[bat_tier]["penalty"] * matchup
                players[batter_id]["batting_points"] += points

            # Batting points (non-striker dismissal)
            if is_wicket and dismissed_id and not is_striker_out and dismissed_id in players:
                dismissed_name = (row.get("Dismissed Batter") or "").strip()
                ns_tier = self._fantasy_tier_for_player(dismissed_id, dismissed_name, player_rows)
                ns_base = self.FANTASY_BAT_POINTS["OUT"]
                ns_matchup = self._fantasy_matchup_multiplier(ns_tier, bowl_tier, "BATTER", ns_base)
                ns_points = ns_base * self.FANTASY_TIERS[ns_tier]["penalty"] * ns_matchup
                players[dismissed_id]["batting_points"] += ns_points

            # Bowling points
            players[bowler_id]["balls_bowled"] += 1
            bowl_role = players[bowler_id]["role"]
            bowl_base = 0
            if is_wicket:
                bowl_base = self.FANTASY_BOWL_POINTS["WICKET"]
            elif is_valid_ball:
                bowl_base = self.FANTASY_BOWL_POINTS.get(runs_bat, 0)
            elif is_wide:
                bowl_base = -1.5
                if runs_extra > 1:
                    additional_runs = runs_extra - 1
                    if additional_runs in self.FANTASY_BOWL_POINTS and self.FANTASY_BOWL_POINTS[additional_runs] < 0:
                        bowl_base += self.FANTASY_BOWL_POINTS[additional_runs]
            elif is_no_ball:
                bowl_base = -2.5
                if runs_bat > 0 and self.FANTASY_BOWL_POINTS.get(runs_bat, 0) < 0:
                    bowl_base += self.FANTASY_BOWL_POINTS[runs_bat]

            bowl_role_mult = 1.2 if bowl_role == "BATTER" else 1.0
            bowl_matchup = self._fantasy_matchup_multiplier(bat_tier, bowl_tier, "BOWLER", bowl_base)
            if bowl_base >= 0:
                bowl_points = bowl_base * self.FANTASY_TIERS[bowl_tier]["reward"] * bowl_matchup * bowl_role_mult
            else:
                bowl_points = bowl_base * self.FANTASY_TIERS[bowl_tier]["penalty"] * bowl_matchup
            players[bowler_id]["bowling_points"] += bowl_points

        raw_scores = {}
        substitute_suppressed = set()
        for player_id, stats in players.items():
            total_balls = stats["balls_faced"] + stats["balls_bowled"]
            if player_id in substitute_players:
                raw_scores[player_id] = 0.0
                substitute_suppressed.add(player_id)
            elif total_balls == 0 and stats["batting_points"] == 0.0 and stats["bowling_points"] == 0.0:
                continue
            else:
                raw_scores[player_id] = stats["batting_points"] + stats["bowling_points"]

        results = {}
        for player_id, raw in raw_scores.items():
            row_meta = player_rows.get(player_id, {})
            results[player_id] = {
                "score": raw,
                "raw": raw,
                "role": stats.get("role") if (stats := players.get(player_id, {})) else row_meta.get("role", "ALL_ROUNDER"),
                "team": row_meta.get("team_id") or stats.get("team", ""),
                "bat_pts": players.get(player_id, {}).get("batting_points", 0.0),
                "bowl_pts": players.get(player_id, {}).get("bowling_points", 0.0),
                "is_substitute": player_id in substitute_suppressed,
            }

        return results

    def _persist_match_stats(self, derived: dict):
        if not self.global_league_service or not getattr(self.global_league_service, "store", None):
            raise ValueError("Global league store is not configured")

        match_key = derived["match_key"]
        match_row = derived["match_row"]
        team_rows = derived["team_rows"]
        player_rows = derived["player_rows"]

        with self.global_league_service.store.write() as db:
            match_table = db.table("scorer_match_stats")
            team_table = db.table("scorer_team_match_stats")
            player_table = db.table("scorer_player_match_stats")

            existing_match = match_table.get(lambda row: row.get("match_key") == match_key)
            if existing_match:
                match_table.update(match_row, doc_ids=[existing_match.doc_id])
            else:
                match_table.insert(match_row)

            team_table.remove(lambda row: row.get("match_key") == match_key)
            player_table.remove(lambda row: row.get("match_key") == match_key)

            if team_rows:
                team_table.insert_multiple(team_rows)
            if player_rows:
                player_table.insert_multiple(player_rows)

            self._rebuild_global_aggregates(db)

        summary = {
            "match_key": match_key,
            "match_id": match_row.get("match_id"),
            "season_slug": match_row.get("season_slug"),
            "team_rows": len(team_rows),
            "player_rows": len(player_rows),
            "source_file": match_row.get("source_file"),
            "uploaded_at": match_row.get("uploaded_at"),
            "include_in_fantasy_points": bool(match_row.get("include_in_fantasy_points", True)),
        }
        return summary

    def _rebuild_global_aggregates(self, db):
        match_rows = db.table("scorer_match_stats").all()
        team_match_rows = db.table("scorer_team_match_stats").all()
        player_match_rows = db.table("scorer_player_match_stats").all()

        include_in_fantasy_by_match = {}
        for row in match_rows:
            key = (row.get("match_key") or "").strip()
            if not key:
                continue
            include_in_fantasy_by_match[key] = bool(row.get("include_in_fantasy_points", True))

        team_aggregate = {}
        for row in team_match_rows:
            team_id = (row.get("team_id") or "").strip()
            if not team_id:
                continue

            match_key = (row.get("match_key") or "").strip()
            include_in_fantasy_points = include_in_fantasy_by_match.get(match_key, True)

            if team_id not in team_aggregate:
                team_aggregate[team_id] = {
                    "team_id": team_id,
                    "team_name": row.get("team_name") or team_id,
                    "matches": 0,
                    "wins": 0,
                    "losses": 0,
                    "ties": 0,
                    "no_results": 0,
                    "runs_scored": 0,
                    "balls_faced": 0,
                    "wickets_lost": 0,
                    "fours": 0,
                    "sixes": 0,
                    "wides_faced": 0,
                    "noballs_faced": 0,
                    "runs_conceded": 0,
                    "balls_bowled": 0,
                    "wickets_taken": 0,
                    "wides_bowled": 0,
                    "noballs_bowled": 0,
                    "fantasy_points": 0,
                    "updated_at": self._now_iso(),
                }

            agg = team_aggregate[team_id]
            agg["team_name"] = row.get("team_name") or agg["team_name"]
            agg["matches"] += 1
            agg["wins"] += self._safe_int(row.get("wins"), 0)
            agg["losses"] += self._safe_int(row.get("losses"), 0)
            agg["ties"] += self._safe_int(row.get("ties"), 0)
            agg["no_results"] += self._safe_int(row.get("no_results"), 0)
            agg["runs_scored"] += self._safe_int(row.get("runs_scored"), 0)
            agg["balls_faced"] += self._safe_int(row.get("balls_faced"), 0)
            agg["wickets_lost"] += self._safe_int(row.get("wickets_lost"), 0)
            agg["fours"] += self._safe_int(row.get("fours"), 0)
            agg["sixes"] += self._safe_int(row.get("sixes"), 0)
            agg["wides_faced"] += self._safe_int(row.get("wides_faced"), 0)
            agg["noballs_faced"] += self._safe_int(row.get("noballs_faced"), 0)
            agg["runs_conceded"] += self._safe_int(row.get("runs_conceded"), 0)
            agg["balls_bowled"] += self._safe_int(row.get("balls_bowled"), 0)
            agg["wickets_taken"] += self._safe_int(row.get("wickets_taken"), 0)
            agg["wides_bowled"] += self._safe_int(row.get("wides_bowled"), 0)
            agg["noballs_bowled"] += self._safe_int(row.get("noballs_bowled"), 0)
            if include_in_fantasy_points:
                agg["fantasy_points"] += self._round_nearest_int(row.get("fantasy_points"), 0)
            agg["updated_at"] = self._now_iso()

        for agg in team_aggregate.values():
            agg["overs_faced"] = self._overs_string(agg["balls_faced"])
            agg["overs_bowled"] = self._overs_string(agg["balls_bowled"])
            agg["run_rate_for"] = round((agg["runs_scored"] * 6.0 / agg["balls_faced"]), 2) if agg["balls_faced"] else 0.0
            agg["run_rate_against"] = (
                round((agg["runs_conceded"] * 6.0 / agg["balls_bowled"]), 2) if agg["balls_bowled"] else 0.0
            )
            agg["net_run_rate"] = round(agg["run_rate_for"] - agg["run_rate_against"], 2)
            agg["fantasy_points_per_match"] = (
                self._round_nearest_int(agg["fantasy_points"] / agg["matches"], 0) if agg["matches"] else 0
            )

        player_aggregate = {}
        for row in player_match_rows:
            player_id = (row.get("player_id") or "").strip()
            if not player_id:
                continue

            match_key = (row.get("match_key") or "").strip()
            include_in_fantasy_points = include_in_fantasy_by_match.get(match_key, True)

            if player_id not in player_aggregate:
                player_aggregate[player_id] = {
                    "player_id": player_id,
                    "player_name": row.get("player_name") or player_id,
                    "team_id": row.get("team_id") or "",
                    "team_name": row.get("team_name") or "",
                    "role": row.get("role") or "ALL_ROUNDER",
                    "tier": row.get("tier") or "gold",
                    "matches": 0,
                    "innings_batted": 0,
                    "not_out": 0,
                    "dismissed": 0,
                    "runs": 0,
                    "balls_faced": 0,
                    "fours": 0,
                    "sixes": 0,
                    "innings_bowled": 0,
                    "balls_bowled": 0,
                    "runs_conceded": 0,
                    "wickets": 0,
                    "wides": 0,
                    "noballs": 0,
                    "fantasy_score": 0,
                    "fantasy_bat_points": 0.0,
                    "fantasy_bowl_points": 0.0,
                    "updated_at": self._now_iso(),
                }

            agg = player_aggregate[player_id]
            agg["player_name"] = row.get("player_name") or agg["player_name"]
            agg["team_id"] = row.get("team_id") or agg["team_id"]
            agg["team_name"] = row.get("team_name") or agg["team_name"]
            agg["role"] = row.get("role") or agg["role"]
            agg["tier"] = row.get("tier") or agg["tier"]
            agg["matches"] += 1
            agg["innings_batted"] += self._safe_int(row.get("innings_batted"), 0)
            agg["not_out"] += self._safe_int(row.get("not_out"), 0)
            agg["dismissed"] += self._safe_int(row.get("dismissed"), 0)
            agg["runs"] += self._safe_int(row.get("runs"), 0)
            agg["balls_faced"] += self._safe_int(row.get("balls_faced"), 0)
            agg["fours"] += self._safe_int(row.get("fours"), 0)
            agg["sixes"] += self._safe_int(row.get("sixes"), 0)
            agg["innings_bowled"] += self._safe_int(row.get("innings_bowled"), 0)
            agg["balls_bowled"] += self._safe_int(row.get("balls_bowled"), 0)
            agg["runs_conceded"] += self._safe_int(row.get("runs_conceded"), 0)
            agg["wickets"] += self._safe_int(row.get("wickets"), 0)
            agg["wides"] += self._safe_int(row.get("wides"), 0)
            agg["noballs"] += self._safe_int(row.get("noballs"), 0)
            if include_in_fantasy_points:
                agg["fantasy_score"] += self._round_nearest_int(row.get("fantasy_score"), 0)
                agg["fantasy_bat_points"] += float(row.get("fantasy_bat_points") or 0.0)
                agg["fantasy_bowl_points"] += float(row.get("fantasy_bowl_points") or 0.0)
            agg["updated_at"] = self._now_iso()

        for agg in player_aggregate.values():
            agg["strike_rate"] = round((agg["runs"] * 100.0 / agg["balls_faced"]), 2) if agg["balls_faced"] else 0.0
            agg["batting_average"] = round((agg["runs"] / agg["dismissed"]), 2) if agg["dismissed"] else float(agg["runs"])
            agg["economy"] = round((agg["runs_conceded"] * 6.0 / agg["balls_bowled"]), 2) if agg["balls_bowled"] else 0.0
            agg["overs_bowled"] = self._overs_string(agg["balls_bowled"])
            agg["fantasy_average"] = self._round_nearest_int(agg["fantasy_score"] / agg["matches"], 0) if agg["matches"] else 0

        team_global_table = db.table("scorer_team_global_stats")
        player_global_table = db.table("scorer_player_global_stats")
        team_global_table.truncate()
        player_global_table.truncate()

        if team_aggregate:
            team_global_table.insert_multiple(list(team_aggregate.values()))
        if player_aggregate:
            player_global_table.insert_multiple(list(player_aggregate.values()))
