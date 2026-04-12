import json
import re
from pathlib import Path


class ScorerService:
    DEFAULT_CONFIG = {
        "title": "SCL Scorer",
        "version": "1.0.0",
        "season_slug": "",
        "max_overs": 5,
    }

    def __init__(self, season_store_manager, auction_service, app_root: str, config_path: str):
        self.season_store_manager = season_store_manager
        self.auction_service = auction_service

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

    def _build_teams(self, tables):
        teams = list(tables.get("teams", [])) if isinstance(tables, dict) else []
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

        roster_teams = []
        for team in teams:
            team_id = (team.get("id") or "").strip()
            team_name = (team.get("name") or team_id or "Team").strip()
            manager_username = (team.get("manager_username") or "").strip()
            manager_user = users_by_username.get(manager_username, {})

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
                roster.append(
                    {
                        "id": player.get("id"),
                        "name": player.get("name") or "Unknown",
                        "tier": (player.get("tier") or "").strip().lower(),
                        "speciality": (player.get("speciality") or "-").strip() or "-",
                    }
                )

            if manager_username:
                roster.append(
                    {
                        "id": team_id,
                        "name": manager_user.get("display_name") or manager_username or team_name,
                        "tier": (team.get("manager_tier") or "").strip().lower(),
                        "speciality": (manager_user.get("speciality") or team.get("manager_speciality") or "-").strip() or "-",
                    }
                )

            roster.sort(key=lambda item: item.get("name", "").lower())
            roster_teams.append(
                {
                    "id": team_id,
                    "name": team_name,
                    "manager_id": team_id,
                    "manager_username": manager_username,
                    "manager_name": manager_user.get("display_name") or manager_username or team_name,
                    "manager_tier": (team.get("manager_tier") or "").strip().lower(),
                    "players": roster,
                }
            )

        return roster_teams

    def build_context(self):
        config = self.load_config()
        season_slug = config.get("season_slug") or self._default_season_slug()
        tables, source_meta = self._load_tables(season_slug)
        teams = self._build_teams(tables)

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
