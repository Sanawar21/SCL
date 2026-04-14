import json
import re
import sqlite3
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
        self._db_path = getattr(getattr(auction_service.store, "db", None), "path", None)

        app_root_path = Path(app_root)
        self.workspace_root = app_root_path.parent
        self.template_path = self.workspace_root / "scorer.html"

        config_file = Path(config_path)
        self.config_path = config_file if config_file.is_absolute() else (self.workspace_root / config_file)
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        if not self._db_path:
            raise RuntimeError("Scorer database path is not configured")
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        return conn

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
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT s.slug,
                       s.name,
                       a.saved_at,
                       s.published_at,
                       a.id
                FROM seasons s
                JOIN auctions a ON a.season_id = s.id
                WHERE a.status = 'published'
                ORDER BY COALESCE(a.saved_at, s.published_at, '') DESC, a.id DESC
                """
            ).fetchall()

        seasons = []
        seen = set()
        for row in rows:
            slug = (row["slug"] or "").strip().lower()
            if not slug or slug in seen:
                continue
            seen.add(slug)
            seasons.append(
                {
                    "slug": slug,
                    "name": (row["name"] or slug),
                    "published_at": row["saved_at"] or row["published_at"] or "",
                }
            )
        return seasons

    def _default_season_slug(self):
        seasons = self.list_seasons()
        return seasons[0]["slug"] if seasons else ""

    def _resolve_auction(self, conn: sqlite3.Connection, season_slug: str):
        safe_slug = (season_slug or "").strip().lower()
        if safe_slug:
            row = conn.execute(
                """
                SELECT a.id,
                       a.phase,
                       a.current_player_id,
                       s.slug,
                       s.name
                FROM auctions a
                JOIN seasons s ON s.id = a.season_id
                WHERE a.status = 'published'
                  AND (s.slug = ? OR s.id = ?)
                ORDER BY COALESCE(a.saved_at, '') DESC, COALESCE(a.created_at, '') DESC, a.id DESC
                LIMIT 1
                """,
                (safe_slug, safe_slug),
            ).fetchone()
            if row:
                return row

        return conn.execute(
            """
            SELECT a.id,
                   a.phase,
                   a.current_player_id,
                   s.slug,
                   s.name
            FROM auctions a
            JOIN seasons s ON s.id = a.season_id
            ORDER BY
                CASE a.status WHEN 'active' THEN 0 WHEN 'published' THEN 1 ELSE 2 END,
                COALESCE(a.saved_at, '') DESC,
                COALESCE(a.updated_at, '') DESC,
                a.id DESC
            LIMIT 1
            """
        ).fetchone()

    def _build_teams_for_auction(self, conn: sqlite3.Connection, auction_id: str):
        team_rows = conn.execute(
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

        roster_rows = conn.execute(
            """
            SELECT tr.team_id,
                   tr.player_id,
                   tr.roster_role,
                   p.display_name,
                   p.tier,
                   p.speciality
            FROM team_rosters tr
            JOIN players p ON p.id = tr.player_id
            WHERE tr.auction_id = ?
            ORDER BY LOWER(COALESCE(p.display_name, '')) ASC
            """,
            (auction_id,),
        ).fetchall()

        roster_by_team = {}
        for row in roster_rows:
            role = (row["roster_role"] or "").strip().lower()
            if role not in {"active", "bench"}:
                continue
            roster_by_team.setdefault(row["team_id"], []).append(
                {
                    "id": row["player_id"],
                    "name": row["display_name"] or "Unknown",
                    "tier": (row["tier"] or "").strip().lower(),
                    "speciality": (row["speciality"] or "-").strip() or "-",
                }
            )

        output = []
        for team in team_rows:
            team_id = team["id"]
            team_name = (team["name"] or team_id or "Team").strip()
            manager_username = (team["manager_username"] or "").strip()

            roster = list(roster_by_team.get(team_id, []))
            if manager_username:
                roster.append(
                    {
                        "id": team_id,
                        "name": (team["manager_name"] or manager_username or team_name),
                        "tier": (team["manager_tier"] or "").strip().lower(),
                        "speciality": (team["manager_speciality"] or "-").strip() or "-",
                    }
                )

            roster.sort(key=lambda item: item.get("name", "").lower())
            output.append(
                {
                    "id": team_id,
                    "name": team_name,
                    "manager_id": team_id,
                    "manager_username": manager_username,
                    "manager_name": (team["manager_name"] or manager_username or team_name),
                    "manager_tier": (team["manager_tier"] or "").strip().lower(),
                    "players": roster,
                }
            )
        return output

    def build_context(self):
        config = self.load_config()
        season_slug = config.get("season_slug") or self._default_season_slug()

        with self._connect() as conn:
            auction = self._resolve_auction(conn, season_slug)
            teams = self._build_teams_for_auction(conn, auction["id"]) if auction else []

            resolved_slug = (auction["slug"] if auction and auction["slug"] else season_slug) or ""
            resolved_name = (auction["name"] if auction and auction["name"] else resolved_slug) or config["title"]

        payload = {
            "title": config["title"],
            "version": config["version"],
            "max_overs": config["max_overs"],
            "season": {
                "slug": resolved_slug,
                "name": resolved_name,
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
