import threading
from contextlib import contextmanager
from pathlib import Path
import re
from tinydb import TinyDB


class LockedTinyDB:
    def __init__(self, path: str):
        path_obj = Path(path)
        if path_obj.parent and str(path_obj.parent) not in {"", "."}:
            path_obj.parent.mkdir(parents=True, exist_ok=True)
        self.db = TinyDB(path)
        self._lock = threading.RLock()

    @contextmanager
    def read(self):
        with self._lock:
            yield self.db

    @contextmanager
    def write(self):
        with self._lock:
            yield self.db

    def export_tables(self):
        with self._lock:
            return {
                table_name: self.db.table(table_name).all()
                for table_name in sorted(self.db.tables())
            }

    def import_tables(self, tables):
        with self._lock:
            for table_name in list(self.db.tables()):
                self.db.table(table_name).truncate()

            for table_name, rows in tables.items():
                table = self.db.table(table_name)
                if rows:
                    table.insert_multiple(rows)


class SeasonStoreManager:
    _SEASON_SLUG_RE = re.compile(r"^[A-Za-z0-9_-]+$")

    def __init__(self, base_dir: str, app_root: str):
        configured = Path(base_dir)
        app_base = Path(app_root).parent
        self.base_dir = configured if configured.is_absolute() else (app_base / configured)
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self._stores = {}
        self._lock = threading.RLock()

    def _validate_slug(self, season_slug: str) -> str:
        safe_slug = (season_slug or "").strip().lower()
        if not self._SEASON_SLUG_RE.fullmatch(safe_slug):
            raise ValueError("Invalid season slug")
        return safe_slug

    def _file_path(self, season_slug: str) -> Path:
        safe_slug = self._validate_slug(season_slug)
        return self.base_dir / f"{safe_slug}.json"

    def has_season(self, season_slug: str) -> bool:
        return self._file_path(season_slug).exists()

    def get_store(self, season_slug: str, create: bool = False) -> LockedTinyDB:
        safe_slug = self._validate_slug(season_slug)
        file_path = self._file_path(safe_slug)

        with self._lock:
            if not file_path.exists() and not create:
                raise ValueError("Season database not found")

            key = str(file_path)
            if key not in self._stores:
                self._stores[key] = LockedTinyDB(key)
            return self._stores[key]

    def list_slugs(self):
        files = sorted(
            self.base_dir.glob("*.json"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        return [file_path.stem for file_path in files]
