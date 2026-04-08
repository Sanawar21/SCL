import threading
from contextlib import contextmanager
from tinydb import TinyDB


class LockedTinyDB:
    def __init__(self, path: str):
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
