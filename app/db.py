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
