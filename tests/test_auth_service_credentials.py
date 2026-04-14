import sys
import tempfile
import types
import unittest
from pathlib import Path

if "flask_socketio" not in sys.modules:
    stub = types.ModuleType("flask_socketio")

    class _SocketIOStub:
        def __init__(self, *args, **kwargs):
            pass

    stub.SocketIO = _SocketIOStub
    sys.modules["flask_socketio"] = stub

from app.db import LockedTinyDB
from app.services.auth_service import AuthService


class AuthServiceCredentialsTests(unittest.TestCase):
    def test_create_team_credentials_generates_secure_temp_password(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            auth_db = Path(temp_dir) / "auth.json"
            service = AuthService(LockedTinyDB(str(auth_db)))

            first = service.create_team_credentials("Alpha Team")
            second = service.create_team_credentials("Beta Team")
            service.auth_store.db.close()

        self.assertNotEqual(first["temporary_password"], "password123")
        self.assertEqual(len(first["temporary_password"]), 14)
        self.assertNotEqual(first["temporary_password"], second["temporary_password"])


if __name__ == "__main__":
    unittest.main()
