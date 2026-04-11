import secrets
from werkzeug.security import check_password_hash, generate_password_hash

from app.rules import ROLE_ADMIN, ROLE_MANAGER


class AuthService:
    def __init__(self, auth_store):
        self.auth_store = auth_store

    def seed_admin_if_missing(self):
        with self.auth_store.write() as db:
            users = db.table("auth_users")
            if users.get(lambda u: u.get("role") == ROLE_ADMIN):
                return
            users.insert(
                {
                    "username": "admin",
                    "password_hash": generate_password_hash("admin123"),
                    "role": ROLE_ADMIN,
                    "display_name": "Administrator",
                }
            )

    def get_user(self, username: str):
        safe_username = (username or "").strip()
        if not safe_username:
            return None
        with self.auth_store.read() as db:
            users = db.table("auth_users")
            return users.get(lambda u: u.get("username") == safe_username)

    def assert_username_available(self, username: str, except_username: str | None = None):
        safe_username = (username or "").strip()
        except_name = (except_username or "").strip() or None
        with self.auth_store.read() as db:
            users = db.table("auth_users")
            user = users.get(lambda u: u.get("username") == safe_username)
            if user and user.get("username") != except_name:
                raise ValueError("Username already exists")

    def login(self, username: str, password: str):
        safe_username = (username or "").strip()
        with self.auth_store.read() as db:
            users = db.table("auth_users")
            user = users.get(lambda u: u.get("username") == safe_username)
            if not user:
                return None
            if not check_password_hash(user["password_hash"], password):
                return None
            return {
                "username": user["username"],
                "role": user["role"],
                "display_name": user.get("display_name", user["username"]),
            }

    def create_manager_credentials(self, username: str, display_name: str):
        temp_password = "password123"  # TODO: Remove this
        safe_username = (username or "").strip()
        safe_display_name = (display_name or "").strip() or safe_username

        with self.auth_store.write() as db:
            users = db.table("auth_users")
            if users.get(lambda u: u.get("username") == safe_username):
                raise ValueError("Username already exists")

            users.insert(
                {
                    "username": safe_username,
                    "password_hash": generate_password_hash(temp_password),
                    "role": ROLE_MANAGER,
                    "display_name": safe_display_name,
                }
            )

        return {"temporary_password": temp_password}

    def update_user(self, current_username: str, new_username: str, display_name: str):
        safe_current = (current_username or "").strip()
        safe_new = (new_username or "").strip()
        safe_display = (display_name or "").strip() or safe_new

        with self.auth_store.write() as db:
            users = db.table("auth_users")
            existing = users.get(lambda u: u.get("username") == safe_current)
            if not existing:
                raise ValueError("User not found")

            username_conflict = users.get(
                lambda u: u.get("username") == safe_new and u.get("username") != safe_current
            )
            if username_conflict:
                raise ValueError("Username already exists")

            users.update(
                {
                    "username": safe_new,
                    "display_name": safe_display,
                },
                lambda u: u.get("username") == safe_current,
            )

    def delete_user(self, username: str):
        safe_username = (username or "").strip()
        with self.auth_store.write() as db:
            users = db.table("auth_users")
            users.remove(lambda u: u.get("username") == safe_username)
