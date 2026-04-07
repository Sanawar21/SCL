import secrets
from werkzeug.security import check_password_hash, generate_password_hash

from app.rules import ROLE_ADMIN, ROLE_MANAGER


class AuthService:
    def __init__(self, store):
        self.store = store

    def seed_admin_if_missing(self):
        with self.store.write() as db:
            users = db.table("users")
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

    def login(self, username: str, password: str):
        with self.store.read() as db:
            users = db.table("users")
            user = users.get(lambda u: u.get("username") == username)
            if not user:
                return None
            if not check_password_hash(user["password_hash"], password):
                return None
            return {
                "username": user["username"],
                "role": user["role"],
                "display_name": user.get("display_name", user["username"]),
                "team_id": user.get("team_id"),
            }

    def create_manager(self, username: str, display_name: str, team_name: str, manager_tier: str):
        # temp_password = secrets.token_urlsafe(8)
        temp_password = "password123" # TODO: Remove this 
        with self.store.write() as db:
            users = db.table("users")
            teams = db.table("teams")
            if users.get(lambda u: u.get("username") == username):
                raise ValueError("Username already exists")

            team_id = secrets.token_hex(8)
            users.insert(
                {
                    "username": username,
                    "password_hash": generate_password_hash(temp_password),
                    "role": ROLE_MANAGER,
                    "display_name": display_name,
                    "team_id": team_id,
                }
            )
            teams.insert(
                {
                    "id": team_id,
                    "name": team_name,
                    "manager_username": username,
                    "manager_tier": manager_tier,
                    "players": [],
                    "bench": [],
                    "spent": 0,
                    "purse_remaining": None,
                    "credits_remaining": None,
                }
            )

        return {"team_id": team_id, "temporary_password": temp_password}
