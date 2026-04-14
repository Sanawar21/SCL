import json
import sqlite3
from werkzeug.security import check_password_hash, generate_password_hash

from app.rules import ROLE_ADMIN, ROLE_MANAGER


class AuthService:
    def __init__(self, auth_store):
        self.auth_store = auth_store
        self._db_path = getattr(getattr(auth_store, "db", None), "path", None)

    def _connect(self):
        if not self._db_path:
            raise RuntimeError("Authentication database path is not configured")
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _normalize_role(value: str):
        role = (value or "").strip().lower()
        if role == ROLE_ADMIN:
            return ROLE_ADMIN
        return ROLE_MANAGER

    def _get_auth_row(self, conn: sqlite3.Connection, username: str):
        safe_username = (username or "").strip()
        if not safe_username:
            return None
        return conn.execute(
            """
            SELECT id, username, role, display_name, password_hash
            FROM users
            WHERE username = ?
              AND password_hash IS NOT NULL
              AND LENGTH(TRIM(password_hash)) > 0
            ORDER BY
                CASE WHEN LOWER(role) = 'admin' THEN 0 ELSE 1 END,
                id ASC
            LIMIT 1
            """,
            (safe_username,),
        ).fetchone()

    def seed_admin_if_missing(self):
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT 1
                FROM users
                WHERE LOWER(role) = ?
                  AND password_hash IS NOT NULL
                  AND LENGTH(TRIM(password_hash)) > 0
                LIMIT 1
                """,
                (ROLE_ADMIN,),
            ).fetchone()
            if existing:
                return

            conn.execute(
                """
                INSERT INTO users
                (username, role, display_name, speciality, team_id, password_hash, source_scope, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "admin",
                    ROLE_ADMIN,
                    "Administrator",
                    None,
                    None,
                    generate_password_hash("admin123"),
                    "runtime-auth",
                    json.dumps({"seeded": True}, ensure_ascii=True),
                ),
            )
            conn.commit()

    def get_user(self, username: str):
        safe_username = (username or "").strip()
        if not safe_username:
            return None
        with self._connect() as conn:
            row = self._get_auth_row(conn, safe_username)
            if not row:
                return None
            return {
                "username": row["username"],
                "password_hash": row["password_hash"],
                "role": self._normalize_role(row["role"]),
                "display_name": (row["display_name"] or row["username"]),
            }

    def assert_username_available(self, username: str, except_username: str | None = None):
        safe_username = (username or "").strip()
        except_name = (except_username or "").strip() or None
        with self._connect() as conn:
            row = self._get_auth_row(conn, safe_username)
            if row and row["username"] != except_name:
                raise ValueError("Username already exists")

    def login(self, username: str, password: str):
        safe_username = (username or "").strip()
        with self._connect() as conn:
            row = self._get_auth_row(conn, safe_username)
            if not row:
                return None
            if not check_password_hash(row["password_hash"], password):
                return None
            return {
                "username": row["username"],
                "role": self._normalize_role(row["role"]),
                "display_name": (row["display_name"] or row["username"]),
            }

    def create_manager_credentials(self, username: str, display_name: str):
        temp_password = "password123"  # TODO: Remove this
        safe_username = (username or "").strip()
        safe_display_name = (display_name or "").strip() or safe_username

        with self._connect() as conn:
            if self._get_auth_row(conn, safe_username):
                raise ValueError("Username already exists")

            conn.execute(
                """
                INSERT INTO users
                (username, role, display_name, speciality, team_id, password_hash, source_scope, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    safe_username,
                    ROLE_MANAGER,
                    safe_display_name,
                    None,
                    None,
                    generate_password_hash(temp_password),
                    "runtime-auth",
                    json.dumps({"created_by": "auth_service"}, ensure_ascii=True),
                ),
            )
            conn.commit()

        return {"temporary_password": temp_password}

    def update_user(self, current_username: str, new_username: str, display_name: str):
        safe_current = (current_username or "").strip()
        safe_new = (new_username or "").strip()
        safe_display = (display_name or "").strip() or safe_new

        with self._connect() as conn:
            existing = self._get_auth_row(conn, safe_current)
            if not existing:
                raise ValueError("User not found")

            conflict = self._get_auth_row(conn, safe_new)
            if conflict and safe_new != safe_current:
                raise ValueError("Username already exists")

            conn.execute(
                """
                UPDATE users
                SET username = ?,
                    display_name = ?
                WHERE id = ?
                """,
                (safe_new, safe_display, int(existing["id"])),
            )
            conn.commit()

    def delete_user(self, username: str):
        safe_username = (username or "").strip()
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM users
                WHERE username = ?
                  AND password_hash IS NOT NULL
                  AND LENGTH(TRIM(password_hash)) > 0
                """,
                (safe_username,),
            )
            conn.commit()
