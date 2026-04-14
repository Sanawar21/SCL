import json
import re
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from app.rules import TIER_BASE_PRICE, TIER_CREDIT_COST


def _iter_legacy_rows(table_obj):
    if isinstance(table_obj, dict):
        for key in sorted(table_obj.keys(), key=lambda k: (len(str(k)), str(k))):
            row = table_obj.get(key)
            if isinstance(row, dict):
                yield str(key), row
    elif isinstance(table_obj, list):
        for idx, row in enumerate(table_obj, start=1):
            if isinstance(row, dict):
                yield str(idx), row


def migrate_legacy_json_to_sqlite(legacy_json_path: str | Path, sqlite_path: str | Path):
    legacy_path = Path(legacy_json_path)
    target_path = Path(sqlite_path)
    if not legacy_path.exists():
        return False

    try:
        payload = json.loads(legacy_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return False

    if not isinstance(payload, dict):
        return False

    target_store = LockedTinyDB(str(target_path))

    tables = {}
    for table_name, table_obj in payload.items():
        if not isinstance(table_name, str):
            continue
        rows = [dict(row) for _, row in _iter_legacy_rows(table_obj)]
        tables[table_name] = rows

    target_store.import_tables(tables)
    return True


def _safe_json_loads(raw_value):
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except Exception:  # noqa: BLE001
        return {}
    return parsed if isinstance(parsed, dict) else {}


def bootstrap_auction_store_from_normalized(store, sqlite_path: str | Path):
    with store.read() as db:
        if len(db.table("teams")) > 0 or len(db.table("players")) > 0:
            return False

    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    try:
        auction_row = conn.execute(
            """
            SELECT id, phase, current_player_id, created_at, metadata_json
            FROM auctions
            ORDER BY
                CASE status WHEN 'active' THEN 0 WHEN 'published' THEN 1 ELSE 2 END,
                COALESCE(saved_at, '') DESC,
                COALESCE(updated_at, '') DESC,
                id DESC
            LIMIT 1
            """
        ).fetchone()
        if not auction_row:
            return False

        auction_id = auction_row["id"]
        auction_meta_json = _safe_json_loads(auction_row["metadata_json"])
        source_meta = auction_meta_json.get("meta") if isinstance(auction_meta_json.get("meta"), dict) else {}

        auction_team_rows = conn.execute(
            """
            SELECT at.team_id,
                   at.purse_remaining,
                   at.credits_remaining,
                   t.name,
                   t.manager_username,
                   t.manager_tier,
                   t.manager_player_id
            FROM auction_teams at
            JOIN teams t ON t.id = at.team_id
            WHERE at.auction_id = ?
            ORDER BY t.name ASC
            """,
            (auction_id,),
        ).fetchall()

        auction_player_rows = conn.execute(
            """
            SELECT ap.player_id,
                   ap.opening_price,
                   ap.entry_status,
                   ap.current_bid,
                   ap.sold_to_team_id,
                   ap.sold_price,
                   ap.phase_sold,
                   ap.metadata_json AS ap_metadata_json,
                   p.display_name,
                   p.tier,
                   p.speciality
            FROM auction_players ap
            JOIN players p ON p.id = ap.player_id
            WHERE ap.auction_id = ?
            ORDER BY p.display_name ASC
            """,
            (auction_id,),
        ).fetchall()

        bid_rows = conn.execute(
            """
            SELECT id, ts, team_id, player_id, amount, phase, kind
            FROM bids
            WHERE auction_id = ?
            ORDER BY COALESCE(ts, '') ASC, id ASC
            """,
            (auction_id,),
        ).fetchall()

        trade_rows = conn.execute(
            """
            SELECT id,
                   status,
                   created_at,
                   from_team_id,
                   to_team_id,
                   offered_player_id,
                   requested_player_id,
                   metadata_json
            FROM trades
            WHERE auction_id = ?
            ORDER BY COALESCE(created_at, '') DESC, id DESC
            """,
            (auction_id,),
        ).fetchall()

        sold_spend_by_team = {
            row[0]: int(row[1] or 0)
            for row in conn.execute(
                """
                SELECT sold_to_team_id, SUM(COALESCE(sold_price, 0))
                FROM auction_players
                WHERE auction_id = ? AND sold_to_team_id IS NOT NULL
                GROUP BY sold_to_team_id
                """,
                (auction_id,),
            ).fetchall()
        }

        roster_rows = conn.execute(
            """
            SELECT team_id, player_id, roster_role
            FROM team_rosters
            WHERE auction_id = ?
            ORDER BY team_id ASC, roster_role ASC, player_id ASC
            """,
            (auction_id,),
        ).fetchall()
        active_roster_by_team = {}
        bench_roster_by_team = {}
        for roster_row in roster_rows:
            team_id = roster_row["team_id"]
            player_id = roster_row["player_id"]
            role = (roster_row["roster_role"] or "").strip().lower()
            if role == "active":
                active_roster_by_team.setdefault(team_id, []).append(player_id)
            elif role == "bench":
                bench_roster_by_team.setdefault(team_id, []).append(player_id)

        manager_profiles = {}
        manager_player_ids = [
            row["manager_player_id"]
            for row in auction_team_rows
            if row["manager_player_id"]
        ]
        if manager_player_ids:
            placeholders = ",".join(["?" for _ in manager_player_ids])
            profile_rows = conn.execute(
                f"""
                SELECT id, display_name, speciality
                FROM players
                WHERE id IN ({placeholders})
                """,
                manager_player_ids,
            ).fetchall()
            manager_profiles = {
                row["id"]: {
                    "display_name": (row["display_name"] or "").strip(),
                    "speciality": (row["speciality"] or "ALL_ROUNDER").strip() or "ALL_ROUNDER",
                }
                for row in profile_rows
            }
    finally:
        conn.close()

    teams = []
    users = []
    for team_row in auction_team_rows:
        team_id = team_row["team_id"]
        roster_active = active_roster_by_team.get(team_id, [])
        roster_bench = bench_roster_by_team.get(team_id, [])

        teams.append(
            {
                "id": team_id,
                "name": team_row["name"],
                "manager_username": team_row["manager_username"],
                "manager_tier": (team_row["manager_tier"] or "silver").strip().lower(),
                "players": roster_active,
                "bench": roster_bench,
                "spent": sold_spend_by_team.get(team_id, 0),
                "purse_remaining": team_row["purse_remaining"],
                "credits_remaining": team_row["credits_remaining"],
            }
        )

        manager_username = (team_row["manager_username"] or "").strip()
        if manager_username:
            profile = manager_profiles.get(team_row["manager_player_id"], {})
            users.append(
                {
                    "username": manager_username,
                    "role": "manager",
                    "display_name": profile.get("display_name") or manager_username,
                    "speciality": profile.get("speciality") or "ALL_ROUNDER",
                    "team_id": team_id,
                }
            )

    players = []
    for row in auction_player_rows:
        tier = (row["tier"] or "").strip().lower()
        ap_meta = _safe_json_loads(row["ap_metadata_json"])
        status = (row["entry_status"] or "").strip().lower()
        if not status:
            status = "sold" if row["sold_to_team_id"] else "unsold"
        if status not in {"sold", "unsold"}:
            status = "sold" if row["sold_to_team_id"] else "unsold"

        players.append(
            {
                "id": row["player_id"],
                "name": row["display_name"],
                "tier": tier,
                "speciality": (row["speciality"] or "ALL_ROUNDER").strip() or "ALL_ROUNDER",
                "base_price": row["opening_price"] if row["opening_price"] is not None else TIER_BASE_PRICE.get(tier, 0),
                "status": status,
                "sold_to": row["sold_to_team_id"],
                "sold_price": int(row["sold_price"] or 0),
                "phase_sold": row["phase_sold"],
                "credits": int(ap_meta.get("credits") or TIER_CREDIT_COST.get(tier, 0)),
                "current_bid": int(row["current_bid"] or 0),
                "current_bidder_team_id": ap_meta.get("current_bidder_team_id"),
                "nominated_phase_a": bool(ap_meta.get("nominated_phase_a", False)),
            }
        )

    bids = [
        {
            "id": row["id"],
            "ts": row["ts"],
            "team_id": row["team_id"],
            "player_id": row["player_id"],
            "amount": int(row["amount"] or 0),
            "phase": row["phase"],
            "kind": row["kind"],
        }
        for row in bid_rows
    ]

    trade_requests = []
    for row in trade_rows:
        trade_meta = _safe_json_loads(row["metadata_json"])
        trade_requests.append(
            {
                "id": row["id"],
                "status": row["status"] or trade_meta.get("status") or "pending",
                "created_at": row["created_at"] or trade_meta.get("created_at"),
                "from_team_id": row["from_team_id"],
                "to_team_id": row["to_team_id"],
                "offered_player_id": row["offered_player_id"],
                "requested_player_id": row["requested_player_id"],
                "cash_from_initiator": int(trade_meta.get("cash_from_initiator", 0) or 0),
                "cash_from_target": int(trade_meta.get("cash_from_target", 0) or 0),
                "responded_at": trade_meta.get("responded_at"),
                "responded_by_team_id": trade_meta.get("responded_by_team_id"),
            }
        )

    meta = {
        "phase": auction_row["phase"] or source_meta.get("phase") or "setup",
        "created_at": auction_row["created_at"],
        "current_player_id": auction_row["current_player_id"],
        "nomination_history": source_meta.get("nomination_history") or [],
    }

    with store.write() as db:
        for table_name in ["meta", "teams", "users", "players", "bids", "trade_requests"]:
            db.table(table_name).truncate()

        db.table("meta").insert(meta)
        if teams:
            db.table("teams").insert_multiple(teams)
        if users:
            db.table("users").insert_multiple(users)
        if players:
            db.table("players").insert_multiple(players)
        if bids:
            db.table("bids").insert_multiple(bids)
        if trade_requests:
            db.table("trade_requests").insert_multiple(trade_requests)

    return True


class QueryCondition:
    def __init__(self, predicate):
        self._predicate = predicate

    def __call__(self, row):
        return bool(self._predicate(row))

    def __and__(self, other):
        return QueryCondition(lambda row: self(row) and bool(other(row)))

    def __or__(self, other):
        return QueryCondition(lambda row: self(row) or bool(other(row)))

    def __invert__(self):
        return QueryCondition(lambda row: not self(row))


class QueryField:
    def __init__(self, field_name: str):
        self.field_name = field_name

    def _value(self, row):
        return row.get(self.field_name)

    def __eq__(self, other):
        return QueryCondition(lambda row: self._value(row) == other)

    def __ne__(self, other):
        return QueryCondition(lambda row: self._value(row) != other)

    def __lt__(self, other):
        return QueryCondition(lambda row: self._value(row) < other)

    def __le__(self, other):
        return QueryCondition(lambda row: self._value(row) <= other)

    def __gt__(self, other):
        return QueryCondition(lambda row: self._value(row) > other)

    def __ge__(self, other):
        return QueryCondition(lambda row: self._value(row) >= other)


class Query:
    def __getattr__(self, item):
        return QueryField(item)

    def __getitem__(self, item):
        return QueryField(item)


class SQLiteDocument(dict):
    def __init__(self, doc_id: int, payload: dict):
        super().__init__(payload)
        self.doc_id = doc_id


class SQLiteTable:
    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock, table_name: str):
        self._conn = conn
        self._lock = lock
        self._table_name = table_name

    def _load_docs(self):
        cur = self._conn.execute(
            """
            SELECT doc_id, data_json
            FROM tinydb_store
            WHERE table_name = ?
            ORDER BY doc_id ASC
            """,
            (self._table_name,),
        )
        docs = []
        for row in cur.fetchall():
            payload = json.loads(row["data_json"])
            docs.append(SQLiteDocument(int(row["doc_id"]), payload))
        return docs

    @staticmethod
    def _matches(condition, row):
        if condition is None:
            return True
        if isinstance(condition, QueryCondition):
            return condition(row)
        if callable(condition):
            return bool(condition(row))
        return False

    def all(self):
        with self._lock:
            return self._load_docs()

    def __len__(self):
        with self._lock:
            cur = self._conn.execute(
                "SELECT COUNT(*) AS count FROM tinydb_store WHERE table_name = ?",
                (self._table_name,),
            )
            row = cur.fetchone()
            return int(row["count"] if row else 0)

    def get(self, condition=None, doc_id=None):
        with self._lock:
            if doc_id is not None:
                cur = self._conn.execute(
                    """
                    SELECT doc_id, data_json
                    FROM tinydb_store
                    WHERE table_name = ? AND doc_id = ?
                    """,
                    (self._table_name, int(doc_id)),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return SQLiteDocument(int(row["doc_id"]), json.loads(row["data_json"]))

            for row in self._load_docs():
                if self._matches(condition, row):
                    return row
            return None

    def search(self, condition):
        with self._lock:
            return [row for row in self._load_docs() if self._matches(condition, row)]

    def _next_doc_id(self):
        cur = self._conn.execute(
            "SELECT COALESCE(MAX(doc_id), 0) + 1 AS next_id FROM tinydb_store WHERE table_name = ?",
            (self._table_name,),
        )
        row = cur.fetchone()
        return int(row["next_id"] if row else 1)

    def insert(self, document):
        with self._lock:
            doc_id = self._next_doc_id()
            self._conn.execute(
                """
                INSERT INTO tinydb_store (table_name, doc_id, data_json)
                VALUES (?, ?, ?)
                """,
                (self._table_name, doc_id, json.dumps(dict(document), ensure_ascii=True)),
            )
            self._conn.commit()
            return doc_id

    def insert_multiple(self, documents):
        with self._lock:
            next_doc_id = self._next_doc_id()
            for document in documents:
                self._conn.execute(
                    """
                    INSERT INTO tinydb_store (table_name, doc_id, data_json)
                    VALUES (?, ?, ?)
                    """,
                    (self._table_name, next_doc_id, json.dumps(dict(document), ensure_ascii=True)),
                )
                next_doc_id += 1
            self._conn.commit()

    def update(self, fields, condition=None, doc_ids=None):
        with self._lock:
            if doc_ids:
                target_ids = {int(item) for item in doc_ids}
                docs = [row for row in self._load_docs() if row.doc_id in target_ids]
            else:
                docs = [row for row in self._load_docs() if self._matches(condition, row)]

            for doc in docs:
                updated = dict(doc)
                updated.update(fields)
                self._conn.execute(
                    """
                    UPDATE tinydb_store
                    SET data_json = ?
                    WHERE table_name = ? AND doc_id = ?
                    """,
                    (json.dumps(updated, ensure_ascii=True), self._table_name, doc.doc_id),
                )
            self._conn.commit()

    def remove(self, condition):
        with self._lock:
            docs = [row for row in self._load_docs() if self._matches(condition, row)]
            for doc in docs:
                self._conn.execute(
                    "DELETE FROM tinydb_store WHERE table_name = ? AND doc_id = ?",
                    (self._table_name, doc.doc_id),
                )
            self._conn.commit()

    def truncate(self):
        with self._lock:
            self._conn.execute("DELETE FROM tinydb_store WHERE table_name = ?", (self._table_name,))
            self._conn.commit()


class SQLiteTinyDB:
    def __init__(self, path: str):
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._ensure_schema()
        self._lock = threading.RLock()

    def _ensure_schema(self):
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tinydb_store (
                table_name TEXT NOT NULL,
                doc_id INTEGER NOT NULL,
                data_json TEXT NOT NULL,
                PRIMARY KEY (table_name, doc_id)
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tinydb_store_table ON tinydb_store(table_name)"
        )
        self._conn.commit()

    def table(self, table_name: str):
        return SQLiteTable(self._conn, self._lock, table_name)

    def tables(self):
        cur = self._conn.execute("SELECT DISTINCT table_name FROM tinydb_store ORDER BY table_name")
        return {row["table_name"] for row in cur.fetchall()}

    def close(self):
        self._conn.close()


class LockedTinyDB:
    def __init__(self, path: str):
        path_obj = Path(path)
        if path_obj.parent and str(path_obj.parent) not in {"", "."}:
            path_obj.parent.mkdir(parents=True, exist_ok=True)
        self.db = SQLiteTinyDB(path)
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
                table_name: [dict(row) for row in self.db.table(table_name).all()]
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
        return self.base_dir / f"{safe_slug}.sqlite3"

    def _legacy_file_path(self, season_slug: str) -> Path:
        safe_slug = self._validate_slug(season_slug)
        return self.base_dir / f"{safe_slug}.json"

    def has_season(self, season_slug: str) -> bool:
        return self._file_path(season_slug).exists() or self._legacy_file_path(season_slug).exists()

    def _migrate_legacy_if_needed(self, safe_slug: str):
        sqlite_path = self._file_path(safe_slug)
        legacy_path = self._legacy_file_path(safe_slug)
        if sqlite_path.exists() or not legacy_path.exists():
            return
        migrate_legacy_json_to_sqlite(legacy_path, sqlite_path)

    def get_store(self, season_slug: str, create: bool = False) -> LockedTinyDB:
        safe_slug = self._validate_slug(season_slug)
        file_path = self._file_path(safe_slug)

        with self._lock:
            self._migrate_legacy_if_needed(safe_slug)

            if not file_path.exists() and not create:
                raise ValueError("Season database not found")

            key = str(file_path)
            if key not in self._stores:
                self._stores[key] = LockedTinyDB(key)
            return self._stores[key]

    def list_slugs(self):
        files = {
            file_path.stem: file_path
            for file_path in self.base_dir.glob("*.sqlite3")
        }
        for legacy_path in self.base_dir.glob("*.json"):
            files.setdefault(legacy_path.stem, legacy_path)

        sorted_files = sorted(
            files.values(),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        return [file_path.stem for file_path in sorted_files]
