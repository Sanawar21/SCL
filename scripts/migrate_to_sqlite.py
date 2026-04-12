#!/usr/bin/env python3
"""Migrate SCL JSON stores in /data to a normalized SQLite3 database.

Usage:
    python scripts/migrate_to_sqlite.py
    python scripts/migrate_to_sqlite.py --data-dir data --db-path data/scl.sqlite3 --force
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_hash(value: str, length: int = 12) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:length]


def slug_from_name(name: str) -> str:
    safe = (name or "").strip().lower()
    out = []
    for ch in safe:
        if ch.isalnum() or ch in {"-", "_"}:
            out.append(ch)
        elif ch in {" ", "."}:
            out.append("-")
    slug = "".join(out).strip("-")
    return slug or "unknown"


def iter_rows(table_obj: Any):
    """Yield (row_key, row_dict) from TinyDB-like list or dict containers."""
    if isinstance(table_obj, dict):
        # TinyDB json storage is typically {"1": {...}, "2": {...}}
        for key in sorted(table_obj.keys(), key=lambda k: (len(str(k)), str(k))):
            row = table_obj.get(key)
            if isinstance(row, dict):
                yield str(key), row
    elif isinstance(table_obj, list):
        for idx, row in enumerate(table_obj, start=1):
            if isinstance(row, dict):
                yield str(idx), row


def safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=False)


@dataclass
class SourceDoc:
    rel_path: str
    payload: dict[str, Any]


class Migrator:
    def __init__(self, data_dir: Path, db_path: Path, force: bool = False):
        self.data_dir = data_dir
        self.db_path = db_path
        self.force = force

    def migrate(self) -> None:
        if not self.data_dir.exists() or not self.data_dir.is_dir():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

        if self.force and self.db_path.exists():
            self.db_path.unlink()

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        docs = self._load_json_docs()
        if not docs:
            raise RuntimeError(f"No JSON files found under {self.data_dir}")
        final_draft_bids_by_slug = self._load_final_draft_bids_by_slug()

        manager_speciality_by_username = self._build_manager_speciality_lookup(docs)
        admin_user = self._find_single_admin_user(docs)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            self._create_schema(conn)
            run_id = self._insert_import_run(conn)

            for doc in docs:
                self._persist_source_doc(conn, run_id, doc)

            self._insert_single_admin_user(conn, admin_user)

            for doc in docs:
                self._migrate_doc(
                    conn,
                    run_id,
                    doc,
                    manager_speciality_by_username,
                    final_draft_bids_by_slug,
                )

            self._finalize_run(conn, run_id)
            conn.commit()

            self._print_summary(conn, run_id)

    def _load_json_docs(self) -> list[SourceDoc]:
        docs: list[SourceDoc] = []
        for file_path in sorted(self.data_dir.rglob("*.json")):
            rel_path = file_path.relative_to(self.data_dir).as_posix()
            if rel_path.startswith("auction_snapshots/"):
                continue
            if rel_path == "auction_live_db.json":
                continue
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                docs.append(SourceDoc(rel_path=rel_path, payload=payload))
        return docs

    def _load_final_draft_bids_by_slug(self) -> dict[str, list[dict[str, Any]]]:
        bids_by_slug: dict[str, list[dict[str, Any]]] = {}
        snapshots_dir = self.data_dir / "auction_snapshots"
        if not snapshots_dir.exists():
            return bids_by_slug

        for file_path in sorted(snapshots_dir.glob("*-final-draft.json")):
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue

            if not isinstance(payload, dict):
                continue

            tables = self._extract_tables(payload)
            bid_table = tables.get("bids")
            if bid_table is None:
                continue

            slug = file_path.stem
            if slug.endswith("-final-draft"):
                slug = slug[: -len("-final-draft")]
            season_slug = slug_from_name(slug)

            bids: list[dict[str, Any]] = []
            for _, bid in iter_rows(bid_table):
                bids.append(dict(bid))
            if bids:
                bids_by_slug[season_slug] = bids

        return bids_by_slug

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS import_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_root TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS source_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                source_path TEXT NOT NULL,
                content_json TEXT NOT NULL,
                loaded_at TEXT NOT NULL,
                UNIQUE(run_id, source_path),
                FOREIGN KEY(run_id) REFERENCES import_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS source_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                source_path TEXT NOT NULL,
                source_table TEXT NOT NULL,
                source_row_key TEXT NOT NULL,
                row_json TEXT NOT NULL,
                loaded_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES import_runs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS seasons (
                id TEXT PRIMARY KEY,
                slug TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                season_year INTEGER,
                created_at TEXT NOT NULL,
                published_at TEXT,
                metadata_json TEXT
            );

            CREATE TABLE IF NOT EXISTS players (
                id TEXT PRIMARY KEY,
                canonical_name TEXT,
                display_name TEXT,
                speciality TEXT,
                tier TEXT,
                is_manager INTEGER NOT NULL DEFAULT 0,
                manager_username TEXT,
                created_at TEXT NOT NULL,
                metadata_json TEXT
            );

            CREATE TABLE IF NOT EXISTS teams (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                manager_player_id TEXT NOT NULL,
                manager_username TEXT,
                manager_tier TEXT,
                created_at TEXT NOT NULL,
                metadata_json TEXT,
                FOREIGN KEY(manager_player_id) REFERENCES players(id)
            );

            CREATE TABLE IF NOT EXISTS auctions (
                id TEXT PRIMARY KEY,
                season_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                status TEXT NOT NULL,
                phase TEXT,
                current_player_id TEXT,
                created_at TEXT NOT NULL,
                saved_at TEXT,
                metadata_json TEXT,
                FOREIGN KEY(season_id) REFERENCES seasons(id),
                FOREIGN KEY(current_player_id) REFERENCES players(id)
            );

            CREATE TABLE IF NOT EXISTS bids (
                id TEXT PRIMARY KEY,
                auction_id TEXT NOT NULL,
                season_id TEXT NOT NULL,
                player_id TEXT,
                team_id TEXT,
                amount INTEGER,
                phase TEXT,
                kind TEXT,
                ts TEXT,
                metadata_json TEXT,
                FOREIGN KEY(auction_id) REFERENCES auctions(id),
                FOREIGN KEY(season_id) REFERENCES seasons(id),
                FOREIGN KEY(player_id) REFERENCES players(id),
                FOREIGN KEY(team_id) REFERENCES teams(id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                auction_id TEXT,
                season_id TEXT,
                from_team_id TEXT,
                to_team_id TEXT,
                offered_player_id TEXT,
                requested_player_id TEXT,
                cash_delta INTEGER,
                status TEXT,
                requested_by_team_id TEXT,
                responded_by_team_id TEXT,
                created_at TEXT,
                responded_at TEXT,
                metadata_json TEXT,
                FOREIGN KEY(auction_id) REFERENCES auctions(id),
                FOREIGN KEY(season_id) REFERENCES seasons(id),
                FOREIGN KEY(from_team_id) REFERENCES teams(id),
                FOREIGN KEY(to_team_id) REFERENCES teams(id),
                FOREIGN KEY(offered_player_id) REFERENCES players(id),
                FOREIGN KEY(requested_player_id) REFERENCES players(id)
            );

            CREATE TABLE IF NOT EXISTS fantasy_teams (
                id TEXT PRIMARY KEY,
                season_id TEXT NOT NULL,
                entrant_name TEXT NOT NULL,
                entrant_key TEXT,
                total_credits INTEGER,
                team_signature TEXT,
                created_at TEXT,
                metadata_json TEXT,
                FOREIGN KEY(season_id) REFERENCES seasons(id)
            );

            CREATE TABLE IF NOT EXISTS fantasy_team_picks (
                fantasy_team_id TEXT NOT NULL,
                pick_index INTEGER NOT NULL,
                player_id TEXT,
                player_name TEXT,
                tier TEXT,
                credits INTEGER,
                metadata_json TEXT,
                PRIMARY KEY(fantasy_team_id, pick_index),
                FOREIGN KEY(fantasy_team_id) REFERENCES fantasy_teams(id) ON DELETE CASCADE,
                FOREIGN KEY(player_id) REFERENCES players(id)
            );

            CREATE TABLE IF NOT EXISTS team_rosters (
                auction_id TEXT NOT NULL,
                season_id TEXT NOT NULL,
                team_id TEXT NOT NULL,
                player_id TEXT NOT NULL,
                roster_role TEXT NOT NULL,
                created_at TEXT NOT NULL,
                metadata_json TEXT,
                PRIMARY KEY(auction_id, team_id, player_id, roster_role),
                FOREIGN KEY(auction_id) REFERENCES auctions(id),
                FOREIGN KEY(season_id) REFERENCES seasons(id),
                FOREIGN KEY(team_id) REFERENCES teams(id),
                FOREIGN KEY(player_id) REFERENCES players(id)
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                role TEXT,
                display_name TEXT,
                speciality TEXT,
                team_id TEXT,
                password_hash TEXT,
                source_scope TEXT NOT NULL,
                metadata_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_players_name ON players(canonical_name);
            CREATE INDEX IF NOT EXISTS idx_bids_auction ON bids(auction_id, ts);
            CREATE INDEX IF NOT EXISTS idx_bids_player ON bids(player_id);
            CREATE INDEX IF NOT EXISTS idx_fantasy_season ON fantasy_teams(season_id);
            CREATE INDEX IF NOT EXISTS idx_source_rows ON source_rows(run_id, source_path, source_table);
            """
        )

    def _insert_import_run(self, conn: sqlite3.Connection) -> int:
        cur = conn.execute(
            """
            INSERT INTO import_runs (source_root, started_at, notes)
            VALUES (?, ?, ?)
            """,
            (str(self.data_dir), utc_now_iso(), "JSON -> SQLite migration"),
        )
        return int(cur.lastrowid)

    def _finalize_run(self, conn: sqlite3.Connection, run_id: int) -> None:
        conn.execute(
            "UPDATE import_runs SET completed_at=? WHERE id=?",
            (utc_now_iso(), run_id),
        )

    def _persist_source_doc(self, conn: sqlite3.Connection, run_id: int, doc: SourceDoc) -> None:
        conn.execute(
            """
            INSERT INTO source_documents (run_id, source_path, content_json, loaded_at)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, doc.rel_path, safe_json(doc.payload), utc_now_iso()),
        )

        tables = self._extract_tables(doc.payload)
        for table_name, table_obj in tables.items():
            for row_key, row in iter_rows(table_obj):
                conn.execute(
                    """
                    INSERT INTO source_rows (run_id, source_path, source_table, source_row_key, row_json, loaded_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, doc.rel_path, str(table_name), str(row_key), safe_json(row), utc_now_iso()),
                )

    def _extract_tables(self, payload: dict[str, Any]) -> dict[str, Any]:
        tables = payload.get("tables")
        if isinstance(tables, dict):
            return tables
        return payload

    def _upsert_season(
        self,
        conn: sqlite3.Connection,
        season_slug: str,
        name: str | None,
        published_at: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        season_id = season_slug
        conn.execute(
            """
            INSERT INTO seasons (id, slug, name, created_at, published_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                published_at=COALESCE(excluded.published_at, seasons.published_at),
                metadata_json=excluded.metadata_json
            """,
            (
                season_id,
                season_slug,
                name or season_slug,
                utc_now_iso(),
                published_at,
                safe_json(metadata or {}),
            ),
        )
        return season_id

    def _upsert_player(
        self,
        conn: sqlite3.Connection,
        player_id: str,
        display_name: str | None,
        speciality: str | None,
        tier: str | None,
        is_manager: bool,
        manager_username: str | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        canonical_name = (display_name or "").strip().lower() or None
        conn.execute(
            """
            INSERT INTO players (
                id, canonical_name, display_name, speciality, tier, is_manager, manager_username, created_at, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                canonical_name=COALESCE(excluded.canonical_name, players.canonical_name),
                display_name=COALESCE(excluded.display_name, players.display_name),
                speciality=COALESCE(excluded.speciality, players.speciality),
                tier=COALESCE(excluded.tier, players.tier),
                is_manager=MAX(players.is_manager, excluded.is_manager),
                manager_username=COALESCE(excluded.manager_username, players.manager_username),
                metadata_json=COALESCE(excluded.metadata_json, players.metadata_json)
            """,
            (
                player_id,
                canonical_name,
                display_name,
                speciality,
                tier,
                1 if is_manager else 0,
                manager_username,
                utc_now_iso(),
                safe_json(metadata) if metadata is not None else None,
            ),
        )

    def _upsert_team(
        self,
        conn: sqlite3.Connection,
        team_id: str,
        name: str,
        manager_player_id: str,
        manager_username: str | None,
        manager_tier: str | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO teams (id, name, manager_player_id, manager_username, manager_tier, created_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                manager_player_id=excluded.manager_player_id,
                manager_username=COALESCE(excluded.manager_username, teams.manager_username),
                manager_tier=COALESCE(excluded.manager_tier, teams.manager_tier),
                metadata_json=excluded.metadata_json
            """,
            (
                team_id,
                name,
                manager_player_id,
                manager_username,
                manager_tier,
                utc_now_iso(),
                safe_json(metadata or {}),
            ),
        )

    def _upsert_user(self, conn: sqlite3.Connection, source_scope: str, row: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO users (
                username, role, display_name, speciality, team_id, password_hash, source_scope, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("username"),
                row.get("role"),
                row.get("display_name"),
                row.get("speciality"),
                row.get("team_id"),
                row.get("password_hash"),
                source_scope,
                safe_json(row),
            ),
        )

    def _find_single_admin_user(self, docs: list[SourceDoc]) -> dict[str, Any] | None:
        best_admin: dict[str, Any] | None = None
        for doc in docs:
            tables = self._extract_tables(doc.payload)
            for table_name in ("auth_users", "users"):
                table_obj = tables.get(table_name)
                if table_obj is None:
                    continue
                for _, row in iter_rows(table_obj):
                    role = str(row.get("role") or "").strip().lower()
                    if role != "admin":
                        continue
                    if best_admin is None:
                        best_admin = row
                        continue
                    # Prefer admin entries that contain a password hash.
                    if not best_admin.get("password_hash") and row.get("password_hash"):
                        best_admin = row
        return best_admin

    def _insert_single_admin_user(self, conn: sqlite3.Connection, admin_user: dict[str, Any] | None) -> None:
        if not admin_user:
            return
        self._upsert_user(conn, source_scope="singleton-admin", row=admin_user)

    def _build_manager_speciality_lookup(self, docs: list[SourceDoc]) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for doc in docs:
            tables = self._extract_tables(doc.payload)
            user_table = tables.get("users")
            if user_table is None:
                continue
            for _, user_row in iter_rows(user_table):
                role = str(user_row.get("role") or "").strip().lower()
                if role != "manager":
                    continue
                username = str(user_row.get("username") or "").strip().lower()
                speciality = str(user_row.get("speciality") or "").strip().upper()
                if username and speciality:
                    lookup[username] = speciality
        return lookup

    def _migrate_doc(
        self,
        conn: sqlite3.Connection,
        run_id: int,
        doc: SourceDoc,
        manager_speciality_by_username: dict[str, str],
        final_draft_bids_by_slug: dict[str, list[dict[str, Any]]],
    ) -> None:
        rel_path = doc.rel_path
        payload = doc.payload
        tables = self._extract_tables(payload)

        # If this file has no auction-like tables, it is still preserved in source_documents/source_rows.
        if not any(name in tables for name in ("players", "teams", "bids", "meta", "season_meta", "fantasy_entries")):
            return

        season_slug, season_name, published_at = self._derive_season_info(rel_path, payload, tables)
        season_id = self._upsert_season(
            conn,
            season_slug=season_slug,
            name=season_name,
            published_at=published_at,
            metadata={"source_path": rel_path},
        )

        auction_id = self._derive_auction_id(rel_path, season_slug)
        meta_row = self._first_row(tables.get("meta"))
        phase = (meta_row or {}).get("phase")
        current_player_id = (meta_row or {}).get("current_player_id")
        auction_status = self._derive_auction_status(rel_path=rel_path, payload=payload, tables=tables)

        conn.execute(
            """
            INSERT INTO auctions (id, season_id, source_path, status, phase, current_player_id, created_at, saved_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status=excluded.status,
                phase=excluded.phase,
                current_player_id=excluded.current_player_id,
                saved_at=COALESCE(excluded.saved_at, auctions.saved_at),
                metadata_json=excluded.metadata_json
            """,
            (
                auction_id,
                season_id,
                rel_path,
                auction_status,
                phase,
                current_player_id,
                utc_now_iso(),
                payload.get("saved_at"),
                safe_json({"meta": meta_row} if meta_row else {}),
            ),
        )

        users_by_username: dict[str, dict[str, Any]] = {}
        user_table = tables.get("users")
        if user_table is not None:
            for _, user_row in iter_rows(user_table):
                username = (user_row.get("username") or "").strip()
                if username:
                    users_by_username[username.lower()] = user_row

        player_table = tables.get("players")
        if player_table is not None:
            for _, player in iter_rows(player_table):
                pid = (player.get("id") or "").strip()
                if not pid:
                    continue
                self._upsert_player(
                    conn,
                    player_id=pid,
                    display_name=player.get("name"),
                    speciality=player.get("speciality"),
                    tier=player.get("tier"),
                    is_manager=False,
                    manager_username=None,
                    metadata=player,
                )

        team_table = tables.get("teams")
        if team_table is not None:
            for _, team in iter_rows(team_table):
                team_id = (team.get("id") or "").strip()
                if not team_id:
                    continue

                manager_username = (team.get("manager_username") or "").strip() or None
                manager_tier = (team.get("manager_tier") or "").strip() or None
                manager_player_id = f"manager::{(manager_username or f'team-{team_id}').lower()}"
                manager_user = users_by_username.get((manager_username or "").lower(), {})
                manager_display_name = (
                    (manager_user.get("display_name") or "").strip()
                    or manager_username
                    or f"Manager {team_id[:6]}"
                )
                manager_speciality = (
                    manager_speciality_by_username.get((manager_username or "").lower())
                    or (manager_user.get("speciality") or "").strip().upper()
                    or "ALL_ROUNDER"
                )

                # Enforce "manager is a player first" by materializing every manager as a player row.
                self._upsert_player(
                    conn,
                    player_id=manager_player_id,
                    display_name=manager_display_name,
                    speciality=manager_speciality,
                    tier=manager_tier,
                    is_manager=True,
                    manager_username=manager_username,
                    metadata={"source_team_id": team_id, "source_path": rel_path},
                )

                self._upsert_team(
                    conn,
                    team_id=team_id,
                    name=team.get("name") or team_id,
                    manager_player_id=manager_player_id,
                    manager_username=manager_username,
                    manager_tier=manager_tier,
                    metadata=team,
                )

                # Persist roster links (active, bench, manager).
                for pid in team.get("players", []) or []:
                    if not pid:
                        continue
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO team_rosters
                        (auction_id, season_id, team_id, player_id, roster_role, created_at, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            auction_id,
                            season_id,
                            team_id,
                            pid,
                            "active",
                            utc_now_iso(),
                            safe_json({"source_path": rel_path}),
                        ),
                    )
                for pid in team.get("bench", []) or []:
                    if not pid:
                        continue
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO team_rosters
                        (auction_id, season_id, team_id, player_id, roster_role, created_at, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            auction_id,
                            season_id,
                            team_id,
                            pid,
                            "bench",
                            utc_now_iso(),
                            safe_json({"source_path": rel_path}),
                        ),
                    )

                conn.execute(
                    """
                    INSERT OR IGNORE INTO team_rosters
                    (auction_id, season_id, team_id, player_id, roster_role, created_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        auction_id,
                        season_id,
                        team_id,
                        manager_player_id,
                        "manager",
                        utc_now_iso(),
                        safe_json({"manager_username": manager_username}),
                    ),
                )

        bids_to_import: list[tuple[str, dict[str, Any]]] = []
        bid_table = tables.get("bids")
        if bid_table is not None:
            for row_key, bid in iter_rows(bid_table):
                bids_to_import.append((str(row_key), bid))
        elif rel_path.startswith("season_dbs/"):
            for idx, bid in enumerate(final_draft_bids_by_slug.get(season_slug, []), start=1):
                bids_to_import.append((str(idx), bid))

        if bids_to_import:
            for row_key, bid in bids_to_import:
                bid_id = (bid.get("id") or f"bid::{auction_id}::{row_key}")
                conn.execute(
                    """
                    INSERT OR REPLACE INTO bids
                    (id, auction_id, season_id, player_id, team_id, amount, phase, kind, ts, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(bid_id),
                        auction_id,
                        season_id,
                        bid.get("player_id"),
                        bid.get("team_id"),
                        bid.get("amount"),
                        bid.get("phase"),
                        bid.get("kind"),
                        bid.get("ts"),
                        safe_json(bid),
                    ),
                )

        trade_table = tables.get("trade_requests")
        if trade_table is not None:
            for row_key, trade in iter_rows(trade_table):
                trade_id = str(trade.get("id") or f"trade::{auction_id}::{row_key}")
                conn.execute(
                    """
                    INSERT OR REPLACE INTO trades
                    (
                        id, auction_id, season_id, from_team_id, to_team_id, offered_player_id,
                        requested_player_id, cash_delta, status, requested_by_team_id,
                        responded_by_team_id, created_at, responded_at, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade_id,
                        auction_id,
                        season_id,
                        trade.get("from_team_id"),
                        trade.get("to_team_id"),
                        trade.get("offered_player_id"),
                        trade.get("requested_player_id"),
                        trade.get("cash_delta"),
                        trade.get("status"),
                        trade.get("from_team_id"),
                        trade.get("responded_by_team_id"),
                        trade.get("created_at"),
                        trade.get("responded_at"),
                        safe_json(trade),
                    ),
                )

        fantasy_table = tables.get("fantasy_entries")
        if fantasy_table is not None:
            for _, entry in iter_rows(fantasy_table):
                entry_id = str(entry.get("id") or f"ft::{auction_id}::{stable_hash(safe_json(entry))}")
                entry_season_slug = slug_from_name(entry.get("season_slug") or season_slug)
                entry_season_id = self._upsert_season(
                    conn,
                    season_slug=entry_season_slug,
                    name=entry.get("season_slug") or season_name,
                    published_at=published_at,
                    metadata={"source_path": rel_path, "derived_from": "fantasy_entry"},
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO fantasy_teams
                    (id, season_id, entrant_name, entrant_key, total_credits, team_signature, created_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry_id,
                        entry_season_id,
                        entry.get("entrant_name") or "Unknown",
                        entry.get("entrant_key"),
                        entry.get("total_credits"),
                        entry.get("team_signature"),
                        entry.get("created_at"),
                        safe_json(entry),
                    ),
                )

                picks = entry.get("picks") or []
                for index, pick in enumerate(picks, start=1):
                    pick_player_id = pick.get("player_id")
                    if isinstance(pick_player_id, str) and pick_player_id.startswith("manager::"):
                        manager_username = pick_player_id.replace("manager::", "", 1)
                        manager_speciality = (
                            manager_speciality_by_username.get(manager_username.lower())
                            or "ALL_ROUNDER"
                        )
                        self._upsert_player(
                            conn,
                            player_id=pick_player_id,
                            display_name=pick.get("player_name") or manager_username,
                            speciality=manager_speciality,
                            tier=pick.get("tier"),
                            is_manager=True,
                            manager_username=manager_username,
                            metadata={"source": "fantasy_pick", "source_path": rel_path},
                        )

                    conn.execute(
                        """
                        INSERT OR REPLACE INTO fantasy_team_picks
                        (fantasy_team_id, pick_index, player_id, player_name, tier, credits, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            entry_id,
                            index,
                            pick_player_id,
                            pick.get("player_name"),
                            pick.get("tier"),
                            pick.get("credits"),
                            safe_json(pick),
                        ),
                    )

    def _derive_season_info(
        self,
        rel_path: str,
        payload: dict[str, Any],
        tables: dict[str, Any],
    ) -> tuple[str, str, str | None]:
        season_meta = self._first_row(tables.get("season_meta")) or {}

        if season_meta.get("slug"):
            slug = slug_from_name(str(season_meta.get("slug")))
            name = str(season_meta.get("name") or slug)
            published_at = season_meta.get("published_at")
            return slug, name, published_at

        if payload.get("slug"):
            slug = slug_from_name(str(payload.get("slug")))
            name = str(payload.get("session_name") or slug)
            published_at = payload.get("saved_at")
            return slug, name, published_at

        base = Path(rel_path).stem
        if rel_path.startswith("season_dbs/"):
            slug = slug_from_name(base)
            return slug, slug.replace("-", " ").title(), None

        if rel_path.startswith("auction_live_db"):
            return "live", "Live Auction", None

        slug = slug_from_name(base)
        return slug, slug.replace("-", " ").title(), payload.get("saved_at")

    @staticmethod
    def _derive_auction_id(rel_path: str, season_slug: str) -> str:
        return f"auction::{season_slug}::{stable_hash(rel_path)}"

    @staticmethod
    def _derive_auction_status(rel_path: str, payload: dict[str, Any], tables: dict[str, Any]) -> str:
        season_meta = Migrator._first_row(tables.get("season_meta")) or {}
        if rel_path.startswith("season_dbs/"):
            return "published"
        if bool(season_meta.get("published")):
            return "published"
        if bool(payload.get("published")):
            return "published"
        return "active"

    @staticmethod
    def _first_row(table_obj: Any) -> dict[str, Any] | None:
        for _, row in iter_rows(table_obj):
            return row
        return None

    def _print_summary(self, conn: sqlite3.Connection, run_id: int) -> None:
        table_names = [
            "source_documents",
            "source_rows",
            "seasons",
            "players",
            "teams",
            "auctions",
            "bids",
            "trades",
            "fantasy_teams",
            "fantasy_team_picks",
            "team_rosters",
            "users",
        ]
        print("Migration complete")
        print(f"Run id: {run_id}")
        print(f"Database: {self.db_path}")
        for table_name in table_names:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"- {table_name}: {count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate /data JSON files into SQLite")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Path to input data directory (default: data)",
    )
    parser.add_argument(
        "--db-path",
        default="data/scl.sqlite3",
        help="Path for output sqlite db (default: data/scl.sqlite3)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing db-path before migration",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir).resolve()
    db_path = Path(args.db_path).resolve()
    migrator = Migrator(data_dir=data_dir, db_path=db_path, force=bool(args.force))
    migrator.migrate()


if __name__ == "__main__":
    main()
