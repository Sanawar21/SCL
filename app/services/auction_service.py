import secrets
import json
import sqlite3
from datetime import datetime

from app.db import Query

from app.rules import (
    PHASE_A_BREAK,
    PHASE_A_P,
    PHASE_A_SG,
    PHASE_B,
    PHASE_B_FLAT_PRICE,
    PHASE_COMPLETE,
    PHASE_SETUP,
    REQUIRED_ACTIVE_PLAYERS,
    TIER_BASE_PRICE,
    TIER_CREDIT_COST,
    TIER_STARTING_PURSE,
    TOTAL_CREDITS,
)


class AuctionService:
    def __init__(self, store):
        self.store = store
        self._db_path = getattr(getattr(store, "db", None), "path", None)

    def _connect(self):
        if not self._db_path:
            raise RuntimeError("Auction database path is not configured")
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _safe_json_dumps(payload):
        return json.dumps(payload or {}, ensure_ascii=True)

    @staticmethod
    def _safe_json_loads(raw_value):
        if not raw_value:
            return {}
        try:
            parsed = json.loads(raw_value)
        except Exception:  # noqa: BLE001
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _resolve_runtime_auction_row(self, conn: sqlite3.Connection):
        row = conn.execute(
            """
            SELECT id, season_id, name, status, source_path, metadata_json
            FROM auctions
            ORDER BY
                CASE status WHEN 'active' THEN 0 WHEN 'published' THEN 1 ELSE 2 END,
                COALESCE(updated_at, '') DESC,
                COALESCE(saved_at, '') DESC,
                id DESC
            LIMIT 1
            """
        ).fetchone()

        if row:
            return row

        season_id = "live"
        conn.execute(
            """
            INSERT OR IGNORE INTO seasons (id, slug, name, created_at, published_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                season_id,
                season_id,
                "Live Auction",
                datetime.utcnow().isoformat(),
                None,
                self._safe_json_dumps({"source": "runtime"}),
            ),
        )
        auction_id = f"auction::{season_id}::runtime"
        conn.execute(
            """
            INSERT INTO auctions
            (id, season_id, name, mode, source_path, status, phase, current_player_id, started_at, ended_at, created_at, updated_at, saved_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                auction_id,
                season_id,
                "Live Auction",
                "live",
                "runtime",
                "active",
                None,
                None,
                None,
                None,
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
                None,
                self._safe_json_dumps({"created_by": "auction_service"}),
            ),
        )
        conn.commit()
        return conn.execute(
            "SELECT id, season_id, name, status, source_path, metadata_json FROM auctions WHERE id = ?",
            (auction_id,),
        ).fetchone()

    def _sync_runtime_tables_to_normalized(self):
        try:
            runtime_tables = self.store.export_tables()
        except Exception:  # noqa: BLE001
            return

        meta = (runtime_tables.get("meta") or [{}])[0] if isinstance(runtime_tables, dict) else {}
        teams = list(runtime_tables.get("teams") or []) if isinstance(runtime_tables, dict) else []
        users = list(runtime_tables.get("users") or []) if isinstance(runtime_tables, dict) else []
        players = list(runtime_tables.get("players") or []) if isinstance(runtime_tables, dict) else []
        bids = list(runtime_tables.get("bids") or []) if isinstance(runtime_tables, dict) else []
        trade_requests = list(runtime_tables.get("trade_requests") or []) if isinstance(runtime_tables, dict) else []

        with self._connect() as conn:
            auction_row = self._resolve_runtime_auction_row(conn)
            auction_id = auction_row["id"]
            season_id = auction_row["season_id"]

            merged_metadata = self._safe_json_loads(auction_row["metadata_json"])
            merged_metadata["meta"] = {
                "phase": meta.get("phase"),
                "current_player_id": meta.get("current_player_id"),
                "nomination_history": list(meta.get("nomination_history") or []),
                "created_at": meta.get("created_at"),
            }

            conn.execute(
                """
                UPDATE auctions
                SET phase = ?,
                    current_player_id = ?,
                    updated_at = ?,
                    metadata_json = ?
                WHERE id = ?
                """,
                (
                    meta.get("phase"),
                    meta.get("current_player_id"),
                    datetime.utcnow().isoformat(),
                    self._safe_json_dumps(merged_metadata),
                    auction_id,
                ),
            )

            conn.execute("DELETE FROM auction_teams WHERE auction_id = ?", (auction_id,))
            conn.execute(
                """
                DELETE FROM users
                WHERE source_scope = 'runtime-auction'
                  AND (password_hash IS NULL OR LENGTH(TRIM(password_hash)) = 0)
                """
            )

            users_by_username = {
                (user.get("username") or "").strip(): user
                for user in users
                if (user.get("username") or "").strip()
            }

            for player in players:
                player_id = (player.get("id") or "").strip()
                if not player_id:
                    continue

                conn.execute(
                    """
                    INSERT INTO players
                    (id, canonical_name, display_name, speciality, tier, is_manager, manager_username, created_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        canonical_name = excluded.canonical_name,
                        display_name = excluded.display_name,
                        speciality = excluded.speciality,
                        tier = excluded.tier,
                        metadata_json = excluded.metadata_json
                    """,
                    (
                        player_id,
                        (player.get("name") or "").strip().lower() or None,
                        player.get("name"),
                        player.get("speciality"),
                        player.get("tier"),
                        0,
                        None,
                        datetime.utcnow().isoformat(),
                        self._safe_json_dumps(player),
                    ),
                )

            for team in teams:
                team_id = (team.get("id") or "").strip()
                if not team_id:
                    continue

                manager_username = (team.get("manager_username") or "").strip()
                manager_player_id = f"manager::{manager_username.lower()}" if manager_username else f"manager::{team_id}"
                manager_profile = users_by_username.get(manager_username, {})
                manager_tier = (team.get("manager_tier") or "silver").strip().lower()

                conn.execute(
                    """
                    INSERT INTO players
                    (id, canonical_name, display_name, speciality, tier, is_manager, manager_username, created_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        canonical_name = excluded.canonical_name,
                        display_name = excluded.display_name,
                        speciality = excluded.speciality,
                        tier = excluded.tier,
                        is_manager = 1,
                        manager_username = excluded.manager_username,
                        metadata_json = excluded.metadata_json
                    """,
                    (
                        manager_player_id,
                        ((manager_profile.get("display_name") or manager_username) or "").strip().lower() or None,
                        (manager_profile.get("display_name") or manager_username or team.get("name") or team_id),
                        manager_profile.get("speciality") or "ALL_ROUNDER",
                        manager_tier,
                        1,
                        manager_username or None,
                        datetime.utcnow().isoformat(),
                        self._safe_json_dumps({"team_id": team_id}),
                    ),
                )

                conn.execute(
                    """
                    INSERT INTO teams
                    (id, name, manager_player_id, manager_username, manager_tier, created_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name = excluded.name,
                        manager_player_id = excluded.manager_player_id,
                        manager_username = excluded.manager_username,
                        manager_tier = excluded.manager_tier,
                        metadata_json = excluded.metadata_json
                    """,
                    (
                        team_id,
                        team.get("name") or team_id,
                        manager_player_id,
                        manager_username or None,
                        team.get("manager_tier"),
                        datetime.utcnow().isoformat(),
                        self._safe_json_dumps(team),
                    ),
                )

                conn.execute(
                    """
                    INSERT INTO auction_teams
                    (id, auction_id, team_id, manager_player_id, purse_start, purse_remaining, credits_start, credits_remaining, entry_status, added_at, removed_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        purse_remaining = excluded.purse_remaining,
                        credits_remaining = excluded.credits_remaining,
                        metadata_json = excluded.metadata_json
                    """,
                    (
                        f"auction-team::{auction_id}::{team_id}",
                        auction_id,
                        team_id,
                        manager_player_id,
                        team.get("purse_remaining"),
                        team.get("purse_remaining"),
                        team.get("credits_remaining"),
                        team.get("credits_remaining"),
                        "active",
                        datetime.utcnow().isoformat(),
                        None,
                        self._safe_json_dumps(team),
                    ),
                )

                if manager_username:
                    user_row = users_by_username.get(manager_username, {})
                    existing_auth = conn.execute(
                        """
                        SELECT id, password_hash
                        FROM users
                        WHERE username = ?
                          AND password_hash IS NOT NULL
                          AND LENGTH(TRIM(password_hash)) > 0
                        ORDER BY id ASC
                        LIMIT 1
                        """,
                        (manager_username,),
                    ).fetchone()

                    if existing_auth:
                        conn.execute(
                            """
                            UPDATE users
                            SET role = ?,
                                display_name = ?,
                                speciality = ?,
                                team_id = ?,
                                source_scope = ?,
                                metadata_json = ?
                            WHERE id = ?
                            """,
                            (
                                "manager",
                                user_row.get("display_name") or manager_username,
                                user_row.get("speciality"),
                                team_id,
                                "runtime-auction",
                                self._safe_json_dumps(user_row),
                                int(existing_auth["id"]),
                            ),
                        )
                    else:
                        existing_user = conn.execute(
                            """
                            SELECT id
                            FROM users
                            WHERE username = ?
                              AND source_scope = 'runtime-auction'
                            ORDER BY id ASC
                            LIMIT 1
                            """,
                            (manager_username,),
                        ).fetchone()
                        if existing_user:
                            conn.execute(
                                """
                                UPDATE users
                                SET role = ?,
                                    display_name = ?,
                                    speciality = ?,
                                    team_id = ?,
                                    metadata_json = ?
                                WHERE id = ?
                                """,
                                (
                                    "manager",
                                    user_row.get("display_name") or manager_username,
                                    user_row.get("speciality"),
                                    team_id,
                                    self._safe_json_dumps(user_row),
                                    int(existing_user["id"]),
                                ),
                            )
                        else:
                            conn.execute(
                                """
                                INSERT INTO users
                                (username, role, display_name, speciality, team_id, password_hash, source_scope, metadata_json)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    manager_username,
                                    "manager",
                                    user_row.get("display_name") or manager_username,
                                    user_row.get("speciality"),
                                    team_id,
                                    None,
                                    "runtime-auction",
                                    self._safe_json_dumps(user_row),
                                ),
                            )

            conn.execute("DELETE FROM team_rosters WHERE auction_id = ?", (auction_id,))
            for team in teams:
                team_id = (team.get("id") or "").strip()
                if not team_id:
                    continue

                manager_username = (team.get("manager_username") or "").strip()
                manager_player_id = f"manager::{manager_username.lower()}" if manager_username else f"manager::{team_id}"

                conn.execute(
                    """
                    INSERT INTO team_rosters
                    (auction_id, season_id, team_id, player_id, roster_role, created_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        auction_id,
                        season_id,
                        team_id,
                        manager_player_id,
                        "manager",
                        datetime.utcnow().isoformat(),
                        self._safe_json_dumps({}),
                    ),
                )

                for player_id in team.get("players", []) or []:
                    if not player_id:
                        continue
                    conn.execute(
                        """
                        INSERT INTO team_rosters
                        (auction_id, season_id, team_id, player_id, roster_role, created_at, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            auction_id,
                            season_id,
                            team_id,
                            player_id,
                            "active",
                            datetime.utcnow().isoformat(),
                            self._safe_json_dumps({}),
                        ),
                    )

                for player_id in team.get("bench", []) or []:
                    if not player_id:
                        continue
                    conn.execute(
                        """
                        INSERT INTO team_rosters
                        (auction_id, season_id, team_id, player_id, roster_role, created_at, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            auction_id,
                            season_id,
                            team_id,
                            player_id,
                            "bench",
                            datetime.utcnow().isoformat(),
                            self._safe_json_dumps({}),
                        ),
                    )

            conn.execute("DELETE FROM auction_players WHERE auction_id = ?", (auction_id,))
            for player in players:
                player_id = (player.get("id") or "").strip()
                if not player_id:
                    continue
                conn.execute(
                    """
                    INSERT INTO auction_players
                    (id, auction_id, player_id, nomination_order, entry_status, opening_price, current_bid, sold_to_team_id, sold_price, phase_sold, added_at, removed_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"auction-player::{auction_id}::{player_id}",
                        auction_id,
                        player_id,
                        None,
                        player.get("status") or "unsold",
                        player.get("base_price"),
                        int(player.get("current_bid") or 0),
                        player.get("sold_to"),
                        int(player.get("sold_price") or 0),
                        player.get("phase_sold"),
                        datetime.utcnow().isoformat(),
                        None,
                        self._safe_json_dumps(player),
                    ),
                )

            conn.execute("DELETE FROM bids WHERE auction_id = ?", (auction_id,))
            for bid in bids:
                bid_id = (bid.get("id") or "").strip() or secrets.token_hex(8)
                conn.execute(
                    """
                    INSERT INTO bids
                    (id, auction_id, season_id, player_id, team_id, amount, phase, kind, ts, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        bid_id,
                        auction_id,
                        season_id,
                        bid.get("player_id"),
                        bid.get("team_id"),
                        int(bid.get("amount") or 0),
                        bid.get("phase"),
                        bid.get("kind"),
                        bid.get("ts"),
                        self._safe_json_dumps(bid),
                    ),
                )

            conn.execute("DELETE FROM trades WHERE auction_id = ?", (auction_id,))
            for trade in trade_requests:
                trade_id = (trade.get("id") or "").strip() or secrets.token_hex(8)
                conn.execute(
                    """
                    INSERT INTO trades
                    (id, auction_id, season_id, from_team_id, to_team_id, offered_player_id, requested_player_id, cash_delta, status, requested_by_team_id, responded_by_team_id, created_at, responded_at, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade_id,
                        auction_id,
                        season_id,
                        trade.get("from_team_id"),
                        trade.get("to_team_id"),
                        trade.get("offered_player_id"),
                        trade.get("requested_player_id"),
                        int(trade.get("cash_from_target", 0) or 0) - int(trade.get("cash_from_initiator", 0) or 0),
                        trade.get("status") or "pending",
                        trade.get("from_team_id"),
                        trade.get("responded_by_team_id"),
                        trade.get("created_at"),
                        trade.get("responded_at"),
                        self._safe_json_dumps(trade),
                    ),
                )

            conn.commit()

    def sync_to_normalized(self):
        self._sync_runtime_tables_to_normalized()

    def _build_state_from_data(self, meta, teams, users, players, bids, bid_limit=25):
        def format_bid_time(ts: str):
            try:
                dt = datetime.fromisoformat(ts)
                return f"{dt.strftime('%H:%M:%S')}.{int(dt.microsecond / 1000):03d}"
            except Exception:  # noqa: BLE001
                return ts

        teams_by_id = {t["id"]: t for t in teams}
        players_by_id = {p["id"]: p for p in players}
        users_by_team_id = {}
        for user in users:
            team_id = user.get("team_id")
            if team_id:
                users_by_team_id[team_id] = user

        all_bids = sorted(list(bids), key=lambda b: str(b.get("ts", "")), reverse=True)
        enriched_bids = []
        current_lot_bids = []
        current_player_id = meta.get("current_player_id")

        for bid in all_bids:
            bidder_team = teams_by_id.get(bid.get("team_id"))
            player = players_by_id.get(bid.get("player_id"))
            enriched_bid = {
                **bid,
                "bid_id": bid.get("id"),
                "team_name": bidder_team.get("name") if bidder_team else "-",
                "player_name": player.get("name") if player else "-",
                "ts_display": format_bid_time(str(bid.get("ts", ""))),
            }
            if bid_limit is None or len(enriched_bids) < bid_limit:
                enriched_bids.append(enriched_bid)
            if current_player_id and bid.get("player_id") == current_player_id:
                current_lot_bids.append(enriched_bid)

        current_player = None
        if current_player_id:
            current_player = players_by_id.get(current_player_id)
            if current_player:
                bidder_id = current_player.get("current_bidder_team_id")
                bidder_team = teams_by_id.get(bidder_id) if bidder_id else None
                current_player = {**current_player}
                current_player["current_bidder_team_name"] = bidder_team.get("name") if bidder_team else "-"

        incomplete_fill_needed = sum(
            max(0, REQUIRED_ACTIVE_PLAYERS - len(t.get("players", [])))
            for t in teams
            if len(t.get("players", [])) < REQUIRED_ACTIVE_PLAYERS
        )
        unsold_players = sum(1 for p in players if p.get("status") == "unsold")
        can_enter_phase_b = unsold_players > incomplete_fill_needed

        enriched_teams = []
        prefix_map = {"gold": "(G)", "silver": "(S)", "platinum": "(P)"}
        for team in teams:
            manager_user = users_by_team_id.get(team["id"])
            player_labels = [
                f"{prefix_map.get(players_by_id[pid].get('tier'), '')} {players_by_id[pid]['name']}".strip()
                for pid in team.get("players", [])
                if pid in players_by_id
            ]
            bench_labels = [
                f"{prefix_map.get(players_by_id[pid].get('tier'), '')} {players_by_id[pid]['name']}".strip()
                for pid in team.get("bench", [])
                if pid in players_by_id
            ]
            enriched_teams.append(
                {
                    **team,
                    "manager_name": (
                        f"{prefix_map.get(team.get('manager_tier'), '')} {manager_user.get('display_name', manager_user.get('username', '-'))}".strip()
                        if manager_user
                        else "-"
                    ),
                    "manager_speciality": manager_user.get("speciality", "-") if manager_user else "-",
                    "player_labels": player_labels,
                    "bench_labels": bench_labels,
                }
            )

        player_rows = [
            {
                **p,
                "sold_to_team_name": teams_by_id.get(p.get("sold_to"), {}).get("name", "-") if p.get("sold_to") else "-",
                "is_manager": bool(p.get("is_manager", False)),
                "selection_type": "player",
            }
            for p in players
        ]

        for team in teams:
            team_id = team.get("id")
            manager_username = (team.get("manager_username") or "").strip()
            if not team_id or not manager_username:
                continue

            manager_user = users_by_team_id.get(team_id, {})
            manager_tier = (team.get("manager_tier") or "silver").strip().lower()
            manager_id = f"manager::{manager_username.lower()}"

            if any((item.get("id") or "").strip() == manager_id for item in player_rows):
                continue

            player_rows.append(
                {
                    "id": manager_id,
                    "name": manager_user.get("display_name") or manager_username,
                    "tier": manager_tier,
                    "speciality": manager_user.get("speciality") or "ALL_ROUNDER",
                    "base_price": 0,
                    "status": "manager",
                    "sold_to": team_id,
                    "sold_to_team_name": team.get("name") or "-",
                    "sold_price": 0,
                    "phase_sold": None,
                    "credits": TIER_CREDIT_COST.get(manager_tier, 0),
                    "current_bid": 0,
                    "current_bidder_team_id": None,
                    "nominated_phase_a": False,
                    "is_manager": True,
                    "selection_type": "manager",
                }
            )

        player_rows.sort(
            key=lambda item: (
                0 if item.get("selection_type") == "player" else 1,
                (item.get("name") or "").lower(),
            )
        )

        return {
            "phase": meta.get("phase", PHASE_SETUP),
            "current_player": current_player,
            "teams": enriched_teams,
            "managers": [
                {
                    "username": m.get("username"),
                    "display_name": m.get("display_name", m.get("username", "")),
                    "speciality": m.get("speciality", "-"),
                    "team_id": m.get("team_id"),
                    "team_name": teams_by_id.get(m.get("team_id"), {}).get("name", "-"),
                }
                for m in users
                if m.get("role") == "manager"
            ],
            "players": player_rows,
            "bids": enriched_bids,
            "current_lot_bids": current_lot_bids,
            "phase_b_readiness": {
                "unsold_players": unsold_players,
                "incomplete_fill_needed": incomplete_fill_needed,
                "can_enter_phase_b": can_enter_phase_b,
            },
            "public_budget_board": [
                {
                    "team_name": t["name"],
                    "purse_remaining": t.get("purse_remaining"),
                    "credits_remaining": t.get("credits_remaining"),
                    "active_count": len(t.get("players", [])),
                    "bench_count": len(t.get("bench", [])),
                }
                for t in teams
            ],
        }

    def build_state_from_tables(self, tables, bid_limit=None):
        meta_rows = tables.get("meta", []) if isinstance(tables, dict) else []
        meta = dict(meta_rows[0]) if meta_rows else {"phase": PHASE_SETUP, "current_player_id": None}
        teams = list(tables.get("teams", [])) if isinstance(tables, dict) else []
        users = list(tables.get("users", [])) if isinstance(tables, dict) else []
        players = list(tables.get("players", [])) if isinstance(tables, dict) else []
        bids = list(tables.get("bids", [])) if isinstance(tables, dict) else []
        return self._build_state_from_data(meta, teams, users, players, bids, bid_limit=bid_limit)

    def _get_meta(self, db):
        meta_table = db.table("meta")
        meta = meta_table.get(doc_id=1)
        if not meta:
            meta = {
                "phase": PHASE_SETUP,
                "created_at": datetime.utcnow().isoformat(),
                "current_player_id": None,
                "nomination_history": [],
            }
            meta_table.insert(meta)
            meta = meta_table.get(doc_id=1)
        elif "nomination_history" not in meta:
            meta_table.update({"nomination_history": []}, doc_ids=[1])
            meta = meta_table.get(doc_id=1)
        return meta

    def bootstrap_defaults(self):
        with self.store.write() as db:
            meta = self._get_meta(db)
            player_table = db.table("players")
            bids_table = db.table("bids")
            if len(player_table) == 0:
                seed_players = [
                    ("Arjun", "silver"),
                    ("Rohit", "silver"),
                    ("Dev", "gold"),
                    ("Aman", "gold"),
                    ("Ishaan", "platinum"),
                    ("Karan", "platinum"),
                ]
                for name, tier in seed_players:
                    player_table.insert(
                        {
                            "id": secrets.token_hex(8),
                            "name": name,
                            "tier": tier,
                            "speciality": "ALL_ROUNDER",
                            "base_price": TIER_BASE_PRICE[tier],
                            "status": "unsold",
                            "sold_to": None,
                            "sold_price": 0,
                            "phase_sold": None,
                            "credits": TIER_CREDIT_COST[tier],
                            "current_bid": 0,
                            "current_bidder_team_id": None,
                            "nominated_phase_a": False,
                        }
                    )
            else:
                # Backfill older records so nomination order logic works for existing DBs.
                Player = Query()
                for p in player_table.all():
                    if "nominated_phase_a" not in p:
                        player_table.update({"nominated_phase_a": False}, Player.id == p["id"])
                    if "speciality" not in p:
                        player_table.update({"speciality": "ALL_ROUNDER"}, Player.id == p["id"])

            # Backfill older bid records with a stable id so admin can delete specific bids.
            for b in bids_table.all():
                if "id" not in b:
                    bids_table.update({"id": secrets.token_hex(8)}, doc_ids=[b.doc_id])
            db.table("meta").update(meta, doc_ids=[1])

    def setup_team_budgets(self):
        Team = Query()
        with self.store.write() as db:
            teams = db.table("teams")
            all_teams = teams.all()
            for team in all_teams:
                manager_tier = team.get("manager_tier", "silver")
                teams.update(
                    {
                        "purse_remaining": TIER_STARTING_PURSE[manager_tier],
                        "credits_remaining": TOTAL_CREDITS - TIER_CREDIT_COST[manager_tier],
                    },
                    Team.id == team["id"],
                )

    def _recalculate_team_credits(self, db, team):
        players_by_id = {p["id"]: p for p in db.table("players").all()}
        manager_tier = team.get("manager_tier", "silver")
        used = TIER_CREDIT_COST.get(manager_tier, 0)

        for pid in team.get("players", []):
            if pid in players_by_id:
                used += players_by_id[pid].get("credits", 0)
        for pid in team.get("bench", []):
            if pid in players_by_id:
                used += players_by_id[pid].get("credits", 0)

        return TOTAL_CREDITS - used

    def set_phase(self, phase: str):
        with self.store.write() as db:
            db.table("meta").update({"phase": phase}, doc_ids=[1])

    def get_state(self):
        self._sync_runtime_tables_to_normalized()
        with self.store.read() as db:
            meta = self._get_meta(db)
            teams = db.table("teams").all()
            users = db.table("users").all()
            players = db.table("players").all()
            bids = db.table("bids").all()
            return self._build_state_from_data(meta, teams, users, players, bids, bid_limit=25)

    def nominate_next_player(self, previous_player_id: str | None = None):
        with self.store.write() as db:
            meta = self._get_meta(db)
            phase = meta["phase"]
            Player = Query()
            players_table = db.table("players")
            history = list(meta.get("nomination_history", []))

            if previous_player_id:
                history.append(previous_player_id)

            if phase == PHASE_A_SG:
                # Phase A must nominate all Silver players first, then Gold players.
                player = players_table.get(
                    lambda p: p.get("status") == "unsold"
                    and p.get("tier") == "silver"
                    and not p.get("nominated_phase_a", False)
                )
                if not player:
                    player = players_table.get(
                        lambda p: p.get("status") == "unsold"
                        and p.get("tier") == "gold"
                        and not p.get("nominated_phase_a", False)
                    )
            elif phase == PHASE_A_P:
                player = players_table.get(
                    lambda p: p.get("status") == "unsold" and p.get("tier") == "platinum"
                )
            elif phase == PHASE_B:
                player = players_table.get(lambda p: p.get("status") == "unsold")
            else:
                player = None

            if not player:
                return None

            update_fields = {"current_bid": 0, "current_bidder_team_id": None}
            if phase == PHASE_A_SG:
                update_fields["nominated_phase_a"] = True

            players_table.update(update_fields, Player.id == player["id"])
            db.table("meta").update(
                {
                    "current_player_id": player["id"],
                    "nomination_history": history,
                },
                doc_ids=[1],
            )
            return players_table.get(Player.id == player["id"])

    def previous_player(self):
        Team = Query()
        Player = Query()
        Bid = Query()
        with self.store.write() as db:
            meta = self._get_meta(db)
            current_player_id = meta.get("current_player_id")
            history = list(meta.get("nomination_history", []))

            if not current_player_id:
                raise ValueError("No active player to step back from")
            if not history:
                raise ValueError("No previous player available")

            current_player = db.table("players").get(Player.id == current_player_id)
            if not current_player:
                raise ValueError("Current player not found")
            if current_player.get("current_bid", 0) > 0 or current_player.get("current_bidder_team_id"):
                raise ValueError("Cannot go to the previous player after bidding has started")

            previous_player_id = history.pop()
            previous_player = db.table("players").get(Player.id == previous_player_id)
            if not previous_player:
                raise ValueError("Previous player not found")

            teams_table = db.table("teams")
            bids_table = db.table("bids")

            # If previous player was sold by an accidental next-lot action, undo that sale and reopen lot.
            if previous_player.get("status") == "sold" and previous_player.get("sold_to"):
                sold_team = teams_table.get(Team.id == previous_player.get("sold_to"))
                if sold_team:
                    players_list = list(sold_team.get("players", []))
                    bench_list = list(sold_team.get("bench", []))
                    if previous_player_id in players_list:
                        players_list.remove(previous_player_id)
                    if previous_player_id in bench_list:
                        bench_list.remove(previous_player_id)

                    refund_price = int(previous_player.get("sold_price", 0) or 0)
                    refund_credits = int(previous_player.get("credits", 0) or 0)
                    teams_table.update(
                        {
                            "players": players_list,
                            "bench": bench_list,
                            "purse_remaining": int(sold_team.get("purse_remaining", 0) or 0) + refund_price,
                            "spent": max(0, int(sold_team.get("spent", 0) or 0) - refund_price),
                            "credits_remaining": int(sold_team.get("credits_remaining", 0) or 0) + refund_credits,
                        },
                        Team.id == sold_team["id"],
                    )

                db.table("players").update(
                    {
                        "status": "unsold",
                        "sold_to": None,
                        "sold_price": 0,
                        "phase_sold": None,
                    },
                    Player.id == previous_player_id,
                )

            # Restore top bid state for reopened lot, if bids existed earlier.
            previous_bid_rows = bids_table.search((Bid.player_id == previous_player_id) & (Bid.kind == "bid"))
            if previous_bid_rows:
                top = sorted(
                    previous_bid_rows,
                    key=lambda b: (int(b.get("amount", 0)), str(b.get("ts", ""))),
                    reverse=True,
                )[0]
                previous_bid_update = {
                    "current_bid": int(top.get("amount", 0)),
                    "current_bidder_team_id": top.get("team_id"),
                }
            else:
                previous_bid_update = {"current_bid": 0, "current_bidder_team_id": None}

            current_updates = {"current_bid": 0, "current_bidder_team_id": None}
            if meta.get("phase") == PHASE_A_SG:
                current_updates["nominated_phase_a"] = False
            db.table("players").update(current_updates, Player.id == current_player_id)

            previous_updates = previous_bid_update
            if meta.get("phase") == PHASE_A_SG:
                previous_updates["nominated_phase_a"] = True
            db.table("players").update(previous_updates, Player.id == previous_player_id)

            db.table("meta").update(
                {
                    "current_player_id": previous_player_id,
                    "nomination_history": history,
                },
                doc_ids=[1],
            )
            return db.table("players").get(Player.id == previous_player_id)

    def place_bid(self, team_id: str, amount: int):
        Team = Query()
        Player = Query()
        with self.store.write() as db:
            meta = self._get_meta(db)
            phase = meta["phase"]
            player_id = meta.get("current_player_id")
            if not player_id:
                raise ValueError("No active player nominated")

            players_table = db.table("players")
            player = players_table.get(Player.id == player_id)
            if not player or player.get("status") != "unsold":
                raise ValueError("Player is no longer available")

            teams_table = db.table("teams")
            team = teams_table.get(Team.id == team_id)
            if not team:
                raise ValueError("Invalid team")

            if phase == PHASE_B and len(team.get("players", [])) < REQUIRED_ACTIVE_PLAYERS:
                raise ValueError("Incomplete teams cannot participate in Phase B")

            if phase == PHASE_B:
                required_amount = PHASE_B_FLAT_PRICE
                if amount != required_amount:
                    raise ValueError("Phase B price is fixed at 200")
            else:
                required_amount = max(player.get("base_price", 0), player.get("current_bid", 0) + 50)

            if amount < required_amount:
                raise ValueError(f"Bid must be at least {required_amount}")

            if team.get("purse_remaining", 0) < amount:
                raise ValueError("Not enough purse")

            credits_cost = player.get("credits", 0)
            if team.get("credits_remaining", 0) < credits_cost:
                raise ValueError("Not enough credits")

            players_table.update(
                {"current_bid": amount, "current_bidder_team_id": team_id},
                Player.id == player_id,
            )
            db.table("bids").insert(
                {
                    "id": secrets.token_hex(8),
                    "ts": datetime.utcnow().isoformat(),
                    "team_id": team_id,
                    "player_id": player_id,
                    "amount": amount,
                    "phase": phase,
                    "kind": "bid",
                }
            )
            return players_table.get(Player.id == player_id)

    def pass_current(self, team_id: str):
        with self.store.write() as db:
            meta = self._get_meta(db)
            player_id = meta.get("current_player_id")
            if not player_id:
                raise ValueError("No active player")
            db.table("bids").insert(
                {
                    "id": secrets.token_hex(8),
                    "ts": datetime.utcnow().isoformat(),
                    "team_id": team_id,
                    "player_id": player_id,
                    "amount": 0,
                    "phase": meta.get("phase"),
                    "kind": "pass",
                }
            )
            return {"ok": True}

    def close_current_player(self):
        Team = Query()
        Player = Query()
        with self.store.write() as db:
            meta = self._get_meta(db)
            player_id = meta.get("current_player_id")
            if not player_id:
                raise ValueError("No active player")

            players = db.table("players")
            teams = db.table("teams")
            player = players.get(Player.id == player_id)
            if not player:
                raise ValueError("Invalid player")

            if not player.get("current_bidder_team_id"):
                players.update({"status": "unsold"}, Player.id == player_id)
                db.table("meta").update({"current_player_id": None}, doc_ids=[1])
                return {"sold": False, "reason": "No bid"}

            team = teams.get(Team.id == player["current_bidder_team_id"])
            if not team:
                raise ValueError("Bidder team not found")

            players_list = team.get("players", [])
            bench_list = team.get("bench", [])
            is_bench = db.table("meta").get(doc_id=1).get("phase") == PHASE_B and len(players_list) >= REQUIRED_ACTIVE_PLAYERS

            if is_bench:
                bench_list.append(player_id)
            else:
                players_list.append(player_id)

            teams.update(
                {
                    "players": players_list,
                    "bench": bench_list,
                    "purse_remaining": team.get("purse_remaining", 0) - player["current_bid"],
                    "spent": team.get("spent", 0) + player["current_bid"],
                    "credits_remaining": team.get("credits_remaining", 0) - player.get("credits", 0),
                },
                Team.id == team["id"],
            )

            players.update(
                {
                    "status": "sold",
                    "sold_to": team["id"],
                    "sold_price": player["current_bid"],
                    "phase_sold": db.table("meta").get(doc_id=1).get("phase"),
                },
                Player.id == player_id,
            )
            db.table("meta").update({"current_player_id": None}, doc_ids=[1])
            return {"sold": True, "team_name": team["name"], "price": player["current_bid"]}

    def delete_bid(self, bid_id: str):
        Bid = Query()
        Player = Query()
        with self.store.write() as db:
            meta = self._get_meta(db)
            current_player_id = meta.get("current_player_id")

            bids_table = db.table("bids")
            players_table = db.table("players")

            bid = bids_table.get(Bid.id == bid_id)
            if not bid:
                raise ValueError("Bid not found")

            if not current_player_id or bid.get("player_id") != current_player_id:
                raise ValueError("Only bids from the current lot can be deleted")

            player = players_table.get(Player.id == current_player_id)
            if not player or player.get("status") != "unsold":
                raise ValueError("Cannot delete bids for a closed lot")

            bids_table.remove(Bid.id == bid_id)

            remaining = bids_table.search((Bid.player_id == current_player_id) & (Bid.kind == "bid"))
            if not remaining:
                players_table.update(
                    {"current_bid": 0, "current_bidder_team_id": None},
                    Player.id == current_player_id,
                )
                return {"deleted": True, "current_bid": 0}

            top = sorted(
                remaining,
                key=lambda b: (int(b.get("amount", 0)), str(b.get("ts", ""))),
                reverse=True,
            )[0]
            players_table.update(
                {
                    "current_bid": int(top.get("amount", 0)),
                    "current_bidder_team_id": top.get("team_id"),
                },
                Player.id == current_player_id,
            )
            return {"deleted": True, "current_bid": int(top.get("amount", 0))}

    def complete_phase_b_with_penalties(self):
        Team = Query()
        Player = Query()
        with self.store.write() as db:
            teams = db.table("teams")
            players = db.table("players")
            unsold = [p for p in players.all() if p.get("status") == "unsold"]

            for team in teams.all():
                if len(team.get("players", [])) >= REQUIRED_ACTIVE_PLAYERS:
                    continue

                needed = REQUIRED_ACTIVE_PLAYERS - len(team.get("players", []))
                assign = unsold[:needed]
                unsold = unsold[needed:]

                player_ids = team.get("players", []) + [p["id"] for p in assign]
                updated_team = {**team, "players": player_ids}
                teams.update(
                    {
                        "players": player_ids,
                        "purse_remaining": 0,
                        "credits_remaining": self._recalculate_team_credits(db, updated_team),
                    },
                    Team.id == team["id"],
                )

                for p in assign:
                    players.update(
                        {
                            "status": "sold",
                            "sold_to": team["id"],
                            "sold_price": 0,
                            "phase_sold": PHASE_B,
                        },
                        Player.id == p["id"],
                    )

            db.table("meta").update({"phase": PHASE_COMPLETE, "current_player_id": None}, doc_ids=[1])

    def request_trade(
        self,
        from_team_id: str,
        to_team_id: str,
        offered_player_id: str,
        requested_player_id: str | None = None,
        cash_from_initiator: int = 0,
        cash_from_target: int = 0,
    ):
        Team = Query()
        with self.store.write() as db:
            phase = self._get_meta(db).get("phase")
            if phase != PHASE_A_BREAK:
                raise ValueError("Trades are allowed only during the Phase A break")

            teams_table = db.table("teams")
            players_table = db.table("players")
            trade_requests = db.table("trade_requests")

            from_team = teams_table.get(Team.id == from_team_id)
            to_team = teams_table.get(Team.id == to_team_id)
            if not from_team or not to_team:
                raise ValueError("Invalid teams for trade")

            from_players = list(from_team.get("players", []))
            to_players = list(to_team.get("players", []))

            if offered_player_id not in from_players:
                raise ValueError("You can only offer a player you own")

            if requested_player_id:
                if requested_player_id not in to_players:
                    raise ValueError("Requested player is not owned by target team")
            cash_from_initiator = max(0, int(cash_from_initiator or 0))
            cash_from_target = max(0, int(cash_from_target or 0))

            trade_id = secrets.token_hex(8)
            trade_requests.insert(
                {
                    "id": trade_id,
                    "status": "pending",
                    "created_at": datetime.utcnow().isoformat(),
                    "from_team_id": from_team_id,
                    "to_team_id": to_team_id,
                    "offered_player_id": offered_player_id,
                    "requested_player_id": requested_player_id,
                    "cash_from_initiator": cash_from_initiator,
                    "cash_from_target": cash_from_target,
                }
            )

            return {
                "id": trade_id,
                "status": "pending",
                "from_team_id": from_team_id,
                "to_team_id": to_team_id,
                "offered_player_id": offered_player_id,
                "requested_player_id": requested_player_id,
                "cash_from_initiator": cash_from_initiator,
                "cash_from_target": cash_from_target,
            }

    def respond_trade(self, trade_id: str, target_team_id: str, action: str):
        Team = Query()
        Trade = Query()
        Player = Query()
        with self.store.write() as db:
            phase = self._get_meta(db).get("phase")
            if phase != PHASE_A_BREAK:
                raise ValueError("Trade responses are allowed only during the Phase A break")

            teams_table = db.table("teams")
            players_table = db.table("players")
            trade_requests = db.table("trade_requests")

            trade = trade_requests.get(Trade.id == trade_id)
            if not trade:
                raise ValueError("Trade request not found")
            if trade.get("status") != "pending":
                raise ValueError("Trade request is already resolved")
            if trade.get("to_team_id") != target_team_id:
                raise ValueError("Only the target manager can respond to this trade")

            if action == "reject":
                trade_requests.update(
                    {
                        "status": "rejected",
                        "responded_at": datetime.utcnow().isoformat(),
                        "responded_by_team_id": target_team_id,
                    },
                    Trade.id == trade_id,
                )
                return {"id": trade_id, "status": "rejected"}

            if action != "accept":
                raise ValueError("Invalid action")

            from_team_id = trade["from_team_id"]
            to_team_id = trade["to_team_id"]
            offered_player_id = trade["offered_player_id"]
            requested_player_id = trade.get("requested_player_id")
            cash_from_initiator = int(trade.get("cash_from_initiator", 0))
            cash_from_target = int(trade.get("cash_from_target", 0))

            from_team = teams_table.get(Team.id == from_team_id)
            to_team = teams_table.get(Team.id == to_team_id)
            if not from_team or not to_team:
                raise ValueError("Teams no longer available")

            from_players = list(from_team.get("players", []))
            to_players = list(to_team.get("players", []))

            if offered_player_id not in from_players:
                raise ValueError("Offered player is no longer owned by initiator")

            if requested_player_id:
                if requested_player_id not in to_players:
                    raise ValueError("Requested player is no longer owned by target")
                from_players.remove(offered_player_id)
                to_players.remove(requested_player_id)
                from_players.append(requested_player_id)
                to_players.append(offered_player_id)
            else:
                from_players.remove(offered_player_id)
                to_players.append(offered_player_id)

            from_updated = {**from_team, "players": from_players}
            to_updated = {**to_team, "players": to_players}

            from_credits = self._recalculate_team_credits(db, from_updated)
            to_credits = self._recalculate_team_credits(db, to_updated)
            if from_credits < 0 or to_credits < 0:
                raise ValueError("Trade violates 8-credit team limit")

            from_purse = int(from_team.get("purse_remaining", 0))
            to_purse = int(to_team.get("purse_remaining", 0))
            from_purse_after = from_purse - cash_from_initiator + cash_from_target
            to_purse_after = to_purse - cash_from_target + cash_from_initiator
            if from_purse_after < 0 or to_purse_after < 0:
                raise ValueError("Trade cash transfer exceeds available purse")

            teams_table.update(
                {
                    "players": from_players,
                    "credits_remaining": from_credits,
                    "purse_remaining": from_purse_after,
                },
                Team.id == from_team_id,
            )
            teams_table.update(
                {
                    "players": to_players,
                    "credits_remaining": to_credits,
                    "purse_remaining": to_purse_after,
                },
                Team.id == to_team_id,
            )

            players_table.update({"sold_to": to_team_id}, Player.id == offered_player_id)
            if requested_player_id:
                players_table.update({"sold_to": from_team_id}, Player.id == requested_player_id)

            trade_requests.update(
                {
                    "status": "accepted",
                    "responded_at": datetime.utcnow().isoformat(),
                    "responded_by_team_id": target_team_id,
                },
                Trade.id == trade_id,
            )

            return {"id": trade_id, "status": "accepted"}

    def get_trade_requests_for_team(self, team_id: str):
        Trade = Query()
        with self.store.read() as db:
            requests = db.table("trade_requests").search(
                (Trade.from_team_id == team_id) | (Trade.to_team_id == team_id)
            )
            players_by_id = {p["id"]: p for p in db.table("players").all()}
            teams_by_id = {t["id"]: t for t in db.table("teams").all()}

            def enrich(item):
                offered = players_by_id.get(item.get("offered_player_id"), {})
                requested = players_by_id.get(item.get("requested_player_id"), {}) if item.get("requested_player_id") else None
                from_team = teams_by_id.get(item.get("from_team_id"), {})
                to_team = teams_by_id.get(item.get("to_team_id"), {})
                return {
                    **item,
                    "offered_player_name": offered.get("name", "Unknown"),
                    "requested_player_name": requested.get("name", "-") if requested else "-",
                    "from_team_name": from_team.get("name", "Unknown"),
                    "to_team_name": to_team.get("name", "Unknown"),
                }

            incoming = [enrich(r) for r in requests if r.get("to_team_id") == team_id and r.get("status") == "pending"]
            outgoing = [enrich(r) for r in requests if r.get("from_team_id") == team_id]
            return {"incoming": incoming, "outgoing": outgoing}

    def get_team_by_username(self, username: str):
        with self.store.read() as db:
            user = db.table("users").get(lambda u: u.get("username") == username)
            if not user:
                return None
            team = db.table("teams").get(Query().id == user.get("team_id"))
            if not team:
                return None
            return {
                **team,
                "manager_speciality": user.get("speciality", "-"),
            }
