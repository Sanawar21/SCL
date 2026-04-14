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
from app.rules import PHASE_A_SG
from app.services.auction_service import AuctionService


class TeamParticipationTests(unittest.TestCase):
    def test_phase_b_readiness_ignores_excluded_teams(self):
        service = AuctionService(store=None)
        tables = {
            "meta": [{"phase": PHASE_A_SG, "current_player_id": None, "nomination_history": []}],
            "teams": [
                {"id": "t1", "name": "Active Team", "players": [], "bench": [], "manager_tier": "silver", "is_active": True},
                {"id": "t2", "name": "Excluded Team", "players": [], "bench": [], "manager_tier": "silver", "is_active": False},
            ],
            "users": [],
            "players": [
                {"id": "p1", "name": "P1", "tier": "silver", "status": "unsold", "credits": 1, "speciality": "ALL_ROUNDER"},
                {"id": "p2", "name": "P2", "tier": "silver", "status": "unsold", "credits": 1, "speciality": "ALL_ROUNDER"},
                {"id": "p3", "name": "P3", "tier": "silver", "status": "unsold", "credits": 1, "speciality": "ALL_ROUNDER"},
                {"id": "p4", "name": "P4", "tier": "silver", "status": "unsold", "credits": 1, "speciality": "ALL_ROUNDER"},
            ],
            "bids": [],
        }

        state = service.build_state_from_tables(tables)
        readiness = state["phase_b_readiness"]
        self.assertEqual(readiness["incomplete_fill_needed"], 3)

    def test_place_bid_rejects_excluded_team(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "auction.json"
            store = LockedTinyDB(str(db_path))
            service = AuctionService(store)

            with store.write() as db:
                db.table("meta").insert(
                    {
                        "phase": PHASE_A_SG,
                        "created_at": "2026-04-14T00:00:00",
                        "current_player_id": "p1",
                        "nomination_history": [],
                    }
                )
                db.table("players").insert(
                    {
                        "id": "p1",
                        "name": "Player One",
                        "tier": "silver",
                        "base_price": 400,
                        "status": "unsold",
                        "sold_to": None,
                        "sold_price": 0,
                        "phase_sold": None,
                        "credits": 1,
                        "current_bid": 0,
                        "current_bidder_team_id": None,
                        "nominated_phase_a": True,
                        "speciality": "BATTER",
                    }
                )
                db.table("teams").insert(
                    {
                        "id": "t-excluded",
                        "name": "Excluded Team",
                        "manager_tier": "silver",
                        "purse_remaining": 2000,
                        "credits_remaining": 8,
                        "players": [],
                        "bench": [],
                        "is_active": False,
                    }
                )

            with self.assertRaises(ValueError):
                service.place_bid("t-excluded", 400)

            store.db.close()


if __name__ == "__main__":
    unittest.main()
