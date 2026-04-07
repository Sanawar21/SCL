import secrets
from datetime import datetime

from tinydb import Query

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

    def _get_meta(self, db):
        meta_table = db.table("meta")
        meta = meta_table.get(doc_id=1)
        if not meta:
            meta = {
                "phase": PHASE_SETUP,
                "created_at": datetime.utcnow().isoformat(),
                "current_player_id": None,
            }
            meta_table.insert(meta)
            meta = meta_table.get(doc_id=1)
        return meta

    def bootstrap_defaults(self):
        with self.store.write() as db:
            meta = self._get_meta(db)
            player_table = db.table("players")
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
        with self.store.read() as db:
            meta = self._get_meta(db)
            teams = db.table("teams").all()
            teams_by_id = {t["id"]: t for t in teams}
            players = db.table("players").all()
            bids = db.table("bids").all()[-25:]
            current_player = None
            if meta.get("current_player_id"):
                current_player = db.table("players").get(Query().id == meta["current_player_id"])
                if current_player:
                    bidder_id = current_player.get("current_bidder_team_id")
                    bidder_team = teams_by_id.get(bidder_id) if bidder_id else None
                    current_player["current_bidder_team_name"] = bidder_team.get("name") if bidder_team else "-"
            return {
                "phase": meta["phase"],
                "current_player": current_player,
                "teams": teams,
                "players": players,
                "bids": bids,
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

    def nominate_next_player(self):
        with self.store.write() as db:
            meta = self._get_meta(db)
            phase = meta["phase"]
            Player = Query()
            players_table = db.table("players")

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
            db.table("meta").update({"current_player_id": player["id"]}, doc_ids=[1])
            return players_table.get(Player.id == player["id"])

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
                teams.update({"players": player_ids, "purse_remaining": 0}, Team.id == team["id"])

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

    def trade_players(self, from_team_id: str, to_team_id: str, offered_player_id: str, requested_player_id: str | None = None):
        Team = Query()
        with self.store.write() as db:
            phase = self._get_meta(db).get("phase")
            if phase != PHASE_A_BREAK:
                raise ValueError("Trades are allowed only during the Phase A break")

            teams_table = db.table("teams")
            players_table = db.table("players")
            trades_table = db.table("trades")

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
                raise ValueError("Trade violates 10-credit team limit")

            teams_table.update({"players": from_players, "credits_remaining": from_credits}, Team.id == from_team_id)
            teams_table.update({"players": to_players, "credits_remaining": to_credits}, Team.id == to_team_id)

            players_table.update({"sold_to": to_team_id}, Query().id == offered_player_id)
            if requested_player_id:
                players_table.update({"sold_to": from_team_id}, Query().id == requested_player_id)

            trades_table.insert(
                {
                    "ts": datetime.utcnow().isoformat(),
                    "from_team_id": from_team_id,
                    "to_team_id": to_team_id,
                    "offered_player_id": offered_player_id,
                    "requested_player_id": requested_player_id,
                }
            )

            return {
                "from_team_id": from_team_id,
                "to_team_id": to_team_id,
                "offered_player_id": offered_player_id,
                "requested_player_id": requested_player_id,
            }

    def get_team_by_username(self, username: str):
        with self.store.read() as db:
            user = db.table("users").get(lambda u: u.get("username") == username)
            if not user:
                return None
            return db.table("teams").get(Query().id == user.get("team_id"))
