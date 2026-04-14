# SCL Auction Dashboard

Mobile-first Flask + SQLite auction dashboard for Small Cricket League.

## Features implemented

- SQLite storage with process-wide thread-safe lock for all reads/writes.
- Roles:
  - Administrator: setup/control auction, create manager credentials, manage phases.
  - Manager: login and bid with quick action buttons.
  - Viewer: public live board without credentials.
- Public visibility of purse remaining and credits remaining.
- Auction rules enforced:
  - Team active composition is 4 including manager, so each team buys 3 players.
  - Team cap uses credits with manager-tier deduction from total 8 credits.
  - Manager purse by tier: Platinum 4000, Gold 4800, Silver 5500.
  - Phase A sequence support: Silver+Gold, break, Platinum.
  - Break phase supports manager trades and one-way transfers for already won players.
  - Phase B uses all unsold players at flat 200.
  - Incomplete teams cannot bid in Phase B.
  - Draft completion penalty auto-assigns players to reach 3 bought players and zeroes wallet.
- Live updates with Flask-SocketIO and fallback polling on viewer page.
- Published completed auction snapshots are available at `/<name>` after the admin publishes them.

## Run

1. Install dependencies:

   python -m pip install -r requirements.txt

2. Start app:

   python run.py

3. Open:

- Viewer live: /viewer/live
- Manager login: /manager/login
- Admin login: /admin/login

Default admin credentials:

- username: admin
- password: admin123

## Quick start flow

1. Login as admin.
2. Create managers from Admin Control Room.
3. Set phase and nominate players.
4. Managers place bids.
5. During break phase, managers can submit trades/transfers.
6. Close lot from admin when ready.
7. Move to Phase B and continue unsold pool purchases at 200.
8. Click Complete Draft + Penalties to auto-assign incomplete teams.
9. Publish the completed auction snapshot with a name such as `season-1` to expose it at `/<name>`.
