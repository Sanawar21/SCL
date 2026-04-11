const socket = io();

const bidForm = document.getElementById("bidForm");
const bidAmount = document.getElementById("bidAmount");
const bidMsg = document.getElementById("bidMsg");
const tradeForm = document.getElementById("tradeForm");
const tradeMsg = document.getElementById("tradeMsg");
const baseBidBtn = document.getElementById("baseBidBtn");
const flat200Btn = document.getElementById("flat200Btn");
const passBtn = document.getElementById("passBtn");

const plusButtons = document.querySelectorAll("[data-add]");
const initialStateNode = document.getElementById("initialState");
let latestState = initialStateNode ? JSON.parse(initialStateNode.textContent) : {};

function formatSpeciality(value) {
  return String(value || "-")
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function toInt(v) {
  return Number.parseInt(v, 10) || 0;
}

function cashTerms(tr) {
  const pay = toInt(tr.cash_from_initiator);
  const receive = toInt(tr.cash_from_target);
  const parts = [];
  if (pay > 0) parts.push(`Initiator->Target: ${pay}`);
  if (receive > 0) parts.push(`Target->Initiator: ${receive}`);
  return parts.length ? parts.join(" | ") : "No cash";
}

function wireTradeActions() {
  document.querySelectorAll(".trade-action").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const tradeId = btn.getAttribute("data-trade-id");
      const action = btn.getAttribute("data-action");
      const fd = new FormData();
      fd.append("trade_id", tradeId);
      fd.append("action", action);
      const res = await fetch("/auction/manager/trade/respond", { method: "POST", body: fd });
      const data = await res.json();
      tradeMsg.textContent = data.ok ? `Trade ${action}ed` : `Trade ${action} failed: ${data.error}`;
    });
  });
}

function renderTradeTables(managerState) {
  const incomingBody = document.getElementById("incomingTradesBody");
  const outgoingBody = document.getElementById("outgoingTradesBody");
  const trades = managerState.trade_requests || { incoming: [], outgoing: [] };

  if (incomingBody) {
    incomingBody.innerHTML = trades.incoming.length
      ? trades.incoming
          .map(
            (tr) => `<tr>
              <td>${tr.from_team_name}</td>
              <td>${tr.offered_player_name}</td>
              <td>${tr.requested_player_name || "-"}</td>
              <td>${cashTerms(tr)}</td>
              <td>
                <button class="btn trade-action" data-action="accept" data-trade-id="${tr.id}">Accept</button>
                <button class="btn btn-outline trade-action" data-action="reject" data-trade-id="${tr.id}">Reject</button>
              </td>
            </tr>`
          )
          .join("")
      : "<tr><td colspan=\"5\">No incoming requests</td></tr>";
  }

  if (outgoingBody) {
    outgoingBody.innerHTML = trades.outgoing.length
      ? trades.outgoing
          .map(
            (tr) => `<tr>
              <td>${tr.to_team_name}</td>
              <td>${tr.offered_player_name}</td>
              <td>${tr.requested_player_name || "-"}</td>
              <td>${cashTerms(tr)}</td>
              <td>${tr.status}</td>
            </tr>`
          )
          .join("")
      : "<tr><td colspan=\"5\">No outgoing requests</td></tr>";
  }

  wireTradeActions();
}

function renderRecentBids(managerState) {
  const bidsBody = document.querySelector("#recentBidsTable tbody");
  if (!bidsBody) {
    return;
  }

  bidsBody.innerHTML = (managerState.current_lot_bids || [])
    .slice()
    .map(
      (b) => `<tr><td>${b.ts_display || "-"}</td><td>${b.team_name || "-"}</td><td>${b.amount}</td></tr>`
    )
    .join("");
}

function refreshView(state) {
  latestState = state;
  document.getElementById("phaseBadge").textContent = state.phase;

  const current = state.current_player;
  document.getElementById("lotTitle").textContent = current
    ? `${current.name} (${current.tier}, ${formatSpeciality(current.speciality)})`
    : "No player nominated";
  document.getElementById("lotBase").textContent = current ? current.base_price : "-";
  document.getElementById("lotCurrent").textContent = current ? current.current_bid : 0;
  document.getElementById("lotBidder").textContent = current ? (current.current_bidder_team_name || "-") : "-";

  fetch("/auction/manager/state")
    .then((r) => r.json())
    .then((managerState) => {
      const team = managerState.my_team;
      document.getElementById("myPurse").textContent = team.purse_remaining;
      document.getElementById("myCredits").textContent = team.credits_remaining;
      document.getElementById("myActive").textContent = team.players.length;
      document.getElementById("myBench").textContent = team.bench.length;

      const tbody = document.querySelector("#budgetTable tbody");
      tbody.innerHTML = managerState.public_budget_board
        .map(
          (t) => `<tr><td>${t.team_name}</td><td>${t.purse_remaining}</td><td>${t.credits_remaining}</td><td>${t.active_count}</td><td>${t.bench_count}</td></tr>`
        )
        .join("");

      const playersBody = document.querySelector("#playersTable tbody");
      if (playersBody) {
        playersBody.innerHTML = managerState.players
          .map(
            (p) => `<tr><td>${p.name}</td><td>${p.tier}</td><td>${formatSpeciality(p.speciality)}</td><td>${p.status}</td><td>${p.current_bid}</td><td>${p.sold_to_team_name || "-"}</td><td>${p.sold_price}</td></tr>`
          )
          .join("");
      }

      renderRecentBids(managerState);

      renderTradeTables(managerState);
    });
}

async function placeBid(value) {
  const fd = new FormData();
  fd.append("amount", String(value));
  const res = await fetch("/auction/manager/bid", { method: "POST", body: fd });
  const data = await res.json();
  bidMsg.textContent = data.ok ? "Bid accepted" : `Bid failed: ${data.error}`;
}

if (bidForm) {
  bidForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    await placeBid(toInt(bidAmount.value));
  });
}

plusButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    const current = toInt(document.getElementById("lotCurrent").textContent);
    bidAmount.value = current + toInt(btn.getAttribute("data-add"));
  });
});

if (baseBidBtn) {
  baseBidBtn.addEventListener("click", () => {
    const base = toInt(document.getElementById("lotBase").textContent);
    bidAmount.value = base;
  });
}

if (flat200Btn) {
  flat200Btn.addEventListener("click", async () => {
    bidAmount.value = 200;
    await placeBid(200);
  });
}

if (passBtn) {
  passBtn.addEventListener("click", async (e) => {
    e.preventDefault();
    const res = await fetch("/auction/manager/pass", { method: "POST", body: new FormData() });
    const data = await res.json();
    bidMsg.textContent = data.ok ? "Pass recorded" : `Pass failed: ${data.error}`;
  });
}

if (tradeForm) {
  tradeForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const res = await fetch("/auction/manager/trade", { method: "POST", body: new FormData(tradeForm) });
    const data = await res.json();
    tradeMsg.textContent = data.ok ? "Trade processed" : `Trade failed: ${data.error}`;
  });
}

socket.on("state_update", (state) => {
  refreshView(state);
});

refreshView(latestState);
