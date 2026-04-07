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

function toInt(v) {
  return Number.parseInt(v, 10) || 0;
}

function refreshView(state) {
  latestState = state;
  document.getElementById("phaseBadge").textContent = state.phase;

  const current = state.current_player;
  document.getElementById("lotTitle").textContent = current
    ? `${current.name} (${current.tier})`
    : "No player nominated";
  document.getElementById("lotBase").textContent = current ? current.base_price : "-";
  document.getElementById("lotCurrent").textContent = current ? current.current_bid : 0;
  document.getElementById("lotBidder").textContent = current ? (current.current_bidder_team_name || "-") : "-";

  fetch("/manager/state")
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
    });
}

async function placeBid(value) {
  const fd = new FormData();
  fd.append("amount", String(value));
  const res = await fetch("/manager/bid", { method: "POST", body: fd });
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
  passBtn.addEventListener("click", async () => {
    const res = await fetch("/manager/pass", { method: "POST", body: new FormData() });
    const data = await res.json();
    bidMsg.textContent = data.ok ? "Pass recorded" : `Pass failed: ${data.error}`;
  });
}

if (tradeForm) {
  tradeForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const res = await fetch("/manager/trade", { method: "POST", body: new FormData(tradeForm) });
    const data = await res.json();
    tradeMsg.textContent = data.ok ? "Trade processed" : `Trade failed: ${data.error}`;
  });
}

socket.on("state_update", (state) => {
  refreshView(state);
});
