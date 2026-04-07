const socket = io();

function renderState(state) {
  const phaseBadge = document.getElementById("phaseBadge");
  if (phaseBadge) {
    phaseBadge.textContent = state.phase;
  }

  const currentWrap = document.getElementById("currentPlayerWrap");
  if (currentWrap) {
    if (state.current_player) {
      const p = state.current_player;
      currentWrap.innerHTML = `<div class="lot"><h3>${p.name} (${p.tier})</h3><p>Base ${p.base_price} | Current ${p.current_bid}</p><p>Highest Bidder: ${p.current_bidder_team_name || "-"}</p></div>`;
    } else {
      currentWrap.innerHTML = "<p>No player currently nominated.</p>";
    }
  }

  const tbody = document.querySelector("#budgetTable tbody");
  if (tbody) {
    tbody.innerHTML = state.public_budget_board
      .map(
        (t) => `<tr><td>${t.team_name}</td><td>${t.purse_remaining}</td><td>${t.credits_remaining}</td><td>${t.active_count}</td><td>${t.bench_count}</td></tr>`
      )
      .join("");
  }
}

socket.on("state_update", (state) => {
  renderState(state);
});

setInterval(async () => {
  const res = await fetch("/api/state");
  renderState(await res.json());
}, 3000);
