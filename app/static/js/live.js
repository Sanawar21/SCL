const socket = io();

function formatSpeciality(value) {
  return String(value || "-")
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function renderState(state) {
  const phaseBadge = document.getElementById("phaseBadge");
  if (phaseBadge) {
    phaseBadge.textContent = state.phase;
  }

  const currentWrap = document.getElementById("currentPlayerWrap");
  if (currentWrap) {
    if (state.current_player) {
      const p = state.current_player;
      currentWrap.innerHTML = `
        <div class="lot-meta">
          <div class="lot"><h3>${p.name} (${p.tier}, ${formatSpeciality(p.speciality)})</h3><p>Base ${p.base_price} | Current ${p.current_bid}</p><p>Highest Bidder: ${p.current_bidder_team_name || "-"}</p></div>
        </div>
        <div class="table-wrap lot-bids-wrap">
          <table id="viewerCurrentBidsTable">
            <thead><tr><th>Time</th><th>Team</th><th>Bid</th></tr></thead>
            <tbody>
              ${(state.current_lot_bids || [])
                .slice()
                .map((b) => `<tr><td>${b.ts_display || "-"}</td><td>${b.team_name || "-"}</td><td>${b.amount}</td></tr>`)
                .join("")}
            </tbody>
          </table>
        </div>`;
    } else {
      currentWrap.innerHTML = "<div class=\"lot-meta\"><p>No player currently nominated.</p></div><div class=\"table-wrap lot-bids-wrap\"><table id=\"viewerCurrentBidsTable\"><thead><tr><th>Time</th><th>Team</th><th>Bid</th></tr></thead><tbody></tbody></table></div>";
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

  const teamsBody = document.querySelector("#teamsTable tbody");
  if (teamsBody) {
    teamsBody.innerHTML = state.teams
      .map(
        (t) => `<tr><td>${t.name}</td><td>${t.manager_name || "-"}</td><td>${formatSpeciality(t.manager_speciality)}</td><td>${(t.player_labels || []).join(", ") || "-"}</td><td>${(t.bench_labels || []).join(", ") || "-"}</td></tr>`
      )
      .join("");
  }

  const playersBody = document.querySelector("#playersTable tbody");
  if (playersBody) {
    playersBody.innerHTML = state.players
      .map(
        (p) => `<tr><td>${p.name}</td><td>${p.tier}</td><td>${formatSpeciality(p.speciality)}</td><td>${p.status}</td><td>${p.current_bid}</td><td>${p.sold_to_team_name || "-"}</td><td>${p.sold_price}</td></tr>`
      )
      .join("");
  }
}

socket.on("state_update", (state) => {
  renderState(state);
});

setInterval(async () => {
  const res = await fetch("/auction/api/state");
  renderState(await res.json());
}, 3000);
