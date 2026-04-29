const socket = io();

const managerForm = document.getElementById("managerForm");
const credResult = document.getElementById("credResult");
const phaseForm = document.getElementById("phaseForm");
const nominateBtn = document.getElementById("nominateBtn");
const previousBtn = document.getElementById("previousBtn");
const closeBtn = document.getElementById("closeBtn");
const completeBtn = document.getElementById("completeBtn");
const saveSessionBtn = document.getElementById("saveSessionBtn");
const loadSessionBtn = document.getElementById("loadSessionBtn");
const sessionNameInput = document.getElementById("sessionNameInput");
const overwriteSessionCheckbox = document.getElementById("overwriteSessionCheckbox");
const sessionSelect = document.getElementById("sessionSelect");
const sessionStatus = document.getElementById("sessionStatus");
const publishSessionBtn = document.getElementById("publishSessionBtn");
const publishNameInput = document.getElementById("publishNameInput");
const publishSuffixInput = document.getElementById("publishSuffixInput");
const publishStatus = document.getElementById("publishStatus");
const scorerForm = document.getElementById("scorerForm");
const scorerStatus = document.getElementById("scorerStatus");
const scorerMatchForm = document.getElementById("scorerMatchForm");
const scorerMatchStatus = document.getElementById("scorerMatchStatus");
const scorerImportForm = document.getElementById("scorerImportForm");
const scorerImportStatus = document.getElementById("scorerImportStatus");
const scorerImportSummary = document.getElementById("scorerImportSummary");
const financeAdjustForm = document.getElementById("financeAdjustForm");
const financeTransferForm = document.getElementById("financeTransferForm");
const financePlayerTransferForm = document.getElementById("financePlayerTransferForm");
const financeStatus = document.getElementById("financeStatus");
const seasonFinanceTable = document.getElementById("seasonFinanceTable");
const adminDashboard = document.getElementById("adminDashboard");
const isSetupPhase = adminDashboard?.getAttribute("data-is-setup") === "true";
const financeSelectedSeason = adminDashboard?.getAttribute("data-finance-selected-season") || "";
const teamManagerOptionsNode = document.getElementById("teamManagerOptions");
const teamManagerOptions = teamManagerOptionsNode ? JSON.parse(teamManagerOptionsNode.textContent || "{}") : {};
const scorerSeasonTeamOptionsNode = document.getElementById("scorerSeasonTeamOptions");
const scorerSeasonTeamOptions = scorerSeasonTeamOptionsNode
  ? JSON.parse(scorerSeasonTeamOptionsNode.textContent || "{}")
  : {};
const financeSeasonTeamOptionsNode = document.getElementById("financeSeasonTeamOptions");
const financeSeasonTeamOptions = financeSeasonTeamOptionsNode
  ? JSON.parse(financeSeasonTeamOptionsNode.textContent || "{}")
  : {};
const financeSeasonPlayerOptionsNode = document.getElementById("financeSeasonPlayerOptions");
const financeSeasonPlayerOptions = financeSeasonPlayerOptionsNode
  ? JSON.parse(financeSeasonPlayerOptionsNode.textContent || "{}")
  : {};

function wireDeleteBidButtons() {
  document.querySelectorAll(".delete-bid-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const bidId = btn.getAttribute("data-bid-id");
      if (!bidId) {
        return;
      }
      if (!confirm("Delete this bid from the current lot?")) {
        return;
      }
      const fd = new FormData();
      fd.append("bid_id", bidId);
      const res = await postForm("/admin/delete-bid", fd);
      if (!res.ok) {
        alert(res.error || "Unable to delete bid");
      }
    });
  });
}

function postForm(url, formData) {
  return fetch(url, { method: "POST", body: formData }).then((r) => r.json());
}

async function refreshSessions() {
  if (!sessionSelect) {
    return;
  }

  const response = await fetch("/admin/session/list");
  const result = await response.json();
  if (!result.ok) {
    if (sessionStatus) {
      sessionStatus.textContent = result.error || "Unable to load sessions";
    }
    return;
  }

  sessionSelect.innerHTML = "";
  if (!result.sessions.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No saved sessions";
    sessionSelect.appendChild(option);
    return;
  }

  result.sessions.forEach((session) => {
    const option = document.createElement("option");
    option.value = session.file;
    const savedAt = session.saved_at ? new Date(session.saved_at).toLocaleString() : "";
    option.textContent = savedAt ? `${session.label} (${savedAt})` : session.label;
    sessionSelect.appendChild(option);
  });
}

function ensureSetupPhase() {
  if (isSetupPhase) {
    return true;
  }
  alert("This action is only available during setup phase.");
  return false;
}

const TIER_OPTIONS = ["silver", "gold", "platinum"];
const SPECIALITY_OPTIONS = [
  { value: "ALL_ROUNDER", label: "All-Rounder" },
  { value: "BATTER", label: "Batter" },
  { value: "BOWLER", label: "Bowler" },
];

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function tierOptionsHtml(selected) {
  return TIER_OPTIONS
    .map((value) => `<option value="${value}" ${value === selected ? "selected" : ""}>${value[0].toUpperCase()}${value.slice(1)}</option>`)
    .join("");
}

function specialityOptionsHtml(selected) {
  return SPECIALITY_OPTIONS
    .map((option) => `<option value="${option.value}" ${option.value === selected ? "selected" : ""}>${option.label}</option>`)
    .join("");
}

function managerPlayerOptionsHtml(teamId, selectedPlayerId) {
  const options = Array.isArray(teamManagerOptions[teamId]) ? teamManagerOptions[teamId] : [];
  const safeSelected = selectedPlayerId || "";

  const optionRows = options
    .map((item) => {
      const playerId = String(item.id || "").trim();
      if (!playerId) {
        return "";
      }
      const tier = String(item.tier || "").trim();
      const speciality = String(item.speciality || "-").trim().replace(/_/g, " ");
      const label = `${item.name || "Unknown"} (${tier || "-"}, ${speciality || "-"})`;
      return `<option value="${escapeHtml(playerId)}" ${playerId === safeSelected ? "selected" : ""}>${escapeHtml(label)}</option>`;
    })
    .join("");

  return optionRows || `<option value="${escapeHtml(safeSelected)}" selected>${escapeHtml(safeSelected || "Current manager")}</option>`;
}

function titleCaseTier(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) {
    return "-";
  }
  return `${text[0].toUpperCase()}${text.slice(1)}`;
}

function setSelectOptions(selectEl, options, placeholder) {
  if (!selectEl) {
    return;
  }

  const currentValue = selectEl.value;
  selectEl.innerHTML = "";

  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = placeholder || "Select";
  selectEl.appendChild(defaultOption);

  options.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.id || "";
    option.textContent = item.name ? `${item.name} (${item.id || ""})` : (item.id || "");
    selectEl.appendChild(option);
  });

  if (currentValue) {
    selectEl.value = currentValue;
  }
}

function refreshScorerMatchTeamOptions() {
  if (!scorerMatchForm) {
    return;
  }

  const seasonSlug = scorerMatchForm.querySelector('select[name="season_slug"]')?.value || "";
  const options = Array.isArray(scorerSeasonTeamOptions[seasonSlug]) ? scorerSeasonTeamOptions[seasonSlug] : [];

  const teamASelect = scorerMatchForm.querySelector('select[name="team_a_global_id"]');
  const teamBSelect = scorerMatchForm.querySelector('select[name="team_b_global_id"]');
  const walkoverWinnerSelect = scorerMatchForm.querySelector('select[name="walkover_winner_global_id"]');

  setSelectOptions(teamASelect, options, "Select Team A");
  setSelectOptions(teamBSelect, options, "Select Team B");
  setSelectOptions(walkoverWinnerSelect, options, "Select walkover winner (if walkover)");
}

function financeRowsForSeason(seasonSlug) {
  const key = String(seasonSlug || "").toLowerCase();
  return Array.isArray(financeSeasonTeamOptions[key]) ? financeSeasonTeamOptions[key] : [];
}

function financePlayersForSeason(seasonSlug) {
  const key = String(seasonSlug || "").toLowerCase();
  return Array.isArray(financeSeasonPlayerOptions[key]) ? financeSeasonPlayerOptions[key] : [];
}

function populateFinanceTeamSelect(selectEl, rows, placeholder) {
  if (!selectEl) {
    return;
  }

  const currentValue = selectEl.value;
  selectEl.innerHTML = "";

  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = placeholder || "Select team";
  selectEl.appendChild(defaultOption);

  rows.forEach((row) => {
    const option = document.createElement("option");
    option.value = row.id || "";
    option.textContent = `${row.name || row.id || "Team"} (Purse: ${row.purse_remaining ?? 0})`;
    selectEl.appendChild(option);
  });

  if (currentValue) {
    selectEl.value = currentValue;
  }
}

function populateFinancePlayerSelect(selectEl, rows, placeholder) {
  if (!selectEl) {
    return;
  }

  const currentValue = selectEl.value;
  selectEl.innerHTML = "";

  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = placeholder || "Select player";
  selectEl.appendChild(defaultOption);

  rows.forEach((row) => {
    const option = document.createElement("option");
    option.value = row.id || "";
    const squadLabel = row.squad === "bench" ? "Bench" : "Active";
    option.textContent = `${row.name || row.id || "Player"} (${row.from_team_name || row.from_team_id || "-"}, ${squadLabel})`;
    selectEl.appendChild(option);
  });

  if (currentValue) {
    selectEl.value = currentValue;
  }
}

function renderSeasonFinanceTable(rows) {
  const tbody = seasonFinanceTable?.querySelector("tbody");
  if (!tbody) {
    return;
  }

  if (!Array.isArray(rows) || !rows.length) {
    tbody.innerHTML = "<tr><td colspan=\"5\">No season finance data available. Create or load a season first.</td></tr>";
    return;
  }

  tbody.innerHTML = rows
    .map(
      (row) => `<tr>
        <td>${escapeHtml(row.name || row.id || "Team")}</td>
        <td>${escapeHtml(row.purse_remaining ?? 0)}</td>
        <td>${escapeHtml(row.credits_remaining ?? 0)}</td>
        <td>${escapeHtml(row.active_count ?? 0)}</td>
        <td>${escapeHtml(row.bench_count ?? 0)}</td>
      </tr>`
    )
    .join("");
}

function setFinanceSeason(seasonSlug) {
  const safeSeason = String(seasonSlug || "").toLowerCase();
  const rows = financeRowsForSeason(safeSeason);
  const playerRows = financePlayersForSeason(safeSeason);

  const adjustSeasonSelect = financeAdjustForm?.querySelector('select[name="season_slug"]');
  const transferSeasonSelect = financeTransferForm?.querySelector('select[name="season_slug"]');
  const playerTransferSeasonSelect = financePlayerTransferForm?.querySelector('select[name="season_slug"]');
  if (adjustSeasonSelect && adjustSeasonSelect.value !== safeSeason) {
    adjustSeasonSelect.value = safeSeason;
  }
  if (transferSeasonSelect && transferSeasonSelect.value !== safeSeason) {
    transferSeasonSelect.value = safeSeason;
  }
  if (playerTransferSeasonSelect && playerTransferSeasonSelect.value !== safeSeason) {
    playerTransferSeasonSelect.value = safeSeason;
  }

  populateFinanceTeamSelect(financeAdjustForm?.querySelector('select[name="team_id"]'), rows, "Select team");
  populateFinanceTeamSelect(financeTransferForm?.querySelector('select[name="from_team_id"]'), rows, "From team");
  populateFinanceTeamSelect(financeTransferForm?.querySelector('select[name="to_team_id"]'), rows, "To team");
  populateFinanceTeamSelect(financePlayerTransferForm?.querySelector('select[name="to_team_id"]'), rows, "To team");
  populateFinancePlayerSelect(financePlayerTransferForm?.querySelector('select[name="player_id"]'), playerRows, "Select player");
  renderSeasonFinanceTable(rows);
}

if (financeAdjustForm || financeTransferForm || financePlayerTransferForm) {
  const initialSeason = financeSelectedSeason
    || financeAdjustForm?.querySelector('select[name="season_slug"]')?.value
    || financeTransferForm?.querySelector('select[name="season_slug"]')?.value
    || financePlayerTransferForm?.querySelector('select[name="season_slug"]')?.value
    || "";
  setFinanceSeason(initialSeason);

  const adjustSeasonSelect = financeAdjustForm?.querySelector('select[name="season_slug"]');
  const transferSeasonSelect = financeTransferForm?.querySelector('select[name="season_slug"]');
  const playerTransferSeasonSelect = financePlayerTransferForm?.querySelector('select[name="season_slug"]');

  if (adjustSeasonSelect) {
    adjustSeasonSelect.addEventListener("change", () => {
      setFinanceSeason(adjustSeasonSelect.value || "");
    });
  }

  if (transferSeasonSelect) {
    transferSeasonSelect.addEventListener("change", () => {
      setFinanceSeason(transferSeasonSelect.value || "");
    });
  }

  if (playerTransferSeasonSelect) {
    playerTransferSeasonSelect.addEventListener("change", () => {
      setFinanceSeason(playerTransferSeasonSelect.value || "");
    });
  }
}

if (financeAdjustForm) {
  financeAdjustForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const payload = new FormData(financeAdjustForm);
    const seasonSlug = String(payload.get("season_slug") || "").trim().toLowerCase();
    const operation = String(payload.get("operation") || "").trim().toLowerCase();
    const amount = String(payload.get("amount") || "").trim();
    const teamId = String(payload.get("team_id") || "").trim();
    const comment = String(payload.get("comment") || "").trim();

    if (!seasonSlug || !teamId || !amount || !comment) {
      if (financeStatus) {
        financeStatus.textContent = "Error: season, team, amount, and comment are required.";
      }
      return;
    }

    const res = await postForm("/admin/finances/adjust", payload);
    if (!res.ok) {
      if (financeStatus) {
        financeStatus.textContent = `Error: ${res.error || "Unable to adjust purse"}`;
      }
      return;
    }

    financeSeasonTeamOptions[seasonSlug] = Array.isArray(res.team_rows) ? res.team_rows : [];
    setFinanceSeason(seasonSlug);
    if (financeStatus) {
      const verb = operation === "remove" ? "Removed" : "Added";
      financeStatus.textContent = `${verb} ${amount} for selected team in ${seasonSlug}.`;
    }
  });
}

if (financeTransferForm) {
  financeTransferForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const payload = new FormData(financeTransferForm);
    const seasonSlug = String(payload.get("season_slug") || "").trim().toLowerCase();
    const amount = String(payload.get("amount") || "").trim();
    const comment = String(payload.get("comment") || "").trim();

    if (!seasonSlug || !amount || !comment) {
      if (financeStatus) {
        financeStatus.textContent = "Error: season, amount, and comment are required.";
      }
      return;
    }

    const res = await postForm("/admin/finances/transfer", payload);
    if (!res.ok) {
      if (financeStatus) {
        financeStatus.textContent = `Error: ${res.error || "Unable to transfer purse"}`;
      }
      return;
    }

    financeSeasonTeamOptions[seasonSlug] = Array.isArray(res.team_rows) ? res.team_rows : [];
    setFinanceSeason(seasonSlug);
    if (financeStatus) {
      financeStatus.textContent = `Transferred ${amount} in ${seasonSlug}.`;
    }
  });
}

if (financePlayerTransferForm) {
  financePlayerTransferForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const payload = new FormData(financePlayerTransferForm);
    const seasonSlug = String(payload.get("season_slug") || "").trim().toLowerCase();
    const playerId = String(payload.get("player_id") || "").trim();
    const toTeamId = String(payload.get("to_team_id") || "").trim();
    const comment = String(payload.get("comment") || "").trim();

    if (!seasonSlug || !playerId || !toTeamId || !comment) {
      if (financeStatus) {
        financeStatus.textContent = "Error: season, player, target team, and comment are required.";
      }
      return;
    }

    const res = await postForm("/admin/finances/player-transfer", payload);
    if (!res.ok) {
      if (financeStatus) {
        financeStatus.textContent = `Error: ${res.error || "Unable to transfer player"}`;
      }
      return;
    }

    financeSeasonTeamOptions[seasonSlug] = Array.isArray(res.team_rows) ? res.team_rows : [];
    financeSeasonPlayerOptions[seasonSlug] = Array.isArray(res.player_rows) ? res.player_rows : [];
    setFinanceSeason(seasonSlug);
    if (financeStatus) {
      financeStatus.textContent = "Player transfer completed.";
    }
  });
}

function startInlineEdit(row, html) {
  if (!row || row.dataset.inlineEditing === "true") {
    return;
  }
  row.dataset.inlineEditing = "true";
  row.dataset.originalHtml = row.innerHTML;
  row.innerHTML = html;
}

function cancelInlineEdit(row) {
  if (!row || !row.dataset.originalHtml) {
    return;
  }
  row.innerHTML = row.dataset.originalHtml;
  delete row.dataset.originalHtml;
  delete row.dataset.inlineEditing;
}

function startPlayerInlineEdit(btn) {
  const row = btn.closest("tr");
  if (!row) {
    return;
  }
  const cells = row.querySelectorAll("td");
  const playerId = btn.getAttribute("data-player-id") || "";
  const name = btn.getAttribute("data-player-name") || "";
  const tier = btn.getAttribute("data-player-tier") || "silver";
  const speciality = btn.getAttribute("data-player-speciality") || "ALL_ROUNDER";
  const base = cells[3]?.textContent?.trim() || "";
  const status = cells[4]?.textContent?.trim() || "";
  const currentBid = cells[5]?.textContent?.trim() || "";
  const soldTo = cells[6]?.textContent?.trim() || "";
  const soldPrice = cells[7]?.textContent?.trim() || "";

  startInlineEdit(
    row,
    `<td><input class="inline-player-name" value="${escapeHtml(name)}"></td>
     <td><select class="inline-player-tier">${tierOptionsHtml(tier)}</select></td>
     <td><select class="inline-player-speciality">${specialityOptionsHtml(speciality)}</select></td>
     <td>${escapeHtml(base)}</td>
     <td>${escapeHtml(status)}</td>
     <td>${escapeHtml(currentBid)}</td>
     <td>${escapeHtml(soldTo)}</td>
     <td>${escapeHtml(soldPrice)}</td>
     <td>
       <button class="btn save-player-inline" type="button" data-player-id="${escapeHtml(playerId)}">Save</button>
       <button class="btn btn-outline cancel-inline" type="button">Cancel</button>
     </td>`
  );
}

function startTeamInlineEdit(btn) {
  const row = btn.closest("tr");
  if (!row) {
    return;
  }
  const cells = row.querySelectorAll("td");
  const teamId = btn.getAttribute("data-team-id") || "";
  const teamName = btn.getAttribute("data-team-name") || "";
  const managerTier = btn.getAttribute("data-manager-tier") || "silver";
  const managerPlayerId = btn.getAttribute("data-manager-player-id") || "";
  const manager = cells[1]?.textContent?.trim() || "";
  const managerSpeciality = cells[2]?.textContent?.trim() || "-";
  const managerPlayer = cells[4]?.textContent?.trim() || "-";

  startInlineEdit(
    row,
    `<td><input class="inline-team-name" value="${escapeHtml(teamName)}"></td>
     <td>${escapeHtml(manager)}</td>
      <td>${escapeHtml(managerSpeciality)}</td>
    <td>${escapeHtml(titleCaseTier(managerTier))}</td>
     <td><select class="inline-team-manager-player">${managerPlayerOptionsHtml(teamId, managerPlayerId || (managerPlayer !== "-" ? managerPlayer : ""))}</select></td>
     <td>
       <button class="btn save-team-inline" type="button" data-team-id="${escapeHtml(teamId)}">Save</button>
       <button class="btn btn-outline cancel-inline" type="button">Cancel</button>
     </td>`
  );
}

if (managerForm) {
  managerForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!ensureSetupPhase()) {
      return;
    }
    const data = new FormData(managerForm);
    const res = await postForm("/admin/create-manager", data);
    credResult.textContent = res.ok
      ? `Team account created (${res.username}). Temporary password: ${res.temporary_password}`
      : `Error: ${res.error}`;
    if (res.ok) {
      managerForm.reset();
    }
  });
}

if (phaseForm) {
  phaseForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const res = await postForm("/admin/set-phase", new FormData(phaseForm));
    if (!res.ok) {
      alert(res.error || "Phase update failed");
    }
  });
}

if (nominateBtn) {
  nominateBtn.addEventListener("click", async () => {
    const res = await postForm("/admin/nominate-next", new FormData());
    if (!res.ok) {
      alert(res.error || "Unable to sell current lot and nominate next");
    }
  });
}

if (previousBtn) {
  previousBtn.addEventListener("click", async () => {
    const res = await postForm("/admin/previous-player", new FormData());
    if (!res.ok) {
      alert(res.error || "Unable to go to previous player");
    }
  });
}

if (closeBtn) {
  closeBtn.addEventListener("click", async () => {
    const res = await postForm("/admin/close-current", new FormData());
    if (!res.ok) {
      alert(res.error || "Unable to close lot");
    }
  });
}

if (completeBtn) {
  completeBtn.addEventListener("click", async () => {
    if (!confirm("Complete draft and run incomplete-team penalty?")) {
      return;
    }
    const res = await postForm("/admin/complete-draft", new FormData());
    if (!res.ok) {
      alert(res.error || "Unable to complete draft");
    }
  });
}

if (saveSessionBtn) {
  saveSessionBtn.addEventListener("click", async () => {
    const fd = new FormData();
    fd.append("session_name", (sessionNameInput?.value || "").trim());
    fd.append("overwrite", overwriteSessionCheckbox?.checked ? "true" : "false");
    const res = await postForm("/admin/session/save", fd);
    if (!res.ok) {
      sessionStatus.textContent = `Error: ${res.error || "Unable to save session"}`;
      return;
    }
    sessionStatus.textContent = res.overwritten
      ? `Replaced existing session: ${res.file}`
      : `Saved session: ${res.file}`;
    if (sessionNameInput) {
      sessionNameInput.value = "";
    }
    await refreshSessions();
  });
}

if (publishSessionBtn) {
  publishSessionBtn.addEventListener("click", async () => {
    const fd = new FormData();
    fd.append("session_name", (publishNameInput?.value || "").trim());
    fd.append("session_link_suffix", (publishSuffixInput?.value || "").trim());
    fd.append("overwrite", "false");
    if (!confirm("Publish this completed auction snapshot?")) {
      return;
    }

    const res = await postForm("/admin/publish-session", fd);
    if (!res.ok) {
      if (publishStatus) {
        publishStatus.textContent = `Error: ${res.error || "Unable to publish session"}`;
      }
      return;
    }

    if (publishStatus) {
      publishStatus.textContent = `Published session: ${res.file} at ${res.public_path || `/${res.file.replace(/\.json$/, "")}`}`;
    }
    if (publishNameInput) {
      publishNameInput.value = "";
    }
    if (publishSuffixInput) {
      publishSuffixInput.value = "";
    }
  });
}

if (loadSessionBtn) {
  loadSessionBtn.addEventListener("click", async () => {
    const selected = sessionSelect?.value || "";
    if (!selected) {
      sessionStatus.textContent = "Select a saved session to load.";
      return;
    }
    if (!confirm(`Load session ${selected}? Current auction state will be replaced.`)) {
      return;
    }

    const fd = new FormData();
    fd.append("session_file", selected);
    const res = await postForm("/admin/session/load", fd);
    if (!res.ok) {
      sessionStatus.textContent = `Error: ${res.error || "Unable to load session"}`;
      return;
    }
    sessionStatus.textContent = `Loaded session: ${res.loaded}`;
    await refreshSessions();
  });
}

if (scorerForm) {
  scorerForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const res = await postForm("/admin/scorer", new FormData(scorerForm));
    if (!res.ok) {
      if (scorerStatus) {
        scorerStatus.textContent = `Error: ${res.error || "Unable to save scorer config"}`;
      }
      return;
    }

    if (scorerStatus) {
      scorerStatus.textContent = `Saved ${res.config.title} (${res.config.version}) to ${res.download_filename}`;
    }
  });
}

if (scorerMatchForm) {
  const seasonSelect = scorerMatchForm.querySelector('select[name="season_slug"]');
  if (seasonSelect) {
    seasonSelect.addEventListener("change", refreshScorerMatchTeamOptions);
  }
  refreshScorerMatchTeamOptions();

  scorerMatchForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const res = await postForm("/admin/scorer/matches", new FormData(scorerMatchForm));
    if (!res.ok) {
      if (scorerMatchStatus) {
        scorerMatchStatus.textContent = `Error: ${res.error || "Unable to save match"}`;
      }
      return;
    }

    if (scorerMatchStatus) {
      const row = res.row || {};
      scorerMatchStatus.textContent = `Saved ${row.match_id || "match"} (${row.season_slug || "-"}).`;
    }

    const walkoverField = scorerMatchForm.querySelector('input[name="walkover"]');
    if (walkoverField) {
      walkoverField.checked = false;
    }
    const winnerField = scorerMatchForm.querySelector('select[name="walkover_winner_global_id"]');
    if (winnerField) {
      winnerField.value = "";
    }

    const url = new URL(window.location.href);
    url.searchParams.set("tab", "scorer");
    window.location.href = url.toString();
  });
}

if (scorerImportForm) {
  scorerImportForm.addEventListener("submit", async (e) => {
    e.preventDefault();

    if (scorerImportStatus) {
      scorerImportStatus.textContent = "Uploading CSV files and computing stats...";
    }
    if (scorerImportSummary) {
      scorerImportSummary.textContent = "";
    }

    const submitImport = async (forceOverwrite = false) => {
      const payload = new FormData(scorerImportForm);
      if (forceOverwrite) {
        payload.set("confirm_overwrite", "true");
      }
      payload.set("include_in_fantasy_points", scorerImportForm.querySelector('input[name="include_in_fantasy_points"]')?.checked ? "true" : "false");
      return postForm("/admin/scorer/import", payload);
    };

    let res = await submitImport(false);
    if (!res.ok && res.confirmation_required) {
      const duplicates = Array.isArray(res.duplicates) ? res.duplicates : [];
      const duplicateList = duplicates
        .map((item) => `${item.match_id || "-"} (${item.season_slug || "-"})`)
        .join(", ");
      const confirmed = confirm(
        `Duplicate match IDs found${duplicateList ? `: ${duplicateList}` : ""}. Overwrite existing stats?`
      );
      if (!confirmed) {
        if (scorerImportStatus) {
          scorerImportStatus.textContent = "Import cancelled. Duplicate match IDs require overwrite confirmation.";
        }
        return;
      }
      res = await submitImport(true);
    }

    if (!res.ok) {
      if (scorerImportStatus) {
        scorerImportStatus.textContent = `Error: ${res.error || "Unable to import scorer CSV"}`;
      }
      return;
    }

    if (scorerImportStatus) {
      const imported = Array.isArray(res.imports) ? res.imports.length : 0;
      const failed = Array.isArray(res.errors) ? res.errors.length : 0;
      let status = `Imported ${imported} file(s). Failed: ${failed}.`;
      if (res.confirmation_required) {
        status += " Some duplicate match IDs were skipped until overwrite confirmation.";
      }
      scorerImportStatus.textContent = status;
    }

    if (scorerImportSummary && res.summary) {
      const teamCount = res.summary.team_rows || 0;
      const playerCount = res.summary.player_rows || 0;
      const matchLabel = res.summary.match_id || "-";
      const fantasyMode = res.summary.include_in_fantasy_points === false ? "excluded from" : "included in";
      scorerImportSummary.textContent = `Latest match ${matchLabel}: ${teamCount} team rows, ${playerCount} player rows updated in global stats; fantasy contribution ${fantasyMode} aggregates.`;
    }

    if (Array.isArray(res.errors) && res.errors.length && scorerImportSummary) {
      const details = res.errors.map((item) => `${item.file}: ${item.error}`).join(" | ");
      scorerImportSummary.textContent = `${scorerImportSummary.textContent} Errors: ${details}`.trim();
    }
  });
}

wireDeleteBidButtons();
refreshSessions();

document.addEventListener("click", async (event) => {
  const btn = event.target.closest("button");
  if (!btn) {
    return;
  }

  if (btn.classList.contains("undo-scorer-import-btn")) {
    const matchKey = btn.getAttribute("data-match-key") || "";
    const matchId = btn.getAttribute("data-match-id") || "";
    const seasonSlug = btn.getAttribute("data-season-slug") || "";

    if (!matchKey) {
      alert("Missing match key for undo.");
      return;
    }

    const confirmed = confirm(
      `Undo imported stats for match ${matchId || "-"} in season ${seasonSlug || "-"}? This will remove match, team, and player stats for this import.`
    );
    if (!confirmed) {
      return;
    }

    const fd = new FormData();
    fd.append("match_key", matchKey);
    const res = await postForm("/admin/scorer/import/undo", fd);
    if (!res.ok) {
      if (scorerImportStatus) {
        scorerImportStatus.textContent = `Error: ${res.error || "Unable to undo scorer import"}`;
      }
      return;
    }

    if (scorerImportStatus) {
      const summary = res.summary || {};
      scorerImportStatus.textContent = `Undo complete for ${summary.match_id || matchId || "match"}. Removed ${summary.removed_team_rows || 0} team rows and ${summary.removed_player_rows || 0} player rows.`;
    }
    if (scorerImportSummary) {
      scorerImportSummary.textContent = "Global aggregates were rebuilt after undo.";
    }

    const url = new URL(window.location.href);
    url.searchParams.set("tab", "scorer");
    window.location.href = url.toString();
    return;
  }

  if (btn.classList.contains("delete-scorer-match-btn")) {
    const seasonSlug = btn.getAttribute("data-season-slug") || "";
    const matchId = btn.getAttribute("data-match-id") || "";
    const isWalkover = (btn.getAttribute("data-is-walkover") || "").toLowerCase() === "true";

    const confirmed = confirm(
      isWalkover
        ? `Delete walkover match ${matchId || "-"} in ${seasonSlug || "-"}? Walkover stats will be restored.`
        : `Delete match ${matchId || "-"} in ${seasonSlug || "-"}?`
    );
    if (!confirmed) {
      return;
    }

    const fd = new FormData();
    fd.append("season_slug", seasonSlug);
    fd.append("match_id", matchId);
    const res = await postForm("/admin/scorer/matches/delete", fd);
    if (!res.ok) {
      if (scorerMatchStatus) {
        scorerMatchStatus.textContent = `Error: ${res.error || "Unable to delete match"}`;
      }
      return;
    }

    if (scorerMatchStatus) {
      const summary = res.summary || {};
      scorerMatchStatus.textContent = summary.restored_walkover_stats
        ? `Deleted ${matchId}. Restored walkover stats.`
        : `Deleted ${matchId}.`;
    }

    const url = new URL(window.location.href);
    url.searchParams.set("tab", "scorer");
    window.location.href = url.toString();
    return;
  }

  if (btn.classList.contains("cancel-inline")) {
    cancelInlineEdit(btn.closest("tr"));
    return;
  }

  if (btn.classList.contains("edit-player-btn")) {
    if (!ensureSetupPhase()) {
      return;
    }
    startPlayerInlineEdit(btn);
    return;
  }

  if (btn.classList.contains("save-player-inline")) {
    const row = btn.closest("tr");
    const playerId = btn.getAttribute("data-player-id") || "";
    const name = row?.querySelector(".inline-player-name")?.value?.trim() || "";
    const tier = row?.querySelector(".inline-player-tier")?.value || "";
    const speciality = row?.querySelector(".inline-player-speciality")?.value || "";
    if (!name) {
      alert("Player name is required");
      return;
    }

    const fd = new FormData();
    fd.append("player_id", playerId);
    fd.append("name", name);
    fd.append("tier", tier);
    fd.append("speciality", speciality);
    const res = await postForm("/admin/update-player", fd);
    if (!res.ok) {
      alert(res.error || "Unable to update player");
      return;
    }
    window.location.reload();
    return;
  }

  if (btn.classList.contains("delete-player-btn")) {
    if (!ensureSetupPhase()) {
      return;
    }
    const playerId = btn.getAttribute("data-player-id") || "";
    const name = btn.getAttribute("data-player-name") || "this player";
    if (!confirm(`Delete player ${name}?`)) {
      return;
    }

    const fd = new FormData();
    fd.append("player_id", playerId);
    const res = await postForm("/admin/delete-player", fd);
    if (!res.ok) {
      alert(res.error || "Unable to delete player");
      return;
    }
    window.location.reload();
    return;
  }

  if (btn.classList.contains("toggle-team-participation-btn")) {
    if (!ensureSetupPhase()) {
      return;
    }
    const teamId = btn.getAttribute("data-team-id") || "";
    const teamName = btn.getAttribute("data-team-name") || "this team";
    const currentlyActive = (btn.getAttribute("data-is-active") || "true").toLowerCase() === "true";
    const nextActive = !currentlyActive;

    const prompt = nextActive
      ? `Include ${teamName} in auction participation?`
      : `Exclude ${teamName} from auction participation? Team will remain in the database.`;
    if (!confirm(prompt)) {
      return;
    }

    const fd = new FormData();
    fd.append("team_id", teamId);
    fd.append("is_active", nextActive ? "true" : "false");
    const res = await postForm("/admin/set-team-participation", fd);
    if (!res.ok) {
      alert(res.error || "Unable to update team participation");
      return;
    }
    window.location.reload();
    return;
  }

  if (btn.classList.contains("edit-team-btn")) {
    if (!ensureSetupPhase()) {
      return;
    }
    startTeamInlineEdit(btn);
    return;
  }

  if (btn.classList.contains("save-team-inline")) {
    const row = btn.closest("tr");
    const teamId = btn.getAttribute("data-team-id") || "";
    const teamName = row?.querySelector(".inline-team-name")?.value?.trim() || "";
    const managerPlayerId = row?.querySelector(".inline-team-manager-player")?.value || "";
    if (!teamName) {
      alert("Team name is required");
      return;
    }
    if (!managerPlayerId) {
      alert("Manager player is required");
      return;
    }

    const fd = new FormData();
    fd.append("team_id", teamId);
    fd.append("team_name", teamName);
    fd.append("manager_player_id", managerPlayerId);
    const res = await postForm("/admin/update-team", fd);
    if (!res.ok) {
      alert(res.error || "Unable to update team");
      return;
    }
    window.location.reload();
    return;
  }

  if (btn.classList.contains("delete-team-btn")) {
    if (!ensureSetupPhase()) {
      return;
    }
    const teamId = btn.getAttribute("data-team-id") || "";
    const teamName = btn.getAttribute("data-team-name") || "this team";
    if (!confirm(`Delete team ${teamName} and linked manager?`)) {
      return;
    }

    const fd = new FormData();
    fd.append("team_id", teamId);
    const res = await postForm("/admin/delete-team", fd);
    if (!res.ok) {
      alert(res.error || "Unable to delete team");
      return;
    }
    window.location.reload();
  }
});

socket.on("state_update", () => {
  window.location.reload();
});
