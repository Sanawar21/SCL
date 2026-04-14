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
const adminDashboard = document.getElementById("adminDashboard");
const isSetupPhase = adminDashboard?.getAttribute("data-is-setup") === "true";
const ADMIN_SCROLL_KEY = "adminDashboardScrollY";

function rememberScrollPosition() {
  try {
    sessionStorage.setItem(ADMIN_SCROLL_KEY, String(window.scrollY || 0));
  } catch (err) {
    // Ignore storage failures in restricted browsers.
  }
}

function restoreScrollPosition() {
  try {
    const raw = sessionStorage.getItem(ADMIN_SCROLL_KEY);
    if (raw == null) {
      return;
    }
    sessionStorage.removeItem(ADMIN_SCROLL_KEY);
    const value = Number(raw);
    if (!Number.isFinite(value) || value < 0) {
      return;
    }
    window.requestAnimationFrame(() => {
      window.scrollTo({ top: value, behavior: "auto" });
    });
  } catch (err) {
    // Ignore storage failures in restricted browsers.
  }
}

restoreScrollPosition();

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
      const res = await postForm("/auction/admin/delete-bid", fd);
      if (!res.ok) {
        alert(res.error || "Unable to delete bid");
      }
    });
  });
}

function postForm(url, formData) {
  rememberScrollPosition();
  return fetch(url, { method: "POST", body: formData }).then((r) => r.json());
}

async function refreshSessions() {
  if (!sessionSelect) {
    return;
  }

  const response = await fetch("/auction/admin/session/list");
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

function startManagerInlineEdit(btn) {
  const row = btn.closest("tr");
  if (!row) {
    return;
  }
  const cells = row.querySelectorAll("td");
  const username = btn.getAttribute("data-username") || "";
  const displayName = btn.getAttribute("data-display-name") || "";
  const speciality = btn.getAttribute("data-speciality") || "ALL_ROUNDER";
  const team = cells[3]?.textContent?.trim() || "";

  startInlineEdit(
    row,
    `<td><input class="inline-manager-username" value="${escapeHtml(username)}"></td>
     <td><input class="inline-manager-display-name" value="${escapeHtml(displayName)}"></td>
     <td><select class="inline-manager-speciality">${specialityOptionsHtml(speciality)}</select></td>
     <td>${escapeHtml(team)}</td>
     <td>
       <button class="btn save-manager-inline" type="button" data-manager-username="${escapeHtml(username)}">Save</button>
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
  const manager = cells[1]?.textContent?.trim() || "";
  const managerSpeciality = cells[2]?.textContent?.trim() || "-";

  startInlineEdit(
    row,
    `<td><input class="inline-team-name" value="${escapeHtml(teamName)}"></td>
     <td>${escapeHtml(manager)}</td>
      <td>${escapeHtml(managerSpeciality)}</td>
     <td><select class="inline-team-tier">${tierOptionsHtml(managerTier)}</select></td>
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
    const res = await postForm("/auction/admin/create-manager", data);
    credResult.textContent = res.ok
      ? `Credentials created. Temporary password for team ${res.team_id}: ${res.temporary_password}`
      : `Error: ${res.error}`;
    if (res.ok) {
      managerForm.reset();
    }
  });
}

if (phaseForm) {
  phaseForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const res = await postForm("/auction/admin/set-phase", new FormData(phaseForm));
    if (!res.ok) {
      alert(res.error || "Phase update failed");
    }
  });
}

if (nominateBtn) {
  nominateBtn.addEventListener("click", async () => {
    const res = await postForm("/auction/admin/nominate-next", new FormData());
    if (!res.ok) {
      alert(res.error || "Unable to sell current lot and nominate next");
    }
  });
}

if (previousBtn) {
  previousBtn.addEventListener("click", async () => {
    const res = await postForm("/auction/admin/previous-player", new FormData());
    if (!res.ok) {
      alert(res.error || "Unable to go to previous player");
    }
  });
}

if (closeBtn) {
  closeBtn.addEventListener("click", async () => {
    const res = await postForm("/auction/admin/close-current", new FormData());
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
    const res = await postForm("/auction/admin/complete-draft", new FormData());
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
    const res = await postForm("/auction/admin/session/save", fd);
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

    const res = await postForm("/auction/admin/publish-session", fd);
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
    const res = await postForm("/auction/admin/session/load", fd);
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

wireDeleteBidButtons();
refreshSessions();

document.addEventListener("submit", (event) => {
  const form = event.target;
  if (!(form instanceof HTMLFormElement)) {
    return;
  }
  if (!form.closest("#adminDashboard")) {
    return;
  }
  rememberScrollPosition();
});

document.addEventListener("click", async (event) => {
  const btn = event.target.closest("button");
  if (!btn) {
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
    const res = await postForm("/auction/admin/update-player", fd);
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
    const res = await postForm("/auction/admin/delete-player", fd);
    if (!res.ok) {
      alert(res.error || "Unable to delete player");
      return;
    }
    window.location.reload();
    return;
  }

  if (btn.classList.contains("edit-manager-btn")) {
    if (!ensureSetupPhase()) {
      return;
    }
    startManagerInlineEdit(btn);
    return;
  }

  if (btn.classList.contains("save-manager-inline")) {
    const row = btn.closest("tr");
    const managerUsername = btn.getAttribute("data-manager-username") || "";
    const username = row?.querySelector(".inline-manager-username")?.value?.trim() || "";
    const displayName = row?.querySelector(".inline-manager-display-name")?.value?.trim() || "";
    const speciality = row?.querySelector(".inline-manager-speciality")?.value || "";
    if (!username || !displayName) {
      alert("Username and display name are required");
      return;
    }

    const fd = new FormData();
    fd.append("manager_username", managerUsername);
    fd.append("username", username);
    fd.append("display_name", displayName);
    fd.append("speciality", speciality);
    const res = await postForm("/auction/admin/update-manager", fd);
    if (!res.ok) {
      alert(res.error || "Unable to update manager");
      return;
    }
    window.location.reload();
    return;
  }

  if (btn.classList.contains("delete-manager-btn")) {
    if (!ensureSetupPhase()) {
      return;
    }
    const managerUsername = btn.getAttribute("data-username") || "";
    if (!confirm(`Delete manager ${managerUsername} and linked team?`)) {
      return;
    }

    const fd = new FormData();
    fd.append("manager_username", managerUsername);
    const res = await postForm("/auction/admin/delete-manager", fd);
    if (!res.ok) {
      alert(res.error || "Unable to delete manager");
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
    const managerTier = row?.querySelector(".inline-team-tier")?.value || "";
    if (!teamName) {
      alert("Team name is required");
      return;
    }

    const fd = new FormData();
    fd.append("team_id", teamId);
    fd.append("team_name", teamName);
    fd.append("manager_tier", managerTier);
    const res = await postForm("/auction/admin/update-team", fd);
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
    const res = await postForm("/auction/admin/delete-team", fd);
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
