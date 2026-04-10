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
const adminDashboard = document.getElementById("adminDashboard");
const isSetupPhase = adminDashboard?.getAttribute("data-is-setup") === "true";

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

function pickTier(defaultTier) {
  const value = (prompt("Enter tier (silver/gold/platinum)", defaultTier || "silver") || "")
    .trim()
    .toLowerCase();
  if (!value) {
    return null;
  }
  if (!["silver", "gold", "platinum"].includes(value)) {
    alert("Tier must be one of: silver, gold, platinum.");
    return null;
  }
  return value;
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

wireDeleteBidButtons();
refreshSessions();

document.querySelectorAll(".edit-player-btn").forEach((btn) => {
  btn.addEventListener("click", async () => {
    if (!ensureSetupPhase()) {
      return;
    }
    const playerId = btn.getAttribute("data-player-id");
    const currentName = btn.getAttribute("data-player-name") || "";
    const currentTier = btn.getAttribute("data-player-tier") || "silver";

    const name = (prompt("Edit player name", currentName) || "").trim();
    if (!name) {
      return;
    }
    const tier = pickTier(currentTier);
    if (!tier) {
      return;
    }

    const fd = new FormData();
    fd.append("player_id", playerId);
    fd.append("name", name);
    fd.append("tier", tier);

    const res = await postForm("/admin/update-player", fd);
    if (!res.ok) {
      alert(res.error || "Unable to update player");
    }
  });
});

document.querySelectorAll(".delete-player-btn").forEach((btn) => {
  btn.addEventListener("click", async () => {
    if (!ensureSetupPhase()) {
      return;
    }
    const playerId = btn.getAttribute("data-player-id");
    const name = btn.getAttribute("data-player-name") || "this player";
    if (!confirm(`Delete player ${name}?`)) {
      return;
    }

    const fd = new FormData();
    fd.append("player_id", playerId);
    const res = await postForm("/admin/delete-player", fd);
    if (!res.ok) {
      alert(res.error || "Unable to delete player");
    }
  });
});

document.querySelectorAll(".edit-manager-btn").forEach((btn) => {
  btn.addEventListener("click", async () => {
    if (!ensureSetupPhase()) {
      return;
    }
    const managerUsername = btn.getAttribute("data-username") || "";
    const currentName = btn.getAttribute("data-display-name") || "";

    const username = (prompt("Edit manager username", managerUsername) || "").trim();
    if (!username) {
      return;
    }
    const displayName = (prompt("Edit manager display name", currentName) || "").trim();
    if (!displayName) {
      return;
    }

    const fd = new FormData();
    fd.append("manager_username", managerUsername);
    fd.append("username", username);
    fd.append("display_name", displayName);

    const res = await postForm("/admin/update-manager", fd);
    if (!res.ok) {
      alert(res.error || "Unable to update manager");
    }
  });
});

document.querySelectorAll(".delete-manager-btn").forEach((btn) => {
  btn.addEventListener("click", async () => {
    if (!ensureSetupPhase()) {
      return;
    }
    const managerUsername = btn.getAttribute("data-username") || "";
    if (!confirm(`Delete manager ${managerUsername} and linked team?`)) {
      return;
    }

    const fd = new FormData();
    fd.append("manager_username", managerUsername);
    const res = await postForm("/admin/delete-manager", fd);
    if (!res.ok) {
      alert(res.error || "Unable to delete manager");
    }
  });
});

document.querySelectorAll(".edit-team-btn").forEach((btn) => {
  btn.addEventListener("click", async () => {
    if (!ensureSetupPhase()) {
      return;
    }
    const teamId = btn.getAttribute("data-team-id") || "";
    const currentName = btn.getAttribute("data-team-name") || "";
    const currentTier = btn.getAttribute("data-manager-tier") || "silver";

    const teamName = (prompt("Edit team name", currentName) || "").trim();
    if (!teamName) {
      return;
    }
    const managerTier = pickTier(currentTier);
    if (!managerTier) {
      return;
    }

    const fd = new FormData();
    fd.append("team_id", teamId);
    fd.append("team_name", teamName);
    fd.append("manager_tier", managerTier);

    const res = await postForm("/admin/update-team", fd);
    if (!res.ok) {
      alert(res.error || "Unable to update team");
    }
  });
});

document.querySelectorAll(".delete-team-btn").forEach((btn) => {
  btn.addEventListener("click", async () => {
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
    }
  });
});

socket.on("state_update", () => {
  window.location.reload();
});
