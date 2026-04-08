const socket = io();

const managerForm = document.getElementById("managerForm");
const credResult = document.getElementById("credResult");
const phaseForm = document.getElementById("phaseForm");
const nominateBtn = document.getElementById("nominateBtn");
const previousBtn = document.getElementById("previousBtn");
const closeBtn = document.getElementById("closeBtn");
const completeBtn = document.getElementById("completeBtn");

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

if (managerForm) {
  managerForm.addEventListener("submit", async (e) => {
    e.preventDefault();
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

wireDeleteBidButtons();

socket.on("state_update", () => {
  window.location.reload();
});
