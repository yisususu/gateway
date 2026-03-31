// ── BooMGateway Dashboard SPA ────────────────────────────
(function () {
  "use strict";

  const API = "/dashboard/api";
  let currentUser = null;
  let usageRefreshTimer = null;

  // ── Init ──────────────────────────────────────────────
  document.addEventListener("DOMContentLoaded", () => {
    setupLogin();
    setupLogout();
    setupAdminButtons();
    window.addEventListener("hashchange", onRoute);
    checkSession();
  });

  // ── API helpers ───────────────────────────────────────
  async function api(path, opts = {}) {
    const res = await fetch(API + path, {
      headers: { "Content-Type": "application/json", ...opts.headers },
      ...opts,
    });
    if (res.status === 401) { showLogin(); throw new Error("unauthorized"); }
    if (res.status === 204) return null;
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || data.message || res.statusText);
    return data;
  }

  // ── Session ───────────────────────────────────────────
  async function checkSession() {
    try {
      const me = await api("/auth/me");
      currentUser = me;
      navigateToDashboard(me.role);
    } catch {
      showLogin();
    }
  }

  function showLogin() {
    currentUser = null;
    clearUsageRefresh();
    document.querySelectorAll(".page").forEach((p) => p.classList.remove("active"));
    document.getElementById("page-login").classList.add("active");
  }

  function navigateToDashboard(role) {
    document.querySelectorAll(".page").forEach((p) => p.classList.remove("active"));
    if (role === "admin") {
      document.getElementById("page-admin").classList.add("active");
      onRoute();
    } else {
      document.getElementById("page-dashboard").classList.add("active");
      loadUserData();
      startUsageRefresh();
    }
  }

  // ── Login ─────────────────────────────────────────────
  function setupLogin() {
    document.getElementById("login-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const errEl = document.getElementById("login-error");
      errEl.classList.add("hidden");
      const btn = document.getElementById("login-btn");
      btn.disabled = true;
      btn.textContent = "Logging in...";
      try {
        const res = await fetch(API + "/auth/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_id: document.getElementById("user_id").value.trim(),
            api_key: document.getElementById("api_key").value,
          }),
        });
        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          throw new Error(data.error || data.message || "Login failed");
        }
        const data = await res.json();
        currentUser = data;
        navigateToDashboard(data.role);
      } catch (err) {
        errEl.textContent = err.message;
        errEl.classList.remove("hidden");
      } finally {
        btn.disabled = false;
        btn.textContent = "Login";
      }
    });
  }

  // ── Logout ────────────────────────────────────────────
  function setupLogout() {
    document.getElementById("logout-btn").addEventListener("click", doLogout);
    document.getElementById("logout-btn-admin").addEventListener("click", doLogout);
  }

  async function doLogout() {
    await fetch(API + "/auth/logout", { method: "POST" }).catch(() => {});
    showLogin();
  }

  // ── Routing (admin) ───────────────────────────────────
  function onRoute() {
    const hash = location.hash || "#/admin/plans";
    document.querySelectorAll("#page-admin .nav-link").forEach((a) => {
      a.classList.toggle("active", a.getAttribute("href") === hash);
    });
    document.querySelectorAll("#page-admin .section").forEach((s) => {
      s.classList.toggle("active", s.id === sectionFromHash(hash));
    });
    const section = sectionFromHash(hash);
    if (section === "admin-plans") loadPlans();
    else if (section === "admin-keys") loadKeys();
    else if (section === "admin-assignments") loadAssignments();
  }

  function sectionFromHash(hash) {
    if (hash.includes("/admin/plans")) return "admin-plans";
    if (hash.includes("/admin/keys")) return "admin-keys";
    if (hash.includes("/admin/assignments")) return "admin-assignments";
    return "admin-plans";
  }

  // ── User Dashboard ────────────────────────────────────
  async function loadUserData() {
    try {
      const [plan, usage, keyInfo] = await Promise.all([
        api("/user/plan"),
        api("/user/usage"),
        api("/user/key-info"),
      ]);
      renderPlan(plan);
      renderUsage(usage);
      renderKeyInfo(keyInfo);
    } catch (err) {
      console.error("Failed to load user data:", err);
    }
  }

  function renderPlan(plan) {
    const el = document.getElementById("plan-info");
    if (!plan.plan_name) {
      el.innerHTML = "<p>No plan assigned. Using default limits.</p>";
      return;
    }
    const limits = [];
    if (plan.concurrency_limit) limits.push(`Concurrency: ${plan.concurrency_limit}`);
    if (plan.rpm_limit) limits.push(`RPM: ${plan.rpm_limit}`);
    if (plan.window_limits && plan.window_limits.length > 0) {
      plan.window_limits.forEach(([l, w]) => limits.push(`${l} requests / ${formatDuration(w)}`));
    }
    el.innerHTML = `
      <p><strong>${esc(plan.plan_name)}</strong></p>
      <ul>${limits.map((l) => `<li>${esc(l)}</li>`).join("")}</ul>
    `;
  }

  function renderUsage(usage) {
    const el = document.getElementById("usage-info");
    let html = `<p>Current concurrency: <strong>${usage.concurrency}</strong></p>`;
    if (usage.windows.length === 0) {
      html += "<p>No active rate limit windows.</p>";
    } else {
      html += '<table><tr><th>Model</th><th>Window</th><th>Count</th><th>Progress</th></tr>';
      usage.windows.forEach((w) => {
        const parts = w.cache_key.split(":");
        const model = parts[1] || "unknown";
        const windowLabel = formatDuration(w.window_secs);
        const pct = w.count > 0 && w.window_secs > 0 ? Math.min((w.elapsed_secs / w.window_secs) * 100, 100) : 0;
        html += `<tr>
          <td class="mono">${esc(model)}</td>
          <td>${esc(windowLabel)}</td>
          <td>${w.count}</td>
          <td><div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div></td>
        </tr>`;
      });
      html += "</table>";
    }
    el.innerHTML = html;
  }

  function renderKeyInfo(info) {
    const el = document.getElementById("key-info");
    if (info.error) { el.innerHTML = `<p>${esc(info.error)}</p>`; return; }
    const rows = [
      ["Token", info.token_prefix],
      ["Key Name", info.key_name || "-"],
      ["Spend", "$" + (info.spend || 0).toFixed(4)],
      ["Max Budget", info.max_budget != null ? "$" + info.max_budget : "Unlimited"],
      ["Blocked", info.blocked ? "Yes" : "No"],
      ["RPM Limit", info.rpm_limit || "Default"],
      ["Expires", info.expires || "Never"],
      ["Created", info.created_at || "-"],
    ];
    el.innerHTML = `<table>${rows.map(([k, v]) => `<tr><td>${esc(k)}</td><td>${esc(String(v))}</td></tr>`).join("")}</table>`;
  }

  function startUsageRefresh() {
    clearUsageRefresh();
    usageRefreshTimer = setInterval(async () => {
      try {
        const usage = await api("/user/usage");
        renderUsage(usage);
      } catch {}
    }, 5000);
  }

  function clearUsageRefresh() {
    if (usageRefreshTimer) { clearInterval(usageRefreshTimer); usageRefreshTimer = null; }
  }

  // ── Admin: Plans ──────────────────────────────────────
  async function loadPlans() {
    try {
      const data = await api("/admin/plans");
      renderPlansTable(data.plans || []);
    } catch {}
  }

  function renderPlansTable(plans) {
    const wrap = document.getElementById("plans-table-wrap");
    if (plans.length === 0) { wrap.innerHTML = "<p>No plans defined.</p>"; return; }
    wrap.innerHTML = `<table>
      <tr><th>Name</th><th>Concurrency</th><th>RPM</th><th>Windows</th><th>Actions</th></tr>
      ${plans.map((p) => `<tr>
        <td><strong>${esc(p.name)}</strong></td>
        <td>${p.concurrency_limit || "-"}</td>
        <td>${p.rpm_limit || "-"}</td>
        <td>${(p.window_limits || []).map(([l, w]) => `${l}/${formatDuration(w)}`).join(", ") || "-"}</td>
        <td><button class="btn-danger" onclick="window._deletePlan('${esc(p.name)}')">Delete</button></td>
      </tr>`).join("")}
    </table>`;
  }

  window._deletePlan = async (name) => {
    if (!confirm(`Delete plan "${name}"?`)) return;
    await api(`/admin/plans/${encodeURIComponent(name)}`, { method: "DELETE" });
    loadPlans();
  };

  // ── Admin: Keys ───────────────────────────────────────
  let keysPage = 1;

  async function loadKeys(page) {
    if (page !== undefined) keysPage = page;
    try {
      const data = await api(`/admin/keys?page=${keysPage}&per_page=50`);
      renderKeysTable(data.keys || []);
      renderKeysPagination(data);
    } catch (err) {
      const wrap = document.getElementById("keys-table-wrap");
      if (wrap) wrap.innerHTML = `<p class="error-msg">Failed to load keys: ${esc(err.message)}</p>`;
      console.error("loadKeys error:", err);
    }
  }

  function renderKeysTable(keys) {
    const wrap = document.getElementById("keys-table-wrap");
    if (keys.length === 0) { wrap.innerHTML = "<p>No keys found.</p>"; return; }
    wrap.innerHTML = `<table>
      <tr><th>Token</th><th>Name</th><th>User</th><th>Spend</th><th>Budget</th><th>Status</th><th>Actions</th></tr>
      ${keys.map((k) => `<tr>
        <td class="mono">${esc(k.token_prefix)}</td>
        <td>${esc(k.key_name || "-")}</td>
        <td>${esc(k.user_id || "-")}</td>
        <td>$${(k.spend || 0).toFixed(4)}</td>
        <td>${k.max_budget != null ? "$" + k.max_budget : "-"}</td>
        <td>${k.blocked ? '<span style="color:var(--danger)">Blocked</span>' : "Active"}</td>
        <td>
          ${k.blocked
            ? `<button class="btn-small" onclick="window._unblockKey('${esc(k.token_hash)}')">Unblock</button>`
            : `<button class="btn-danger" onclick="window._blockKey('${esc(k.token_hash)}')">Block</button>`}
        </td>
      </tr>`).join("")}
    </table>`;
  }

  function renderKeysPagination(data) {
    const el = document.getElementById("keys-pagination");
    const pages = Math.ceil(data.total / data.per_page);
    if (pages <= 1) { el.innerHTML = ""; return; }
    el.innerHTML = `
      <button ${data.page <= 1 ? "disabled" : ""} onclick="window._loadKeysPage(${data.page - 1})">&lt;</button>
      <span>Page ${data.page} of ${pages} (${data.total} keys)</span>
      <button ${data.page >= pages ? "disabled" : ""} onclick="window._loadKeysPage(${data.page + 1})">&gt;</button>
    `;
  }

  window._loadKeysPage = (p) => loadKeys(p);
  window._blockKey = async (hash) => {
    await api(`/admin/keys/${encodeURIComponent(hash)}/block`, { method: "POST" });
    loadKeys();
  };
  window._unblockKey = async (hash) => {
    await api(`/admin/keys/${encodeURIComponent(hash)}/unblock`, { method: "POST" });
    loadKeys();
  };

  // ── Admin: Assignments ────────────────────────────────
  async function loadAssignments() {
    try {
      const data = await api("/admin/assignments");
      renderAssignmentsTable(data.assignments || []);
    } catch {}
  }

  function renderAssignmentsTable(assignments) {
    const wrap = document.getElementById("assignments-table-wrap");
    if (assignments.length === 0) { wrap.innerHTML = "<p>No assignments.</p>"; return; }
    wrap.innerHTML = `<table>
      <tr><th>Key Hash</th><th>Plan</th><th>Actions</th></tr>
      ${assignments.map((a) => `<tr>
        <td class="mono">${esc(a.key_hash.substring(0, 16))}...</td>
        <td>${esc(a.plan_name)}</td>
        <td><button class="btn-danger" onclick="window._unassignKey('${esc(a.key_hash)}')">Remove</button></td>
      </tr>`).join("")}
    </table>`;
  }

  window._unassignKey = async (hash) => {
    await api(`/admin/assignments/${encodeURIComponent(hash)}`, { method: "DELETE" });
    loadAssignments();
  };

  // ── Admin: Modals ─────────────────────────────────────
  function setupAdminButtons() {
    document.getElementById("btn-new-plan").addEventListener("click", showNewPlanModal);
    document.getElementById("btn-new-key").addEventListener("click", showNewKeyModal);
    document.getElementById("btn-new-assignment").addEventListener("click", showNewAssignmentModal);
  }

  function showModal(html) {
    document.getElementById("modal-content").innerHTML = html;
    document.getElementById("modal-overlay").classList.remove("hidden");
  }

  function hideModal() {
    document.getElementById("modal-overlay").classList.add("hidden");
  }

  document.getElementById("modal-overlay").addEventListener("click", (e) => {
    if (e.target === e.currentTarget) hideModal();
  });

  function showNewPlanModal() {
    showModal(`
      <h3>Create / Update Plan</h3>
      <div class="form-group"><label>Name</label><input id="m-plan-name" required></div>
      <div class="form-group"><label>Concurrency Limit</label><input id="m-plan-concurrency" type="number"></div>
      <div class="form-group"><label>RPM Limit</label><input id="m-plan-rpm" type="number"></div>
      <div class="form-group"><label>Window Limits (JSON array e.g. [[100,18000]])</label><textarea id="m-plan-windows" rows="2">[]</textarea></div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="hideModal()" style="width:auto">Cancel</button>
        <button class="btn-primary" id="m-plan-submit">Save</button>
      </div>
    `);
    document.getElementById("m-plan-submit").addEventListener("click", async () => {
      try {
        const windows = JSON.parse(document.getElementById("m-plan-windows").value || "[]");
        await api("/admin/plans", {
          method: "PUT",
          body: JSON.stringify({
            name: document.getElementById("m-plan-name").value,
            concurrency_limit: document.getElementById("m-plan-concurrency").value ? Number(document.getElementById("m-plan-concurrency").value) : null,
            rpm_limit: document.getElementById("m-plan-rpm").value ? Number(document.getElementById("m-plan-rpm").value) : null,
            window_limits: windows,
          }),
        });
        hideModal();
        loadPlans();
      } catch (err) { alert("Error: " + err.message); }
    });
  }

  function showNewKeyModal() {
    showModal(`
      <h3>Create API Key</h3>
      <div class="form-group"><label>Key Name</label><input id="m-key-name"></div>
      <div class="form-group"><label>User ID</label><input id="m-key-user"></div>
      <div class="form-group"><label>Team ID</label><input id="m-key-team"></div>
      <div class="form-group"><label>Models (comma-separated)</label><input id="m-key-models"></div>
      <div class="form-group"><label>Max Budget</label><input id="m-key-budget" type="number" step="0.01"></div>
      <div class="form-group"><label>RPM Limit</label><input id="m-key-rpm" type="number"></div>
      <div class="form-group"><label>Plan Name (optional)</label><input id="m-key-plan"></div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="hideModal()" style="width:auto">Cancel</button>
        <button class="btn-primary" id="m-key-submit">Create</button>
      </div>
    `);
    document.getElementById("m-key-submit").addEventListener("click", async () => {
      try {
        const modelsVal = document.getElementById("m-key-models").value.trim();
        const data = await api("/admin/keys", {
          method: "POST",
          body: JSON.stringify({
            key_name: document.getElementById("m-key-name").value || null,
            user_id: document.getElementById("m-key-user").value || null,
            team_id: document.getElementById("m-key-team").value || null,
            models: modelsVal ? modelsVal.split(",").map((s) => s.trim()) : null,
            max_budget: document.getElementById("m-key-budget").value ? Number(document.getElementById("m-key-budget").value) : null,
            rpm_limit: document.getElementById("m-key-rpm").value ? Number(document.getElementById("m-key-rpm").value) : null,
            plan_name: document.getElementById("m-key-plan").value || null,
          }),
        });
        hideModal();
        showModal(`
          <h3>Key Created</h3>
          <p class="key-warning">Copy this key now. It will NOT be shown again.</p>
          <div class="key-display">${esc(data.key)}</div>
          <p>Key Name: ${esc(data.key_name || "-")}</p>
          <div class="modal-actions">
            <button class="btn-primary" onclick="hideModal(); window._loadKeysPage();">Done</button>
          </div>
        `);
      } catch (err) { alert("Error: " + err.message); }
    });
  }

  function showNewAssignmentModal() {
    showModal(`
      <h3>Assign Key to Plan</h3>
      <div class="form-group"><label>Key Hash (full)</label><input id="m-asgn-hash" required></div>
      <div class="form-group"><label>Plan Name</label><input id="m-asgn-plan" required></div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="hideModal()" style="width:auto">Cancel</button>
        <button class="btn-primary" id="m-asgn-submit">Assign</button>
      </div>
    `);
    document.getElementById("m-asgn-submit").addEventListener("click", async () => {
      try {
        await api("/admin/assignments", {
          method: "POST",
          body: JSON.stringify({
            key_hash: document.getElementById("m-asgn-hash").value,
            plan_name: document.getElementById("m-asgn-plan").value,
          }),
        });
        hideModal();
        loadAssignments();
      } catch (err) { alert("Error: " + err.message); }
    });
  }

  // ── Helpers ───────────────────────────────────────────
  function esc(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function formatDuration(secs) {
    if (secs < 60) return secs + "s";
    if (secs < 3600) return (secs / 60) + "min";
    if (secs < 86400) return (secs / 3600) + "h";
    return (secs / 86400) + "d";
  }
})();
