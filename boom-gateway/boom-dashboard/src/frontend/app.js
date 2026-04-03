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
      const titleEl = document.getElementById("user-sidebar-title");
      if (titleEl && currentUser) titleEl.textContent = currentUser.user_id || "Dashboard";
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
        const userId = document.getElementById("user_id").value.trim();
        const res = await fetch(API + "/auth/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_id: userId || "",
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
    const hash = location.hash || "#/admin/models";
    document.querySelectorAll("#page-admin .nav-link").forEach((a) => {
      a.classList.toggle("active", a.getAttribute("href") === hash);
    });
    document.querySelectorAll("#page-admin .section").forEach((s) => {
      s.classList.toggle("active", s.id === sectionFromHash(hash));
    });
    const section = sectionFromHash(hash);
    if (section === "admin-models") loadModels();
    else if (section === "admin-aliases") loadAliases();
    else if (section === "admin-plans") loadPlans();
    else if (section === "admin-keys") { setupKeysSearch(); loadKeys(); }
    else if (section === "admin-assignments") loadAssignments();
    else if (section === "admin-teams") loadTeams();
    else if (section === "admin-logs") { setupLogsFilters(); loadLogs(); }
    else if (section === "admin-config") loadConfig();
  }

  function sectionFromHash(hash) {
    if (hash.includes("/admin/models")) return "admin-models";
    if (hash.includes("/admin/aliases")) return "admin-aliases";
    if (hash.includes("/admin/plans")) return "admin-plans";
    if (hash.includes("/admin/keys")) return "admin-keys";
    if (hash.includes("/admin/teams")) return "admin-teams";
    if (hash.includes("/admin/assignments")) return "admin-assignments";
    if (hash.includes("/admin/logs")) return "admin-logs";
    if (hash.includes("/admin/config")) return "admin-config";
    return "admin-models";
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
      renderTokenInfo(keyInfo);
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
    const concLimit = usage.concurrency_limit || "-";
    let html = `<p>Current concurrency: <strong>${usage.concurrency}</strong> / ${concLimit}</p>`;
    if (usage.windows.length === 0) {
      html += "<p>No active rate limit windows.</p>";
    } else {
      html += '<table><tr><th>Model</th><th>Window</th><th>Count</th><th>Progress</th></tr>';
      usage.windows.forEach((w) => {
        const parts = w.cache_key.split(":");
        const model = parts[1] || "unknown";
        const windowLabel = formatDuration(w.window_secs);
        const limit = w.limit;
        const countLabel = limit != null ? `${w.count} / ${limit}` : `${w.count}`;
        const pct = w.count > 0 && w.window_secs > 0 ? Math.min((w.elapsed_secs / w.window_secs) * 100, 100) : 0;
        const fillClass = limit != null && w.count >= limit ? "danger" : limit != null && w.count / limit >= 0.8 ? "warn" : "";
        html += `<tr>
          <td class="mono">${esc(model)}</td>
          <td>${esc(windowLabel)}</td>
          <td>${countLabel}</td>
          <td><div class="progress-bar"><div class="progress-fill ${fillClass}" style="width:${pct}%"></div></div></td>
        </tr>`;
      });
      html += "</table>";
    }
    el.innerHTML = html;
  }

  function renderTokenInfo(info) {
    const el = document.getElementById("token-info");
    const input = info.total_input_tokens;
    const output = info.total_output_tokens;
    // If both are null the SpendLogs table doesn't exist — hide the card.
    if (input == null && output == null) {
      el.innerHTML = '<p style="color:var(--text3)">Token usage data not available.</p>';
      return;
    }
    const total = (input || 0) + (output || 0);
    const inputPct = total > 0 ? ((input || 0) / total * 100).toFixed(1) : 0;
    const outputPct = total > 0 ? ((output || 0) / total * 100).toFixed(1) : 0;
    el.innerHTML = `
      <div class="token-stats">
        <div class="token-stat">
          <div class="token-stat-label">Input Tokens</div>
          <div class="token-stat-value">${formatNumber(input || 0)}</div>
          <div class="token-stat-pct">${inputPct}%</div>
        </div>
        <div class="token-stat">
          <div class="token-stat-label">Output Tokens</div>
          <div class="token-stat-value">${formatNumber(output || 0)}</div>
          <div class="token-stat-pct">${outputPct}%</div>
        </div>
        <div class="token-stat token-stat-total">
          <div class="token-stat-label">Total</div>
          <div class="token-stat-value">${formatNumber(total)}</div>
        </div>
      </div>
    `;
  }

  function renderKeyInfo(info) {
    const el = document.getElementById("key-info");
    if (info.error) { el.innerHTML = `<p>${esc(info.error)}</p>`; return; }
    const rows = [
      ["Key Alias", info.key_alias || "-"],
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
        <td>
          <button class="btn-small" onclick="window._editPlan('${esc(p.name)}')">Edit</button>
          <button class="btn-danger" onclick="window._deletePlan('${esc(p.name)}')">Delete</button>
        </td>
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
  let keysSearch = "";
  let keysSearchTimer = null;
  let keysDataCache = [];

  function setupKeysSearch() {
    const el = document.getElementById("keys-search");
    if (!el) return;
    el.value = keysSearch;
    el.addEventListener("input", () => {
      clearTimeout(keysSearchTimer);
      keysSearchTimer = setTimeout(() => {
        keysSearch = el.value.trim();
        keysPage = 1;
        loadKeys();
      }, 300);
    });
  }

  async function loadKeys(page) {
    if (page !== undefined) keysPage = page;
    try {
      let url = `/admin/keys?page=${keysPage}&per_page=50`;
      if (keysSearch) url += `&search=${encodeURIComponent(keysSearch)}`;
      const data = await api(url);
      keysDataCache = data.keys || [];
      renderKeysTable(keysDataCache);
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
      <tr><th>Token</th><th>Alias</th><th>Name</th><th>User</th><th>Spend</th><th>Budget</th><th>Status</th><th>Actions</th></tr>
      ${keys.map((k) => `<tr>
        <td class="mono">${esc(k.token_prefix)}</td>
        <td>${esc(k.key_alias || "-")}</td>
        <td>${esc(k.key_name || "-")}</td>
        <td>${esc(k.user_id || "-")}</td>
        <td>$${(k.spend || 0).toFixed(4)}</td>
        <td>${k.max_budget != null ? "$" + k.max_budget : "-"}</td>
        <td>${k.blocked ? '<span style="color:var(--danger)">Blocked</span>' : "Active"}</td>
        <td>
          <button class="btn-small" onclick="window._editKey('${esc(k.token_hash)}')">Edit</button>
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
  window._editKey = (tokenHash) => {
    const key = keysDataCache.find((k) => k.token_hash === tokenHash);
    if (key) showEditKeyModal(key);
  };
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

  // ── Admin: Models ─────────────────────────────────────
  async function loadModels() {
    try {
      const data = await api("/admin/models");
      renderModelsTable(data.models || []);
    } catch (err) {
      const wrap = document.getElementById("models-table-wrap");
      if (wrap) wrap.innerHTML = `<p class="error-msg">Failed to load models: ${esc(err.message)}</p>`;
    }
  }

  function renderModelsTable(models) {
    const wrap = document.getElementById("models-table-wrap");
    if (models.length === 0) { wrap.innerHTML = "<p>No model deployments.</p>"; return; }
    wrap.innerHTML = `<table>
      <tr><th>Model Name</th><th>LiteLLM Model</th><th>Base URL</th><th>RPM</th><th>Timeout</th><th>Enabled</th><th>Source</th><th>Actions</th></tr>
      ${models.map((m) => `<tr>
        <td><strong>${esc(m.model_name)}</strong></td>
        <td class="mono">${esc(m.litellm_model)}</td>
        <td class="mono">${esc(m.api_base || "-")}</td>
        <td>${m.rpm || "-"}</td>
        <td>${m.timeout}s</td>
        <td>${m.enabled ? '<span class="badge badge-active">Yes</span>' : '<span class="badge badge-blocked">No</span>'}</td>
        <td><span class="badge badge-plan">${esc(m.source || "-")}</span></td>
        <td>
          <button class="btn-small" onclick="window._editModel('${m.id}')">Edit</button>
          <button class="btn-danger" onclick="window._deleteModel('${m.id}','${esc(m.model_name)}')">Delete</button>
        </td>
      </tr>`).join("")}
    </table>`;
  }

  function showNewModelModal(prefill) {
    const p = prefill || {};
    showModal(`
      <h3>${p.id ? "Edit" : "Create"} Model Deployment</h3>
      <div class="form-group"><label>Model Name *</label><input id="m-model-name" value="${esc(p.model_name || "")}" required></div>
      <div class="form-group"><label>LiteLLM Model *</label><input id="m-litellm-model" value="${esc(p.litellm_model || "")}" required></div>
      <div class="form-group"><label>API Key</label><input id="m-model-key" type="password" placeholder="sk-... or os.environ/VAR"></div>
      <div class="form-group"><label>API Key is env reference</label><select id="m-model-key-env"><option value="false">No</option><option value="true">Yes</option></select></div>
      <div class="form-group"><label>API Base URL</label><input id="m-model-base" value="${esc(p.api_base || "")}" placeholder="https://api.openai.com/v1"></div>
      <div class="form-group"><label>API Version (Azure)</label><input id="m-model-version" value="${esc(p.api_version || "")}"></div>
      <div class="form-group"><label>RPM Limit</label><input id="m-model-rpm" type="number" value="${p.rpm || ""}"></div>
      <div class="form-group"><label>Timeout (seconds)</label><input id="m-model-timeout" type="number" value="${p.timeout || 120}"></div>
      <div class="form-group"><label>Temperature</label><input id="m-model-temp" type="number" step="0.1" value="${p.temperature || ""}"></div>
      <div class="form-group"><label>Max Tokens</label><input id="m-model-maxtok" type="number" value="${p.max_tokens || ""}"></div>
      <div class="form-group"><label>Enabled</label><select id="m-model-enabled"><option value="true" ${p.enabled !== false ? "selected" : ""}>Yes</option><option value="false" ${p.enabled === false ? "selected" : ""}>No</option></select></div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="hideModal()" style="width:auto">Cancel</button>
        <button class="btn-primary" id="m-model-submit">${p.id ? "Update" : "Create"}</button>
      </div>
    `);
    document.getElementById("m-model-submit").addEventListener("click", async () => {
      try {
        const body = {
          model_name: document.getElementById("m-model-name").value,
          litellm_model: document.getElementById("m-litellm-model").value,
          api_key: document.getElementById("m-model-key").value || null,
          api_key_env: document.getElementById("m-model-key-env").value === "true",
          api_base: document.getElementById("m-model-base").value || null,
          api_version: document.getElementById("m-model-version").value || null,
          rpm: document.getElementById("m-model-rpm").value ? Number(document.getElementById("m-model-rpm").value) : null,
          timeout: Number(document.getElementById("m-model-timeout").value) || 120,
          temperature: document.getElementById("m-model-temp").value ? Number(document.getElementById("m-model-temp").value) : null,
          max_tokens: document.getElementById("m-model-maxtok").value ? Number(document.getElementById("m-model-maxtok").value) : null,
          enabled: document.getElementById("m-model-enabled").value === "true",
          headers: {},
        };
        const url = p.id ? `/admin/models/${p.id}` : "/admin/models";
        const method = p.id ? "PUT" : "POST";
        await api(url, { method, body: JSON.stringify(body) });
        hideModal();
        loadModels();
      } catch (err) { alert("Error: " + err.message); }
    });
  }

  window._editModel = async (id) => {
    try {
      const data = await api("/admin/models");
      const m = (data.models || []).find((x) => x.id === id);
      if (!m) return;
      showNewModelModal(m);
    } catch (err) { alert("Error: " + err.message); }
  };

  window._deleteModel = async (id, name) => {
    if (!confirm(`Delete model deployment "${name}"?`)) return;
    await api(`/admin/models/${encodeURIComponent(id)}`, { method: "DELETE" });
    loadModels();
  };

  // ── Admin: Aliases ────────────────────────────────────
  async function loadAliases() {
    try {
      const data = await api("/admin/aliases");
      renderAliasesTable(data.aliases || []);
    } catch (err) {
      const wrap = document.getElementById("aliases-table-wrap");
      if (wrap) wrap.innerHTML = `<p class="error-msg">Failed to load aliases: ${esc(err.message)}</p>`;
    }
  }

  function renderAliasesTable(aliases) {
    const wrap = document.getElementById("aliases-table-wrap");
    if (aliases.length === 0) { wrap.innerHTML = "<p>No aliases defined.</p>"; return; }
    wrap.innerHTML = `<table>
      <tr><th>Alias</th><th>Target Model</th><th>Hidden</th><th>Source</th><th>Actions</th></tr>
      ${aliases.map((a) => `<tr>
        <td><strong>${esc(a.alias_name)}</strong></td>
        <td class="mono">${esc(a.target_model)}</td>
        <td>${a.hidden ? "Yes" : "No"}</td>
        <td><span class="badge badge-plan">${esc(a.source || "-")}</span></td>
        <td>
          <button class="btn-small" onclick="window._editAlias('${esc(a.alias_name)}')">Edit</button>
          <button class="btn-danger" onclick="window._deleteAlias('${esc(a.alias_name)}')">Delete</button>
        </td>
      </tr>`).join("")}
    </table>`;
  }

  function showNewAliasModal(prefill) {
    const p = prefill || {};
    showModal(`
      <h3>${p.alias_name ? "Edit" : "Create"} Alias</h3>
      <div class="form-group"><label>Alias Name *</label><input id="m-alias-name" value="${esc(p.alias_name || "")}" ${p.alias_name ? "readonly" : ""}></div>
      <div class="form-group"><label>Target Model *</label><input id="m-alias-target" value="${esc(p.target_model || "")}" required></div>
      <div class="form-group"><label>Hidden</label><select id="m-alias-hidden"><option value="false" ${!p.hidden ? "selected" : ""}>No</option><option value="true" ${p.hidden ? "selected" : ""}>Yes</option></select></div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="hideModal()" style="width:auto">Cancel</button>
        <button class="btn-primary" id="m-alias-submit">${p.alias_name ? "Update" : "Create"}</button>
      </div>
    `);
    document.getElementById("m-alias-submit").addEventListener("click", async () => {
      try {
        const body = {
          alias_name: document.getElementById("m-alias-name").value,
          target_model: document.getElementById("m-alias-target").value,
          hidden: document.getElementById("m-alias-hidden").value === "true",
        };
        const url = p.alias_name ? `/admin/aliases/${encodeURIComponent(p.alias_name)}` : "/admin/aliases";
        const method = p.alias_name ? "PUT" : "POST";
        await api(url, { method, body: JSON.stringify(body) });
        hideModal();
        loadAliases();
      } catch (err) { alert("Error: " + err.message); }
    });
  }

  window._editAlias = async (name) => {
    try {
      const data = await api("/admin/aliases");
      const a = (data.aliases || []).find((x) => x.alias_name === name);
      if (!a) return;
      showNewAliasModal(a);
    } catch (err) { alert("Error: " + err.message); }
  };

  window._deleteAlias = async (name) => {
    if (!confirm(`Delete alias "${name}"?`)) return;
    await api(`/admin/aliases/${encodeURIComponent(name)}`, { method: "DELETE" });
    loadAliases();
  };

  // ── Admin: Config ─────────────────────────────────────
  async function loadConfig() {
    try {
      const data = await api("/admin/config");
      renderConfigTable(data.config || {});
    } catch (err) {
      const wrap = document.getElementById("config-table-wrap");
      if (wrap) wrap.innerHTML = `<p class="error-msg">Failed to load config: ${esc(err.message)}</p>`;
    }
  }

  function renderConfigTable(config) {
    const wrap = document.getElementById("config-table-wrap");
    const keys = Object.keys(config);
    if (keys.length === 0) { wrap.innerHTML = "<p>No configuration entries.</p>"; return; }
    wrap.innerHTML = `<table>
      <tr><th>Key</th><th>Value</th><th>Actions</th></tr>
      ${keys.map((k) => `<tr>
        <td><strong>${esc(k)}</strong></td>
        <td class="mono" style="max-width:400px;word-break:break-all;white-space:pre-wrap">${esc(JSON.stringify(config[k], null, 2))}</td>
        <td><button class="btn-small" onclick="window._editConfig('${esc(k)}')">Edit</button></td>
      </tr>`).join("")}
    </table>`;
  }

  function showNewConfigModal(prefill) {
    const p = prefill || {};
    showModal(`
      <h3>${p.key ? "Edit" : "Set"} Configuration</h3>
      <div class="form-group"><label>Key *</label><input id="m-config-key" value="${esc(p.key || "")}" ${p.key ? "readonly" : ""}></div>
      <div class="form-group"><label>Value (JSON) *</label><textarea id="m-config-value" rows="6">${esc(p.value ? JSON.stringify(p.value, null, 2) : "")}</textarea></div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="hideModal()" style="width:auto">Cancel</button>
        <button class="btn-primary" id="m-config-submit">Save</button>
      </div>
    `);
    document.getElementById("m-config-submit").addEventListener("click", async () => {
      try {
        const value = JSON.parse(document.getElementById("m-config-value").value);
        await api("/admin/config", {
          method: "PATCH",
          body: JSON.stringify({
            key: document.getElementById("m-config-key").value,
            value: value,
          }),
        });
        hideModal();
        loadConfig();
      } catch (err) { alert("Error: " + err.message); }
    });
  }

  window._editConfig = async (key) => {
    try {
      const data = await api("/admin/config");
      const config = data.config || {};
      if (config[key] !== undefined) {
        showNewConfigModal({ key, value: config[key] });
      }
    } catch (err) { alert("Error: " + err.message); }
  };

  // ── Admin: Modals ─────────────────────────────────────
  function setupAdminButtons() {
    document.getElementById("btn-new-plan").addEventListener("click", showNewPlanModal);
    document.getElementById("btn-new-key").addEventListener("click", showNewKeyModal);
    document.getElementById("btn-new-assignment").addEventListener("click", showNewAssignmentModal);
    const btnModel = document.getElementById("btn-new-model");
    if (btnModel) btnModel.addEventListener("click", showNewModelModal);
    const btnAlias = document.getElementById("btn-new-alias");
    if (btnAlias) btnAlias.addEventListener("click", showNewAliasModal);
    const btnConfig = document.getElementById("btn-new-config");
    if (btnConfig) btnConfig.addEventListener("click", showNewConfigModal);
  }

  function showModal(html) {
    document.getElementById("modal-content").innerHTML = html;
    document.getElementById("modal-overlay").classList.remove("hidden");
  }

  function hideModal() {
    document.getElementById("modal-overlay").classList.add("hidden");
  }
  window.hideModal = hideModal;

  document.getElementById("modal-overlay").addEventListener("click", (e) => {
    if (e.target === e.currentTarget) hideModal();
  });

  function showNewPlanModal(prefill) {
    const p = prefill || {};
    showModal(`
      <h3>${p.name ? "Edit" : "Create"} Plan</h3>
      <div class="form-group"><label>Name</label><input id="m-plan-name" value="${esc(p.name || "")}" ${p.name ? "readonly" : ""} required></div>
      <div class="form-group"><label>Concurrency Limit</label><input id="m-plan-concurrency" type="number" value="${p.concurrency_limit || ""}"></div>
      <div class="form-group"><label>RPM Limit</label><input id="m-plan-rpm" type="number" value="${p.rpm_limit || ""}"></div>
      <div class="form-group"><label>Window Limits (JSON e.g. [[100,18000]])</label><textarea id="m-plan-windows" rows="2">${JSON.stringify(p.window_limits || [])}</textarea></div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="hideModal()" style="width:auto">Cancel</button>
        <button class="btn-primary" id="m-plan-submit">${p.name ? "Update" : "Create"}</button>
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

  window._editPlan = async (name) => {
    try {
      const data = await api("/admin/plans");
      const p = (data.plans || []).find((x) => x.name === name);
      if (!p) return;
      showNewPlanModal(p);
    } catch (err) { alert("Error: " + err.message); }
  };

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

  function showEditKeyModal(key) {
    const modelsStr = Array.isArray(key.models) ? key.models.join(", ") : "";
    showModal(`
      <h3>Edit Key</h3>
      <div class="form-group"><label>Alias</label><input id="m-edit-alias" value="${esc(key.key_alias || "")}"></div>
      <div class="form-group"><label>User ID</label><input id="m-edit-user" value="${esc(key.user_id || "")}"></div>
      <div class="form-group"><label>Models (comma-separated)</label><input id="m-edit-models" value="${esc(modelsStr)}"></div>
      <div class="form-group"><label>Max Budget</label><input id="m-edit-budget" type="number" step="0.01" value="${key.max_budget != null ? key.max_budget : ""}"></div>
      <div class="form-group"><label>RPM Limit</label><input id="m-edit-rpm" type="number" value="${key.rpm_limit || ""}"></div>
      <div class="modal-actions">
        <button class="btn-secondary" onclick="hideModal()" style="width:auto">Cancel</button>
        <button class="btn-primary" id="m-edit-submit">Save</button>
      </div>
    `);
    document.getElementById("m-edit-submit").addEventListener("click", async () => {
      try {
        const aliasVal = document.getElementById("m-edit-alias").value.trim();
        const userVal = document.getElementById("m-edit-user").value.trim();
        const modelsVal = document.getElementById("m-edit-models").value.trim();
        const body = {
          key_alias: aliasVal || null,
          user_id: userVal || null,
          models: modelsVal ? modelsVal.split(",").map((s) => s.trim()) : null,
          max_budget: document.getElementById("m-edit-budget").value ? Number(document.getElementById("m-edit-budget").value) : null,
          rpm_limit: document.getElementById("m-edit-rpm").value ? Number(document.getElementById("m-edit-rpm").value) : null,
        };
        await api(`/admin/keys/${encodeURIComponent(key.token_hash)}`, {
          method: "PUT",
          body: JSON.stringify(body),
        });
        hideModal();
        loadKeys();
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

  // ── Admin: Teams ──────────────────────────────────────
  async function loadTeams() {
    try {
      const data = await api("/admin/teams");
      renderTeamsTable(data.teams || []);
    } catch (err) {
      const wrap = document.getElementById("teams-table-wrap");
      if (wrap) wrap.innerHTML = `<p class="error-msg">Failed to load teams: ${esc(err.message)}</p>`;
    }
  }

  function renderTeamsTable(teams) {
    const wrap = document.getElementById("teams-table-wrap");
    if (teams.length === 0) { wrap.innerHTML = "<p>No teams found.</p>"; return; }
    wrap.innerHTML = `<table>
      <tr><th>Team Alias</th><th>Team ID</th><th>Keys</th><th>Requests</th><th>Input Tokens</th><th>Output Tokens</th><th>Total Tokens</th></tr>
      ${teams.map((t) => `<tr>
        <td>${esc(t.team_alias || "-")}</td>
        <td class="mono" title="${esc(t.team_id)}">${esc((t.team_id || "").substring(0, 8))}</td>
        <td>${t.key_count}</td>
        <td>${formatNumber(t.request_count)}</td>
        <td>${formatNumber(t.total_input_tokens || 0)}</td>
        <td>${formatNumber(t.total_output_tokens || 0)}</td>
        <td>${formatNumber((t.total_input_tokens || 0) + (t.total_output_tokens || 0))}</td>
      </tr>`).join("")}
    </table>`;
  }

  // ── Admin: Logs ──────────────────────────────────────
  let logsPage = 1;
  let logsFilters = {};
  let logsFiltersTimer = null;
  let logsFiltersSetup = false;

  function setupLogsFilters() {
    if (logsFiltersSetup) return;
    logsFiltersSetup = true;
    const wrap = document.getElementById("logs-table-wrap");
    if (!wrap) return;
    wrap.addEventListener("input", (e) => {
      if (!e.target.classList.contains("col-filter")) return;
      clearTimeout(logsFiltersTimer);
      logsFiltersTimer = setTimeout(() => {
        const col = e.target.dataset.col;
        const val = e.target.value.trim();
        if (val) { logsFilters[col] = val; } else { delete logsFilters[col]; }
        logsPage = 1;
        loadLogs();
      }, 400);
    });
    const resetBtn = document.getElementById("btn-reset-logs-filters");
    if (resetBtn) {
      resetBtn.addEventListener("click", () => {
        logsFilters = {};
        logsPage = 1;
        loadLogs();
      });
    }
  }

  async function loadLogs(page) {
    if (page !== undefined) logsPage = page;
    try {
      let url = `/admin/logs?page=${logsPage}&per_page=50`;
      for (const [k, v] of Object.entries(logsFilters)) {
        url += `&${encodeURIComponent(k)}=${encodeURIComponent(v)}`;
      }
      const data = await api(url);
      renderLogsTable(data.logs || []);
      renderLogsPagination(data);
    } catch (err) {
      const wrap = document.getElementById("logs-table-wrap");
      if (wrap) wrap.innerHTML = `<p class="error-msg">Failed to load logs: ${esc(err.message)}</p>`;
    }
  }

  const LOGS_FILTER_COLS = [
    { col: "",           placeholder: "",     label: "Time" },
    { col: "team_alias", placeholder: "filter", label: "Team" },
    { col: "key_alias",  placeholder: "filter", label: "Key Alias" },
    { col: "model",      placeholder: "filter", label: "Model" },
    { col: "api_path",   placeholder: "filter", label: "Path" },
    { col: "status_code",placeholder: "filter", label: "Status" },
    { col: "stream",     placeholder: "filter", label: "Stream" },
    { col: "",           placeholder: "",     label: "Input" },
    { col: "",           placeholder: "",     label: "Output" },
    { col: "",           placeholder: "",     label: "Duration" },
    { col: "error",      placeholder: "filter", label: "Error" },
  ];

  function logsFilterRow() {
    return LOGS_FILTER_COLS.map(f =>
      f.col
        ? `<td><input class="col-filter" data-col="${f.col}" placeholder="${f.placeholder}" value="${esc(logsFilters[f.col] || "")}"></td>`
        : "<td></td>"
    ).join("");
  }

  function logsHeaderRow() {
    return LOGS_FILTER_COLS.map(f => `<th>${f.label}</th>`).join("");
  }

  function renderLogsTable(logs) {
    const wrap = document.getElementById("logs-table-wrap");
    const filterRow = logsFilterRow();
    const headerRow = logsHeaderRow();
    if (logs.length === 0) {
      wrap.innerHTML = `<table>
        <tr class="filter-row">${filterRow}</tr>
        <tr>${headerRow}</tr>
        <tr><td colspan="${LOGS_FILTER_COLS.length}" class="no-results">No matching logs found.</td></tr>
      </table>`;
      return;
    }
    wrap.innerHTML = `<table>
      <tr class="filter-row">${filterRow}</tr>
      <tr>${headerRow}</tr>
      ${logs.map((l) => `<tr>
        <td class="mono">${formatTimestamp(l.created_at)}</td>
        <td>${esc(l.team_alias || l.team_id || "-")}</td>
        <td>${esc(l.key_alias || l.key_name || "-")}</td>
        <td class="mono">${esc(l.model)}</td>
        <td class="mono">${esc(l.api_path)}</td>
        <td>${l.status_code >= 400 ? '<span style="color:var(--danger)">' + l.status_code + '</span>' : l.status_code}</td>
        <td>${l.is_stream ? "Yes" : "No"}</td>
        <td>${l.input_tokens != null ? formatNumber(l.input_tokens) : "-"}</td>
        <td>${l.output_tokens != null ? formatNumber(l.output_tokens) : "-"}</td>
        <td>${l.duration_ms != null ? l.duration_ms + "ms" : "-"}</td>
        <td>${l.error_message ? '<span style="color:var(--danger)" title="' + esc(l.error_message) + '">' + esc((l.error_type || "").substring(0, 20)) + '</span>' : "-"}</td>
      </tr>`).join("")}
    </table>`;
  }

  function renderLogsPagination(data) {
    const el = document.getElementById("logs-pagination");
    const pages = Math.ceil(data.total / data.per_page);
    if (pages <= 1) { el.innerHTML = ""; return; }
    el.innerHTML = `
      <button ${data.page <= 1 ? "disabled" : ""} onclick="window._loadLogsPage(${data.page - 1})">&lt;</button>
      <span>Page ${data.page} of ${pages} (${data.total} logs)</span>
      <button ${data.page >= pages ? "disabled" : ""} onclick="window._loadLogsPage(${data.page + 1})">&gt;</button>
    `;
  }

  window._loadLogsPage = (p) => loadLogs(p);

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

  function formatNumber(n) {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + "M";
    if (n >= 1_000) return (n / 1_000).toFixed(1) + "K";
    return String(n);
  }

  function formatTimestamp(iso) {
    if (!iso) return "-";
    const d = new Date(iso);
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
           `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
})();
