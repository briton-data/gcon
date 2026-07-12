/**
 * GCON Dashboard — live client-side controller.
 *
 * Drives all dashboard tabs (Control Center, Topology, Explorer,
 * Monitoring, Analytics, Admin) via polling + in-place DOM updates,
 * so the dashboard stays live without full page reloads.
 */



let currentTab = "control-center";
let explorerView = "jobs";
let explorerData = [];
let isPaused = false;

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

function escapeHtml(value) {
    if (value === null || value === undefined) return "";
    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
}

function statusBadge(status) {
    const normalized = (status || "").toLowerCase();

    const classMap = {
        healthy: "bg-success", idle: "bg-secondary", offline: "bg-secondary",
        running: "bg-primary", completed: "bg-success", failed: "bg-danger",
        pending: "bg-warning text-dark", verified: "bg-success",
        active: "bg-success", suspended: "bg-warning text-dark",
        disabled: "bg-secondary", revoked: "bg-danger", expired: "bg-secondary",
    };
    const labelMap = {
        healthy: "Healthy", idle: "Idle", offline: "Idle", running: "Running",
        completed: "Completed", failed: "Failed", pending: "Pending", verified: "Verified",
        active: "Active", suspended: "Suspended", disabled: "Disabled",
        revoked: "Revoked", expired: "Expired",
    };

    const cls = classMap[normalized] || "bg-secondary";
    const label = labelMap[normalized] || escapeHtml(status || "Unknown");
    return `<span class="badge ${cls}">${label}</span>`;
}

function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
}

function formatUptime(seconds) {
    if (seconds === undefined || seconds === null) return "--";
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);
    return `${h}h ${m}m ${s}s`;
}

async function fetchJson(url, options) {
    const response = await fetch(url, options);
    if (response.status === 401) {
        window.location.href = "/login";
        throw new Error("Not authenticated");
    }
    if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        throw new Error(data.detail || `${url} returned ${response.status}`);
    }
    setConnectionStatus(true);
    return response.json();
}

function setConnectionStatus(ok) {
    const el = document.getElementById("conn-status");
    if (!el) return;
    if (ok) {
        el.className = "badge bg-success me-3";
        el.textContent = "● Coordinator Online";
    } else {
        el.className = "badge bg-danger me-3";
        el.textContent = "● Disconnected";
    }
}

function renderFeed(containerId, events) {
    const feed = document.getElementById(containerId);
    if (!feed) return;

    if (!events || events.length === 0) {
        feed.innerHTML = `<div class="text-secondary">No recent activity.</div>`;
        return;
    }

    let items = "";
    for (const event of events) {
        items += `
            <div class="gcon-activity-item">
                <div class="gcon-activity-time">${escapeHtml(event.timestamp)}</div>
                <div class="gcon-activity-message">${escapeHtml(event.message)}</div>
            </div>
        `;
    }
    feed.innerHTML = items;
}

// ---------------------------------------------------------------
// Tab switching
// ---------------------------------------------------------------

const TAB_TITLES = {
    "control-center": "Control Center",
    "topology": "Cluster Visualization",
    "explorer": "Explorer",
    "monitoring": "Real-Time Monitoring",
    "analytics": "Analytics & History",
    "admin": "Administration",
    "users": "Users",
    "organizations": "Organizations",
    "teams": "Teams",
    "api-keys": "API Keys",
    "permissions": "Permissions",
    "audit-logs": "Audit Logs",
    "notifications": "Notifications",
};

function switchTab(tab) {
    currentTab = tab;

    document.querySelectorAll(".gcon-tab").forEach(el => el.classList.add("d-none"));
    const target = document.getElementById(`tab-${tab}`);
    if (target) target.classList.remove("d-none");

    document.querySelectorAll("#tab-nav a, #tab-nav-top a, #tab-nav-mgmt a").forEach(el => {
        el.classList.toggle("active", el.dataset.tab === tab);
    });

    setText("tab-title", TAB_TITLES[tab] || tab);
    loadActiveTab();
}

function setupTabNav() {
    document.querySelectorAll("#tab-nav a, #tab-nav-top a, #tab-nav-mgmt a").forEach(el => {
        el.addEventListener("click", (e) => {
            e.preventDefault();
            switchTab(el.dataset.tab);
        });
    });
}

function loadActiveTab() {
    if (currentTab === "control-center") loadControlCenter();
    else if (currentTab === "topology") loadTopology();
    else if (currentTab === "explorer") loadExplorer();
    else if (currentTab === "monitoring") loadMonitoring();
    else if (currentTab === "analytics") loadAnalytics();
    else if (currentTab === "admin") loadAdmin();
    else if (currentTab === "users") loadUsersTab();
    else if (currentTab === "organizations") loadOrganizationsTab();
    else if (currentTab === "teams") loadTeamsTab();
    else if (currentTab === "api-keys") loadApiKeysTab();
    else if (currentTab === "permissions") loadPermissionsTab();
    else if (currentTab === "audit-logs") loadAuditLogsTab();
    else if (currentTab === "notifications") loadNotificationsTab();
}



// ---------------------------------------------------------------
// Cluster Health Panel
// ---------------------------------------------------------------

async function loadClusterHealth() {
    try {
        const health = await fetchJson("/health");

        // Overall score
        setText("health-score", `${health.score}%`);

        // Overall state badge
        const badge = document.getElementById("health-state-badge");
        if (badge) {

            const classMap = {
                healthy: "badge bg-success",
                degraded: "badge bg-warning text-dark",
                critical: "badge bg-danger",
            };

            badge.className = classMap[health.state] || "badge bg-secondary";
            badge.textContent =
                health.state.charAt(0).toUpperCase() +
                health.state.slice(1);
        }

        // Main reason
        setText("health-reason", health.reason);

        // Detailed reasons
        const list = document.getElementById("health-reasons-list");

        if (list) {

            list.innerHTML = "";

            if (health.reasons && health.reasons.length) {

                health.reasons.forEach(reason => {
                    const item = document.createElement("div");
                    item.className = `small ${reason.healthy ? "text-secondary" : "text-warning"}`;
                    item.innerHTML = `
                    <i class="bi bi-dot"></i>
                    <strong>${escapeHtml(reason.label)}:</strong> ${escapeHtml(reason.detail)}
    `;
                    list.appendChild(item);


                });

            } else {

                list.innerHTML =
                    `<div class="small text-success">No issues detected.</div>`;

            }

        }

    } catch (err) {

        console.error("Failed to load cluster health:", err);

        setConnectionStatus(false);

    }
}






// ---------------------------------------------------------------
// Control Center
// ---------------------------------------------------------------

async function loadCluster() {
    try {
        const cluster = await fetchJson("/cluster");
        setText("metric-total-nodes", cluster.total_nodes);
        setText("metric-running-jobs", cluster.running_jobs);
        setText("metric-completed-jobs", cluster.completed_jobs);
        setText("metric-failed-jobs", cluster.failed_jobs);
        setText("overview-registered-nodes", cluster.total_nodes);
        setText("overview-active-jobs", cluster.running_jobs);
        setText("cc-node-summary", `${cluster.total_nodes} nodes · ${cluster.idle_nodes} idle`);
    } catch (err) {
        console.error("Failed to load cluster state:", err);
        setConnectionStatus(false);
    }
}

async function loadNodes() {
    const body = document.getElementById("nodes-body");
    if (!body) return;
    try {
        const nodes = await fetchJson("/nodes");
        if (nodes.length === 0) {
            body.innerHTML = `<tr><td colspan="6" class="text-center text-secondary">No registered nodes.</td></tr>`;
            return;
        }
        let rows = "";
        for (const node of nodes) {
            rows += `
                <tr>
                    <td>${escapeHtml(node.node_id)}</td>
                    <td>${statusBadge(node.status)}</td>
                    <td>${escapeHtml(node.running_jobs)}</td>
                    <td>${escapeHtml(node.cpu)}</td>
                    <td>${escapeHtml(node.memory)}</td>
                    <td>${escapeHtml(node.last_seen)}</td>
                </tr>
            `;
        }
        body.innerHTML = rows;
    } catch (err) {
        console.error("Failed to load nodes:", err);
        setConnectionStatus(false);
    }
}

async function loadJobs() {
    const body = document.getElementById("jobs-body");
    if (!body) return;
    try {
        const jobs = await fetchJson("/jobs");
        if (jobs.length === 0) {
            body.innerHTML = `<tr><td colspan="6" class="text-center text-secondary">No jobs submitted.</td></tr>`;
            return;
        }
        let rows = "";
        for (const job of jobs) {
            rows += `
                <tr>
                    <td>${escapeHtml(job.job_id)}</td>
                    <td>${statusBadge(job.status)}</td>
                    <td>${escapeHtml(job.node_id || "-")}</td>
                    <td>${escapeHtml(job.artifacts)}</td>
                    <td>${escapeHtml(job.created_at || "-")}</td>
                    <td>${escapeHtml(job.completed_at || "-")}</td>
                </tr>
            `;
        }
        body.innerHTML = rows;
    } catch (err) {
        console.error("Failed to load jobs:", err);
        setConnectionStatus(false);
    }
}

async function loadEvents() {
    try {
        const events = await fetchJson("/events");
        renderFeed("activity-feed", events);
    } catch (err) {
        console.error("Failed to load events:", err);
        setConnectionStatus(false);
    }
}

async function loadControlCenter() {
    await Promise.all([
        loadCluster(), loadNodes(), loadJobs(), loadClusterHealth(), loadEvents()]);
}

// ---------------------------------------------------------------
// Cluster Visualization (Topology)
// ---------------------------------------------------------------

async function loadTopology() {
    const container = document.getElementById("topology-container");
    if (!container) return;

    try {
        const topo = await fetchJson("/topology");
        container.innerHTML = buildTopologySvg(topo);
    } catch (err) {
        console.error("Failed to load topology:", err);
        setConnectionStatus(false);
    }
}

function buildTopologySvg(topo) {
    const nodes = topo.nodes || [];
    const width = 800;
    const height = Math.max(320, 140 + Math.ceil(nodes.length / 6) * 110);
    const centerX = width / 2;
    const centerY = 80;

    const statusColor = {
        idle: "#6c757d", healthy: "#22c55e", busy: "#3b82f6", offline: "#ef4444",
    };

    let svg = `<svg viewBox="0 0 ${width} ${height}" width="100%" style="max-width:100%">`;

    // Coordinator node
    svg += `
        <circle cx="${centerX}" cy="${centerY}" r="34" fill="#7c3aed" />
        <text x="${centerX}" y="${centerY + 5}" text-anchor="middle" fill="white" font-size="12" font-weight="bold">Coordinator</text>
    `;

    if (nodes.length === 0) {
        svg += `<text x="${centerX}" y="${centerY + 80}" text-anchor="middle" fill="#94a3b8" font-size="13">No worker nodes registered</text>`;
        svg += "</svg>";
        return svg;
    }

    const perRow = 6;
    const spacingX = width / (Math.min(nodes.length, perRow) + 1);

    nodes.forEach((node, i) => {
        const row = Math.floor(i / perRow);
        const col = i % perRow;
        const nodesInRow = Math.min(nodes.length - row * perRow, perRow);
        const rowSpacingX = width / (nodesInRow + 1);
        const x = rowSpacingX * (col + 1);
        const y = centerY + 140 + row * 110;

        const color = statusColor[(node.status || "").toLowerCase()] || "#6c757d";

        svg += `<line x1="${centerX}" y1="${centerY + 34}" x2="${x}" y2="${y - 26}" stroke="#334155" stroke-width="2" />`;
        svg += `
            <circle cx="${x}" cy="${y}" r="26" fill="${color}" />
            <text x="${x}" y="${y + 4}" text-anchor="middle" fill="white" font-size="10" font-weight="bold">${escapeHtml(node.node_id)}</text>
            <text x="${x}" y="${y + 44}" text-anchor="middle" fill="#94a3b8" font-size="11">${escapeHtml(node.status)} · ${escapeHtml(node.running_jobs)} jobs</text>
        `;
    });

    svg += "</svg>";
    return svg;
}

// ---------------------------------------------------------------
// Explorer
// ---------------------------------------------------------------

const EXPLORER_COLUMNS = {
    jobs: ["job_id", "status", "node_id", "artifacts", "created_at", "completed_at"],
    nodes: ["node_id", "status", "running_jobs", "cpu", "memory", "last_seen"],
    receipts: ["receipt_id", "job_id", "status", "created_at"],
    artifacts: ["artifact_id", "filename", "sha256", "size", "uploaded_at"],
};

const EXPLORER_HEADERS = {
    jobs: ["Job ID", "Status", "Node", "Artifacts", "Created", "Completed"],
    nodes: ["Node ID", "Status", "Running Jobs", "CPU", "Memory", "Last Seen"],
    receipts: ["Receipt ID", "Job ID", "Status", "Created"],
    artifacts: ["Artifact ID", "Filename", "SHA-256", "Size (bytes)", "Uploaded"],
};

const EXPLORER_BADGE_COLUMNS = new Set(["status"]);

function setupExplorerNav() {
    document.querySelectorAll("#explorer-subnav button").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll("#explorer-subnav button").forEach(b => {
                b.classList.remove("btn-primary");
                b.classList.add("btn-outline-primary");
            });
            btn.classList.remove("btn-outline-primary");
            btn.classList.add("btn-primary");
            explorerView = btn.dataset.explorer;
            loadExplorer();
        });
    });

    const search = document.getElementById("explorer-search");
    if (search) {
        search.addEventListener("input", () => renderExplorerRows(filterExplorerData(search.value)));
    }
}

function filterExplorerData(query) {
    if (!query) return explorerData;
    const q = query.toLowerCase();
    return explorerData.filter(row =>
        Object.values(row).some(v => String(v).toLowerCase().includes(q))
    );
}

function renderExplorerRows(rows) {
    const thead = document.getElementById("explorer-thead");
    const body = document.getElementById("explorer-body");
    if (!thead || !body) return;

    const columns = EXPLORER_COLUMNS[explorerView];
    const headers = EXPLORER_HEADERS[explorerView];

    thead.innerHTML = `<tr>${headers.map(h => `<th>${escapeHtml(h)}</th>`).join("")}</tr>`;

    if (rows.length === 0) {
        body.innerHTML = `<tr><td colspan="${columns.length}" class="text-center text-secondary">No data.</td></tr>`;
        return;
    }

    let html = "";
    for (const row of rows) {
        html += "<tr>";
        for (const col of columns) {
            const value = row[col];
            html += EXPLORER_BADGE_COLUMNS.has(col)
                ? `<td>${statusBadge(value)}</td>`
                : `<td>${escapeHtml(value ?? "-")}</td>`;
        }
        html += "</tr>";
    }
    body.innerHTML = html;
}

async function loadExplorer() {
    try {
        explorerData = await fetchJson(`/${explorerView}`);
        const search = document.getElementById("explorer-search");
        renderExplorerRows(filterExplorerData(search ? search.value : ""));
    } catch (err) {
        console.error("Failed to load explorer data:", err);
        setConnectionStatus(false);
    }
}

// ---------------------------------------------------------------
// Real-Time Monitoring
// ---------------------------------------------------------------

async function loadMonitoring() {
    try {
        const metrics = await fetchJson("/system-metrics");
        setText("sm-avg-cpu", `${metrics.avg_cpu}%`);
        setText("sm-avg-memory", `${metrics.avg_memory}%`);
        setText("sm-running", metrics.running_jobs);
        setText("sm-event-count", metrics.event_count);
        setText("sm-uptime", formatUptime(metrics.uptime_seconds));
        setText("sm-connection", "Live");

        const events = await fetchJson("/events");
        renderFeed("monitoring-activity-feed", events);
    } catch (err) {
        console.error("Failed to load monitoring data:", err);
        setConnectionStatus(false);
        setText("sm-connection", "Down");
    }
}

// ---------------------------------------------------------------
// Analytics & History
// ---------------------------------------------------------------

function renderBarChart(totals) {
    const container = document.getElementById("analytics-bars");
    if (!container) return;

    const max = Math.max(1, ...Object.values(totals));
    const colors = { completed: "#22c55e", failed: "#ef4444", running: "#3b82f6", pending: "#f59e0b" };

    let html = `<div class="gcon-bars-row">`;
    for (const [key, value] of Object.entries(totals)) {
        const heightPct = Math.round((value / max) * 100);
        html += `
            <div class="gcon-bar-col">
                <div class="gcon-bar-track">
                    <div class="gcon-bar-fill" style="height:${heightPct}%; background:${colors[key] || "#64748b"}"></div>
                </div>
                <div class="gcon-bar-label">${escapeHtml(key)}</div>
                <div class="gcon-bar-value">${escapeHtml(value)}</div>
            </div>
        `;
    }
    html += `</div>`;
    container.innerHTML = html;
}

async function loadAnalytics() {
    try {
        const data = await fetchJson("/analytics");
        setText("an-success-rate", `${data.success_rate}%`);
        setText("an-completed", data.totals.completed);
        setText("an-failed", data.totals.failed);
        setText("an-pending", data.totals.pending);
        renderBarChart(data.totals);
        renderFeed("analytics-timeline", data.timeline);
    } catch (err) {
        console.error("Failed to load analytics:", err);
        setConnectionStatus(false);
    }
}

// ---------------------------------------------------------------
// Administration
// ---------------------------------------------------------------

async function loadAdminConfig() {
    try {
        const config = await fetchJson("/admin/config");
        setText("admin-min-nodes", config.min_nodes);
        setText("admin-total-nodes", config.total_nodes);
        setText("admin-idle-nodes", config.idle_nodes);
        setText("admin-pending-jobs", config.pending_jobs);
        setText("admin-subscribers", config.subscriber_count);
        setText("admin-event-count", config.event_count);
        setText("admin-uptime", formatUptime(config.uptime_seconds));
    } catch (err) {
        console.error("Failed to load admin config:", err);
        setConnectionStatus(false);
    }
}

async function loadAdminNodes() {
    const body = document.getElementById("admin-nodes-body");
    if (!body) return;
    try {
        const nodes = await fetchJson("/nodes");
        if (nodes.length === 0) {
            body.innerHTML = `<tr><td colspan="4" class="text-center text-secondary">No registered nodes.</td></tr>`;
            return;
        }
        let rows = "";
        for (const node of nodes) {
            rows += `
                <tr>
                    <td>${escapeHtml(node.node_id)}</td>
                    <td>${statusBadge(node.status)}</td>
                    <td>${escapeHtml(node.running_jobs)}</td>
                    <td>
                        <button class="btn btn-sm btn-outline-danger admin-deregister-btn" data-node-id="${escapeHtml(node.node_id)}">
                            Deregister
                        </button>
                    </td>
                </tr>
            `;
        }
        body.innerHTML = rows;

        document.querySelectorAll(".admin-deregister-btn").forEach(btn => {
            btn.addEventListener("click", async () => {
                btn.disabled = true;
                try {
                    await fetchJson(`/admin/nodes/${encodeURIComponent(btn.dataset.nodeId)}/deregister`, { method: "POST" });
                    await loadAdmin();
                } catch (err) {
                    console.error("Failed to deregister node:", err);
                    alert(err.message || "Failed to deregister node.");
                    btn.disabled = false;
                }
            });
        });
    } catch (err) {
        console.error("Failed to load admin node list:", err);
        setConnectionStatus(false);
    }
}

async function loadAdmin() {
    await Promise.all([loadAdminConfig(), loadAdminNodes()]);
}

async function triggerScale(direction) {
    try {
        await fetchJson(`/admin/scale-${direction}`, { method: "POST" });
        await refreshDashboard();
    } catch (err) {
        console.error(`Failed to scale ${direction}:`, err);
        alert(err.message || `Failed to scale ${direction}.`);
    }
}

// ---------------------------------------------------------------
// Global refresh loop
// ---------------------------------------------------------------

async function refreshDashboard() {
    loadActiveTab();
    setText("last-updated", new Date().toLocaleTimeString());
}

function updateClock() {
    const clock = document.getElementById("clock");
    if (clock) clock.textContent = new Date().toLocaleTimeString();
}

function setupControls() {
    const refreshBtn = document.getElementById("refresh-now-btn");
    if (refreshBtn) refreshBtn.addEventListener("click", refreshDashboard);

    const pauseBtn = document.getElementById("pause-btn");
    if (pauseBtn) {
        pauseBtn.addEventListener("click", () => {
            isPaused = !isPaused;
            pauseBtn.innerHTML = isPaused
                ? '<i class="bi bi-play-fill"></i>'
                : '<i class="bi bi-pause-fill"></i>';
            pauseBtn.title = isPaused ? "Resume live updates" : "Pause live updates";
        });
    }

    ["cc-scale-up-btn", "admin-scale-up-btn"].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) btn.addEventListener("click", () => triggerScale("up"));
    });
    ["cc-scale-down-btn", "admin-scale-down-btn"].forEach(id => {
        const btn = document.getElementById(id);
        if (btn) btn.addEventListener("click", () => triggerScale("down"));
    });
}

async function openHealthInspector() {
    try {
        const details = await fetchJson("/health/details");
        setText("drawer-title", "Health Inspector");
        const body = document.getElementById("drawer-body");
        body.innerHTML = Object.values(details.checks).map(c => `
            <div class="gcon-panel mb-2">
                <div class="gcon-panel-body">
                    <div class="d-flex justify-content-between">
                        <strong>${escapeHtml(c.label)}</strong>
                        <span class="badge ${c.healthy ? "bg-success" : "bg-danger"}">
                            ${c.healthy ? "Healthy" : "Unhealthy"}
                        </span>
                    </div>
                    <div class="text-secondary small mt-1">${escapeHtml(c.detail)}</div>
                </div>
            </div>
        `).join("");
        openDrawer();
    } catch (err) {
        console.error("Failed to load health details:", err);
    }
}

// ---------------------------------------------------------------
// Management: Users
// ---------------------------------------------------------------

let usersData = [];
let usersStatusFilter = "all";

async function loadUsersTab() {
    try {
        const [cards, users] = await Promise.all([
            fetchJson("/management/dashboard-cards"),
            fetchJson("/management/users"),
        ]);

        setText("uc-total-users", cards.total_users);
        setText("uc-active-users", cards.active_users);
        setText("uc-organizations", cards.organizations);
        setText("uc-api-keys", cards.api_keys);
        setText("uc-active-sessions", cards.active_sessions);
        setText("uc-total-workflows", cards.total_workflows);

        usersData = users;
        renderUsersTable();
    } catch (err) {
        console.error("Failed to load users tab:", err);
        setConnectionStatus(false);
    }
}

function renderUsersTable() {
    const body = document.getElementById("users-body");
    if (!body) return;

    const search = document.getElementById("users-search");
    const query = search ? search.value.toLowerCase() : "";

    let rows = usersData;
    if (usersStatusFilter !== "all") {
        rows = rows.filter(u => u.status === usersStatusFilter);
    }
    if (query) {
        rows = rows.filter(u =>
            u.name.toLowerCase().includes(query) ||
            u.email.toLowerCase().includes(query) ||
            u.role.toLowerCase().includes(query)
        );
    }

    if (rows.length === 0) {
        body.innerHTML = `<tr><td colspan="7" class="text-center text-secondary">No users found.</td></tr>`;
        return;
    }

    let html = "";
    for (const u of rows) {
        html += `
            <tr data-user-id="${escapeHtml(u.user_id)}">
                <td><span class="gcon-avatar">${escapeHtml(u.avatar_initials)}</span></td>
                <td class="gcon-row-link" data-open-user="${escapeHtml(u.user_id)}">${escapeHtml(u.name)}</td>
                <td>${escapeHtml(u.email)}</td>
                <td>${escapeHtml(u.role)}</td>
                <td>${statusBadge(u.status)}</td>
                <td>${escapeHtml(new Date(u.last_active).toLocaleString())}</td>
                <td>
                    <button class="btn btn-sm btn-outline-light user-view-btn" data-user-id="${escapeHtml(u.user_id)}">View</button>
                    <button class="btn btn-sm btn-outline-danger user-delete-btn" data-user-id="${escapeHtml(u.user_id)}">Delete</button>
                </td>
            </tr>
        `;
    }
    body.innerHTML = html;

    document.querySelectorAll(".gcon-row-link[data-open-user], .user-view-btn").forEach(el => {
        el.addEventListener("click", () => openUserDrawer(el.dataset.userId || el.dataset.openUser));
    });
    document.querySelectorAll(".user-delete-btn").forEach(btn => {
        btn.addEventListener("click", async () => {
            if (!confirm("Delete this user?")) return;
            try {
                await fetchJson(`/management/users/${btn.dataset.userId}`, { method: "DELETE" });
                await loadUsersTab();
            } catch (err) {
                console.error("Failed to delete user:", err);
                alert(err.message || "Failed to delete user.");
            }
        });
    });
}

function setupUsersTab() {
    const search = document.getElementById("users-search");
    if (search) search.addEventListener("input", renderUsersTable);

    document.querySelectorAll("#users-status-filters button").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll("#users-status-filters button").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            usersStatusFilter = btn.dataset.filter;
            renderUsersTable();
        });
    });

    const addBtn = document.getElementById("users-add-btn");
    if (addBtn) {
        addBtn.addEventListener("click", async () => {
            try {
                const orgs = await fetchJson("/management/organizations");
                const select = document.getElementById("add-user-org");
                select.innerHTML = `<option value="">No organization</option>` +
                    orgs.map(o => `<option value="${escapeHtml(o.org_id)}">${escapeHtml(o.name)}</option>`).join("");
            } catch (err) { /* non-fatal */ }
            new bootstrap.Modal(document.getElementById("addUserModal")).show();
        });
    }

    const submitBtn = document.getElementById("add-user-submit");
    if (submitBtn) {
        submitBtn.addEventListener("click", async () => {
            const name = document.getElementById("add-user-name").value.trim();
            const email = document.getElementById("add-user-email").value.trim();
            const role = document.getElementById("add-user-role").value;
            const organization_id = document.getElementById("add-user-org").value || null;
            const password = document.getElementById("add-user-password").value || null;
            if (!name || !email) return;

            try {
                await fetchJson("/management/users", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ name, email, role, organization_id, password }),
                });
                bootstrap.Modal.getInstance(document.getElementById("addUserModal")).hide();
                document.getElementById("add-user-name").value = "";
                document.getElementById("add-user-email").value = "";
                document.getElementById("add-user-password").value = "";
                await loadUsersTab();
            } catch (err) {
                console.error("Failed to create user:", err);
                alert(err.message || "Failed to create user.");
            }
        });
    }

    const exportCsv = document.getElementById("users-export-csv");
    if (exportCsv) exportCsv.addEventListener("click", () => window.open("/management/export/users?format=csv"));

    const exportJson = document.getElementById("users-export-json");
    if (exportJson) exportJson.addEventListener("click", () => window.open("/management/export/users?format=json"));
}

async function openUserDrawer(userId) {
    const user = usersData.find(u => u.user_id === userId) || await fetchJson(`/management/users/${userId}`);

    let auditEntries = [];
    try {
        const allAudit = await fetchJson("/management/audit-logs");
        auditEntries = allAudit.filter(a => a.actor === user.name || a.target === user.name);
    } catch (err) { /* non-fatal */ }

    let apiKeys = [];
    try {
        const allKeys = await fetchJson("/management/api-keys");
        apiKeys = allKeys.filter(k => k.owner_user_id === user.user_id);
    } catch (err) { /* non-fatal */ }

    const permissions = ROLE_PERMISSIONS_CACHE[user.role] || [];

    const body = document.getElementById("drawer-body");
    setText("drawer-title", user.name);

    body.innerHTML = `
        <div class="d-flex align-items-center gap-3 mb-3">
            <span class="gcon-avatar gcon-avatar-lg">${escapeHtml(user.avatar_initials)}</span>
            <div>
                <div class="fw-bold">${escapeHtml(user.name)}</div>
                <div class="text-secondary small">${escapeHtml(user.email)}</div>
                <div class="mt-1">${statusBadge(user.status)} <span class="badge bg-dark">${escapeHtml(user.role)}</span></div>
            </div>
        </div>

        <ul class="nav nav-pills gcon-drawer-tabs mb-3" id="user-drawer-tabs">
            <li class="nav-item"><a class="nav-link active" data-udtab="overview" href="#">Overview</a></li>
            <li class="nav-item"><a class="nav-link" data-udtab="jobs" href="#">Jobs</a></li>
            <li class="nav-item"><a class="nav-link" data-udtab="workflows" href="#">Workflows</a></li>
            <li class="nav-item"><a class="nav-link" data-udtab="activity" href="#">Activity</a></li>
            <li class="nav-item"><a class="nav-link" data-udtab="apikeys" href="#">API Keys</a></li>
            <li class="nav-item"><a class="nav-link" data-udtab="permissions" href="#">Permissions</a></li>
            <li class="nav-item"><a class="nav-link" data-udtab="settings" href="#">Settings</a></li>
        </ul>

        <div id="ud-overview" class="ud-pane">
            <div class="row gy-2">
                <div class="col-6"><small class="text-secondary">Login Count</small><div class="fw-bold">${user.stats.login_count}</div></div>
                <div class="col-6"><small class="text-secondary">API Requests</small><div class="fw-bold">${user.stats.api_requests}</div></div>
                <div class="col-6"><small class="text-secondary">CPU Usage</small><div class="fw-bold">${user.stats.cpu_usage}%</div></div>
                <div class="col-6"><small class="text-secondary">Storage Usage</small><div class="fw-bold">${user.stats.storage_usage_gb} GB</div></div>
                <div class="col-6"><small class="text-secondary">Member Since</small><div class="fw-bold">${new Date(user.created_at).toLocaleDateString()}</div></div>
                <div class="col-6"><small class="text-secondary">Last Active</small><div class="fw-bold">${new Date(user.last_active).toLocaleString()}</div></div>
            </div>
            <p class="text-secondary small mt-3 mb-0"><i class="bi bi-info-circle me-1"></i>Usage figures are illustrative demo data — GCON does not yet attribute real jobs to individual users.</p>
        </div>

        <div id="ud-jobs" class="ud-pane d-none">
            <div class="row gy-2">
                <div class="col-6"><small class="text-secondary">Submitted</small><div class="fw-bold">${user.stats.jobs_submitted}</div></div>
                <div class="col-6"><small class="text-secondary">Running</small><div class="fw-bold">${user.stats.jobs_running}</div></div>
                <div class="col-6"><small class="text-secondary">Completed</small><div class="fw-bold">${user.stats.jobs_completed}</div></div>
                <div class="col-6"><small class="text-secondary">Failed</small><div class="fw-bold">${user.stats.jobs_failed}</div></div>
            </div>
        </div>

        <div id="ud-workflows" class="ud-pane d-none">
            <div><small class="text-secondary">Workflows Created</small><div class="fw-bold">${user.stats.workflows_created}</div></div>
        </div>

        <div id="ud-activity" class="ud-pane d-none">
            ${auditEntries.length ? auditEntries.map(a => `
                <div class="gcon-activity-item">
                    <div class="gcon-activity-time">${new Date(a.timestamp).toLocaleTimeString()}</div>
                    <div class="gcon-activity-message">${escapeHtml(a.actor)} ${escapeHtml(a.action)}${a.target ? " — " + escapeHtml(a.target) : ""}</div>
                </div>
            `).join("") : `<div class="text-secondary">No recorded activity.</div>`}
        </div>

        <div id="ud-apikeys" class="ud-pane d-none">
            ${apiKeys.length ? apiKeys.map(k => `
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <span>${escapeHtml(k.name)}</span>
                    ${statusBadge(k.status)}
                </div>
            `).join("") : `<div class="text-secondary">No API keys owned by this user.</div>`}
        </div>

        <div id="ud-permissions" class="ud-pane d-none">
            ${permissions.length ? permissions.map(p => `<div><i class="bi bi-check-circle text-success me-2"></i>${escapeHtml(p)}</div>`).join("") : `<div class="text-secondary">No permissions.</div>`}
        </div>

        <div id="ud-settings" class="ud-pane d-none">
            <div class="mb-3">
                <label class="form-label">Status</label>
                <select class="form-select" id="ud-status-select">
                    ${["Active", "Pending", "Suspended", "Disabled"].map(s => `<option value="${s}" ${s === user.status ? "selected" : ""}>${s}</option>`).join("")}
                </select>
            </div>
            <div class="mb-3">
                <label class="form-label">Role</label>
                <select class="form-select" id="ud-role-select">
                    ${["Owner", "Administrator", "Operator", "Developer", "Viewer"].map(r => `<option value="${r}" ${r === user.role ? "selected" : ""}>${r}</option>`).join("")}
                </select>
            </div>
            <button class="btn btn-primary btn-sm" id="ud-save-btn">Save Changes</button>
        </div>
    `;

    document.querySelectorAll("#user-drawer-tabs a").forEach(el => {
        el.addEventListener("click", (e) => {
            e.preventDefault();
            document.querySelectorAll("#user-drawer-tabs a").forEach(a => a.classList.remove("active"));
            el.classList.add("active");
            document.querySelectorAll(".ud-pane").forEach(p => p.classList.add("d-none"));
            document.getElementById(`ud-${el.dataset.udtab}`).classList.remove("d-none");
        });
    });

    const saveBtn = document.getElementById("ud-save-btn");
    if (saveBtn) {
        saveBtn.addEventListener("click", async () => {
            const status = document.getElementById("ud-status-select").value;
            const role = document.getElementById("ud-role-select").value;
            try {
                await fetchJson(`/management/users/${user.user_id}`, {
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ status, role }),
                });
                closeDrawer();
                if (currentTab === "users") await loadUsersTab();
            } catch (err) {
                console.error("Failed to update user:", err);
                alert(err.message || "Failed to update user.");
            }
        });
    }

    openDrawer();
}

// ---------------------------------------------------------------
// Management: Organizations & Teams
// ---------------------------------------------------------------

async function loadOrganizationsTab() {
    const body = document.getElementById("organizations-body");
    if (!body) return;
    try {
        const orgs = await fetchJson("/management/organizations");
        if (orgs.length === 0) {
            body.innerHTML = `<tr><td colspan="5" class="text-center text-secondary">No organizations.</td></tr>`;
            return;
        }
        body.innerHTML = orgs.map(o => `
            <tr>
                <td>${escapeHtml(o.name)}</td>
                <td><span class="badge bg-dark">${escapeHtml(o.plan)}</span></td>
                <td>${escapeHtml(o.member_count)}</td>
                <td>${escapeHtml(o.team_count)}</td>
                <td>${escapeHtml(o.storage_used_gb)} GB</td>
            </tr>
        `).join("");
    } catch (err) {
        console.error("Failed to load organizations:", err);
        setConnectionStatus(false);
    }
}

async function loadTeamsTab() {
    const body = document.getElementById("teams-body");
    if (!body) return;
    try {
        const [teams, orgs, users] = await Promise.all([
            fetchJson("/management/teams"),
            fetchJson("/management/organizations"),
            fetchJson("/management/users"),
        ]);
        const orgName = {};
        orgs.forEach(o => orgName[o.org_id] = o.name);
        const userName = {};
        users.forEach(u => userName[u.user_id] = u.name);

        if (teams.length === 0) {
            body.innerHTML = `<tr><td colspan="4" class="text-center text-secondary">No teams.</td></tr>`;
            return;
        }
        body.innerHTML = teams.map(t => `
            <tr>
                <td>${escapeHtml(t.name)}</td>
                <td>${escapeHtml(orgName[t.org_id] || "-")}</td>
                <td>${escapeHtml(t.member_count)}</td>
                <td>${escapeHtml(t.admin_user_id ? (userName[t.admin_user_id] || t.admin_user_id) : "Unassigned")}</td>
            </tr>
        `).join("");
    } catch (err) {
        console.error("Failed to load teams:", err);
        setConnectionStatus(false);
    }
}

function setupOrganizationsTab() {
    const createBtn = document.getElementById("org-create-btn");
    if (!createBtn) return;
    createBtn.addEventListener("click", () => {
        new bootstrap.Modal(document.getElementById("createOrgModal")).show();
    });

    const submitBtn = document.getElementById("create-org-submit");
    if (submitBtn) {
        submitBtn.addEventListener("click", async () => {
            const name = document.getElementById("create-org-name").value.trim();
            const plan = document.getElementById("create-org-plan").value;
            if (!name) return;
            try {
                await fetchJson("/management/organizations", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ name, plan }),
                });
                bootstrap.Modal.getInstance(document.getElementById("createOrgModal")).hide();
                document.getElementById("create-org-name").value = "";
                await loadOrganizationsTab();
            } catch (err) {
                console.error("Failed to create organization:", err);
                alert(err.message || "Failed to create organization.");
            }
        });
    }
}

function setupTeamsTab() {
    const createBtn = document.getElementById("team-create-btn");
    if (!createBtn) return;
    createBtn.addEventListener("click", async () => {
        try {
            const [orgs, users] = await Promise.all([
                fetchJson("/management/organizations"),
                fetchJson("/management/users"),
            ]);
            document.getElementById("create-team-org").innerHTML =
                orgs.map(o => `<option value="${escapeHtml(o.org_id)}">${escapeHtml(o.name)}</option>`).join("")
                || `<option value="">No organizations yet</option>`;
            document.getElementById("create-team-admin").innerHTML =
                `<option value="">Unassigned</option>` +
                users.map(u => `<option value="${escapeHtml(u.user_id)}">${escapeHtml(u.name)}</option>`).join("");
        } catch (err) { /* non-fatal */ }
        new bootstrap.Modal(document.getElementById("createTeamModal")).show();
    });

    const submitBtn = document.getElementById("create-team-submit");
    if (submitBtn) {
        submitBtn.addEventListener("click", async () => {
            const errorBox = document.getElementById("create-team-error");
            errorBox.classList.add("d-none");

            const org_id = document.getElementById("create-team-org").value;
            const name = document.getElementById("create-team-name").value.trim();
            const admin_user_id = document.getElementById("create-team-admin").value || null;
            if (!org_id || !name) return;

            try {
                await fetchJson("/management/teams", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ org_id, name, admin_user_id }),
                });
                bootstrap.Modal.getInstance(document.getElementById("createTeamModal")).hide();
                document.getElementById("create-team-name").value = "";
                await loadTeamsTab();
            } catch (err) {
                errorBox.textContent = err.message || "Failed to create team.";
                errorBox.classList.remove("d-none");
            }
        });
    }
}
let liveSocket = null;

function connectLiveSocket() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    liveSocket = new WebSocket(`${proto}//${location.host}/ws`);

    liveSocket.onmessage = (event) => {
        if (isPaused) return;
        const data = JSON.parse(event.data);
        if (currentTab === "control-center") {
            renderFeed("activity-feed", data.events);
        }
        setConnectionStatus(true);
    };

    liveSocket.onclose = () => {
        setConnectionStatus(false);
        setTimeout(connectLiveSocket, 5000);
    };

    liveSocket.onerror = () => liveSocket.close();
}

connectLiveSocket();

// ---------------------------------------------------------------
// Management: API Keys
// ---------------------------------------------------------------

async function loadApiKeysTab() {
    const body = document.getElementById("apikeys-body");
    if (!body) return;
    try {
        const keys = await fetchJson("/management/api-keys");

        if (keys.length === 0) {
            body.innerHTML = `<tr><td colspan="7" class="text-center text-secondary">No API keys.</td></tr>`;
            return;
        }

        body.innerHTML = keys.map(k => `
            <tr>
                <td><code>${escapeHtml(k.secret)}</code><div class="text-secondary small">${escapeHtml(k.name)}</div></td>
                <td>${new Date(k.created_at).toLocaleDateString()}</td>
                <td>${k.expires_at ? new Date(k.expires_at).toLocaleDateString() : "Never"}</td>
                <td>${k.last_used_at ? new Date(k.last_used_at).toLocaleString() : "Never"}</td>
                <td>${(k.scopes || []).map(s => `<span class="badge bg-dark me-1">${escapeHtml(s)}</span>`).join("")}</td>
                <td>${statusBadge(k.status)}</td>
                <td>
                    <button class="btn btn-sm btn-outline-danger apikey-revoke-btn" data-key-id="${escapeHtml(k.key_id)}" ${k.status !== "Active" ? "disabled" : ""}>Revoke</button>
                    <button class="btn btn-sm btn-outline-light apikey-regen-btn" data-key-id="${escapeHtml(k.key_id)}">Regenerate</button>
                </td>
            </tr>
        `).join("");

        document.querySelectorAll(".apikey-revoke-btn").forEach(btn => {
            btn.addEventListener("click", async () => {
                try {
                    await fetchJson(`/management/api-keys/${btn.dataset.keyId}/revoke`, { method: "POST" });
                    await loadApiKeysTab();
                } catch (err) {
                    console.error("Failed to revoke API key:", err);
                    alert(err.message || "Failed to revoke API key.");
                }
            });
        });
        document.querySelectorAll(".apikey-regen-btn").forEach(btn => {
            btn.addEventListener("click", async () => {
                try {
                    const result = await fetchJson(`/management/api-keys/${btn.dataset.keyId}/regenerate`, { method: "POST" });
                    showKeyReveal(result.secret);
                    await loadApiKeysTab();
                } catch (err) {
                    console.error("Failed to regenerate API key:", err);
                    alert(err.message || "Failed to regenerate API key.");
                }
            });
        });
    } catch (err) {
        console.error("Failed to load API keys:", err);
        setConnectionStatus(false);
    }
}

function showKeyReveal(secret) {
    const reveal = document.getElementById("apikeys-reveal");
    if (!reveal) return;
    reveal.classList.remove("d-none");
    reveal.innerHTML = `<i class="bi bi-exclamation-triangle me-2"></i>Copy this secret now — it won't be shown again: <code>${escapeHtml(secret)}</code>`;
}

async function setupApiKeysTab() {
    const createBtn = document.getElementById("apikeys-create-btn");
    if (createBtn) {
        createBtn.addEventListener("click", async () => {
            try {
                const users = await fetchJson("/management/users");
                const select = document.getElementById("create-key-owner");
                select.innerHTML = users.map(u => `<option value="${escapeHtml(u.user_id)}">${escapeHtml(u.name)}</option>`).join("");
            } catch (err) { /* non-fatal */ }
            new bootstrap.Modal(document.getElementById("createKeyModal")).show();
        });
    }

    const submitBtn = document.getElementById("create-key-submit");
    if (submitBtn) {
        submitBtn.addEventListener("click", async () => {
            const name = document.getElementById("create-key-name").value.trim();
            const owner = document.getElementById("create-key-owner").value;
            const expires = parseInt(document.getElementById("create-key-expires").value, 10) || 0;
            if (!name || !owner) return;

            try {
                const result = await fetchJson("/management/api-keys", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ name, owner_user_id: owner, expires_in_days: expires || null }),
                });
                bootstrap.Modal.getInstance(document.getElementById("createKeyModal")).hide();
                document.getElementById("create-key-name").value = "";
                showKeyReveal(result.secret);
                await loadApiKeysTab();
            } catch (err) {
                console.error("Failed to create API key:", err);
                alert(err.message || "Failed to create API key.");
            }
        });
    }
}

// ---------------------------------------------------------------
// Management: Permissions
// ---------------------------------------------------------------

let ROLE_PERMISSIONS_CACHE = {};

async function loadPermissionsTab() {
    const thead = document.getElementById("permissions-thead");
    const body = document.getElementById("permissions-body");
    if (!thead || !body) return;

    try {
        const matrix = await fetchJson("/management/permission-matrix");
        const allPerms = await fetchJson("/management/permissions");

        ROLE_PERMISSIONS_CACHE = {};
        matrix.forEach(row => {
            ROLE_PERMISSIONS_CACHE[row.role] = Object.keys(row.permissions).filter(p => row.permissions[p]);
        });

        thead.innerHTML = `<tr><th>Role</th>${allPerms.map(p => `<th>${escapeHtml(p)}</th>`).join("")}</tr>`;

        body.innerHTML = matrix.map(row => `
            <tr>
                <td class="fw-bold">${escapeHtml(row.role)}</td>
                ${allPerms.map(p => `<td>${row.permissions[p] ? '<i class="bi bi-check-circle-fill text-success"></i>' : '<i class="bi bi-dash text-secondary"></i>'}</td>`).join("")}
            </tr>
        `).join("");
    } catch (err) {
        console.error("Failed to load permissions:", err);
        setConnectionStatus(false);
    }
}

// ---------------------------------------------------------------
// Management: Audit Logs & Notifications
// ---------------------------------------------------------------

async function loadAuditLogsTab() {
    const feed = document.getElementById("audit-log-feed");
    if (!feed) return;
    try {
        const entries = await fetchJson("/management/audit-logs");
        if (entries.length === 0) {
            feed.innerHTML = `<div class="text-secondary">No audit entries.</div>`;
            return;
        }
        feed.innerHTML = entries.map(e => `
            <div class="gcon-activity-item">
                <div class="gcon-activity-time">${new Date(e.timestamp).toLocaleString()}</div>
                <div class="gcon-activity-message">${escapeHtml(e.actor)} ${escapeHtml(e.action)}${e.target ? " — <strong>" + escapeHtml(e.target) + "</strong>" : ""}</div>
            </div>
        `).join("");
    } catch (err) {
        console.error("Failed to load audit logs:", err);
        setConnectionStatus(false);
    }
}

const NOTIF_ICONS = {
    user_registered: "bi-person-plus", invitation_accepted: "bi-envelope-check",
    password_changed: "bi-shield-lock", api_key_created: "bi-key",
    workflow_completed: "bi-check-circle", node_failure: "bi-exclamation-triangle",
    storage_warning: "bi-hdd",
};

async function loadNotificationsTab() {
    const list = document.getElementById("notifications-list");
    try {
        const entries = await fetchJson("/management/notifications");
        await refreshNotifBadge();

        if (!list) return;
        if (entries.length === 0) {
            list.innerHTML = `<div class="text-secondary">No notifications.</div>`;
            return;
        }
        list.innerHTML = entries.map(n => `
            <div class="gcon-activity-item ${n.read ? "" : "gcon-notif-unread"}">
                <div class="gcon-activity-time"><i class="bi ${NOTIF_ICONS[n.type] || "bi-bell"} me-1"></i>${new Date(n.timestamp).toLocaleTimeString()}</div>
                <div class="gcon-activity-message">${escapeHtml(n.message)}</div>
            </div>
        `).join("");
    } catch (err) {
        console.error("Failed to load notifications:", err);
        setConnectionStatus(false);
    }
}

async function refreshNotifBadge() {
    try {
        const entries = await fetchJson("/management/notifications");
        const unread = entries.filter(n => !n.read).length;
        const badge = document.getElementById("notif-count-badge");
        if (badge) badge.textContent = unread > 0 ? unread : "";
    } catch (err) { /* non-fatal */ }
}

// ---------------------------------------------------------------
// Global search
// ---------------------------------------------------------------

function setupGlobalSearch() {
    const input = document.getElementById("global-search");
    const results = document.getElementById("global-search-results");
    if (!input || !results) return;

    let debounceTimer;
    input.addEventListener("input", () => {
        clearTimeout(debounceTimer);
        const query = input.value.trim();
        if (!query) {
            results.classList.add("d-none");
            return;
        }
        debounceTimer = setTimeout(async () => {
            try {
                const data = await fetchJson(`/management/search?q=${encodeURIComponent(query)}`);
                renderSearchResults(data);
            } catch (err) {
                console.error("Search failed:", err);
            }
        }, 250);
    });

    document.addEventListener("click", (e) => {
        if (!results.contains(e.target) && e.target !== input) {
            results.classList.add("d-none");
        }
    });
}

function renderSearchResults(data) {
    const results = document.getElementById("global-search-results");
    if (!results) return;

    const sections = [
        ["Users", data.users, u => `${u.name} · ${u.email}`],
        ["Organizations", data.organizations, o => o.name],
        ["API Keys", data.api_keys, k => k.name],
        ["Jobs", data.jobs, j => `${j.job_id} · ${j.status}`],
        ["Nodes", data.nodes, n => `${n.node_id} · ${n.status}`],
    ];

    let html = "";
    let hasAny = false;
    for (const [label, items, formatter] of sections) {
        if (!items || items.length === 0) continue;
        hasAny = true;
        html += `<div class="gcon-search-group-label">${label}</div>`;
        for (const item of items.slice(0, 5)) {
            html += `<div class="gcon-search-result-item">${escapeHtml(formatter(item))}</div>`;
        }
    }

    results.innerHTML = hasAny ? html : `<div class="gcon-search-result-item text-secondary">No matches.</div>`;
    results.classList.remove("d-none");
}

// ---------------------------------------------------------------
// Detail drawer (shared)
// ---------------------------------------------------------------

function openDrawer() {
    document.getElementById("detail-drawer").classList.add("gcon-drawer-open");
    document.getElementById("drawer-backdrop").classList.remove("d-none");
}

function closeDrawer() {
    document.getElementById("detail-drawer").classList.remove("gcon-drawer-open");
    document.getElementById("drawer-backdrop").classList.add("d-none");
}

function setupDrawer() {
    const closeBtn = document.getElementById("drawer-close-btn");
    if (closeBtn) closeBtn.addEventListener("click", closeDrawer);

    const backdrop = document.getElementById("drawer-backdrop");
    if (backdrop) backdrop.addEventListener("click", closeDrawer);
}

// ---------------------------------------------------------------
// Cluster health badge (navbar, always live)
// ---------------------------------------------------------------

async function loadHealthBadge() {
    try {
        const health = await fetchJson("/health");
        const badge = document.getElementById("cluster-health-badge");
        if (!badge) return;
        const classMap = { healthy: "bg-success", degraded: "bg-warning text-dark", critical: "bg-danger" };
        badge.className = `badge ${classMap[health.state] || "bg-secondary"} me-3`;
        badge.textContent = `● ${health.state.charAt(0).toUpperCase() + health.state.slice(1)}`;
        badge.title = health.reason;
    } catch (err) {
        setConnectionStatus(false);
    }
}

// ---------------------------------------------------------------
// Current user / logout / change password
// ---------------------------------------------------------------

async function loadCurrentUser() {
    try {
        const user = await fetchJson("/auth/me");
        setText("navbar-user-avatar", user.avatar_initials);
        setText("navbar-user-name", user.name);
        setText("navbar-user-role", `${user.role} · ${user.email}`);
    } catch (err) {
        // fetchJson already redirects to /login on 401
        console.error("Failed to load current user:", err);
    }
}

function setupAuthMenu() {
    const logoutLink = document.getElementById("logout-link");
    if (logoutLink) {
        logoutLink.addEventListener("click", async (e) => {
            e.preventDefault();
            try {
                await fetch("/auth/logout", { method: "POST" });
            } finally {
                window.location.href = "/login";
            }
        });
    }

    const changePasswordLink = document.getElementById("change-password-link");
    if (changePasswordLink) {
        changePasswordLink.addEventListener("click", (e) => {
            e.preventDefault();
            document.getElementById("change-password-error").classList.add("d-none");
            document.getElementById("cp-current-password").value = "";
            document.getElementById("cp-new-password").value = "";
            new bootstrap.Modal(document.getElementById("changePasswordModal")).show();
        });
    }

    const submitBtn = document.getElementById("change-password-submit");
    if (submitBtn) {
        submitBtn.addEventListener("click", async () => {
            const errorBox = document.getElementById("change-password-error");
            errorBox.classList.add("d-none");

            const current_password = document.getElementById("cp-current-password").value;
            const new_password = document.getElementById("cp-new-password").value;

            try {
                await fetchJson("/auth/change-password", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ current_password, new_password }),
                });
                bootstrap.Modal.getInstance(document.getElementById("changePasswordModal")).hide();
            } catch (err) {
                errorBox.textContent = err.message || "Failed to change password.";
                errorBox.classList.remove("d-none");
            }
        });
    }
}



// ---------------------------------------------------------------
// Boot
// ---------------------------------------------------------------

setupTabNav();
setupExplorerNav();
setupControls();
setupUsersTab();
setupApiKeysTab();
setupOrganizationsTab();
setupTeamsTab();
setupGlobalSearch();
setupDrawer();
setupAuthMenu();

loadCurrentUser();
refreshDashboard();
loadHealthBadge();
refreshNotifBadge();
updateClock();

setInterval(() => { if (!isPaused) refreshDashboard(); }, REFRESH_INTERVAL_MS);
setInterval(loadHealthBadge, REFRESH_INTERVAL_MS);
setInterval(refreshNotifBadge, REFRESH_INTERVAL_MS);
setInterval(updateClock, 1000);
