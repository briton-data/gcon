/**
 * GCON Dashboard — live client-side controller.
 *
 * Drives all dashboard tabs (Control Center, Topology, Explorer,
 * Monitoring, Analytics, Admin) via polling + in-place DOM updates,
 * so the dashboard stays live without full page reloads.
 */

const REFRESH_INTERVAL_MS = 5000;

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
    };
    const labelMap = {
        healthy: "Healthy", idle: "Idle", offline: "Idle", running: "Running",
        completed: "Completed", failed: "Failed", pending: "Pending", verified: "Verified",
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
    if (!response.ok) throw new Error(`${url} returned ${response.status}`);
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
};

function switchTab(tab) {
    currentTab = tab;

    document.querySelectorAll(".gcon-tab").forEach(el => el.classList.add("d-none"));
    const target = document.getElementById(`tab-${tab}`);
    if (target) target.classList.remove("d-none");

    document.querySelectorAll("#tab-nav a, #tab-nav-top a").forEach(el => {
        el.classList.toggle("active", el.dataset.tab === tab);
    });

    setText("tab-title", TAB_TITLES[tab] || tab);
    loadActiveTab();
}

function setupTabNav() {
    document.querySelectorAll("#tab-nav a, #tab-nav-top a").forEach(el => {
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
    await Promise.all([loadCluster(), loadNodes(), loadJobs(), loadEvents()]);
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

// ---------------------------------------------------------------
// Boot
// ---------------------------------------------------------------

setupTabNav();
setupExplorerNav();
setupControls();

refreshDashboard();
updateClock();

setInterval(() => { if (!isPaused) refreshDashboard(); }, REFRESH_INTERVAL_MS);
setInterval(updateClock, 1000);
