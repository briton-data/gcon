/**
 * GCON Dashboard — live client-side controller.
 *
 * Drives all dashboard tabs (Control Center, Topology, Explorer,
 * Monitoring, Analytics, Admin) via polling + in-place DOM updates,
 * so the dashboard stays live without full page reloads.
 */



let currentTab = "control-center";
let explorerView = "nodes";
let explorerData = [];
let isPaused = false;
// How often the dashboard re-polls REST endpoints (cluster state,
// nodes, jobs, health badge, notifications) when not paused. The
// /ws socket already pushes live activity-feed events every 2s
// independently of this.
const REFRESH_INTERVAL_MS = 5000;

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

function showToast(message, isError) {
    const stack = document.getElementById("gcon-toast-stack");
    if (!stack) return;

    const toast = document.createElement("div");
    toast.className = `gcon-toast ${isError ? "error" : "success"}`;
    toast.innerHTML = `
        <i class="bi ${isError ? "bi-exclamation-triangle-fill" : "bi-check-circle-fill"}"></i>
        <span>${escapeHtml(message)}</span>
    `;
    stack.appendChild(toast);

    setTimeout(() => toast.classList.add("show"), 10);
    setTimeout(() => {
        toast.classList.remove("show");
        setTimeout(() => toast.remove(), 250);
    }, isError ? 6000 : 3500);
}

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
        healthy: "bg-success", idle: "bg-secondary", offline: "bg-danger",
        running: "bg-primary", completed: "bg-success", failed: "bg-danger",
        pending: "bg-warning text-dark", verified: "bg-success",
        active: "bg-success", suspended: "bg-warning text-dark",
        disabled: "bg-secondary", revoked: "bg-danger", expired: "bg-secondary",
    };
    const labelMap = {
        healthy: "Healthy", idle: "Idle", offline: "Offline", running: "Running",
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

function formatBytes(bytes) {
    if (bytes === undefined || bytes === null || isNaN(bytes)) return "--";
    if (bytes === 0) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
    return `${(bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatAge(seconds) {
    if (seconds === undefined || seconds === null) return "--";
    if (seconds < 60) return `${Math.round(seconds)}s`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
    return `${Math.round(seconds / 3600)}h`;
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
    "executions": "Executions",
    "topology": "Cluster Visualization",
    "receipts": "Receipts",
    "trust-center": "Trust Center",
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

    document.querySelectorAll("#tab-nav a, #tab-nav-mgmt a").forEach(el => {
        el.classList.toggle("active", el.dataset.tab === tab);
    });

    setText("tab-title", TAB_TITLES[tab] || tab);
    loadActiveTab();
}

function setupTabNav() {
    document.querySelectorAll("#tab-nav a, #tab-nav-mgmt a").forEach(el => {
        el.addEventListener("click", (e) => {
            e.preventDefault();
            switchTab(el.dataset.tab);
        });
    });
}

function loadActiveTab() {
    if (currentTab === "control-center") loadControlCenter();
    else if (currentTab === "executions") loadExecutionsTab();
    else if (currentTab === "topology") loadTopology();
    else if (currentTab === "receipts") loadReceiptsTab();
    else if (currentTab === "trust-center") loadTrustCenter();
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
        const [health, trust] = await Promise.all([
            fetchJson("/health"),
            fetchJson("/trust-score"),
        ]);
        renderTrustHealth(health, trust);
    } catch (err) {
        console.error("Failed to load cluster health:", err);
        setConnectionStatus(false);
    }
}

// Single render path for the Trust & Health panel — called both from
// loadClusterHealth() (fetch, used on tab switch / initial load) and
// from renderHomeDashboard() (the bootstrap payload + every /ws tick),
// so the panel never has two different code paths deciding what it
// shows.
function renderTrustHealth(health, trust, globalStatus) {
    if (!health) return;

    const stateClassMap = {
        healthy: "badge bg-success",
        degraded: "badge bg-warning text-dark",
        critical: "badge bg-danger",
    };
    const badge = document.getElementById("health-state-badge");
    if (badge) {
        badge.className = stateClassMap[health.state] || "badge bg-secondary";
        badge.textContent = health.state ? health.state.charAt(0).toUpperCase() + health.state.slice(1) : "--";
    }
    setText("health-reason", health.reason);

    if (trust) {
        const gauge = document.getElementById("trust-score-gauge");
        if (gauge) {
            const pct = trust.trust_score ?? 0;
            gauge.style.setProperty("--gauge-pct", pct);
            gauge.style.setProperty(
                "--gauge-color",
                pct >= 90 ? "var(--success)" : pct >= 70 ? "var(--warning)" : "var(--danger)"
            );
        }
        setText("trust-score-value", `${trust.trust_score ?? "--"}%`);
        setText("trust-verification-rate", `Verification ${trust.verification_rate ?? "--"}%`);
        setText("trust-node-rate", `Node trust ${trust.node_trust_rate ?? "--"}%`);
    }

    const grid = document.getElementById("health-branch-grid");
    if (grid && health.checks) {
        grid.innerHTML = Object.values(health.checks).map(c => `
            <div class="gcon-health-branch ${c.healthy ? "" : "unhealthy"}">
                <span class="dot"></span>
                <div>
                    <div class="label">${escapeHtml(c.label)}</div>
                    <div class="detail">${escapeHtml(c.detail)}</div>
                </div>
            </div>
        `).join("");
    }

    const heartbeatEl = document.getElementById("health-heartbeat-status");
    if (heartbeatEl && globalStatus) {
        const age = globalStatus.heartbeat_age_seconds;
        heartbeatEl.classList.remove("ok", "warn", "bad");
        heartbeatEl.classList.add(age === null || age === undefined ? "warn" : age < 30 ? "ok" : age < 120 ? "warn" : "bad");
        setText("health-heartbeat-age", formatAge(age));
    }

    const issueEl = document.getElementById("health-last-issue");
    if (issueEl) {
        issueEl.classList.remove("ok", "bad");
        if (health.last_issue) {
            issueEl.classList.add("bad");
            issueEl.innerHTML = `<i class="bi bi-exclamation-triangle"></i> ${escapeHtml(health.last_issue.label)}: ${escapeHtml(health.last_issue.detail)}`;
        } else {
            issueEl.classList.add("ok");
            issueEl.innerHTML = `<i class="bi bi-check-circle"></i> No issues detected`;
        }
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

function nodeActionButtons(node) {
    return `
        <div class="btn-group">
            <button class="btn btn-sm btn-outline-warning gcon-node-action-btn"
                data-action="drain"
                data-node-id="${escapeHtml(node.node_id)}"
                title="Drain">
                <i class="bi bi-sign-turn-slight-right"></i>
            </button>

            <button class="btn btn-sm btn-outline-info gcon-node-action-btn"
                data-action="restart"
                data-node-id="${escapeHtml(node.node_id)}"
                title="Restart">
                <i class="bi bi-arrow-repeat"></i>
            </button>

            <button class="btn btn-sm btn-outline-danger gcon-node-action-btn"
                data-action="stop"
                data-node-id="${escapeHtml(node.node_id)}"
                title="Stop">
                <i class="bi bi-stop-circle"></i>
            </button>
        </div>
    `;
}

function bindNodeActionButtons() {
    document.querySelectorAll(".gcon-node-action-btn").forEach(btn => {

        btn.addEventListener("click", async () => {

            const action = btn.dataset.action;
            const nodeId = btn.dataset.nodeId;

            if (
                action === "stop" &&
                !confirm(`Stop and remove node ${nodeId} from the cluster?`)
            ) {
                return;
            }

            btn.disabled = true;

            try {

                await fetchJson(
                    `/cluster/nodes/${encodeURIComponent(nodeId)}/${action}`,
                    {
                        method: "POST",
                    }
                );

                await refreshDashboard();

            } catch (err) {

                console.error(`Failed to ${action} node:`, err);

                showToast(err.message || `Failed to ${action} node.`, true);

                btn.disabled = false;

            }

        });

    });
}

async function loadNodes() {

    const body = document.getElementById("nodes-body");

    try {

        const nodes = await fetchJson("/nodes");

        if (!body) {
            return nodes;
        }

        if (nodes.length === 0) {

            body.innerHTML =
                `<tr><td colspan="7" class="text-center text-secondary">No registered nodes.</td></tr>`;

        } else {

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
                        <td>${nodeActionButtons(node)}</td>
                    </tr>
                `;

            }

            body.innerHTML = rows;

            bindNodeActionButtons();

        }

        return nodes;

    } catch (err) {

        console.error("Failed to load nodes:", err);

        setConnectionStatus(false);

        return [];

    }

}

function jobActionCell(job) {

    if (job.status !== "running") {
        return `<span class="text-secondary">&mdash;</span>`;
    }

    return `
        <button class="btn btn-sm btn-outline-danger gcon-job-cancel-btn"
            data-job-id="${escapeHtml(job.job_id)}">
            <i class="bi bi-x-circle me-1"></i>
            Cancel
        </button>
    `;
}

function bindJobActionButtons() {

    document.querySelectorAll(".gcon-job-cancel-btn").forEach(btn => {

        btn.addEventListener("click", async () => {

            const jobId = btn.dataset.jobId;

            if (!confirm(`Cancel running job ${jobId}?`)) {
                return;
            }

            btn.disabled = true;

            try {

                await fetchJson(
                    `/jobs/${encodeURIComponent(jobId)}/cancel`,
                    {
                        method: "POST",
                    }
                );

                await refreshDashboard();

            } catch (err) {

                console.error("Failed to cancel job:", err);

                showToast(err.message || "Failed to cancel job.", true);

                btn.disabled = false;

            }

        });

    });

}


async function loadJobs() {

    const body = document.getElementById("jobs-body");

    try {

        const jobs = await fetchJson("/jobs");

        if (!body) {
            return jobs;
        }

        if (jobs.length === 0) {

            body.innerHTML =
                `<tr><td colspan="7" class="text-center text-secondary">No jobs submitted.</td></tr>`;

        } else {

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
                        <td>${jobActionCell(job)}</td>
                    </tr>
                `;

            }

            body.innerHTML = rows;

            bindJobActionButtons();

        }

        return jobs;

    } catch (err) {

        console.error("Failed to load jobs:", err);

        setConnectionStatus(false);

        return [];

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

    const [, nodes, jobs] = await Promise.all([
        loadCluster(),
        loadNodes(),
        loadJobs(),
        loadClusterHealth(),
        loadEvents(),
        loadTopologyMini()
    ]);

    populateOperationsSelectors(nodes, jobs);

}

// ---------------------------------------------------------------
// Home Dashboard widgets — Global Status, Alerts, Summaries,
// Execution Timeline, mini Topology. All driven by the same
// payload shape the server embeds on first paint (#dashboard-
// bootstrap-data) and the /ws socket pushes every 2s, so there is
// exactly one render path regardless of data source.
// ---------------------------------------------------------------

async function loadTopologyMini() {
    const container = document.getElementById("topology-mini-container");
    if (!container) return;

    try {
        const topo = await fetchJson("/topology");
        container.innerHTML = buildTopologySvg(topo);
    } catch (err) {
        console.error("Failed to load mini topology:", err);
    }
}

function renderGlobalStatus(status) {
    if (!status) return;

    const pill = (id, healthy, label) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.remove("ok", "warn", "bad");
        el.classList.add(healthy ? "ok" : "bad");
        el.innerHTML = `<span class="dot"></span>${label}`;
    };

    pill("status-pill-coordinator", status.coordinator_online, "Coordinator");
    pill("status-pill-scheduler", status.scheduler_running, "Scheduler");
    pill("status-pill-storage", status.storage_online, "Storage");
    pill("status-pill-receipts", status.receipt_engine_online, "Receipts");

    const heartbeatEl = document.getElementById("status-pill-heartbeat");
    if (heartbeatEl) {
        const age = status.heartbeat_age_seconds;
        heartbeatEl.classList.remove("ok", "warn", "bad");
        heartbeatEl.classList.add(age === null ? "warn" : age < 30 ? "ok" : age < 120 ? "warn" : "bad");
        heartbeatEl.innerHTML = `<span class="dot"></span>Heartbeat ${formatAge(age)}`;
    }

    const coordEl = document.getElementById("overview-coordinator-id");
    if (coordEl && status.coordinator_id) {
        coordEl.textContent = status.coordinator_id;
        coordEl.title = status.coordinator_id;
    }
}

function renderCriticalAlerts(alerts) {
    alerts = alerts || [];

    const panel = document.getElementById("critical-alerts-panel");
    const body = document.getElementById("critical-alerts-body");
    const count = document.getElementById("critical-alerts-count");
    if (!body || !count || !panel) return;

    count.textContent = alerts.length;
    count.className = `badge ${alerts.length ? "bg-danger" : "bg-success"}`;
    panel.classList.toggle("has-alerts", alerts.length > 0);

    if (alerts.length === 0) {
        body.innerHTML = `
            <div class="gcon-alerts-empty">
                <i class="bi bi-check-circle-fill"></i>
                No critical alerts. Every monitored system is within normal range.
            </div>`;
        return;
    }

    body.innerHTML = alerts.map(a => `
        <div class="gcon-alert-item ${escapeHtml(a.severity)}">
            <i class="bi bi-exclamation-triangle-fill gcon-alert-icon"></i>
            <div>
                <div class="gcon-alert-source">${escapeHtml(a.source)}</div>
                <div class="gcon-alert-message">${escapeHtml(a.message)}</div>
            </div>
        </div>
    `).join("");
}

function renderNodeSummary(summary) {
    if (!summary) return;
    const total = summary.total || 0;
    const pct = (n) => total ? (n / total * 100).toFixed(1) : 0;

    setText("node-summary-total", total);

    const bar = document.getElementById("node-summary-bar");
    if (bar) {
        bar.innerHTML = `
            <div class="seg" style="width:${pct(summary.idle)}%; background: var(--success);"></div>
            <div class="seg" style="width:${pct(summary.busy)}%; background: var(--primary);"></div>
            <div class="seg" style="width:${pct(summary.offline)}%; background: var(--danger);"></div>
        `;
    }

    const legend = document.getElementById("node-summary-legend");
    if (legend) {
        legend.innerHTML = `
            <span class="item"><span class="swatch" style="background:var(--success);"></span>Idle ${summary.idle}</span>
            <span class="item"><span class="swatch" style="background:var(--primary);"></span>Busy ${summary.busy}</span>
            <span class="item"><span class="swatch" style="background:var(--danger);"></span>Offline ${summary.offline}</span>
            <span class="item"><span class="swatch" style="background:var(--warning);"></span>Draining ${summary.draining}</span>
        `;
    }
}

function renderReceiptsSummary(summary) {
    if (!summary) return;
    const total = summary.total || 0;
    const pct = (n) => total ? (n / total * 100).toFixed(1) : 0;

    setText("receipt-summary-total", total);

    const bar = document.getElementById("receipt-summary-bar");
    if (bar) {
        bar.innerHTML = `
            <div class="seg" style="width:${pct(summary.verified)}%; background: var(--success);"></div>
            <div class="seg" style="width:${pct(summary.unverified)}%; background: var(--danger);"></div>
        `;
    }

    const legend = document.getElementById("receipt-summary-legend");
    if (legend) {
        legend.innerHTML = `
            <span class="item"><span class="swatch" style="background:var(--success);"></span>Verified ${summary.verified}</span>
            <span class="item"><span class="swatch" style="background:var(--danger);"></span>Unverified ${summary.unverified}</span>
        `;
    }
}

function renderStorageSummary(summary) {
    if (!summary) return;

    setText("storage-summary-artifacts", summary.artifact_count);
    setText("storage-summary-bytes", formatBytes(summary.artifacts_total_bytes));

    const usedPct = Math.max(0, 100 - (summary.disk_remaining_pct || 0));
    const seg = document.getElementById("storage-summary-used-seg");
    if (seg) seg.style.width = `${usedPct.toFixed(1)}%`;

    setText("storage-summary-disk-pct", `${summary.disk_remaining_pct ?? "--"}%`);
}

function renderExecutionTimeline(jobs) {
    const list = document.getElementById("execution-timeline-list");
    if (!list) return;

    jobs = jobs || [];

    if (jobs.length === 0) {
        list.innerHTML = `<div class="text-secondary small">No executions submitted yet.</div>`;
        return;
    }

    list.innerHTML = jobs.map(job => `
        <div class="gcon-timeline-item">
            <span class="gcon-timeline-dot ${escapeHtml(job.status)}"></span>
            <span class="gcon-timeline-id">${escapeHtml(job.job_id)}</span>
            ${statusBadge(job.status)}
            <span class="gcon-timeline-meta">${escapeHtml(job.node_id || "unassigned")} &middot; ${escapeHtml(job.created_at || "-")}</span>
        </div>
    `).join("");
}

function renderHero(hero) {
    if (!hero) return;
    setText("hero-connected-nodes", hero.connected_nodes);
    setText("hero-total-nodes", hero.total_nodes);
    setText("hero-running-executions", hero.running_executions);
    setText("hero-verified-receipts", hero.verified_receipts);
    setText("hero-total-receipts", hero.total_receipts);
    setText("hero-trust-score", hero.trust_score);
    setText("hero-coordinator-id", hero.coordinator_id);

    const status = document.getElementById("hero-coordinator-status");
    if (status) status.classList.toggle("offline", !hero.coordinator_online);
}

function renderHomeDashboard(data) {
    if (!data) return;
    renderHero(data.hero);
    renderGlobalStatus(data.global_status);
    renderTrustHealth(data.health, data.trust, data.global_status);
    renderCriticalAlerts(data.critical_alerts);
    renderNodeSummary(data.node_summary);
    renderReceiptsSummary(data.receipts_summary);
    renderStorageSummary(data.storage_summary);
    renderExecutionTimeline(data.execution_timeline);
}

// ---------------------------------------------------------------
// Trust Center
// ---------------------------------------------------------------

function renderTrustCenter(data) {
    if (!data) return;
    const trust = data.trust || {};

    setText("tc-trust-score", `${trust.trust_score ?? "--"}%`);
    setText("tc-verification-rate", `${trust.verification_rate ?? "--"}%`);
    setText("tc-node-trust-rate", `${trust.node_trust_rate ?? "--"}%`);
    setText("tc-verification-failures", (data.verification_failures || []).length);

    // Trust score history — simple bar strip, one bar per sample.
    const chart = document.getElementById("trust-history-chart");
    if (chart) {
        const history = data.history || [];
        if (history.length === 0) {
            chart.innerHTML = `<div class="text-secondary text-center py-4 w-100">Collecting samples…</div>`;
        } else {
            chart.innerHTML = history.map(h => `
                <div class="bar" style="height:${Math.max(2, h.score)}%"
                     title="${escapeHtml(h.score)}% at ${new Date(h.timestamp).toLocaleTimeString()}"></div>
            `).join("");
        }
    }

    // Signature validation summary
    const sigEl = document.getElementById("tc-signature-summary");
    if (sigEl) {
        const summary = data.receipts_summary || {};
        sigEl.innerHTML = `
            <div class="gcon-stat-list">
                <div class="cell"><div class="text-secondary small">Total Receipts</div><div class="fw-bold">${summary.total ?? 0}</div></div>
                <div class="cell"><div class="text-secondary small">Verified</div><div class="fw-bold text-success">${summary.verified ?? 0}</div></div>
                <div class="cell"><div class="text-secondary small">Unverified</div><div class="fw-bold text-danger">${summary.unverified ?? 0}</div></div>
                <div class="cell"><div class="text-secondary small">Verification Rate</div><div class="fw-bold">${trust.verification_rate ?? "--"}%</div></div>
            </div>
        `;
    }

    // Verification failures
    const failuresList = document.getElementById("tc-failures-list");
    const failuresCount = document.getElementById("tc-failures-count");
    if (failuresCount) failuresCount.textContent = (data.verification_failures || []).length;
    if (failuresList) {
        const failures = data.verification_failures || [];
        failuresList.innerHTML = failures.length === 0
            ? `<div class="text-secondary text-center py-3"><i class="bi bi-check-circle-fill text-success me-1"></i>No verification failures.</div>`
            : failures.map(r => `
                <div class="gcon-activity-item">
                    <div class="gcon-activity-time"><i class="bi bi-shield-x text-danger me-1"></i>${escapeHtml(r.receipt_id)}</div>
                    <div class="gcon-activity-message">Job ${escapeHtml(r.job_id)} on ${escapeHtml(r.node_id || "unassigned")}</div>
                </div>
            `).join("");
    }

    // Node trust status
    const nodeTrustBody = document.getElementById("tc-node-trust-body");
    if (nodeTrustBody) {
        const nodes = data.node_trust || [];
        nodeTrustBody.innerHTML = nodes.length === 0
            ? `<tr><td colspan="4" class="text-center text-secondary">No registered nodes.</td></tr>`
            : nodes.map(n => `
                <tr>
                    <td>${escapeHtml(n.node_id)}</td>
                    <td>${statusBadge(n.status)}</td>
                    <td>${n.trusted ? '<i class="bi bi-shield-check text-success"></i> Trusted' : '<i class="bi bi-shield-x text-danger"></i> Untrusted'}</td>
                    <td class="text-secondary small">${escapeHtml(n.last_seen || "-")}</td>
                </tr>
            `).join("");
    }

    // Verification timeline (recent receipt/verification-related events)
    const timelineEl = document.getElementById("tc-verification-timeline");
    if (timelineEl) {
        const events = (data.verification_timeline || []).filter(e =>
            (e.event_type || "").toLowerCase().includes("receipt") ||
            (e.event_type || "").toLowerCase().includes("health")
        );
        timelineEl.innerHTML = events.length === 0
            ? `<div class="text-secondary text-center py-3">No verification activity yet.</div>`
            : events.slice(0, 15).map(e => `
                <div class="gcon-activity-item">
                    <div class="gcon-activity-time">${new Date(e.timestamp).toLocaleString()}</div>
                    <div class="gcon-activity-message">${escapeHtml(e.event_type)} &middot; ${escapeHtml(e.source || "")}</div>
                </div>
            `).join("");
    }
}

async function loadTrustCenter() {
    try {
        const data = await fetchJson("/trust-center");
        renderTrustCenter(data);
    } catch (err) {
        console.error("Failed to load trust center:", err);
    }
}

function bindPanelLinks() {
    document.querySelectorAll(".gcon-panel-link[data-tab]").forEach(link => {
        link.addEventListener("click", (e) => {
            e.preventDefault();
            const tabLink = document.querySelector(`#tab-nav a[data-tab="${link.dataset.tab}"]`);
            if (tabLink) tabLink.click();
        });
    });
}

function bootstrapHomeDashboard() {
    const el = document.getElementById("dashboard-bootstrap-data");
    if (!el) return;
    try {
        const data = JSON.parse(el.textContent);
        renderHomeDashboard(data);
    } catch (err) {
        console.error("Failed to parse bootstrap dashboard data:", err);
    }
}

function populateOperationsSelectors(nodes, jobs) {

    if (nodes !== undefined) {

        const nodeSelect = document.getElementById("op-node-select");

        if (nodeSelect) {

            const current = nodeSelect.value;

            nodeSelect.innerHTML =
                `<option value="">Select node…</option>` +
                nodes.map(n =>
                    `<option value="${escapeHtml(n.node_id)}">
                        ${escapeHtml(n.node_id)}
                        (${escapeHtml(n.status)})
                    </option>`
                ).join("");

            if (nodes.some(n => n.node_id === current)) {
                nodeSelect.value = current;
            }

        }

    }

    if (jobs !== undefined) {

        const jobSelect = document.getElementById("op-job-select");

        if (jobSelect) {

            const current = jobSelect.value;

            const running =
                jobs.filter(j => j.status === "running");

            jobSelect.innerHTML =
                `<option value="">Select running job…</option>` +
                running.map(j =>
                    `<option value="${escapeHtml(j.job_id)}">
                        ${escapeHtml(j.job_id)}
                        (${escapeHtml(j.node_id || "-")})
                    </option>`
                ).join("");

            if (running.some(j => j.job_id === current)) {
                jobSelect.value = current;
            }

        }

    }

}

function setOpResult(message, isError) {

    const el = document.getElementById("op-result");

    if (!el) return;

    el.textContent = message;

    el.className =
        isError
            ? "mt-3 small text-danger"
            : "mt-3 small text-success";

}

async function opCall(url, options, successMessage) {

    try {

        const data = await fetchJson(url, options);

        setOpResult(successMessage || "Done.", false);

        await refreshDashboard();

        return data;

    } catch (err) {

        console.error(`Operation failed (${url}):`, err);

        setOpResult(err.message || "Action failed.", true);

        throw err;

    }
}

function setupOperationsPanel() {

    const bind = (id, handler) => {

        const btn = document.getElementById(id);

        if (btn) {

            btn.addEventListener("click", () =>
                handler().catch(() => {})
            );

        }

    };

    bind(
        "op-pause-scheduler-btn",
        () => opCall(
            "/cluster/scheduler/pause",
            { method: "POST" },
            "Scheduler paused — no new jobs will be assigned."
        )
    );

    bind(
        "op-resume-scheduler-btn",
        () => opCall(
            "/cluster/scheduler/resume",
            { method: "POST" },
            "Scheduler resumed."
        )
    );

    bind(
        "op-refresh-cluster-btn",
        () => opCall(
            "/cluster",
            {},
            "Cluster state refreshed."
        )
    );

    bind(
        "op-emergency-stop-btn",
        async () => {

            if (
                !confirm(
                    "Emergency stop: pause the scheduler and cancel every running job?"
                )
            ) {
                return;
            }

            await opCall(
                "/cluster/emergency-stop",
                { method: "POST" },
                "Emergency stop triggered."
            );

        }
    );

    bind(
        "op-drain-node-btn",
        async () => {

            const nodeId =
                document.getElementById("op-node-select").value;

            if (!nodeId) {

                setOpResult("Select a node first.", true);

                return;

            }

            await opCall(
                `/cluster/nodes/${encodeURIComponent(nodeId)}/drain`,
                { method: "POST" },
                `Node ${nodeId} is draining.`
            );

        }
    );

    bind(
        "op-restart-worker-btn",
        async () => {

            const nodeId =
                document.getElementById("op-node-select").value;

            if (!nodeId) {

                setOpResult("Select a node first.", true);

                return;

            }

            await opCall(
                `/cluster/nodes/${encodeURIComponent(nodeId)}/restart`,
                { method: "POST" },
                `Node ${nodeId} restarted.`
            );

        }
    );

    bind(
        "op-stop-worker-btn",
        async () => {

            const nodeId =
                document.getElementById("op-node-select").value;

            if (!nodeId) {

                setOpResult("Select a node first.", true);

                return;

            }

            if (
                !confirm(
                    `Stop and remove node ${nodeId} from the cluster?`
                )
            ) {
                return;
            }

            await opCall(
                `/cluster/nodes/${encodeURIComponent(nodeId)}/stop`,
                { method: "POST" },
                `Node ${nodeId} stopped and removed.`
            );

        }
    );

    bind(
        "op-cancel-job-btn",
        async () => {

            const jobId =
                document.getElementById("op-job-select").value;

            if (!jobId) {

                setOpResult("Select a running job first.", true);

                return;

            }

            if (!confirm(`Cancel job ${jobId}?`)) {
                return;
            }

            await opCall(
                `/jobs/${encodeURIComponent(jobId)}/cancel`,
                {
                    method: "POST",
                },
                `Job ${jobId} cancelled.`
            );

        }
    );

    bind(
        "op-clear-queue-btn",
        async () => {

            if (!confirm("Remove every job still waiting in the queue?")) {
                return;
            }

            await opCall(
                "/cluster/queue/clear",
                { method: "POST" },
                "Queue cleared."
            );

        }
    );

    bind(
        "op-retry-failed-btn",
        () => opCall(
            "/jobs/retry-failed",
            { method: "POST" },
            "Failed jobs re-queued."
        )
    );

    bind(
        "op-verify-receipts-btn",
        () => opCall(
            "/receipts/verify-all",
            {
                method: "POST",
            },
            "Receipt verification started."
        )
    );

    bind(
        "op-export-logs-btn",
        () => {

            window.open(
                "/logs/export",
                "_blank"
            );

            setOpResult(
                "Export started.",
                false
            );

            return Promise.resolve();

        }
    );

    bind(
        "op-export-metrics-btn",
        () => {

            window.open(
                "/metrics/export",
                "_blank"
            );

            setOpResult(
                "Export started.",
                false
            );

            return Promise.resolve();

        }
    );

    bind(
        "op-snapshot-btn",
        () => {

            window.open(
                "/cluster/snapshot",
                "_blank"
            );

            setOpResult(
                "Snapshot generated.",
                false
            );

            return Promise.resolve();

        }
    );
}

// ---------------------------------------------------------------
// Cluster Visualization (Topology)
// ---------------------------------------------------------------

async function loadTopology() {
    const container = document.getElementById("topology-container");
    if (!container) return;

    try {
        const topo = await fetchJson("/topology");
        container.innerHTML = buildTopologySvg(topo, true) + buildTopologyLegend();
        attachTopologyHandlers(container, topo);
    } catch (err) {
        console.error("Failed to load topology:", err);
        setConnectionStatus(false);
 
    }
}

function nodeHeartbeatAgeSeconds(lastSeen) {
    if (!lastSeen || lastSeen === "N/A") return null;
    const seen = new Date(lastSeen).getTime();
    if (isNaN(seen)) return null;
    return (Date.now() - seen) / 1000;
}

function buildTopologySvg(topo, interactive) {
    const nodes = topo.nodes || [];
    const coordinator = topo.coordinator || {};
    const width = 800;
    const height = Math.max(320, 140 + Math.ceil(nodes.length / 6) * 110);
    const centerX = width / 2;
    const centerY = 80;

    const statusColor = {
        idle: "#10B981", healthy: "#10B981", busy: "#3B82F6", offline: "#EF4444",
    };

    const cursor = interactive ? ' style="cursor:pointer"' : "";

    let svg = `<svg viewBox="0 0 ${width} ${height}" width="100%" style="max-width:100%">`;

    // Coordinator node
    const coordFresh = coordinator.online;
    svg += `
        <g class="gcon-topo-coordinator" data-coordinator="1"${cursor}>
            ${coordFresh ? `<circle cx="${centerX}" cy="${centerY}" r="42" fill="none" stroke="#8B5CF6" stroke-width="2" opacity=".35" class="gcon-topo-pulse-ring" />` : ""}
            <circle cx="${centerX}" cy="${centerY}" r="34" fill="#8B5CF6" />
            <text x="${centerX}" y="${centerY + 5}" text-anchor="middle" fill="white" font-size="12" font-weight="bold">Coordinator</text>
        </g>
    `;

    if (nodes.length === 0) {
        svg += `<text x="${centerX}" y="${centerY + 80}" text-anchor="middle" fill="#8A8F9B" font-size="13">No worker nodes registered</text>`;
        svg += "</svg>";
        return svg;
    }

    const perRow = 6;

    nodes.forEach((node, i) => {
        const row = Math.floor(i / perRow);
        const col = i % perRow;
        const nodesInRow = Math.min(nodes.length - row * perRow, perRow);
        const rowSpacingX = width / (nodesInRow + 1);
        const x = rowSpacingX * (col + 1);
        const y = centerY + 140 + row * 110;

        const status = (node.status || "").toLowerCase();
        const color = statusColor[status] || "#8A8F9B";
        const age = nodeHeartbeatAgeSeconds(node.last_seen);
        const isFresh = age !== null && age < 15 && status !== "offline";
        const isBusy = (node.running_jobs || 0) > 0;
        const isOffline = status === "offline";

        const edgeStroke = isOffline ? "rgba(239,68,68,0.35)" : isBusy ? "#3B82F6" : "rgba(255,255,255,0.14)";
        const edgeDash = isBusy ? ' stroke-dasharray="6 5" class="gcon-topo-edge-active"' : "";

        svg += `<line x1="${centerX}" y1="${centerY + 34}" x2="${x}" y2="${y - 26}" stroke="${edgeStroke}" stroke-width="2"${edgeDash} />`;

        svg += `<g class="gcon-topo-node" data-node-id="${escapeHtml(node.node_id)}"${cursor}>`;

        if (isFresh) {
            svg += `<circle cx="${x}" cy="${y}" r="32" fill="none" stroke="${color}" stroke-width="2" opacity=".35" class="gcon-topo-pulse-ring" />`;
        }

        svg += `<circle cx="${x}" cy="${y}" r="26" fill="${color}" ${node.draining ? 'stroke="#F59E0B" stroke-width="3"' : ""} />`;

        if (isOffline) {
            svg += `<text x="${x}" y="${y + 5}" text-anchor="middle" fill="white" font-size="16" font-weight="bold">!</text>`;
        } else if (isBusy) {
            svg += `
                <circle cx="${x + 18}" cy="${y - 18}" r="9" fill="#0B0D10" stroke="${color}" stroke-width="1.5" />
                <text x="${x + 18}" y="${y - 14}" text-anchor="middle" fill="white" font-size="9" font-weight="bold">${escapeHtml(node.running_jobs)}</text>
            `;
        }

        svg += `
            <text x="${x}" y="${isOffline ? y + 22 : y + 4}" text-anchor="middle" fill="white" font-size="10" font-weight="bold">${isOffline ? "" : escapeHtml(node.node_id)}</text>
            <text x="${x}" y="${y + 44}" text-anchor="middle" fill="#8A8F9B" font-size="11">${escapeHtml(node.node_id)} · ${escapeHtml(node.status)}${node.draining ? " · draining" : ""}</text>
        `;

        svg += `</g>`;
    });

    svg += "</svg>";
    return svg;
}

function buildTopologyLegend() {
    return `
        <div class="gcon-topo-legend">
            <span class="item"><span class="swatch" style="background:#10B981;"></span>Idle</span>
            <span class="item"><span class="swatch" style="background:#3B82F6;"></span>Busy</span>
            <span class="item"><span class="swatch" style="background:#EF4444;"></span>Offline</span>
            <span class="item"><span class="swatch" style="border:2px solid #F59E0B;"></span>Draining</span>
            <span class="item"><span class="ring"></span>Fresh heartbeat</span>
        </div>
    `;
}

function openNodeTopologyDetail(node) {
    setText("drawer-title", "Node Inspector");
    const body = document.getElementById("drawer-body");
    const age = nodeHeartbeatAgeSeconds(node.last_seen);

    body.innerHTML = `
        <div class="gcon-panel mb-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
                <strong>${escapeHtml(node.node_id)}</strong>
                <span class="badge ${node.status === "offline" ? "bg-danger" : node.status === "busy" ? "bg-primary" : "bg-success"}">
                    ${escapeHtml(node.status)}
                </span>
            </div>
            ${node.draining ? `<div class="text-warning small"><i class="bi bi-exclamation-triangle me-1"></i>Draining — not accepting new jobs</div>` : ""}
        </div>
        <div class="gcon-panel mb-3">
            <strong class="d-block mb-2">Live State</strong>
            ${receiptDetailRow("CPU", escapeHtml(node.cpu))}
            ${receiptDetailRow("Memory", escapeHtml(node.memory))}
            ${receiptDetailRow("Running Jobs", escapeHtml(node.running_jobs))}
            ${receiptDetailRow("Last Heartbeat", escapeHtml(node.last_seen || "-"))}
            ${receiptDetailRow("Heartbeat Age", age === null ? "--" : formatAge(age))}
        </div>
    `;
    openDrawer();
}

function openCoordinatorTopologyDetail(coordinator) {
    setText("drawer-title", "Coordinator Inspector");
    const body = document.getElementById("drawer-body");

    body.innerHTML = `
        <div class="gcon-panel mb-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
                <strong>${escapeHtml(coordinator.id)}</strong>
                <span class="badge ${coordinator.online ? "bg-success" : "bg-danger"}">
                    ${coordinator.online ? "Online" : "Offline"}
                </span>
            </div>
        </div>
        <div class="gcon-panel mb-3">
            <strong class="d-block mb-2">State</strong>
            ${receiptDetailRow("Scheduler", coordinator.scheduler_running ? "Running" : "Paused")}
            ${receiptDetailRow("Started", escapeHtml(coordinator.started_at || "-"))}
            ${receiptDetailRow("Registered Nodes", escapeHtml(coordinator.total_nodes))}
            ${receiptDetailRow("Running Jobs", escapeHtml(coordinator.running_jobs))}
        </div>
    `;
    openDrawer();
}

function attachTopologyHandlers(container, topo) {
    const nodesById = {};
    (topo.nodes || []).forEach(n => { nodesById[n.node_id] = n; });

    container.querySelectorAll(".gcon-topo-node").forEach(el => {
        el.addEventListener("click", () => {
            const node = nodesById[el.dataset.nodeId];
            if (node) openNodeTopologyDetail(node);
        });
    });

    const coordEl = container.querySelector(".gcon-topo-coordinator");
    if (coordEl) {
        coordEl.addEventListener("click", () => openCoordinatorTopologyDetail(topo.coordinator || {}));
    }
}

// always the live result from /receipts (HMAC re-check), never a
// cached flag; detail comes from /receipts/{id}, which stitches
// together the receipt's proof with the job it attests to and the
// artifacts that job produced.
// ---------------------------------------------------------------

let receiptsData = [];

function filterReceiptsData(query) {
    if (!query) return receiptsData;
    const q = query.toLowerCase();
    return receiptsData.filter(r =>
        (r.receipt_id || "").toLowerCase().includes(q) ||
        (r.job_id || "").toLowerCase().includes(q)
    );
}

function renderReceiptSummaryTiles(receipts) {
    const verified = receipts.filter(r => r.verified).length;
    setText("receipts-tab-total", receipts.length);
    setText("receipts-tab-verified", verified);
    setText("receipts-tab-unverified", receipts.length - verified);
}

function renderReceiptCards(receipts) {
    const grid = document.getElementById("receipts-grid");
    if (!grid) return;

    if (receipts.length === 0) {
        grid.innerHTML = `<div class="text-secondary text-center py-4">No receipts generated yet.</div>`;
        return;
    }

    grid.innerHTML = receipts.map(r => `
        <div class="gcon-receipt-card" data-receipt-id="${escapeHtml(r.receipt_id)}">
            <div class="d-flex justify-content-between align-items-start">
                <span class="gcon-receipt-id" title="${escapeHtml(r.receipt_id)}">${escapeHtml(r.receipt_id)}</span>
                <span class="badge ${r.verified ? "bg-success" : "bg-danger"}">
                    <i class="bi ${r.verified ? "bi-shield-check" : "bi-shield-x"} me-1"></i>${r.verified ? "Verified" : "Unverified"}
                </span>
            </div>
            <div class="gcon-receipt-meta">
                <span><i class="bi bi-braces me-1"></i>${escapeHtml(r.job_id || "-")}</span>
                ${statusBadge(r.status)}
            </div>
            <div class="gcon-receipt-meta text-secondary small">
                <i class="bi bi-clock-history me-1"></i>${escapeHtml(r.created_at || "-")}
            </div>
            <button class="btn btn-sm btn-outline-light w-100 mt-2 gcon-receipt-inspect-btn" data-receipt-id="${escapeHtml(r.receipt_id)}">
                <i class="bi bi-search me-1"></i>Inspect
            </button>
        </div>
    `).join("");

    grid.querySelectorAll(".gcon-receipt-inspect-btn").forEach(btn => {
        btn.addEventListener("click", () => openReceiptDetail(btn.dataset.receiptId));
    });
}

async function loadReceiptsTab() {
    try {
        receiptsData = await fetchJson("/receipts");
        renderReceiptSummaryTiles(receiptsData);

        const search = document.getElementById("receipts-search");
        renderReceiptCards(filterReceiptsData(search ? search.value : ""));
    } catch (err) {
        console.error("Failed to load receipts:", err);
        setConnectionStatus(false);
    }
}

function setupReceiptsTab() {
    const search = document.getElementById("receipts-search");
    if (search) {
        search.addEventListener("input", () => renderReceiptCards(filterReceiptsData(search.value)));
    }

    const verifyBtn = document.getElementById("receipts-tab-verify-all-btn");
    if (verifyBtn) {
        verifyBtn.addEventListener("click", async () => {
            verifyBtn.disabled = true;
            const originalHtml = verifyBtn.innerHTML;
            verifyBtn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Verifying...`;
            try {
                await opCall("/receipts/verify-all", { method: "POST" }, "Receipt verification started.");
                await loadReceiptsTab();
            } catch (err) {
                // opCall already surfaced the failure via setOpResult
            } finally {
                verifyBtn.disabled = false;
                verifyBtn.innerHTML = originalHtml;
            }
        });
    }
}

function receiptDetailRow(label, value, mono) {
    return `
        <div class="gcon-kv-row">
            <span class="gcon-kv-label">${escapeHtml(label)}</span>
            <span class="gcon-kv-value ${mono ? "mono" : ""}">${value}</span>
        </div>
    `;
}

function copyableValue(value) {
    if (!value) return `<span class="text-secondary">&mdash;</span>`;
    const safe = escapeHtml(value);
    return `
        <span class="gcon-copy-value" title="Click to copy" onclick="navigator.clipboard.writeText('${safe.replace(/'/g, "\\'")}')">
            ${safe}<i class="bi bi-copy ms-2"></i>
        </span>
    `;
}

async function openReceiptDetail(receiptId) {
    try {
        const r = await fetchJson(`/receipts/${encodeURIComponent(receiptId)}`);

        setText("drawer-title", "Receipt Inspector");
        const body = document.getElementById("drawer-body");

        const metricsRows = Object.entries(r.proof.metrics || {})
            .map(([k, v]) => receiptDetailRow(k, escapeHtml(v)))
            .join("") || `<div class="text-secondary small">No additional metrics recorded.</div>`;

        const artifactRows = (r.artifacts || []).map(a => `
            <div class="gcon-kv-row">
                <span class="gcon-kv-label">${escapeHtml(a.filename)}</span>
                <span class="gcon-kv-value mono small">${escapeHtml(a.sha256.slice(0, 16))}&hellip; &middot; ${formatBytes(a.size)}</span>
            </div>
        `).join("") || `<div class="text-secondary small">No artifacts recorded for this execution.</div>`;

        body.innerHTML = `
            <div class="gcon-panel mb-3">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <strong>Verification</strong>
                    <span class="badge ${r.verified ? "bg-success" : "bg-danger"}">
                        <i class="bi ${r.verified ? "bi-shield-check" : "bi-shield-x"} me-1"></i>${r.verified ? "Verified" : "Unverified"}
                    </span>
                </div>
                <div class="text-secondary small">${escapeHtml(r.verification_message)}</div>
            </div>

            <div class="gcon-panel mb-3">
                <strong class="d-block mb-2">Signature</strong>
                ${receiptDetailRow("Algorithm", escapeHtml(r.proof.algorithm), true)}
                ${receiptDetailRow("Signature", copyableValue(r.proof.signature), true)}
                ${receiptDetailRow("Input Hash", copyableValue(r.input_hash), true)}
                ${receiptDetailRow("Output Hash", copyableValue(r.output_hash), true)}
                ${receiptDetailRow("Timestamp", escapeHtml(r.proof.timestamp))}
            </div>

            <div class="gcon-panel mb-3">
                <strong class="d-block mb-2">Execution Details</strong>
                ${receiptDetailRow("Job ID", copyableValue(r.job_id), true)}
                ${receiptDetailRow("Node", escapeHtml(r.execution.node_id || "-"))}
                ${receiptDetailRow("GPU", escapeHtml(r.proof.gpu || "-"))}
                ${receiptDetailRow("Runtime", `${escapeHtml(r.proof.runtime_seconds ?? "-")}s`)}
                ${receiptDetailRow("Started", escapeHtml(r.execution.created_at || "-"))}
                ${receiptDetailRow("Completed", escapeHtml(r.execution.completed_at || "-"))}
            </div>

            <div class="gcon-panel mb-3">
                <strong class="d-block mb-2">Metrics</strong>
                ${metricsRows}
            </div>

            <div class="gcon-panel mb-3">
                <strong class="d-block mb-2">Artifacts</strong>
                ${artifactRows}
            </div>

            <button class="btn btn-sm btn-outline-light w-100" id="receipt-export-btn">
                <i class="bi bi-download me-1"></i>Export Receipt (JSON)
            </button>
        `;

        document.getElementById("receipt-export-btn").addEventListener("click", () => {
            const blob = new Blob([JSON.stringify(r, null, 2)], { type: "application/json" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = `${r.receipt_id}.json`;
            a.click();
            URL.revokeObjectURL(url);
        });

        openDrawer();
    } catch (err) {
        console.error("Failed to load receipt detail:", err);
        setOpResult("Could not load that receipt.", true);
    }
}

// ---------------------------------------------------------------
// Executions — visualizes each job's real lifecycle. The list view
// uses only fields already on /jobs (no live crypto check per row,
// per poll); the detail drawer fetches /jobs/{id} for the one-off
// live receipt verification, the same pattern the Receipt Explorer
// uses.
// ---------------------------------------------------------------

let executionsData = [];

function filterExecutionsData(query) {
    if (!query) return executionsData;
    const q = query.toLowerCase();
    return executionsData.filter(j =>
        (j.job_id || "").toLowerCase().includes(q) ||
        (j.node_id || "").toLowerCase().includes(q)
    );
}

function buildLifecycleStepper(job) {
    const terminal = ["completed", "failed", "cancelled"].includes(job.status);
    const running = job.status === "running" || terminal;

    const outcomeLabel = job.status === "failed" ? "Failed"
        : job.status === "cancelled" ? "Cancelled"
        : job.status === "completed" ? "Completed"
        : "Outcome";

    const outcomeState = !terminal ? "pending"
        : job.status === "completed" ? "done"
        : "failed";

    const steps = [
        { label: "Submitted", state: "done" },
        { label: "Running", state: running ? "done" : (terminal ? "skipped" : "current") },
        { label: outcomeLabel, state: outcomeState },
        { label: "Receipt", state: job.receipt_id ? "done" : (terminal && job.status === "completed" ? "current" : "skipped") },
    ];

    return `
        <div class="gcon-stepper">
            ${steps.map((s, i) => `
                <div class="gcon-step ${s.state}">
                    <span class="gcon-step-dot"></span>
                    <span class="gcon-step-label">${escapeHtml(s.label)}</span>
                </div>
                ${i < steps.length - 1 ? `<span class="gcon-step-connector ${s.state === "done" ? "done" : ""}"></span>` : ""}
            `).join("")}
        </div>
    `;
}

function renderExecutionCards(jobs) {
    const list = document.getElementById("executions-list");
    if (!list) return;

    if (jobs.length === 0) {
        list.innerHTML = `<div class="text-secondary text-center py-4">No executions submitted yet.</div>`;
        return;
    }

    list.innerHTML = jobs.map(job => `
        <div class="gcon-exec-card" data-job-id="${escapeHtml(job.job_id)}">
            <div class="d-flex justify-content-between align-items-center mb-2">
                <span class="gcon-receipt-id" title="${escapeHtml(job.job_id)}">${escapeHtml(job.job_id)}</span>
                <span class="text-secondary small">${escapeHtml(job.node_id || "unassigned")} &middot; ${escapeHtml(job.created_at || "-")}</span>
            </div>
            ${buildLifecycleStepper(job)}
        </div>
    `).join("");

    list.querySelectorAll(".gcon-exec-card").forEach(card => {
        card.addEventListener("click", () => openExecutionDetail(card.dataset.jobId));
    });
}

function renderExecutionSummaryTiles(jobs) {
    const running = jobs.filter(j => j.status === "running").length;
    const completed = jobs.filter(j => j.status === "completed").length;
    const failed = jobs.filter(j => j.status === "failed" || j.status === "cancelled").length;
    setText("exec-tab-total", jobs.length);
    setText("exec-tab-running", running);
    setText("exec-tab-completed", completed);
    setText("exec-tab-failed", failed);
}

async function loadExecutionsTab() {
    try {
        executionsData = await fetchJson("/jobs");
        renderExecutionSummaryTiles(executionsData);

        const search = document.getElementById("executions-search");
        renderExecutionCards(filterExecutionsData(search ? search.value : ""));
    } catch (err) {
        console.error("Failed to load executions:", err);
        setConnectionStatus(false);
    }
}

function setupExecutionsTab() {
    const search = document.getElementById("executions-search");
    if (search) {
        search.addEventListener("input", () => renderExecutionCards(filterExecutionsData(search.value)));
    }
}

async function openExecutionDetail(jobId) {
    try {
        const j = await fetchJson(`/jobs/${encodeURIComponent(jobId)}`);

        setText("drawer-title", "Execution Inspector");
        const body = document.getElementById("drawer-body");

        const artifactRows = (j.artifacts || []).map(a => `
            <div class="gcon-kv-row">
                <span class="gcon-kv-label">${escapeHtml(a.filename)}</span>
                <span class="gcon-kv-value mono small">${escapeHtml(a.sha256.slice(0, 16))}&hellip; &middot; ${formatBytes(a.size)}</span>
            </div>
        `).join("") || `<div class="text-secondary small">No artifacts recorded for this execution.</div>`;

        body.innerHTML = `
            <div class="gcon-panel mb-3">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <strong>${escapeHtml(j.job_id)}</strong>
                    ${statusBadge(j.status)}
                </div>
                ${buildLifecycleStepper(j)}
            </div>

            <div class="gcon-panel mb-3">
                <strong class="d-block mb-2">Execution Details</strong>
                ${receiptDetailRow("Node", escapeHtml(j.node_id || "unassigned"))}
                ${receiptDetailRow("Created", escapeHtml(j.created_at || "-"))}
                ${receiptDetailRow("Completed", escapeHtml(j.completed_at || "-"))}
            </div>

            <div class="gcon-panel mb-3">
                <strong class="d-block mb-2">Receipt</strong>
                ${j.receipt_id
                    ? `
                        <div class="d-flex justify-content-between align-items-center mb-2">
                            <span class="gcon-kv-value mono small">${escapeHtml(j.receipt_id)}</span>
                            <span class="badge ${j.verified ? "bg-success" : "bg-danger"}">
                                <i class="bi ${j.verified ? "bi-shield-check" : "bi-shield-x"} me-1"></i>${j.verified ? "Verified" : "Unverified"}
                            </span>
                        </div>
                        <div class="text-secondary small">${escapeHtml(j.verification_message || "")}</div>
                        <button class="btn btn-sm btn-outline-light w-100 mt-2" id="exec-view-receipt-btn">
                            <i class="bi bi-patch-check me-1"></i>Open in Receipt Explorer
                        </button>
                    `
                    : `<div class="text-secondary small">No receipt generated yet for this execution.</div>`
                }
            </div>

            <div class="gcon-panel mb-3">
                <strong class="d-block mb-2">Artifacts</strong>
                ${artifactRows}
            </div>
        `;

        const viewReceiptBtn = document.getElementById("exec-view-receipt-btn");
        if (viewReceiptBtn) {
            viewReceiptBtn.addEventListener("click", () => openReceiptDetail(j.receipt_id));
        }

        openDrawer();
    } catch (err) {
        console.error("Failed to load execution detail:", err);
        setOpResult("Could not load that execution.", true);
    }
}

// ---------------------------------------------------------------
// Explorer
// ---------------------------------------------------------------

const EXPLORER_COLUMNS = {
    nodes: ["node_id", "status", "running_jobs", "cpu", "memory", "last_seen"],
    artifacts: ["artifact_id", "filename", "sha256", "size", "uploaded_at"],
};

const EXPLORER_HEADERS = {
    nodes: ["Node ID", "Status", "Running Jobs", "CPU", "Memory", "Last Seen"],
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

    if (!query) {
        return explorerData;
    }
    const q = query.toLowerCase();

    return explorerData.filter(row =>
        Object.values(row).some(value =>

            String(value ?? "")
                .toLowerCase()
                .includes(q)
        )
    );
}

function renderExplorerRows(rows) {

    const thead =
        document.getElementById("explorer-thead");

    const body =
        document.getElementById("explorer-body");

    if (!thead || !body) return;

    const columns =
        EXPLORER_COLUMNS[explorerView];

    const headers =
        EXPLORER_HEADERS[explorerView];

    thead.innerHTML =
        `<tr>${
            headers.map(h => `<th>${escapeHtml(h)}</th>`).join("")
        }</tr>`;

    if (!rows.length) {

        body.innerHTML =
            `<tr>
                <td colspan="${columns.length}"
                    class="text-center text-secondary">
                    No data.
                </td>
            </tr>`;

        return;

    }

    let html = "";

    rows.forEach(row => {

        html += "<tr>";

        columns.forEach(column => {

            const value = row[column];

            if (EXPLORER_BADGE_COLUMNS.has(column)) {

                html +=
                    `<td>${statusBadge(value)}</td>`;

            } else {

                html +=
                    `<td>${escapeHtml(value ?? "-")}</td>`;
            }

        });

        html += "</tr>";

    });

    body.innerHTML = html;
}

async function loadExplorer() {

    try {

        explorerData =
            await fetchJson(`/${explorerView}`);

        const search =
            document.getElementById(
                "explorer-search"
            );

        renderExplorerRows(

            filterExplorerData(

                search
                    ? search.value
                    : ""

            )

        );

    } catch (err) {

        console.error(
            "Failed to load explorer:",
            err
        );

        setConnectionStatus(false);

    }

}

// ---------------------------------------------------------------
// Real-Time Monitoring
// ---------------------------------------------------------------

async function loadMonitoring() {

    try {

        const [metrics, events] = await Promise.all([
            fetchJson("/system-metrics"),
            fetchJson("/events"),
        ]);

        setText("sm-avg-cpu", `${metrics.avg_cpu}%`);
        setText("sm-avg-memory", `${metrics.avg_memory}%`);
        setText("sm-running", metrics.running_jobs);
        setText("sm-event-count", metrics.event_count);
        setText("sm-uptime", formatUptime(metrics.uptime_seconds));
        setText("sm-connection", "Live");

        renderFeed(
            "monitoring-activity-feed",
            events
        );
    } catch (err) {

        console.error(
            "Failed to load monitoring:",
            err
        );
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
    const colors = { completed: "#10B981", failed: "#EF4444", running: "#3B82F6", pending: "#F59E0B" };

    let html = `<div class="gcon-bars-row">`;
    for (const [key, value] of Object.entries(totals)) {
        const heightPct = Math.round((value / max) * 100);
        html += `
            <div class="gcon-bar-col">
                <div class="gcon-bar-track">
                    <div class="gcon-bar-fill" style="height:${heightPct}%; background:${colors[key] || "#8A8F9B"}"></div>
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

        const analytics =
            await fetchJson("/analytics");

        setText(
            "an-success-rate",
            `${analytics.success_rate}%`
        );

        setText(
            "an-completed",
            analytics.totals.completed
        );

        setText(
            "an-failed",
            analytics.totals.failed
        );

        setText(
            "an-pending",
            analytics.totals.pending
        );

        renderBarChart(
            analytics.totals
        );
        renderFeed(
            "analytics-timeline",
            analytics.timeline
        );

    } catch (err) {
        console.error(
            "Failed to load analytics:",
            err
        );
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
                    showToast(`Node ${btn.dataset.nodeId} deregistered.`, false);
                    await loadAdmin();
                } catch (err) {
                    console.error("Failed to deregister node:", err);
                    showToast(err.message || "Failed to deregister node.", true);
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
        showToast(direction === "up" ? "Scaling up." : "Scaling down.", false);
        await refreshDashboard();
    } catch (err) {
        console.error(`Failed to scale ${direction}:`, err);
        showToast(err.message || `Failed to scale ${direction}.`, true);
    }
}

// ---------------------------------------------------------------
// Global refresh loop
// ---------------------------------------------------------------

async function refreshDashboard() {
    // Control Center is kept live by the WebSocket push in
    // connectLiveSocket() (every 2s, via renderHomeDashboard/renderFeed).
    // Re-fetching it here too on a separate 5s cadence would double-update
    // the same panels from two independent sources. Every other tab has
    // no socket coverage, so it still refreshes on this interval.
    if (currentTab !== "control-center") {
        await loadActiveTab();
    }
    refreshHealthInspector();
    setText(
        "last-updated",
        new Date().toLocaleTimeString()
);
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

    setupOperationsPanel();

    const inspectorBtn =
        document.getElementById("open-health-inspector-btn");

    if (inspectorBtn) {

        inspectorBtn.addEventListener(
            "click",
            openHealthInspector
        );

    }
}


async function openHealthInspector() {

    try {

        const details = await fetchJson("/health/details");

        setText("drawer-title", "Health Inspector");

        const body =
            document.getElementById("drawer-body");

        body.innerHTML = "";

        Object.values(details.checks).forEach(check => {

            body.innerHTML += `
                <div class="gcon-panel mb-3">
                    <div class="gcon-panel-body">

                        <div class="d-flex justify-content-between align-items-center">

                            <strong>${escapeHtml(check.label)}</strong>

                            <span class="badge ${check.healthy ? "bg-success" : "bg-danger"}">
                                ${check.healthy ? "Healthy" : "Unhealthy"}
                            </span>

                        </div>

                        <div class="small text-secondary mt-2">
                            ${escapeHtml(check.detail)}
                        </div>

                    </div>
                </div>
            `;

        });

        openDrawer();

    } catch (err) {

        console.error(
            "Failed to load health inspector:",
            err
        );

    }

}

function refreshHealthInspector() {
    const drawer =
        document.getElementById("detail-drawer");
    if ( drawer &&
        drawer.classList.contains("gcon-drawer-open") &&
        document.getElementById("drawer-title")?.textContent === "Health Inspector"
    ) {
        openHealthInspector();

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
                showToast(err.message || "Failed to delete user.", true);
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
                showToast(err.message || "Failed to create user.", true);
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
                showToast(err.message || "Failed to update user.", true);
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
                showToast(err.message || "Failed to create organization.", true);
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
            renderHomeDashboard(data);
            setText("last-updated", new Date().toLocaleTimeString());
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
                    showToast(err.message || "Failed to revoke API key.", true);
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
                    showToast(err.message || "Failed to regenerate API key.", true);
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
                showToast(err.message || "Failed to create API key.", true);
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
    node_failure: "bi-exclamation-triangle", node_registered: "bi-hdd-network",
    job_failed: "bi-x-circle", receipt_generated: "bi-patch-check",
    storage_warning: "bi-hdd", workflow_completed: "bi-check-circle",
};

// Severity/category are computed once, authoritatively, by the backend
// (NotificationCenter.notify — see TYPE_SEVERITY / TYPE_CATEGORY) and
// arrive on every notification entry as n.severity / n.category. The
// client never re-derives them, so there is exactly one place that
// decides what a notification type means.
const SEVERITY_LABEL = {
    critical: "Critical", warning: "Warning",
    information: "Information", security: "Security",
};

let notificationsData = [];
let notifFilter = "all";

function filterNotifications(entries, filter) {
    if (filter === "all") return entries;
    if (filter === "unread") return entries.filter(n => !n.read);
    if (SEVERITY_LABEL[filter]) return entries.filter(n => n.severity === filter);
    return entries.filter(n => n.category === filter);
}

function renderNotificationItem(n, compact) {
    const severity = n.severity || "information";
    return `
        <div class="gcon-activity-item gcon-notif-item gcon-notif-sev-${escapeHtml(severity)} ${n.read ? "" : "gcon-notif-unread"}" data-notif-id="${escapeHtml(n.notification_id)}">
            <span class="gcon-notif-sev-dot" title="${escapeHtml(SEVERITY_LABEL[severity] || severity)}"></span>
            <div class="gcon-activity-time">
                <i class="bi ${NOTIF_ICONS[n.type] || "bi-bell"} me-1"></i>${new Date(n.timestamp).toLocaleString()}
            </div>
            <div class="gcon-activity-message">${escapeHtml(n.message)}</div>
            ${!n.read ? `<button class="btn btn-sm btn-link p-0 gcon-notif-read-btn" title="Mark as read"><i class="bi bi-check2"></i></button>` : ""}
        </div>
    `;
}

async function markNotificationRead(notificationId) {
    try {
        await fetchJson(`/management/notifications/${encodeURIComponent(notificationId)}/read`, { method: "POST" });
        const entry = notificationsData.find(n => n.notification_id === notificationId);
        if (entry) entry.read = true;
        renderNotificationsList();
        renderNotifDropdown();
        updateNotifBadge();
    } catch (err) {
        console.error("Failed to mark notification read:", err);
    }
}

function attachNotifClickHandlers(container) {
    container.querySelectorAll(".gcon-notif-read-btn").forEach(btn => {
        btn.addEventListener("click", (e) => {
            e.stopPropagation();
            markNotificationRead(btn.closest(".gcon-notif-item").dataset.notifId);
        });
    });
}

function renderNotificationsList() {
    const list = document.getElementById("notifications-list");
    if (!list) return;

    const filtered = filterNotifications(notificationsData, notifFilter);

    if (filtered.length === 0) {
        list.innerHTML = `<div class="text-secondary">No notifications${notifFilter !== "all" ? " in this filter" : ""}.</div>`;
        return;
    }

    list.innerHTML = filtered.map(n => renderNotificationItem(n, false)).join("");
    attachNotifClickHandlers(list);
}

function renderNotifDropdown() {
    const list = document.getElementById("notif-dropdown-list");
    if (!list) return;

    const recent = notificationsData.slice(0, 8);

    if (recent.length === 0) {
        list.innerHTML = `<div class="text-secondary small p-3">No notifications yet.</div>`;
        return;
    }

    list.innerHTML = recent.map(n => renderNotificationItem(n, true)).join("");
    attachNotifClickHandlers(list);
}

function updateNotifBadge(unreadBySeverity) {
    const unreadEntries = notificationsData.filter(n => !n.read);
    const unread = unreadEntries.length;
    const badge = document.getElementById("notif-count-badge");
    if (!badge) return;

    badge.textContent = unread > 9 ? "9+" : unread;
    badge.classList.toggle("d-none", unread === 0);

    // Highest-severity-present drives the badge color: a single
    // critical notification should be impossible to miss even if
    // it's outnumbered by informational ones.
    const bySeverity = unreadBySeverity || unreadEntries.reduce((acc, n) => {
        const s = n.severity || "information";
        acc[s] = (acc[s] || 0) + 1;
        return acc;
    }, {});
    badge.classList.remove("gcon-badge-critical", "gcon-badge-warning", "gcon-badge-security", "gcon-badge-info");
    if (bySeverity.critical) badge.classList.add("gcon-badge-critical");
    else if (bySeverity.warning) badge.classList.add("gcon-badge-warning");
    else if (bySeverity.security) badge.classList.add("gcon-badge-security");
    else badge.classList.add("gcon-badge-info");
}

async function refreshNotifications() {
    try {
        const [entries, unreadBySeverity] = await Promise.all([
            fetchJson("/management/notifications"),
            fetchJson("/management/notifications/unread-by-severity"),
        ]);
        notificationsData = entries;
        updateNotifBadge(unreadBySeverity);
        renderNotifDropdown();
        if (currentTab === "notifications") renderNotificationsList();
    } catch (err) {
        // non-fatal — the badge just won't update this tick
    }
}

async function loadNotificationsTab() {
    await refreshNotifications();
}

async function refreshNotifBadge() {
    await refreshNotifications();
}

function setupNotifications() {
    document.querySelectorAll("#notif-filter-group [data-notif-filter]").forEach(btn => {
        btn.addEventListener("click", () => {
            notifFilter = btn.dataset.notifFilter;
            document.querySelectorAll("#notif-filter-group [data-notif-filter]").forEach(b => {
                b.classList.toggle("active", b === btn);
            });
            renderNotificationsList();
        });
    });

    const markAll = async () => {
        const unreadCount = notificationsData.filter(n => !n.read).length;
        await fetchJson("/management/notifications/mark-all-read", { method: "POST" });
        await refreshNotifications();
        showToast(unreadCount ? `${unreadCount} notification(s) marked read.` : "Nothing to mark read.", false);
    };

    const markAllBtn1 = document.getElementById("notif-mark-all-read-btn");
    const markAllBtn2 = document.getElementById("notif-tab-mark-all-read-btn");
    if (markAllBtn1) markAllBtn1.addEventListener("click", markAll);
    if (markAllBtn2) markAllBtn2.addEventListener("click", markAll);
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
setupReceiptsTab();
setupExecutionsTab();
setupNotifications();
bindPanelLinks();

bootstrapHomeDashboard();
loadCurrentUser();
refreshDashboard();
loadHealthBadge();
refreshNotifBadge();
updateClock();

setInterval(() => { if (!isPaused) refreshDashboard(); }, REFRESH_INTERVAL_MS);
setInterval(loadHealthBadge, REFRESH_INTERVAL_MS);
setInterval(refreshNotifBadge, REFRESH_INTERVAL_MS);
setInterval(updateClock, 1000);
