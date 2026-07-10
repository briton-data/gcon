/**
 * GCON Dashboard — live client-side refresh.
 *
 * Polls the coordinator's REST endpoints on an interval and updates
 * the DOM in place, so the dashboard reflects cluster state without
 * requiring a full page reload.
 */

const REFRESH_INTERVAL_MS = 5000;

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
        healthy: "bg-success",
        idle: "bg-secondary",
        offline: "bg-secondary",
        running: "bg-primary",
        completed: "bg-success",
        failed: "bg-danger",
    };

    const labelMap = {
        healthy: "Healthy",
        idle: "Idle",
        offline: "Idle",
        running: "Running",
        completed: "Completed",
        failed: "Failed",
    };

    const cls = classMap[normalized] || "bg-secondary";
    const label = labelMap[normalized] || escapeHtml(status || "Unknown");

    return `<span class="badge ${cls}">${label}</span>`;
}

function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
}

async function fetchJson(url) {
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`${url} returned ${response.status}`);
    }
    return response.json();
}

async function loadCluster() {
    try {
        const cluster = await fetchJson("/cluster");

        setText("metric-total-nodes", cluster.total_nodes);
        setText("metric-running-jobs", cluster.running_jobs);
        setText("metric-completed-jobs", cluster.completed_jobs);
        setText("metric-failed-jobs", cluster.failed_jobs);

        setText("overview-registered-nodes", cluster.total_nodes);
        setText("overview-active-jobs", cluster.running_jobs);

    } catch (err) {
        console.error("Failed to load cluster state:", err);
    }
}

async function loadNodes() {
    const body = document.getElementById("nodes-body");
    if (!body) return;

    try {
        const nodes = await fetchJson("/nodes");

        if (nodes.length === 0) {
            body.innerHTML = `
                <tr>
                    <td colspan="6" class="text-center text-secondary">
                        No registered nodes.
                    </td>
                </tr>
            `;
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
        body.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-secondary">
                    Unable to load nodes.
                </td>
            </tr>
        `;
    }
}

async function loadJobs() {
    const body = document.getElementById("jobs-body");
    if (!body) return;

    try {
        const jobs = await fetchJson("/jobs");

        if (jobs.length === 0) {
            body.innerHTML = `
                <tr>
                    <td colspan="6" class="text-center text-secondary">
                        No jobs submitted.
                    </td>
                </tr>
            `;
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
        body.innerHTML = `
            <tr>
                <td colspan="6" class="text-center text-secondary">
                    Unable to load jobs.
                </td>
            </tr>
        `;
    }
}

async function loadEvents() {
    const feed = document.getElementById("activity-feed");
    if (!feed) return;

    try {
        const events = await fetchJson("/events");

        if (events.length === 0) {
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

    } catch (err) {
        console.error("Failed to load events:", err);
        feed.innerHTML = `<div class="text-secondary">Unable to load activity.</div>`;
    }
}

async function refreshDashboard() {
    await Promise.all([
        loadCluster(),
        loadNodes(),
        loadJobs(),
        loadEvents(),
    ]);

    setText("last-updated", new Date().toLocaleTimeString());
}

function updateClock() {
    const clock = document.getElementById("clock");
    if (!clock) return;

    clock.textContent = new Date().toLocaleTimeString();
}

// Kick things off.
refreshDashboard();
updateClock();

setInterval(refreshDashboard, REFRESH_INTERVAL_MS);
setInterval(updateClock, 1000);
