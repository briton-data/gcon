"""
GCON Management Layer.

A sibling to the cluster PresentationLayer: handles platform
management concerns (users, organizations, RBAC, API keys, audit
log, notifications) rather than cluster operations.

Users/orgs/API keys/audit log are seeded with a small set of
bootstrap accounts so the UI/RBAC can be exercised without a signup
flow, but login itself is real (see auth.py), and notifications are
generated from real coordinator events (see _bridge_cluster_events)
rather than pre-seeded demo text.
"""


import csv
import io
import json
import logging
import os
import secrets
from datetime import datetime, UTC

from . import rbac
from .auth import SessionManager, ResetTokenManager
from .users import UserRegistry, bootstrap_owner_account
from .organizations import OrganizationRegistry
from .api_keys import APIKeyManager
from .audit_log import AuditLogger

from gcon.monitoring.notifications import NotificationCenter
from gcon.storage.database import Database

logger = logging.getLogger(__name__)


def _mask_webhook_secret(secret):
    """Same masking shape as api_keys.py's _mask -- kept as a
    separate helper (not imported from there) since webhook secrets
    are hex, not the gcon_-prefixed API key format, and don't share
    that module's length assumptions."""
    if not secret or len(secret) < 12:
        return "*" * len(secret or "")
    return f"{secret[:6]}{'*' * 24}{secret[-4:]}"

# Bootstrap owner account, created once on first boot. Always set
# GCON_OWNER_NAME / GCON_OWNER_EMAIL per deployment -- the fallbacks
# below are deliberately generic placeholders, not a real identity,
# so nobody's actual name/email ships hard-coded in source control.
BOOTSTRAP_OWNER_NAME = os.environ.get("GCON_OWNER_NAME", "GCON Owner")
BOOTSTRAP_OWNER_EMAIL = os.environ.get("GCON_OWNER_EMAIL", "owner@example.com")

# Deliberately NOT a hardcoded fallback like the old "GCON2024" --
# a fixed default password shipped in source control is guessable by
# anyone who has ever read this file, in any deployment that forgot
# to override it. `None` here means "not set"; `_bootstrap_owner_account`
# below generates and logs a strong random password instead, but only
# on a genuine first boot (no existing owner account), so this is
# resolved at most once per deployment's lifetime, not on every
# restart. See docs/deployment.md for the operational impact.
BOOTSTRAP_OWNER_PASSWORD = os.environ.get("GCON_OWNER_PASSWORD") or None

class ManagementLayer:
    def __init__(self, coordinator=None, db_path=None):
        """
        db_path: where the durable SQLite store lives. Defaults to
        `Database`'s own default (env var GCON_DB_PATH, or
        data/gcon.db). Pass db_path=":memory:" for tests/ephemeral
        use that should NOT survive a restart on purpose.
        """
        self.coordinator = coordinator
        self.db = Database(db_path)

        self.user_registry = UserRegistry(db=self.db)
        self.org_registry = OrganizationRegistry(db=self.db)
        self.api_key_manager = APIKeyManager(db=self.db)
        self.audit_logger = AuditLogger(db=self.db)
        self.notification_center = NotificationCenter(db=self.db)
        self.session_manager = SessionManager(db=self.db)
        self.reset_token_manager = ResetTokenManager(db=self.db)

        self._bootstrap_owner_account()
        self._bridge_cluster_events()

    
    def _bootstrap_owner_account(self):
        """
        Create the one real account that exists on first boot, so
        there's a way to log in. No demo users, organizations, API
        keys, or audit entries are created.

        Persistence-aware: on a restart, the owner account is loaded
        back from the database (see UserRegistry.__init__), so this
        only actually creates anything — and only logs an audit
        entry — on a genuine first boot.

        If GCON_OWNER_PASSWORD isn't set, a strong random password is
        generated here and logged once (at WARNING level, so it isn't
        lost in routine INFO-level startup noise) -- never a fixed,
        guessable default. This only happens on a genuine first boot;
        on every later restart the (already-hashed, unrecoverable)
        existing password is left untouched and nothing is generated
        or logged.
        """
        is_first_boot = self.user_registry.get_user_by_email(BOOTSTRAP_OWNER_EMAIL) is None

        # Read live rather than the module-level BOOTSTRAP_OWNER_PASSWORD
        # constant: that constant is resolved once, at first import of
        # this module, which is too early in some processes (e.g. a
        # test suite, or any entry point that doesn't set the env var
        # before its first `import gcon...`). BOOTSTRAP_OWNER_PASSWORD
        # is kept around for backward-compatible introspection (and is
        # what existing tests that reload this module check), but the
        # actual bootstrap decision below always uses the current
        # environment.
        password = os.environ.get("GCON_OWNER_PASSWORD") or None
        if is_first_boot and not password:
            password = secrets.token_urlsafe(18)
            logger.warning(
                "No GCON_OWNER_PASSWORD set -- generated a random bootstrap "
                "owner password for '%s' on first boot: %s\n"
                "This is shown ONLY this once and is not recoverable (only "
                "its hash is stored). Log in and change it, or set "
                "GCON_OWNER_PASSWORD before the next first boot of a fresh "
                "deployment to control it directly.",
                BOOTSTRAP_OWNER_EMAIL, password,
            )
        elif not password:
            # Not first boot, and no env var set: bootstrap_owner_account()
            # is a no-op for an existing account (see users.py), so this
            # value is unused. Pass a random placeholder rather than None
            # to keep bootstrap_owner_account's signature simple.
            password = secrets.token_urlsafe(18)

        bootstrap_owner_account(
            self.user_registry,
            BOOTSTRAP_OWNER_NAME,
            BOOTSTRAP_OWNER_EMAIL,
            password,
    )
        if is_first_boot:
            self.audit_logger.log("System", "created user", BOOTSTRAP_OWNER_NAME)

    # Real coordinator events -> notifications. This replaces the old
    # seed_notifications() demo text: every notification below is
    # triggered by something that actually happened on the cluster.
    _EVENT_NOTIFICATIONS = {
        "NODE_OFFLINE": (
            "node_failure",
            lambda p: f"Node {p.get('node_id')} missed its heartbeat and was marked offline",
        ),
        "NODE_REGISTERED": (
            "node_registered",
            lambda p: f"Node {p.get('node_id')} registered with the cluster",
        ),
        "JOB_FAILED": (
            "job_failed",
            lambda p: f"Job {p.get('job_id')} failed"
            + (f": {p['error']}" if p.get("error") else ""),
        ),
        "RECEIPT_GENERATED": (
            "receipt_generated",
            lambda p: f"Receipt generated for job {p.get('job_id')}",
        ),
        "RECEIPT_VERIFICATION_FAILED": (
            "receipt_verification_failed",
            lambda p: f"Receipt {p.get('receipt_id')} for job {p.get('job_id')} failed verification",
        ),
        "RECEIPT_VERIFICATION_RECOVERED": (
            "receipt_verification_recovered",
            lambda p: f"Receipt {p.get('receipt_id')} for job {p.get('job_id')} is verified again",
        ),
        "POLICY_VIOLATION": (
            "policy_violation",
            lambda p: f"Job {p.get('job_id')} violated policy: "
            + "; ".join(p.get("failed_checks", [])),
        ),
        "HEALTH_DEGRADED": (
            "health_degraded",
            lambda p: f"Cluster health degraded: {p.get('reason')}",
        ),
        "HEALTH_CRITICAL": (
            "health_critical",
            lambda p: f"Cluster health is critical: {p.get('reason')}",
        ),
        "HEALTH_RECOVERED": (
            "health_recovered",
            lambda p: "Cluster health recovered to normal",
        ),
        "NODE_STAKE_SLASHED": (
            "node_stake_slashed",
            lambda p: f"Node {p.get('node_id')} was slashed {p.get('amount')} "
            f"stake units ({p.get('reason')})",
        ),
        "WEBHOOK_DELIVERY_FAILED": (
            "webhook_delivery_failed",
            lambda p: f"Webhook delivery to {p.get('url')} failed after "
            f"{p.get('attempt_count')} attempts: {p.get('error')}",
        ),
        "COORDINATOR_BECAME_LEADER": (
            "coordinator_became_leader",
            lambda p: f"This coordinator ({p.get('holder_id')}) became the active leader",
        ),
        "COORDINATOR_LOST_LEADERSHIP": (
            "coordinator_lost_leadership",
            lambda p: f"This coordinator ({p.get('holder_id')}) lost leadership, now standby",
        ),
    }

    def create_organization(self, name, plan="Standard"):
        org = self.org_registry.add_organization(name, plan)
        self.audit_logger.log("Admin", "created organization", org.name)
        data = org.to_dict()
        data["member_count"] = 0
        data["team_count"] = 0
        return data
    
    def create_team(self, org_id, name, admin_user_id=None):
        team = self.org_registry.add_team(org_id, name, admin_user_id)
        self.audit_logger.log("Admin", "created team", team.name)
        data = team.to_dict()
        data["member_count"] = 0
        return data
       
    def authenticate_api_key(self, secret, required_scope=None):
        """
        Validate a raw API key secret for the public API (see
        api_v1.py). Returns (key, owner_user) on success, raises
        ValueError on any failure (unknown key, revoked, expired,
        missing scope, or disabled owner) with a generic message so
        the failure reason can't be used to enumerate valid keys.
        """
        key = self.api_key_manager.find_by_secret(secret)
        if not key or not self.api_key_manager.is_valid(key):
            raise ValueError("Invalid or expired API key.")

        if required_scope and required_scope not in (key.scopes or []):
            raise ValueError(f"This API key does not have the '{required_scope}' scope.")

        owner = None
        try:
            owner = self.user_registry.get_user(key.owner_user_id)
        except ValueError:
            owner = None

        if owner is not None and owner.status != "Active":
            raise ValueError("Invalid or expired API key.")

        self.api_key_manager.mark_used(key)
        if owner is not None:
            self.user_registry.increment_stat(owner.user_id, "api_requests")

        return key, owner   
    
    def _bridge_cluster_events(self):
        """
        Subscribe the notification center to the coordinator's real
        event bus, so notifications reflect what's actually happening
        on the cluster (offline nodes, failed jobs, receipts) instead
        of canned sss text.
        """
        if not self.coordinator:
            return

        def handle(event):
            mapping = self._EVENT_NOTIFICATIONS.get(event.event_type)
            if not mapping:
                return
            notif_type, build_message = mapping
            self.notification_center.notify(notif_type, build_message(event.payload or {}))

        self.coordinator.event_bus.subscribe(handle)

    # ------------------------------------------------------------
    # Users
    # ------------------------------------------------------------

    def get_users(self):
        return [self._user_dict_with_live_stats(u) for u in self.user_registry.list_users()]

    def get_user(self, user_id):
        user = self.user_registry.get_user(user_id)
        return self._user_dict_with_live_stats(user)

    def _user_dict_with_live_stats(self, user):
        """
        A user's `to_dict()` carries whatever was last persisted to
        `stats`, which only covers counters that are incremented as a
        side effect elsewhere (login_count, api_requests). Job/workflow
        counters are computed fresh here from live coordinator state
        (see get_user_stats) so callers never see a stale or
        permanently-zero value for them.
        """
        data = user.to_dict()
        data["stats"] = self.get_user_stats(user.user_id)
        return data

    def create_user(self, name, email, role, organization_id=None, status="Active", password=None):
        if self.user_registry.get_user_by_email(email):
            raise ValueError(f"A user with email '{email}' already exists.")

        user = self.user_registry.add_user(name, email, role, organization_id, status)
        if password:
            user.set_password(password)
        self.audit_logger.log("Admin", "created user", user.name)
        self.notification_center.notify("user_registered", f"{user.name} was added")
        return user.to_dict()

    def update_user(self, user_id, **fields):
        user = self.user_registry.update_user(user_id, **fields)
        self.audit_logger.log("Admin", "updated user", user.name)
        return user.to_dict()

    def delete_user(self, user_id):
        user = self.user_registry.get_user(user_id)
        name = user.name
        self.user_registry.delete_user(user_id)
        self.org_registry.remove_user_everywhere(user_id)
        self.audit_logger.log("Admin", "deleted user", name)

    def set_user_status(self, user_id, status):
        user = self.user_registry.set_status(user_id, status)
        self.audit_logger.log("Admin", f"set status to {status}", user.name)
        return user.to_dict()

    def get_user_counts(self):
        return self.user_registry.counts()

    def get_user_stats(self, user_id):
        """
        Compute real, live per-user usage metrics from actual
        execution data -- never a hardcoded/illustrative value.

        Jobs, workflows and receipts are aggregated fresh from the
        coordinator's real in-memory state (filtered by
        `created_by`/`workflow_id`/`job_id` == this user's jobs) each
        time this is called, rather than maintained as a separately
        incremented counter, so there is no way for it to drift out
        of sync with what the cluster actually did. `login_count` and
        `api_requests` remain incremental counters on the User record
        itself (see UserRegistry.increment_stat) since they are not
        derivable from coordinator state.
        """
        user = self.user_registry.get_user(user_id)  # raises ValueError if unknown

        jobs = self.coordinator.get_jobs(created_by=user_id) if self.coordinator else []
        jobs_submitted = len(jobs)
        jobs_completed = sum(1 for j in jobs if j["status"] == "completed")
        jobs_failed = sum(1 for j in jobs if j["status"] in ("failed", "cancelled"))
        jobs_running = sum(1 for j in jobs if j["status"] in ("running", "pending"))

        workflows_created = 0
        if self.coordinator:
            workflows_created = sum(
                1 for state in self.coordinator.workflow_engine.states.values()
                if state.created_by == user_id
            )

        receipts = self.coordinator.get_receipts() if self.coordinator else []
        job_ids = {j["job_id"] for j in jobs}
        receipt_count = sum(1 for r in receipts if r.get("job_id") in job_ids)

        # Per-job CPU consumption isn't sampled/recorded anywhere in
        # the execution pipeline today (MetricsCollector reports
        # cluster-wide node CPU, not per-job attribution), so this
        # honestly reports 0 rather than fabricating a number, until
        # per-job resource accounting exists.
        cpu_usage = 0

        stats = dict(user.stats)
        stats.update({
            "jobs_submitted": jobs_submitted,
            "jobs_completed": jobs_completed,
            "jobs_failed": jobs_failed,
            "jobs_running": jobs_running,
            "workflows_created": workflows_created,
            "receipt_count": receipt_count,
            "cpu_usage": cpu_usage,
        })
        return stats

    # ------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------

    def login(self, email, password):
        """
        Verify credentials and start a session. Returns
        (session_token, user_dict) on success, raises ValueError
        on failure. Failures use the same generic message so login
        can't be used to enumerate valid emails.
        """
        user = self.user_registry.get_user_by_email(email)

        if not user or not user.check_password(password):
            self.audit_logger.log(email or "unknown", "failed login attempt")
            raise ValueError("Invalid email or password.")

        if user.status != "Active":
            self.audit_logger.log(user.name, f"blocked login attempt (status: {user.status})")
            raise ValueError(f"This account is {user.status.lower()} and cannot log in.")

        self.user_registry.touch_last_active(user.user_id)
        self.user_registry.increment_stat(user.user_id, "login_count")

        token = self.session_manager.create_session(user.user_id)
        self.audit_logger.log(user.name, "logged in")
        return token, user.to_dict()

    def logout(self, token):
        user_id = self.session_manager.get_user_id(token)
        if user_id:
            user = self.user_registry.get_user(user_id)
            self.audit_logger.log(user.name, "logged out")
        self.session_manager.destroy_session(token)

    def get_current_user(self, token):
        """
        Return the user dict for a valid session token, or None.
        """
        user_id = self.session_manager.get_user_id(token)
        if not user_id:
            return None
        try:
            return self.user_registry.get_user(user_id)
        except ValueError:
            return None

    def change_password(self, user_id, current_password, new_password):
        user = self.user_registry.get_user(user_id)
        if not user.check_password(current_password):
            raise ValueError("Current password is incorrect.")
        user.set_password(new_password)
        self.session_manager.destroy_all_for_user(user_id)
        self.audit_logger.log(user.name, "changed password")
        self.notification_center.notify("password_changed", f"{user.name} changed their password")

    def set_password(self, user_id, new_password):
        """
        Admin-initiated password set (e.g. right after creating a
        user), no current password required.
        """
        user = self.user_registry.get_user(user_id)
        user.set_password(new_password)
        self.session_manager.destroy_all_for_user(user_id)
        self.audit_logger.log("Admin", "set password for", user.name)

    def signup(self, name, email, role, password, organization_id=None):
        """
        Self-service account creation (no auth required to call
        this). Unlike create_user() -- which is an admin action gated
        by the "Manage users" permission -- a signup always lands
        with status="Pending", regardless of the role requested, so
        a new account can never log in and act until an existing
        admin/owner reviews and activates it via set_user_status().
        """
        if not email or not password:
            raise ValueError("Email and password are required.")
        if role not in rbac.ROLES:
            raise ValueError(f"Invalid role '{role}'.")
        if self.user_registry.get_user_by_email(email):
            raise ValueError(f"A user with email '{email}' already exists.")

        user = self.user_registry.add_user(
            name, email, role, organization_id, status="Pending",
        )
        user.set_password(password)
        self.user_registry._persist(user)
        self.audit_logger.log(user.name, "signed up (pending approval)")
        self.notification_center.notify(
            "user_registered", f"{user.name} signed up and is awaiting approval",
        )
        return user.to_dict()

    def request_password_reset(self, email):
        """
        Start a self-service password reset. Always returns None
        with no error, whether or not the email matches an account,
        so this endpoint can't be used to enumerate registered
        emails. If it does match, logs the reset token at WARNING
        level (there is no outbound email integration yet -- see
        docs/deployment.md) so an operator can hand it to the user
        out of band; a real deployment should replace this log line
        with an actual email send.
        """
        user = self.user_registry.get_user_by_email(email)
        if not user:
            return
        token = self.reset_token_manager.create_token(user.user_id)
        logger.warning(
            "Password reset requested for '%s'. Reset token (valid %d minutes): %s\n"
            "No email integration is configured -- deliver this token to the "
            "user out of band, e.g. a link to /reset-password?token=%s",
            email, self.reset_token_manager.ttl_minutes, token, token,
        )
        self.audit_logger.log(user.name, "requested password reset")

    def reset_password(self, token, new_password):
        """
        Complete a self-service password reset. Raises ValueError on
        an invalid/expired/already-used token.
        """
        user_id = self.reset_token_manager.get_user_id(token)
        if not user_id:
            raise ValueError("This reset link is invalid or has expired.")

        user = self.user_registry.get_user(user_id)
        user.set_password(new_password)
        self.user_registry._persist(user)
        self.reset_token_manager.consume_token(token)
        self.session_manager.destroy_all_for_user(user_id)
        self.audit_logger.log(user.name, "reset their password")
        self.notification_center.notify(
            "password_changed", f"{user.name} reset their password",
        )

    def get_user_sessions(self, user_id):
        """
        List active-session metadata (no raw tokens) for a user, for
        the admin session-controls UI.
        """
        self.user_registry.get_user(user_id)  # raises ValueError if unknown
        return self.session_manager.list_active_for_user(user_id)

    def force_logout_user(self, user_id):
        """
        Admin-initiated: invalidate every active session for a user
        without touching their password (unlike set_password, which
        also does this as a side effect of a reset).
        """
        user = self.user_registry.get_user(user_id)
        self.session_manager.destroy_all_for_user(user_id)
        self.audit_logger.log("Admin", "force logged out", user.name)

    def user_has_permission(self, user, permission):
        if user is None:
            return False
        return permission in rbac.get_permissions_for_role(user.role)

    def require_permission(self, user, permission):
        if not self.user_has_permission(user, permission):
            raise PermissionError(
                f"'{permission}' permission is required for this action."
            )

    # ------------------------------------------------------------
    # Organizations & Teams
    # ------------------------------------------------------------

    def get_organizations(self):
        orgs = []
        for org in self.org_registry.list_organizations():
            data = org.to_dict()
            members = [u for u in self.user_registry.list_users()
                       if u.organization_id == org.org_id]
            data["member_count"] = len(members)
            data["team_count"] = len(self.org_registry.list_teams(org.org_id))
            orgs.append(data)
        return orgs

    def get_organization(self, org_id):
        org = self.org_registry.get_organization(org_id)
        data = org.to_dict()
        members = [u for u in self.user_registry.list_users()
                   if u.organization_id == org.org_id]
        data["member_count"] = len(members)
        data["team_count"] = len(self.org_registry.list_teams(org.org_id))
        return data

    def get_org_usage_summary(self):
        """
        Per-company rollup for the dashboard's Companies panel: which
        organizations have nodes online right now, and the status
        breakdown of their jobs (pending/running/completed/failed/
        cancelled), plus whatever compute/token usage those jobs
        reported (see GCONAgent.execute_job's usage_report_path /
        GCONCoordinator.get_jobs()'s "usage" field).

        Every count here comes from a live query against the
        coordinator's real node registry and job list, filtered by
        org_id -- never a cached or incrementally-maintained counter,
        so it can't drift out of sync with what's actually running.
        Organizations with no dedicated nodes and no jobs yet are
        still listed (zeroed out), since "onboarded but not yet
        provisioned" is a real, useful state to see on the dashboard.

        `org_id` on nodes/jobs is populated only for nodes started
        with --org-id (see run_worker.py) and jobs submitted through a
        path that resolves the submitter's organization (see
        api_v1.py's submit_job route) -- rows submitted before this
        feature existed, or through a path that doesn't yet resolve an
        org, will have org_id = None and simply won't be attributed to
        any company here.
        """
        if self.coordinator is None:
            return []

        all_nodes = self.coordinator.get_nodes()
        all_jobs = self.coordinator.get_jobs()

        summaries = []
        for org in self.org_registry.list_organizations():
            org_nodes = [n for n in all_nodes if n.get("org_id") == org.org_id]
            org_jobs = [j for j in all_jobs if j.get("org_id") == org.org_id]

            nodes_online = sum(1 for n in org_nodes if n.get("status") in ("idle", "busy"))

            job_status_counts = {
                "pending": 0, "running": 0, "completed": 0,
                "failed": 0, "cancelled": 0,
            }
            for job in org_jobs:
                status = job.get("status", "")
                if status in job_status_counts:
                    job_status_counts[status] += 1

            compute_seconds = 0.0
            llm_input_tokens = 0
            llm_output_tokens = 0
            jobs_with_usage = 0
            for job in org_jobs:
                # Automatically-measured runtime -- always available
                # once a job finishes, no cooperation from the job
                # required. This is the real compute-usage signal.
                compute_seconds += float(job.get("runtime_seconds") or 0)

                usage = job.get("usage")
                if not isinstance(usage, dict):
                    continue
                jobs_with_usage += 1
                tokens = usage.get("llm_tokens")
                if isinstance(tokens, dict):
                    llm_input_tokens += int(tokens.get("input", 0) or 0)
                    llm_output_tokens += int(tokens.get("output", 0) or 0)

            summaries.append({
                "org_id": org.org_id,
                "name": org.name,
                "plan": getattr(org, "plan", None),
                "nodes_online": nodes_online,
                "nodes_total": len(org_nodes),
                "jobs": job_status_counts,
                "jobs_total": len(org_jobs),
                "usage": {
                    "jobs_reporting_usage": jobs_with_usage,
                    "llm_input_tokens": llm_input_tokens,
                    "llm_output_tokens": llm_output_tokens,
                    "compute_seconds": compute_seconds,
                },
            })

        return summaries

    # ------------------------------------------------------------------
    # Key rotation (HMAC receipt-signing keys + mTLS certs) -- see
    # gcon.execution.hmac_keyring / gcon.transport.tls_rotation for the
    # actual mechanics; this is just the dashboard-facing surface over
    # the coordinator's already-live verifier and cert directory.
    # ------------------------------------------------------------------
    def get_key_rotation_status(self):
        """Never returns a raw secret -- HmacKeyring.list_keys() is
        metadata-only by construction, and the mTLS side only ever
        surfaces fingerprints."""
        status = {"hmac": None, "mtls": None}
        if self.coordinator is not None and getattr(self.coordinator, "verifier", None):
            verifier = self.coordinator.verifier
            keyring = getattr(verifier, "_keyring", None)
            if keyring is not None:
                status["hmac"] = {
                    "current_key_id": keyring.current_key_id,
                    "keys": keyring.list_keys(),
                }
        cert_dir = self._tls_cert_dir()
        if cert_dir:
            from gcon.transport import tls, tls_rotation
            import os as _os
            ca_cert_path = _os.path.join(cert_dir, tls.CA_CERT_FILE)
            mtls = {"active_ca_fingerprint": None, "retired_cas": [], "revoked_certificates": []}
            if _os.path.exists(ca_cert_path):
                mtls["active_ca_fingerprint"] = tls.cert_fingerprint(ca_cert_path)
            mtls["retired_cas"] = tls_rotation._load_manifest(cert_dir)["retired_cas"]
            mtls["revoked_certificates"] = [
                {"fingerprint": fp, **meta}
                for fp, meta in tls_rotation._load_revocations(cert_dir).items()
            ]
            status["mtls"] = mtls
        return status

    def rotate_hmac_key(self):
        if self.coordinator is None or not getattr(self.coordinator, "verifier", None):
            raise ValueError("No coordinator/verifier available to rotate.")
        new_key_id = self.coordinator.verifier.rotate_key()
        self.audit_logger.log("Admin", "rotated HMAC signing key", new_key_id)
        return self.get_key_rotation_status()

    def rotate_mtls_ca(self):
        cert_dir = self._tls_cert_dir()
        if not cert_dir:
            raise ValueError("No TLS cert directory configured to rotate.")
        from gcon.transport import tls_rotation
        hostname = self._tls_hostname()
        new_fp = tls_rotation.rotate_ca(cert_dir, hostname=hostname)
        self.audit_logger.log("Admin", "rotated mTLS cluster CA", new_fp[:16])
        return self.get_key_rotation_status()

    def revoke_node_certificate(self, node_id, reason=""):
        cert_dir = self._tls_cert_dir()
        if not cert_dir:
            raise ValueError("No TLS cert directory configured.")
        from gcon.transport import tls_rotation
        fingerprint = tls_rotation.revoke_node_cert(cert_dir, node_id, reason=reason)
        self.audit_logger.log("Admin", f"revoked mTLS certificate for node '{node_id}'", reason or "")
        return {"node_id": node_id, "fingerprint": fingerprint}

    def _tls_cert_dir(self):
        if self.coordinator is None:
            return None
        control_plane = getattr(self.coordinator, "control_plane", None)
        if control_plane is None:
            return None
        from gcon.transport.config import TransportConfig
        try:
            return TransportConfig.load(control_plane).tls_cert_dir
        except Exception:
            return None

    def _tls_hostname(self):
        cert_dir = self._tls_cert_dir()
        if not cert_dir:
            return "localhost"
        control_plane = getattr(self.coordinator, "control_plane", None)
        from gcon.transport.config import TransportConfig
        try:
            host = TransportConfig.load(control_plane).grpc_host
            return host if host not in ("0.0.0.0", "") else "localhost"
        except Exception:
            return "localhost"

    # ------------------------------------------------------------------
    # Staking (see gcon.execution.staking / persistence.repositories
    # .staking) -- bonded deposits per node, slashed on failed/
    # fraudulent receipt verification. Read-only unless staking is
    # actually enabled (GCON_STAKING_REQUIRED); bond/unbond are
    # allowed regardless so an operator can build up stake ahead of
    # turning enforcement on.
    # ------------------------------------------------------------------
    def get_node_stakes(self):
        if self.coordinator is None or getattr(self.coordinator, "stake_ledger", None) is None:
            return []
        ledger = self.coordinator.stake_ledger
        nodes_by_id = {}
        if self.coordinator.control_plane is not None:
            for n in self.coordinator.control_plane.nodes.list_all():
                nodes_by_id[n["node_id"]] = n
        out = []
        for row in ledger.list_all():
            node = nodes_by_id.get(row["node_id"], {})
            out.append({
                **row,
                "hostname": node.get("hostname"),
                "status": node.get("status"),
                "meets_minimum": ledger.meets_minimum(row["node_id"]),
            })
        return out

    def get_stake_events(self, node_id=None, limit=200):
        if self.coordinator is None or getattr(self.coordinator, "stake_ledger", None) is None:
            return []
        return self.coordinator.stake_ledger.list_events(node_id=node_id, limit=limit)

    def get_staking_config(self):
        if self.coordinator is None or getattr(self.coordinator, "stake_ledger", None) is None:
            return {"enabled": False, "min_stake_required": 0, "slash_fraction": 0,
                    "unbonding_period_days": 0}
        ledger = self.coordinator.stake_ledger
        return {
            "enabled": ledger.staking_required,
            "min_stake_required": ledger.min_stake_required,
            "slash_fraction": ledger.slash_fraction,
            "unbonding_period_days": ledger.unbonding_period_days,
        }

    def bond_node_stake(self, node_id, amount):
        if self.coordinator is None or getattr(self.coordinator, "stake_ledger", None) is None:
            raise ValueError("Staking is not available on this coordinator.")
        result = self.coordinator.stake_ledger.bond(node_id, int(amount))
        self.audit_logger.log("Admin", f"bonded {amount} stake units for node '{node_id}'", "")
        return result

    def request_unbond_node_stake(self, node_id, amount):
        if self.coordinator is None or getattr(self.coordinator, "stake_ledger", None) is None:
            raise ValueError("Staking is not available on this coordinator.")
        result = self.coordinator.stake_ledger.request_unbond(node_id, int(amount))
        self.audit_logger.log("Admin", f"requested unbond of {amount} stake units for node '{node_id}'", "")
        return result

    # ------------------------------------------------------------------
    # Billing (see gcon.billing.invoicing/pricing/providers) -- invoice
    # generation from real usage-metering data. No real payment
    # provider is wired (see providers.py's docstring); finalize_
    # invoice_now uses whatever GCON_PAYMENT_PROVIDER resolves to,
    # which is the mock provider unless an operator has configured
    # something else.
    # ------------------------------------------------------------------
    def get_invoices(self, org_id=None, limit=100):
        if self.coordinator is None or self.coordinator.control_plane is None:
            return []
        if org_id:
            return self.coordinator.control_plane.invoices.list_for_org(org_id, limit=limit)
        return self.coordinator.control_plane.invoices.list_all(limit=limit)

    def get_pricing(self):
        if self.coordinator is None or self.coordinator.control_plane is None:
            from gcon.billing.pricing import load_pricing
            return load_pricing(None).to_dict()
        from gcon.billing.pricing import load_pricing
        return load_pricing(self.coordinator.control_plane).to_dict()

    def generate_invoice_now(self, org_id, period_start, period_end):
        if self.coordinator is None or self.coordinator.control_plane is None:
            raise ValueError("No control plane available to generate an invoice from.")
        from gcon.billing.invoicing import generate_invoice
        invoice = generate_invoice(self.coordinator.control_plane, org_id, period_start, period_end)
        self.audit_logger.log("Admin", f"generated invoice for org '{org_id}'",
                               f"{period_start} to {period_end}")
        return invoice

    def finalize_invoice_now(self, invoice_id):
        if self.coordinator is None or self.coordinator.control_plane is None:
            raise ValueError("No control plane available.")
        from gcon.billing.invoicing import finalize_invoice
        invoice = finalize_invoice(self.coordinator.control_plane, invoice_id)
        self.audit_logger.log("Admin", f"finalized invoice '{invoice_id}'", invoice.get("status", ""))
        return invoice

    # ------------------------------------------------------------------
    # Webhooks (see gcon.transport.webhooks) -- standing org-level
    # subscriptions and their delivery history. Per-job ad-hoc
    # callback_url deliveries also land in webhook_deliveries but have
    # no subscription to manage here; they show up in
    # get_webhook_deliveries via job_id filtering instead.
    # ------------------------------------------------------------------
    def get_webhook_subscriptions(self, org_id=None):
        """Secrets are masked here, same convention as
        get_api_keys()/APIKey.to_dict -- only create_webhook_
        subscription (the moment of creation) reveals the real
        secret, since that's the one time the caller needs it to
        configure their receiving endpoint's signature verification."""
        if self.coordinator is None or self.coordinator.control_plane is None:
            subs = []
        elif org_id:
            subs = self.coordinator.control_plane.webhooks.list_for_org(org_id, active_only=False)
        else:
            # No org filter: aggregate across every org that has one.
            # There's no list-all on the repository (subscriptions are
            # always scoped to an org in the UI) -- callers that want a
            # single org's subscriptions should pass org_id.
            all_orgs = self.org_registry.list_organizations()
            subs = []
            for org in all_orgs:
                subs.extend(self.coordinator.control_plane.webhooks.list_for_org(org.org_id, active_only=False))
        for sub in subs:
            sub["secret"] = _mask_webhook_secret(sub["secret"])
        return subs

    def create_webhook_subscription(self, org_id, url, event_types):
        if self.coordinator is None or self.coordinator.control_plane is None:
            raise ValueError("No control plane available.")
        sub = self.coordinator.control_plane.webhooks.create_subscription(org_id, url, event_types)
        self.audit_logger.log("Admin", f"created webhook subscription for org '{org_id}'", url)
        # Secret is only ever revealed at creation time -- see
        # get_webhook_subscriptions's masking.
        return sub

    def deactivate_webhook_subscription(self, subscription_id):
        if self.coordinator is None or self.coordinator.control_plane is None:
            raise ValueError("No control plane available.")
        self.coordinator.control_plane.webhooks.deactivate(subscription_id)
        self.audit_logger.log("Admin", "deactivated webhook subscription", subscription_id)

    def get_webhook_deliveries(self, subscription_id=None, job_id=None, limit=50):
        if self.coordinator is None or self.coordinator.control_plane is None:
            return []
        if job_id:
            return self.coordinator.control_plane.webhooks.list_for_job(job_id)
        now_iso = datetime.now(UTC).isoformat()
        # due_deliveries surfaces pending/retrying; for a dashboard
        # history view we want everything recent regardless of
        # status, so query directly rather than reusing that method.
        rows = self.coordinator.control_plane.db.query(
            "SELECT * FROM webhook_deliveries "
            + ("WHERE subscription_id = ? " if subscription_id else "")
            + "ORDER BY created_at DESC LIMIT ?",
            (subscription_id, limit) if subscription_id else (limit,),
        )
        import json as _json
        out = []
        for r in rows:
            d = dict(r)
            d["payload"] = _json.loads(d.pop("payload_json"))
            out.append(d)
        return out

    # ------------------------------------------------------------------
    # HA / leader election status (see gcon.cluster.leader_election) --
    # read-only: starting/stopping HA itself is a --ha flag on
    # run_coordinator.py, not something toggled from the dashboard of
    # a process that's already running one way or the other.
    # ------------------------------------------------------------------
    def get_ha_status(self):
        elector = getattr(self.coordinator, "leader_elector", None) if self.coordinator else None
        if elector is None:
            return {"enabled": False}
        lease = elector.control_plane.leases.read(elector.lease_name)
        return {
            "enabled": True,
            "holder_id": elector.holder_id,
            "is_leader": elector.is_leader,
            "lease": lease,
        }

    def update_organization(self, org_id, **fields):
        org = self.org_registry.update_organization(org_id, **fields)
        self.audit_logger.log("Admin", "updated organization", org.name)
        return self.get_organization(org.org_id)

    def delete_organization(self, org_id):
        org = self.org_registry.get_organization(org_id)
        name = org.name
        self.org_registry.delete_organization(org_id, user_registry=self.user_registry)
        self.audit_logger.log("Admin", "deleted organization", name)

    def remove_organization_membership(self, org_id, user_id):
        """
        Detach a user from an organization.

        `update_user(organization_id=None)` can't be used for this:
        UserRegistry.update_user() treats None as "field not supplied"
        (partial-update semantics), so it silently no-ops instead of
        clearing organization_id. This bypasses that convention and
        writes the clear directly, then persists it.
        """
        user = self.user_registry.get_user(user_id)
        if user.organization_id != org_id:
            raise ValueError(
                f"User '{user_id}' does not belong to organization '{org_id}'."
            )
        user.organization_id = None
        self.user_registry._persist(user)
        self.audit_logger.log("Admin", "removed organization membership", user.name)
        return self.get_user(user_id)

    def get_teams(self):
        teams = []
        for team in self.org_registry.list_teams():
            data = team.to_dict()
            data["member_count"] = len(team.member_ids)
            teams.append(data)
        return teams

    def get_team(self, team_id):
        team = self.org_registry.get_team(team_id)
        data = team.to_dict()
        data["member_count"] = len(team.member_ids)
        return data

    def update_team(self, team_id, **fields):
        team = self.org_registry.update_team(team_id, **fields)
        self.audit_logger.log("Admin", "updated team", team.name)
        return self.get_team(team.team_id)

    def delete_team(self, team_id):
        team = self.org_registry.get_team(team_id)
        name = team.name
        self.org_registry.delete_team(team_id)
        self.audit_logger.log("Admin", "deleted team", name)

    def add_team_member(self, team_id, user_id):
        # Validate the user actually exists rather than letting a
        # bad user_id silently sit in member_ids_json forever.
        self.user_registry.get_user(user_id)
        team = self.org_registry.add_member(team_id, user_id)
        self.audit_logger.log("Admin", "added team member", team.name)
        return self.get_team(team.team_id)

    def remove_team_member(self, team_id, user_id):
        team = self.org_registry.remove_member(team_id, user_id)
        self.audit_logger.log("Admin", "removed team member", team.name)
        return self.get_team(team.team_id)

    # ------------------------------------------------------------
    # RBAC
    # ------------------------------------------------------------

    def get_roles(self):
        return rbac.ROLES

    def get_permissions(self):
        return rbac.PERMISSIONS

    def get_permission_matrix(self):
        return rbac.get_permission_matrix()

    # ------------------------------------------------------------
    # API Keys
    # ------------------------------------------------------------

    def get_api_keys(self):
        return [k.to_dict() for k in self.api_key_manager.list_keys()]

    def create_api_key(self, name, owner_user_id, scopes=None, expires_in_days=90):
        key = self.api_key_manager.create_key(name, owner_user_id, scopes, expires_in_days)
        self.audit_logger.log("Admin", "generated API key", key.name)
        self.notification_center.notify("api_key_created", f"API key '{key.name}' was created")
        # Secret is only ever revealed at creation time.
        return key.to_dict(reveal_secret=True)

    def revoke_api_key(self, key_id):
        key = self.api_key_manager.revoke_key(key_id)
        self.audit_logger.log("Admin", "revoked API key", key.name)
        return key.to_dict()

    def regenerate_api_key(self, key_id):
        key = self.api_key_manager.regenerate_key(key_id)
        self.audit_logger.log("Admin", "regenerated API key", key.name)
        return key.to_dict(reveal_secret=True)

    # ------------------------------------------------------------
    # Audit log & notifications
    # ------------------------------------------------------------

    def get_audit_logs(self, limit=100):
        return self.audit_logger.list_entries(limit)

    def get_notifications(self, limit=50):
        return self.notification_center.list_entries(limit)

    def get_unread_notification_count(self):
        return self.notification_center.unread_count()

    def get_unread_notification_count_by_severity(self):
        return self.notification_center.unread_count_by_severity()

    def mark_notification_read(self, notification_id):
        return self.notification_center.mark_read(notification_id)

    def mark_all_notifications_read(self):
        return self.notification_center.mark_all_read()

    # ------------------------------------------------------------
    # Dashboard cards
    # ------------------------------------------------------------

    def get_dashboard_cards(self):
        user_counts = self.user_registry.counts()

        total_workflows = sum(
            u.stats.get("workflows_created", 0) for u in self.user_registry.list_users()
        )
        active_keys = sum(
            1 for k in self.api_key_manager.list_keys() if k.status == "Active"
        )

        return {
            "total_users": user_counts["total"],
            "active_users": user_counts["active"],
            "organizations": len(self.org_registry.list_organizations()),
            "api_keys": len(self.api_key_manager.list_keys()),
            "active_sessions": self.session_manager.count_active(),
            "total_workflows": total_workflows,
            "active_api_keys": active_keys,
        }

    # ------------------------------------------------------------
    # Search
    # ------------------------------------------------------------

    def search(self, query):
        if not query:
            return {"users": [], "organizations": [], "api_keys": [], "jobs": [], "nodes": []}

        q = query.lower()
        results = {
            "users": [
                u.to_dict() for u in self.user_registry.list_users()
                if q in u.name.lower() or q in u.email.lower() or q in u.user_id.lower()
            ],
            "organizations": [
                o.to_dict() for o in self.org_registry.list_organizations()
                if q in o.name.lower()
            ],
            "api_keys": [
                k.to_dict() for k in self.api_key_manager.list_keys()
                if q in k.name.lower()
            ],
            "jobs": [],
            "nodes": [],
        }

        if self.coordinator:
            results["jobs"] = [
                j for j in self.coordinator.get_jobs()
                if q in j["job_id"].lower() or q in (j["status"] or "").lower()
            ]
            results["nodes"] = [
                n for n in self.coordinator.get_nodes()
                if q in n["node_id"].lower() or q in (n["status"] or "").lower()
            ]

        return results

    # ------------------------------------------------------------
    # Export
    # ------------------------------------------------------------

    def export(self, entity, fmt):
        exporters = {
            "users": self.get_users,
            "organizations": self.get_organizations,
            "api_keys": self.get_api_keys,
            "audit_logs": self.get_audit_logs,
        }
        if entity not in exporters:
            raise ValueError(f"Unknown export entity '{entity}'.")

        rows = exporters[entity]()

        if fmt == "json":
            return json.dumps(rows, indent=2), "application/json", f"{entity}.json"

        if fmt == "csv":
            if not rows:
                return "", "text/csv", f"{entity}.csv"
            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for row in rows:
                writer.writerow({k: json.dumps(v) if isinstance(v, (dict, list)) else v
                                  for k, v in row.items()})
            return buffer.getvalue(), "text/csv", f"{entity}.csv"

        raise ValueError(f"Unsupported export format '{fmt}'.")