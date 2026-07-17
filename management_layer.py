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
from datetime import datetime, UTC

import rbac
from auth import SessionManager
from users import UserRegistry, bootstrap_owner_account
from organizations import OrganizationRegistry
from api_keys import APIKeyManager
from audit_log import AuditLogger
from notifications import NotificationCenter
import os

# Bootstrap owner account, created once on first boot. Overridable
# via environment variables for deployments that don't want this
# exact identity hard-coded in source control.
BOOTSTRAP_OWNER_NAME = os.environ.get("GCON_OWNER_NAME", "Briton Nyongesa")
BOOTSTRAP_OWNER_EMAIL = os.environ.get("GCON_OWNER_EMAIL", "nyongesabriton620@gmail.com")
BOOTSTRAP_OWNER_PASSWORD = os.environ.get("GCON_OWNER_PASSWORD", "GCON2024")

class ManagementLayer:
    def __init__(self, coordinator=None):
        self.coordinator = coordinator

        self.user_registry = UserRegistry()
        self.org_registry = OrganizationRegistry()
        self.api_key_manager = APIKeyManager()
        self.audit_logger = AuditLogger()
        self.notification_center = NotificationCenter()
        self.session_manager = SessionManager()

        self._bootstrap_owner_account()
        self._bridge_cluster_events()

    
    def _bootstrap_owner_account(self):
        """
        Create the one real account that exists on first boot, so
        there's a way to log in. No demo users, organizations, API
        keys, or audit entries are created.
        """
        bootstrap_owner_account(
            self.user_registry,
            BOOTSTRAP_OWNER_NAME,
            BOOTSTRAP_OWNER_EMAIL,
            BOOTSTRAP_OWNER_PASSWORD,
    )
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

        key.mark_used()
        if owner is not None:
            owner.stats["api_requests"] += 1

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
        return [u.to_dict() for u in self.user_registry.list_users()]

    def get_user(self, user_id):
        return self.user_registry.get_user(user_id).to_dict()

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
        self.audit_logger.log("Admin", "deleted user", name)

    def set_user_status(self, user_id, status):
        user = self.user_registry.set_status(user_id, status)
        self.audit_logger.log("Admin", f"set status to {status}", user.name)
        return user.to_dict()

    def get_user_counts(self):
        return self.user_registry.counts()

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

        user.last_active = datetime.now(UTC)
        user.stats["login_count"] += 1

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

    def get_teams(self):
        teams = []
        for team in self.org_registry.list_teams():
            data = team.to_dict()
            data["member_count"] = len(team.member_ids)
            teams.append(data)
        return teams

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

    def mark_notification_read(self, notification_id):
        return self.notification_center.mark_read(notification_id)

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
            "active_sessions": user_counts["active"],  # no real session tracking yet
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
