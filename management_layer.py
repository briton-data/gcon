"""
GCON Management Layer.

A sibling to the cluster PresentationLayer: handles platform
management concerns (users, organizations, RBAC, API keys, audit
log, notifications) rather than cluster operations.

GCON has no authentication system yet, so this layer operates on
seeded, in-memory demo data. It's built to demonstrate the intended
UI/API shape for a future real implementation, not to be a source
of truth for production access control.
"""

import csv
import io
import json

import rbac
from users import UserRegistry, seed_users
from organizations import OrganizationRegistry, seed_organizations
from api_keys import APIKeyManager, seed_api_keys
from audit_log import AuditLogger, seed_audit_log
from notifications import NotificationCenter, seed_notifications


class ManagementLayer:
    def __init__(self, coordinator=None):
        self.coordinator = coordinator

        self.user_registry = UserRegistry()
        self.org_registry = OrganizationRegistry()
        self.api_key_manager = APIKeyManager()
        self.audit_logger = AuditLogger()
        self.notification_center = NotificationCenter()

        self._seed_demo_data()

    def _seed_demo_data(self):
        orgs = seed_organizations(self.org_registry)
        org_ids = [o.org_id for o in orgs]
        users = seed_users(self.user_registry, org_ids)
        seed_api_keys(self.api_key_manager, users)
        seed_audit_log(self.audit_logger)
        seed_notifications(self.notification_center)

    # ------------------------------------------------------------
    # Users
    # ------------------------------------------------------------

    def get_users(self):
        return [u.to_dict() for u in self.user_registry.list_users()]

    def get_user(self, user_id):
        return self.user_registry.get_user(user_id).to_dict()

    def create_user(self, name, email, role, organization_id=None, status="Active"):
        user = self.user_registry.add_user(name, email, role, organization_id, status)
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
