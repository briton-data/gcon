"""
GCON Users — user records and an in-memory user registry.

GCON has no authentication system yet, so this registry is seeded
with illustrative demo users rather than backed by real accounts.
Per-user job/workflow/usage statistics are likewise demo figures,
since the coordinator does not currently attribute jobs to users.
"""

from datetime import datetime, UTC, timedelta
from uuid import uuid4

from auth import hash_password, verify_password

VALID_STATUSES = ["Active", "Pending", "Suspended", "Disabled"]


def _initials(name):
    parts = name.split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[-1][0]).upper()
    return name[:2].upper()


class User:
    def __init__(self, name, email, role, organization_id=None,
                 status="Active", user_id=None, created_at=None):
        self.user_id = user_id or f"usr_{uuid4().hex[:8]}"
        self.name = name
        self.email = email
        self.role = role
        self.organization_id = organization_id
        self.status = status
        self.avatar_initials = _initials(name)
        self.created_at = created_at or datetime.now(UTC)
        self.last_active = datetime.now(UTC)
        self.password_hash = None

        # Demo/seed usage figures — GCON does not yet attribute real
        # jobs or workflows to individual users.
        self.stats = {
            "jobs_submitted": 0,
            "jobs_running": 0,
            "jobs_failed": 0,
            "jobs_completed": 0,
            "workflows_created": 0,
            "cpu_usage": 0,
            "storage_usage_gb": 0,
            "api_requests": 0,
            "login_count": 0,
        }

    def set_password(self, password):
        self.password_hash = hash_password(password)

    def check_password(self, password):
        if not self.password_hash:
            return False
        return verify_password(password, self.password_hash)

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "name": self.name,
            "email": self.email,
            "role": self.role,
            "organization_id": self.organization_id,
            "status": self.status,
            "avatar_initials": self.avatar_initials,
            "created_at": self.created_at.isoformat(),
            "last_active": self.last_active.isoformat(),
            "has_password": self.password_hash is not None,
            "stats": self.stats,
        }


class UserRegistry:
    def __init__(self):
        self.users = {}

    def add_user(self, name, email, role, organization_id=None, status="Active"):
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'.")
        user = User(name, email, role, organization_id, status)
        self.users[user.user_id] = user
        return user

    def get_user(self, user_id):
        if user_id not in self.users:
            raise ValueError(f"User '{user_id}' does not exist.")
        return self.users[user_id]

    def get_user_by_email(self, email):
        email = email.lower().strip()
        for user in self.users.values():
            if user.email.lower() == email:
                return user
        return None

    def update_user(self, user_id, **fields):
        user = self.get_user(user_id)
        for key, value in fields.items():
            if value is None:
                continue
            if key == "status" and value not in VALID_STATUSES:
                raise ValueError(f"Invalid status '{value}'.")
            setattr(user, key, value)
        return user

    def delete_user(self, user_id):
        self.get_user(user_id)
        del self.users[user_id]

    def set_status(self, user_id, status):
        return self.update_user(user_id, status=status)

    def touch_last_active(self, user_id):
        user = self.get_user(user_id)
        user.last_active = datetime.now(UTC)
        return user

    def list_users(self):
        return list(self.users.values())

    def counts(self):
        users = self.list_users()
        return {
            "total": len(users),
            "active": sum(1 for u in users if u.status == "Active"),
            "inactive": sum(1 for u in users if u.status != "Active"),
        }


def seed_users(registry, organization_ids):
    """
    Populate the registry with illustrative demo users.
    """
    demo = [
        ("Avery Chen", "avery.chen@example.com", "Owner", "Active"),
        ("Priya Nair", "priya.nair@example.com", "Administrator", "Active"),
        ("Marcus Webb", "marcus.webb@example.com", "Operator", "Active"),
        ("Sofia Ramirez", "sofia.ramirez@example.com", "Developer", "Active"),
        ("Ken Osei", "ken.osei@example.com", "Developer", "Pending"),
        ("Lena Novak", "lena.novak@example.com", "Viewer", "Suspended"),
        ("Tariq Hassan", "tariq.hassan@example.com", "Operator", "Disabled"),
    ]

    orgs = list(organization_ids) or [None]
    created = []

    for i, (name, email, role, status) in enumerate(demo):
        user = registry.add_user(
            name, email, role,
            organization_id=orgs[i % len(orgs)],
            status=status,
        )
        # Demo credential — every seeded account uses this password so
        # the login flow can be tried out. Real accounts created via
        # the "Add User" form must have their own password set.
        user.set_password("gcon-demo-2026")
        # Vary last_active for realism.
        user.last_active = datetime.now(UTC) - timedelta(hours=i * 7)
        user.stats.update({
            "jobs_submitted": (i + 1) * 12,
            "jobs_running": i % 3,
            "jobs_failed": i % 4,
            "jobs_completed": (i + 1) * 10,
            "workflows_created": i + 1,
            "cpu_usage": round(10 + i * 7.5, 1),
            "storage_usage_gb": round(1.2 + i * 3.4, 1),
            "api_requests": (i + 1) * 84,
            "login_count": (i + 1) * 5,
        })
        created.append(user)

    return created
