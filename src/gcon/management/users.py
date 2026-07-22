"""
GCON Users — user records and a persistent user registry.

Authentication is real (PBKDF2-HMAC-SHA256, see auth.py). On first
boot the registry is bootstrapped with exactly one real account (the
platform owner) so there's a way to log in before any signup/invite
flow exists — no illustrative/demo users are created.

Persistence: every mutation is written through to the shared
`Database` (see database.py) inside the same call, and the full user
set is loaded back into memory on registry construction — so a
crash or redeploy no longer erases every account.
"""

from datetime import datetime, UTC
from uuid import uuid4

from .auth import hash_password, verify_password
from ..storage.database import Database, dumps, loads

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

        # Real per-user usage counters. These start at zero and are
        # incremented as the user actually does things (logs in,
        # makes API requests, etc.) rather than being pre-filled.
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

    # --- persistence helpers -------------------------------------------------

    def _row(self):
        return (
            self.user_id, self.name, self.email, self.role, self.organization_id,
            self.status, self.avatar_initials, self.created_at.isoformat(),
            self.last_active.isoformat(), self.password_hash, dumps(self.stats),
        )

    @classmethod
    def _from_row(cls, row):
        user = cls(
            row["name"], row["email"], row["role"], row["organization_id"],
            row["status"], user_id=row["user_id"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )
        user.avatar_initials = row["avatar_initials"]
        user.last_active = datetime.fromisoformat(row["last_active"])
        user.password_hash = row["password_hash"]
        user.stats = loads(row["stats_json"], default=user.stats)
        return user


class UserRegistry:
    def __init__(self, db: Database = None):
        self.db = db or Database(":memory:")
        self.users = {}
        for row in self.db.query("SELECT * FROM users"):
            user = User._from_row(row)
            self.users[user.user_id] = user

    def _persist(self, user):
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO users (user_id, name, email, role, organization_id,
                       status, avatar_initials, created_at, last_active,
                       password_hash, stats_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       name=excluded.name, email=excluded.email, role=excluded.role,
                       organization_id=excluded.organization_id, status=excluded.status,
                       avatar_initials=excluded.avatar_initials, created_at=excluded.created_at,
                       last_active=excluded.last_active, password_hash=excluded.password_hash,
                       stats_json=excluded.stats_json""",
                user._row(),
            )

    def add_user(self, name, email, role, organization_id=None, status="Active"):
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'.")
        user = User(name, email, role, organization_id, status)
        self.users[user.user_id] = user
        self._persist(user)
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
        self._persist(user)
        return user

    def delete_user(self, user_id):
        self.get_user(user_id)
        del self.users[user_id]
        self.db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))

    def set_status(self, user_id, status):
        return self.update_user(user_id, status=status)

    def touch_last_active(self, user_id):
        user = self.get_user(user_id)
        user.last_active = datetime.now(UTC)
        self._persist(user)
        return user

    def increment_stat(self, user_id, stat_name, amount=1):
        """
        Atomically increment one of a user's `stats` counters and
        persist the result. Replaces call sites that used to do
        `user.stats["x"] += 1` directly (e.g. authenticate_api_key's
        `owner.stats["api_requests"] += 1`), which was an unguarded
        read-modify-write — the same lost-update shape stress_test2.py
        found in the audit log and notification center.
        """
        with self.db.transaction():
            user = self.get_user(user_id)
            user.stats[stat_name] = user.stats.get(stat_name, 0) + amount
            self._persist(user)
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


def bootstrap_owner_account(registry, name, email, password, organization_id=None):
    """
    Create the single real bootstrap account (the platform owner) on
    first boot, so there's a way to log in before any signup/invite
    flow exists. Replaces the old seed_users() demo-data function.

    Persistence-aware: if an owner account already exists (loaded
    from the database on a restart), this is a no-op instead of
    creating a second owner every time the process starts.
    """
    existing = registry.get_user_by_email(email)
    if existing is not None:
        return existing
    user = registry.add_user(
        name, email, "Owner",
        organization_id=organization_id,
        status="Active",
    )
    user.set_password(password)
    registry._persist(user)
    return user
