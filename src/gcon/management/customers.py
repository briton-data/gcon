"""
GCON Customer Accounts -- the actual customer-facing signup/login
identity, deliberately separate from management/users.py's `User`.

A `User` (users.py) is a GCON staff account: it has an RBAC role
(Owner/Administrator/Operator/Developer/Viewer -- see rbac.py) that
controls what internal operations they can perform. A customer of
GCON's hosted service is a different kind of account entirely: they
don't operate GCON's infrastructure, they submit jobs to it, so none
of those roles apply. Every CustomerUser in an organization has the
same full access to that organization's own data -- no internal
hierarchy, by design (see the architecture discussion this file
implements).

`org_id` is what ties a customer account to everything else in the
system. Nodes, jobs, receipts, and the internal staff console's
Clients panel are all already org_id-scoped (see
ManagementLayer.get_client_recent_jobs / get_org_usage_summary) --
so a new signup is automatically visible to staff the moment it
exists, with zero extra wiring, simply by sharing that same id.
"""

from datetime import datetime, UTC
from uuid import uuid4

from .auth import hash_password, verify_password
from ..storage.database import Database

VALID_CUSTOMER_STATUSES = ["Active", "Disabled"]


class CustomerUser:
    def __init__(self, name, email, org_id, status="Active",
                 customer_user_id=None, created_at=None, last_active=None):
        self.customer_user_id = customer_user_id or f"cust_{uuid4().hex[:8]}"
        self.name = name
        self.email = email
        self.org_id = org_id
        self.status = status
        self.created_at = created_at or datetime.now(UTC)
        self.last_active = last_active
        self.password_hash = None

    def set_password(self, password):
        self.password_hash = hash_password(password)

    def check_password(self, password):
        if not self.password_hash:
            return False
        return verify_password(password, self.password_hash)

    def to_dict(self):
        return {
            "customer_user_id": self.customer_user_id,
            "name": self.name,
            "email": self.email,
            "org_id": self.org_id,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "last_active": self.last_active.isoformat() if self.last_active else None,
        }

    # --- persistence helpers -------------------------------------------------

    def _row(self):
        return (
            self.customer_user_id, self.org_id, self.name, self.email,
            self.password_hash, self.status, self.created_at.isoformat(),
            self.last_active.isoformat() if self.last_active else None,
        )

    @classmethod
    def _from_row(cls, row):
        user = cls(
            row["name"], row["email"], row["org_id"], row["status"],
            customer_user_id=row["customer_user_id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            last_active=datetime.fromisoformat(row["last_active"]) if row["last_active"] else None,
        )
        user.password_hash = row["password_hash"]
        return user


class CustomerUserRegistry:
    def __init__(self, db: Database = None):
        self.db = db or Database(":memory:")
        self.users = {}
        for row in self.db.query("SELECT * FROM customer_users"):
            user = CustomerUser._from_row(row)
            self.users[user.customer_user_id] = user

    def _persist(self, user):
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO customer_users
                       (customer_user_id, org_id, name, email, password_hash,
                        status, created_at, last_active)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(customer_user_id) DO UPDATE SET
                       org_id=excluded.org_id, name=excluded.name,
                       email=excluded.email, password_hash=excluded.password_hash,
                       status=excluded.status, created_at=excluded.created_at,
                       last_active=excluded.last_active""",
                user._row(),
            )

    def add_user(self, name, email, org_id, password, status="Active"):
        if status not in VALID_CUSTOMER_STATUSES:
            raise ValueError(f"Invalid status '{status}'.")
        if self.get_user_by_email(email) is not None:
            raise ValueError(f"An account with email '{email}' already exists.")
        user = CustomerUser(name, email, org_id, status)
        user.set_password(password)
        self.users[user.customer_user_id] = user
        self._persist(user)
        return user

    def get_user(self, customer_user_id):
        if customer_user_id not in self.users:
            raise ValueError(f"Customer user '{customer_user_id}' does not exist.")
        return self.users[customer_user_id]

    def get_user_by_email(self, email):
        email = email.lower().strip()
        for user in self.users.values():
            if user.email.lower() == email:
                return user
        return None

    def list_for_org(self, org_id):
        return [u for u in self.users.values() if u.org_id == org_id]

    def authenticate(self, email, password):
        """Returns the CustomerUser on success, None on any failure
        (unknown email, wrong password, disabled account) -- a
        single generic outcome so a login form can't be used to
        enumerate which emails have accounts."""
        user = self.get_user_by_email(email)
        if user is None or user.status != "Active":
            return None
        if not user.check_password(password):
            return None
        user.last_active = datetime.now(UTC)
        self._persist(user)
        return user

    def set_password(self, customer_user_id, password):
        user = self.get_user(customer_user_id)
        user.set_password(password)
        self._persist(user)
        return user

    def update_status(self, customer_user_id, status):
        if status not in VALID_CUSTOMER_STATUSES:
            raise ValueError(f"Invalid status '{status}'.")
        user = self.get_user(customer_user_id)
        user.status = status
        self._persist(user)
        return user


class _CustomerOwnerView:
    """
    Thin adapter so a CustomerUser presents the same three attributes
    every existing api_v1.py route already reads off an
    authenticate_api_key() owner -- .status, .organization_id,
    .user_id -- without those routes needing to know or care whether
    the key belongs to staff or a customer. See
    ManagementLayer.authenticate_api_key's fallback.
    """
    def __init__(self, customer_user: CustomerUser):
        self.user_id = customer_user.customer_user_id
        self.organization_id = customer_user.org_id
        self.status = customer_user.status
        # Added after a real end-to-end test caught GET /whoami
        # (an existing, otherwise-untouched route) raising
        # AttributeError on owner.name for any customer-authenticated
        # request -- this adapter never exposed it, and no customer
        # key had exercised that route before now.
        self.name = customer_user.name
