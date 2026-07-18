"""
GCON RBAC — Roles & Permissions reference data.

This is static reference data describing the platform's role-based
access control model. Permissions are enforced against real requests
via ManagementLayer.require_permission() (management_layer.py) and
WebServer.require_permission() (web_server.py), which is used as a
FastAPI dependency on the cluster/user/API-key management routes.
Authentication is handled in auth.py (PBKDF2 password hashing plus
in-memory sessions). This module is exposed here so the dashboard
can also display and manage roles/permissions.
"""

ROLES = ["Owner", "Administrator", "Operator", "Developer", "Viewer"]

PERMISSIONS = [
    "Manage cluster",
    "Submit workflows",
    "View monitoring",
    "Manage users",
    "Manage API keys",
    "Access analytics",
    "Read-only mode",
]

# Default permission grants per role. Owner has everything;
# Viewer is effectively read-only.
ROLE_PERMISSIONS = {
    "Owner": [
        "Manage cluster", "Submit workflows", "View monitoring",
        "Manage users", "Manage API keys", "Access analytics",
    ],
    "Administrator": [
        "Manage cluster", "Submit workflows", "View monitoring",
        "Manage users", "Manage API keys", "Access analytics",
    ],
    "Operator": [
        "Submit workflows", "View monitoring", "Access analytics",
    ],
    "Developer": [
        "Submit workflows", "View monitoring",
    ],
    "Viewer": [
        "View monitoring", "Read-only mode",
    ],
}


def get_permissions_for_role(role):
    """
    Return the list of permissions granted to a role.
    """
    return ROLE_PERMISSIONS.get(role, [])


def get_permission_matrix():
    """
    Return a role x permission matrix, for display in the
    Permissions view.
    """
    return [
        {
            "role": role,
            "permissions": {
                perm: perm in ROLE_PERMISSIONS.get(role, [])
                for perm in PERMISSIONS
            },
        }
        for role in ROLES
    ]
