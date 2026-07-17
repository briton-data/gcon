"""
GCON RBAC — Roles & Permissions reference data.

This is static reference data describing the platform's role-based
access control model. It is not yet enforced against real requests
(GCON has no authentication layer), but is exposed so the dashboard
can display and manage roles/permissions.
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
