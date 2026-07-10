"""
GCON Organizations — organizations and teams for multi-tenant style
grouping of users.
"""

from datetime import datetime, UTC
from uuid import uuid4


class Organization:
    def __init__(self, name, plan="Standard", org_id=None):
        self.org_id = org_id or f"org_{uuid4().hex[:8]}"
        self.name = name
        self.plan = plan
        self.created_at = datetime.now(UTC)
        self.storage_used_gb = 0

    def to_dict(self):
        return {
            "org_id": self.org_id,
            "name": self.name,
            "plan": self.plan,
            "created_at": self.created_at.isoformat(),
            "storage_used_gb": self.storage_used_gb,
        }


class Team:
    def __init__(self, org_id, name, admin_user_id=None, team_id=None):
        self.team_id = team_id or f"team_{uuid4().hex[:8]}"
        self.org_id = org_id
        self.name = name
        self.admin_user_id = admin_user_id
        self.member_ids = []

    def to_dict(self):
        return {
            "team_id": self.team_id,
            "org_id": self.org_id,
            "name": self.name,
            "admin_user_id": self.admin_user_id,
            "member_ids": list(self.member_ids),
        }


class OrganizationRegistry:
    def __init__(self):
        self.organizations = {}
        self.teams = {}

    def add_organization(self, name, plan="Standard"):
        org = Organization(name, plan)
        self.organizations[org.org_id] = org
        return org

    def add_team(self, org_id, name, admin_user_id=None):
        if org_id not in self.organizations:
            raise ValueError(f"Organization '{org_id}' does not exist.")
        team = Team(org_id, name, admin_user_id)
        self.teams[team.team_id] = team
        return team

    def add_member(self, team_id, user_id):
        if team_id not in self.teams:
            raise ValueError(f"Team '{team_id}' does not exist.")
        team = self.teams[team_id]
        if user_id not in team.member_ids:
            team.member_ids.append(user_id)
        return team

    def list_organizations(self):
        return list(self.organizations.values())

    def list_teams(self, org_id=None):
        teams = list(self.teams.values())
        if org_id:
            teams = [t for t in teams if t.org_id == org_id]
        return teams


def seed_organizations(registry):
    """
    Populate the registry with illustrative demo organizations and teams.
    """
    acme = registry.add_organization("Acme Compute", plan="Enterprise")
    acme.storage_used_gb = 128.4

    nimbus = registry.add_organization("Nimbus Labs", plan="Standard")
    nimbus.storage_used_gb = 42.1

    registry.add_team(acme.org_id, "Platform Engineering")
    registry.add_team(acme.org_id, "Data Science")
    registry.add_team(nimbus.org_id, "Core")

    return [acme, nimbus]
