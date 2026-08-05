"""
GCON Organizations — organizations and teams for multi-tenant style
grouping of users.

Persistence: every mutation is written through to the shared
`Database`, and all organizations/teams are loaded back into memory
on registry construction.
"""

from datetime import datetime, UTC
from uuid import uuid4

from ..storage.database import Database, dumps, loads


class Organization:
    def __init__(self, name, plan="Standard", org_id=None, created_at=None):
        self.org_id = org_id or f"org_{uuid4().hex[:8]}"
        self.name = name
        self.plan = plan
        self.created_at = created_at or datetime.now(UTC)
        self.storage_used_gb = 0

    def to_dict(self):
        return {
            "org_id": self.org_id,
            "name": self.name,
            "plan": self.plan,
            "created_at": self.created_at.isoformat(),
            "storage_used_gb": self.storage_used_gb,
        }

    def _row(self):
        return (self.org_id, self.name, self.plan, self.created_at.isoformat(), self.storage_used_gb)

    @classmethod
    def _from_row(cls, row):
        org = cls(row["name"], row["plan"], org_id=row["org_id"],
                   created_at=datetime.fromisoformat(row["created_at"]))
        org.storage_used_gb = row["storage_used_gb"]
        return org


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

    def _row(self):
        return (self.team_id, self.org_id, self.name, self.admin_user_id, dumps(self.member_ids))

    @classmethod
    def _from_row(cls, row):
        team = cls(row["org_id"], row["name"], row["admin_user_id"], team_id=row["team_id"])
        team.member_ids = loads(row["member_ids_json"], default=[])
        return team


class OrganizationRegistry:
    def __init__(self, db: Database = None):
        self.db = db or Database(":memory:")
        self.organizations = {}
        self.teams = {}
        for row in self.db.query("SELECT * FROM organizations"):
            org = Organization._from_row(row)
            self.organizations[org.org_id] = org
        for row in self.db.query("SELECT * FROM teams"):
            team = Team._from_row(row)
            self.teams[team.team_id] = team

    def _persist_org(self, org):
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO organizations (org_id, name, plan, created_at, storage_used_gb)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(org_id) DO UPDATE SET
                       name=excluded.name, plan=excluded.plan,
                       created_at=excluded.created_at, storage_used_gb=excluded.storage_used_gb""",
                org._row(),
            )

    def _persist_team(self, team):
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO teams (team_id, org_id, name, admin_user_id, member_ids_json)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(team_id) DO UPDATE SET
                       org_id=excluded.org_id, name=excluded.name,
                       admin_user_id=excluded.admin_user_id, member_ids_json=excluded.member_ids_json""",
                team._row(),
            )

    def add_organization(self, name, plan="Standard"):
        org = Organization(name, plan)
        self.organizations[org.org_id] = org
        self._persist_org(org)
        return org

    def add_team(self, org_id, name, admin_user_id=None):
        if org_id not in self.organizations:
            raise ValueError(f"Organization '{org_id}' does not exist.")
        team = Team(org_id, name, admin_user_id)
        self.teams[team.team_id] = team
        self._persist_team(team)
        return team

    def add_member(self, team_id, user_id):
        if team_id not in self.teams:
            raise ValueError(f"Team '{team_id}' does not exist.")
        team = self.teams[team_id]
        if user_id not in team.member_ids:
            team.member_ids.append(user_id)
            self._persist_team(team)
        return team

    def remove_member(self, team_id, user_id):
        if team_id not in self.teams:
            raise ValueError(f"Team '{team_id}' does not exist.")
        team = self.teams[team_id]
        if user_id in team.member_ids:
            team.member_ids.remove(user_id)
            self._persist_team(team)
        return team

    def get_organization(self, org_id):
        if org_id not in self.organizations:
            raise ValueError(f"Organization '{org_id}' does not exist.")
        return self.organizations[org_id]

    def get_team(self, team_id):
        if team_id not in self.teams:
            raise ValueError(f"Team '{team_id}' does not exist.")
        return self.teams[team_id]

    def update_organization(self, org_id, **fields):
        org = self.get_organization(org_id)
        for key, value in fields.items():
            if value is None:
                continue
            setattr(org, key, value)
        self._persist_org(org)
        return org

    def update_team(self, team_id, **fields):
        team = self.get_team(team_id)
        for key, value in fields.items():
            if value is None:
                continue
            if key == "admin_user_id" and value == "":
                value = None
            setattr(team, key, value)
        self._persist_team(team)
        return team

    def delete_organization(self, org_id, user_registry=None):
        self.get_organization(org_id)

        # Cascade: an org with no members/teams left dangling is a
        # worse failure mode than an org that can't be deleted while
        # it still has dependents, so we clean up the dependents
        # rather than either blocking the delete or orphaning rows.
        for team in self.list_teams(org_id):
            del self.teams[team.team_id]
            self.db.execute("DELETE FROM teams WHERE team_id = ?", (team.team_id,))

        if user_registry is not None:
            user_registry.detach_organization(org_id)

        del self.organizations[org_id]
        self.db.execute("DELETE FROM organizations WHERE org_id = ?", (org_id,))

    def delete_team(self, team_id):
        self.get_team(team_id)
        del self.teams[team_id]
        self.db.execute("DELETE FROM teams WHERE team_id = ?", (team_id,))

    def remove_user_everywhere(self, user_id):
        """
        Strip a deleted user out of every team's member list and
        clear them as team admin wherever they were assigned, so a
        deleted user_id doesn't linger as a dangling reference in
        `teams` (these tables don't have real foreign keys, so
        nothing else does this for us).
        """
        for team in self.teams.values():
            changed = False
            if user_id in team.member_ids:
                team.member_ids.remove(user_id)
                changed = True
            if team.admin_user_id == user_id:
                team.admin_user_id = None
                changed = True
            if changed:
                self._persist_team(team)

    def list_organizations(self):
        return list(self.organizations.values())

    def list_teams(self, org_id=None):
        teams = list(self.teams.values())
        if org_id:
            teams = [t for t in teams if t.org_id == org_id]
        return teams
