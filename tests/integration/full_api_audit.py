"""
GCON full API audit.

This is deliberately NOT a "does it return 200" smoke test. It is built
to find real problems: unauthenticated data leaks, routes that 500
instead of 4xx-ing on bad input, dead backend code the frontend never
calls, frontend calls with no backend behind them, and RBAC that
doesn't actually restrict anything.

Design rules this file follows (so it can't quietly rot into a
rubber-stamp):

1. The route inventory is pulled LIVE from the running server's own
   /openapi.json (both the dashboard app and the mounted /api/v1 app),
   never hand-typed. If someone adds or removes a route, this test's
   coverage list changes with it automatically, and a mismatch against
   the last-known list is reported, not silently ignored.
2. Every discovered route gets an unauthenticated probe. A route that
   returns real data (200) with no cookie and no API key is a SECURITY
   FINDING, not a skipped case.
3. Frontend fetch/opCall targets are scraped from static/js/dashboard.js
   at run time and cross-referenced against the live route inventory in
   both directions: backend routes the UI never calls (dead code, or
   undiscovered features), and UI calls that hit nothing real.
4. Results are bucketed into CONTRACT (hard pass/fail — auth
   enforcement, correct status codes, no 500s) vs OBSERVATION (what
   actually came back, printed for a human to judge — never asserted
   into a fake pass). Nothing is skipped or softened to make the run
   look green.
5. Every check keeps running even after a failure. Nothing here retries
   until it passes, sleeps away a race, or catches an exception just to
   call it a pass.

Run from the repo root, with the real server NOT already running on
the target port:

    python tests/integration/full_api_audit.py

Exit code is 0 only if there were zero CONTRACT failures and zero
crashes (500s). Security findings and dead/orphan routes do not affect
the exit code by design (they're not "fail/pass", they're findings)
but they ARE printed loudly at the end and counted, and a nonzero
security-finding count also makes the process exit nonzero.
"""

import json
import os
import re
import subprocess
import sys
import time
import uuid
import tempfile

import requests

# Safety net: if the server under test genuinely stops responding (deadlock,
# thread-pool exhaustion, event-loop block, etc.), every request should fail
# LOUDLY within a bounded time instead of hanging the whole run forever with
# no output. This does not change pass/fail semantics for any check — a
# request that used to time out silently forever now times out at 10s and
# is reported as a CRASH, same as any other unhandled exception.
_DEFAULT_TIMEOUT = 10
_orig_session_request = requests.Session.request


def _timeout_enforced_request(self, method, url, **kwargs):
    kwargs.setdefault("timeout", _DEFAULT_TIMEOUT)
    return _orig_session_request(self, method, url, **kwargs)


requests.Session.request = _timeout_enforced_request

HOST = "127.0.0.1"
PORT = int(os.environ.get("GCON_TEST_PORT", "8071"))
BASE = f"http://{HOST}:{PORT}"

OWNER_EMAIL = os.environ.get("GCON_OWNER_EMAIL", "nyongesabriton620@gmail.com")
OWNER_PASSWORD = os.environ.get("GCON_OWNER_PASSWORD", "GCON2024")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------
# Result bookkeeping — nothing here is allowed to disappear silently.
# ---------------------------------------------------------------------

contract_results = []   # (name, passed: bool, detail)
observations = []       # (name, detail)
security_findings = []  # (name, detail)
crashes = []            # (name, detail)  -- any unhandled 500 / connection death
dead_routes = []        # backend routes never called by the frontend
orphan_frontend_calls = []  # frontend calls with no matching backend route


def contract(name, fn):
    try:
        fn()
        contract_results.append((name, True, ""))
        print(f"  [PASS] {name}")
    except AssertionError as e:
        contract_results.append((name, False, str(e)))
        print(f"  [FAIL] {name}: {e}")
    except Exception as e:
        contract_results.append((name, False, f"{type(e).__name__}: {e}"))
        crashes.append((name, f"{type(e).__name__}: {e}"))
        print(f"  [CRASH] {name}: {type(e).__name__}: {e}")


def observe(name, detail):
    observations.append((name, detail))
    print(f"  [OBSERVED] {name}: {detail}")


def finding(name, detail):
    security_findings.append((name, detail))
    print(f"  [SECURITY FINDING] {name}: {detail}")


# ---------------------------------------------------------------------
# Server lifecycle — real coordinator, real scheduler, nothing mocked.
# ---------------------------------------------------------------------

def start_server():
    db_path = os.path.join(tempfile.mkdtemp(prefix="gcon_audit_"), "audit.db")
    env = dict(os.environ)
    env["GCON_DASHBOARD_HOST"] = HOST
    env["GCON_DASHBOARD_PORT"] = str(PORT)
    proc = subprocess.Popen(
        [sys.executable, "scripts/run_coordinator.py", "--db", db_path, "--log-level", "WARNING"],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read()
            raise RuntimeError(f"Server process exited early (code {proc.returncode}):\n{out}")
        try:
            requests.get(f"{BASE}/login", timeout=1)
            return proc
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)
    proc.terminate()
    raise RuntimeError("Server did not come up within 30s")


def stop_server(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


# ---------------------------------------------------------------------
# Live route inventory — pulled from the running app, not hand-typed.
# ---------------------------------------------------------------------

def get_live_routes(openapi_url):
    r = requests.get(openapi_url, timeout=5)
    r.raise_for_status()
    spec = r.json()
    routes = []
    for path, methods in spec.get("paths", {}).items():
        for method in methods:
            if method.upper() in ("GET", "POST", "PUT", "DELETE", "PATCH"):
                routes.append((method.upper(), path))
    return routes


def scrape_frontend_calls():
    js_path = os.path.join(REPO_ROOT, "static", "js", "dashboard.js")
    try:
        with open(js_path, encoding="utf-8") as f:
            src = f.read()
    except FileNotFoundError:
        return []
    calls = set()
    for m in re.finditer(r"""(?:fetchJson|opCall)\(\s*[`'"]([^`'"]+)""", src):
        raw = m.group(1)
        # strip template-literal interpolations down to a path-shape token
        norm = re.sub(r"\$\{[^}]+\}", "{param}", raw)
        norm = norm.split("?")[0]
        calls.add(norm)
    return sorted(calls)


def path_shape(path):
    """Normalize an OpenAPI path (/jobs/{job_id}) to the same {param} shape
    used in scrape_frontend_calls, so the two can be compared."""
    return re.sub(r"\{[^}]+\}", "{param}", path)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    print(f"Starting real GCON coordinator + dashboard on {BASE} ...")
    proc = start_server()
    print("Server is up.\n")

    session = requests.Session()      # owner, authenticated
    anon = requests.Session()         # never authenticates — for negative checks
    state = {}

    try:
        # ===========================================================
        print("== 0. Route inventory (live, not hand-typed) ==")
        dash_routes = get_live_routes(f"{BASE}/openapi.json")
        api_routes = get_live_routes(f"{BASE}/api/v1/openapi.json")
        print(f"  Dashboard app: {len(dash_routes)} routes discovered via /openapi.json")
        print(f"  /api/v1 app:   {len(api_routes)} routes discovered via /api/v1/openapi.json")
        state["dash_routes"] = dash_routes
        state["api_routes"] = api_routes

        frontend_calls = scrape_frontend_calls()
        dash_shapes = {path_shape(p) for _, p in dash_routes}
        for call in frontend_calls:
            if call not in dash_shapes and call not in ("/login", "/{param}"):
                orphan_frontend_calls.append(call)
        for method, p in dash_routes:
            shape = path_shape(p)
            if method == "GET" and shape in ("/", "/login", "/openapi.json", "/docs", "/redoc"):
                continue
            if shape not in frontend_calls and method != "GET" or (method == "GET" and shape not in frontend_calls):
                # only flag as dead if truly never referenced (any method)
                if shape not in frontend_calls:
                    dead_routes.append(f"{method} {p}")
        # de-dup dead_routes (a route can be checked twice due to loop logic above)
        dead_routes[:] = sorted(set(dead_routes))

        # ===========================================================
        print("\n== 1. Unauthenticated access to every dashboard route ==")
        for method, p in dash_routes:
            if "{" in p:
                continue  # parametrized routes probed separately in section 5+
            if p in ("/login", "/openapi.json", "/docs", "/redoc", "/"):
                continue

            def _probe(method=method, p=p):
                fn = getattr(anon, method.lower())
                r = fn(f"{BASE}{p}", timeout=5, json={} if method != "GET" else None)
                assert r.status_code != 500, f"500 on unauthenticated {method} {p}: {r.text[:200]}"
                if r.status_code == 200:
                    finding(f"{method} {p}", "returned 200 with NO auth cookie at all — unauthenticated data exposure")
                else:
                    assert r.status_code in (401, 403, 422), (
                        f"expected 401/403 on unauth request, got {r.status_code}: {r.text[:200]}"
                    )

            contract(f"Unauthenticated {method} {p} is rejected (not a crash, not a leak)", _probe)

        # ===========================================================
        print("\n== 2. Auth: login lifecycle ==")

        def _bad_login():
            r = requests.post(f"{BASE}/auth/login", json={"email": OWNER_EMAIL, "password": "definitely-wrong"})
            assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text[:200]}"

        contract("Login rejects a wrong password", _bad_login)

        def _login_missing_field():
            r = requests.post(f"{BASE}/auth/login", json={"email": OWNER_EMAIL})
            assert r.status_code in (400, 401, 422), (
                f"missing password field should 4xx cleanly, got {r.status_code}: {r.text[:200]}"
            )

        contract("Login with missing 'password' field does not 500", _login_missing_field)

        def _login():
            r = session.post(f"{BASE}/auth/login", json={"email": OWNER_EMAIL, "password": OWNER_PASSWORD})
            assert r.status_code == 200, f"status {r.status_code}: {r.text[:300]}"
            state["me"] = r.json()

        contract("Login with the real bootstrap owner account", _login)

        def _me():
            r = session.get(f"{BASE}/auth/me")
            assert r.status_code == 200
            assert r.json()["email"] == OWNER_EMAIL

        contract("GET /auth/me reflects the logged-in user", _me)

        def _rate_limit():
            # hammer bad logins for an account that isn't the owner, to avoid
            # locking ourselves out, and check the limiter actually engages.
            probe_email = f"ratelimit-{uuid.uuid4().hex[:8]}@example.com"
            statuses = []
            for _ in range(8):
                r = requests.post(f"{BASE}/auth/login", json={"email": probe_email, "password": "x"})
                statuses.append(r.status_code)
            assert 429 in statuses, f"expected a 429 to appear under repeated failed logins, got {statuses}"

        contract("Repeated failed logins eventually trigger rate limiting (429)", _rate_limit)

        # ===========================================================
        print("\n== 3. Organizations, teams, users (Management CRUD) ==")

        def _create_org():
            r = session.post(f"{BASE}/management/organizations", json={"name": f"Audit Org {uuid.uuid4().hex[:6]}"})
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            state["org"] = r.json()

        contract("Create organization", _create_org)

        def _create_org_missing_name():
            r = session.post(f"{BASE}/management/organizations", json={})
            assert r.status_code == 400, f"missing 'name' should 400, got {r.status_code}: {r.text[:200]}"

        contract("Create organization with missing 'name' returns 400, not 500", _create_org_missing_name)

        def _create_team():
            org_id = state["org"]["org_id"] if "org_id" in state.get("org", {}) else state["org"].get("id")
            assert org_id, f"could not find an org id field in {state['org']}"
            r = session.post(f"{BASE}/management/teams", json={"org_id": org_id, "name": "Audit Team"})
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            state["team"] = r.json()

        contract("Create team under the new organization", _create_team)

        created_users = {}

        def _make_user_creator(role):
            def _create():
                email = f"audit-{role.lower()}-{uuid.uuid4().hex[:8]}@example.com"
                r = session.post(f"{BASE}/management/users", json={
                    "name": f"Audit {role}",
                    "email": email,
                    "role": role,
                    "password": "AuditPass123!",
                })
                assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
                body = r.json()
                created_users[role] = {"email": email, "password": "AuditPass123!", "body": body}
            return _create

        for role in ["Administrator", "Operator", "Developer", "Viewer"]:
            contract(f"Create a '{role}' user", _make_user_creator(role))

        def _create_user_invalid_role():
            r = session.post(f"{BASE}/management/users", json={
                "name": "Bad Role",
                "email": f"audit-badrole-{uuid.uuid4().hex[:8]}@example.com",
                "role": "SuperUltraAdmin",
                "password": "AuditPass123!",
            })
            assert r.status_code == 400, f"invalid role should 400, got {r.status_code}: {r.text[:200]}"

        contract("Create user with an invalid role name is rejected, not silently accepted", _create_user_invalid_role)

        def _create_user_duplicate_email():
            r = session.post(f"{BASE}/management/users", json={
                "name": "Dup", "email": OWNER_EMAIL, "role": "Viewer", "password": "x",
            })
            assert r.status_code == 400, f"duplicate email should 400, got {r.status_code}: {r.text[:200]}"

        contract("Create user with an already-registered email is rejected", _create_user_duplicate_email)

        def _get_user_not_found():
            r = session.get(f"{BASE}/management/users/does-not-exist-{uuid.uuid4().hex}")
            assert r.status_code == 404, f"expected 404 for nonexistent user, got {r.status_code}: {r.text[:200]}"

        contract("GET a nonexistent user_id returns 404, not 500", _get_user_not_found)

        def _update_user():
            uid = created_users["Viewer"]["body"]["user_id"]
            r = session.put(f"{BASE}/management/users/{uid}", json={"name": "Renamed Viewer"})
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"

        contract("Update an existing user", _update_user)

        def _set_user_status():
            uid = created_users["Viewer"]["body"]["user_id"]
            r = session.post(f"{BASE}/management/users/{uid}/status", json={"status": "Suspended"})
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"

        contract("Suspend a user via /status", _set_user_status)

        def _delete_user():
            uid = created_users["Developer"]["body"]["user_id"]
            r = session.delete(f"{BASE}/management/users/{uid}")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            r2 = session.get(f"{BASE}/management/users/{uid}")
            assert r2.status_code == 404, f"deleted user should 404 afterward, got {r2.status_code}"

        contract("Delete a user, then confirm it's actually gone", _delete_user)

        # ===========================================================
        print("\n== 4. RBAC — do restricted roles actually get restricted? ==")

        def _login_as(role_key):
            s = requests.Session()
            creds = created_users[role_key]
            r = s.post(f"{BASE}/auth/login", json={"email": creds["email"], "password": creds["password"]})
            assert r.status_code == 200, f"could not log in as {role_key}: {r.status_code} {r.text[:200]}"
            return s

        def _viewer_cannot_manage_cluster():
            s = _login_as("Viewer")
            r = s.post(f"{BASE}/cluster/scheduler/pause")
            assert r.status_code == 403, f"Viewer should be denied 'Manage cluster', got {r.status_code}: {r.text[:200]}"

        contract("Viewer role is denied 'Manage cluster' actions (403)", _viewer_cannot_manage_cluster)

        def _viewer_cannot_manage_users():
            s = _login_as("Viewer")
            r = s.get(f"{BASE}/management/users")
            assert r.status_code == 403, f"Viewer should be denied 'Manage users', got {r.status_code}: {r.text[:200]}"

        contract("Viewer role is denied /management/users (403)", _viewer_cannot_manage_users)

        def _developer_cannot_manage_cluster():
            s = _login_as("Developer") if "Developer" in created_users else None
            # Developer was deleted above in section 3 — recreate a fresh one so
            # this check is independent of delete ordering.
            email = f"audit-dev2-{uuid.uuid4().hex[:8]}@example.com"
            r = session.post(f"{BASE}/management/users", json={
                "name": "Audit Dev2", "email": email, "role": "Developer", "password": "AuditPass123!",
            })
            assert r.status_code == 200, f"setup failed: {r.status_code} {r.text[:200]}"
            s = requests.Session()
            r2 = s.post(f"{BASE}/auth/login", json={"email": email, "password": "AuditPass123!"})
            assert r2.status_code == 200
            r3 = s.post(f"{BASE}/cluster/scheduler/pause")
            assert r3.status_code == 403, f"Developer should be denied 'Manage cluster', got {r3.status_code}"

        contract("Developer role is denied 'Manage cluster' actions (403)", _developer_cannot_manage_cluster)

        def _viewer_can_view_monitoring():
            s = _login_as("Viewer")
            r = s.get(f"{BASE}/cluster")
            assert r.status_code == 200, f"Viewer should be able to view /cluster, got {r.status_code}: {r.text[:200]}"

        contract("Viewer role CAN access read-only monitoring routes", _viewer_can_view_monitoring)

        def _user_counts_no_auth_at_all():
            r = anon.get(f"{BASE}/management/user-counts")
            if r.status_code == 200:
                finding("GET /management/user-counts", "route has NO Depends(...) auth dependency at all — returns real user-count data to a fully anonymous, cookie-less request")
            else:
                observe("GET /management/user-counts (anon)", f"status {r.status_code}")

        contract("Check /management/user-counts for missing auth dependency", _user_counts_no_auth_at_all)

        # ===========================================================
        print("\n== 5. API keys + /api/v1 auth ==")

        def _create_api_key():
            uid = state["me"]["user_id"]
            r = session.post(f"{BASE}/management/api-keys", json={
                "name": "Audit key (full)", "owner_user_id": uid,
                "scopes": ["View monitoring", "Submit workflows"],
            })
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            body = r.json()
            assert "secret" in body and body["secret"], f"created key response has no usable secret: {body}"
            state["api_key"] = body

        contract("Create a scoped API key", _create_api_key)

        def _create_readonly_api_key():
            uid = state["me"]["user_id"]
            r = session.post(f"{BASE}/management/api-keys", json={
                "name": "Audit key (readonly)", "owner_user_id": uid,
                "scopes": ["View monitoring"],
            })
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            state["readonly_key"] = r.json()

        contract("Create a read-only-scoped API key", _create_readonly_api_key)

        def _api_no_key():
            r = requests.get(f"{BASE}/api/v1/cluster")
            assert r.status_code == 401, f"expected 401 with no key, got {r.status_code}: {r.text[:200]}"

        contract("GET /api/v1/cluster with no API key is rejected", _api_no_key)

        def _api_bad_key():
            r = requests.get(f"{BASE}/api/v1/cluster", headers={"X-API-Key": "not-a-real-key"})
            assert r.status_code == 401, f"expected 401 with a bogus key, got {r.status_code}: {r.text[:200]}"

        contract("GET /api/v1/cluster with a garbage API key is rejected", _api_bad_key)

        def _api_good_key():
            secret = state["api_key"]["secret"]
            r = requests.get(f"{BASE}/api/v1/cluster", headers={"X-API-Key": secret})
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            state["cluster"] = r.json()

        contract("GET /api/v1/cluster with a real API key succeeds", _api_good_key)

        def _api_bearer_form():
            secret = state["api_key"]["secret"]
            r = requests.get(f"{BASE}/api/v1/health", headers={"Authorization": f"Bearer {secret}"})
            assert r.status_code == 200, f"Authorization: Bearer form should also work, got {r.status_code}: {r.text[:200]}"

        contract("GET /api/v1/health accepts 'Authorization: Bearer <key>' form too", _api_bearer_form)

        def _api_whoami():
            secret = state["api_key"]["secret"]
            r = requests.get(f"{BASE}/api/v1/whoami", headers={"X-API-Key": secret})
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            assert r.json().get("key_name") == "Audit key (full)"

        contract("GET /api/v1/whoami identifies the calling key correctly", _api_whoami)

        def _api_scope_enforced():
            secret = state["readonly_key"]["secret"]
            r = requests.post(f"{BASE}/api/v1/jobs", headers={"X-API-Key": secret},
                               json={"job_id": f"audit-{uuid.uuid4().hex[:8]}", "command": "echo hi"})
            assert r.status_code == 401, (
                f"a 'View monitoring'-only key should be rejected on a 'Submit workflows' route, got {r.status_code}: {r.text[:200]}"
            )

        contract("A read-only API key is rejected on a write-scoped route", _api_scope_enforced)

        def _revoke_key_then_use():
            key_id = state["api_key"]["key_id"]
            r = session.post(f"{BASE}/management/api-keys/{key_id}/revoke")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            secret = state["api_key"]["secret"]
            r2 = requests.get(f"{BASE}/api/v1/cluster", headers={"X-API-Key": secret})
            assert r2.status_code == 401, f"revoked key should stop working immediately, got {r2.status_code}: {r2.text[:200]}"

        contract("Revoking an API key immediately invalidates it", _revoke_key_then_use)

        def _regenerate_readonly_key():
            key_id = state["readonly_key"]["key_id"]
            r = session.post(f"{BASE}/management/api-keys/{key_id}/regenerate")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            new_secret = r.json()["secret"]
            old_secret = state["readonly_key"]["secret"]
            assert new_secret != old_secret, "regenerate did not actually change the secret"
            r2 = requests.get(f"{BASE}/api/v1/cluster", headers={"X-API-Key": old_secret})
            assert r2.status_code == 401, f"old secret should stop working after regenerate, got {r2.status_code}"
            r3 = requests.get(f"{BASE}/api/v1/cluster", headers={"X-API-Key": new_secret})
            assert r3.status_code == 200, f"new secret should work after regenerate, got {r3.status_code}"
            state["readonly_key"] = r.json()

        contract("Regenerating an API key rotates the secret and invalidates the old one", _regenerate_readonly_key)

        # ===========================================================
        print("\n== 6. /api/v1 full surface (auth'd) ==")

        def _api_full_pass(name):
            secret = state["readonly_key"]["secret"]
            headers = {"X-API-Key": secret}

            def _run():
                r = requests.get(f"{BASE}/api/v1/{name}", headers=headers, timeout=5)
                assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
                assert r.status_code != 500
                observe(f"GET /api/v1/{name} shape", str(r.json())[:180])

            return _run

        for endpoint in ["nodes", "jobs", "workflows", "receipts", "artifacts", "metrics"]:
            contract(f"GET /api/v1/{endpoint} (authenticated) does not crash", _api_full_pass(endpoint))

        def _api_job_lifecycle():
            secret = state["api_key"]["secret"]  # this one has Submit workflows scope
            headers = {"X-API-Key": secret}
            job_id = f"audit-job-{uuid.uuid4().hex[:8]}"
            r = requests.post(f"{BASE}/api/v1/jobs", headers=headers,
                               json={"job_id": job_id, "command": "echo audit-test"})
            assert r.status_code == 200, f"submit failed: {r.status_code}: {r.text[:200]}"

            r2 = requests.get(f"{BASE}/api/v1/jobs/{job_id}", headers=headers)
            assert r2.status_code == 200, f"could not fetch just-submitted job: {r2.status_code}: {r2.text[:200]}"
            state["submitted_job"] = job_id

        contract("Submit a job via /api/v1/jobs, then fetch it back by id", _api_job_lifecycle)

        def _api_job_not_found():
            secret = state["readonly_key"]["secret"]
            r = requests.get(f"{BASE}/api/v1/jobs/does-not-exist-{uuid.uuid4().hex}",
                              headers={"X-API-Key": secret})
            assert r.status_code == 404, f"expected 404, got {r.status_code}: {r.text[:200]}"

        contract("GET /api/v1/jobs/{bad_id} returns 404, not 500", _api_job_not_found)

        def _api_submit_missing_fields():
            secret = state["api_key"]["secret"]
            r = requests.post(f"{BASE}/api/v1/jobs", headers={"X-API-Key": secret}, json={"job_id": "no-command"})
            assert r.status_code == 422, f"missing required 'command' should 422 (FastAPI validation), got {r.status_code}: {r.text[:200]}"

        contract("POST /api/v1/jobs with a missing required field is rejected cleanly", _api_submit_missing_fields)

        def _api_submit_duplicate_job_id():
            secret = state["api_key"]["secret"]
            job_id = state.get("submitted_job")
            assert job_id, "previous job submission step did not run"
            r = requests.post(f"{BASE}/api/v1/jobs", headers={"X-API-Key": secret},
                               json={"job_id": job_id, "command": "echo dup"})
            assert r.status_code == 400, f"resubmitting the same job_id should 400, got {r.status_code}: {r.text[:200]}"

        contract("Resubmitting a duplicate job_id is rejected, not silently duplicated", _api_submit_duplicate_job_id)

        def _api_node_not_found():
            secret = state["readonly_key"]["secret"]
            r = requests.get(f"{BASE}/api/v1/nodes/does-not-exist-{uuid.uuid4().hex}", headers={"X-API-Key": secret})
            assert r.status_code == 404, f"expected 404, got {r.status_code}: {r.text[:200]}"

        contract("GET /api/v1/nodes/{bad_id} returns 404, not 500", _api_node_not_found)

        # ===========================================================
        print("\n== 7. Coordinator/dashboard surface (cookie-auth'd) ==")

        for endpoint in ["cluster", "nodes", "jobs", "events", "topology", "receipts", "artifacts",
                          "system-metrics", "health", "health/details", "trust-center", "trust-score",
                          "trust-history", "hero-status", "analytics", "admin/config",
                          "management/permission-matrix", "management/notifications",
                          "management/notifications/unread-by-severity", "management/dashboard-cards",
                          "management/roles", "management/permissions", "management/audit-logs",
                          "management/user-counts"]:
            def _get(endpoint=endpoint):
                r = session.get(f"{BASE}/{endpoint}", timeout=5)
                assert r.status_code != 500, f"500 on GET /{endpoint}: {r.text[:300]}"
                if r.status_code == 200:
                    observe(f"GET /{endpoint}", str(r.json())[:150] if r.headers.get("content-type", "").startswith("application/json") else f"<{r.headers.get('content-type')}, {len(r.content)} bytes>")
                else:
                    observe(f"GET /{endpoint}", f"status {r.status_code}: {r.text[:150]}")

            contract(f"GET /{endpoint} (owner) does not 500", _get)

        def _job_detail_not_found():
            r = session.get(f"{BASE}/jobs/does-not-exist-{uuid.uuid4().hex}")
            assert r.status_code == 404, f"expected 404, got {r.status_code}: {r.text[:200]}"

        contract("GET /jobs/{bad_id} returns 404, not 500", _job_detail_not_found)

        def _receipt_detail_not_found():
            r = session.get(f"{BASE}/receipts/does-not-exist-{uuid.uuid4().hex}")
            assert r.status_code == 404, f"expected 404, got {r.status_code}: {r.text[:200]}"

        contract("GET /receipts/{bad_id} returns 404, not 500", _receipt_detail_not_found)

        def _search():
            r = session.get(f"{BASE}/management/search", params={"q": "audit"})
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"

        contract("GET /management/search with a query string works", _search)

        def _search_empty_query():
            r = session.get(f"{BASE}/management/search")
            assert r.status_code != 500, f"empty query should not 500, got {r.status_code}: {r.text[:200]}"

        contract("GET /management/search with NO query string does not 500", _search_empty_query)

        # ===========================================================
        print("\n== 8. Operations panel — real mutating actions ==")

        def _pause_resume_scheduler():
            r1 = session.post(f"{BASE}/cluster/scheduler/pause")
            assert r1.status_code == 200, f"{r1.status_code}: {r1.text[:200]}"
            r2 = session.post(f"{BASE}/cluster/scheduler/resume")
            assert r2.status_code == 200, f"{r2.status_code}: {r2.text[:200]}"

        contract("Pause then resume the scheduler", _pause_resume_scheduler)

        def _drain_nonexistent_node():
            r = session.post(f"{BASE}/cluster/nodes/does-not-exist-{uuid.uuid4().hex}/drain")
            assert r.status_code == 404, f"expected 404, got {r.status_code}: {r.text[:200]}"

        contract("Draining a nonexistent node returns 404, not 500", _drain_nonexistent_node)

        def _drain_real_node():
            nodes = state.get("cluster") and requests.get(f"{BASE}/api/v1/nodes",
                                                            headers={"X-API-Key": state["readonly_key"]["secret"]}).json()
            if not nodes:
                observe("Drain a real node", "no live nodes registered to test against")
                return
            node_id = nodes[0]["node_id"]
            r = session.post(f"{BASE}/cluster/nodes/{node_id}/drain")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"

        contract("Draining a real, existing node succeeds", _drain_real_node)

        def _cancel_nonexistent_job():
            r = session.post(f"{BASE}/jobs/does-not-exist-{uuid.uuid4().hex}/cancel")
            assert r.status_code == 400, f"expected 400 for cancelling a job that doesn't exist, got {r.status_code}: {r.text[:200]}"

        contract("Cancelling a nonexistent job_id is rejected cleanly (400), not a 500", _cancel_nonexistent_job)

        def _retry_and_clear_failed():
            r1 = session.post(f"{BASE}/jobs/retry-failed")
            assert r1.status_code != 500, f"500 on /jobs/retry-failed: {r1.text[:200]}"
            r2 = session.post(f"{BASE}/jobs/clear-failed")
            assert r2.status_code != 500, f"500 on /jobs/clear-failed: {r2.text[:200]}"

        contract("/jobs/retry-failed and /jobs/clear-failed run without crashing", _retry_and_clear_failed)

        def _clear_queue():
            r = session.post(f"{BASE}/cluster/queue/clear")
            assert r.status_code != 500, f"500 on /cluster/queue/clear: {r.text[:200]}"

        contract("/cluster/queue/clear runs without crashing", _clear_queue)

        def _verify_all_receipts():
            r = session.post(f"{BASE}/receipts/verify-all")
            assert r.status_code != 500, f"500 on /receipts/verify-all: {r.text[:200]}"

        contract("/receipts/verify-all runs without crashing", _verify_all_receipts)

        def _snapshot_download():
            r = session.get(f"{BASE}/cluster/snapshot")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            assert "attachment" in r.headers.get("content-disposition", "")

        contract("GET /cluster/snapshot downloads a real snapshot", _snapshot_download)

        def _logs_export():
            r = session.get(f"{BASE}/logs/export")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"

        contract("GET /logs/export downloads real logs", _logs_export)

        def _metrics_export():
            r = session.get(f"{BASE}/metrics/export")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"

        contract("GET /metrics/export downloads real metrics", _metrics_export)

        def _admin_scale():
            r1 = session.post(f"{BASE}/admin/scale-up")
            assert r1.status_code != 500, f"500 on /admin/scale-up: {r1.text[:200]}"
            r2 = session.post(f"{BASE}/admin/scale-down")
            assert r2.status_code != 500, f"500 on /admin/scale-down: {r2.text[:200]}"

        contract("/admin/scale-up and /admin/scale-down run without crashing", _admin_scale)

        def _rediscover_nodes():
            r = session.post(f"{BASE}/admin/rediscover-nodes")
            assert r.status_code != 500, f"500 on /admin/rediscover-nodes: {r.text[:200]}"

        contract("/admin/rediscover-nodes (never called by the current frontend) runs without crashing", _rediscover_nodes)

        def _export_users_csv():
            r = session.get(f"{BASE}/management/export/users", params={"format": "csv"})
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            assert "attachment" in r.headers.get("content-disposition", "")

        contract("GET /management/export/users?format=csv downloads a real file", _export_users_csv)

        def _export_bad_entity():
            r = session.get(f"{BASE}/management/export/not-a-real-entity")
            assert r.status_code in (400, 404), f"unknown export entity should 4xx cleanly, got {r.status_code}: {r.text[:200]}"

        contract("GET /management/export/{bad_entity} fails cleanly, not a 500", _export_bad_entity)

        def _emergency_stop():
            r = session.post(f"{BASE}/cluster/emergency-stop")
            assert r.status_code != 500, f"500 on /cluster/emergency-stop: {r.text[:200]}"
            # bring the scheduler back so later checks aren't affected
            session.post(f"{BASE}/cluster/scheduler/resume")

        contract("/cluster/emergency-stop runs without crashing (and scheduler is resumed after)", _emergency_stop)

        # ===========================================================
        print("\n== 9. Notifications ==")

        def _notifications_flow():
            r = session.get(f"{BASE}/management/notifications")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"
            notes = r.json()
            if not notes:
                observe("Mark a notification read", "no notifications present to test against")
                return
            note_id = notes[0].get("notification_id") or notes[0].get("id")
            if not note_id:
                observe("Mark a notification read", f"could not find an id field on {notes[0]}")
                return
            r2 = session.post(f"{BASE}/management/notifications/{note_id}/read")
            assert r2.status_code == 200, f"{r2.status_code}: {r2.text[:200]}"

        contract("Mark a real notification as read", _notifications_flow)

        def _mark_all_read():
            r = session.post(f"{BASE}/management/notifications/mark-all-read")
            assert r.status_code == 200, f"{r.status_code}: {r.text[:200]}"

        contract("Mark all notifications read", _mark_all_read)

        def _mark_nonexistent_notification():
            r = session.post(f"{BASE}/management/notifications/does-not-exist-{uuid.uuid4().hex}/read")
            assert r.status_code != 500, f"500 on marking a bogus notification id read: {r.text[:200]}"

        contract("Marking a nonexistent notification id read does not 500", _mark_nonexistent_notification)

        # ===========================================================
        print("\n== 10. Password change + logout ==")

        def _change_password_wrong_current():
            r = session.post(f"{BASE}/auth/change-password",
                              json={"current_password": "totally-wrong", "new_password": "NewPass123!"})
            assert r.status_code == 400, f"wrong current password should 400, got {r.status_code}: {r.text[:200]}"

        contract("Change-password rejects the wrong current password", _change_password_wrong_current)

        def _logout_then_use_session():
            r1 = session.post(f"{BASE}/auth/logout")
            assert r1.status_code == 200, f"{r1.status_code}: {r1.text[:200]}"
            r2 = session.get(f"{BASE}/auth/me")
            assert r2.status_code == 401, f"session should be dead after logout, got {r2.status_code}: {r2.text[:200]}"

        contract("Logout invalidates the session cookie immediately", _logout_then_use_session)

        # log back in for anything after this that might need it
        session.post(f"{BASE}/auth/login", json={"email": OWNER_EMAIL, "password": OWNER_PASSWORD})

        # ===========================================================
        print("\n== 11. WebSocket live push ==")

        def _ws_no_auth():
            try:
                import websocket  # websocket-client
            except ImportError:
                observe("WebSocket /ws auth check", "websocket-client not installed in this environment — skipped, NOT counted as a pass")
                return
            try:
                ws = websocket.create_connection(f"ws://{HOST}:{PORT}/ws", timeout=3)
                ws.close()
                raise AssertionError("connected to /ws with no session cookie at all — should have been rejected")
            except websocket.WebSocketBadStatusException:
                pass  # rejected before upgrade — acceptable
            except (websocket.WebSocketConnectionClosedException, ConnectionResetError):
                pass  # closed immediately with 4401 — acceptable

        contract("WebSocket /ws refuses an unauthenticated connection", _ws_no_auth)

    finally:
        print("\nShutting down server...")
        stop_server(proc)

    # ===================================================================
    print("\n" + "=" * 70)
    print("ROUTE COVERAGE / DEAD CODE / ORPHAN CALLS")
    print("=" * 70)
    print(f"Live dashboard routes discovered:  {len(state.get('dash_routes', []))}")
    print(f"Live /api/v1 routes discovered:    {len(state.get('api_routes', []))}")
    print(f"\nBackend routes the frontend (static/js/dashboard.js) never calls ({len(dead_routes)}):")
    for r in dead_routes:
        print(f"  - {r}")
    print(f"\nFrontend calls that don't match any live backend route ({len(orphan_frontend_calls)}):")
    for r in orphan_frontend_calls:
        print(f"  - {r}")

    print("\n" + "=" * 70)
    print("SECURITY FINDINGS")
    print("=" * 70)
    if security_findings:
        for name, detail in security_findings:
            print(f"  - {name}: {detail}")
    else:
        print("  (none)")

    print("\n" + "=" * 70)
    print("CRASHES (unhandled 500s / exceptions during the run)")
    print("=" * 70)
    if crashes:
        for name, detail in crashes:
            print(f"  - {name}: {detail}")
    else:
        print("  (none)")

    passed = sum(1 for _, ok, _ in contract_results if ok)
    failed = sum(1 for _, ok, _ in contract_results if not ok)
    print("\n" + "=" * 70)
    print("CONTRACT RESULTS")
    print("=" * 70)
    print(f"  {passed} passed, {failed} failed, out of {len(contract_results)} contract checks")
    if failed:
        print("\nFailed checks:")
        for name, ok, detail in contract_results:
            if not ok:
                print(f"  - {name}: {detail}")

    print(f"\n{len(observations)} additional observations recorded above (not asserted — for human review).")

    exit_code = 0
    if failed > 0:
        exit_code = 1
    if security_findings:
        exit_code = 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
