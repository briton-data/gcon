"""
GCON Public API v1 — versioned, API-key-authenticated REST API.

This is a separate FastAPI application mounted at /api/v1 by
web_server.py. It is intentionally independent from the dashboard's
cookie-session routes: every request here is authenticated with a
real API key (created from the Management > API Keys panel or the
`/management/api-keys` endpoint), never a browser session cookie.

Every endpoint is backed by the real coordinator/presentation layer
— there is no mock or placeholder data. Interactive docs are
available at /api/v1/docs (Swagger UI) and /api/v1/redoc, with the
raw schema at /api/v1/openapi.json.
"""

import hashlib
import json
import logging
import os
import re
import threading
import uuid
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Depends, Request, Response
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, model_validator

from gcon.cluster.coordinator import NotLeaderError, PolicyRejectionError
from gcon.management.rate_limit import LoginRateLimiter
from gcon.management.auth import RESET_TOKEN_TTL_MINUTES
from gcon.management.mailer import SmtpMailer, password_reset_message

log = logging.getLogger(__name__)


# ---------------------------------------------------------------
# Response / request schemas (also drive the OpenAPI docs)
# ---------------------------------------------------------------

class NodeOut(BaseModel):
    node_id: str
    status: str
    address: Optional[str] = None
    cpu: object = Field(description="CPU utilization percentage, or 'N/A'")
    memory: object = Field(description="Memory utilization percentage, or 'N/A'")
    running_jobs: int
    last_seen: object
    draining: bool
    quarantined: bool = False
    quarantine_reason: Optional[str] = None
    org_id: Optional[str] = None
    gpu_name: Optional[str] = Field(
        default=None,
        description=(
            "Live GPU reading, self-reported by the node's own software "
            "(same trust level cpu/memory always had -- not a "
            "cryptographically attested measurement). None until the "
            "node's first resource report; distinct from the static "
            "'gpu' capability flag used for requires={'gpu': true} "
            "scheduling."
        ),
    )
    gpu_memory_total: int = 0
    gpu_memory_used: int = 0
    gpu_utilization_percent: float = 0.0


class JobOut(BaseModel):
    job_id: str
    client_reference: Optional[str] = None
    status: str
    node_id: Optional[str] = None
    created_at: object = None
    completed_at: object = None
    receipt_id: Optional[str] = None
    artifacts: int = 0
    created_by: Optional[str] = None
    workflow_id: Optional[str] = None
    org_id: Optional[str] = None
    runtime_seconds: object = None
    usage: object = None
    output: Optional[str] = None
    kind: Optional[str] = Field(
        default=None,
        description="'command', 'resourced' or 'staged' -- how the job was submitted.",
    )
    requires: Optional[Dict[str, Any]] = Field(
        default=None, description="Capability requirements the job was submitted with, if any.",
    )
    verify: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Replicated-execution request the job was submitted with, if any.",
    )
    verification: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "For a replicated (verify) job, whether the replicas agreed: outcome is "
            "'agreed', 'disputed' (the replicas produced different output) or "
            "'unavailable' (they could not be compared). A disputed job still has "
            "status 'completed' and still carries the first replica's output -- check "
            "this before acting on it. None for a job that was not replicated."
        ),
    )
    error: Optional[str] = Field(
        default=None,
        description="Why a failed/cancelled job ended, when the result carries one.",
    )
    return_code: Optional[int] = None


class ClearJobsIn(BaseModel):
    job_ids: List[str] = Field(
        description="Jobs to clear. Named explicitly -- there is no 'clear everything' form.",
    )


class ClearSkippedOut(BaseModel):
    job_id: str
    reason: str = Field(description="'not_found', 'not_clearable' or 'has_receipt'")
    message: str


class ClearJobsOut(BaseModel):
    cleared: List[str]
    skipped: List[ClearSkippedOut]


MAX_CLEAR_JOBS = 200
_CLEAR_MESSAGES = {
    "not_found": "This job was not found.",
    "not_clearable": "Only failed or cancelled jobs can be cleared.",
    "has_receipt": "This job has a receipt, and receipts are never deleted.",
}


# Fields that decide HOW or WHERE a job runs (privilege, backend, image, node,
# tenant). They belong to the platform, never to the submitter: a request that
# names one is refused outright rather than silently ignored, so nobody can
# believe they asked for, say, an unsandboxed run and got it (or didn't).
_PLATFORM_ONLY_FIELDS = frozenset({
    "sandbox", "sandbox_policy", "sandboxed", "sandbox_required", "require_sandbox",
    "trusted", "privileged", "execution_backend", "backend", "image",
    "docker_image", "docker", "network", "user", "run_as", "node_id", "node",
    "org_id", "created_by",
})


class JobSubmitRequest(BaseModel):
    job_id: Optional[str] = Field(
        default=None,
        description=(
            "DEPRECATED and NOT a job identifier: GCON generates the canonical "
            "job_id and returns it. If sent (and `client_reference` is not), "
            "this value is stored as `client_reference`."
        ),
    )
    client_reference: Optional[str] = Field(
        default=None,
        description=(
            "Your own label for this job (up to 128 printable characters), "
            "returned on the job so you can correlate it with your systems. "
            "Not unique and never used to look a job up or authorise anything."
        ),
    )
    command: str = Field(..., description="Shell command the job will run")
    artifacts: Optional[List[str]] = Field(
        default=None, description="Not supported through the API: a non-empty list is refused (400). Use dataset_artifacts."
    )
    kind: str = Field(
        default="command",
        description=(
            "'command' (default -- plain, unstructured), 'resourced' "
            "(adds `requires`, matched against node capabilities at "
            "scheduling time), or 'staged' (adds `stages`, a "
            "per-checkpoint progress-reporting contract)."
        ),
    )
    requires: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "'resourced' jobs only: capability requirements matched "
            "against each candidate node before dispatch, e.g. "
            '{"gpu": true, "min_vram_gb": 12, "min_cpu_cores": 4}.'
        ),
    )
    stages: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "'staged' jobs only, e.g. {\"expected\": 15}. GCON does not "
            "run the stages itself -- the job's own code reports each "
            "one via GCON_STAGE_REPORT_PATH; see docs."
        ),
    )
    dataset_artifacts: Optional[List[str]] = Field(
        default=None,
        description=(
            "IDs of artifacts already registered with this coordinator "
            "(not filepaths) that this job declares as input data. "
            "Folded into the receipt's input_hash at completion."
        ),
    )
    callback_url: Optional[str] = Field(
        default=None,
        description=(
            "If set, GCON POSTs a signed JSON payload here when this "
            "job reaches a terminal state (completed/failed/cancelled), "
            "instead of requiring the submitter to poll. Body is signed "
            "via HMAC-SHA256 in the X-GCON-Signature header."
        ),
    )
    verify: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Dispatches this job to N independently-selected idle nodes "
            "instead of one, and compares their results for agreement -- "
            "e.g. {\"replicas\": 2, \"tolerance\": 0.02}. Orthogonal to "
            "`kind`/`requires`/`stages`: a 'resourced' or 'staged' job "
            "can also ask for replication. Each replica still gets its "
            "own independently-signed receipt; this only adds a derived "
            "agreement annotation on top, it does not replace or weaken "
            "per-node signing. Previously Python-API only -- see "
            "GCONCoordinator.submit_job's `verify` docstring for full "
            "detail."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _reject_platform_only_fields(cls, data):
        if isinstance(data, dict):
            named = sorted(k for k in data if isinstance(k, str) and k.lower() in _PLATFORM_ONLY_FIELDS)
            if named:
                raise ValueError(
                    "Not accepted on job submission: " + ", ".join(named)
                    + ". How and where a job runs (sandbox, privilege, image, "
                    "node, organization) is decided by GCON, not by the request."
                )
        return data


class JobSubmitResponse(BaseModel):
    job_id: str
    submitted: bool = True
    client_reference: Optional[str] = None


class JobCancelResponse(BaseModel):
    job_id: str
    cancelled: bool
    process_killed: bool


class WorkflowOut(BaseModel):
    workflow_id: str
    status: str

    class Config:
        extra = "allow"


class ReceiptOut(BaseModel):
    receipt_id: str
    job_id: Optional[str] = None
    status: str
    created_at: object = None


class ArtifactOut(BaseModel):
    artifact_id: str
    filename: str
    sha256: str
    size: int
    uploaded_at: object = None


class ClusterStateOut(BaseModel):
    total_nodes: int
    idle_nodes: int
    registered_node_count: int
    running_jobs: int
    completed_jobs: int
    failed_jobs: int

    class Config:
        extra = "allow"


class HealthOut(BaseModel):
    state: str

    class Config:
        extra = "allow"


class ErrorOut(BaseModel):
    detail: str


class CustomerSignupIn(BaseModel):
    org_name: str
    name: str
    email: str
    password: str


class CustomerLoginIn(BaseModel):
    email: str
    password: str


class CustomerAuthOut(BaseModel):
    """
    Response for /auth/signup and /auth/login. Both return a freshly
    created API key in `api_key` (the secret is revealed exactly once,
    same convention as ManagementLayer.create_api_key() everywhere
    else -- it cannot be retrieved again after this response). A
    separate frontend stores `api_key.secret` and sends it as a Bearer
    token on every subsequent /api/v1 request -- the same
    authentication every other endpoint in this file already uses --
    and revokes it on logout. No session cookie is involved anywhere
    in this file.
    """
    organization: Optional[dict] = None
    customer_user: Optional[dict] = None
    api_key: Optional[dict] = None

    class Config:
        extra = "allow"


class ForgotPasswordIn(BaseModel):
    email: str


class ForgotPasswordOut(BaseModel):
    """
    Password reset responses are deliberately identical for known and
    unknown email addresses. When SMTP is configured, a single-use,
    time-limited reset token is emailed to the account's address and is
    never returned in the API response. In local development,
    GCON_EXPOSE_RESET_TOKEN can be enabled to return the token directly.
    If SMTP is not configured, no token is issued.
    """
    token: Optional[str] = None
    delivery: str = Field(
        default="unavailable",
        description=(
            "'email': a reset link was emailed if the account exists (the same "
            "answer is given for every address). 'unavailable': email isn't set up, "
            "so nothing was sent and no token was issued. 'dev_token': "
            "GCON_EXPOSE_RESET_TOKEN is set, so the token is returned in this "
            "response -- local development only."
        ),
    )


class ResetPasswordIn(BaseModel):
    token: str
    new_password: str


class ApiKeyCreateIn(BaseModel):
    name: str


MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 256
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _validate_password(password):
    """Server-side password rule (the frontend's own check is only a
    convenience -- it is not the security boundary). Raises ValueError
    with a message safe to show to the customer."""
    if not isinstance(password, str) or len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters.")
    if len(password) > MAX_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at most {MAX_PASSWORD_LENGTH} characters.")


def _validate_signup(payload):
    if not (payload.org_name or "").strip():
        raise ValueError("Organization name is required.")
    if not (payload.name or "").strip():
        raise ValueError("Your name is required.")
    if not _EMAIL_RE.match((payload.email or "").strip()):
        raise ValueError("Enter a valid email address.")
    _validate_password(payload.password)


SIGNUPS_PER_HOUR_PER_IP = 10
MAX_SESSION_KEYS_PER_CUSTOMER = 5
_CLIENT_REFERENCE_MAX = 128
_IDEMPOTENCY_KEY_MAX = 255
# Serialises "look up key -> submit -> record key" per (org, key). Only the
# leader accepts submissions, so one process-local lock set is enough; striped
# so unrelated keys never wait on each other.
_IDEMPOTENCY_LOCKS = [threading.Lock() for _ in range(64)]


def _new_job_id() -> str:
    """Canonical, server-minted job id: unguessable and globally unique, so
    no submitter can pick, collide with, or probe for another tenant's id."""
    return "job_" + uuid.uuid4().hex


def _new_workflow_id() -> str:
    return "wf_" + uuid.uuid4().hex


def _clean_client_reference(value):
    """Validate the submitter's own label: a correlation tag, nothing more."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail="client_reference must be a string.")
    value = value.strip()
    if not value:
        return None
    if len(value) > _CLIENT_REFERENCE_MAX or not value.isprintable():
        raise HTTPException(
            status_code=400,
            detail=f"client_reference must be at most {_CLIENT_REFERENCE_MAX} printable characters.",
        )
    return value


def _request_fingerprint(payload) -> str:
    """Hash of what a submission asks GCON to DO. `client_reference` is only a
    label, so changing it does not make a retry a different request."""
    material = {
        name: getattr(payload, name)
        for name in ("command", "artifacts", "kind", "requires", "stages",
                     "dataset_artifacts", "callback_url", "verify")
    }
    blob = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _idempotency_lock(org_id, key):
    digest = hashlib.sha256(f"{org_id or ''}\0{key}".encode("utf-8")).digest()
    return _IDEMPOTENCY_LOCKS[digest[0] % len(_IDEMPOTENCY_LOCKS)]


def create_api_v1_app(management, presentation, rate_limiter=None, client_ip=None, mailer=None):
    """
    Build the /api/v1 sub-application. `management` is the shared
    ManagementLayer instance (for API key auth) and `presentation`
    is the shared PresentationLayer (for real cluster data) — the
    same instances the dashboard itself uses, so the public API and
    the dashboard are always looking at the same live state.

    `rate_limiter` / `client_ip` are optional: WebServer passes its own
    LoginRateLimiter and trusted-proxy-aware IP resolver so /auth/login
    is throttled with the same configuration as the staff login. When
    omitted (tests, standalone use) a private in-process limiter and the
    plain TCP peer address are used. `mailer` sends password-reset emails
    (default: SmtpMailer, configured through GCON_SMTP_* environment variables).
    """
    if mailer is None:
        mailer = SmtpMailer()
    if rate_limiter is None:
        rate_limiter = LoginRateLimiter(db=getattr(management, "db", None))
    if client_ip is None:
        def client_ip(request):
            return request.client.host if request.client else None

    # Signup creates an organization, a user and a working API key with no
    # human check. Without a limit one client can mint them without bound
    # (16 parallel signups, 0 errors). Every signup attempt counts, per client
    # IP: SIGNUPS_PER_HOUR_PER_IP of them, then 429 until the window ages out.
    signup_limiter = LoginRateLimiter(
        max_attempts=SIGNUPS_PER_HOUR_PER_IP, window_minutes=60, lockout_minutes=60,
        db=getattr(management, "db", None),
    )

    app = FastAPI(
        title="GCON Public API",
        version="1.0.0",
        description=(
            "Versioned public API for the GCON distributed compute "
            "cluster. Authenticate with an API key created in the "
            "dashboard's Management > API Keys panel, sent either as "
            "`Authorization: Bearer <key>` or `X-API-Key: <key>`."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # ------------------------------------------------------------
    # Auth dependency factory
    # ------------------------------------------------------------

    def require_scope(scope: Optional[str] = None, tenant: bool = True):
        # `tenant=True` (the default) marks a route that serves a customer's
        # own data: the key must belong to an organization. An org-less or
        # ownerless key resolves to org_id=None, which every query below reads
        # as "no filter" -- i.e. EVERY tenant's jobs, receipts and workflows --
        # so it is refused here, once, before any route body runs. Cluster-wide
        # routes that never filter by organization opt out with tenant=False.
        def dependency(
            authorization: str = Header(default=None),
            x_api_key: str = Header(default=None, alias="X-API-Key"),
        ):
            secret = x_api_key
            if not secret and authorization:
                parts = authorization.split(" ", 1)
                if len(parts) == 2 and parts[0].lower() == "bearer":
                    secret = parts[1]
                else:
                    secret = authorization

            if not secret:
                raise HTTPException(
                    status_code=401,
                    detail="Missing API key. Send it as 'Authorization: Bearer <key>' "
                           "or 'X-API-Key: <key>'.",
                )

            try:
                key, owner = management.authenticate_api_key(secret, required_scope=scope)
            except ValueError as e:
                raise HTTPException(status_code=401, detail=str(e))

            if tenant and (owner is None or getattr(owner, "organization_id", None) is None):
                raise HTTPException(
                    status_code=403,
                    detail="This API key is not attached to an organization, so it "
                           "cannot be used to read or change customer data.",
                )

            return {"key": key, "owner": owner}

        return dependency

    # ------------------------------------------------------------
    # Auth
    #
    # /auth/signup, /auth/login, /auth/forgot-password and
    # /auth/reset-password are deliberately the only routes in this
    # file that do NOT require an API key -- they are how a customer
    # gets one in the first place. A separate frontend (its own
    # repo/deploy, not part of gcon-rebuild) calls them directly:
    # signup and login each return a usable API key immediately (same
    # one-time-reveal convention as every other key creation in this
    # codebase), which the frontend sends as a Bearer token on every
    # other call and revokes on logout. No session cookie, no
    # server-rendered page -- pure JSON in, JSON out.
    # ------------------------------------------------------------

    @app.post(
        "/auth/signup",
        response_model=CustomerAuthOut,
        tags=["Auth"],
        summary="Create a new organization and customer account",
        responses={400: {"model": ErrorOut}},
    )
    def auth_signup(payload: CustomerSignupIn, request: Request):
        ip = client_ip(request)
        try:
            signup_limiter.check("signup", ip)
        except ValueError:
            raise HTTPException(status_code=429, detail="Too many signups from this address. Try again later.")
        signup_limiter.record_failure("signup", ip)      # counts the attempt, success or not
        try:
            _validate_signup(payload)
            result = management.signup_customer(
                payload.org_name, payload.name, payload.email, payload.password,
            )
        except ValueError as e:
            # signup_customer's own duplicate-email check (see its
            # docstring) -- surfaced as-is, a signup form is expected
            # to show this directly to the customer.
            raise HTTPException(status_code=400, detail=str(e))
        return result

    @app.post(
        "/auth/login",
        response_model=CustomerAuthOut,
        tags=["Auth"],
        summary="Log in: verify credentials and receive a session API key",
        responses={401: {"model": ErrorOut}, 429: {"model": ErrorOut}},
    )
    def auth_login(payload: CustomerLoginIn, request: Request):
        # Rate-limited exactly like the staff login (check -> 429,
        # record_failure on a bad password, record_success clears the
        # counter), but under its own "customer-login:" key namespace
        # so a customer's failed attempts can never lock out a staff
        # account that happens to share an email address.
        limiter_key = f"customer-login:{payload.email}"
        ip = client_ip(request)
        try:
            rate_limiter.check(limiter_key, ip)
        except ValueError as e:
            raise HTTPException(status_code=429, detail=str(e))

        # Deliberately calls customer_registry.authenticate() directly
        # rather than management.customer_login() -- the latter also
        # creates a cookie-session row this API never reads.
        customer = management.customer_registry.authenticate(payload.email, payload.password)
        if customer is None:
            rate_limiter.record_failure(limiter_key, ip)
            raise HTTPException(status_code=401, detail="Invalid email or password.")
        rate_limiter.record_success(limiter_key, ip)

        try:
            organization = management.org_registry.get_organization(customer.org_id).to_dict()
        except ValueError:
            organization = None

        # A successful password login is what grants the frontend a
        # credential: a fresh, named API key (same scopes, same 90-day
        # expiry as every customer key) revealed exactly once in this
        # response. The frontend sends it as a Bearer token and revokes
        # it on logout (DELETE /auth/api-keys/{key_id}, key_id from
        # /whoami). Without this a customer who logged out could never
        # get back in, since no other route hands out a first key.
        stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
        session_key = management.create_customer_api_key(
            customer.org_id, customer.customer_user_id, f"Web session {stamp} UTC",
        )
        # Every login mints a key; without a bound they pile up forever
        # (and each stays valid 90 days). Keep the newest few per customer.
        management.limit_session_api_keys(customer.customer_user_id, keep=MAX_SESSION_KEYS_PER_CUSTOMER)
        return {
            "organization": organization,
            "customer_user": customer.to_dict(),
            "api_key": session_key,
        }

    def _deliver_reset_email(to, subject, body):
        # Runs in a background thread: the response must not wait for (or
        # reveal, by its timing) whether an email was actually sent. Nothing
        # sensitive is logged -- the token only ever appears in `body`.
        try:
            mailer.send(to, subject, body)
        except Exception as e:
            log.error("Password reset email could not be sent (%s: %s)", type(e).__name__, str(e)[:200])

    @app.post(
        "/auth/forgot-password",
        response_model=ForgotPasswordOut,
        tags=["Auth"],
        summary="Request a password reset",
    )
    def auth_forgot_password(payload: ForgotPasswordIn, request: Request):
        # Three modes, decided fresh on every call (environment is read live):
        #
        # 1. GCON_EXPOSE_RESET_TOKEN=1 -- local development ONLY: the token is
        #    returned in the response so the flow can be tried without a mail
        #    server. Never enable this in production: it lets anyone who knows
        #    an email address take over that account.
        # 2. Email configured (GCON_SMTP_HOST / _FROM, GCON_WEB_BASE_URL) -- the
        #    real thing: create a single-use, time-limited token and EMAIL it to
        #    the account's own address. The caller is never given the token.
        # 3. Otherwise -- no way to deliver a token to its rightful owner, so none
        #    is created; the response says so.
        #
        # In every mode the answer is identical for known and unknown emails, so
        # this can't be used to discover which addresses have accounts.
        expose = os.environ.get("GCON_EXPOSE_RESET_TOKEN", "").strip().lower() in ("1", "true", "yes")
        if expose:
            customer = management.customer_registry.get_user_by_email(payload.email)
            if customer is None:
                return {"token": None, "delivery": "dev_token"}
            token = management.customer_reset_token_manager.create_token(customer.customer_user_id)
            return {"token": token, "delivery": "dev_token"}

        if not mailer.configured():
            return {"token": None, "delivery": "unavailable"}

        # Every request counts against a per-address limit (whether or not the
        # account exists), so this can't be used to flood someone's inbox.
        limiter_key = f"customer-reset:{payload.email.strip().lower()}"
        ip = client_ip(request)
        try:
            rate_limiter.check(limiter_key, ip)
        except ValueError:
            raise HTTPException(status_code=429, detail="Too many reset requests. Please wait a few minutes and try again.")
        rate_limiter.record_failure(limiter_key, ip)

        customer = management.customer_registry.get_user_by_email(payload.email)
        if customer is not None:
            token = management.customer_reset_token_manager.create_token(customer.customer_user_id)
            subject, body = password_reset_message(getattr(customer, "name", ""), token, RESET_TOKEN_TTL_MINUTES)
            threading.Thread(target=_deliver_reset_email, args=(customer.email, subject, body), daemon=True).start()
        return {"token": None, "delivery": "email"}

    @app.post(
        "/auth/reset-password",
        tags=["Auth"],
        summary="Set a new password using a reset token",
        responses={400: {"model": ErrorOut}},
    )
    def auth_reset_password(payload: ResetPasswordIn):
        try:
            _validate_password(payload.new_password)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        customer_user_id = management.customer_reset_token_manager.get_customer_user_id(payload.token)
        if customer_user_id is None:
            raise HTTPException(status_code=400, detail="This reset link is invalid or has expired.")
        management.customer_registry.set_password(customer_user_id, payload.new_password)
        management.customer_reset_token_manager.consume_token(payload.token)
        # Invalidate every other outstanding token for this user too,
        # not just the one just used -- otherwise an old, still-valid
        # reset link sitting in an inbox could reset the password
        # again later, silently, after the customer thinks this is
        # done and forgotten.
        management.customer_reset_token_manager.invalidate_all_for_user(customer_user_id)
        return {"reset": True}

    # ------------------------------------------------------------
    # API keys (customer self-service)
    #
    # Uses require_scope() with no scope argument -- valid
    # authentication only, no specific scope required. Neither
    # existing scope ("Submit workflows" / "View monitoring") is
    # semantically about key management, so requiring either one
    # would arbitrarily lock out a key that only has the other.
    # ------------------------------------------------------------

    @app.get(
        "/auth/api-keys",
        tags=["Auth"],
        summary="List this organization's API keys",
        responses={401: {"model": ErrorOut}},
    )
    def list_api_keys(auth=Depends(require_scope())):
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if org_id is None:
            return []
        return jsonable_encoder(management.list_customer_api_keys(org_id))

    @app.post(
        "/auth/api-keys",
        tags=["Auth"],
        summary="Create a new API key for this organization",
        responses={401: {"model": ErrorOut}},
    )
    def create_api_key(payload: ApiKeyCreateIn, auth=Depends(require_scope())):
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if org_id is None:
            raise HTTPException(status_code=400, detail="This key has no organization to create a key for.")
        # created_by is the CALLER (owner.user_id), not necessarily
        # whoever's key this ends up looking like it belongs to -- any
        # teammate's key can create a new key for the org, matching
        # list_customer_api_keys' own "org-level, not per-user
        # resource" principle.
        return jsonable_encoder(
            management.create_customer_api_key(org_id, owner.user_id, payload.name)
        )

    @app.delete(
        "/auth/api-keys/{key_id}",
        tags=["Auth"],
        summary="Revoke an API key",
        responses={401: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def revoke_api_key(key_id: str, auth=Depends(require_scope())):
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        try:
            return jsonable_encoder(management.revoke_customer_api_key(org_id, key_id))
        except ValueError as e:
            # revoke_customer_api_key already gives the same message
            # for "doesn't exist" and "belongs to a different org" --
            # see its own docstring - so this can't be used to probe
            # for other orgs' key ids either.
            raise HTTPException(status_code=404, detail=str(e))

    # ------------------------------------------------------------
    # Cluster
    # ------------------------------------------------------------

    @app.get(
        "/cluster",
        response_model=ClusterStateOut,
        tags=["Cluster"],
        summary="Get current cluster state",
        responses={401: {"model": ErrorOut}},
    )
    def get_cluster(auth=Depends(require_scope("View monitoring", tenant=False))):
        return jsonable_encoder(presentation.get_cluster_state())

    @app.get(
        "/health",
        response_model=HealthOut,
        tags=["Cluster"],
        summary="Get overall cluster health",
        responses={401: {"model": ErrorOut}},
    )
    def get_health(auth=Depends(require_scope("View monitoring", tenant=False))):
        return jsonable_encoder(presentation.get_cluster_health())

    @app.get(
        "/metrics",
        tags=["Cluster"],
        summary="Get aggregate node and job metrics",
        responses={401: {"model": ErrorOut}},
    )
    def get_metrics(auth=Depends(require_scope("View monitoring", tenant=False))):
        return jsonable_encoder(presentation.get_system_metrics())

    # ------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------

    @app.get(
        "/nodes",
        response_model=List[NodeOut],
        tags=["Nodes"],
        summary="List all registered nodes",
        responses={401: {"model": ErrorOut}},
    )
    def list_nodes(auth=Depends(require_scope("View monitoring"))):
        # Org-scoped keys (a user with organization_id set) only ever
        # see their own company's nodes -- previously this endpoint
        # returned every node in the cluster to any key with "View
        # monitoring", regardless of which company it belonged to.
        # A key with no organization_id (system/internal) still sees
        # everything, matching the same convention submit_job already
        # uses for attribution.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        return jsonable_encoder(presentation.get_nodes(org_id=org_id))

    @app.get(
        "/nodes/{node_id}",
        response_model=NodeOut,
        tags=["Nodes"],
        summary="Get a single node by id",
        responses={401: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def get_node(node_id: str, auth=Depends(require_scope("View monitoring"))):
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        for node in presentation.get_nodes(org_id=org_id):
            if node["node_id"] == node_id:
                return jsonable_encoder(node)
        # Deliberately the same 404 whether the node doesn't exist at
        # all or exists but belongs to a different company -- an
        # org-scoped 403/differentiated error would leak that a node
        # with this id exists somewhere in the cluster.
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found.")

    @app.get(
        "/nodes/{node_id}/enrollment-history",
        tags=["Nodes"],
        summary="Durable audit trail of Enroll RPC attempts for this node_id",
        responses={401: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def get_node_enrollment_history(node_id: str, auth=Depends(require_scope("View monitoring"))):
        # Same org-scoping + same-404-either-way pattern as get_node()
        # above -- an org-scoped key can only pull enrollment history
        # (source IPs, which enroll_token_id was used) for nodes that
        # are actually theirs, and gets the same 404 for "doesn't
        # exist" vs "belongs to a different company" so this can't be
        # used to probe which node_ids exist elsewhere in the cluster.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if not any(n["node_id"] == node_id for n in presentation.get_nodes(org_id=org_id)):
            raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found.")
        return jsonable_encoder(presentation.get_node_enrollment_history(node_id))

    # ------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------

    @app.get(
        "/jobs",
        response_model=List[JobOut],
        tags=["Jobs"],
        summary="List all jobs",
        responses={401: {"model": ErrorOut}},
    )
    def list_jobs(
        status: Optional[str] = None,
        limit: Optional[int] = None,
        client_reference: Optional[str] = None,
        auth=Depends(require_scope("View monitoring")),
    ):
        # Same org-scoping as list_nodes above -- this previously
        # returned every job ever submitted by every company to any
        # key with "View monitoring", not just the caller's own jobs.
        #
        # `status`/`limit` were added here as optional query params --
        # presentation.get_jobs() already accepts and correctly
        # applies both (used internally by the dashboard's jobs
        # panel), this route just never exposed them. Both default to
        # None, which get_jobs() already treats as "no filter" --
        # so an unfiltered GET /jobs call is byte-for-byte the same
        # request/response it always was; nothing about the existing,
        # working default path changes.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if client_reference is None:
            return jsonable_encoder(presentation.get_jobs(org_id=org_id, status=status, limit=limit))
        # Look up your own jobs by the label you gave them (still scoped to the
        # caller's organization, like every other read here).
        matches = [
            j for j in presentation.get_jobs(org_id=org_id, status=status)
            if j.get("client_reference") == client_reference
        ]
        return jsonable_encoder(matches[:limit] if limit is not None else matches)

    @app.get(
        "/jobs/{job_id}",
        response_model=JobOut,
        tags=["Jobs"],
        summary="Get a single job by id",
        responses={401: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def get_job(job_id: str, auth=Depends(require_scope("View monitoring"))):
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        for job in presentation.get_jobs(org_id=org_id):
            if job["job_id"] == job_id:
                return jsonable_encoder(job)
        # Same reasoning as get_node: identical 404 for "doesn't
        # exist" and "belongs to a different company" so the error
        # can't be used to probe for other companies' job ids.
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    @app.post(
        "/jobs",
        response_model=JobSubmitResponse,
        tags=["Jobs"],
        summary="Submit a new job",
        responses={401: {"model": ErrorOut}, 400: {"model": ErrorOut}},
    )
    def submit_job(
        payload: JobSubmitRequest,
        response: Response,
        auth=Depends(require_scope("Submit workflows")),
        idempotency_key: str = Header(default=None, alias="Idempotency-Key"),
    ):
        owner = auth["owner"]
        # A job is attributed to a company via its submitter's
        # organization_id -- not the API key itself, since scopes/keys
        # aren't org-scoped objects in this codebase, users are (see
        # gcon.management.users.User.organization_id). No owner (e.g.
        # a system/internal key with no user attached) or a user with
        # no organization both legitimately resolve to org_id=None.
        org_id = getattr(owner, "organization_id", None) if owner else None

        # The submitter's label. The deprecated `job_id` field is only ever
        # read as a label -- it never becomes the canonical id.
        client_reference = _clean_client_reference(
            payload.client_reference if payload.client_reference is not None else payload.job_id
        )

        # `artifacts` takes paths on the COORDINATOR's disk. A customer must never
        # be able to name one (it would read and hash any file the coordinator
        # can see, and list the result), so the field is refused here. To use
        # data in a job, reference an artifact your own jobs produced, in
        # `dataset_artifacts`.
        if payload.artifacts:
            raise HTTPException(
                status_code=400,
                detail="Registering artifacts by file path is not supported through the API.",
            )

        def _do_submit(job_id):
            try:
                presentation.submit_job(
                    job_id,
                    payload.command,
                    payload.artifacts,
                    created_by=owner.user_id if owner else None,
                    org_id=org_id,
                    kind=payload.kind,
                    requires=payload.requires,
                    stages=payload.stages,
                    dataset_artifacts=payload.dataset_artifacts,
                    callback_url=payload.callback_url,
                    verify=payload.verify,
                    client_reference=client_reference,
                    # Everything arriving through this route is a public job:
                    # only ever dispatched to a sandboxed worker, whatever
                    # GCON_SANDBOX_POLICY says.
                    sandbox_required=True,
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            except PolicyRejectionError as e:
                # Same handling as ValueError above -- a policy-rejected
                # submission is a client-facing 400 (the request itself
                # was fine, but policy says no), not a 500.
                raise HTTPException(status_code=400, detail=str(e))
            except NotLeaderError as e:
                # 503 (not 400/404): the request itself is fine, this
                # coordinator process just isn't the one that should
                # handle it right now -- a client/load-balancer should
                # retry, ideally against the leader. See
                # gcon.cluster.leader_election / GCONCoordinator.submit_job.
                raise HTTPException(status_code=503, detail=str(e))

        if not idempotency_key:
            job_id = _new_job_id()
            _do_submit(job_id)
            return {"job_id": job_id, "submitted": True, "client_reference": client_reference}

        # Durable idempotency, scoped per-org (see IdempotencyKeyRepository).
        # Job ids are now minted here, so two concurrent requests with the same
        # key would otherwise each create a job: the lookup, the submission and
        # the record therefore happen under one per-(org, key) lock.
        if len(idempotency_key) > _IDEMPOTENCY_KEY_MAX or not idempotency_key.isprintable():
            raise HTTPException(
                status_code=400,
                detail=f"Idempotency-Key must be at most {_IDEMPOTENCY_KEY_MAX} printable characters.",
            )
        request_hash = _request_fingerprint(payload)
        with _idempotency_lock(org_id, idempotency_key):
            record = presentation.get_idempotency_record(org_id, idempotency_key)
            if record is not None:
                stored_hash = record.get("request_hash")
                if stored_hash and stored_hash != request_hash:
                    raise HTTPException(
                        status_code=422,
                        detail="This Idempotency-Key was already used for a different request.",
                    )
                response.headers["Idempotent-Replayed"] = "true"
                original_id = record["job_id"]
                original = presentation.coordinator.jobs.get(original_id)
                if original is None and presentation.coordinator.control_plane is not None:
                    original = presentation.coordinator.control_plane.jobs.get(original_id)
                return {
                    "job_id": original_id,
                    "submitted": True,
                    "client_reference": (original or {}).get("client_reference"),
                }
            job_id = _new_job_id()
            _do_submit(job_id)
            presentation.record_idempotency_key(org_id, idempotency_key, job_id, request_hash)
        return {"job_id": job_id, "submitted": True, "client_reference": client_reference}

    @app.post(
        "/jobs/clear",
        response_model=ClearJobsOut,
        tags=["Jobs"],
        summary="Permanently clear failed or cancelled jobs",
        responses={400: {"model": ErrorOut}, 401: {"model": ErrorOut}},
    )
    def clear_jobs(payload: ClearJobsIn, auth=Depends(require_scope("Submit workflows"))):
        # Clears only the jobs named in the request, only if they belong to the
        # caller's organization, are failed or cancelled, and have no receipt.
        # Everything else comes back in `skipped` with a reason and is left
        # untouched. Another organization's job id gets the same "not_found"
        # as a job that doesn't exist, so ids can't be probed.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if not org_id:
            raise HTTPException(status_code=400, detail="Clearing jobs needs an organization account.")
        ids = [j.strip() for j in payload.job_ids if isinstance(j, str) and j.strip()]
        if not ids:
            raise HTTPException(status_code=400, detail="Choose at least one job to clear.")
        if len(ids) > MAX_CLEAR_JOBS:
            raise HTTPException(status_code=400, detail=f"You can clear at most {MAX_CLEAR_JOBS} jobs at a time.")
        try:
            result = presentation.coordinator.clear_jobs_for_org(org_id, ids)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {
            "cleared": result["cleared"],
            "skipped": [{**sk, "message": _CLEAR_MESSAGES.get(sk["reason"], "This job could not be cleared.")} for sk in result["skipped"]],
        }

    @app.post(
        "/jobs/{job_id}/cancel",
        response_model=JobCancelResponse,
        tags=["Jobs"],
        summary="Cancel a queued or running job",
        responses={401: {"model": ErrorOut}, 400: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def cancel_job(job_id: str, auth=Depends(require_scope("Submit workflows"))):
        # SECURITY FIX: this route previously called
        # presentation.cancel_job(job_id) with no org check at all --
        # any authenticated key from ANY org could cancel ANY job in
        # the whole system just by knowing/guessing its job_id, a
        # real cross-tenant authorization bypass. Same org-ownership
        # check as get_job() above, and same reasoning: 404 (not 403)
        # for "doesn't exist" and "belongs to a different company" so
        # this can't be used to probe which job_ids exist elsewhere.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if not any(j["job_id"] == job_id for j in presentation.get_jobs(org_id=org_id)):
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
        try:
            result = presentation.cancel_job(job_id)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return jsonable_encoder(result)

    @app.get(
        "/jobs/{job_id}/attempts",
        tags=["Jobs"],
        summary="Durable dispatch-attempt history for a job",
        responses={401: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def get_job_attempts(job_id: str, auth=Depends(require_scope("View monitoring"))):
        # Same org-scoping + same-404-either-way pattern as
        # get_node_enrollment_history -- an org-scoped key can only
        # pull attempt history for jobs that are actually theirs, and
        # gets the same 404 for "doesn't exist" vs "belongs to a
        # different company" so this can't be used to probe which job
        # ids exist elsewhere in the cluster.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if not any(j["job_id"] == job_id for j in presentation.get_jobs(org_id=org_id)):
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
        return jsonable_encoder(presentation.get_job_attempts(job_id))

    @app.post(
        "/jobs/{job_id}/retry",
        tags=["Jobs"],
        summary="Retry a failed or stuck-pending job",
        responses={401: {"model": ErrorOut}, 400: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def retry_job(job_id: str, auth=Depends(require_scope("Submit workflows"))):
        # Backend (coordinator.retry_job/presentation.retry_job) was
        # already fully built and tested; this route simply never
        # existed. Same org-ownership check as cancel_job above --
        # written correctly from the start this time.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if not any(j["job_id"] == job_id for j in presentation.get_jobs(org_id=org_id)):
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
        try:
            result = presentation.retry_job(job_id)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return jsonable_encoder(result)

    # ------------------------------------------------------------
    # Workflows
    # ------------------------------------------------------------

    class WorkflowJobIn(BaseModel):
        # A label that is only meaningful inside this request (it is what
        # `depends_on` refers to). GCON mints the real job ids and returns the
        # label -> id mapping; the label is stored as the job's client_reference.
        job_id: str
        command: str
        depends_on: List[str] = Field(default_factory=list)

    class WorkflowSubmitRequest(BaseModel):
        # Optional label, stored as the workflow's client_reference. GCON mints
        # the canonical workflow_id.
        workflow_id: Optional[str] = None
        name: str = ""
        jobs: List[WorkflowJobIn]

    class WorkflowSubmitResponse(BaseModel):
        workflow_id: str
        status: str
        submitted: bool
        client_reference: Optional[str] = None
        jobs: Dict[str, str] = Field(
            default_factory=dict,
            description="Your job label -> the canonical GCON job_id.",
        )

    @app.post(
        "/workflows",
        response_model=WorkflowSubmitResponse,
        tags=["Workflows"],
        summary="Submit a new workflow (DAG of jobs)",
        responses={401: {"model": ErrorOut}, 400: {"model": ErrorOut}},
    )
    def submit_workflow(payload: WorkflowSubmitRequest, auth=Depends(require_scope("Submit workflows"))):
        from gcon.workflow.workflow import Workflow, WorkflowJob

        owner = auth["owner"]
        labels = [job_in.job_id for job_in in payload.jobs]
        if len(set(labels)) != len(labels):
            raise HTTPException(status_code=400, detail="Job labels within a workflow must be unique.")
        client_reference = _clean_client_reference(payload.workflow_id)
        # Canonical ids are minted here, exactly as for POST /jobs: a workflow
        # can neither pick nor collide with another tenant's job or workflow id.
        canonical = {label: _new_job_id() for label in labels}
        workflow = Workflow(
            workflow_id=_new_workflow_id(),
            name=payload.name or (client_reference or ""),
            # Persisted with the workflow definition, so the platform-set
            # sandbox requirement survives into every job the engine submits.
            metadata={"client_reference": client_reference, "sandbox_required": True},
            created_by=owner.user_id if owner else None,
            # Same attribution rule as a directly submitted job: the
            # submitter's organization (None for a system key).
            org_id=getattr(owner, "organization_id", None) if owner else None,
        )
        try:
            for job_in in payload.jobs:
                workflow.add_job(WorkflowJob(
                    job_id=canonical[job_in.job_id], command=job_in.command,
                    metadata={"client_reference": _clean_client_reference(job_in.job_id)},
                ))
            for job_in in payload.jobs:
                for parent_label in job_in.depends_on:
                    if parent_label not in canonical:
                        raise ValueError(
                            f"Job '{job_in.job_id}' depends on '{parent_label}', "
                            "which is not a job in this workflow."
                        )
                    workflow.add_dependency(canonical[parent_label], canonical[job_in.job_id])

            state = presentation.submit_workflow(workflow)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except NotLeaderError as e:
            # Same as submit_job: the request is fine, this coordinator is
            # just not the leader right now -- retry against the leader.
            raise HTTPException(status_code=503, detail=str(e))

        return {
            "workflow_id": workflow.workflow_id, "status": state.status, "submitted": True,
            "client_reference": client_reference, "jobs": canonical,
        }

    @app.get(
        "/workflows",
        tags=["Workflows"],
        summary="List all workflows",
        responses={401: {"model": ErrorOut}},
    )
    def list_workflows(auth=Depends(require_scope("View monitoring"))):
        # Org-scoped keys only ever see their own company's workflows.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        return jsonable_encoder(presentation.get_workflows(org_id=org_id))

    # ------------------------------------------------------------
    # Receipts & Artifacts
    # ------------------------------------------------------------

    @app.get(
        "/receipts",
        response_model=List[ReceiptOut],
        tags=["Receipts"],
        summary="List all job receipts",
        responses={401: {"model": ErrorOut}},
    )
    def list_receipts(
        verified: Optional[bool] = None,
        limit: Optional[int] = None,
        auth=Depends(require_scope("View monitoring")),
    ):
        # Same org-scoping as list_nodes/list_jobs above -- this
        # previously returned every receipt ever issued to any company
        # to any key with "View monitoring", not just the caller's
        # own receipts. Receipts are the customer-facing proof
        # artifact, so this was the sharpest version of the
        # cross-tenant leak: any org's API key could read every other
        # org's execution receipts.
        #
        # `verified`/`limit`: presentation.get_receipts() itself takes
        # no filter args (only org_id) -- unlike get_jobs(), it was
        # never extended, so it's left completely untouched here and
        # still backs the plain, unfiltered call exactly as before.
        # When a filter IS given, this instead calls the already-
        # built, already-used-by-the-dashboard get_receipts_page()
        # (real DB-backed filtering, see its own docstring) and
        # returns just its `items` -- offset/total pagination isn't
        # exposed here, this is deliberately just "give me my most
        # recent N verified/unverified receipts", not a new paging
        # API surface.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        if verified is None and limit is None:
            return jsonable_encoder(presentation.get_receipts(org_id=org_id))
        items, _total = presentation.get_receipts_page(
            verified=verified, org_id=org_id, limit=limit or 50, offset=0,
        )
        return jsonable_encoder(items)

    @app.get(
        "/receipts/{receipt_id}",
        tags=["Receipts"],
        summary="Get full evidence detail for a single receipt",
        responses={401: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def get_receipt(receipt_id: str, auth=Depends(require_scope("View monitoring"))):
        # Same org-check pattern as get_job: confirm this receipt_id
        # is actually in the caller's own org-scoped list BEFORE
        # fetching detail, and use the same 404 for "doesn't exist"
        # and "belongs to a different org" -- get_receipt_detail()
        # itself takes no org_id and does no scoping (it's the
        # internal staff dashboard's method, staff sees everything),
        # so this route enforces isolation itself rather than relying
        # on that method to.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        own_receipt_ids = {r["receipt_id"] for r in presentation.get_receipts(org_id=org_id)}
        if receipt_id not in own_receipt_ids:
            raise HTTPException(status_code=404, detail=f"Receipt '{receipt_id}' not found.")
        detail = presentation.get_receipt_detail(receipt_id)
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Receipt '{receipt_id}' not found.")
        return jsonable_encoder(detail)

    @app.get(
        "/artifacts",
        response_model=List[ArtifactOut],
        tags=["Artifacts"],
        summary="List all registered artifacts",
        responses={401: {"model": ErrorOut}},
    )
    def list_artifacts(auth=Depends(require_scope("View monitoring"))):
        # Only the caller's own organization's artifacts.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        return jsonable_encoder(presentation.get_artifacts(org_id=org_id, scoped=True))

    # ------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------

    @app.get(
        "/telemetry/events",
        tags=["Telemetry"],
        summary="List job-lifecycle telemetry events",
        responses={401: {"model": ErrorOut}},
    )
    def list_telemetry_events(
        job_id: Optional[str] = None,
        node_id: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 200,
        auth=Depends(require_scope("View monitoring")),
    ):
        # Same org-scoping as list_receipts/list_jobs above - a
        # telemetry_events row has no org_id column of its own, so
        # TelemetryRepository.query() joins through jobs.org_id itself
        # (see its docstring); this was simply never wired to a route
        # at all before, so presentation.get_telemetry_events()'s
        # already-correct org_id handling was unreachable from the API.
        owner = auth["owner"]
        org_id = getattr(owner, "organization_id", None) if owner else None
        return jsonable_encoder(presentation.get_telemetry_events(
            job_id=job_id, node_id=node_id, since=since, org_id=org_id, limit=limit,
        ))

    # ------------------------------------------------------------
    # Whoami
    # ------------------------------------------------------------

    @app.get(
        "/whoami",
        tags=["Auth"],
        summary="Identify the API key making this request",
        responses={401: {"model": ErrorOut}},
    )
    def whoami(auth=Depends(require_scope(tenant=False))):
        key = auth["key"]
        owner = auth["owner"]
        return {
            "key_id": key.key_id,
            "key_name": key.name,
            "scopes": key.scopes,
            "owner_user_id": owner.user_id if owner else None,
            "owner_name": owner.name if owner else None,
        }

    return app