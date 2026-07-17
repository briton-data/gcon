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

from typing import List, Optional

from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field


# ---------------------------------------------------------------
# Response / request schemas (also drive the OpenAPI docs)
# ---------------------------------------------------------------

class NodeOut(BaseModel):
    node_id: str
    status: str
    cpu: object = Field(description="CPU utilization percentage, or 'N/A'")
    memory: object = Field(description="Memory utilization percentage, or 'N/A'")
    running_jobs: int
    last_seen: object
    draining: bool


class JobOut(BaseModel):
    job_id: str
    status: str
    node_id: Optional[str] = None
    created_at: object = None
    completed_at: object = None
    receipt_id: Optional[str] = None
    artifacts: int = 0


class JobSubmitRequest(BaseModel):
    job_id: str = Field(..., description="Unique identifier for the job")
    command: str = Field(..., description="Shell command the job will run")
    artifacts: Optional[List[str]] = Field(
        default=None, description="Optional list of file paths to register as artifacts"
    )


class JobSubmitResponse(BaseModel):
    job_id: str
    submitted: bool = True


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


def create_api_v1_app(management, presentation):
    """
    Build the /api/v1 sub-application. `management` is the shared
    ManagementLayer instance (for API key auth) and `presentation`
    is the shared PresentationLayer (for real cluster data) — the
    same instances the dashboard itself uses, so the public API and
    the dashboard are always looking at the same live state.
    """

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

    def require_scope(scope: Optional[str] = None):
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

            return {"key": key, "owner": owner}

        return dependency

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
    def get_cluster(auth=Depends(require_scope("View monitoring"))):
        return jsonable_encoder(presentation.get_cluster_state())

    @app.get(
        "/health",
        response_model=HealthOut,
        tags=["Cluster"],
        summary="Get overall cluster health",
        responses={401: {"model": ErrorOut}},
    )
    def get_health(auth=Depends(require_scope("View monitoring"))):
        return jsonable_encoder(presentation.get_cluster_health())

    @app.get(
        "/metrics",
        tags=["Cluster"],
        summary="Get aggregate node and job metrics",
        responses={401: {"model": ErrorOut}},
    )
    def get_metrics(auth=Depends(require_scope("View monitoring"))):
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
        return jsonable_encoder(presentation.get_nodes())

    @app.get(
        "/nodes/{node_id}",
        response_model=NodeOut,
        tags=["Nodes"],
        summary="Get a single node by id",
        responses={401: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def get_node(node_id: str, auth=Depends(require_scope("View monitoring"))):
        for node in presentation.get_nodes():
            if node["node_id"] == node_id:
                return jsonable_encoder(node)
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found.")

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
    def list_jobs(auth=Depends(require_scope("View monitoring"))):
        return jsonable_encoder(presentation.get_jobs())

    @app.get(
        "/jobs/{job_id}",
        response_model=JobOut,
        tags=["Jobs"],
        summary="Get a single job by id",
        responses={401: {"model": ErrorOut}, 404: {"model": ErrorOut}},
    )
    def get_job(job_id: str, auth=Depends(require_scope("View monitoring"))):
        for job in presentation.get_jobs():
            if job["job_id"] == job_id:
                return jsonable_encoder(job)
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    @app.post(
        "/jobs",
        response_model=JobSubmitResponse,
        tags=["Jobs"],
        summary="Submit a new job",
        responses={401: {"model": ErrorOut}, 400: {"model": ErrorOut}},
    )
    def submit_job(payload: JobSubmitRequest, auth=Depends(require_scope("Submit workflows"))):
        try:
            presentation.submit_job(payload.job_id, payload.command, payload.artifacts)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"job_id": payload.job_id, "submitted": True}

    @app.post(
        "/jobs/{job_id}/cancel",
        response_model=JobCancelResponse,
        tags=["Jobs"],
        summary="Cancel a running job",
        responses={401: {"model": ErrorOut}, 400: {"model": ErrorOut}},
    )
    def cancel_job(job_id: str, auth=Depends(require_scope("Submit workflows"))):
        try:
            result = presentation.cancel_job(job_id)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return jsonable_encoder(result)

    # ------------------------------------------------------------
    # Workflows
    # ------------------------------------------------------------

    @app.get(
        "/workflows",
        tags=["Workflows"],
        summary="List all workflows",
        responses={401: {"model": ErrorOut}},
    )
    def list_workflows(auth=Depends(require_scope("View monitoring"))):
        return jsonable_encoder(presentation.get_workflows())

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
    def list_receipts(auth=Depends(require_scope("View monitoring"))):
        return jsonable_encoder(presentation.get_receipts())

    @app.get(
        "/artifacts",
        response_model=List[ArtifactOut],
        tags=["Artifacts"],
        summary="List all registered artifacts",
        responses={401: {"model": ErrorOut}},
    )
    def list_artifacts(auth=Depends(require_scope("View monitoring"))):
        return jsonable_encoder(presentation.get_artifacts())

    # ------------------------------------------------------------
    # Whoami
    # ------------------------------------------------------------

    @app.get(
        "/whoami",
        tags=["Auth"],
        summary="Identify the API key making this request",
        responses={401: {"model": ErrorOut}},
    )
    def whoami(auth=Depends(require_scope())):
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
