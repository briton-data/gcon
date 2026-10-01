"""
GCON Python SDK — client for the GCON Public API (/api/v1).

Example:

    from gcon_sdk import GconClient

    client = GconClient(api_key="gcon_...", base_url="http://localhost:8000")

    print(client.get_cluster())
    print(client.list_nodes())

    job = client.submit_job("job-42", "python train.py")
    print(client.get_job("job-42"))
"""

from typing import Any, Dict, List, Optional

import requests


class GconAPIError(Exception):
    """
    Raised for any non-2xx response from the GCON API. Carries the
    HTTP status code and the server's error detail message.
    """

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"GCON API error {status_code}: {detail}")


class GconClient:
    """
    Thin, synchronous client for the GCON Public API v1.

    Args:
        api_key: An API key created from the GCON dashboard's
            Management > API Keys panel.
        base_url: The root URL of the GCON server (no trailing
            slash), e.g. "http://localhost:8000" or
            "https://gcon.example.com".
        timeout: Per-request timeout in seconds.
    """

    def __init__(self, api_key: str, base_url: str = "http://localhost:8000",
                 timeout: float = 30.0):
        if not api_key:
            raise ValueError("api_key is required.")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    def _url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    def _request(self, method: str, path: str, **kwargs) -> Any:
        response = self._session.request(
            method, self._url(path), timeout=self.timeout, **kwargs
        )
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise GconAPIError(response.status_code, detail)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    # ------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------

    def whoami(self) -> Dict[str, Any]:
        """Identify the API key this client is authenticated as."""
        return self._request("GET", "/whoami")

    # ------------------------------------------------------------
    # Cluster
    # ------------------------------------------------------------

    def get_cluster(self) -> Dict[str, Any]:
        """Get the current cluster state."""
        return self._request("GET", "/cluster")

    def get_health(self) -> Dict[str, Any]:
        """Get overall cluster health."""
        return self._request("GET", "/health")

    def get_metrics(self) -> Dict[str, Any]:
        """Get aggregate node/job metrics."""
        return self._request("GET", "/metrics")

    # ------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------

    def list_nodes(self) -> List[Dict[str, Any]]:
        """List all registered nodes."""
        return self._request("GET", "/nodes")

    def get_node(self, node_id: str) -> Dict[str, Any]:
        """Get a single node by id."""
        return self._request("GET", f"/nodes/{node_id}")

    def get_node_enrollment_history(self, node_id: str) -> List[Dict[str, Any]]:
        """Durable audit trail of Enroll RPC attempts for this
        node_id -- source IPs and which enroll_token_id was used for
        each attempt."""
        return self._request("GET", f"/nodes/{node_id}/enrollment-history")

    # ------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------

    def list_jobs(self, status: Optional[str] = None,
                  limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """List jobs, newest first.

        `status`/`limit` are optional -- omit both for the original
        "every job" behavior. `status` filters to a single job status
        (e.g. "failed", "pending", "running", "completed"); `limit`
        caps how many are returned.
        """
        params: Dict[str, Any] = {}
        if status is not None:
            params["status"] = status
        if limit is not None:
            params["limit"] = limit
        return self._request("GET", "/jobs", params=params or None)

    def get_job(self, job_id: str) -> Dict[str, Any]:
        """Get a single job by id."""
        return self._request("GET", f"/jobs/{job_id}")

    def submit_job(self, job_id: str, command: str,
                    artifacts: Optional[List[str]] = None,
                    kind: Optional[str] = None,
                    requires: Optional[Dict[str, Any]] = None,
                    stages: Optional[Dict[str, Any]] = None,
                    dataset_artifacts: Optional[List[str]] = None,
                    callback_url: Optional[str] = None,
                    verify: Optional[Dict[str, Any]] = None,
                    idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        """
        Submit a new job to the cluster.

        `kind`/`requires`/`stages`/`dataset_artifacts`/`callback_url`/
        `verify` mirror the server's JobSubmitRequest exactly (see the
        GCON API reference) -- all optional, and omitted from the
        request body entirely when left as None so a plain command
        job's request looks exactly as it did before these existed:

            kind: "command" (default, plain/unstructured), "resourced"
                (adds `requires`, matched against node capabilities
                before dispatch), or "staged" (adds `stages`, a
                per-checkpoint progress contract the job's own code
                reports against).
            requires: 'resourced' jobs only, e.g.
                {"gpu": true, "min_vram_gb": 12, "min_cpu_cores": 4}.
            stages: 'staged' jobs only, e.g. {"expected": 15}.
            dataset_artifacts: IDs of artifacts already registered
                with this coordinator (not filepaths) this job
                declares as input data.
            callback_url: if set, GCON POSTs a signed payload here on
                job completion instead of requiring you to poll.
            verify: dispatch to N independently-selected nodes and
                compare their results, e.g.
                {"replicas": 2, "tolerance": 0.02}. Orthogonal to
                kind/requires/stages -- a resourced or staged job can
                also ask for replication.
            idempotency_key: if you're retrying a submission whose
                original outcome is unknown (e.g. the request timed
                out, or your process died before it saw the
                response), pass the SAME key you used the first time
                and you'll get back that original job (even if
                `job_id` here is a new one you generated for this
                retry attempt) instead of risking a second job being
                created. Durable server-side, scoped to your
                organization -- a fresh key always creates a new job,
                same as leaving this unset.

        Raises GconAPIError with status_code=400 if the submission is
        rejected by server-side policy (e.g. a `requires`/`verify`
        value over a configured ceiling) -- same clean error shape as
        any other rejected request, not a raw server error.
        """
        payload: Dict[str, Any] = {"job_id": job_id, "command": command, "artifacts": artifacts}
        if kind is not None:
            payload["kind"] = kind
        if requires is not None:
            payload["requires"] = requires
        if stages is not None:
            payload["stages"] = stages
        if dataset_artifacts is not None:
            payload["dataset_artifacts"] = dataset_artifacts
        if callback_url is not None:
            payload["callback_url"] = callback_url
        if verify is not None:
            payload["verify"] = verify
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        return self._request("POST", "/jobs", json=payload, headers=headers)

    def get_job_attempts(self, job_id: str) -> List[Dict[str, Any]]:
        """Durable dispatch-attempt history for a job -- attempt
        number, node, status, dispatched/completed timestamps, most
        recent first."""
        return self._request("GET", f"/jobs/{job_id}/attempts")

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """Cancel a running job."""
        return self._request("POST", f"/jobs/{job_id}/cancel")

    def retry_job(self, job_id: str) -> Dict[str, Any]:
        """Retry a failed or stuck-pending job. Raises GconAPIError
        (status_code=400) if the job isn't in a retryable state, or
        404 if it doesn't exist (or isn't yours)."""
        return self._request("POST", f"/jobs/{job_id}/retry")

    def clear_jobs(self, job_ids: List[str]) -> Dict[str, Any]:
        """Permanently clear failed or cancelled jobs (at most 200
        per call, and only jobs that are failed/cancelled with no
        receipt -- receipts are never deleted). Returns
        {"cleared": [...], "skipped": [{"job_id", "reason",
        "message"}, ...]} -- a job that can't be cleared is reported
        in `skipped` with why, not raised as an error, so one
        unclearable id in a batch doesn't fail the whole call."""
        return self._request("POST", "/jobs/clear", json={"job_ids": job_ids})

    # ------------------------------------------------------------
    # Workflows
    # ------------------------------------------------------------

    def list_workflows(self) -> List[Dict[str, Any]]:
        """List all workflows."""
        return self._request("GET", "/workflows")

    def submit_workflow(self, workflow_id: str, jobs: List[Dict[str, Any]],
                         name: str = "") -> Dict[str, Any]:
        """
        Submit a DAG of jobs as one workflow.

        `jobs` is a list of dicts, each shaped like:
            {"job_id": "...", "command": "...", "depends_on": ["..."]}
        `depends_on` is optional per job (defaults to no dependencies
        server-side) -- a job with no `depends_on` runs as soon as the
        coordinator has capacity; one that names other jobs in this
        same submission waits for all of them to complete first.

        Example:
            client.submit_workflow("wf-1", jobs=[
                {"job_id": "fetch", "command": "python fetch.py"},
                {"job_id": "train", "command": "python train.py",
                 "depends_on": ["fetch"]},
            ])
        """
        payload = {"workflow_id": workflow_id, "name": name, "jobs": jobs}
        return self._request("POST", "/workflows", json=payload)

    # ------------------------------------------------------------
    # Receipts & artifacts
    # ------------------------------------------------------------

    def list_receipts(self, verified: Optional[bool] = None,
                       limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """List job receipts. `verified`/`limit` are optional -- omit
        both for the original "every receipt" behavior. `verified`
        filters to only genuinely HMAC-valid (or only invalid)
        receipts; `limit` caps how many are returned (most recent
        first)."""
        params: Dict[str, Any] = {}
        if verified is not None:
            params["verified"] = verified
        if limit is not None:
            params["limit"] = limit
        return self._request("GET", "/receipts", params=params or None)

    def get_receipt(self, receipt_id: str) -> Dict[str, Any]:
        """Full evidence detail for a single receipt: the signed
        proof, worker attestation (if any), policy report, execution
        proof + replica agreement for a replicated job (if any), and
        the reconciled `assurance` decision combining all of them --
        see the GCON API reference's Verification section for the
        assurance level precedence."""
        return self._request("GET", f"/receipts/{receipt_id}")

    def list_artifacts(self) -> List[Dict[str, Any]]:
        """List all registered artifacts."""
        return self._request("GET", "/artifacts")

    # ------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------

    def get_telemetry_events(self, job_id: Optional[str] = None,
                              node_id: Optional[str] = None,
                              since: Optional[str] = None,
                              limit: int = 200) -> List[Dict[str, Any]]:
        """Job-lifecycle telemetry events, most recent first, scoped
        to your own org. `job_id`/`node_id` narrow to one job/node;
        `since` is an ISO-8601 timestamp filtering to events at or
        after it; `limit` caps how many are returned (default 200,
        matching the server's own default)."""
        params: Dict[str, Any] = {"limit": limit}
        if job_id is not None:
            params["job_id"] = job_id
        if node_id is not None:
            params["node_id"] = node_id
        if since is not None:
            params["since"] = since
        return self._request("GET", "/telemetry/events", params=params)

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
