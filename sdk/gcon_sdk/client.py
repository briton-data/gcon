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

    # ------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------

    def list_jobs(self) -> List[Dict[str, Any]]:
        """List all jobs."""
        return self._request("GET", "/jobs")

    def get_job(self, job_id: str) -> Dict[str, Any]:
        """Get a single job by id."""
        return self._request("GET", f"/jobs/{job_id}")

    def submit_job(self, job_id: str, command: str,
                    artifacts: Optional[List[str]] = None) -> Dict[str, Any]:
        """Submit a new job to the cluster."""
        payload = {"job_id": job_id, "command": command, "artifacts": artifacts}
        return self._request("POST", "/jobs", json=payload)

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """Cancel a running job."""
        return self._request("POST", f"/jobs/{job_id}/cancel")

    # ------------------------------------------------------------
    # Workflows
    # ------------------------------------------------------------

    def list_workflows(self) -> List[Dict[str, Any]]:
        """List all workflows."""
        return self._request("GET", "/workflows")

    # ------------------------------------------------------------
    # Receipts & artifacts
    # ------------------------------------------------------------

    def list_receipts(self) -> List[Dict[str, Any]]:
        """List all job receipts."""
        return self._request("GET", "/receipts")

    def list_artifacts(self) -> List[Dict[str, Any]]:
        """List all registered artifacts."""
        return self._request("GET", "/artifacts")

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
