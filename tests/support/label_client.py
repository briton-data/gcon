"""
TestClient that remembers the canonical job/workflow ids GCON mints.

POST /jobs no longer lets the caller choose the job id: the caller sends a
`client_reference` (its own label) and gets the canonical `job_id` back. Tests
that think in labels ("stuck-1") read the real id from `client.ids[label]`.
"""
from fastapi.testclient import TestClient


class LabelClient(TestClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ids = {}

    def request(self, method, url, *args, **kwargs):
        response = super().request(method, url, *args, **kwargs)
        try:
            if method.upper() == "POST" and str(url).split("?")[0] in ("/jobs", "/workflows") \
                    and response.status_code == 200:
                body = kwargs.get("json") or {}
                data = response.json()
                if url == "/jobs":
                    label = body.get("client_reference") or body.get("job_id")
                    if label:
                        self.ids[label] = data["job_id"]
                else:
                    for label, canonical in (data.get("jobs") or {}).items():
                        self.ids[label] = canonical
                    if body.get("workflow_id"):
                        self.ids[body["workflow_id"]] = data["workflow_id"]
        except Exception:
            pass
        return response
