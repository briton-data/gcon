"""
RemoteNodeProxy — makes a node connected over GrpcTransport look like
a local GCONNode/GCONAgent to the (untouched) scheduler and
NodeRegistry. NodeRegistry.register() only needs `.node_id` and
`.status`; the scheduler's dispatch path needs `.execute_job(job_id,
command, timeout=)` and `.cancel()`. This class supplies all four by
delegating to the transport over the network.
"""

class RemoteNodeProxy:
    def __init__(self, node_id, transport):
        self.node_id = node_id
        self.transport = transport
        self.status = "idle"

    def execute_job(self, job_id, command, timeout=None):
        self.status = "busy"
        try:
            response = self.transport.send_job(self.node_id, job_id, command, timeout=timeout)
            return response["result"]
        finally:
            self.status = "idle"

    def cancel(self):
        return self.transport.cancel_job(self.node_id, "")