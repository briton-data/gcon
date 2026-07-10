import asyncio
import json


from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
import uvicorn

"""
GCON Web Server

Hosts the GCON Web Dashboard and exposes the Presentation Layer
to external clients.
"""

def _sse_frame(event_type: str, payload) -> str:
    """
    Format a single Server-Sent Events frame.
    """
    return f"event: {event_type}\ndata: {json.dumps(payload, default=str)}\n\n"


class WebServer:
    """
    Web server for the GCON dashboard.
    """

    def __init__(self, presentation):
        """
        Initialize the web server.

        Args:
        presentation: Active PresentationLayer instance.
        """
        self.presentation = presentation
        self.app = FastAPI(title="GCON Dashboard")
        
        self.templates = Jinja2Templates(directory="templates")
        self.app.mount(
            "/static",
            StaticFiles(directory="static"),
            name="static"
        )
        self._register_routes()

    
    def _register_routes(self):
        """
        Register all HTTP routes.
        """

        @self.app.get("/", response_class=HTMLResponse)
        def home(request: Request):

            return self.templates.TemplateResponse(
                request=request,
                name="dashboard.html",
                context={"dashboard": self.presentation.get_dashboard()}
            )
        @self.app.get("/cluster")
        def cluster():
            return self.presentation.get_cluster_state()    
            
        @self.app.get("/nodes")
        def nodes():
            return self.presentation.get_nodes()
        
        @self.app.get("/jobs")
        def jobs():
            return self.presentation.get_jobs()

        @self.app.get("/events")
        def events():
            return self.presentation.get_events()
        
                # ---- Live updates (Dashboard 2.0) ----

        @self.app.get("/stream")
        async def stream(request: Request):
            """
            Server-Sent Events stream. Pushes a "snapshot" frame
            (cluster state) whenever a real cluster event happens, plus
            a heartbeat snapshot every ~2s so the UI stays fresh even
            during quiet periods. This replaces polling /cluster and
            /events every 5s with a single long-lived connection.
            """
            queue: asyncio.Queue = asyncio.Queue()
            loop = asyncio.get_event_loop()
            event_bus = self.presentation.coordinator.event_bus

            def on_event(event):
                # Called synchronously from the coordinator's
                # background threads -- hop back onto the asyncio loop
                # thread-safely rather than touching the queue directly.
                try:
                    loop.call_soon_threadsafe(queue.put_nowait, event.to_dict())
                except RuntimeError:
                    pass  # event loop already shutting down

            event_bus.subscribe(on_event)

            async def generator():
                try:
                    yield _sse_frame(
                        "snapshot",
                        self.presentation.get_cluster_state()
                    )

                    while True:
                        if await request.is_disconnected():
                            break

                        try:
                            raw_event = await asyncio.wait_for(
                                queue.get(),
                                timeout=2.0
                            )

                            yield _sse_frame("event", raw_event)
                            yield _sse_frame(
                                "snapshot",
                                self.presentation.get_cluster_state()
                            )

                        except asyncio.TimeoutError:
                            yield _sse_frame(
                                "heartbeat",
                                self.presentation.get_cluster_state()
                            )

                finally:
                    event_bus.unsubscribe(on_event)

            return StreamingResponse(
                generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        # ---- Cluster Visualization ----

        @self.app.get("/topology")
        def topology():
            return self.presentation.get_topology()
        
        @self.app.get("/workflows")
        def workflows():
            return self.presentation.get_workflows()

        # ---- Explorer views ----

        @self.app.get("/receipts")
        def receipts():
            return self.presentation.get_receipts()

        @self.app.get("/artifacts")
        def artifacts():
            return self.presentation.get_artifacts()

        # ---- Real-Time Monitoring ----

        @self.app.get("/system-metrics")
        def system_metrics():
            return self.presentation.get_system_metrics()

        # ---- Analytics & History ----

        @self.app.get("/analytics")
        def analytics():
            return self.presentation.get_analytics()

        # ---- Administration ----

        @self.app.get("/admin/config")
        def admin_config():
            return self.presentation.get_admin_config()

        @self.app.post("/admin/scale-up")
        def admin_scale_up():
            return self.presentation.scale_up()

        @self.app.post("/admin/scale-down")
        def admin_scale_down():
            return self.presentation.scale_down()

        @self.app.post("/admin/nodes/{node_id}/deregister")
        def admin_deregister_node(node_id: str):
            self.presentation.deregister_node(node_id)
            return self.presentation.get_admin_config()
       
            
  
    def start(self):
        """
        Start the web server.
        """
        uvicorn.run(
            self.app,
            host="127.0.0.1",
            port=8000
        )

    def stop(self):
        """
        Stop the web server.
        """
        pass
    
    

    