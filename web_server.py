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

        # ---- Cluster Visualization ----

        @self.app.get("/topology")
        def topology():
            return self.presentation.get_topology()

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
    
    

    