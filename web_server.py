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
    
    

    