from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, Response
from fastapi.encoders import jsonable_encoder
from fastapi.templating import Jinja2Templates
from fastapi import Request
from management_layer import ManagementLayer
from fastapi import Cookie, HTTPException, Depends
from fastapi.responses import RedirectResponse
from auth import SESSION_COOKIE_NAME
import asyncio
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
        self.management = ManagementLayer(coordinator=presentation.coordinator)
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
        def home(
            request: Request,
            gcon_session: str = Cookie(default=None),
):
            user = self.management.get_current_user(gcon_session)

            if not user:
                return RedirectResponse(url="/login")

            return self.templates.TemplateResponse(
                request=request,
                name="dashboard.html",
                context={
                    "dashboard": self.presentation.get_dashboard(),
                     "current_user": user.to_dict(),
        },
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
        def events(user=Depends(self.require_permission("Manage users"))):
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

        @self.app.get("/health")
        def cluster_health():
            return self.presentation.get_cluster_health()

        # ---- Analytics & History ----

        @self.app.get("/analytics")
        def analytics():
            return self.presentation.get_analytics()

        # ---- Administration ----

        @self.app.get("/admin/config")
        def admin_config():
            return self.presentation.get_admin_config()

        @self.app.post("/admin/scale-up")
        def admin_scale_up(
            user=Depends(self.require_permission("Manage cluster"))):
            return self.presentation.scale_up()

        @self.app.post("/admin/scale-down")
        def admin_scale_down( 
            user=Depends(self.require_permission("Manage cluster"))):
            return self.presentation.scale_down()

        @self.app.post("/admin/nodes/{node_id}/deregister")
        def admin_deregister_node(
            node_id: str,  user=Depends (self.require_permission("Manage cluster"))):
            self.presentation.deregister_node(node_id)
            return {"success": True,
                "node_id": node_id,
}
            

        # ---- Live push (WebSocket) ----

        @self.app.websocket("/ws")
        async def ws_live(websocket: WebSocket):
            session_token = websocket.cookies.get(
            SESSION_COOKIE_NAME
)
            if not self.management.get_current_user(session_token):
                await websocket.close(code=4401)
                return 
            
            await websocket.accept()
            try:
                while True:
                    payload = {
                        "cluster": self.presentation.get_cluster_state(),
                        "nodes": self.presentation.get_nodes(),
                        "jobs": self.presentation.get_jobs(),
                        "events": self.presentation.get_events(),
                        "health": self.presentation.get_cluster_health(),
                        "system_metrics": self.presentation.get_system_metrics(),
                    }
                    await websocket.send_json(jsonable_encoder(payload))
                    await asyncio.sleep(2)
            except WebSocketDisconnect:
                pass

        # ---- Management: Users ----

        @self.app.get("/management/users")
        def mgmt_users():
            return self.management.get_users()

        @self.app.get("/management/users/{user_id}")
        def mgmt_get_user(user_id: str,):
            return self.management.get_user(user_id)

        @self.app.post("/management/users")
        def mgmt_create_user(payload:
            dict,user=Depends(self.require_permission("Manage users")), ):
            # NEW
            try:
                return self.management.create_user(
                name=payload["name"],
                email=payload["email"],
                role=payload["role"],
                organization_id=payload.get("organization_id"),
                status=payload.get("status", "Active"),
                password=payload.get("password"),
    )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            

        @self.app.put("/management/users/{user_id}")
        def mgmt_update_user(
            user_id: str, payload: dict, user=Depends(self.require_permission("Manage users")),):
            return self.management.get_user()

        @self.app.delete("/management/users/{user_id}")
        def mgmt_delete_user(
            user_id: str,
            user=Depends(self.require_permission("Manage users")),):
            self.management.delete_user(user_id)
            return {"deleted": user_id}

        @self.app.post("/management/users/{user_id}/status")
        def mgmt_set_user_status(
            user_id: str, payload: dict,user=Depends(self.require_permission("Manage post")),
):
            return self.management.get_user()

        @self.app.get("/management/user-counts")
        def mgmt_user_counts():
            return self.management.get_user_counts()

        # ---- Management: Organizations & Teams ----

        @self.app.get("/management/organizations")
        def mgmt_organizations():
            return self.management.get_organizations()

        @self.app.get("/management/teams")
        def mgmt_teams():
            return self.management.get_teams()

        # ---- Management: RBAC ----

        @self.app.get("/management/roles")
        def mgmt_roles(  user=Depends(self.require_permission("Manage roles"))):
            return self.management.get_roles()

        @self.app.get("/management/permissions")
        def mgmt_permissions(
            user=Depends(self.require_permission("Manage permission"))):
            return self.management.get_permissions()

        @self.app.get("/management/permission-matrix")
        def mgmt_permission_matrix():
            return self.management.get_permission_matrix()

        # ---- Management: API Keys ----

        @self.app.get("/management/api-keys")
        def mgmt_api_keys(  
            user=Depends(self.require_permission("Manage api_keys"))):
            return self.management.get_api_keys()

        @self.app.post("/management/api-keys")
        def mgmt_create_api_key(
            payload: dict,
            user=Depends(self.require_permission("Manage API keys")),):
            return self.management.create_api_key(
                name=payload["name"],
                owner_user_id=payload["owner_user_id"],
                scopes=payload.get("scopes"),
                expires_in_days=payload.get("expires_in_days", 90),
            )

        @self.app.post("/management/api-keys/{key_id}/revoke")
        def mgmt_revoke_api_key(key_id: str):
            return self.management.revoke_api_key(key_id)

        @self.app.post("/management/api-keys/{key_id}/regenerate")
        def mgmt_regenerate_api_key(key_id: str):
            return self.management.regenerate_api_key(key_id)

        # ---- Management: Audit log & notifications ----

        @self.app.get("/management/audit-logs")
        def mgmt_audit_logs(
            user=Depends(self.require_permission("Manage audit_logs"))):
            return self.management.get_audit_logs()

        @self.app.get("/management/notifications")
        def mgmt_notifications():
            return self.management.get_notifications()

        @self.app.post("/management/notifications/{notification_id}/read")
        def mgmt_mark_notification_read(notification_id: str):
            return self.management.mark_notification_read(notification_id)

        # ---- Management: dashboard cards & search ----

        @self.app.get("/management/dashboard-cards")
        def mgmt_dashboard_cards(
            user=Depends(self.require_permission("Manage dashboard_cards"))):
            return self.management.get_dashboard_cards()

        @self.app.get("/management/search")
        def mgmt_search(q: str = ""):
            return self.management.search(q)

        # ---- Management: export ----

        @self.app.get("/management/export/{entity}")
        def mgmt_export(entity: str, format: str = "json"):
            content, mime, filename = self.management.export(entity, format)
            return Response(
                content=content,
                media_type=mime,
                headers={"Content-Disposition": f"attachment; filename={filename}"},
            )
        @self.app.get("/login", response_class=HTMLResponse)
        def login_page(request: Request):
            return self.templates.TemplateResponse(
                request=request,
                name="login.html",
                context={}
    )


        @self.app.post("/auth/login")
        def auth_login(payload: dict, response: Response):
            try:
                token, user = self.management.login(
                    payload["email"],
                    payload["password"],
        )
            except ValueError as e:
                raise HTTPException(status_code=401, detail=str(e))

            response.set_cookie(
                key=SESSION_COOKIE_NAME,
                value=token,
                httponly=True,
                samesite="lax",
                max_age=60 * 60 * 24,
    )

            return user


        @self.app.post("/auth/logout")
        def auth_logout(
            response: Response,
            gcon_session: str = Cookie(default=None),
):
            self.management.logout(gcon_session)
            response.delete_cookie(SESSION_COOKIE_NAME)
            return {"logged_out": True}


        @self.app.get("/auth/me")
        def auth_me(user=Depends(self.current_user)):
            return user.to_dict()


        @self.app.post("/auth/change-password")
        def auth_change_password(
            payload: dict,
            user=Depends(self.current_user),
):
            try:
                self.management.change_password(
                    user.user_id,
                    payload["current_password"],
                    payload["new_password"],
        )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

            return {"changed": True}    
            
            
            
    def current_user(self, gcon_session: str = Cookie(default=None)):
        user = self.management.get_current_user(gcon_session)
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        return user


    def require_permission(self,permission):
        def dependency(
            gcon_session: str = Cookie(default=None),
    ):
            user = self.current_user(gcon_session)

            if not self.management.user_has_permission(
                user,
                permission,
        ):
                raise HTTPException(
                    status_code=403,
                    detail=f"'{permission}' permission is required.",
            )
            return user
        return dependency

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
    
    

    