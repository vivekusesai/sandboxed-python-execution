"""API v1 router - aggregates all endpoint routers."""

from fastapi import APIRouter

from app.api.v1.auth import router as auth_router
from app.api.v1.jobs import router as jobs_router
from app.api.v1.scripts import router as scripts_router
from app.api.v1.tables import router as tables_router

api_router = APIRouter()

# Include all routers
api_router.include_router(auth_router)
api_router.include_router(tables_router)
api_router.include_router(scripts_router)
api_router.include_router(jobs_router)


@api_router.get("/")
async def api_info():
    """API information endpoint."""
    return {
        "name": "Data Transformation Platform API",
        "version": "1.0.0",
        "endpoints": {
            "auth": "/api/v1/auth",
            "tables": "/api/v1/tables",
            "scripts": "/api/v1/scripts",
            "jobs": "/api/v1/jobs",
        },
    }
