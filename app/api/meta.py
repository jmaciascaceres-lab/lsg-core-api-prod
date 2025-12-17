import os
from fastapi import APIRouter, Depends

from app.db import DB_HOST, DB_NAME
from app.security import require_roles, guard_player_access, ROLE_ALL

router = APIRouter(tags=["meta"])


@router.get("/info", dependencies=[Depends(require_roles(ROLE_ALL))])
def get_meta_info():
    """
    Metadatos de la API y del entorno.
    Pensado para debugging / monitoreo.

    Acceso: abierto a todos.
    """
    api_version = os.getenv("API_VERSION", "1.0.0")
    environment = os.getenv("APP_ENV", "production")
    git_commit = os.getenv("GIT_COMMIT", "0174c59")

    return {
        "api_version": api_version,
        "environment": environment,
        "git_commit": git_commit,
        "database": {
            "host": DB_HOST,
            "name": DB_NAME,
        },
    }
