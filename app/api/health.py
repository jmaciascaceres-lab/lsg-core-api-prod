from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.security import (
    require_roles,
    guard_player_access,
    CurrentUser,
    ROLE_ALL,
)   
from app.db import get_db

router = APIRouter(tags=["health"])


@router.get("/health", dependencies=[Depends(require_roles(ROLE_ALL))])
def health_check():
    """
    Liveness básico: solo indica que la app está levantada.

    Acceso: abierto a todos.
    """
    return {"status": "ok"}


@router.get("/health/full", dependencies=[Depends(require_roles(ROLE_ALL))])
def health_full(db: Session = Depends(get_db)):
    """
    Readiness / health extendido:
    - Chequea conexión a la base de datos.
    - Verifica acceso a vistas críticas.

    Acceso: todos.
    """
    checks = {}

    # 1) Conexión a DB
    try:
        db.execute(text("SELECT 1"))
        checks["database"] = {"status": "ok"}
    except Exception as e:
        checks["database"] = {"status": "error", "detail": str(e)}

    # 2) Vistas críticas para LSG
    views = [
        "v_points_balance",
        "v_player_game_overview",
        "v_player_attribute_balance",
    ]
    view_results = []

    for view in views:
        try:
            # Si la vista existe, esto debería funcionar aunque esté vacía
            db.execute(text(f"SELECT 1 FROM {view} LIMIT 1"))
            view_results.append({"name": view, "status": "ok"})
        except Exception as e:
            view_results.append(
                {"name": view, "status": "error", "detail": str(e)}
            )

    checks["views"] = view_results

    # 3) Estado global
    if checks["database"]["status"] != "ok":
        status = "error"
    elif any(v["status"] != "ok" for v in view_results):
        status = "degraded"
    else:
        status = "ok"

    return {
        "status": status,
        "checks": checks,
    }
