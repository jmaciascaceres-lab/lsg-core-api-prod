import os
from fastapi import FastAPI

from app.api import health, analytics, players, points, games, sensors, meta, admin_config, admin_points, research_export

ROOT_PATH = os.getenv("LSG_CORE_API_ROOT_PATH", "")

CORE_DOCS_DESCRIPTION = """
## Flujo de uso (Token → Core API)

1. **Obtén un token en LSG-auth**:
   - Swagger Auth: `/lsg-auth/docs`
   - `POST /login` → copia `access_token`

2. **Autoriza en este Swagger**:
   - Botón **Authorize**
   - Pega: `Bearer <access_token>`

3. **Usa los endpoints según tu necesidad**.

"""

app = FastAPI(
    title="LifeSync-Games Core API",
    version="1.0.1",
    root_path=ROOT_PATH,
    description=CORE_DOCS_DESCRIPTION,
)


# Routers
app.include_router(health.router)

app.include_router(players.router, prefix="/players", tags=["players"])
app.include_router(points.router)  # << sin tags aquí
app.include_router(games.router, prefix="/videogames", tags=["videogames"])
app.include_router(sensors.router, prefix="/sensors", tags=["sensors"])

app.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
app.include_router(meta.router, prefix="/meta", tags=["meta"])
app.include_router(admin_config.router)  # ya tiene prefix="/admin"
app.include_router(admin_points.router)  # ya tiene prefix="/admin"
app.include_router(research_export.router)  # ya tiene prefix="/admin"
