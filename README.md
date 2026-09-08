# LSG Core API (LifeSync-Games)

Servicio FastAPI que expone la lógica de dominio de LifeSync-Games sobre la base de datos `db_lsg` (MySQL 8).

**Versión:** 1.2.5 | **Swagger:** https://lsg.diinf.usach.cl/lsg-core-api/docs  
**Requiere token de:** https://lsg.diinf.usach.cl/lsg-auth/docs

---

## Estructura del proyecto

```text
lsg-core-api-prod/
│
├── app/
│   ├── api/
│   │   ├── health.py          # GET /health, /health/full  ← sin token en ambos
│   │   ├── meta.py            # GET /meta/info
│   │   ├── players.py         # /players/... (lista con roles, detalle, timeline)
│   │   ├── points.py          # /attributes, /points/ledger, balances, ajustes
│   │   ├── games.py           # /videogames/... (canjes, sesiones, bulk mecánicas)
│   │   ├── sensors.py         # /sensors/... (gestión + ingest con ownership check)
│   │   ├── analytics.py       # /analytics/... (balances, overview, calidad, IC²)
│   │   ├── admin_config.py    # /admin/... (atributos, dimensiones, mecánicas)
│   │   ├── admin_points.py    # /admin/points/consistency-check
│   │   ├── research_export.py # /research/export/... (CSV/JSON seudonimizado + IC²)
│   │   ├── ic2.py             # /ic2/... (Índice Compuesto IC² LSG)       ← v1.2
│   │   └── offline.py         # /offline/... (puntos offline Starbound/BG3) ← v1.2
│   │
│   ├── security.py            # JWT decode, require_roles, guard_player_access
│   ├── db.py                  # Conexión SQLAlchemy a MySQL
│   └── main.py                # Instancia FastAPI + registro de routers
│
├── db/
│   └── init/
│       ├── 01_db_lsg_dump.sql   # Schema base + datos semilla
│       └── 02_migrations.sql    # PATCH-01→09: player_roles, IC², offline, temp users
│
├── .env.example
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## Despliegue en producción (DIINF-USACH)

### 1. Prerequisitos

```bash
# Red compartida con lsg-auth (crear solo una vez por VM)
docker network create lsg_shared
```

### 2. Variables de entorno

```bash
cp .env.example .env
# Editar .env con los valores reales del entorno DIINF
```

| Variable | Descripción |
|----------|-------------|
| `MYSQL_ROOT_PASSWORD` | Contraseña root MySQL (solo init) |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` | Credenciales BD |
| `API_PORT` | Puerto host para Nginx (default: `8012`) |
| `AUTH_JWT_SECRET` | **Mismo valor** que en lsg-auth |
| `AUTH_JWT_ALGORITHM` | `HS256` |
| `AUTH_JWT_ISSUER` / `AUTH_JWT_AUDIENCE` | `lsg-auth` / `lsg-core-api` |
| `AUTH_OPEN_ALL` | `false` en producción |
| `AUTH_DISABLED` | `false` en producción |
| `LSG_CORE_API_ROOT_PATH` | `/lsg-core-api` |
| `RESEARCH_PSEUDONYM_SALT` | Salt seudonimización (hex 64 chars) |
| `OFFLINE_SYNC_MAX_AGE_DAYS` | Ventana offline máxima (default: `30`) |

### 3. Levantar el stack

```bash
docker compose up -d --build
docker ps
docker logs -n 100 lsg_core_api
```

### 4. Inicialización de BD

```bash
# En entorno fresco (primer levantamiento):
# docker-entrypoint-initdb.d ejecuta automáticamente los archivos en db/init/

# En BD existente, aplicar parches manualmente:
docker compose exec db mysql -u${DB_USER} -p${DB_PASSWORD} ${DB_NAME} \
  < db/init/02_migrations.sql
```

### 5. Verificar

```bash
curl https://lsg.diinf.usach.cl/lsg-core-api/health/full
# → {"status": "ok", "checks": {"database": ..., "views": [...], "feature_tables": [...]}}
```

---

## Flujo de autenticación

```
1. Obtener token → lsg-auth
   POST https://lsg.diinf.usach.cl/lsg-auth/login
   Body: username=email&password=...
   → {"access_token": "eyJ...", "token_type": "bearer"}
   Vigencia: 120 minutos. Renovar sin re-login: POST /lsg-auth/token/refresh

2. Autorizar en Swagger
   Botón "Authorize" → Bearer <access_token>
```

---

## Sistema de roles

| Rol | Descripción |
|-----|-------------|
| `player` | Sus propios datos: balance, sesiones, canjes, IC², sensores |
| `teacher` | Lectura de todos los jugadores y analíticas |
| `researcher` | Todo lo de teacher + ajuste de puntos, exportación, IC² ajeno |
| `developer` | Integración de mods: crear juegos, mecánicas y vincularlas |
| `admin` | Acceso completo incluyendo configuración del sistema |

---

## Endpoints

### Core

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `GET` | `/health` | - | Liveness (sin token requerido) |
| `GET` | `/health/full` | - | Readiness: BD + vistas + tablas IC²/offline (sin token requerido) |
| `GET` | `/meta/info` | todos | Versión, entorno, BD |

### Jugadores

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `GET` | `/players` | teacher, researcher, developer, admin | Lista paginada con `roles: []` |
| `GET` | `/players/{id}` | todos | Detalle + `roles: []` activos |
| `DELETE` | `/players/{id}` | admin | Borrado en cascada |
| `POST` | `/players/{id}/attributes/init` | admin, teacher, researcher | Inicializar atributos |
| `GET` | `/players/{id}/games` | todos | Videojuegos del jugador |
| `GET` | `/players/{id}/timeline` | todos | Timeline unificado de eventos |

### Puntos y atributos

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `GET` | `/attributes` | todos | Catálogo de atributos |
| `GET` | `/attributes/{id}/subattributes` | todos | Subatributos del atributo |
| `GET` | `/attributes-map` | todos | Mapa JSON completo atributos↔subatributos |
| `GET` | `/players/{id}/points/balance` | todos | Balance por dimensión |
| `GET` | `/players/{id}/attributes/points` | todos | Balance por atributo (`balance_ledger` y `snapshot_points`) |
| `GET` | `/points/ledger` | todos* | Historial de movimientos |
| `POST` | `/players/{id}/points/adjust` | admin, researcher | Ajuste manual de puntos |

*`player` solo ve su propio ledger.

### Videojuegos

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `GET` | `/videogames` | todos | Catálogo de juegos |
| `POST` | `/videogames` | admin, researcher, developer | Crear videojuego |
| `GET` | `/videogames/{id}/mechanics` | todos | Mecánicas del juego (campo `options` como JSON) |
| `POST` | `/videogames/{id}/mechanics` | admin, researcher, developer | Agregar mecánica unitaria |
| `POST` | `/videogames/{id}/mechanics/bulk` | admin, researcher, developer | Carga masiva hasta 500 mecánicas (HTTP 207) |
| `POST` | `/videogames/{id}/players/{pid}/connect` | todos | Vincular jugador↔juego (upsert) |
| `POST` | `/videogames/{id}/players/{pid}/sessions` | todos | Iniciar sesión de juego |
| `PATCH` | `/videogames/{id}/players/{pid}/sessions/{sid}/end` | todos | Cerrar sesión + calcular `duration_seconds` |
| `POST` | `/videogames/{id}/players/{pid}/redeem/preview` | todos | Preview de canje sin modificar BD |
| `POST` | `/videogames/{id}/players/{pid}/redeem` | todos | Canje de puntos (registra en `interaction_logs`) |

### Sensores

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `GET` | `/sensors` | todos | Catálogo de proveedores de sensores |
| `POST` | `/sensors` | admin, researcher | Crear nuevo proveedor de sensor |
| `GET` | `/sensors/{id}/endpoints` | todos | Endpoints del sensor - `token_parameters`, `specific_parameters`, `watch_parameters` como JSON |
| `POST` | `/sensors/{id}/endpoints` | admin, researcher | Agregar endpoint a un sensor |
| `GET` | `/sensors/players/{id}` | todos* | Sensores y endpoints activos del jugador - `tokens` como JSON |
| `POST` | `/sensors/players/{id}/link` | admin, researcher | Vincular sensor a jugador |
| `POST` | `/sensors/players/{id}/link-endpoint` | admin, researcher | Activar endpoint para jugador (`schedule_time` en formato HHMM entero: 800=08:00) |
| `POST` | `/sensors/ingest/webhook` | todos** | Ingestar evento de sensor |
| `GET` | `/sensors/players/{id}/ingest-events` | todos* | Historial de ingestas - `raw_payload` como JSON |

*player solo sus propios datos. **player solo su propio `player_id`.

### IC² LSG ← v1.2

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `POST` | `/ic2/compute` | todos* | Calcula IC² (F1-F4, Ec.5-7, IAR). `admissibility` como JSON |
| `GET` | `/ic2/history` | todos* | Historial - `admissibility` como JSON |
| `GET` | `/ic2/goalposts` | todos | Goalposts - `goalposts` como JSON (no string) |

*player solo su propio `player_id`.

### Puntos offline ← v1.2

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `POST` | `/offline/sync` | todos* | Sincroniza lote offline. HTTP 207. Idempotente por `client_ref` |
| `GET` | `/offline/queue` | todos* | Estado de la cola offline |

*player solo su propia cola.

### Analytics

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `GET` | `/analytics/points-balance` | admin, researcher | Balance global |
| `GET` | `/analytics/player-game-overview` | admin, researcher | Resumen por juego |
| `GET` | `/analytics/player-attribute-balance` | admin, researcher | Balance por atributo (`balance_ledger` vs `snapshot_points`) |
| `GET` | `/analytics/games/time-to-first-redeem` | admin, researcher | Tiempo promedio sesión→canje por juego (solo flujos válidos, incluye `n_players`) |
| `GET` | `/analytics/sensors/quality` | admin, researcher | Calidad de ingestión por sensor |
| `GET` | `/analytics/sensors/ingest-vs-points` | admin, researcher | Tasa de conversión sensor→puntos |
| `GET` | `/analytics/ic2/summary` | admin, researcher | Estadísticos IC² por condición y período |

### Admin configuración

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `GET/POST/PUT/DELETE` | `/admin/attributes` | admin (write), researcher (read) | CRUD atributos |
| `GET/POST/PUT/DELETE` | `/admin/subattributes` | admin (write), researcher (read) | CRUD subatributos |
| `GET/POST/PUT/DELETE` | `/admin/point-dimensions` | admin (write), researcher (read) | CRUD dimensiones de puntos |
| `GET/POST/PUT/DELETE` | `/admin/modifiable-mechanics` | admin (write), researcher (read) | CRUD mecánicas |
| `GET/POST/PUT/DELETE` | `/admin/modifiable-mechanics-videogames` | admin (write), researcher (read) | Vínculo mecánica↔juego - `options` como JSON |
| `GET` | `/admin/points/consistency-check` | admin, researcher | Checks de integridad del ledger |

### Exportación investigación

| Método | Ruta | Roles | Descripción |
|--------|------|-------|-------------|
| `GET` | `/research/export/points` | admin, researcher | Ledger seudonimizado (JSON/CSV) |
| `GET` | `/research/export/sessions` | admin, researcher | Sesiones de juego (JSON/CSV) |
| `GET` | `/research/export/sensors` | admin, researcher | Eventos de sensor (JSON/CSV) |
| `GET` | `/research/export/ic2` | admin, researcher | Resultados IC² con señales crudas. Seudonimización `LSG-PXXX` |

---

## Changelog

### v1.2.5.2 (2026-06-30)

**Bugfixes:**
    - **`games.py`** - Se corrigió un error en el endpoint `POST /videogames/{game_id}/players/{player_id}/redeem/preview` y `POST /videogames/{game_id}/players/{player_id}/redeem` que no permitía canjear, ya que estaba considerando los puntos por subdimensión/subatributo y no por dimensión/atributo.

### v1.2.5.1 (2026-06-30)

**Bugfixes:**
    - **`points.py`** - Se corrigió un error en el endpoint `GET /players/{player_id}/points/balance` y `GET /players/{player_id}/attributes/points` ya que estaba visualizando los puntos por subdimensión/subatributo, en vez de la dimensión/atributo.

### v1.2.5 (2026-06-25)

**Bugfixes:**
    - **`points.py`** - Se corrigió un error en el endpoint `POST /players/{player_id}/points/adjust` que no permitía ajustar manualmente los puntos de un jugador.

### v1.2.4 (2026-06-06)

**Nuevas funcionalidades:**
- Se agregó el endpoint `POST /game-logs/sessions` que permite subir el log de una sesión de juego.
- **Bugfixes:**
    - Se corrigió un error en el endpoint `DELETE /admin/modifiable-mechanics/{mm_id}` que no permitía eliminar una mecánica.

### v1.2.3.2 (2026-06-02)

**Nuevas funcionalidades:**

- Se agregó el campo `executable` a la tabla `videogame`.

### v1.2.3.1 (2026-05-16)

**Bugfixes:**
- **`security.py`** - Fix para que los endpoints de healthcheck no requieran token JWT.

### v1.2.3 (2026-05-15)

- Agregado endpoint para agregar mecánicas por juego (bulk).

**Bugfixes:**
- Fix de JSON options en algunos endpoints.
- Se corrigieron los nombres de attribute y subattribute para las dimensiones.
- Se arregló el game session para que guarde la duración de la partida.

### v1.2.2.1 (2026-05-13)

- Roles expandidos a "player" y "developer" para que puedan usar la API.

### v1.2.2 (2026-05-13)

**Bugfixes críticos:**
- **`sensors.py`** - Corregido `NameError: name '_json' is not defined` que causaba HTTP 500 en todos los endpoints que parseaban campos JSON de la BD. Causa: `import json as _json` faltaba en el módulo. Fix: agregado como primera línea del archivo.
  - Endpoints reparados: `GET /sensors/{id}/endpoints`, `GET /sensors/players/{id}`, `GET /sensors/players/{id}/ingest-events`.
  - Campos ahora retornados como JSON (no string): `token_parameters`, `specific_parameters`, `watch_parameters`, `tokens`, `raw_payload`.
- **`GET /health` y `GET /health/full`** - Liberados de requerir token JWT. Ahora son accesibles sin autenticación (útil para healthchecks de Nginx y monitoreo externo).
- **`analytics.py` - `GET /analytics/games/time-to-first-redeem`** - Corregido: retornaba valores negativos (`-91812`) cuando la fecha de canje era anterior a la primera sesión. Fix: filtro `WHERE fr.first_redeem >= f.first_started`, campo adicional `n_players` y redondeo a 1 decimal.
- **`admin_config.py`** - Campos `options` en endpoints de `modifiable-mechanics-videogames` ahora retornan JSON parseado (no string escapado).
- **`ic2.py`** - Campo `admissibility` en `GET /ic2/history` y `goalposts` en `GET /ic2/goalposts` ahora retornan JSON parseado.
- **`sensors.py`** - `POST /sensors/players/{id}/link-endpoint`: `schedule_time` corregido de `str` a `int` (formato HHMM: 800=08:00).
- **`sensors.py`** - `POST /sensors/ingest/webhook`: validación previa de `players_sensor_endpoint_id` con mensaje 404 descriptivo (`PLAYERS_SENSOR_ENDPOINT_NOT_FOUND`) en lugar del `IntegrityError` críptico.

**Datos de prueba:**
- SQL `TEST_DATA_time_to_first_redeem.sql`: script de diagnóstico y datos ficticios para verificar `GET /analytics/games/time-to-first-redeem`.

### v1.2.1 (2026-05-07)

**Bugfixes:**
- `games.py` - `PATCH sessions/{sid}/end`: calcula `duration_seconds` con `TIMESTAMPDIFF`. Registra evento en `interaction_logs` (`session_end`).
- `games.py` - `POST .../redeem`: registra evento en `interaction_logs` (`redeem`).
- `games.py` - `GET /videogames/{id}/mechanics`: campo `options` parseado de JSON string a dict.
- `ic2.py` - `GET /ic2/goalposts`: campo `goalposts` parseado correctamente.
- `points.py` - `GET /attributes-map`: resultado del stored procedure parseado de JSON string.
- `points.py` - `GET /points/ledger`: rol `player` forzado a ver solo su propio ledger.
- `sensors.py` - `POST /sensors/ingest/webhook`: validación ownership (player solo su propio id).
- `players.py` - `GET /players` y `GET /players/{id}`: campo `roles: []` incluido via JOIN con `player_roles`.
- `health.py` - `GET /health/full`: verifica tablas IC², offline, `v_ic2_latest`, `v_player_active_roles`.
- `admin_config.py` - Migración a Pydantic v2 (`model_validator`).
- BD PATCH-07: `source_type` ENUM extendido con `OFFLINE_GAME`.
- BD PATCH-08: tabla `research_pseudonym` con códigos `LSG-PXXX` secuenciales.

### v1.2.0 (2026-05)

- `ic2.py` - nuevo módulo: `POST /ic2/compute`, `GET /ic2/history`, `GET /ic2/goalposts`.
- `offline.py` - nuevo módulo: `POST /offline/sync` (idempotente), `GET /offline/queue`.
- `games.py` - `POST /{id}/mechanics/bulk` (hasta 500 mecánicas, HTTP 207).
- `sensors.py` - 4 nuevos endpoints de gestión: `POST /sensors`, `POST /sensors/{id}/endpoints`, `POST /sensors/players/{id}/link`, `POST /sensors/players/{id}/link-endpoint`.
- `analytics.py` - `GET /analytics/ic2/summary`.
- `research_export.py` - `GET /research/export/ic2` con seudonimización `LSG-PXXX`.
- BD: PATCH-03 (tablas IC²), PATCH-04 (offline queue), PATCH-05 (sp_bulk_attach), PATCH-06 (R1-R6 + v_ic2_latest).

### v1.1.0 (2026-05)

- `security.py` - `AUTH_OPEN_ALL=false` por defecto. `CurrentUser.roles` como lista.
- `games.py` - escritura restringida a researcher/admin.
- `research_export.py` - acceso restringido a researcher/admin; guard duplicado eliminado.
- `admin_config.py` - migración a Pydantic v2.
- BD: PATCH-01 (player_roles multi-rol), PATCH-02 (interaction_logs).

---

## Referencias

- [1] González-Ibáñez, R., Macías-Cáceres, J., Villalta-Paucar, M. (2025). LifeSync-Games: A Technical Note on a Novel Framework for Video Game Development. 2025 44th International Conference of the Chilean Computer Science Society (SCCC), Valparaiso, Chile, pp. 1-4, doi: 10.1109/SCCC67219.2025.11420722.<br>
- [2] González-Ibáñez R., Macías-Cáceres J., Villalta-Paucar M., (2025). LifeSync-Games: Toward a Video Game Paradigm for Promoting Responsible Gaming and Human Development. arXiv preprint: 2510.19691 [cs.HC].<br>
- [3] Macías-Cáceres J., Gutiérrez-Vela F., Paderewski-Rodriguez P., González-Ibáñez, R., (2026). LifeSync-Games: Signal-Driven Pervasive Game Design: The LifeSync-Games Framework as a Player Experience Integration Layer. arXiv preprint: 2609.03169 [cs.HC].