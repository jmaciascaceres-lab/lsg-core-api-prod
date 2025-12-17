# LSG Core API (LifeSync-Games)

Servicio FastAPI que expone la lógica de dominio de LifeSync-Games sobre la base de datos `db_lsg` (MySQL 8).

---

## Estructura del proyecto

```text
lsg-core-api-prod/
│
├── app/
│   ├── api/
│   │   ├── health.py          # /health, /health/full
│   │   ├── meta.py            # /meta/info
│   │   ├── players.py         # /players/... (+ /players/{id}/timeline)
│   │   ├── points.py          # /attributes..., /points..., /players/{id}/points/...
│   │   ├── games.py           # /videogames/..., canjes y sesiones de juego
│   │   ├── sensors.py         # /sensors/... (config e ingest)
│   │   ├── analytics.py       # /analytics/... (puntos, juegos, sensores)
│   │   ├── admin_config.py    # /admin/... (atributos, dimensiones, mecánicas)
│   │   ├── admin_points.py    # /admin/points/consistency-check
│   │   └── research_export.py # /research/export/... (puntos, sesiones, sensores)
│   │
│   ├── security.py            # JWT, roles y dependencias de autorización
│   ├── db.py                  # conexión SQLAlchemy a MySQL
│   └── main.py                # instancia FastAPI y registro de routers
│
├── db/
│   └── init/
│       └── 01_db_lsg_dump.sql # dump de la base de datos
│
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Uso del servicio (Producción)

### 1. Características

- Docker + Docker Compose
- Red docker externa `lsg_shared` (la usan `lsg-auth` y `lsg-core-api`)

Se utiliza una red compartida:
```bash
docker network create lsg_shared
```

### 2. Variables de entorno

Configuración del archivo .env:
- DB_NAME
- DB_USER
- DB_PASSWORD
- API_PORT=8012

JWT:
- AUTH_JWT_SECRET
- AUTH_JWT_ALGORITHM=HS256
- AUTH_JWT_ISSUER=lsg-auth
- AUTH_JWT_AUDIENCE=lsg-core-api

LSG_CORE_API_ROOT_PATH=/lsg-core-api

Control de roles:
- AUTH_OPEN_ALL=true
- AUTH_OPEN_ALL=false

### 3. Levantar el stack

```bash
docker compose up -d --build
docker ps
docker logs -n 100 lsg_core_api
```

### 4. Verificar Swagger

- Core API Swagger: `https://lsg.diinf.usach.cl/lsg-core-api/docs`
- OpenAPI JSON: `https://lsg.diinf.usach.cl/lsg-core-api/openapi.json`

### 5. Flujo de autenticación

1. Ir a Swagger de Auth: `https://lsg.diinf.usach.cl/lsg-auth/docs`
2. Si no existe usuario: POST /players
3. Login: POST /login → copiar access_token

Notas:
- El token expira según JWT_EXPIRE_MINUTES configurado en lsg-auth (default 60 min).
- El JWT debe incluir el claim role (player/teacher/researcher/admin)

### 6. Autorizar en Swagger de Core API

1. Abrir Swagger de Core API: `https://lsg.diinf.usach.cl/lsg-core-api/docs`
2. Botón Authorize
3. Pegar: Bearer <access_token>
4. Probar endpoints

### 7. Endpoints

- /health
- /health/full
- /meta/info
- /players/...
- /points/...
- /videogames/...
- /sensors/...
- /analytics/...
- /admin/...
- /research/export/...

### 8. Fuente

González-Ibáñez, R., Macías-Cáceres, J., Villalta-Paucar, M. (2025). LifeSync-Games: Toward a Video Game Paradigm for Promoting Responsible Gaming and Human Development. arXiv preprint: 2510.19691 [cs.HC], 2025. DOI: https://arxiv.org/abs/2510.19691

