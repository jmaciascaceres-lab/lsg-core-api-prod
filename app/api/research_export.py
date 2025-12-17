import csv
import hashlib
import io
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Annotated
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BeforeValidator
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import get_db
from app.security import require_roles

router = APIRouter(prefix="/research/export", tags=["research-export"])


# ==========
# Helpers
# ==========

RESEARCH_PSEUDONYM_SALT = os.getenv("RESEARCH_PSEUDONYM_SALT", "change-me-for-prod")


def decode_ts(v: Any) -> Any:
    if isinstance(v, str):
        return unquote(v)
    return v


def _pseudonymize_player(player_id: Optional[int]) -> Optional[str]:
    """
    Genera un ID seudonimizado estable para un player_id dado,
    usando un salt definido en RESEARCH_PSEUDONYM_SALT.
    """
    if player_id is None:
        return None
    base = f"{RESEARCH_PSEUDONYM_SALT}:{player_id}".encode("utf-8")
    # recortamos para que sea manejable
    return hashlib.sha256(base).hexdigest()[:16]


def _apply_pseudonymization(
    rows: List[Dict[str, Any]],
    include_raw_ids: bool,
) -> List[Dict[str, Any]]:
    """
    Agrega la columna player_pseudo y opcionalmente elimina id_players,
    player_name y player_email.
    """
    out: List[Dict[str, Any]] = []

    for r in rows:
        r = dict(r)
        pid = r.get("id_players")
        r["player_pseudo"] = _pseudonymize_player(pid)

        if not include_raw_ids:
            r.pop("id_players", None)
            r.pop("player_name", None)
            r.pop("player_email", None)

        out.append(r)

    return out


def _build_csv_response(rows: List[Dict[str, Any]], filename: str) -> Response:
    """
    Convierte una lista de dicts en CSV (texto) y devuelve un Response.
    """
    buf = io.StringIO()

    if not rows:
        # CSV solo con cabecera vacía
        buf.write("")
    else:
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    content = buf.getvalue()
    buf.close()

    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# =========================
# 1) Export: Points ledger
# =========================

@router.get("/points", dependencies=[require_roles(ROLE_ALL)])
def export_points(
    from_ts: Optional[Annotated[datetime, BeforeValidator(decode_ts)]] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (inicio ventana tiempo, opcional)"
    ),
    to_ts: Optional[Annotated[datetime, BeforeValidator(decode_ts)]] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (fin ventana tiempo, opcional)"
    ),
    player_id: Optional[int] = Query(
        None, description="Filtra por id_players (opcional)"
    ),
    videogame_id: Optional[int] = Query(
        None, description="Filtra por id_videogame (opcional)"
    ),
    source_type: Optional[str] = Query(
        None, description="Filtra por source_type (opcional, ej. SENSOR, REDEMPTION)"
    ),
    format: str = Query(
        "json", pattern="^(json|csv)$", description="Formato de salida: json o csv"
    ),
    include_raw_ids: bool = Query(
        False,
        description="Si es false, elimina id_players / nombre/email del export",
    ),
    limit: Optional[int] = Query(
        None, ge=1, le=100000, description="Límite máximo de filas (opcional)"
    ),
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(require_roles(["admin", "researcher", "teacher", "player"])),
):
    """
    Exporta movimientos de puntos (points_ledger) con contexto mínimo
    para análisis de investigación.

    Incluye:
    - info básica de points_ledger
    - info de jugador (opcionalmente seudonimizada)
    - info de videojuego y point_dimension

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          pl.id_points_ledger,
          pl.id_players,
          p.name AS player_name,
          p.email AS player_email,
          pl.id_point_dimension,
          pd.code AS point_dimension_code,
          pd.name AS point_dimension_name,
          pl.id_videogame,
          vg.name AS videogame_name,
          pl.direction,
          pl.amount,
          pl.source_type,
          pl.source_ref,
          pl.payload,
          pl.occurred_at,
          pl.created_at,
          pl.id_sensor_ingest_event
        FROM points_ledger pl
        JOIN players p
          ON p.id_players = pl.id_players
        LEFT JOIN point_dimension pd
          ON pd.id_point_dimension = pl.id_point_dimension
        LEFT JOIN videogame vg
          ON vg.id_videogame = pl.id_videogame
    """

    conditions = []
    params: Dict[str, Any] = {}

    if from_ts is not None:
        conditions.append("pl.occurred_at >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts is not None:
        conditions.append("pl.occurred_at <= :to_ts")
        params["to_ts"] = to_ts
    if player_id is not None:
        conditions.append("pl.id_players = :pid")
        params["pid"] = player_id
    if videogame_id is not None:
        conditions.append("pl.id_videogame = :vgid")
        params["vgid"] = videogame_id
    if source_type is not None:
        conditions.append("pl.source_type = :stype")
        params["stype"] = source_type

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    base += " ORDER BY pl.occurred_at"

    if limit is not None:
        base += " LIMIT :limit"
        params["limit"] = limit

    rows = db.execute(text(base), params).mappings().all()
    data = _apply_pseudonymization([dict(r) for r in rows], include_raw_ids)

    if format == "csv":
        return _build_csv_response(data, "points_export.csv")

    return {"items": data, "count": len(data)}


# =========================
# 2) Export: Game sessions
# =========================

@router.get("/sessions", dependencies=[require_roles(["admin", "researcher"])])
def export_sessions(
    from_ts: Optional[Annotated[datetime, BeforeValidator(decode_ts)]] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (inicio ventana tiempo, opcional)"
    ),
    to_ts: Optional[Annotated[datetime, BeforeValidator(decode_ts)]] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (fin ventana tiempo, opcional)"
    ),
    player_id: Optional[int] = Query(
        None, description="Filtra por id_players (opcional)"
    ),
    videogame_id: Optional[int] = Query(
        None, description="Filtra por id_videogame (opcional)"
    ),
    format: str = Query(
        "json", pattern="^(json|csv)$", description="Formato de salida: json o csv"
    ),
    include_raw_ids: bool = Query(
        False,
        description="Si es false, elimina id_players / nombre/email del export",
    ),
    limit: Optional[int] = Query(
        None, ge=1, le=100000, description="Límite máximo de filas (opcional)"
    ),
    db: Session = Depends(get_db),
):
    """
    Exporta sesiones de juego (lsg_game_session + player_videogame + videogame + players).

    Incluye:
    - info de sesión (started_at, ended_at, duration_seconds)
    - info de videojuego
    - info de jugador (seudonimizada)

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          s.id_lsg_game_session,
          s.id_player_videogame,
          s.started_at,
          s.ended_at,
          s.duration_seconds,
          s.session_metrics,
          pvg.id_players,
          p.name AS player_name,
          p.email AS player_email,
          pvg.id_videogame,
          vg.name AS videogame_name
        FROM lsg_game_session s
        JOIN player_videogame pvg
          ON pvg.id_player_videogame = s.id_player_videogame
        JOIN players p
          ON p.id_players = pvg.id_players
        JOIN videogame vg
          ON vg.id_videogame = pvg.id_videogame
    """

    conditions = []
    params: Dict[str, Any] = {}

    if from_ts is not None:
        conditions.append("s.started_at >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts is not None:
        conditions.append("s.started_at <= :to_ts")
        params["to_ts"] = to_ts
    if player_id is not None:
        conditions.append("pvg.id_players = :pid")
        params["pid"] = player_id
    if videogame_id is not None:
        conditions.append("pvg.id_videogame = :vgid")
        params["vgid"] = videogame_id

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    base += " ORDER BY s.started_at"

    if limit is not None:
        base += " LIMIT :limit"
        params["limit"] = limit

    rows = db.execute(text(base), params).mappings().all()
    data = _apply_pseudonymization([dict(r) for r in rows], include_raw_ids)

    if format == "csv":
        return _build_csv_response(data, "sessions_export.csv")

    return {"items": data, "count": len(data)}


# =========================
# 3) Export: Sensor ingest
# =========================

@router.get("/sensors", dependencies=[require_roles(["admin", "researcher"])])
def export_sensors(
    from_ts: Optional[Annotated[datetime, BeforeValidator(decode_ts)]] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (inicio ventana tiempo, opcional)"
    ),
    to_ts: Optional[Annotated[datetime, BeforeValidator(decode_ts)]] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (fin ventana tiempo, opcional)"
    ),
    player_id: Optional[int] = Query(
        None, description="Filtra por id_players (opcional)"
    ),
    sensor_endpoint_id: Optional[int] = Query(
        None, description="Filtra por id_sensor_endpoint (opcional)"
    ),
    format: str = Query(
        "json", pattern="^(json|csv)$", description="Formato de salida: json o csv"
    ),
    include_raw_ids: bool = Query(
        False,
        description="Si es false, elimina id_players / nombre/email del export",
    ),
    limit: Optional[int] = Query(
        None, ge=1, le=100000, description="Límite máximo de filas (opcional)"
    ),
    db: Session = Depends(get_db),
):
    """
    Exporta eventos de sensor (sensor_ingest_event) con contexto:

    - datos de ingest (status, parsed_value, raw_payload opcional si decides mantenerlo)
    - info de jugador (seudonimizada)
    - info de sensor_endpoint

    Nota ética:
    - Considera si quieres exportar `raw_payload` completo o solo métricas derivadas
      (según CEI / protocolo). Aquí lo incluimos tal cual existe en la tabla.

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          sie.id_sensor_ingest_event,
          sie.id_players,
          p.name AS player_name,
          p.email AS player_email,
          sie.id_players_sensor_endpoint,
          sie.id_sensor_endpoint,
          se.name AS sensor_endpoint_name,
          sie.raw_payload,
          sie.parsed_value,
          sie.status,
          sie.error_message,
          sie.occurred_at,
          sie.created_at
        FROM sensor_ingest_event sie
        JOIN players p
          ON p.id_players = sie.id_players
        LEFT JOIN sensor_endpoint se
          ON se.id_sensor_endpoint = sie.id_sensor_endpoint
    """

    conditions = []
    params: Dict[str, Any] = {}

    if from_ts is not None:
        conditions.append("sie.occurred_at >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts is not None:
        conditions.append("sie.occurred_at <= :to_ts")
        params["to_ts"] = to_ts
    if player_id is not None:
        conditions.append("sie.id_players = :pid")
        params["pid"] = player_id
    if sensor_endpoint_id is not None:
        conditions.append("sie.id_sensor_endpoint = :seid")
        params["seid"] = sensor_endpoint_id

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    base += " ORDER BY sie.occurred_at"

    if limit is not None:
        base += " LIMIT :limit"
        params["limit"] = limit

    rows = db.execute(text(base), params).mappings().all()
    data = _apply_pseudonymization([dict(r) for r in rows], include_raw_ids)

    if format == "csv":
        return _build_csv_response(data, "sensors_export.csv")

    return {"items": data, "count": len(data)}
