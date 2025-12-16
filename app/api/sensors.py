from datetime import datetime
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import get_db

from app.security import (
    CurrentUser,
    get_current_user,
    require_admin_or_researcher,
    guard_player_access,
)

router = APIRouter()


# ---------- Models ----------

class SensorIngestRequest(BaseModel):
    player_id: int
    sensor_endpoint_id: int
    players_sensor_endpoint_id: Optional[int] = None
    raw_payload: dict
    parsed_value: Optional[float] = None
    status: Literal["OK", "ERROR", "IGNORED"] = "OK"
    error_message: Optional[str] = None
    occurred_at: Optional[datetime] = None


# ---------- Sensors ----------

@router.get("")
def list_sensors(
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(require_admin_or_researcher),
):
    """
    # 19. GET /sensors

    Acceso: admin, researcher, teacher, player.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_online_sensor,
              name,
              description,
              base_url,
              initiated_date,
              updated_at
            FROM online_sensor
            ORDER BY id_online_sensor
            """
        )
    ).mappings().all()
    return list(rows)


@router.get("/{sensor_id}/endpoints")
def list_sensor_endpoints(
    sensor_id: int,
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(require_admin_or_researcher),
):
    """
    # 20. GET /sensors/{sensor_id}/endpoints

    Acceso: admin, researcher, teacher, player.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_sensor_endpoint,
              sensor_endpoint_id_online_sensor,
              name,
              description,
              url_endpoint,
              token_parameters,
              specific_parameters,
              watch_parameters,
              created_at,
              updated_at
            FROM sensor_endpoint
            WHERE sensor_endpoint_id_online_sensor = :sid
            """
        ),
        {"sid": sensor_id},
    ).mappings().all()
    return list(rows)


@router.get("/players/{player_id}")
def get_player_sensors(
    player_id: int,
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(guard_player_access),
):
    """
    # 21. GET /sensors/players/{player_id}
    Sensores y endpoints asociados a un jugador.

    Acceso: admin, researcher, teacher, player.
    """
    rows = db.execute(
        text(
            """
            SELECT
              pos.id_players_online_sensor,
              pos.id_players,
              pos.id_online_sensor,
              os.name AS sensor_name,
              os.description AS sensor_description,
              pos.tokens,
              pos.expires_at,
              pos.rotated_at,
              se.id_sensor_endpoint,
              se.name AS endpoint_name,
              se.description AS endpoint_description,
              se.url_endpoint,
              pse.id_players_sensor_endpoint,
              pse.activated,
              pse.schedule_time
            FROM player_online_sensor pos
            JOIN online_sensor os
              ON pos.id_online_sensor = os.id_online_sensor
            LEFT JOIN players_sensor_endpoint pse
              ON pse.id_players = pos.id_players
            LEFT JOIN sensor_endpoint se
              ON se.id_sensor_endpoint = pse.Id_sensor_endpoint
            WHERE pos.id_players = :pid
            """
        ),
        {"pid": player_id},
    ).mappings().all()

    return list(rows)


@router.post("/ingest/webhook")
def ingest_sensor_event(
    payload: SensorIngestRequest,
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(get_current_user),
):
    """
    # 22. POST /sensors/ingest/webhook
    MVP: inserta en sensor_ingest_event (sin lógica de conversión a puntos aún).

    Acceso: admin, researcher.
    """
    import json

    occurred_at = payload.occurred_at or datetime.utcnow()

    try:
        result = db.execute(
            text(
                """
                INSERT INTO sensor_ingest_event (
                  id_players,
                  id_players_sensor_endpoint,
                  id_sensor_endpoint,
                  raw_payload,
                  parsed_value,
                  status,
                  error_message,
                  occurred_at
                ) VALUES (
                  :id_players,
                  :id_pse,
                  :id_se,
                  :raw_payload,
                  :parsed_value,
                  :status,
                  :error_message,
                  :occurred_at
                )
                """
            ),
            {
                "id_players": payload.player_id,
                "id_pse": payload.players_sensor_endpoint_id,
                "id_se": payload.sensor_endpoint_id,
                "raw_payload": json.dumps(payload.raw_payload),
                "parsed_value": payload.parsed_value,
                "status": payload.status,
                "error_message": payload.error_message,
                "occurred_at": occurred_at,
            },
        )
        sie_id = result.lastrowid
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error ingesting sensor data: {e}")

    return {"status": "ok", "id_sensor_ingest_event": sie_id}


@router.get("/players/{player_id}/ingest-events")
def list_player_ingest_events(
    player_id: int,
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(guard_player_access),
):
    """
    # 23. GET /sensors/players/{player_id}/ingest-events

    Acceso: admin, researcher, teacher, player.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_sensor_ingest_event,
              id_players,
              id_players_sensor_endpoint,
              id_sensor_endpoint,
              raw_payload,
              parsed_value,
              status,
              error_message,
              occurred_at,
              created_at
            FROM sensor_ingest_event
            WHERE id_players = :pid
            ORDER BY occurred_at DESC
            LIMIT :limit
            """
        ),
        {"pid": player_id, "limit": limit},
    ).mappings().all()

    return list(rows)
