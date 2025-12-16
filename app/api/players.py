from datetime import datetime
from typing import Optional, List, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import get_db

from app.security import (
    CurrentUser,
    guard_player_access,
    require_admin,
    require_admin_or_researcher,
)

router = APIRouter()


@router.get("")
def list_players(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(require_admin_or_researcher),
):
    """
    # 1. GET /players
    Lista jugadores con paginación.

    Acceso: admin, researcher, teacher.
    """
    offset = (page - 1) * page_size

    items = db.execute(
        text(
            """
            SELECT id_players, name, email, age, created_at
            FROM players
            ORDER BY id_players
            LIMIT :limit OFFSET :offset
            """
        ),
        {"limit": page_size, "offset": offset},
    ).mappings().all()

    total = db.execute(
        text("SELECT COUNT(*) AS cnt FROM players")
    ).scalar_one()

    return {
        "items": list(items),
        "page": page,
        "page_size": page_size,
        "total": total,
    }


@router.get("/{player_id}")
def get_player(
    player_id: int,
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(guard_player_access),
):
    """
    # 2. GET /players/{player_id}
    Detalle de un jugador.

    Acceso: admin, researcher, teacher, player.
    """
    row = db.execute(
        text(
            """
            SELECT id_players, name, email, age, created_at, updated_at
            FROM players
            WHERE id_players = :player_id
            """
        ),
        {"player_id": player_id},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Player not found")

    return dict(row)


@router.delete("/{player_id}")
def delete_player(
    player_id: int,
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(require_admin),
):
    """
    # 3. DELETE /players/{player_id}
    Llama a sp_delete_player_cascade para borrar en cascada.

    Acceso: admin.
    """
    try:
        db.execute(text("CALL sp_delete_player_cascade(:p_id)"), {"p_id": player_id})
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error deleting player: {e}")

    return {"status": "deleted", "id_players": player_id}


@router.post("/{player_id}/attributes/init")
def init_player_attributes(
    player_id: int,
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(require_admin_or_researcher),
):
    """
    # 4. POST /players/{player_id}/attributes/init
    Inicializa players_attributes para este jugador.

    Acceso: admin, researcher.
    """
    try:
        db.execute(
            text("CALL sp_init_player_attributes(:p_id_players)"),
            {"p_id_players": player_id},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Error initializing attributes: {e}"
        )

    return {"status": "initialized", "id_players": player_id}


@router.get("/{player_id}/games")
def get_player_games(
    player_id: int,
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(guard_player_access),
):
    """
    # 5. GET /players/{player_id}/games
    Usa la vista v_player_game_overview.

    Acceso: admin, researcher, teacher, player.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_players,
              player_name,
              player_email,
              id_videogame,
              videogame_name,
              points_spent,
              seconds_with_lsg
            FROM v_player_game_overview
            WHERE id_players = :player_id
            """
        ),
        {"player_id": player_id},
    ).mappings().all()

    return list(rows)


@router.get("/{player_id}/timeline")
def get_player_timeline(
    player_id: int,
    from_ts: Optional[str] = Query(
        None, description="Filtrar desde esta fecha (YYYY-MM-DD HH:MM:SS)"
    ),
    to_ts: Optional[str] = Query(
        None, description="Filtrar hasta esta fecha (YYYY-MM-DD HH:MM:SS)"
    ),
    limit: int = Query(200, ge=10, le=1000),
    db: Session = Depends(get_db),
    _: CurrentUser = Depends(guard_player_access),
):
    """
    Timeline unificado del jugador (sesiones, puntos, sensores, canjes).

    Devuelve una lista mezclada cronológicamente de eventos:
    - game_session
    - points_ledger
    - sensor_ingest
    - redemption

    Acceso: admin, researcher, teacher, player.
    """
    params_base = {"pid": player_id}
    if from_ts is not None:
        params_base["from_ts"] = from_ts
    if to_ts is not None:
        params_base["to_ts"] = to_ts

    def _add_time_filter(base_sql: str, field: str) -> str:
        conditions = []
        if from_ts is not None:
            conditions.append(f"{field} >= :from_ts")
        if to_ts is not None:
            conditions.append(f"{field} <= :to_ts")
        if conditions:
            if "WHERE" in base_sql.upper():
                base_sql += " AND " + " AND ".join(conditions)
            else:
                base_sql += " WHERE " + " AND ".join(conditions)
        return base_sql

    events: List[dict] = []

    # 1) Sesiones de juego
    sql_sessions = """
        SELECT
          s.id_lsg_game_session,
          s.started_at,
          s.ended_at,
          s.duration_seconds,
          pvg.id_videogame,
          vg.name AS videogame_name
        FROM lsg_game_session s
        JOIN player_videogame pvg
          ON s.id_player_videogame = pvg.id_player_videogame
        JOIN videogame vg
          ON vg.id_videogame = pvg.id_videogame
        WHERE pvg.id_players = :pid
    """
    sql_sessions = _add_time_filter(sql_sessions, "s.started_at")
    sql_sessions += " ORDER BY s.started_at DESC LIMIT :limit"
    sessions = db.execute(
        text(sql_sessions),
        {**params_base, "limit": limit},
    ).mappings().all()

    for row in sessions:
        events.append(
            {
                "event_type": "game_session",
                "occurred_at": row["started_at"],
                "data": dict(row),
            }
        )

    # 2) Movimientos de puntos
    sql_points = """
        SELECT
          id_points_ledger,
          id_players,
          id_point_dimension,
          id_videogame,
          direction,
          amount,
          source_type,
          source_ref,
          occurred_at
        FROM points_ledger
        WHERE id_players = :pid
    """
    sql_points = _add_time_filter(sql_points, "occurred_at")
    sql_points += " ORDER BY occurred_at DESC LIMIT :limit"
    points = db.execute(
        text(sql_points),
        {**params_base, "limit": limit},
    ).mappings().all()

    for row in points:
        events.append(
            {
                "event_type": "points",
                "occurred_at": row["occurred_at"],
                "data": dict(row),
            }
        )

    # 3) Ingestas de sensores
    sql_sensors = """
        SELECT
          id_sensor_ingest_event,
          id_players,
          id_players_sensor_endpoint,
          id_sensor_endpoint,
          parsed_value,
          status,
          occurred_at
        FROM sensor_ingest_event
        WHERE id_players = :pid
    """
    sql_sensors = _add_time_filter(sql_sensors, "occurred_at")
    sql_sensors += " ORDER BY occurred_at DESC LIMIT :limit"
    sensor_events = db.execute(
        text(sql_sensors),
        {**params_base, "limit": limit},
    ).mappings().all()

    for row in sensor_events:
        events.append(
            {
                "event_type": "sensor_ingest",
                "occurred_at": row["occurred_at"],
                "data": dict(row),
            }
        )

    # 4) Canjes (redemption_event + points_ledger)
    sql_redemptions = """
        SELECT
          r.id_redemption_event,
          r.id_points_ledger,
          r.redeemed_points,
          pl.id_players,
          pl.id_videogame,
          pl.id_point_dimension,
          pl.occurred_at,
          pl.amount,
          pl.source_ref
        FROM redemption_event r
        JOIN points_ledger pl
          ON pl.id_points_ledger = r.id_points_ledger
        WHERE pl.id_players = :pid
    """
    sql_redemptions = _add_time_filter(sql_redemptions, "pl.occurred_at")
    sql_redemptions += " ORDER BY pl.occurred_at DESC LIMIT :limit"
    redemptions = db.execute(
        text(sql_redemptions),
        {**params_base, "limit": limit},
    ).mappings().all()

    for row in redemptions:
        events.append(
            {
                "event_type": "redemption",
                "occurred_at": row["occurred_at"],
                "data": dict(row),
            }
        )

    # Ordenamos todos los eventos por fecha descendente
    events_sorted = sorted(
        events,
        key=lambda e: e["occurred_at"] or datetime.min,
        reverse=True,
    )

    # Cortamos al límite global
    events_sorted = events_sorted[:limit]

    return {
        "player_id": player_id,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "total_events": len(events_sorted),
        "items": events_sorted,
    }
