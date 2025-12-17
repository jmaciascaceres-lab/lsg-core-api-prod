from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import get_db
from app.security import require_roles, guard_player_access
    
router = APIRouter()


@router.get("/points-balance", dependencies=[Depends(require_roles(["admin", "researcher"]))])
def get_points_balance(
    player_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """
    # 25. GET /analytics/points-balance
    Lee desde v_points_balance.

    Acceso: admin, researcher.
    """
    base_query = "SELECT * FROM v_points_balance"
    params = {}

    if player_id is not None:
        base_query += " WHERE id_players = :player_id"
        params["player_id"] = player_id

    result = db.execute(text(base_query), params)
    rows = [dict(row._mapping) for row in result]
    return {"items": rows}


@router.get("/player-game-overview", dependencies=[Depends(require_roles(["admin", "researcher"]))])
def get_player_game_overview(
    player_id: Optional[int] = Query(None),
    videogame_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """
    # 24. GET /analytics/player-game-overview
    Usa v_player_game_overview.

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          id_players,
          player_name,
          player_email,
          id_videogame,
          videogame_name,
          points_spent,
          seconds_with_lsg
        FROM v_player_game_overview
    """
    conditions = []
    params: dict = {}

    if player_id is not None:
        conditions.append("id_players = :pid")
        params["pid"] = player_id
    if videogame_id is not None:
        conditions.append("id_videogame = :gid")
        params["gid"] = videogame_id

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    rows = db.execute(text(base), params).mappings().all()
    return list(rows)


@router.get("/player-attribute-balance", dependencies=[Depends(require_roles(["admin", "researcher"]))])
def get_player_attribute_balance(
    player_id: Optional[int] = Query(None),
    attribute_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """
    # 26. GET /analytics/player-attribute-balance
    Usa v_player_attribute_balance.

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          id_players,
          player_name,
          player_email,
          id_attributes,
          attribute_name,
          balance_ledger,
          snapshot_points,
          diff_ledger_minus_snapshot
        FROM v_player_attribute_balance
    """
    conditions = []
    params: dict = {}

    if player_id is not None:
        conditions.append("id_players = :pid")
        params["pid"] = player_id
    if attribute_id is not None:
        conditions.append("id_attributes = :aid")
        params["aid"] = attribute_id

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    rows = db.execute(text(base), params).mappings().all()
    return list(rows)


@router.get("/games/time-to-first-redeem", dependencies=[Depends(require_roles(["admin", "researcher"]))] )  
def get_time_to_first_redeem(
    db: Session = Depends(get_db),
):
    """
    # 27. GET /analytics/games/time-to-first-redeem
    Versión simple: tiempo promedio (en minutos) desde primera sesión
    hasta primer canje, por juego.

    Acceso: admin, researcher.
    """
    query = """
        WITH first_session AS (
          SELECT
            pvg.id_players,
            pvg.id_videogame,
            MIN(s.started_at) AS first_started
          FROM player_videogame pvg
          JOIN lsg_game_session s
            ON s.id_player_videogame = pvg.id_player_videogame
          GROUP BY pvg.id_players, pvg.id_videogame
        ),
        first_redeem AS (
          SELECT
            id_players,
            id_videogame,
            MIN(occurred_at) AS first_redeem
          FROM points_ledger
          WHERE direction = 'DEBIT'
            AND source_type = 'REDEMPTION'
          GROUP BY id_players, id_videogame
        )
        SELECT
          f.id_videogame,
          AVG(TIMESTAMPDIFF(MINUTE, f.first_started, fr.first_redeem))
            AS avg_minutes_to_redeem
        FROM first_session f
        JOIN first_redeem fr
          ON fr.id_players = f.id_players
         AND fr.id_videogame = f.id_videogame
        GROUP BY f.id_videogame
    """

    rows = db.execute(text(query)).mappings().all()
    return list(rows)


# ---------- Data quality & sensores ----------


@router.get("/sensors/quality", dependencies=[Depends(require_roles(["admin", "researcher"]))] )
def get_sensors_quality(
    player_id: Optional[int] = Query(
        None, description="Filtra por id_players (opcional)"
    ),
    sensor_endpoint_id: Optional[int] = Query(
        None, description="Filtra por id_sensor_endpoint (opcional)"
    ),
    from_ts: Optional[str] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (inicio ventana tiempo, opcional)"
    ),
    to_ts: Optional[str] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (fin ventana tiempo, opcional)"
    ),
    db: Session = Depends(get_db),
):
    """
    # 28. GET /analytics/sensors/quality

    Resumen de calidad de ingestión por jugador + endpoint:

    - total_events
    - ok_events / error_events / ignored_events
    - ok_rate, error_rate, ignored_rate
    - first_event_at / last_event_at
    - active_days (días distintos con eventos)
    - avg_events_per_day
    - min/avg/max parsed_value

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          sie.id_players,
          p.name AS player_name,
          sie.id_sensor_endpoint,
          se.name AS sensor_endpoint_name,
          COUNT(*) AS total_events,
          SUM(CASE WHEN sie.status = 'OK' THEN 1 ELSE 0 END) AS ok_events,
          SUM(CASE WHEN sie.status = 'ERROR' THEN 1 ELSE 0 END) AS error_events,
          SUM(CASE WHEN sie.status = 'IGNORED' THEN 1 ELSE 0 END) AS ignored_events,
          MIN(sie.occurred_at) AS first_event_at,
          MAX(sie.occurred_at) AS last_event_at,
          COUNT(DISTINCT DATE(sie.occurred_at)) AS active_days,
          AVG(sie.parsed_value) AS avg_parsed_value,
          MIN(sie.parsed_value) AS min_parsed_value,
          MAX(sie.parsed_value) AS max_parsed_value
        FROM sensor_ingest_event sie
        JOIN players p
          ON p.id_players = sie.id_players
        LEFT JOIN sensor_endpoint se
          ON se.id_sensor_endpoint = sie.id_sensor_endpoint
    """

    conditions = []
    params: dict = {}

    if player_id is not None:
        conditions.append("sie.id_players = :pid")
        params["pid"] = player_id
    if sensor_endpoint_id is not None:
        conditions.append("sie.id_sensor_endpoint = :seid")
        params["seid"] = sensor_endpoint_id
    if from_ts is not None:
        conditions.append("sie.occurred_at >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts is not None:
        conditions.append("sie.occurred_at <= :to_ts")
        params["to_ts"] = to_ts

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    base += """
        GROUP BY
          sie.id_players,
          p.name,
          sie.id_sensor_endpoint,
          se.name
    """

    rows = db.execute(text(base), params).mappings().all()

    result = []
    for row in rows:
        r = dict(row)
        total = r["total_events"] or 0
        ok = r["ok_events"] or 0
        err = r["error_events"] or 0
        ign = r["ignored_events"] or 0
        days = r["active_days"] or 0

        r["ok_rate"] = ok / total if total > 0 else 0.0
        r["error_rate"] = err / total if total > 0 else 0.0
        r["ignored_rate"] = ign / total if total > 0 else 0.0
        r["avg_events_per_day"] = total / days if days > 0 else None

        result.append(r)

    return result


@router.get("/sensors/ingest-vs-points", dependencies=[Depends(require_roles(["admin", "researcher"]))] )
def get_sensors_ingest_vs_points(
    player_id: Optional[int] = Query(
        None, description="Filtra por id_players (opcional)"
    ),
    sensor_endpoint_id: Optional[int] = Query(
        None, description="Filtra por id_sensor_endpoint (opcional)"
    ),
    from_ts: Optional[str] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (inicio ventana tiempo, opcional)"
    ),
    to_ts: Optional[str] = Query(
        None, description="YYYY-MM-DD HH:MM:SS (fin ventana tiempo, opcional)"
    ),
    db: Session = Depends(get_db),
):
    """
    # 29. GET /analytics/sensors/ingest-vs-points

    Mira la cadena sensor -> points_ledger (source_type='SENSOR', direction='CREDIT'):

    - ingest_events  : cantidad de eventos de sensor_ingest_event
    - points_events  : cuántos de esos eventos generaron filas en points_ledger
    - total_points   : suma de amount de esos créditos
    - conversion_rate: points_events / ingest_events
    - avg_points_per_event: total_points / points_events

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          sie.id_players,
          p.name AS player_name,
          sie.id_sensor_endpoint,
          se.name AS sensor_endpoint_name,
          COUNT(*) AS ingest_events,
          SUM(
            CASE
              WHEN pl.id_points_ledger IS NOT NULL THEN 1
              ELSE 0
            END
          ) AS points_events,
          SUM(
            CASE
              WHEN pl.id_points_ledger IS NOT NULL THEN pl.amount
              ELSE 0
            END
          ) AS total_points
        FROM sensor_ingest_event sie
        JOIN players p
          ON p.id_players = sie.id_players
        LEFT JOIN sensor_endpoint se
          ON se.id_sensor_endpoint = sie.id_sensor_endpoint
        LEFT JOIN points_ledger pl
          ON pl.id_sensor_ingest_event = sie.id_sensor_ingest_event
         AND pl.direction = 'CREDIT'
         AND pl.source_type = 'SENSOR'
    """

    conditions = []
    params: dict = {}

    if player_id is not None:
        conditions.append("sie.id_players = :pid")
        params["pid"] = player_id
    if sensor_endpoint_id is not None:
        conditions.append("sie.id_sensor_endpoint = :seid")
        params["seid"] = sensor_endpoint_id
    if from_ts is not None:
        conditions.append("sie.occurred_at >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts is not None:
        conditions.append("sie.occurred_at <= :to_ts")
        params["to_ts"] = to_ts

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    base += """
        GROUP BY
          sie.id_players,
          p.name,
          sie.id_sensor_endpoint,
          se.name
    """

    rows = db.execute(text(base), params).mappings().all()

    result = []
    for row in rows:
        r = dict(row)
        ingest_events = r["ingest_events"] or 0
        points_events = r["points_events"] or 0
        total_points = r["total_points"] or 0

        r["conversion_rate"] = (
            points_events / ingest_events if ingest_events > 0 else 0.0
        )
        r["avg_points_per_event"] = (
            total_points / points_events if points_events > 0 else None
        )

        result.append(r)

    return result
