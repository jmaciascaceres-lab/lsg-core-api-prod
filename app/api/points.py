from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import get_db

from app.security import (
    require_roles,
    guard_player_access,
    ROLE_ALL,
)

router = APIRouter()


# ---------- Models ----------

class PointsAdjustRequest(BaseModel):
    point_dimension_id: int = Field(..., alias="point_dimension_id")
    direction: Literal["CREDIT", "DEBIT"]
    amount: int = Field(..., gt=0)
    reason: Optional[str] = None
    videogame_id: Optional[int] = None


# ---------- Attributes & Subattributes ----------

@router.get("/attributes", tags=["attributes"], dependencies=[Depends(require_roles(ROLE_ALL))])
def list_attributes(
    db: Session = Depends(get_db),
):
    """
    # 6. GET /attributes

    Acceso: abierto a todos.
    """
    rows = db.execute(
        text(
            """
            SELECT id_attributes, name, description, data_type, created_at, updated_at
            FROM attributes
            ORDER BY id_attributes
            """
        )
    ).mappings().all()
    return list(rows)


@router.get("/attributes/{attribute_id}/subattributes", tags=["attributes"], dependencies=[Depends(require_roles(ROLE_ALL))])
def list_subattributes(
    attribute_id: int,
    db: Session = Depends(get_db),
):
    """
    # 7. GET /attributes/{attribute_id}/subattributes

    Acceso: abierto a todos.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_subattributes,
              name,
              description,
              created_at,
              updated_at
            FROM subattributes
            WHERE attributes_id_attributes = :attr_id
            ORDER BY id_subattributes
            """
        ),
        {"attr_id": attribute_id},
    ).mappings().all()
    return list(rows)


@router.get("/attributes-map", tags=["attributes"], dependencies=[Depends(require_roles(ROLE_ALL))])
def get_attributes_map(
    db: Session = Depends(get_db),
):
    """
    # 8. GET /attributes-map
    Usa la función sp_get_att_subattributes_name() que retorna JSON.

    Acceso: abierto a todos.
    """
    row = db.execute(
        text("SELECT sp_get_att_subattributes_name() AS data")
    ).mappings().first()

    return row["data"] if row and row["data"] is not None else []


# ---------- Points & Balances ----------

@router.get("/players/{player_id}/points/balance", tags=["points"], dependencies=[Depends(guard_player_access)])
def get_player_points_balance(
    player_id: int,
    db: Session = Depends(get_db),
):
    """
    # 9. GET /players/{player_id}/points/balance
    Lee desde v_points_balance.

    Acceso: admin, researcher, teacher, player.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_players,
              id_point_dimension,
              balance
            FROM v_points_balance
            WHERE id_players = :player_id
            """
        ),
        {"player_id": player_id},
    ).mappings().all()

    return list(rows)


@router.get("/players/{player_id}/attributes/points", tags=["points"], dependencies=[Depends(guard_player_access)])
def get_player_attribute_points(
    player_id: int,
    db: Session = Depends(get_db),
):
    """
    # 10. GET /players/{player_id}/attributes/points
    Usa la vista v_player_attribute_balance.

    Acceso: admin, researcher, teacher, player.
    """
    rows = db.execute(
        text(
            """
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
            WHERE id_players = :player_id
            """
        ),
        {"player_id": player_id},
    ).mappings().all()

    return list(rows)


@router.get("/points/ledger", tags=["points"], dependencies=[Depends(require_roles(ROLE_ALL))])
def get_points_ledger(
    player_id: Optional[int] = Query(None),
    videogame_id: Optional[int] = Query(None),
    source_type: Optional[str] = Query(None),
    from_ts: Optional[str] = Query(None, description="YYYY-MM-DD HH:MM:SS"),
    to_ts: Optional[str] = Query(None, description="YYYY-MM-DD HH:MM:SS"),
    db: Session = Depends(get_db),
):
    """
    # 11. GET /points/ledger
    Consulta filtrable del ledger de puntos.

    Acceso: admin, researcher, teacher, player.
    """
    base = """
        SELECT
          id_points_ledger,
          id_players,
          id_point_dimension,
          id_videogame,
          direction,
          amount,
          source_type,
          source_ref,
          payload,
          occurred_at,
          created_at,
          id_sensor_ingest_event
        FROM points_ledger
    """
    conditions = []
    params: dict = {}

    if player_id is not None:
        conditions.append("id_players = :player_id")
        params["player_id"] = player_id
    if videogame_id is not None:
        conditions.append("id_videogame = :videogame_id")
        params["videogame_id"] = videogame_id
    if source_type is not None:
        conditions.append("source_type = :source_type")
        params["source_type"] = source_type
    if from_ts is not None:
        conditions.append("occurred_at >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts is not None:
        conditions.append("occurred_at <= :to_ts")
        params["to_ts"] = to_ts

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    base += " ORDER BY occurred_at DESC LIMIT 500"  # cap defensivo

    rows = db.execute(text(base), params).mappings().all()
    return list(rows)


@router.post("/players/{player_id}/points/adjust", tags=["points"], dependencies=[Depends(require_roles(["admin", "researcher"]))])
def adjust_player_points(
    player_id: int,
    payload: PointsAdjustRequest,
    db: Session = Depends(get_db),
):
    """
    # 12. POST /players/{player_id}/points/adjust
    Inserta un ajuste manual en points_ledger (source_type='ADJUST').

    Acceso: admin, researcher.
    """
    from uuid import uuid4
    import json

    source_ref = f"ADJUST-{uuid4()}"

    try:
        db.execute(
            text(
                """
                INSERT INTO points_ledger (
                  id_players,
                  id_point_dimension,
                  id_videogame,
                  direction,
                  amount,
                  source_type,
                  source_ref,
                  payload
                ) VALUES (
                  :id_players,
                  :id_point_dimension,
                  :id_videogame,
                  :direction,
                  :amount,
                  'ADJUST',
                  :source_ref,
                  :payload
                )
                """
            ),
            {
                "id_players": player_id,
                "id_point_dimension": payload.point_dimension_id,
                "id_videogame": payload.videogame_id,
                "direction": payload.direction,
                "amount": payload.amount,
                "source_ref": source_ref,
                "payload": json.dumps({"reason": payload.reason}) if payload.reason else None,
            },
        )
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error adjusting points: {e}")

    return {"status": "ok", "source_ref": source_ref}
