from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
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

class RedeemRequest(BaseModel):
    modifiable_mechanic_videogame_id: int
    point_dimension_id: int
    amount: int
    metadata: Optional[dict] = None


class SessionStartRequest(BaseModel):
    started_at: Optional[datetime] = None
    session_metrics: Optional[dict] = None
    plugin_version: Optional[str] = None
    settings: Optional[dict] = None


class SessionEndRequest(BaseModel):
    ended_at: Optional[datetime] = None


# ---------- Helpers ----------

def _get_player_dimension_balance(
    db: Session,
    player_id: int,
    point_dimension_id: int,
) -> int:
    """
    Obtiene el balance actual de puntos para un jugador y una dimensión
    desde v_points_balance. Si no existe fila, se asume 0.

    Acceso: abierto a todos.
    """
    row = db.execute(
        text(
            """
            SELECT balance
            FROM v_points_balance
            WHERE id_players = :pid
              AND id_point_dimension = :pdid
            """
        ),
        {"pid": player_id, "pdid": point_dimension_id},
    ).mappings().first()

    if not row or row["balance"] is None:
        return 0

    return int(row["balance"])


# ---------- Videogames ----------

@router.get("", dependencies=[Depends(require_roles(ROLE_ALL))])
def list_videogames(
    db: Session = Depends(get_db),
):
    """
    # 13. GET /videogames

    Acceso: abierto a todos.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_videogame,
              name,
              genre,
              engine,
              developer,
              publisher,
              launch,
              version,
              type
            FROM videogame
            ORDER BY name
            """
        )
    ).mappings().all()
    return list(rows)


@router.get("/{game_id}", dependencies=[Depends(require_roles(ROLE_ALL))])
def get_videogame(
    game_id: int,
    db: Session = Depends(get_db),
):
    """
    # 14. GET /videogames/{game_id}

    Acceso: abierto a todos.
    """
    row = db.execute(
        text(
            """
            SELECT
              id_videogame,
              name,
              genre,
              engine,
              developer,
              publisher,
              launch,
              version,
              type
            FROM videogame
            WHERE id_videogame = :id
            """
        ),
        {"id": game_id},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Videogame not found")

    return dict(row)


@router.get("/{game_id}/mechanics", dependencies=[Depends(require_roles(ROLE_ALL))])
def get_videogame_mechanics(
    game_id: int,
    db: Session = Depends(get_db),
):
    """
    # 15. GET /videogames/{game_id}/mechanics
    Combina modifiable_mechanic_videogames + modifiable_mechanic.

    Acceso: abierto a todos.
    """
    rows = db.execute(
        text(
            """
            SELECT
              mmv.id_modifiable_mechanic_videogame,
              mmv.id_videogame,
              mmv.options,
              mm.id_modifiable_mechanic,
              mm.name AS mechanic_name,
              mm.description AS mechanic_description,
              mm.type AS mechanic_type
            FROM modifiable_mechanic_videogames mmv
            JOIN modifiable_mechanic mm
              ON mmv.id_modifiable_mechanic = mm.id_modifiable_mechanic
            WHERE mmv.id_videogame = :game_id
            """
        ),
        {"game_id": game_id},
    ).mappings().all()

    return list(rows)


# ---------- Redemptions ----------

@router.post("/{game_id}/players/{player_id}/redeem/preview", dependencies=[Depends(guard_player_access)])
def preview_redeem_mechanic(
    player_id: int,
    payload: RedeemRequest,
    db: Session = Depends(get_db),
):
    """
    Preview de canje:
    - No realiza modificaciones.
    - Indica si el jugador tiene saldo suficiente
      y cuál sería el saldo resultante.

    Acceso: abierto a todos.
    """
    current_balance = _get_player_dimension_balance(
        db=db,
        player_id=player_id,
        point_dimension_id=payload.point_dimension_id,
    )

    would_be_enough = current_balance >= payload.amount
    new_balance = current_balance - payload.amount if would_be_enough else current_balance

    return {
        "can_redeem": would_be_enough,
        "current_balance": current_balance,
        "required_amount": payload.amount,
        "resulting_balance": new_balance,
        "game_id": game_id,
        "player_id": player_id,
        "point_dimension_id": payload.point_dimension_id,
    }


@router.post("/{game_id}/players/{player_id}/redeem", dependencies=[Depends(guard_player_access)])
def redeem_mechanic(
    player_id: int,
    payload: RedeemRequest,
    db: Session = Depends(get_db),
):
    """
    Canje robusto:
      - Verifica saldo en v_points_balance.
      - Si no hay saldo suficiente -> 400 con detalle.
      - Si hay saldo, registra DEBIT en points_ledger (REDEMPTION)
        y crea el registro en redemption_event.

    Acceso: abierto a todos.
    """
    from uuid import uuid4
    import json

    # 1) Obtener saldo actual
    current_balance = _get_player_dimension_balance(
        db=db,
        player_id=player_id,
        point_dimension_id=payload.point_dimension_id,
    )

    if current_balance < payload.amount:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INSUFFICIENT_POINTS",
                "message": "Saldo insuficiente para realizar el canje.",
                "current_balance": current_balance,
                "required_amount": payload.amount,
            },
        )

    source_ref = f"REDEMPTION-{uuid4()}"

    try:
        # 2) Registrar débito en points_ledger
        result = db.execute(
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
                  'DEBIT',
                  :amount,
                  'REDEMPTION',
                  :source_ref,
                  :payload
                )
                """
            ),
            {
                "id_players": player_id,
                "id_point_dimension": payload.point_dimension_id,
                "id_videogame": game_id,
                "amount": payload.amount,
                "source_ref": source_ref,
                "payload": json.dumps(payload.metadata) if payload.metadata else None,
            },
        )
        pl_id = result.lastrowid

        # 3) Registrar en redemption_event
        db.execute(
            text(
                """
                INSERT INTO redemption_event (
                  id_points_ledger,
                  id_modifiable_mechanic_videogame,
                  redeemed_points
                ) VALUES (
                  :pl_id,
                  :mmv_id,
                  :points
                )
                """
            ),
            {
                "pl_id": pl_id,
                "mmv_id": payload.modifiable_mechanic_videogame_id,
                "points": payload.amount,
            },
        )

        db.commit()

        # 4) Estimar nuevo balance (puedes volver a consultar la vista si quieres exactitud)
        resulting_balance = current_balance - payload.amount

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error redeeming: {e}")

    return {
        "status": "redeemed",
        "points_ledger_id": pl_id,
        "source_ref": source_ref,
        "current_balance": current_balance,
        "redeemed_amount": payload.amount,
        "resulting_balance": resulting_balance,
        "game_id": game_id,
        "player_id": player_id,
        "point_dimension_id": payload.point_dimension_id,
    }


# ---------- Game Sessions ----------

def _get_or_create_player_videogame(
    db: Session,
    player_id: int,
    game_id: int,
    plugin_version: Optional[str],
    settings: Optional[dict],
    _: CurrentUser = Depends(require_roles(ROLE_ALL)),
) -> int:
    """
    Ayudante: obtiene id_player_videogame o lo crea.

    Acceso: abierto a todos.
    """
    import json

    row = db.execute(
        text(
            """
            SELECT id_player_videogame
            FROM player_videogame
            WHERE id_players = :pid AND id_videogame = :gid
            """
        ),
        {"pid": player_id, "gid": game_id},
    ).mappings().first()

    if row:
        return row["id_player_videogame"]

    result = db.execute(
        text(
            """
            INSERT INTO player_videogame (
              id_players,
              id_videogame,
              lsg_enabled,
              first_seen,
              plugin_version,
              settings
            ) VALUES (
              :pid,
              :gid,
              1,
              NOW(),
              :plugin_version,
              :settings
            )
            """
        ),
        {
            "pid": player_id,
            "gid": game_id,
            "plugin_version": plugin_version,
            "settings": json.dumps(settings) if settings else None,
        },
    )

    return result.lastrowid


@router.post("/{game_id}/players/{player_id}/sessions", dependencies=[Depends(guard_player_access)])
def start_session(
    player_id: int,
    payload: SessionStartRequest,
    db: Session = Depends(get_db),
):
    """
    # 17. POST /videogames/{game_id}/players/{player_id}/sessions
    Inicia sesión LSG (lsg_game_session).

    Acceso: abierto a todos.
    """
    import json

    started_at = payload.started_at or datetime.utcnow()

    try:
        pvg_id = _get_or_create_player_videogame(
            db=db,
            player_id=player_id,
            game_id=game_id,
            plugin_version=payload.plugin_version,
            settings=payload.settings,
        )

        result = db.execute(
            text(
                """
                INSERT INTO lsg_game_session (
                  id_player_videogame,
                  started_at,
                  session_metrics
                ) VALUES (
                  :pvg_id,
                  :started_at,
                  :session_metrics
                )
                """
            ),
            {
                "pvg_id": pvg_id,
                "started_at": started_at,
                "session_metrics": json.dumps(payload.session_metrics)
                if payload.session_metrics
                else None,
            },
        )
        db.commit()
        session_id = result.lastrowid
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error starting session: {e}")

    return {"status": "started", "id_session": session_id}


@router.patch("/{game_id}/players/{player_id}/sessions/{session_id}/end", dependencies=[Depends(guard_player_access)])
def end_session(
    player_id: int,
    session_id: int,
    payload: SessionEndRequest,
    db: Session = Depends(get_db),
):
    """
    # 18. PATCH /videogames/{game_id}/players/{player_id}/sessions/{session_id}/end
    Cierra la sesión de juego.

    Acceso: abierto a todos.
    """
    ended_at = payload.ended_at or datetime.utcnow()

    try:
        result = db.execute(
            text(
                """
                UPDATE lsg_game_session s
                JOIN player_videogame pvg
                  ON s.id_player_videogame = pvg.id_player_videogame
                SET s.ended_at = :ended_at
                WHERE s.id_lsg_game_session = :sid
                  AND pvg.id_players = :pid
                  AND pvg.id_videogame = :gid
                """
            ),
            {
                "ended_at": ended_at,
                "sid": session_id,
                "pid": player_id,
                "gid": game_id,
            },
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Session not found")
        db.commit()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error ending session: {e}")

    return {"status": "ended", "id_session": session_id}
