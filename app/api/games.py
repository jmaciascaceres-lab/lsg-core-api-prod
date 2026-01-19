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
    CurrentUser,
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

def _get_player_game_dimension_balance(
    db: Session,
    player_id: int,
    game_id: int,
    point_dimension_id: int,
) -> int:
    """
    Balance de puntos por jugador + juego + dimensión.

    Se calcula desde points_ledger para evitar balances cross-game.
    """
    row = db.execute(
        text(
            """
            SELECT COALESCE(SUM(
              CASE
                WHEN direction = 'CREDIT' THEN amount
                WHEN direction = 'DEBIT'  THEN -amount
                ELSE 0
              END
            ), 0) AS balance
            FROM points_ledger
            WHERE id_players = :pid
              AND id_videogame = :gid
              AND id_point_dimension = :pdid
            """
        ),
        {"pid": player_id, "gid": game_id, "pdid": point_dimension_id},
    ).mappings().first()

    return int(row["balance"]) if row and row["balance"] is not None else 0


def _assert_mmv_exists_for_game(db: Session, game_id: int, mmv_id: int) -> None:
    """
    Valida que el id_modifiable_mechanic_videogame exista y pertenezca al juego del path.
    Evita IntegrityError por FK (redemption_event.fk_re_mmv).
    """
    row = db.execute(
        text(
            """
            SELECT 1
            FROM modifiable_mechanic_videogames
            WHERE id_modifiable_mechanic_videogame = :mmv_id
              AND id_videogame = :game_id
            """
        ),
        {"mmv_id": mmv_id, "game_id": game_id},
    ).mappings().first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "MODIFIABLE_MECHANIC_VIDEOGAME_NOT_FOUND",
                "message": "No existe modifiable_mechanic_videogame_id para el game_id indicado.",
                "game_id": game_id,
                "modifiable_mechanic_videogame_id": mmv_id,
            },
        )


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


# ---------- Models ----------

from pydantic import BaseModel
from typing import Optional


class VideogameCreateRequest(BaseModel):
    # Opcional: recomendado NO enviarlo y dejar que MySQL asigne el correlativo
    id_videogame: Optional[int] = None

    name: str
    genre: Optional[str] = None
    engine: Optional[str] = None
    developer: Optional[str] = None
    publisher: Optional[str] = None
    launch: Optional[str] = None
    version: Optional[str] = None
    type: Optional[str] = None


# ---------- Videogames ----------

@router.post("", status_code=201, dependencies=[Depends(require_roles(ROLE_ALL))])
def create_videogame(
    payload: VideogameCreateRequest,
    db: Session = Depends(get_db),
):
    """
    POST /videogames
    Crea un nuevo videojuego en la tabla `videogame`.

    Nota: por defecto conviene omitir id_videogame para que sea AUTO_INCREMENT.

    Acceso: abierto a todos.
    """
    # 1) Validación mínima: evitar duplicados por nombre
    exists = db.execute(
        text(
            """
            SELECT id_videogame
            FROM videogame
            WHERE LOWER(name) = LOWER(:name)
            LIMIT 1
            """
        ),
        {"name": payload.name},
    ).mappings().first()

    if exists:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "VIDEOGAME_ALREADY_EXISTS",
                "message": "Ya existe un videojuego con ese nombre.",
                "id_videogame": exists["id_videogame"],
                "name": payload.name,
            },
        )

    params = {
        "id_videogame": payload.id_videogame,
        "name": payload.name,
        "genre": payload.genre,
        "engine": payload.engine,
        "developer": payload.developer,
        "publisher": payload.publisher,
        "launch": payload.launch,
        "version": payload.version,
        "type": payload.type,
    }

    try:
        # 2) Insert: con o sin id_videogame (si lo mandas explícito)
        if payload.id_videogame is None:
            result = db.execute(
                text(
                    """
                    INSERT INTO videogame (
                      name, genre, engine, developer, publisher, launch, version, type
                    ) VALUES (
                      :name, :genre, :engine, :developer, :publisher, :launch, :version, :type
                    )
                    """
                ),
                params,
            )
            new_id = int(result.lastrowid)
        else:
            db.execute(
                text(
                    """
                    INSERT INTO videogame (
                      id_videogame, name, genre, engine, developer, publisher, launch, version, type
                    ) VALUES (
                      :id_videogame, :name, :genre, :engine, :developer, :publisher, :launch, :version, :type
                    )
                    """
                ),
                params,
            )
            new_id = int(payload.id_videogame)

        db.commit()

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error creating videogame: {e}")

    # 3) Retornar el registro creado
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
        {"id": new_id},
    ).mappings().first()

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
    game_id: int,
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
    _assert_mmv_exists_for_game(db, game_id, payload.modifiable_mechanic_videogame_id)

    current_balance = _get_player_game_dimension_balance(
        db=db,
        player_id=player_id,
        game_id=game_id,
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
        "modifiable_mechanic_videogame_id": payload.modifiable_mechanic_videogame_id,
    }


@router.post("/{game_id}/players/{player_id}/redeem", dependencies=[Depends(guard_player_access)])
def redeem_mechanic(
    game_id: int,
    player_id: int,
    payload: RedeemRequest,
    db: Session = Depends(get_db),
):
    """
    Canje robusto:
      - Verifica saldo por juego+dimensión (points_ledger).
      - Si no hay saldo suficiente -> 400 con detalle.
      - Si hay saldo, registra DEBIT en points_ledger (REDEMPTION)
        y crea el registro en redemption_event.

    Acceso: abierto a todos.
    """
    from uuid import uuid4
    import json

    _assert_mmv_exists_for_game(db, game_id, payload.modifiable_mechanic_videogame_id)

    # 1) Obtener saldo actual (por juego)
    current_balance = _get_player_game_dimension_balance(
        db=db,
        player_id=player_id,
        game_id=game_id,
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
                "game_id": game_id,
                "player_id": player_id,
                "point_dimension_id": payload.point_dimension_id,
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
        "modifiable_mechanic_videogame_id": payload.modifiable_mechanic_videogame_id,
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
    game_id: int,
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
    game_id: int,
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
