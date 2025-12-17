from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import get_db
from app.security import require_roles, guard_player_access     

router = APIRouter(prefix="/admin/points", tags=["admin-points"])


def _count_and_sample(
    db: Session,
    count_query: str,
    sample_query: str,
    params: dict,
    limit: int,
):
    """Helper genérico para ejecutar un COUNT + muestra limitada."""
    total = db.execute(text(count_query), params).scalar() or 0
    sample_rows = []
    if total > 0:
        sample_rows = (
            db.execute(
                text(sample_query + " LIMIT :limit"),
                {**params, "limit": limit},
            )
            .mappings()
            .all()
        )
    return int(total), [dict(r) for r in sample_rows]


@router.get("/consistency-check", dependencies=[Depends(require_roles(["admin", "researcher"]))])
def admin_points_consistency_check(
    limit_issues: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Ejecuta un conjunto de checks de consistencia sobre puntos.

    Invariantes revisadas:

    1) players_attributes vs ledger:
       - v_player_attribute_balance.diff_ledger_minus_snapshot == 0

    2) Movimientos inválidos en points_ledger:
       - amount <= 0

    3) Saldos negativos en v_points_balance:
       - balance < 0

    4) Coherencia de redemption_event:
       - redemption_event vinculado a points_ledger existente
       - points_ledger.direction = 'DEBIT' y source_type = 'REDEMPTION'

    Acceso: admin, researcher.
    """
    issues: Dict[str, Any] = {}

    # 1) Diferencias entre snapshot y ledger
    try:
        c1, s1 = _count_and_sample(
            db,
            """
            SELECT COUNT(*) AS cnt
            FROM v_player_attribute_balance
            WHERE diff_ledger_minus_snapshot <> 0
            """,
            """
            SELECT
              id_players,
              player_name,
              id_attributes,
              attribute_name,
              balance_ledger,
              snapshot_points,
              diff_ledger_minus_snapshot
            FROM v_player_attribute_balance
            WHERE diff_ledger_minus_snapshot <> 0
            """,
            {},
            limit_issues,
        )
        issues["players_attribute_mismatch"] = {
            "status": "ok" if c1 == 0 else "issues",
            "count": c1,
            "sample": s1,
        }
    except Exception as e:
        issues["players_attribute_mismatch"] = {
            "status": "error",
            "detail": str(e),
        }

    # 2) Movimientos inválidos en ledger (amount <= 0)
    try:
        c2, s2 = _count_and_sample(
            db,
            """
            SELECT COUNT(*) AS cnt
            FROM points_ledger
            WHERE amount <= 0
            """,
            """
            SELECT
              id_points_ledger,
              id_players,
              id_point_dimension,
              id_videogame,
              direction,
              amount,
              source_type,
              occurred_at
            FROM points_ledger
            WHERE amount <= 0
            ORDER BY occurred_at DESC
            """,
            {},
            limit_issues,
        )
        issues["invalid_ledger_amounts"] = {
            "status": "ok" if c2 == 0 else "issues",
            "count": c2,
            "sample": s2,
        }
    except Exception as e:
        issues["invalid_ledger_amounts"] = {
            "status": "error",
            "detail": str(e),
        }

    # 3) Saldos negativos por dimensión en v_points_balance
    try:
        c3, s3 = _count_and_sample(
            db,
            """
            SELECT COUNT(*) AS cnt
            FROM v_points_balance
            WHERE balance < 0
            """,
            """
            SELECT
              id_players,
              id_point_dimension,
              balance
            FROM v_points_balance
            WHERE balance < 0
            ORDER BY id_players, id_point_dimension
            """,
            {},
            limit_issues,
        )
        issues["negative_dimension_balances"] = {
            "status": "ok" if c3 == 0 else "issues",
            "count": c3,
            "sample": s3,
        }
    except Exception as e:
        issues["negative_dimension_balances"] = {
            "status": "error",
            "detail": str(e),
        }

    # 4a) Redemption_event sin points_ledger asociado
    try:
        c4a, s4a = _count_and_sample(
            db,
            """
            SELECT COUNT(*) AS cnt
            FROM redemption_event r
            LEFT JOIN points_ledger pl
              ON pl.id_points_ledger = r.id_points_ledger
            WHERE pl.id_points_ledger IS NULL
            """,
            """
            SELECT
              r.id_redemption_event,
              r.id_points_ledger,
              r.redeemed_points
            FROM redemption_event r
            LEFT JOIN points_ledger pl
              ON pl.id_points_ledger = r.id_points_ledger
            WHERE pl.id_points_ledger IS NULL
            ORDER BY r.id_redemption_event DESC
            """,
            {},
            limit_issues,
        )
        issues["orphan_redemptions_missing_ledger"] = {
            "status": "ok" if c4a == 0 else "issues",
            "count": c4a,
            "sample": s4a,
        }
    except Exception as e:
        issues["orphan_redemptions_missing_ledger"] = {
            "status": "error",
            "detail": str(e),
        }

    # 4b) Redemption_event con ledger de tipo incorrecto
    try:
        c4b, s4b = _count_and_sample(
            db,
            """
            SELECT COUNT(*) AS cnt
            FROM redemption_event r
            JOIN points_ledger pl
              ON pl.id_points_ledger = r.id_points_ledger
            WHERE NOT (pl.direction = 'DEBIT' AND pl.source_type = 'REDEMPTION')
            """,
            """
            SELECT
              r.id_redemption_event,
              r.id_points_ledger,
              pl.id_players,
              pl.id_videogame,
              pl.direction,
              pl.source_type,
              pl.amount,
              pl.occurred_at
            FROM redemption_event r
            JOIN points_ledger pl
              ON pl.id_points_ledger = r.id_points_ledger
            WHERE NOT (pl.direction = 'DEBIT' AND pl.source_type = 'REDEMPTION')
            ORDER BY r.id_redemption_event DESC
            """,
            {},
            limit_issues,
        )
        issues["redemption_ledger_mismatch"] = {
            "status": "ok" if c4b == 0 else "issues",
            "count": c4b,
            "sample": s4b,
        }
    except Exception as e:
        issues["redemption_ledger_mismatch"] = {
            "status": "error",
            "detail": str(e),
        }

    # Estado global
    global_status = "ok"
    for v in issues.values():
        if v.get("status") in ("issues", "error"):
            global_status = "issues_found"
            break

    return {
        "status": global_status,
        "limit_issues": limit_issues,
        "checks": issues,
    }
