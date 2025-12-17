from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, root_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import get_db
from app.security import require_roles, guard_player_access

router = APIRouter(prefix="/admin", tags=["admin-config"])


# =========================
# Pydantic models
# =========================

# --- Attributes ---

class AttributeBase(BaseModel):
    name: str
    description: Optional[str] = None
    data_type: Optional[str] = None  # según tu esquema (ej: "int", "float", "json")


class AttributeCreate(AttributeBase):
    pass


class AttributeUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    data_type: Optional[str] = None


# --- Subattributes ---

class SubattributeBase(BaseModel):
    attribute_id: int = Field(..., description="FK a attributes.id_attributes")
    name: str
    description: Optional[str] = None


class SubattributeCreate(SubattributeBase):
    pass


class SubattributeUpdate(BaseModel):
    attribute_id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None


# --- Point Dimension ---

class PointDimensionBase(BaseModel):
    id_attributes: Optional[int] = Field(
        None, description="FK a attributes.id_attributes (opcional)"
    )
    id_subattributes: Optional[int] = Field(
        None, description="FK a subattributes.id_subattributes (opcional)"
    )
    code: str
    name: str

    @root_validator(skip_on_failure=True)
    def validate_linked_entity(cls, values):
        """
        La tabla point_dimension tiene un CHECK que exige
        que se relacione a un atributo o a un subatributo, pero no ambos.
        """
        attr = values.get("id_attributes")
        sub = values.get("id_subattributes")

        if (attr is None and sub is None) or (attr is not None and sub is not None):
            raise ValueError(
                "Debe indicar exactamente uno de id_attributes o id_subattributes"
            )
        return values


class PointDimensionCreate(PointDimensionBase):
    pass


class PointDimensionUpdate(BaseModel):
    id_attributes: Optional[int] = None
    id_subattributes: Optional[int] = None
    code: Optional[str] = None
    name: Optional[str] = None

    @root_validator(skip_on_failure=True)
    def validate_linked_entity(cls, values):
        # Si el update toca alguno de los dos, validamos que queden en estado coherente.
        attr = values.get("id_attributes")
        sub = values.get("id_subattributes")
        if attr is not None and sub is not None:
            raise ValueError(
                "No puede establecer id_attributes e id_subattributes simultáneamente"
            )
        return values


# --- Modifiable Mechanic ---

class ModifiableMechanicBase(BaseModel):
    name: str
    description: Optional[str] = None
    type: Optional[str] = None  # por ejemplo: "SPEED", "XP_RATE", etc.


class ModifiableMechanicCreate(ModifiableMechanicBase):
    pass


class ModifiableMechanicUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None


# --- Modifiable Mechanic Videogames ---

class ModifiableMechanicVGBase(BaseModel):
    id_videogame: int
    id_modifiable_mechanic: int
    options: Optional[dict] = None  # la tabla usa JSON; se serializa en SQL


class ModifiableMechanicVGCreate(ModifiableMechanicVGBase):
    pass


class ModifiableMechanicVGUpdate(BaseModel):
    id_videogame: Optional[int] = None
    id_modifiable_mechanic: Optional[int] = None
    options: Optional[dict] = None


# =========================
# Helpers
# =========================

def _ensure_exists(db: Session, query: str, params: dict, not_found_msg: str):
    row = db.execute(text(query), params).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=not_found_msg)
    return row


# =========================
# Attributes CRUD
# =========================

@router.get(
    "/attributes",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_list_attributes(
    db: Session = Depends(get_db),
):
    """
    # 28. GET /attributes

    Acceso: admin, researcher.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_attributes,
              name,
              description,
              data_type,
              created_at,
              updated_at
            FROM attributes
            ORDER BY id_attributes
            """
        )
    ).mappings().all()
    return list(rows)


@router.get(
    "/attributes/{attribute_id}",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_get_attribute(
    attribute_id: int,
    db: Session = Depends(get_db),
):
    """
    # 29. GET /attributes/{attribute_id}

    Acceso: admin, researcher.
    """
    row = _ensure_exists(
        db,
        """
        SELECT
          id_attributes,
          name,
          description,
          data_type,
          created_at,
          updated_at
        FROM attributes
        WHERE id_attributes = :id
        """,
        {"id": attribute_id},
        "Attribute not found",
    )
    return dict(row)


@router.post(
    "/attributes",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_create_attribute(
    payload: AttributeCreate,
    db: Session = Depends(get_db),
):
    """
    # 30. POST /attributes

    Acceso: admin.
    """
    try:
        result = db.execute(
            text(
                """
                INSERT INTO attributes (name, description, data_type, created_at)
                VALUES (:name, :description, :data_type, :created_at)
                """
            ),
            {
                "name": payload.name,
                "description": payload.description,
                "data_type": payload.data_type,
                "created_at": datetime.now()
            },
        )
        new_id = result.lastrowid
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error creating attribute: {e}")

    return admin_get_attribute(new_id, db)


@router.put(
    "/attributes/{attribute_id}",
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_update_attribute(
    attribute_id: int,
    payload: AttributeUpdate,
    db: Session = Depends(get_db),
):
    """
    # 31. PUT /attributes/{attribute_id}

    Acceso: admin.
    """
    # Verificamos existencia
    _ensure_exists(
        db,
        "SELECT id_attributes FROM attributes WHERE id_attributes = :id",
        {"id": attribute_id},
        "Attribute not found",
    )

    fields = []
    params = {"id": attribute_id}

    if payload.name is not None:
        fields.append("name = :name")
        params["name"] = payload.name
    if payload.description is not None:
        fields.append("description = :description")
        params["description"] = payload.description
    if payload.data_type is not None:
        fields.append("data_type = :data_type")
        params["data_type"] = payload.data_type

    if not fields:
        return admin_get_attribute(attribute_id, db)

    fields.append("updated_at = :updated_at")
    params["updated_at"] = datetime.now()

    sql = "UPDATE attributes SET " + ", ".join(fields) + " WHERE id_attributes = :id"

    try:
        db.execute(text(sql), params)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error updating attribute: {e}")

    return admin_get_attribute(attribute_id, db)


@router.delete(
    "/attributes/{attribute_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_delete_attribute(
    attribute_id: int,
    db: Session = Depends(get_db),
):
    """
    # 32. DELETE /attributes/{attribute_id}

    Acceso: admin.
    """
    # Verificamos existencia
    _ensure_exists(
        db,
        "SELECT id_attributes FROM attributes WHERE id_attributes = :id",
        {"id": attribute_id},
        "Attribute not found",
    )

    try:
        db.execute(
            text("DELETE FROM attributes WHERE id_attributes = :id"),
            {"id": attribute_id},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        # conflicto con FKs, etc.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Error deleting attribute (probably in use): {e}",
        )


# =========================
# Subattributes CRUD
# =========================

@router.get(
    "/subattributes",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_list_subattributes(
    attribute_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """
    # 33. GET /subattributes

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          id_subattributes,
          attributes_id_attributes AS attribute_id,
          name,
          description,
          created_at,
          updated_at
        FROM subattributes
    """
    params = {}
    if attribute_id is not None:
        base += " WHERE attributes_id_attributes = :attr_id"
        params["attr_id"] = attribute_id

    rows = db.execute(text(base), params).mappings().all()
    return list(rows)


@router.get(
    "/subattributes/{sub_id}",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_get_subattribute(
    sub_id: int,
    db: Session = Depends(get_db),
):
    """
    # 34. GET /subattributes/{sub_id}

    Acceso: admin, researcher.
    """
    row = _ensure_exists(
        db,
        """
        SELECT
          id_subattributes,
          attributes_id_attributes AS attribute_id,
          name,
          description,
          created_at,
          updated_at
        FROM subattributes
        WHERE id_subattributes = :id
        """,
        {"id": sub_id},
        "Subattribute not found",
    )
    return dict(row)


@router.post(
    "/subattributes",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_create_subattribute(
    payload: SubattributeCreate,
    db: Session = Depends(get_db),
):
    """
    # 35. POST /subattributes

    Acceso: admin.
    """
    # Aseguramos que el atributo exista
    _ensure_exists(
        db,
        "SELECT id_attributes FROM attributes WHERE id_attributes = :id",
        {"id": payload.attribute_id},
        "Attribute not found",
    )

    try:
        result = db.execute(
            text(
                """
                INSERT INTO subattributes (
                  attributes_id_attributes,
                  name,
                  description,
                  created_at
                )
                VALUES (
                  :attr_id,
                  :name,
                  :description,
                  :created_at
                )
                """
            ),
            {
                "attr_id": payload.attribute_id,
                "name": payload.name,
                "description": payload.description,
                "created_at": datetime.now()
            },
        )
        new_id = result.lastrowid
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error creating subattribute: {e}")

    return admin_get_subattribute(new_id, db)


@router.put(
    "/subattributes/{sub_id}",
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_update_subattribute(
    sub_id: int,
    payload: SubattributeUpdate,
    db: Session = Depends(get_db),
):
    """
    # 36. PUT /subattributes/{sub_id}

    Acceso: admin.
    """
    _ensure_exists(
        db,
        "SELECT id_subattributes FROM subattributes WHERE id_subattributes = :id",
        {"id": sub_id},
        "Subattribute not found",
    )

    fields = []
    params = {"id": sub_id}

    if payload.attribute_id is not None:
        # validar existencia del atributo nuevo
        _ensure_exists(
            db,
            "SELECT id_attributes FROM attributes WHERE id_attributes = :id",
            {"id": payload.attribute_id},
            "Attribute not found",
        )
        fields.append("attributes_id_attributes = :attr_id")
        params["attr_id"] = payload.attribute_id

    if payload.name is not None:
        fields.append("name = :name")
        params["name"] = payload.name

    if payload.description is not None:
        fields.append("description = :description")
        params["description"] = payload.description

    if not fields:
        return admin_get_subattribute(sub_id, db)

    fields.append("updated_at = :updated_at")
    params["updated_at"] = datetime.now()

    sql = (
        "UPDATE subattributes SET " + ", ".join(fields) + " WHERE id_subattributes = :id"
    )

    try:
        db.execute(text(sql), params)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error updating subattribute: {e}")

    return admin_get_subattribute(sub_id, db)


@router.delete(
    "/subattributes/{sub_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_delete_subattribute(
    sub_id: int,
    db: Session = Depends(get_db),
):
    """
    # 37. DELETE /subattributes/{sub_id}

    Acceso: admin.
    """
    _ensure_exists(
        db,
        "SELECT id_subattributes FROM subattributes WHERE id_subattributes = :id",
        {"id": sub_id},
        "Subattribute not found",
    )

    try:
        db.execute(
            text("DELETE FROM subattributes WHERE id_subattributes = :id"),
            {"id": sub_id},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Error deleting subattribute (probably in use): {e}",
        )


# =========================
# Point Dimension CRUD
# =========================

@router.get(
    "/point-dimensions",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_list_point_dimensions(
    db: Session = Depends(get_db),
):
    """
    # 38. GET /point-dimensions

    Acceso: admin, researcher.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_point_dimension,
              id_attributes,
              id_subattributes,
              code,
              name
            FROM point_dimension
            ORDER BY id_point_dimension
            """
        )
    ).mappings().all()
    return list(rows)


@router.get(
    "/point-dimensions/{pd_id}",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_get_point_dimension(
    pd_id: int,
    db: Session = Depends(get_db),
):
    """
    # 39. GET /point-dimensions/{pd_id}

    Acceso: admin, researcher.
    """
    row = _ensure_exists(
        db,
        """
        SELECT
          id_point_dimension,
          id_attributes,
          id_subattributes,
          code,
          name
        FROM point_dimension
        WHERE id_point_dimension = :id
        """,
        {"id": pd_id},
        "Point dimension not found",
    )
    return dict(row)


@router.post(
    "/point-dimensions",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_create_point_dimension(
    payload: PointDimensionCreate,
    db: Session = Depends(get_db),
):
    """
    # 40. POST /point-dimensions

    Acceso: admin.
    """
    # Validamos FKs si se entregan
    if payload.id_attributes is not None:
        _ensure_exists(
            db,
            "SELECT id_attributes FROM attributes WHERE id_attributes = :id",
            {"id": payload.id_attributes},
            "Attribute not found",
        )

    if payload.id_subattributes is not None:
        _ensure_exists(
            db,
            "SELECT id_subattributes FROM subattributes WHERE id_subattributes = :id",
            {"id": payload.id_subattributes},
            "Subattribute not found",
        )

    try:
        result = db.execute(
            text(
                """
                INSERT INTO point_dimension (
                  id_attributes,
                  id_subattributes,
                  code,
                  name
                ) VALUES (
                  :id_attributes,
                  :id_subattributes,
                  :code,
                  :name
                )
                """
            ),
            {
                "id_attributes": payload.id_attributes,
                "id_subattributes": payload.id_subattributes,
                "code": payload.code,
                "name": payload.name,
            },
        )
        new_id = result.lastrowid
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Error creating point dimension: {e}"
        )

    return admin_get_point_dimension(new_id, db)


@router.put(
    "/point-dimensions/{pd_id}",
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_update_point_dimension(
    pd_id: int,
    payload: PointDimensionUpdate,
    db: Session = Depends(get_db),
):
    """
    # 41. PUT /point-dimensions/{pd_id}

    Acceso: admin.
    """
    _ensure_exists(
        db,
        "SELECT id_point_dimension FROM point_dimension WHERE id_point_dimension = :id",
        {"id": pd_id},
        "Point dimension not found",
    )

    fields = []
    params = {"id": pd_id}

    if payload.id_attributes is not None:
        _ensure_exists(
            db,
            "SELECT id_attributes FROM attributes WHERE id_attributes = :id",
            {"id": payload.id_attributes},
            "Attribute not found",
        )
        fields.append("id_attributes = :id_attributes")
        params["id_attributes"] = payload.id_attributes
        # si cambiamos a atributo, anulamos subatributo
        fields.append("id_subattributes = NULL")

    if payload.id_subattributes is not None:
        _ensure_exists(
            db,
            "SELECT id_subattributes FROM subattributes WHERE id_subattributes = :id",
            {"id": payload.id_subattributes},
            "Subattribute not found",
        )
        fields.append("id_subattributes = :id_subattributes")
        params["id_subattributes"] = payload.id_subattributes
        # si cambiamos a subatributo, anulamos atributo
        fields.append("id_attributes = NULL")

    if payload.code is not None:
        fields.append("code = :code")
        params["code"] = payload.code

    if payload.name is not None:
        fields.append("name = :name")
        params["name"] = payload.name

    if not fields:
        return admin_get_point_dimension(pd_id, db)

    sql = (
        "UPDATE point_dimension SET "
        + ", ".join(fields)
        + " WHERE id_point_dimension = :id"
    )

    try:
        db.execute(text(sql), params)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Error updating point dimension: {e}"
        )

    return admin_get_point_dimension(pd_id, db)


@router.delete(
    "/point-dimensions/{pd_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_delete_point_dimension(
    pd_id: int,
    db: Session = Depends(get_db),
):
    """
    # 42. DELETE /point-dimensions/{pd_id}

    Acceso: admin.
    """
    _ensure_exists(
        db,
        "SELECT id_point_dimension FROM point_dimension WHERE id_point_dimension = :id",
        {"id": pd_id},
        "Point dimension not found",
    )

    try:
        db.execute(
            text("DELETE FROM point_dimension WHERE id_point_dimension = :id"),
            {"id": pd_id},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Error deleting point dimension (probably in use): {e}",
        )


# =========================
# Modifiable Mechanic CRUD
# =========================

@router.get(
    "/modifiable-mechanics",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_list_mod_mechanics(
    db: Session = Depends(get_db),
):
    """
    # 43. GET /modifiable-mechanics

    Acceso: admin, researcher.
    """
    rows = db.execute(
        text(
            """
            SELECT
              id_modifiable_mechanic,
              name,
              description,
              type
            FROM modifiable_mechanic
            ORDER BY id_modifiable_mechanic
            """
        )
    ).mappings().all()
    return list(rows)


@router.get(
    "/modifiable-mechanics/{mm_id}",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_get_mod_mechanic(
    mm_id: int,
    db: Session = Depends(get_db),
):
    """
    # 44. GET /modifiable-mechanics/{mm_id}

    Acceso: admin, researcher.
    """
    row = _ensure_exists(
        db,
        """
        SELECT
          id_modifiable_mechanic,
          name,
          description,
          type
        FROM modifiable_mechanic
        WHERE id_modifiable_mechanic = :id
        """,
        {"id": mm_id},
        "Modifiable mechanic not found",
    )
    return dict(row)


@router.post(
    "/modifiable-mechanics",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_create_mod_mechanic(
    payload: ModifiableMechanicCreate,
    db: Session = Depends(get_db),
):
    """
    # 45. POST /modifiable-mechanics

    Acceso: admin.
    """
    try:
        result = db.execute(
            text(
                """
                INSERT INTO modifiable_mechanic (name, description, type, created_at)
                VALUES (:name, :description, :type, :created_at)
                """
            ),
            {
                "name": payload.name,
                "description": payload.description,
                "type": payload.type,
                "created_at": datetime.now()
            },
        )
        new_id = result.lastrowid
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Error creating modifiable mechanic: {e}"
        )

    return admin_get_mod_mechanic(new_id, db)


@router.put(
    "/modifiable-mechanics/{mm_id}",
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_update_mod_mechanic(
    mm_id: int,
    payload: ModifiableMechanicUpdate,
    db: Session = Depends(get_db),
):
    """
    # 46. PUT /modifiable-mechanics/{mm_id}

    Acceso: admin.
    """
    _ensure_exists(
        db,
        "SELECT id_modifiable_mechanic FROM modifiable_mechanic WHERE id_modifiable_mechanic = :id",
        {"id": mm_id},
        "Modifiable mechanic not found",
    )

    fields = []
    params = {"id": mm_id}

    if payload.name is not None:
        fields.append("name = :name")
        params["name"] = payload.name
    if payload.description is not None:
        fields.append("description = :description")
        params["description"] = payload.description
    if payload.type is not None:
        fields.append("type = :type")
        params["type"] = payload.type

    if not fields:
        return admin_get_mod_mechanic(mm_id, db)

    fields.append("updated_at = :updated_at")
    params["updated_at"] = datetime.now()

    sql = (
        "UPDATE modifiable_mechanic SET "
        + ", ".join(fields)
        + " WHERE id_modifiable_mechanic = :id"
    )

    try:
        db.execute(text(sql), params)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Error updating modifiable mechanic: {e}"
        )

    return admin_get_mod_mechanic(mm_id, db)


@router.delete(
    "/modifiable-mechanics/{mm_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_delete_mod_mechanic(
    mm_id: int,
    db: Session = Depends(get_db),
):
    """
    # 47. DELETE /modifiable-mechanics/{mm_id}

    Acceso: admin.
    """
    _ensure_exists(
        db,
        "SELECT id_modifiable_mechanic FROM modifiable_mechanic WHERE id_modifiable_mechanic = :id",
        {"id": mm_id},
        "Modifiable mechanic not found",
    )

    try:
        db.execute(
            text(
                "DELETE FROM modifiable_mechanic WHERE id_modifiable_mechanic = :id"
            ),
            {"id": mm_id},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Error deleting modifiable mechanic (probably in use): {e}",
        )


# =========================
# Modifiable Mechanic Videogames CRUD
# =========================

@router.get(
    "/modifiable-mechanics-videogames",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_list_mod_mech_vg(
    videogame_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    """
    # 48. GET /modifiable-mechanics-videogames

    Acceso: admin, researcher.
    """
    base = """
        SELECT
          mmv.id_modifiable_mechanic_videogame,
          mmv.id_videogame,
          vg.name AS videogame_name,
          mmv.id_modifiable_mechanic,
          mm.name AS mechanic_name,
          mmv.options
        FROM modifiable_mechanic_videogames mmv
        JOIN videogame vg ON vg.id_videogame = mmv.id_videogame
        JOIN modifiable_mechanic mm ON mm.id_modifiable_mechanic = mmv.id_modifiable_mechanic
    """
    params = {}
    if videogame_id is not None:
        base += " WHERE mmv.id_videogame = :vgid"
        params["vgid"] = videogame_id

    rows = db.execute(text(base), params).mappings().all()
    return list(rows)


@router.get(
    "/modifiable-mechanics-videogames/{mmv_id}",
    dependencies=[Depends(require_roles(["admin", "researcher"]))],
)
def admin_get_mod_mech_vg(
    mmv_id: int,
    db: Session = Depends(get_db),
):
    """
    # 49. GET /modifiable-mechanics-videogames/{mmv_id}

    Acceso: admin, researcher.
    """
    row = _ensure_exists(
        db,
        """
        SELECT
          mmv.id_modifiable_mechanic_videogame,
          mmv.id_videogame,
          vg.name AS videogame_name,
          mmv.id_modifiable_mechanic,
          mm.name AS mechanic_name,
          mmv.options
        FROM modifiable_mechanic_videogames mmv
        JOIN videogame vg ON vg.id_videogame = mmv.id_videogame
        JOIN modifiable_mechanic mm ON mm.id_modifiable_mechanic = mmv.id_modifiable_mechanic
        WHERE mmv.id_modifiable_mechanic_videogame = :id
        """,
        {"id": mmv_id},
        "Modifiable mechanic videogame config not found",
    )
    return dict(row)


@router.post(
    "/modifiable-mechanics-videogames",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_create_mod_mech_vg(
    payload: ModifiableMechanicVGCreate,
    db: Session = Depends(get_db),
):
    """
    # 50. POST /modifiable-mechanics-videogames

    Acceso: admin.
    """
    import json

    # Validamos FKs
    _ensure_exists(
        db,
        "SELECT id_videogame FROM videogame WHERE id_videogame = :id",
        {"id": payload.id_videogame},
        "Videogame not found",
    )

    _ensure_exists(
        db,
        "SELECT id_modifiable_mechanic FROM modifiable_mechanic WHERE id_modifiable_mechanic = :id",
        {"id": payload.id_modifiable_mechanic},
        "Modifiable mechanic not found",
    )

    try:
        result = db.execute(
            text(
                """
                INSERT INTO modifiable_mechanic_videogames (
                  id_videogame,
                  id_modifiable_mechanic,
                  options
                ) VALUES (
                  :id_videogame,
                  :id_modifiable_mechanic,
                  :options
                )
                """
            ),
            {
                "id_videogame": payload.id_videogame,
                "id_modifiable_mechanic": payload.id_modifiable_mechanic,
                "options": json.dumps(payload.options) if payload.options else None,
            },
        )
        new_id = result.lastrowid
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Error creating modifiable mechanic videogame config: {e}",
        )

    return admin_get_mod_mech_vg(new_id, db)


@router.put(
    "/modifiable-mechanics-videogames/{mmv_id}",
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_update_mod_mech_vg(
    mmv_id: int,
    payload: ModifiableMechanicVGUpdate,
    db: Session = Depends(get_db),
):
    """
    # 51. PUT /modifiable-mechanics-videogames/{mmv_id}

    Acceso: admin.
    """
    import json

    _ensure_exists(
        db,
        "SELECT id_modifiable_mechanic_videogame FROM modifiable_mechanic_videogames WHERE id_modifiable_mechanic_videogame = :id",
        {"id": mmv_id},
        "Modifiable mechanic videogame config not found",
    )

    fields = []
    params = {"id": mmv_id}

    if payload.id_videogame is not None:
        _ensure_exists(
            db,
            "SELECT id_videogame FROM videogame WHERE id_videogame = :id",
            {"id": payload.id_videogame},
            "Videogame not found",
        )
        fields.append("id_videogame = :id_videogame")
        params["id_videogame"] = payload.id_videogame

    if payload.id_modifiable_mechanic is not None:
        _ensure_exists(
            db,
            "SELECT id_modifiable_mechanic FROM modifiable_mechanic WHERE id_modifiable_mechanic = :id",
            {"id": payload.id_modifiable_mechanic},
            "Modifiable mechanic not found",
        )
        fields.append("id_modifiable_mechanic = :id_modifiable_mechanic")
        params["id_modifiable_mechanic"] = payload.id_modifiable_mechanic

    if payload.options is not None:
        fields.append("options = :options")
        params["options"] = json.dumps(payload.options)

    if not fields:
        return admin_get_mod_mech_vg(mmv_id, db)

    sql = (
        "UPDATE modifiable_mechanic_videogames SET "
        + ", ".join(fields)
        + " WHERE id_modifiable_mechanic_videogame = :id"
    )

    try:
        db.execute(text(sql), params)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Error updating modifiable mechanic videogame config: {e}",
        )

    return admin_get_mod_mech_vg(mmv_id, db)


@router.delete(
    "/modifiable-mechanics-videogames/{mmv_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_roles(["admin"]))],
)
def admin_delete_mod_mech_vg(
    mmv_id: int,
    db: Session = Depends(get_db),
):
    """
    # 52. DELETE /modifiable-mechanics-videogames/{mmv_id}

    Acceso: admin.
    """
    _ensure_exists(
        db,
        "SELECT id_modifiable_mechanic_videogame FROM modifiable_mechanic_videogames WHERE id_modifiable_mechanic_videogame = :id",
        {"id": mmv_id},
        "Modifiable mechanic videogame config not found",
    )

    try:
        db.execute(
            text(
                "DELETE FROM modifiable_mechanic_videogames WHERE id_modifiable_mechanic_videogame = :id"
            ),
            {"id": mmv_id},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Error deleting modifiable mechanic videogame config "
                "(probably referenced in redemption_event): "
                f"{e}"
            ),
        )
