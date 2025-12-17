import os
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel


# === Configuración desde variables de entorno ===

AUTH_JWT_SECRET = os.getenv("AUTH_JWT_SECRET", "dev-secret-change-me")
AUTH_JWT_ALGORITHM = os.getenv("AUTH_JWT_ALGORITHM", "HS256")
AUTH_JWT_ISSUER = os.getenv("AUTH_JWT_ISSUER")  # opcional
AUTH_JWT_AUDIENCE = os.getenv("AUTH_JWT_AUDIENCE")  # opcional

# true, cuando no se necesita JWT, ya que se trabajará en local; te dará un usuario de admin ficticio
AUTH_DISABLED = os.getenv("AUTH_DISABLED", "false").lower() == "true" 

# false, cuando se necesita JWT, ya que se trabajará en producción (DIINF). Además hay que configurar el JWT
#AUTH_DISABLED = os.getenv("AUTH_DISABLED", "false").lower() == "false" 

AUTH_OPEN_ALL = os.getenv("AUTH_OPEN_ALL", "true").lower() == "true"

bearer_scheme = HTTPBearer(auto_error=False)

ROLE_ALL = ["player", "teacher", "researcher", "admin"]

class CurrentUser(BaseModel):
    """
    Representa al sujeto autenticado según el JWT de LSG-auth.
    Ajusta este modelo a las claims que realmente emite tu auth.
    """
    sub: str
    role: str = "player"
    player_id: Optional[int] = None
    email: Optional[str] = None
    type: str = "user"  # user | service
    raw_claims: Dict[str, Any]


def _decode_token(token: str) -> Dict[str, Any]:
    """
    Decodifica y valida el JWT.
    Verifica algoritmo y, opcionalmente, iss/aud.
    """
    options = {"verify_aud": AUTH_JWT_AUDIENCE is not None}
    try:
        payload = jwt.decode(
            token,
            AUTH_JWT_SECRET,
            algorithms=[AUTH_JWT_ALGORITHM],
            issuer=AUTH_JWT_ISSUER,
            audience=AUTH_JWT_AUDIENCE,
            options=options,
        )
        return payload
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid authentication credentials: {e}",
        )


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> CurrentUser:
    """
    Devuelve el usuario actual a partir del header Authorization: Bearer <token>.
    Si AUTH_DISABLED=true, devuelve un usuario admin ficticio (solo para desarrollo).
    """
    if AUTH_DISABLED:
        return CurrentUser(
            sub="dev-admin",
            role="admin",
            player_id=None,
            email="dev@example.com",
            type="service",
            raw_claims={},
        )

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    token = credentials.credentials
    payload = _decode_token(token)

    # Ajusta estas claves a las claims reales de LSG-auth
    sub = str(payload.get("sub", ""))
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token: 'sub' missing",
        )

    role = payload.get("role", "player")
    if role not in ROLE_ALL:
        role = "player"

    # Normalizamos player_id a int + fallback desde sub
    player_id_raw = payload.get("player_id") or payload.get("id_players")
    player_id: Optional[int] = None

    if player_id_raw is not None:
       try:
           player_id = int(player_id_raw)
       except (TypeError, ValueError):
           raise HTTPException(
               status_code=status.HTTP_401_UNAUTHORIZED,
               detail="Invalid token: 'player_id' must be an integer",
        )
    else:
       # Fallback: tokens antiguos que sólo traen sub
       if sub.isdigit():
           player_id = int(sub)

    email = payload.get("email")
    token_type = payload.get("type", "user")

    return CurrentUser(
        sub=sub,
        role=role,
        player_id=player_id,
        email=email,
        type=token_type,
        raw_claims=payload,
    )


def require_roles(allowed_roles: List[str]):
    def dependency(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        # Modo "open" (útil para Fase 1)
        if AUTH_OPEN_ALL:
            return current_user

        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions (required roles: {allowed_roles})",
            )
        return current_user

    return dependency


# Atajos de roles típicos
require_admin = require_roles(["admin"])
require_admin_or_researcher = require_roles(["admin", "researcher"])
require_admin_or_researcher_or_teacher = require_roles(["admin", "researcher", "teacher"])
require_player_or_higher = require_roles(ROLE_ALL)


def guard_player_access(
    player_id: int,
    current_user: CurrentUser = Depends(get_current_user),
) -> CurrentUser:
    # Modo "open" (Fase 1)
    if AUTH_OPEN_ALL:
        return current_user

    # Roles elevados pueden acceder a cualquier player
    if current_user.role in ("admin", "researcher", "teacher"):
        return current_user

    # Player solo puede acceder a sí mismo
    if current_user.role == "player":
        if current_user.player_id is not None and current_user.player_id == player_id:
            return current_user

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Not allowed to access this player's data",
    )

