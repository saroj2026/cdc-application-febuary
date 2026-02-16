"""JWT Authentication middleware for FastAPI."""
import os
from typing import Optional
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import jwt, JWTError

from ingestion.database import get_db
from ingestion.database.models_db import UserModel

# JWT Configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", os.getenv("SECRET_KEY", "dev-secret-key-change-in-production"))
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

security = HTTPBearer(auto_error=False)  # Don't auto-raise error, allow optional auth


def verify_jwt_token(token: str) -> dict:
    """Verify JWT token and return payload."""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token verification failed: {str(e)}"
        )


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db)
) -> UserModel:
    """Get current user from JWT token. Raises 401 if not authenticated."""
    # Handle database connection failure
    if db is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database service is currently unavailable. Please try again later."
        )
    
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required"
        )
    
    token = credentials.credentials
    payload = verify_jwt_token(token)
    user_id = payload.get("sub")
    
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload"
        )
    
    # Ensure user_id is a string for comparison
    user_id_str = str(user_id) if user_id else None
    if not user_id_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user ID in token"
        )
    
    user = db.query(UserModel).filter(UserModel.id == user_id_str).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive"
        )
    
    return user


def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db)
) -> Optional[UserModel]:
    """Get current user from JWT token if present, otherwise return None.
    
    Use this for endpoints that work both with and without authentication.
    """
    # Handle database connection failure - return None for optional auth
    if db is None:
        return None
    
    if not credentials:
        return None
    
    try:
        token = credentials.credentials
        payload = verify_jwt_token(token)
        user_id = payload.get("sub")
        
        if not user_id:
            return None
        
        user_id_str = str(user_id) if user_id else None
        if not user_id_str:
            return None
        
        user = db.query(UserModel).filter(UserModel.id == user_id_str).first()
        if not user or not user.is_active:
            return None
        
        return user
    except Exception:
        # If any error occurs, return None (optional auth)
        return None

