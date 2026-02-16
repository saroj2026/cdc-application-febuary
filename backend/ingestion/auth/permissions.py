"""Role-Based Access Control (RBAC) for CDC Replication Application."""
from fastapi import HTTPException, Depends, status
from typing import List, Optional
from ingestion.auth.middleware import get_current_user
from ingestion.database.models_db import UserModel

# Role definitions - map to existing role_name values
# Current roles: 'user', 'operator', 'viewer', 'admin'
# GitHub repo uses: 'super_admin', 'org_admin', 'data_engineer', 'operator', 'viewer'
# We'll map both systems to work together

# Permission matrix for CDC operations
PERMISSIONS = {
    "create_user": ["admin", "super_admin", "org_admin"],
    "manage_roles": ["admin", "super_admin", "org_admin"],
    "create_connection": ["admin", "super_admin", "org_admin", "data_engineer", "operator"],
    "view_credentials": [],  # No one can view credentials
    "test_connection": ["admin", "super_admin", "org_admin", "data_engineer", "operator"],
    "create_pipeline": ["admin", "super_admin", "org_admin", "data_engineer", "operator"],
    "start_stop_pipeline": ["admin", "super_admin", "org_admin", "operator"],
    "pause_pipeline": ["admin", "super_admin", "org_admin", "operator"],
    "reset_offsets": ["admin", "super_admin", "org_admin"],  # Sensitive action
    "trigger_full_load": ["admin", "super_admin", "org_admin", "data_engineer"],
    "delete_pipeline": ["admin", "super_admin", "org_admin"],
    "view_metrics": ["admin", "super_admin", "org_admin", "data_engineer", "operator", "viewer", "user"],
    "view_audit_logs": ["admin", "super_admin", "org_admin"],
    "delete_user": ["admin", "super_admin", "org_admin"],
    "update_user": ["admin", "super_admin", "org_admin"],
}


def normalize_role(role_name: str) -> str:
    """Normalize role names to handle both old and new role systems."""
    role_lower = role_name.lower() if role_name else ""
    
    # Map old roles to new system
    role_mapping = {
        "admin": "admin",
        "super_admin": "admin",  # Treat super_admin as admin for now
        "org_admin": "admin",
        "data_engineer": "operator",
        "operator": "operator",
        "viewer": "viewer",
        "user": "user",
    }
    
    return role_mapping.get(role_lower, "viewer")


def require_role(*allowed_roles: str):
    """Dependency to require specific roles for an endpoint.
    
    Usage:
        @app.get("/admin-only")
        async def admin_endpoint(user: UserModel = Depends(require_role("admin", "super_admin"))):
            ...
    """
    def role_checker(current_user: UserModel = Depends(get_current_user)) -> UserModel:
        # Super users can do everything
        if current_user.is_superuser:
            return current_user
        
        # Normalize user's role
        user_role = normalize_role(current_user.role_name)
        
        # Check if user's role is in allowed roles
        allowed_roles_normalized = [normalize_role(role) for role in allowed_roles]
        if user_role in allowed_roles_normalized:
            return current_user
        
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied. Required roles: {list(allowed_roles)}"
        )
    
    return role_checker


def require_permission(permission: str):
    """Dependency to require a specific permission.
    
    Usage:
        @app.post("/pipelines")
        async def create_pipeline(user: UserModel = Depends(require_permission("create_pipeline"))):
            ...
    """
    def permission_checker(current_user: UserModel = Depends(get_current_user)) -> UserModel:
        # Super users have all permissions
        if current_user.is_superuser:
            return current_user
        
        # Get allowed roles for this permission
        allowed_roles = PERMISSIONS.get(permission, [])
        if not allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{permission}' is not available to any role"
            )
        
        # Normalize user's role
        user_role = normalize_role(current_user.role_name)
        
        # Check if user's role has this permission
        allowed_roles_normalized = [normalize_role(role) for role in allowed_roles]
        if user_role in allowed_roles_normalized:
            return current_user
        
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied. Permission required: {permission}"
        )
    
    return permission_checker


def require_admin(current_user: UserModel = Depends(get_current_user)) -> UserModel:
    """Dependency for admin-only endpoints."""
    if current_user.is_superuser or normalize_role(current_user.role_name) == "admin":
        return current_user
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Administrator privileges required"
    )


def get_user_permissions(user: UserModel) -> List[str]:
    """Get list of permissions for a user based on their role."""
    if user.is_superuser:
        return list(PERMISSIONS.keys())
    
    user_permissions = []
    user_role = normalize_role(user.role_name)
    
    for permission, allowed_roles in PERMISSIONS.items():
        allowed_roles_normalized = [normalize_role(role) for role in allowed_roles]
        if user_role in allowed_roles_normalized:
            user_permissions.append(permission)
    
    return user_permissions

