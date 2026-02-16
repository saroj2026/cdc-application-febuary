import { createSlice, PayloadAction } from '@reduxjs/toolkit'

interface PermissionState {
  permissions: string[]
  isLoading: boolean
  error: string | null
}

const initialState: PermissionState = {
  permissions: [],
  isLoading: false,
  error: null,
}

const permissionSlice = createSlice({
  name: 'permissions',
  initialState,
  reducers: {
    setPermissions: (state, action: PayloadAction<string[]>) => {
      state.permissions = action.payload
    },
    clearPermissions: (state) => {
      state.permissions = []
    },
  },
})

export const { setPermissions, clearPermissions } = permissionSlice.actions
export default permissionSlice.reducer

// Permission matrix (matches backend)
const PERMISSION_MATRIX: Record<string, string[]> = {
  create_user: ['super_admin', 'org_admin'],
  manage_roles: ['super_admin', 'org_admin'],
  create_connection: ['super_admin', 'org_admin', 'data_engineer'],
  view_credentials: [], // No one can view credentials
  test_connection: ['super_admin', 'org_admin', 'data_engineer'],
  create_pipeline: ['super_admin', 'org_admin', 'data_engineer'],
  start_stop_pipeline: ['super_admin', 'org_admin', 'operator'],
  pause_pipeline: ['super_admin', 'org_admin', 'operator'],
  reset_offsets: ['super_admin', 'org_admin'],
  trigger_full_load: ['super_admin', 'org_admin', 'data_engineer'],
  delete_pipeline: ['super_admin', 'org_admin'],
  view_metrics: ['super_admin', 'org_admin', 'data_engineer', 'operator', 'viewer'],
  view_audit_logs: ['super_admin', 'org_admin'],
}

// Page access permissions - maps routes to required permissions
export const PAGE_PERMISSIONS: Record<string, string[]> = {
  '/dashboard': ['view_metrics'], // All authenticated users can view dashboard
  '/monitoring': ['view_metrics'],
  '/connections': ['create_connection'], // Need create permission to view connections page
  '/pipelines': ['create_pipeline'], // Need create permission to view pipelines page
  '/analytics': ['view_metrics'],
  '/errors': ['view_metrics'], // All can view errors
  '/governance': ['view_metrics'], // All can view governance
  '/users': ['create_user'], // Only admins can manage users
  '/settings': ['manage_roles'], // Only admins can access settings
}

// Permission check helpers
export const hasPermission = (permission: string) => (state: any) => {
  // Check if user has explicit permission
  if (state.permissions?.permissions?.includes(permission)) {
    return true
  }
  
  // Check user role from auth state
  const user = state.auth?.user
  if (!user) return false
  
  // Super admin has all permissions
  if (user.is_superuser || user.role_name === 'super_admin') {
    return true
  }
  
  // Check role-based permissions
  const allowedRoles = PERMISSION_MATRIX[permission] || []
  const userRole = user.role_name?.toLowerCase()
  
  return allowedRoles.includes(userRole)
}

export const hasAnyPermission = (permissions: string[]) => (state: { permissions: PermissionState }) => {
  return permissions.some(perm => state.permissions.permissions.includes(perm))
}

export const hasAllPermissions = (permissions: string[]) => (state: { permissions: PermissionState }) => {
  return permissions.every(perm => state.permissions.permissions.includes(perm))
}

// Check if user can access a page
export const canAccessPage = (path: string) => (state: any) => {
  const user = state.auth?.user
  if (!user) return false
  
  // Super admin has access to all pages - check multiple ways
  const isSuperAdmin = user.is_superuser === true || 
                      user.is_superuser === 'true' ||
                      String(user.is_superuser).toLowerCase() === 'true' ||
                      user.role_name === 'super_admin' ||
                      user.role_name === 'admin'
  
  if (isSuperAdmin) {
    return true
  }
  
  // Dashboard is accessible to all authenticated users
  if (path === '/dashboard') {
    return true
  }
  
  // Get required permissions for the page
  const requiredPermissions = PAGE_PERMISSIONS[path] || []
  
  // If no permissions required, allow access
  if (requiredPermissions.length === 0) {
    return true
  }
  
  // Check if user has any of the required permissions
  return requiredPermissions.some(permission => hasPermission(permission)(state))
}

