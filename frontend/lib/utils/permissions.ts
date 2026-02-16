/**
 * Permission utilities for role-based access control
 * 
 * This file is prepared for future role assignment functionality.
 * Currently not in use but ready when role-based access control is implemented.
 */

interface User {
  id: number;
  email: string;
  full_name: string;
  is_active: boolean;
  is_superuser: boolean;
  roles: Array<{ id: number; name: string }>;
}

/**
 * Check if user is a viewer (read-only access)
 */
export function isViewer(user: User | null): boolean {
  if (!user) return false;
  
  // Check if user has viewer role
  const hasViewerRole = user.roles?.some(role => 
    role.name?.toLowerCase() === 'viewer'
  );
  
  // If user is superuser, they are not a viewer
  if (user.is_superuser) return false;
  
  return hasViewerRole;
}

/**
 * Check if user can create resources (not a viewer)
 */
export function canCreate(user: User | null): boolean {
  if (!user) return false;
  
  // Superusers can always create
  if (user.is_superuser) return true;
  
  // Viewers cannot create
  return !isViewer(user);
}

/**
 * Check if user can edit resources (not a viewer)
 */
export function canEdit(user: User | null): boolean {
  return canCreate(user);
}

/**
 * Check if user can delete resources (not a viewer)
 */
export function canDelete(user: User | null): boolean {
  return canCreate(user);
}

