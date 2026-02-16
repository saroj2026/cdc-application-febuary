"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { useAppSelector } from "@/lib/store/hooks"
import { canAccessPage, hasPermission } from "@/lib/store/slices/permissionSlice"
import { Loader2 } from "lucide-react"

interface ProtectedPageProps {
  children: React.ReactNode
  requiredPermission?: string
  path?: string
}

export function ProtectedPage({ children, requiredPermission, path }: ProtectedPageProps) {
  const router = useRouter()
  // Use specific selectors instead of entire state to avoid unnecessary rerenders
  const { user, isAuthenticated } = useAppSelector((state) => state.auth)
  const permissions = useAppSelector((state) => state.permissions)
  const [mounted, setMounted] = useState(false)
  const [hasAccess, setHasAccess] = useState<boolean | null>(null)

  // Handle client-side mounting to prevent hydration mismatch
  useEffect(() => {
    setMounted(true)
  }, [])

  useEffect(() => {
    if (!mounted) return
    
    // Wait for auth to be determined
    if (isAuthenticated === undefined) {
      setHasAccess(null)
      return
    }

    // If not authenticated, redirect to login
    if (!isAuthenticated || !user) {
      setHasAccess(false)
      router.push("/auth/login")
      return
    }

    // Super admin bypass - check first before any permission checks
    if (user?.is_superuser === true) {
      setHasAccess(true)
      return
    }

    // Check page access - create a minimal state object for permission checks
    let access = true
    if (path) {
      // Dashboard is accessible to all authenticated users
      if (path === "/dashboard") {
        setHasAccess(true)
        return
      }
      
      // Create a minimal state object with only what permission functions need
      const minimalState = { auth: { user, isAuthenticated }, permissions }
      access = canAccessPage(path)(minimalState)
      if (!access) {
        setHasAccess(false)
        router.push("/dashboard") // Redirect to dashboard if no access
        return
      }
    }

    // Check specific permission if provided
    if (requiredPermission) {
      // Create a minimal state object with only what permission functions need
      const minimalState = { auth: { user, isAuthenticated }, permissions }
      access = hasPermission(requiredPermission)(minimalState)
      if (!access) {
        setHasAccess(false)
        router.push("/dashboard") // Redirect to dashboard if no access
        return
      }
    }

    setHasAccess(access)
  }, [mounted, isAuthenticated, user, path, requiredPermission, router, permissions])

  // Show loading while checking auth (client-side only to prevent hydration mismatch)
  if (!mounted || isAuthenticated === undefined || hasAccess === null) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
      </div>
    )
  }

  // If not authenticated, don't render children (redirect will happen)
  if (!isAuthenticated || !user || hasAccess === false) {
    if (hasAccess === false) {
      return (
        <div className="flex items-center justify-center min-h-screen">
          <div className="text-center">
            <h1 className="text-2xl font-bold text-foreground mb-2">Access Denied</h1>
            <p className="text-foreground-muted">You don't have permission to access this page.</p>
          </div>
        </div>
      )
    }
    return null
  }

  return <>{children}</>
}

