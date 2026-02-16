"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { useAppSelector } from "@/lib/store/hooks"
import { DashboardOverview } from "@/components/dashboard/overview"
import { Loader2, LayoutDashboard } from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"
import { ProtectedPage } from "@/components/auth/ProtectedPage"

export default function DashboardPage() {
  const router = useRouter()
  const { isAuthenticated, isLoading } = useAppSelector((state) => state.auth)
  const [mounted, setMounted] = useState(false)

  // Ensure component is mounted on client before rendering
  useEffect(() => {
    setMounted(true)
  }, [])

  useEffect(() => {
    if (mounted && !isLoading && !isAuthenticated) {
      router.push("/auth/login")
    }
  }, [mounted, isAuthenticated, isLoading, router])

  // Show loading state until mounted to prevent hydration mismatch
  if (!mounted || isLoading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
        <span className="ml-2 text-foreground-muted">Loading...</span>
      </div>
    )
  }

  if (!isAuthenticated) {
    return null
  }

  // Dashboard should be accessible to all authenticated users
  // Don't pass path prop - let ProtectedPage allow all authenticated users
  return (
    <ProtectedPage>
      <div className="p-6 space-y-6" suppressHydrationWarning>
        <PageHeader
          title="Dashboard"
        subtitle="Real-time monitoring of your CDC replication pipelines"
        icon={LayoutDashboard}
      />
      <DashboardOverview />
      </div>
    </ProtectedPage>
  )
}
