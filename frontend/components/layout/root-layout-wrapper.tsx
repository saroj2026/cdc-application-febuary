"use client"

import type React from "react"
import { usePathname } from "next/navigation"
import { Sidebar } from "@/components/layout/sidebar"
import { TopNav } from "@/components/layout/top-nav"
import { ThemeProvider } from "@/contexts/theme-context"
import { SidebarProvider } from "@/contexts/sidebar-context"
import { AlertSync } from "@/components/alerts/alert-sync"
import { TopAlertBar } from "@/components/alerts/top-alert-bar"

export function RootLayoutWrapper({ children }: { children: React.ReactNode }) {
  const pathname = usePathname()
  const isAuthPage = pathname?.startsWith("/auth")

  // Auth pages (login, signup) should not have dashboard layout
  if (isAuthPage) {
    return (
      <ThemeProvider>
        {children}
      </ThemeProvider>
    )
  }

  // Dashboard and other pages get the full layout with sidebar and top nav
  return (
    <ThemeProvider>
      <SidebarProvider>
        <div className="flex h-screen bg-sidebar">
          <Sidebar />

          {/* Main content area */}
          <div className="flex-1 flex flex-col overflow-hidden bg-sidebar">
            <AlertSync />
            <TopNav />
            <TopAlertBar />

            {/* Page content */}
            <main className="flex-1 overflow-y-auto bg-sidebar">{children}</main>
          </div>
        </div>
      </SidebarProvider>
    </ThemeProvider>
  )
}
