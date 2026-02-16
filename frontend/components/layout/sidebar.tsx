"use client"

import { useState, useEffect } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import {
  Database,
  BarChart3,
  Settings,
  GitBranch,
  AlertTriangle,
  Activity,
  Home,
  Shield,
  Users,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  ChevronUp,
} from "lucide-react"
import { useSidebar } from "@/contexts/sidebar-context"
import { useAppSelector } from "@/lib/store/hooks"
import { canAccessPage } from "@/lib/store/slices/permissionSlice"

const menuSections = [
  {
    title: "PLATFORM",
    items: [
      { href: "/dashboard", label: "Dashboard", icon: Home },
      { href: "/monitoring", label: "Monitoring", icon: Activity },
    ],
  },
  {
    title: "REPLICATION",
    items: [
      { href: "/connections", label: "Connections", icon: Database },
      { href: "/pipelines", label: "Pipelines", icon: GitBranch },
      { href: "/analytics", label: "Analytics", icon: BarChart3 },
    ],
  },
  {
    title: "OPERATIONS",
    items: [
      { href: "/errors", label: "Errors & Alerts", icon: AlertTriangle },
      { href: "/governance", label: "Data Governance", icon: Shield },
      { href: "/users", label: "User Management", icon: Users },
      { href: "/settings", label: "Settings", icon: Settings },
    ],
  },
]

export function Sidebar() {
  const pathname = usePathname()
  const { isCollapsed, toggleCollapse, mounted } = useSidebar()
  // Use specific selectors instead of root state to prevent unnecessary rerenders
  const { user, isAuthenticated } = useAppSelector((state) => state.auth)
  const permissions = useAppSelector((state) => state.permissions)
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({
    PLATFORM: true,
    REPLICATION: true,
    OPERATIONS: true,
  })

  // Filter menu items based on permissions
  const getFilteredMenuSections = () => {
    // If user is not loaded yet, show all items (will be filtered once user loads)
    if (!user || !isAuthenticated) {
      return menuSections
    }

    // Super admin bypass - show all menu items for super admin
    const isSuperAdmin = user.is_superuser === true ||
      user.role_name === 'super_admin' ||
      user.role_name === 'admin' ||
      String(user.is_superuser).toLowerCase() === 'true'

    if (isSuperAdmin) {
      return menuSections
    }

    // Create minimal state object for permission checks
    const minimalState = { auth: { user, isAuthenticated }, permissions }

    return menuSections.map((section) => ({
      ...section,
      items: section.items.filter((item) => {
        // Check if user can access this page
        return canAccessPage(item.href)(minimalState)
      }),
    })).filter((section) => section.items.length > 0) // Remove empty sections
  }

  // Auto-expand section if current path matches any item in that section
  useEffect(() => {
    if (!isCollapsed) {
      const newExpanded: Record<string, boolean> = {}
      menuSections.forEach((section) => {
        const hasActiveItem = section.items.some(
          (item) => pathname === item.href || pathname.startsWith(item.href + "/")
        )
        newExpanded[section.title] = hasActiveItem || expandedSections[section.title]
      })
      setExpandedSections(newExpanded)
    }
  }, [pathname, isCollapsed]) // Removed setExpandedSections from deps to avoid loop

  const toggleSection = (sectionTitle: string) => {
    if (isCollapsed) return
    setExpandedSections((prev) => ({
      ...prev,
      [sectionTitle]: !prev[sectionTitle],
    }))
  }

  // Show loading state if not mounted
  if (!mounted) {
    return (
      <aside className="w-64 border-r border-border bg-sidebar flex flex-col transition-all duration-300">
        <div className="p-6 border-b border-border" />
      </aside>
    )
  }

  return (
    <aside
      className={cn(
        "h-screen border-r border-border bg-sidebar flex flex-col transition-all duration-300 relative z-20 shadow-sm sticky top-0",
        isCollapsed ? "w-[72px]" : "w-64",
      )}
    >
      {/* Logo Area - Modern 2026 Identity Design */}
      <div className={cn(
        "h-16 border-b border-border flex items-center px-4 shrink-0 transition-all duration-300",
        "bg-white dark:bg-sidebar shadow-sm", // Solid high-contrast background
        isCollapsed ? "justify-center" : "justify-start gap-3"
      )}>
        {!isCollapsed ? (
          <div className="flex items-center gap-2.5 group cursor-default">
            <div className="relative">
              {/* Outer Glow / Halo */}
              <div className="absolute -inset-1 bg-gradient-to-tr from-primary/40 via-purple-500/40 to-cyan-500/40 rounded-lg blur-[4px] opacity-20 group-hover:opacity-60 transition duration-700"></div>

              {/* Logo Mark - Database Centric */}
              <div className="relative w-9 h-9 bg-gradient-to-br from-primary via-primary/80 to-purple-600 rounded-lg flex items-center justify-center flex-shrink-0 border border-white/20 shadow-xl overflow-hidden">
                <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,_var(--tw-gradient-stops))] from-white/20 via-transparent to-transparent"></div>
                <div className="relative flex items-center justify-center">
                  <Database className="w-5 h-5 text-white group-hover:scale-110 transition-transform duration-500 drop-shadow-[0_0_8px_rgba(255,255,255,0.4)]" />
                  <div className="absolute -top-1 -right-1 w-1.5 h-1.5 bg-cyan-300 rounded-full animate-pulse shadow-[0_0_5px_rgba(103,232,249,0.8)]"></div>
                </div>
              </div>
            </div>

            <div className="flex flex-col">
              <div className="flex items-baseline">
                <span className="text-xl font-black tracking-tighter bg-clip-text text-transparent bg-gradient-to-r from-primary via-slate-900 to-slate-800 dark:via-white dark:to-white/70">
                  CDC
                </span>
                <span className="ml-1 text-[10px] font-bold text-primary tracking-widest uppercase opacity-80">
                  Nexus
                </span>
              </div>
              <div className="h-[1.5px] w-full bg-gradient-to-r from-primary to-transparent scale-x-0 group-hover:scale-x-100 transition-transform duration-500 origin-left"></div>
            </div>
          </div>
        ) : (
          <div className="relative group">
            <div className="absolute -inset-0.5 bg-gradient-to-tr from-primary to-purple-500 rounded-lg blur-[2px] opacity-40"></div>
            <div className="relative w-10 h-10 bg-gradient-to-br from-primary to-purple-700 rounded-lg flex items-center justify-center shadow-lg border border-white/10">
              <Database className="w-5 h-5 text-white" />
            </div>
          </div>
        )}
      </div>
      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-4 scrollbar-thin scrollbar-thumb-border scrollbar-track-transparent">
        {getFilteredMenuSections().map((section) => {
          const isExpanded = expandedSections[section.title] ?? true
          const hasActiveItem = section.items.some(
            (item) => pathname === item.href || pathname.startsWith(item.href + "/")
          )

          // Skip rendering section header in collapsed mode if checking for active item
          if (isCollapsed) {
            return (
              <div key={section.title} className="space-y-1 mb-4 border-b border-border/50 pb-4 last:border-0">
                {section.items.map((item) => {
                  const Icon = item.icon
                  const isActive = pathname === item.href || pathname.startsWith(item.href + "/")
                  return (
                    <Link
                      key={item.href}
                      href={item.href}
                      title={item.label} // Tooltip
                      className={cn(
                        "flex items-center justify-center w-10 h-10 mx-auto rounded-lg transition-all duration-200 group relative",
                        isActive
                          ? "bg-primary text-primary-foreground shadow-md"
                          : "text-foreground-muted hover:text-foreground hover:bg-surface-hover",
                      )}
                    >
                      <Icon className={cn("w-5 h-5", isActive ? "text-white" : "")} />
                      {/* Hover Popup Label for Collapsed State */}
                      <span className="absolute left-12 ml-2 px-2 py-1 bg-popover text-popover-foreground text-xs rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap shadow-md pointer-events-none z-50 border border-border">
                        {item.label}
                      </span>
                    </Link>
                  )
                })}
              </div>
            )
          }

          return (
            <div key={section.title} className="group/section">
              {/* Dropdown Header */}
              <button
                onClick={() => toggleSection(section.title)}
                className={cn(
                  "w-full flex items-center justify-between px-3 py-1.5 rounded-md text-[10px] font-bold text-foreground-muted/70 uppercase tracking-wider mb-1 transition-colors hover:text-foreground",
                  hasActiveItem && "text-primary/90"
                )}
              >
                <span>{section.title}</span>
                {isExpanded ? (
                  <ChevronUp className="w-3 h-3 opacity-50" />
                ) : (
                  <ChevronDown className="w-3 h-3 opacity-50" />
                )}
              </button>

              {/* Dropdown Content */}
              <div
                className={cn(
                  "space-y-0.5 overflow-hidden transition-all duration-300 ease-in-out",
                  isExpanded ? "max-h-[500px] opacity-100" : "max-h-0 opacity-0"
                )}
              >
                {section.items.map((item) => {
                  const Icon = item.icon
                  const isActive = pathname === item.href || pathname.startsWith(item.href + "/")
                  return (
                    <Link
                      key={item.href}
                      href={item.href}
                      className={cn(
                        "flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-all duration-200 border border-transparent",
                        isActive
                          ? "bg-primary/5 text-primary border-primary/10 shadow-sm" // Clean active state
                          : "text-foreground-muted hover:text-foreground hover:bg-surface-hover hover:border-border/50",
                      )}
                    >
                      <Icon className={cn("w-4 h-4 flex-shrink-0 transition-colors", isActive ? "text-primary" : "text-foreground-muted group-hover:text-foreground")} />
                      <span>{item.label}</span>
                    </Link>
                  )
                })}
              </div>
            </div>
          )
        })}
      </nav>


    </aside>
  )
}
