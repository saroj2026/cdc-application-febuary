"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { User, Bell, Settings, Moon, Sun, LogOut, Database, Menu, ChevronLeft } from "lucide-react"
import { useTheme } from "@/contexts/theme-context"
import { useSidebar } from "@/contexts/sidebar-context"
import { useAppDispatch, useAppSelector } from "@/lib/store/hooks"
import { logout } from "@/lib/store/slices/authSlice"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"

export function TopNav() {
  const { theme, toggleTheme } = useTheme()
  const { isCollapsed, toggleCollapse } = useSidebar()
  const router = useRouter()
  const dispatch = useAppDispatch()
  const { user } = useAppSelector((state) => state.auth)
  const { unreadCount } = useAppSelector((state) => state.alerts)
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  const handleLogout = async () => {
    try {
      await dispatch(logout())
      router.push("/auth/login")
    } catch (error) {
      console.error("Logout error:", error)
      // Still redirect even if logout fails
      router.push("/auth/login")
    }
  }

  // Safety check for user data
  const userName = user?.full_name || "User"
  const userEmail = user?.email || ""

  return (
    <div className="h-16 border-b border-border bg-sidebar flex items-center justify-between px-4 shrink-0">
      <div className="flex items-center gap-4">
        {/* Toggle Sidebar Button */}
        <button
          onClick={toggleCollapse}
          className="p-2 hover:bg-surface-hover rounded-lg transition-colors text-foreground-muted hover:text-primary"
          title={isCollapsed ? "Expand Sidebar" : "Collapse Sidebar"}
        >
          {isCollapsed ? <Menu className="w-5 h-5" /> : <ChevronLeft className="w-5 h-5" />}
        </button>

      </div>

      <div className="flex items-center gap-4">
        <button
          onClick={() => router.push("/errors")}
          className="relative p-2 hover:bg-surface-hover rounded-lg transition-colors hover:text-primary"
          aria-label="Notifications"
        >
          <Bell className="w-5 h-5 text-foreground-muted hover:text-primary transition-colors" />
          {mounted && unreadCount > 0 && (
            <span className="absolute top-0 right-0 w-4 h-4 bg-error text-white text-xs rounded-full flex items-center justify-center">
              {unreadCount > 9 ? '9+' : unreadCount}
            </span>
          )}
        </button>
        <button
          onClick={() => router.push("/settings")}
          className="p-2 hover:bg-surface-hover rounded-lg transition-colors hover:text-primary"
          aria-label="Settings"
        >
          <Settings className="w-5 h-5 text-foreground-muted hover:text-primary transition-colors" />
        </button>
        <button
          onClick={toggleTheme}
          className="p-2 hover:bg-surface-hover rounded-lg transition-colors hover:text-primary"
          aria-label="Toggle theme"
          suppressHydrationWarning
        >
          <Sun className={`w-5 h-5 text-foreground-muted ${mounted && theme === "dark" ? "block" : "hidden"}`} />
          <Moon className={`w-5 h-5 text-foreground-muted ${mounted && theme === "light" ? "block" : "hidden"}`} />
          {!mounted && <Moon className="w-5 h-5 text-foreground-muted" />}
        </button>

        {/* User Menu with Logout - Always render, but disable interactions until mounted */}
        <div suppressHydrationWarning>
          {mounted ? (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button className="relative outline-none group" aria-label="User menu">
                  {/* Modern 2026 User Identity Component */}
                  <div className="relative flex items-center justify-center">
                    <div className="absolute -inset-1 bg-gradient-to-r from-primary via-purple-500 to-cyan-500 rounded-full blur-[2px] opacity-20 group-hover:opacity-40 transition duration-500"></div>
                    <div className="relative w-9 h-9 border-2 border-border/50 bg-card rounded-full flex items-center justify-center overflow-hidden shadow-lg group-hover:border-primary/50 transition-all duration-300">
                      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-purple-500/5"></div>
                      <div className="w-full h-full flex items-center justify-center font-bold text-xs text-primary group-hover:bg-primary/10 transition-colors">
                        {userName.charAt(0).toUpperCase()}
                      </div>
                      {/* Live status orbit indicator */}
                      <div className="absolute bottom-0 right-0 w-2.5 h-2.5 bg-emerald-500 border-2 border-background rounded-full shadow-[0_0_8px_rgba(16,185,129,0.5)]"></div>
                    </div>
                  </div>
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="bg-surface border-border">
                <DropdownMenuLabel>
                  <div className="flex flex-col space-y-1">
                    <p className="text-sm font-medium text-foreground">{userName}</p>
                    {userEmail && <p className="text-xs text-foreground-muted">{userEmail}</p>}
                  </div>
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={() => router.push("/settings")} className="cursor-pointer">
                  <Settings className="w-4 h-4 mr-2" />
                  Settings
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={handleLogout} className="cursor-pointer text-error focus:text-error">
                  <LogOut className="w-4 h-4 mr-2" />
                  Logout
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          ) : (
            <div className="relative w-9 h-9 border-2 border-border/20 bg-muted/30 rounded-full flex items-center justify-center opacity-50 grayscale animate-pulse">
              <User className="w-4 h-4 text-muted-foreground" />
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
