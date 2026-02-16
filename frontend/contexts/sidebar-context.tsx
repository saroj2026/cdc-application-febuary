"use client"

import type React from "react"
import { createContext, useContext, useState, useEffect } from "react"

interface SidebarContextType {
  isCollapsed: boolean
  toggleCollapse: () => void
  mounted: boolean
}

const SidebarContext = createContext<SidebarContextType | undefined>(undefined)

export function SidebarProvider({ children }: { children: React.ReactNode }) {
  const [isCollapsed, setIsCollapsed] = useState(false)
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    const stored = localStorage.getItem("sidebarCollapsed") === "true"
    setIsCollapsed(stored)
    setMounted(true)
  }, [])

  const toggleCollapse = () => {
    setIsCollapsed((prev) => {
      const newState = !prev
      localStorage.setItem("sidebarCollapsed", String(newState))
      return newState
    })
  }

  return <SidebarContext.Provider value={{ isCollapsed, toggleCollapse, mounted }}>{children}</SidebarContext.Provider>
}

export function useSidebar() {
  const context = useContext(SidebarContext)
  if (context === undefined) {
    throw new Error("useSidebar must be used within SidebarProvider")
  }
  return context
}
