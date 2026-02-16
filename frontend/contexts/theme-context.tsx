"use client"

import type React from "react"
import { createContext, useContext, useEffect, useState } from "react"

type Theme = "light" | "dark"

interface ThemeContextType {
  theme: Theme
  toggleTheme: () => void
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined)

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  // Start with "dark" as default to match server-side rendering
  // This prevents hydration mismatch
  const [theme, setTheme] = useState<Theme>("dark")
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    // Mark as mounted to prevent hydration issues
    setMounted(true)
    
    // Hydrate theme from localStorage on client mount
    const stored = localStorage.getItem("theme") as Theme | null
    if (stored && (stored === "dark" || stored === "light")) {
      setTheme(stored)
      document.documentElement.classList.toggle("dark", stored === "dark")
      document.documentElement.classList.toggle("light", stored === "light")
    } else {
      // Default to dark - ensure DOM matches
      const defaultTheme: Theme = "dark"
      setTheme(defaultTheme)
      localStorage.setItem("theme", defaultTheme)
      document.documentElement.classList.remove("light")
      document.documentElement.classList.add("dark")
    }
  }, [])

  const toggleTheme = () => {
    setTheme((prev) => {
      const newTheme = prev === "dark" ? "light" : "dark"
      localStorage.setItem("theme", newTheme)
      document.documentElement.classList.toggle("dark", newTheme === "dark")
      document.documentElement.classList.toggle("light", newTheme === "light")
      return newTheme
    })
  }

  return <ThemeContext.Provider value={{ theme, toggleTheme }}>{children}</ThemeContext.Provider>
}

export function useTheme() {
  const context = useContext(ThemeContext)
  if (context === undefined) {
    throw new Error("useTheme must be used within ThemeProvider")
  }
  // Return default theme if not mounted yet to prevent hydration issues
  return {
    theme: context.theme || "dark",
    toggleTheme: context.toggleTheme,
  }
}
