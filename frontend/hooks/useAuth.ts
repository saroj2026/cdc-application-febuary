"use client"

import { useEffect, useState } from "react"

interface User {
  id: string
  email: string
  fullName: string
  role: "admin" | "user"
}

export function useAuth() {
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    const storedUser = localStorage.getItem("user")
    if (storedUser) {
      try {
        setUser(JSON.parse(storedUser))
      } catch {
        setUser(null)
      }
    }
    setIsLoading(false)
  }, [])

  const logout = () => {
    localStorage.removeItem("user")
    setUser(null)
  }

  return { user, isLoading, logout }
}
