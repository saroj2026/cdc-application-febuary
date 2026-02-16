"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { useAppSelector } from "@/lib/store/hooks"

export default function Home() {
  const router = useRouter()
  const { isAuthenticated } = useAppSelector((state) => state.auth)

  useEffect(() => {
    if (isAuthenticated) {
      router.push("/dashboard")
    } else {
      router.push("/auth/login")
    }
  }, [isAuthenticated, router])

  return null
}
