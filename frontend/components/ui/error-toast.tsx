"use client"

import { useState, useEffect } from "react"
import { X, AlertCircle, XCircle } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"

interface ErrorToastProps {
  message: string
  title?: string
  onClose: () => void
  duration?: number
}

export function ErrorToast({ message, title = "Error", onClose, duration = 5000 }: ErrorToastProps) {
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
    if (duration > 0) {
      const timer = setTimeout(() => {
        onClose()
      }, duration)
      return () => clearTimeout(timer)
    }
  }, [duration, onClose])

  if (!mounted) return null

  return (
    <div className="fixed top-4 right-4 z-50 animate-in slide-in-from-top-5">
      <Card className="bg-gradient-to-br from-red-500/10 to-red-600/5 border-2 border-red-500/30 shadow-2xl max-w-md w-full">
        <div className="p-4">
          <div className="flex items-start gap-3">
            <div className="w-10 h-10 rounded-full bg-red-500/10 flex items-center justify-center flex-shrink-0">
              <XCircle className="w-6 h-6 text-red-400" />
            </div>
            <div className="flex-1 min-w-0">
              <h3 className="text-sm font-semibold text-foreground mb-1">{title}</h3>
              <p className="text-sm text-foreground-muted whitespace-pre-wrap break-words">{message}</p>
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={onClose}
              className="h-6 w-6 p-0 flex-shrink-0 hover:bg-red-500/10"
            >
              <X className="w-4 h-4" />
            </Button>
          </div>
        </div>
      </Card>
    </div>
  )
}

// Hook for easier usage
export function useErrorToast() {
  const [error, setError] = useState<{ message: string; title?: string } | null>(null)

  const showError = (message: string, title?: string) => {
    setError({ message, title })
  }

  const ErrorToastComponent = () => {
    if (!error) return null
    return (
      <ErrorToast
        message={error.message}
        title={error.title}
        onClose={() => setError(null)}
      />
    )
  }

  return { showError, ErrorToastComponent, error }
}

