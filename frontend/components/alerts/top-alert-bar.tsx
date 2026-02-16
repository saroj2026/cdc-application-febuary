/**
 * Top alert bar component to show critical alerts at the top of the page
 */
"use client"

import { useState, useEffect, useMemo, useCallback } from "react"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { updateAlertStatus, markAllAsRead } from "@/lib/store/slices/alertsSlice"
import { X, AlertTriangle, AlertCircle, Info, CheckCircle } from "lucide-react"
import { Button } from "@/components/ui/button"
import { formatDistanceToNow } from "date-fns"

export function TopAlertBar() {
  const dispatch = useAppDispatch()
  const { alerts, unreadCount } = useAppSelector((state) => state.alerts)
  const [dismissedAlerts, setDismissedAlerts] = useState<Set<string>>(new Set())

  // Get critical/unresolved alerts - memoized to prevent unnecessary recalculations
  const criticalAlerts = useMemo(() => {
    return alerts
      .filter(a => 
        a.status === 'unresolved' && 
        (a.severity === 'critical' || a.severity === 'high') &&
        !dismissedAlerts.has(a.id)
      )
      .slice(0, 3) // Show max 3 alerts
  }, [alerts, dismissedAlerts])

  // Request notification permission
  useEffect(() => {
    if (typeof window !== 'undefined' && 'Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission()
    }
  }, [])

  // Show browser notifications for new critical alerts
  useEffect(() => {
    if (criticalAlerts.length > 0 && typeof window !== 'undefined' && 'Notification' in window) {
      const latestAlert = criticalAlerts[0]
      if (Notification.permission === 'granted' && !dismissedAlerts.has(latestAlert.id)) {
        new Notification('Critical Alert', {
          body: `${latestAlert.sourceName || latestAlert.source}: ${latestAlert.message}`,
          icon: '/icon-dark-32x32.png',
          tag: latestAlert.id,
        })
      }
    }
  }, [criticalAlerts, dismissedAlerts])

  // Memoize handlers to prevent infinite loops
  const handleAcknowledge = useCallback((alertId: string) => {
    dispatch(updateAlertStatus({ id: alertId, status: 'acknowledged' }))
    setDismissedAlerts(prev => {
      const newSet = new Set(prev)
      newSet.add(alertId)
      return newSet
    })
  }, [dispatch])

  const handleDismiss = useCallback((alertId: string) => {
    setDismissedAlerts(prev => {
      const newSet = new Set(prev)
      newSet.add(alertId)
      return newSet
    })
  }, [])

  if (criticalAlerts.length === 0) {
    return null
  }

  const getAlertIcon = (type: string, severity: string) => {
    if (severity === 'critical') {
      return <AlertCircle className="w-5 h-5 text-destructive" />
    }
    if (type === 'error' || severity === 'high') {
      return <AlertTriangle className="w-5 h-5 text-error" />
    }
    if (type === 'warning') {
      return <AlertTriangle className="w-5 h-5 text-warning" />
    }
    return <Info className="w-5 h-5 text-info" />
  }

  return (
    <div className="border-b border-border bg-surface">
      {criticalAlerts.map((alert) => (
        <div
          key={alert.id}
          className={`px-6 py-3 flex items-center gap-4 ${
            alert.severity === 'critical' ? 'bg-destructive/10 border-l-4 border-destructive' :
            alert.severity === 'high' ? 'bg-error/10 border-l-4 border-error' :
            'bg-warning/10 border-l-4 border-warning'
          }`}
        >
          {getAlertIcon(alert.type, alert.severity)}
          <div className="flex-1">
            <p className="text-sm font-semibold text-foreground">
              {alert.sourceName ? `${alert.sourceName}: ` : ''}{alert.message}
            </p>
            <p className="text-xs text-foreground-muted mt-0.5">
              {formatDistanceToNow(new Date(alert.timestamp), { addSuffix: true })}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              variant="ghost"
              onClick={(e) => {
                e.preventDefault()
                e.stopPropagation()
                handleAcknowledge(alert.id)
              }}
              className="text-xs h-7"
            >
              Acknowledge
            </Button>
            <Button
              size="sm"
              variant="ghost"
              onClick={(e) => {
                e.preventDefault()
                e.stopPropagation()
                handleDismiss(alert.id)
              }}
              className="text-xs h-7 px-2"
            >
              <X className="w-4 h-4" />
            </Button>
          </div>
        </div>
      ))}
      {unreadCount > criticalAlerts.length && (
        <div className="px-6 py-2 bg-surface-hover text-center">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => dispatch(markAllAsRead())}
            className="text-xs text-foreground-muted hover:text-foreground"
          >
            {unreadCount - criticalAlerts.length} more alert{unreadCount - criticalAlerts.length !== 1 ? 's' : ''} - View all
          </Button>
        </div>
      )}
    </div>
  )
}

