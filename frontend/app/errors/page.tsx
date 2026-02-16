"use client"

import { useState, useEffect, useMemo } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { AlertTriangle, CheckCircle, Clock, XCircle, AlertCircle } from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"
import { ProtectedPage } from "@/components/auth/ProtectedPage"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { updateAlertStatus } from "@/lib/store/slices/alertsSlice"
import { fetchConnections } from "@/lib/store/slices/connectionSlice"
import { fetchPipelines } from "@/lib/store/slices/pipelineSlice"
import { fetchReplicationEvents } from "@/lib/store/slices/monitoringSlice"
import { format, formatDistanceToNow } from "date-fns"

export default function ErrorsPage() {
  const dispatch = useAppDispatch()
  const { alerts } = useAppSelector((state) => state.alerts)
  const { connections } = useAppSelector((state) => state.connections)
  const { pipelines } = useAppSelector((state) => state.pipelines)
  const { events } = useAppSelector((state) => state.monitoring)

  const [errorFilter, setErrorFilter] = useState("all")
  const [selectedError, setSelectedError] = useState<string | null>(null)

  // Fetch data on mount
  useEffect(() => {
    dispatch(fetchConnections())
    dispatch(fetchPipelines())
    dispatch(fetchReplicationEvents({ limit: 1000 }))
  }, [dispatch])

  // Calculate data quality issues from events
  const dataQualityIssues = useMemo(() => {
    const issues: Array<{
      id: string
      table: string
      column: string
      issue: string
      count: number
      severity: 'low' | 'medium' | 'high' | 'critical'
      lastDetected: string
    }> = []

    // Group failed events by table
    const failedByTable: Record<string, number> = {}
    events
      .filter(e => e.status === 'failed' || e.status === 'error')
      .forEach(e => {
        if (e.table_name) {
          failedByTable[e.table_name] = (failedByTable[e.table_name] || 0) + 1
        }
      })

    Object.entries(failedByTable).forEach(([table, count]) => {
      const severity = count > 100 ? 'critical' : count > 50 ? 'high' : count > 20 ? 'medium' : 'low'
      const latestEvent = events
        .filter(e => e.table_name === table && (e.status === 'failed' || e.status === 'error'))
        .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0]

      issues.push({
        id: `dq_${table}`,
        table,
        column: 'replication',
        issue: `Replication failures detected`,
        count,
        severity,
        lastDetected: latestEvent ? formatDistanceToNow(new Date(latestEvent.created_at), { addSuffix: true }) : 'Unknown',
      })
    })

    return issues.sort((a, b) => {
      const severityOrder = { critical: 4, high: 3, medium: 2, low: 1 }
      return (severityOrder[b.severity] || 0) - (severityOrder[a.severity] || 0)
    })
  }, [events])

  const getSeverityIcon = (severity: string) => {
    switch (severity) {
      case "critical":
        return <XCircle className="w-5 h-5 text-destructive" />
      case "error":
      case "high":
        return <AlertTriangle className="w-5 h-5 text-error" />
      case "warning":
      case "medium":
        return <AlertTriangle className="w-5 h-5 text-warning" />
      default:
        return <CheckCircle className="w-5 h-5 text-success" />
    }
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case "resolved":
        return (
          <Badge className="bg-success/20 text-success hover:bg-success/20">
            <CheckCircle className="w-3 h-3 mr-1" />
            Resolved
          </Badge>
        )
      case "acknowledged":
        return (
          <Badge className="bg-info/20 text-info hover:bg-info/20">
            <Clock className="w-3 h-3 mr-1" />
            Acknowledged
          </Badge>
        )
      default:
        return (
          <Badge className="bg-error/20 text-error hover:bg-error/20">
            <AlertTriangle className="w-3 h-3 mr-1" />
            Unresolved
          </Badge>
        )
    }
  }

  const filteredErrors = useMemo(() => {
    if (errorFilter === "all") return alerts
    return alerts.filter((e) => e.status === errorFilter)
  }, [alerts, errorFilter])

  const unresolvedCount = alerts.filter((e) => e.status === "unresolved").length
  const criticalCount = alerts.filter((e) => e.severity === "critical" || e.type === "error").length
  const resolutionRate = alerts.length > 0
    ? Math.round((alerts.filter((e) => e.status === "resolved").length / alerts.length) * 100)
    : 100

  return (
    <ProtectedPage path="/errors" requiredPermission="view_metrics">
      <div className="p-6 space-y-6">
        <PageHeader
          title="Errors & Alerts"
          subtitle="Monitor replication errors and data quality issues"
          icon={AlertCircle}
          action={
            <div className="text-right">
              <p className="text-4xl font-extrabold text-error">{unresolvedCount}</p>
              <p className="text-sm text-foreground-muted font-semibold">Unresolved Errors</p>
            </div>
          }
        />

        {/* Summary Cards - Enhanced */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <Card className="p-5 bg-card border-l-4 border-l-red-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
            <AlertTriangle className="absolute -right-4 -bottom-4 w-24 h-24 text-red-500/10 group-hover:text-red-500/15 transition-colors transform rotate-12" />
            <div className="flex items-center justify-between relative z-10">
              <div>
                <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide mb-2">Total Errors (24h)</p>
                <p className="text-3xl font-extrabold text-red-500">{alerts.length}</p>
              </div>
            </div>
          </Card>
          <Card className="p-5 bg-card border-l-4 border-l-rose-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
            <XCircle className="absolute -right-4 -bottom-4 w-24 h-24 text-rose-500/10 group-hover:text-rose-500/15 transition-colors transform rotate-12" />
            <div className="flex items-center justify-between relative z-10">
              <div>
                <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide mb-2">Critical Issues</p>
                <p className="text-3xl font-extrabold text-rose-500">{criticalCount}</p>
              </div>
            </div>
          </Card>
          <Card className="p-5 bg-card border-l-4 border-l-amber-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
            <AlertTriangle className="absolute -right-4 -bottom-4 w-24 h-24 text-amber-500/10 group-hover:text-amber-500/15 transition-colors transform rotate-12" />
            <div className="flex items-center justify-between relative z-10">
              <div>
                <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide mb-2">Data Quality Issues</p>
                <p className="text-3xl font-extrabold text-amber-500">{dataQualityIssues.length}</p>
              </div>
            </div>
          </Card>
          <Card className="p-5 bg-card border-l-4 border-l-green-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
            <CheckCircle className="absolute -right-4 -bottom-4 w-24 h-24 text-green-500/10 group-hover:text-green-500/15 transition-colors transform rotate-12" />
            <div className="flex items-center justify-between relative z-10">
              <div>
                <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide mb-2">Resolution Rate</p>
                <p className="text-3xl font-extrabold text-green-500">{resolutionRate}%</p>
              </div>
            </div>
          </Card>
        </div>

        {/* Tabs */}
        <div className="flex gap-4 border-b border-border">
          <button
            onClick={() => setErrorFilter("all")}
            className={`px-4 py-2 font-medium border-b-2 ${errorFilter === "all"
              ? "border-primary text-foreground"
              : "border-transparent text-foreground-muted hover:text-foreground"
              }`}
          >
            All Errors ({alerts.length})
          </button>
          <button
            onClick={() => setErrorFilter("unresolved")}
            className={`px-4 py-2 font-medium border-b-2 ${errorFilter === "unresolved"
              ? "border-primary text-foreground"
              : "border-transparent text-foreground-muted hover:text-foreground"
              }`}
          >
            Unresolved ({unresolvedCount})
          </button>
          <button
            onClick={() => setErrorFilter("acknowledged")}
            className={`px-4 py-2 font-medium border-b-2 ${errorFilter === "acknowledged"
              ? "border-primary text-foreground"
              : "border-transparent text-foreground-muted hover:text-foreground"
              }`}
          >
            Acknowledged ({alerts.filter((e) => e.status === "acknowledged").length})
          </button>
        </div>

        {/* Error List */}
        <div className="space-y-3">
          {filteredErrors.length > 0 ? (
            filteredErrors.map((error) => (
              <Card
                key={error.id}
                className="bg-card border-l-4 border-l-red-500 shadow-sm p-4 hover:shadow-md cursor-pointer transition-all"
                onClick={() => setSelectedError(selectedError === error.id ? null : error.id)}
              >
                <div className="flex items-start gap-4">
                  {getSeverityIcon(error.severity)}

                  <div className="flex-1">
                    <div className="flex items-center justify-between gap-4 mb-2">
                      <div>
                        <h3 className="text-foreground font-semibold">{error.message}</h3>
                        <p className="text-sm text-foreground-muted mt-1">
                          {error.sourceName || error.source} {error.table ? `• ${error.table}` : ''}
                        </p>
                      </div>
                      {getStatusBadge(error.status)}
                    </div>

                    {selectedError === error.id && (
                      <div className="mt-4 p-4 bg-surface-hover rounded-lg space-y-2 border-l-2 border-primary">
                        <div className="grid grid-cols-2 gap-4 text-sm">
                          <div>
                            <p className="text-foreground-muted">Timestamp</p>
                            <p className="text-foreground font-mono">
                              {format(new Date(error.timestamp), "yyyy-MM-dd HH:mm:ss")}
                            </p>
                          </div>
                          {error.rowsAffected !== undefined && (
                            <div>
                              <p className="text-foreground-muted">Rows Affected</p>
                              <p className="text-foreground font-semibold">{error.rowsAffected}</p>
                            </div>
                          )}
                          {error.details && (
                            <div className="col-span-2">
                              <p className="text-foreground-muted">Details</p>
                              <p className="text-foreground">{error.details}</p>
                            </div>
                          )}
                        </div>
                        <div className="flex gap-2">
                          {error.status === 'unresolved' && (
                            <Button
                              size="sm"
                              className="bg-primary hover:bg-primary/90 text-foreground"
                              onClick={(e) => {
                                e.stopPropagation()
                                dispatch(updateAlertStatus({ id: error.id, status: 'acknowledged' }))
                              }}
                            >
                              Acknowledge
                            </Button>
                          )}
                          {error.status !== 'resolved' && (
                            <Button
                              size="sm"
                              variant="outline"
                              className="bg-transparent border-border hover:bg-surface-hover"
                              onClick={(e) => {
                                e.stopPropagation()
                                dispatch(updateAlertStatus({ id: error.id, status: 'resolved' }))
                              }}
                            >
                              Mark Resolved
                            </Button>
                          )}
                        </div>
                      </div>
                    )}
                  </div>

                  <div className="text-right flex-shrink-0">
                    <p className="text-xs text-foreground-muted">
                      {formatDistanceToNow(new Date(error.timestamp), { addSuffix: true })}
                    </p>
                  </div>
                </div>
              </Card>
            ))
          ) : (
            <Card className="bg-card border-l-4 border-l-success shadow-sm p-8 text-center flex flex-col items-center justify-center">
              <CheckCircle className="w-12 h-12 text-success mb-4 opacity-50" />
              <p className="text-foreground-muted">No errors found</p>
            </Card>
          )}
        </div>

        {/* Data Quality Monitoring */}
        {dataQualityIssues.length > 0 && (
          <div className="mt-8">
            <h2 className="text-2xl font-bold text-foreground mb-4">Data Quality Monitoring</h2>

            <div className="space-y-3">
              {dataQualityIssues.map((issue) => (
                <Card key={issue.id} className="bg-card border-l-4 border-l-warning shadow-sm p-4 hover:shadow-md transition-all">
                  <div className="flex items-center justify-between">
                    <div className="flex items-start gap-4 flex-1">
                      {issue.severity === "critical" ? (
                        <XCircle className="w-5 h-5 text-destructive flex-shrink-0 mt-0.5" />
                      ) : issue.severity === "high" ? (
                        <AlertTriangle className="w-5 h-5 text-error flex-shrink-0 mt-0.5" />
                      ) : (
                        <AlertTriangle className="w-5 h-5 text-warning flex-shrink-0 mt-0.5" />
                      )}

                      <div className="flex-1">
                        <h3 className="text-foreground font-semibold">
                          {issue.table}.{issue.column}
                        </h3>
                        <p className="text-sm text-foreground-muted mt-1">{issue.issue}</p>
                        <p className="text-xs text-foreground-muted mt-2">Last detected: {issue.lastDetected}</p>
                      </div>
                    </div>

                    <div className="text-right flex-shrink-0">
                      <p className="text-2xl font-bold text-foreground">{issue.count}</p>
                      <Badge
                        className={`${issue.severity === "critical"
                          ? "bg-destructive/20 text-destructive"
                          : issue.severity === "high"
                            ? "bg-error/20 text-error"
                            : "bg-warning/20 text-warning"
                          }`}
                      >
                        {issue.severity}
                      </Badge>
                    </div>
                  </div>
                </Card>
              ))}
            </div>
          </div>
        )}
      </div>
    </ProtectedPage>
  )
}
