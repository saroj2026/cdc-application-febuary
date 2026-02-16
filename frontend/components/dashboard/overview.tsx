"use client"

import { useState, useEffect, useMemo, useRef } from "react"
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area } from "recharts"
import { Card } from "@/components/ui/card"
import { Activity, Database, AlertCircle, CheckCircle, Loader2, TrendingUp, TrendingDown, ArrowRight, Layers, BarChart3, Shield, Globe } from "lucide-react"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { fetchReplicationEvents } from "@/lib/store/slices/monitoringSlice"
import { fetchPipelines } from "@/lib/store/slices/pipelineSlice"
import { wsClient } from "@/lib/websocket/client"
import { formatDistanceToNow } from "date-fns"
import { ApplicationLogs } from "./application-logs"

import { useRouter } from "next/navigation"

export function DashboardOverview() {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const { events, metrics } = useAppSelector((state) => state.monitoring)
  const { pipelines } = useAppSelector((state) => state.pipelines)

  const [chartData, setChartData] = useState<any[]>([])
  const [dashboardMetrics, setDashboardMetrics] = useState({
    activePipelines: 0,
    totalTables: 0,
    errorRate: 0,
    dataQuality: 0,
  })
  const [backendStats, setBackendStats] = useState<any>(null)

  // Request notification permission
  useEffect(() => {
    if (typeof window !== 'undefined' && 'Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission()
    }
  }, [])

  // Fetch initial data (only once on mount) - fetch 7 days of events
  const hasFetchedRef = useRef(false)
  useEffect(() => {
    if (hasFetchedRef.current) return
    hasFetchedRef.current = true

    dispatch(fetchPipelines())
    // Fetch 7 days of events for the charts
    const sevenDaysAgo = new Date()
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7)
    dispatch(fetchReplicationEvents({
      limit: 10000,
      todayOnly: false,
      startDate: sevenDaysAgo.toISOString()
    }))

    // Fetch dashboard stats from backend
    import("@/lib/api/client").then(({ apiClient }) => {
      apiClient.getDashboardStats()
        .then(stats => {
          setBackendStats(stats)
        })
        .catch(err => {
          console.error("Error fetching dashboard stats:", err)
        })
    })
  }, [dispatch])

  // Auto-refresh pipelines every 10 seconds to keep active pipeline count accurate
  useEffect(() => {
    const interval = setInterval(() => {
      dispatch(fetchPipelines())
    }, 10000) // Refresh every 10 seconds

    return () => clearInterval(interval)
  }, [dispatch])

  // Auto-refresh events every 30 seconds to show 7 days of events
  useEffect(() => {
    const interval = setInterval(() => {
      const sevenDaysAgo = new Date()
      sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7)
      dispatch(fetchReplicationEvents({
        limit: 10000,
        todayOnly: false,
        startDate: sevenDaysAgo.toISOString()
      }))
    }, 30000) // Refresh every 30 seconds

    return () => clearInterval(interval)
  }, [dispatch])

  // Track previous pipeline IDs to prevent unnecessary re-subscriptions
  const prevPipelineIdsRef = useRef<string>('')

  // Subscribe to pipelines for real-time updates (only when pipeline IDs change)
  useEffect(() => {
    const pipelinesArray = Array.isArray(pipelines) ? pipelines : []
    if (pipelinesArray.length === 0) {
      return
    }

    const currentPipelineIds = pipelinesArray
      .filter(p => p.id)
      .map(p => String(p.id))
      .sort()
      .join(',')

    // Only subscribe if pipeline IDs have changed
    if (currentPipelineIds !== prevPipelineIdsRef.current) {
      // Convert pipeline IDs properly - handle both numeric and string IDs
      const pipelineIds = pipelinesArray
        .filter(p => p.id)
        .map(p => {
          const idStr = String(p.id)
          const numId = Number(p.id)
          // Use number if valid, otherwise use string
          return !isNaN(numId) && isFinite(numId) ? numId : idStr
        })
        .filter(id => id && id !== 'NaN' && id !== 'undefined') // Filter out invalid IDs

      // Unsubscribe from old pipelines
      if (prevPipelineIdsRef.current) {
        const prevIds = prevPipelineIdsRef.current.split(',').filter(Boolean)
        prevIds.forEach(id => {
          // Try to convert to number, but keep as string if it's an ObjectId
          const numId = Number(id)
          const pipelineId = !isNaN(numId) && isFinite(numId) ? numId : id
          if (!pipelineIds.includes(pipelineId)) {
            wsClient.unsubscribePipeline(pipelineId)
          }
        })
      }

      // Subscribe to new pipelines
      pipelineIds.forEach(pipelineId => {
        wsClient.subscribePipeline(pipelineId)
      })

      prevPipelineIdsRef.current = currentPipelineIds
    }

    return () => {
      // Cleanup subscriptions on unmount
      if (prevPipelineIdsRef.current) {
        const pipelineIds = prevPipelineIdsRef.current.split(',').filter(Boolean)
        pipelineIds.forEach(id => {
          // Try to convert to number, but keep as string if it's an ObjectId
          const numId = Number(id)
          const pipelineId = !isNaN(numId) && isFinite(numId) ? numId : id
          wsClient.unsubscribePipeline(pipelineId)
        })
      }
    }
  }, [pipelines])

  // Calculate dashboard metrics - memoized to prevent infinite loops
  const dashboardMetricsMemo = useMemo(() => {
    const pipelinesArray = Array.isArray(pipelines) ? pipelines : []
    const eventsArray = Array.isArray(events) ? events : []

    // Always calculate active pipelines from pipelines array (more reliable)
    // Check for both 'active' and 'running' status, and also 'starting' status
    const activePipelines = pipelinesArray.filter(p => {
      const status = p.status?.toLowerCase() || ''
      return status === 'active' || status === 'running' || status === 'starting'
    }).length

    // Calculate total tables
    const totalTables = pipelinesArray.reduce((sum, p) => {
      const sourceTables = Array.isArray(p.table_mappings) ? p.table_mappings.length : 0
      return sum + sourceTables
    }, 0)

    // Use backend stats for events if available, otherwise use frontend events
    let totalEvents = eventsArray.length
    let failedEvents = eventsArray.filter(e => e.status === 'failed' || e.status === 'error').length
    let successEvents = eventsArray.filter(e => e.status === 'applied' || e.status === 'success').length

    if (backendStats) {
      // Use backend stats for more accurate event counts
      totalEvents = backendStats.total_events || totalEvents
      failedEvents = backendStats.failed_events || failedEvents
      successEvents = backendStats.success_events || successEvents
    }

    // Calculate error rate
    const errorRate = totalEvents > 0 ? (failedEvents / totalEvents) * 100 : 0

    // Calculate data quality (success rate)
    const dataQuality = totalEvents > 0 ? (successEvents / totalEvents) * 100 : 100

    return {
      activePipelines,
      totalTables,
      errorRate,
      dataQuality,
    }
  }, [pipelines, events, backendStats])

  // Update dashboard metrics only when memoized value changes
  useEffect(() => {
    setDashboardMetrics(dashboardMetricsMemo)
  }, [dashboardMetricsMemo])

  // Process metrics for charts (last 7 days, grouped by day)
  const chartDataMemo = useMemo(() => {
    const eventsArray = Array.isArray(events) ? events : []
    const metricsArray = Array.isArray(metrics) ? metrics : []

    // Always create 7 days of data buckets to ensure all days are represented
    const now = new Date()
    const days = Array.from({ length: 7 }, (_, i) => {
      const day = new Date(now.getTime() - (6 - i) * 24 * 60 * 60 * 1000)
      day.setHours(0, 0, 0, 0)
      return day
    })

    if (metricsArray.length > 0) {
      // Use metrics data for charts (last 7 days)
      return days.map((day, idx) => {
        const dayDate = day.toISOString().split('T')[0] // YYYY-MM-DD format

        // Find metrics for this day
        const dayMetrics = metricsArray.filter(m => {
          try {
            const metricDate = new Date(m.timestamp)
            metricDate.setHours(0, 0, 0, 0)
            return metricDate.getTime() === day.getTime()
          } catch (err) {
            return false
          }
        })

        // Aggregate metrics for the day
        const totalEvents = dayMetrics.reduce((sum, m) => sum + (m.total_events || 0), 0)
        const totalErrors = dayMetrics.reduce((sum, m) => sum + (m.error_count || 0), 0)
        const totalSynced = totalEvents - totalErrors

        // Format date as "Mon DD" or "Today"
        const dayStr = idx === 6 ? 'Today' : day.toLocaleDateString('en-US', { weekday: 'short', day: 'numeric' })

        return {
          time: dayStr,
          replicated: totalEvents,
          synced: totalSynced,
          errors: totalErrors,
          date: dayDate,
          dateObj: day // Keep for sorting
        }
      }).sort((a, b) => a.dateObj.getTime() - b.dateObj.getTime())
        .map(({ dateObj, ...rest }) => rest) // Remove dateObj before returning
    } else {
      // If no metrics, create chart data from events (7 days, grouped by day)
      return days.map((day, idx) => {
        const dayStart = new Date(day)
        dayStart.setHours(0, 0, 0, 0)
        const dayEnd = new Date(day)
        dayEnd.setHours(23, 59, 59, 999)

        const dayEvents = eventsArray.filter(e => {
          try {
            const eventTime = new Date(e.created_at || e.source_commit_time || Date.now())
            return eventTime >= dayStart && eventTime <= dayEnd
          } catch (err) {
            return false
          }
        })

        const replicated = dayEvents.length
        const synced = dayEvents.filter(e => e.status === 'applied' || e.status === 'success').length
        const errors = dayEvents.filter(e => e.status === 'failed' || e.status === 'error').length

        // Format date as "Mon DD" or "Today"
        const dayStr = idx === 6 ? 'Today' : day.toLocaleDateString('en-US', { weekday: 'short', day: 'numeric' })

        return {
          time: dayStr,
          replicated,
          synced,
          errors,
          date: day.toISOString().split('T')[0],
          dateObj: day // Keep for sorting
        }
      }).sort((a, b) => a.dateObj.getTime() - b.dateObj.getTime())
        .map(({ dateObj, ...rest }) => rest) // Remove dateObj before returning
    }
  }, [metrics.length, events.length])

  // Update chart data only when memoized value changes
  useEffect(() => {
    setChartData(chartDataMemo)
  }, [chartDataMemo])

  // Get recent pipeline activity from events
  const recentActivity = useMemo(() => {
    const eventsArray = Array.isArray(events) ? events : []
    const pipelinesArray = Array.isArray(pipelines) ? pipelines : []

    if (eventsArray.length === 0) {
      return []
    }

    // Sort events by created_at (most recent first) and take the most recent ones
    const sortedEvents = [...eventsArray]
      .filter(e => e && (e.created_at || e.source_commit_time)) // Only events with dates
      .sort((a, b) => {
        try {
          const dateA = new Date(a.created_at || a.source_commit_time || 0).getTime()
          const dateB = new Date(b.created_at || b.source_commit_time || 0).getTime()
          return dateB - dateA // Most recent first
        } catch (err) {
          return 0
        }
      })

    if (sortedEvents.length === 0) {
      return []
    }

    const recentEvents = sortedEvents.slice(0, 30) // Get more events to ensure we have unique pipelines
    const activityMap = new Map()

    recentEvents.forEach(event => {
      if (!event) return

      // Get pipeline ID - try multiple fields
      const pipelineId = event.pipeline_id || null
      if (!pipelineId) {
        return
      }

      // Try to find pipeline by matching ID
      let pipeline = null
      let pipelineKey = String(pipelineId)

      if (pipelinesArray.length > 0) {
        pipeline = pipelinesArray.find(p => {
          const pId = String(p.id)
          const eId = String(pipelineId)
          if (pId === eId) return true
          const pNum = Number(pId)
          const eNum = Number(eId)
          if (!isNaN(pNum) && !isNaN(eNum) && pNum === eNum) return true
          return false
        })

        if (pipeline) {
          pipelineKey = String(pipeline.id)
        }
      }

      if (!activityMap.has(pipelineKey)) {
        try {
          const eventDate = new Date(event.created_at || event.source_commit_time || Date.now())
          const isValidDate = !isNaN(eventDate.getTime())

          if (!isValidDate) return

          const status = event.status === 'applied' || event.status === 'success' ? 'success' :
            event.status === 'failed' || event.status === 'error' ? 'error' :
              event.status === 'captured' || event.status === 'pending' ? 'warning' : 'warning'

          activityMap.set(pipelineKey, {
            pipeline: pipeline ? (pipeline.name || `Pipeline ${pipelineKey}`) : `Pipeline ${pipelineKey}`,
            status: status,
            time: formatDistanceToNow(eventDate, { addSuffix: true }),
            timestamp: eventDate.getTime(),
          })
        } catch (err) {
          console.warn('Error processing event for activity:', err, event)
        }
      }
    })

    return Array.from(activityMap.values())
      .filter(a => a && a.timestamp)
      .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
      .slice(0, 5)
      .map(({ timestamp, ...rest }) => rest)
  }, [events, pipelines])

  const pipelinesArrayForMetrics = Array.isArray(pipelines) ? pipelines : []

  const metricsDisplay = [
    {
      label: "Active Pipelines",
      value: dashboardMetrics.activePipelines.toString(),
      change: `${pipelinesArrayForMetrics.filter(p => {
        const status = p.status?.toLowerCase() || ''
        return status === 'active' || status === 'running' || status === 'starting'
      }).length} running`,
      icon: Activity,
      color: "text-primary",
      bg: "bg-primary/10",
      trend: "up"
    },
    {
      label: "Total Tables",
      value: dashboardMetrics.totalTables.toString(),
      change: backendStats ? `${backendStats.total_connections || 0} connections` : `${events.length} events today`,
      icon: Database,
      color: "text-info",
      bg: "bg-info/10",
      trend: "neutral"
    },
    {
      label: "Error Rate",
      value: dashboardMetrics.errorRate.toFixed(1) + "%",
      change: `${events.filter(e => e.status === 'failed').length} errors`,
      icon: AlertCircle,
      color: "text-error",
      bg: "bg-error/10",
      trend: dashboardMetrics.errorRate > 0 ? "down" : "neutral"
    },
    {
      label: "Data Quality",
      value: (dashboardMetrics.dataQuality % 1 === 0
        ? dashboardMetrics.dataQuality.toFixed(0)
        : dashboardMetrics.dataQuality.toFixed(1)) + "%",
      change: `${events.filter(e => e.status === 'applied').length} successful`,
      icon: CheckCircle,
      color: "text-success",
      bg: "bg-success/10",
      trend: "up"
    },
  ]

  return (
    <div className="space-y-6">
      {/* Metrics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {metricsDisplay.map((metric) => {
          const Icon = metric.icon
          // improved styling to match monitoring page
          let gradientClass = ""
          let iconColorClass = ""
          let accentColorClass = ""

          if (metric.label.includes("Active")) {
            gradientClass = "from-blue-500/10 to-blue-600/5 border-blue-500/20"
            iconColorClass = "text-blue-500"
            accentColorClass = "bg-blue-500/20"
          } else if (metric.label.includes("Total")) {
            gradientClass = "from-amber-500/10 to-amber-600/5 border-amber-500/20"
            iconColorClass = "text-amber-500"
            accentColorClass = "bg-amber-500/20"
          } else if (metric.label.includes("Error")) {
            gradientClass = "from-red-500/10 to-red-600/5 border-red-500/20"
            iconColorClass = "text-red-500"
            accentColorClass = "bg-red-500/20"
          } else {
            gradientClass = "from-green-500/10 to-green-600/5 border-green-500/20"
            iconColorClass = "text-green-500"
            accentColorClass = "bg-green-500/20"
          }

          return (
            <Card
              key={metric.label}
              className={`p-5 bg-gradient-to-br ${gradientClass} shadow-lg hover:shadow-xl transition-all duration-300 group border-l-4 ${metric.label.includes("Active") ? "border-l-blue-500" : metric.label.includes("Total") ? "border-l-amber-500" : metric.label.includes("Error") ? "border-l-red-500" : "border-l-green-500"} relative overflow-hidden`}
            >
              {/* Background Icon */}
              <div className="absolute -right-4 -bottom-4 opacity-[0.08] group-hover:opacity-[0.12] group-hover:scale-110 transition-all duration-500 pointer-events-none">
                {metric.label.includes("Active") ? (
                  <Layers className="w-32 h-32" />
                ) : metric.label.includes("Total") ? (
                  <BarChart3 className="w-32 h-32" />
                ) : metric.label.includes("Error") ? (
                  <Shield className="w-32 h-32" />
                ) : (
                  <Globe className="w-32 h-32" />
                )}
              </div>

              <div className="flex justify-between items-start mb-4 relative z-10">
                <div className={`p-2 rounded-lg ${accentColorClass} group-hover:scale-110 transition-transform duration-200`}>
                  <Icon className={`w-5 h-5 ${iconColorClass}`} />
                </div>
                <div className={`flex items-center text-xs font-medium ${metric.trend === 'up' ? 'text-green-500' :
                  metric.trend === 'down' ? 'text-red-500' : 'text-foreground-muted'
                  }`}>
                  {metric.trend === 'up' && <TrendingUp className="w-3 h-3 mr-1" />}
                  {metric.trend === 'down' && <TrendingDown className="w-3 h-3 mr-1" />}
                  {metric.change}
                </div>
              </div>
              <div className="relative z-10">
                <h3 className="text-3xl font-bold text-foreground mb-1 tracking-tight">{metric.value}</h3>
                <p className="text-xs font-medium text-foreground-muted uppercase tracking-wide">{metric.label}</p>
              </div>
            </Card>
          )
        })}
      </div>

      {/* Main Charts Area */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2 p-6 border-border shadow-sm bg-card">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h3 className="text-base font-bold text-foreground flex items-center gap-2">
                Replication Volume
              </h3>
              <p className="text-xs text-foreground-muted mt-1">Daily event processing volume (7 days)</p>
            </div>
            <div className="flex items-center gap-3">
              <span className="flex items-center gap-1.5 text-xs text-foreground-muted">
                <span className="w-2.5 h-2.5 rounded-sm bg-primary/80"></span> Replicated
              </span>
              <span className="flex items-center gap-1.5 text-xs text-foreground-muted">
                <span className="w-2.5 h-2.5 rounded-sm bg-info/80"></span> Synced
              </span>
            </div>
          </div>

          <div className="h-[300px]">
            {chartData.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={chartData}
                  margin={{ top: 10, right: 10, left: -20, bottom: 0 }}
                >
                  <defs>
                    <linearGradient id="colorReplicated" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--primary)" stopOpacity={0.2} />
                      <stop offset="95%" stopColor="var(--primary)" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="colorSynced" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--info)" stopOpacity={0.2} />
                      <stop offset="95%" stopColor="var(--info)" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" opacity={0.4} />
                  <XAxis
                    dataKey="time"
                    stroke="var(--foreground-muted)"
                    tickLine={false}
                    axisLine={false}
                    tick={{ fontSize: 11, fill: 'var(--foreground-muted)' }}
                    dy={10}
                  />
                  <YAxis
                    stroke="var(--foreground-muted)"
                    tickLine={false}
                    axisLine={false}
                    tick={{ fontSize: 11, fill: 'var(--foreground-muted)' }}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "var(--popover)",
                      borderColor: "var(--border)",
                      borderRadius: "0.5rem",
                      boxShadow: "0 4px 12px rgba(0,0,0,0.1)",
                      fontSize: "12px",
                      padding: "8px 12px"
                    }}
                    cursor={{ stroke: 'var(--border)', strokeWidth: 1 }}
                  />
                  <Area
                    type="monotone"
                    dataKey="replicated"
                    stroke="var(--primary)"
                    strokeWidth={2}
                    fillOpacity={1}
                    fill="url(#colorReplicated)"
                    name="Replicated"
                    activeDot={{ r: 6, strokeWidth: 0 }}
                  />
                  <Area
                    type="monotone"
                    dataKey="synced"
                    stroke="var(--info)"
                    strokeWidth={2}
                    fillOpacity={1}
                    fill="url(#colorSynced)"
                    name="Synced"
                    activeDot={{ r: 6, strokeWidth: 0 }}
                  />
                </AreaChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-full flex flex-col items-center justify-center text-foreground-muted">
                <Activity className="w-10 h-10 mb-3 opacity-20" />
                <p className="text-sm">No data available</p>
              </div>
            )}
          </div>
        </Card>

        {/* Recent Activity Feed */}
        <Card className="p-0 border-border shadow-sm bg-card flex flex-col overflow-hidden h-full">
          <div className="p-4 border-b border-border bg-muted/30 flex justify-between items-center">
            <div>
              <h3 className="text-base font-bold text-foreground">Recent Activity</h3>
              <p className="text-xs text-foreground-muted mt-0.5">Latest status updates</p>
            </div>
            <Activity className="w-4 h-4 text-foreground-muted" />
          </div>

          <div className="flex-1 overflow-y-auto p-0 scrollbar-thin scrollbar-thumb-border scrollbar-track-transparent min-h-[300px]">
            {recentActivity.length > 0 ? (
              <div className="divide-y divide-border/50">
                {recentActivity.map((activity, idx) => (
                  <div
                    key={idx}
                    className="p-4 hover:bg-surface-hover transition-colors flex items-start gap-3 group cursor-default"
                  >
                    <div className={`mt-1.5 w-2.5 h-2.5 rounded-full flex-shrink-0 ${activity.status === "success" ? "bg-success shadow-[0_0_8px_rgba(34,197,94,0.4)]" :
                      activity.status === "error" ? "bg-error shadow-[0_0_8px_rgba(239,68,68,0.4)]" :
                        "bg-warning shadow-[0_0_8px_rgba(245,158,11,0.4)]"
                      }`} />

                    <div className="flex-1 min-w-0">
                      <div className="flex justify-between items-start">
                        <p className="text-sm font-semibold text-foreground truncate group-hover:text-primary transition-colors">
                          {activity.pipeline}
                        </p>
                        <span className="text-[10px] text-foreground-muted whitespace-nowrap ml-2">
                          {activity.time}
                        </span>
                      </div>
                      <div className="flex items-center justify-between mt-1">
                        <span className={`text-xs font-medium ${activity.status === "success" ? "text-success" :
                          activity.status === "error" ? "text-error" :
                            "text-warning"
                          }`}>
                          {activity.status === "success" ? "Replication Success" :
                            activity.status === "error" ? "Replication Error" : "Processing"}
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="h-full flex flex-col items-center justify-center text-foreground-muted p-8">
                <Database className="w-8 h-8 mb-3 opacity-20" />
                <p className="text-sm">No recent activity</p>
              </div>
            )}
          </div>
          <div className="p-3 border-t border-border bg-muted/10 text-center">
            <button
              onClick={() => router.push('/monitoring')}
              className="text-xs font-medium text-primary hover:text-primary/80 transition-colors flex items-center justify-center gap-1 mx-auto"
            >
              View All Activity <ArrowRight className="w-3 h-3" />
            </button>
          </div>
        </Card>
      </div>

      {/* Application Logs Section */}
      <ApplicationLogs />
    </div>
  )
}
