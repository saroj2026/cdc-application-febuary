"use client"

import { useState, useEffect, useMemo } from "react"
import { useParams, useRouter } from "next/navigation"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { fetchReplicationEvents, fetchMonitoringMetrics, addReplicationEvent, addMonitoringMetric } from "@/lib/store/slices/monitoringSlice"
import { fetchPipelines } from "@/lib/store/slices/pipelineSlice"
import { wsClient } from "@/lib/websocket/client"
import { formatDistanceToNow } from "date-fns"
import { Loader2, Activity, Database, AlertCircle, CheckCircle, RefreshCw, ArrowLeft, Eye, RotateCw } from "lucide-react"
import { apiClient } from "@/lib/api/client"
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, PieChart, Pie, Cell } from "recharts"
import { PageHeader } from "@/components/ui/page-header"

export default function PipelineMonitorPage() {
  const params = useParams()
  const router = useRouter()
  const dispatch = useAppDispatch()
  const pipelineId = params?.id ? String(params.id) : null

  const { isAuthenticated, isLoading: authLoading } = useAppSelector((state) => state.auth)
  const { events, metrics, isLoading } = useAppSelector((state) => state.monitoring)
  const { pipelines, isLoading: pipelinesLoading } = useAppSelector((state) => state.pipelines)
  const [mounted, setMounted] = useState(false)
  const [currentPage, setCurrentPage] = useState(1)
  const itemsPerPage = 10
  const [retryingEventId, setRetryingEventId] = useState<string | null>(null)
  const [wsConnected, setWsConnected] = useState(false)
  const [wsAvailable, setWsAvailable] = useState(true) // Default to true to show status

  // Get current pipeline - handle both string and number IDs
  const pipeline = pipelines.find(p => String(p.id) === String(pipelineId))

  // Handle client-side mounting
  useEffect(() => {
    setMounted(true)
  }, [])

  // Redirect if not authenticated
  useEffect(() => {
    if (!authLoading && !isAuthenticated) {
      router.push("/auth/login")
    }
  }, [isAuthenticated, authLoading, router])

  // Fetch initial data and connect WebSocket
  useEffect(() => {
    if (!isAuthenticated || !pipelineId) return

    // Fetch pipelines first to ensure we have the pipeline data
    const numericId = isNaN(Number(pipelineId)) ? pipelineId : Number(pipelineId)

    const fetchData = async () => {
      // Always fetch pipelines to get latest status (including on refresh)
      await dispatch(fetchPipelines()).unwrap()

      // After pipelines are loaded, fetch events and metrics
      // Fetch all events (not just today's) to show past events
      dispatch(fetchReplicationEvents({ pipelineId: numericId, limit: 1000, todayOnly: false }))
      dispatch(fetchMonitoringMetrics({ pipelineId: numericId }))

      // Connect WebSocket and subscribe to this pipeline
      // Note: WebSocket is only for real-time updates, unsubscribing does NOT stop the pipeline
      wsClient.connect()
      wsClient.subscribePipeline(numericId)

      // Update WebSocket status immediately
      setWsConnected(wsClient.isConnected())
      setWsAvailable(wsClient.isAvailable())
    }

    fetchData().catch(err => {
      console.error("Failed to fetch pipeline data:", err)
    })


    // Auto-refresh pipeline status every 1 second for real-time status updates
    const pipelineRefreshInterval = setInterval(() => {
      dispatch(fetchPipelines())
    }, 1000)

    // Auto-refresh events every 1 second to show all events in real-time
    const interval = setInterval(() => {
      dispatch(fetchReplicationEvents({ pipelineId: numericId, limit: 1000, todayOnly: false }))
    }, 1000)

    // Update WebSocket status function
    const updateWsStatus = () => {
      const connected = wsClient.isConnected()
      const available = wsClient.isAvailable()
      setWsConnected(connected)
      setWsAvailable(available)
      // Debug logging
      if (connected) {
        console.log('[Monitor] WebSocket is connected - updating status')
      }
    }

    // Check WebSocket connection status periodically (every 2 seconds)
    const wsStatusInterval = setInterval(updateWsStatus, 2000)

    // Also listen for real-time status changes
    const unsubscribeStatus = wsClient.onStatusChange(updateWsStatus)

    // Update immediately
    updateWsStatus()

    // Cleanup: Unsubscribe from WebSocket (this does NOT stop the pipeline, only stops real-time updates)
    return () => {
      clearInterval(interval)
      clearInterval(pipelineRefreshInterval)
      clearInterval(wsStatusInterval)
      unsubscribeStatus() // Remove status listener
      if (pipelineId) {
        // Unsubscribing from WebSocket does NOT stop the pipeline
        // The pipeline continues running in the backend
        wsClient.unsubscribePipeline(numericId)
      }
    }
  }, [dispatch, isAuthenticated, pipelineId])

  // Filter events for this pipeline - handle both string and number IDs
  // Ensure events is an array before filtering
  const pipelineEvents = Array.isArray(events) ? events.filter(e => {
    const eventPipelineId = String(e.pipeline_id || '')
    const currentPipelineId = String(pipelineId || '')
    return eventPipelineId === currentPipelineId
  }) : []

  // Filter metrics for this pipeline - handle both string and number IDs
  // Ensure metrics is an array before filtering
  const pipelineMetrics = Array.isArray(metrics) ? metrics.filter(m => {
    const metricPipelineId = String(m.pipeline_id || '')
    const currentPipelineId = String(pipelineId || '')
    return metricPipelineId === currentPipelineId
  }) : []

  // Prepare chart data
  const chartData = useMemo(() => {
    const last24Hours = pipelineEvents
      .filter(e => {
        if (!e.created_at) return false
        const eventTime = new Date(e.created_at)
        const now = new Date()
        return (now.getTime() - eventTime.getTime()) < 24 * 60 * 60 * 1000
      })
      .slice(-50) // Last 50 events

    return last24Hours.map((event, index) => ({
      time: new Date(event.created_at || '').toLocaleTimeString(),
      latency: event.latency_ms || 0,
      index,
    }))
  }, [pipelineEvents])

  // Event type distribution
  const eventDistribution = useMemo(() => {
    const distribution = {
      insert: 0,
      update: 0,
      delete: 0,
    }
    pipelineEvents.forEach(event => {
      const type = event.event_type?.toLowerCase() || 'insert'
      if (type in distribution) {
        distribution[type as keyof typeof distribution]++
      }
    })
    return Object.entries(distribution).map(([name, value]) => ({
      name: name.toUpperCase(),
      value,
    }))
  }, [pipelineEvents])


  if (!mounted || authLoading || (pipelinesLoading && pipelines.length === 0)) {
    return (
      <div className="flex items-center justify-center h-screen">
        <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
        <span className="ml-2 text-foreground-muted">Loading pipeline...</span>
      </div>
    )
  }

  if (!pipeline && !pipelinesLoading) {
    return (
      <div className="p-6">
        <Button onClick={() => router.push("/pipelines")} variant="outline" className="mb-4">
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Pipelines
        </Button>
        <Card className="p-6 bg-surface border-border">
          <div className="text-center py-8">
            <AlertCircle className="w-12 h-12 text-error mx-auto mb-4 opacity-50" />
            <h3 className="text-lg font-semibold text-foreground mb-2">Pipeline not found</h3>
            <p className="text-foreground-muted mb-4">
              The pipeline with ID "{pipelineId}" could not be found.
            </p>
            <Button onClick={() => router.push("/pipelines")} className="bg-primary hover:bg-primary/90">
              Go to Pipelines
            </Button>
          </div>
        </Card>
      </div>
    )
  }

  if (!pipeline) {
    return (
      <div className="flex items-center justify-center h-screen">
        <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
        <span className="ml-2 text-foreground-muted">Loading...</span>
      </div>
    )
  }

  const successCount = pipelineEvents.filter(e => e.status === 'applied' || e.status === 'success').length
  const failedCount = pipelineEvents.filter(e => e.status === 'failed').length
  // Calculate average latency only from events that have been applied (have latency_ms > 0)
  const eventsWithLatency = pipelineEvents.filter(e => e.latency_ms != null && e.latency_ms !== undefined && e.latency_ms > 0)
  const avgLatency = eventsWithLatency.length > 0
    ? eventsWithLatency.reduce((sum, e) => sum + (e.latency_ms || 0), 0) / eventsWithLatency.length
    : 0

  return (
    <div className="p-6 space-y-6" suppressHydrationWarning>
      <div className="space-y-4">
        <Button onClick={() => router.push("/pipelines")} variant="outline">
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back
        </Button>
        <PageHeader
          title={`${pipeline.name} - Monitor`}
          subtitle="Real-time CDC event monitoring"
          icon={Eye}
          action={
            <div className="flex items-center gap-2">
              {/* WebSocket Connection Status - Always show when attempting connection */}
              <Badge
                className={
                  wsConnected
                    ? "bg-success/20 text-success border border-success/30 shadow-sm cursor-pointer hover:bg-success/30"
                    : wsAvailable
                      ? "bg-warning/20 text-warning border border-warning/30 shadow-sm cursor-pointer hover:bg-warning/30"
                      : "bg-blue-500/15 text-blue-400 border border-blue-400/40 shadow-sm font-semibold cursor-pointer hover:bg-blue-500/25"
                }
                onClick={() => {
                  if (!wsConnected && !wsAvailable) {
                    // Retry connection if unavailable
                    wsClient.retryConnection();
                    setWsAvailable(true);
                    setWsConnected(false);
                  }
                }}
                title={!wsConnected && !wsAvailable ? "Click to retry WebSocket connection" : undefined}
              >
                {wsConnected ? (
                  <>
                    <Activity className="w-3 h-3 mr-1.5 animate-pulse" />
                    <span className="font-medium">WebSocket Connected</span>
                  </>
                ) : wsAvailable ? (
                  <>
                    <RotateCw className="w-3 h-3 mr-1.5 animate-spin" />
                    <span className="font-medium">Connecting WebSocket...</span>
                  </>
                ) : (
                  <>
                    <AlertCircle className="w-3.5 h-3.5 mr-1.5 text-blue-400" />
                    <span className="font-semibold">WebSocket Unavailable (Click to Retry)</span>
                  </>
                )}
              </Badge>
              <Button
                variant="outline"
                size="sm"
                onClick={() => {
                  if (!pipelineId) return
                  const numericId = isNaN(Number(pipelineId)) ? String(pipelineId) : Number(pipelineId)
                  dispatch(fetchReplicationEvents({ pipelineId: numericId, limit: 1000 }))
                  dispatch(fetchMonitoringMetrics({ pipelineId: numericId }))
                }}
                disabled={isLoading}
              >
                <RefreshCw className={`w-4 h-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
          }
        />
      </div>

      {/* Metrics Cards - Enhanced with gradients */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card className="p-4 bg-card border-l-4 border-l-blue-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
          <div className="absolute top-0 right-0 p-3 opacity-5 group-hover:opacity-10 transition-opacity">
            <Database className="w-16 h-16 text-blue-500" />
          </div>
          <div className="flex items-center gap-3 mb-2 relative z-10">
            <div className="p-2 bg-blue-500/10 rounded-lg group-hover:scale-110 transition-transform">
              <Database className="w-4 h-4 text-blue-500" />
            </div>
            <span className="text-xs font-bold text-blue-500 uppercase tracking-wide">Total Events</span>
          </div>
          <div className="flex items-baseline gap-2 relative z-10">
            <span className="text-2xl font-bold text-foreground">{pipelineEvents.length}</span>
          </div>
        </Card>

        <Card className="p-4 bg-card border-l-4 border-l-green-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
          <div className="absolute top-0 right-0 p-3 opacity-5 group-hover:opacity-10 transition-opacity">
            <CheckCircle className="w-16 h-16 text-green-500" />
          </div>
          <div className="flex items-center gap-3 mb-2 relative z-10">
            <div className="p-2 bg-green-500/10 rounded-lg group-hover:scale-110 transition-transform">
              <CheckCircle className="w-4 h-4 text-green-500" />
            </div>
            <span className="text-xs font-bold text-green-500 uppercase tracking-wide">Success Rate</span>
          </div>
          <div className="flex items-baseline gap-2 relative z-10">
            <span className="text-2xl font-bold text-foreground">
              {pipelineEvents.length > 0
                ? Math.round((successCount / pipelineEvents.length) * 100)
                : 0}%
            </span>
          </div>
        </Card>

        <Card className="p-4 bg-card border-l-4 border-l-red-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
          <div className="absolute top-0 right-0 p-3 opacity-5 group-hover:opacity-10 transition-opacity">
            <AlertCircle className="w-16 h-16 text-red-500" />
          </div>
          <div className="flex items-center gap-3 mb-2 relative z-10">
            <div className="p-2 bg-red-500/10 rounded-lg group-hover:scale-110 transition-transform">
              <AlertCircle className="w-4 h-4 text-red-500" />
            </div>
            <span className="text-xs font-bold text-red-500 uppercase tracking-wide">Failed Events</span>
          </div>
          <div className="flex items-baseline gap-2 relative z-10">
            <span className="text-2xl font-bold text-foreground">{failedCount}</span>
          </div>
        </Card>

        <Card className="p-4 bg-card border-l-4 border-l-purple-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
          <div className="absolute top-0 right-0 p-3 opacity-5 group-hover:opacity-10 transition-opacity">
            <Activity className="w-16 h-16 text-purple-500" />
          </div>
          <div className="flex items-center gap-3 mb-2 relative z-10">
            <div className="p-2 bg-purple-500/10 rounded-lg group-hover:scale-110 transition-transform">
              <Activity className="w-4 h-4 text-purple-500" />
            </div>
            <span className="text-xs font-bold text-purple-500 uppercase tracking-wide">Avg Latency</span>
          </div>
          <div className="flex items-baseline gap-2 relative z-10">
            <span className="text-2xl font-bold text-foreground">
              {avgLatency > 0 ? `${avgLatency.toFixed(0)}ms` : '0ms'}
            </span>
          </div>
        </Card>
      </div>

      {/* Charts - Enhanced visibility */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="p-6 bg-surface border-border shadow-lg flex flex-col items-center justify-center">
          <h3 className="text-lg font-bold text-foreground mb-4 flex items-center gap-2 self-start">
            <Activity className="w-5 h-5 text-green-400" />
            Sync Health
          </h3>
          <div className="relative w-full h-[200px] flex items-center justify-center">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={[
                    { name: 'Progress', value: successCount },
                    { name: 'Remaining', value: Math.max(0, pipelineEvents.length - successCount) }
                  ]}
                  cx="50%"
                  cy="80%"
                  startAngle={180}
                  endAngle={0}
                  innerRadius={80}
                  outerRadius={100}
                  paddingAngle={0}
                  dataKey="value"
                >
                  <Cell fill="rgb(34, 197, 94)" />
                  <Cell fill="rgba(34, 197, 94, 0.1)" />
                </Pie>
              </PieChart>
            </ResponsiveContainer>
            <div className="absolute bottom-[20%] text-center">
              <p className="text-3xl font-bold text-foreground">
                {pipelineEvents.length > 0 ? Math.round((successCount / pipelineEvents.length) * 100) : 0}%
              </p>
              <p className="text-xs text-foreground-muted uppercase tracking-wider font-semibold">Success Rate</p>
            </div>
          </div>
        </Card>

        <Card className="p-6 bg-surface border-border shadow-lg">
          <h3 className="text-lg font-bold text-foreground mb-4 flex items-center gap-2">
            <Activity className="w-5 h-5 text-teal-400" />
            Event Latency (24h)
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.2)" />
              <XAxis
                dataKey="time"
                stroke="rgb(148, 163, 184)"
                tick={{ fill: 'rgb(148, 163, 184)', fontSize: 13, fontWeight: '600' }}
                label={{ value: 'Time', position: 'insideBottom', offset: -5, fill: 'rgb(148, 163, 184)', fontSize: 13, fontWeight: '600' }}
              />
              <YAxis
                stroke="rgb(148, 163, 184)"
                tick={{ fill: 'rgb(148, 163, 184)', fontSize: 13, fontWeight: '600' }}
                label={{ value: 'Latency (ms)', angle: -90, position: 'insideLeft', fill: 'rgb(148, 163, 184)', fontSize: 13, fontWeight: '600' }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(15, 23, 42, 0.95)',
                  border: '1px solid rgb(45, 212, 191)',
                  borderRadius: '8px',
                  color: 'rgb(45, 212, 191)',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
                }}
                labelStyle={{ color: 'rgb(45, 212, 191)', fontWeight: 'bold', fontSize: '14px' }}
                itemStyle={{ color: 'rgb(45, 212, 191)', fontSize: '13px' }}
              />
              <Legend
                wrapperStyle={{ color: 'rgb(148, 163, 184)', fontWeight: '600' }}
              />

              <Line
                type="monotone"
                dataKey="latency"
                stroke="rgb(45, 212, 191)"
                strokeWidth={3}
                dot={{ fill: 'rgb(45, 212, 191)', r: 4 }}
                activeDot={{ r: 6, fill: 'rgb(20, 184, 166)' }}
                name="Latency (ms)"
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </Card>

        <Card className="p-6 bg-surface border-border shadow-lg">
          <h3 className="text-lg font-bold text-foreground mb-4 flex items-center gap-2">
            <Database className="w-5 h-5 text-blue-400" />
            Event Type Distribution
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={eventDistribution}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.2)" />
              <XAxis
                dataKey="name"
                stroke="rgb(148, 163, 184)"
                tick={{ fill: 'rgb(148, 163, 184)', fontSize: 13, fontWeight: '600' }}
                label={{ value: 'Event Type', position: 'insideBottom', offset: -5, fill: 'rgb(148, 163, 184)', fontSize: 13, fontWeight: '600' }}
              />
              <YAxis
                stroke="rgb(148, 163, 184)"
                tick={{ fill: 'rgb(148, 163, 184)', fontSize: 13, fontWeight: '600' }}
                label={{ value: 'Count', angle: -90, position: 'insideLeft', fill: 'rgb(148, 163, 184)', fontSize: 13, fontWeight: '600' }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(15, 23, 42, 0.95)',
                  border: '1px solid rgb(59, 130, 246)',
                  borderRadius: '8px',
                  color: 'rgb(59, 130, 246)',
                  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
                }}
                labelStyle={{ color: 'rgb(59, 130, 246)', fontWeight: 'bold', fontSize: '14px' }}
                itemStyle={{ color: 'rgb(59, 130, 246)', fontSize: '13px' }}
              />

              <Bar
                dataKey="value"
                fill="url(#colorGradient)"
                radius={[8, 8, 0, 0]}
                isAnimationActive={false}
                label={{
                  position: 'top',
                  fill: 'rgb(148, 163, 184)',
                  fontSize: 13,
                  fontWeight: 'bold'
                }}
              >
                <defs>
                  <linearGradient id="colorGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="rgb(59, 130, 246)" stopOpacity={0.8} />
                    <stop offset="100%" stopColor="rgb(37, 99, 235)" stopOpacity={0.6} />
                  </linearGradient>
                </defs>
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>

      {/* Recent Events Table - Enhanced */}
      <Card className="p-6 bg-surface border-border shadow-lg">
        <h3 className="text-lg font-bold text-foreground mb-4 flex items-center gap-2">
          <Activity className="w-5 h-5 text-teal-400" />
          Recent CDC Events
        </h3>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b-2 border-border/50">
                <th className="text-left p-3 text-sm font-bold text-foreground-muted uppercase tracking-wider">Time</th>
                <th className="text-left p-3 text-sm font-bold text-foreground-muted uppercase tracking-wider">Type</th>
                <th className="text-left p-3 text-sm font-bold text-foreground-muted uppercase tracking-wider">Table</th>
                <th className="text-left p-3 text-sm font-bold text-foreground-muted uppercase tracking-wider">LSN/Offset</th>
                <th className="text-left p-3 text-sm font-bold text-foreground-muted uppercase tracking-wider">Status</th>
                <th className="text-left p-3 text-sm font-bold text-foreground-muted uppercase tracking-wider">Latency</th>
                <th className="text-left p-3 text-sm font-bold text-foreground-muted uppercase tracking-wider">Error</th>
              </tr>
            </thead>
            <tbody>
              {/* Pagination Implementation */}
              {(() => {
                const totalPages = Math.ceil(pipelineEvents.length / itemsPerPage)
                const startIndex = (currentPage - 1) * itemsPerPage
                const endIndex = startIndex + itemsPerPage
                const currentEvents = pipelineEvents.slice(startIndex, endIndex)

                if (pipelineEvents.length === 0) {
                  return (
                    <tr>
                      <td colSpan={7}>
                        <div className="text-center py-12 text-foreground-muted">
                          <Database className="w-12 h-12 mx-auto mb-3 opacity-30" />
                          <p className="text-sm">No events yet. Events will appear here in real-time when CDC is active.</p>
                        </div>
                      </td>
                    </tr>
                  )
                }

                return (
                  <>
                    {currentEvents.map((event) => {
                      const isFailed = event.status === 'failed' || event.status === 'error'
                      const isCaptured = event.status === 'captured'
                      const isApplied = event.status === 'applied' || event.status === 'success'

                      return (
                        <tr
                          key={event.id}
                          className={`border-b border-border/30 hover:bg-surface-hover/50 transition-colors duration-150 ${isFailed ? 'bg-red-500/5 border-red-500/20' :
                            isCaptured ? 'bg-blue-500/5 border-blue-500/10' :
                              ''
                            }`}
                        >
                          <td className="p-3 text-sm font-medium text-foreground">
                            <div className="flex items-center gap-2">
                              {isCaptured && (
                                <div className="relative">
                                  <div className="w-2 h-2 bg-cyan-400 rounded-full animate-pulse" />
                                  <div className="absolute inset-0 w-2 h-2 bg-cyan-400/50 rounded-full animate-ping" />
                                </div>
                              )}
                              {event.created_at
                                ? new Date(event.created_at).toLocaleString()
                                : 'N/A'}
                            </div>
                          </td>
                          <td className="p-3">
                            <Badge
                              className={
                                event.event_type === 'insert'
                                  ? 'bg-green-500/20 text-green-400 border border-green-500/30 font-semibold'
                                  : event.event_type === 'update'
                                    ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30 font-semibold'
                                    : 'bg-amber-500/20 text-amber-400 border border-amber-500/30 font-semibold'
                              }
                            >
                              {event.event_type?.toUpperCase() || 'UNKNOWN'}
                            </Badge>
                          </td>
                          <td className="p-3 text-sm font-medium text-foreground">{event.table_name || 'N/A'}</td>
                          <td className="p-3 text-sm font-mono">
                            {(() => {
                              // Try to extract LSN/Offset from event fields first
                              let lsnValue = event.source_lsn || event.sql_server_lsn
                              let scnValue: string | number | null | undefined = event.source_scn
                              let binlogFile = event.source_binlog_file
                              let binlogPos = event.source_binlog_position

                              // If not found, try to extract from run_metadata (fallback)
                              if (!lsnValue && !scnValue && !binlogFile && (event as any).run_metadata) {
                                const metadata = (event as any).run_metadata
                                if (typeof metadata === 'object' && metadata !== null) {
                                  // Try common LSN keys
                                  lsnValue = metadata.source_lsn || metadata.lsn || metadata.offset ||
                                    metadata.transaction_id || metadata.txId ||
                                    metadata.last_lsn || metadata.current_lsn || metadata.commit_lsn

                                  // Try SCN keys
                                  scnValue = metadata.source_scn || metadata.scn || metadata.current_scn

                                  // Try binlog keys
                                  binlogFile = metadata.source_binlog_file || metadata.binlog_file || metadata.file
                                  binlogPos = metadata.source_binlog_position || metadata.binlog_position ||
                                    metadata.pos || metadata.position

                                  // Try nested offset structure
                                  if (!lsnValue && metadata.offset) {
                                    if (typeof metadata.offset === 'object' && metadata.offset !== null) {
                                      lsnValue = metadata.offset.lsn || metadata.offset.transaction_id ||
                                        metadata.offset.txId || metadata.offset.last_lsn
                                      scnValue = scnValue || metadata.offset.scn
                                      binlogFile = binlogFile || metadata.offset.file || metadata.offset.binlog_file
                                      binlogPos = binlogPos || metadata.offset.pos || metadata.offset.position
                                    } else if (typeof metadata.offset === 'string' || typeof metadata.offset === 'number') {
                                      lsnValue = String(metadata.offset)
                                    }
                                  }

                                  // Deep search in nested structures
                                  if (!lsnValue && !scnValue && !binlogFile) {
                                    const deepSearch = (obj: any, depth = 0): any => {
                                      if (depth > 3 || !obj || typeof obj !== 'object') return null

                                      for (const [key, value] of Object.entries(obj)) {
                                        const keyLower = String(key).toLowerCase()
                                        if (keyLower.includes('lsn') && value && typeof value !== 'object') {
                                          return String(value)
                                        }
                                        if (keyLower.includes('scn') && value && typeof value !== 'object') {
                                          scnValue = String(value)
                                        }
                                        if (keyLower.includes('binlog') && value && typeof value !== 'object') {
                                          binlogFile = String(value)
                                        }
                                        if (typeof value === 'object' && value !== null) {
                                          const found = deepSearch(value, depth + 1)
                                          if (found) return found
                                        }
                                      }
                                      return null
                                    }

                                    const foundLsn = deepSearch(metadata)
                                    if (foundLsn) lsnValue = foundLsn
                                  }
                                }
                              }

                              // Display the found value - show ANY available value (LSN, SCN, or Offset)
                              // Priority: LSN > SCN > Binlog > SQL Server LSN > Offset > N/A
                              if (lsnValue) {
                                return (
                                  <span className="text-cyan-400" title="LSN (Log Sequence Number)">
                                    LSN: {String(lsnValue)}
                                  </span>
                                )
                              } else if (scnValue) {
                                return (
                                  <span className="text-blue-400" title="SCN (System Change Number)">
                                    SCN: {String(scnValue)}
                                  </span>
                                )
                              } else if (binlogFile) {
                                return (
                                  <span className="text-green-400" title="MySQL Binlog Position">
                                    {String(binlogFile)}{binlogPos ? `:${binlogPos}` : ''}
                                  </span>
                                )
                              } else if (event.sql_server_lsn) {
                                return (
                                  <span className="text-purple-400" title="SQL Server LSN">
                                    LSN: {String(event.sql_server_lsn)}
                                  </span>
                                )
                              } else {
                                // Last resort: try to find any offset value in run_metadata
                                let offsetValue = null
                                if ((event as any).run_metadata) {
                                  const metadata = (event as any).run_metadata
                                  if (typeof metadata === 'object' && metadata !== null) {
                                    // Try to find any offset-like value
                                    offsetValue = metadata.offset || metadata.transaction_id ||
                                      metadata.txId || metadata.checkpoint ||
                                      metadata.last_offset || metadata.current_offset

                                    // If offset is nested, extract it
                                    if (!offsetValue && metadata.offset && typeof metadata.offset === 'object') {
                                      offsetValue = JSON.stringify(metadata.offset).substring(0, 50)
                                    }
                                  }
                                }

                                if (offsetValue) {
                                  const offsetStr = typeof offsetValue === 'object' ? JSON.stringify(offsetValue).substring(0, 50) : String(offsetValue)
                                  return (
                                    <span className="text-yellow-400" title="Offset/Checkpoint Value">
                                      Offset: {offsetStr.length > 50 ? offsetStr + '...' : offsetStr}
                                    </span>
                                  )
                                }

                                return <span className="text-foreground-muted">N/A</span>
                              }
                            })()}
                          </td>
                          <td className="p-3">
                            <Badge
                              className={
                                isApplied
                                  ? 'bg-green-500/20 text-green-400 border border-green-500/30 font-semibold'
                                  : isFailed
                                    ? 'bg-red-500/20 text-red-400 border border-red-500/30 font-semibold animate-pulse'
                                    : 'bg-yellow-500/20 text-yellow-400 border border-yellow-500/30 font-semibold'
                              }
                            >
                              {isApplied && <CheckCircle className="w-3 h-3 mr-1" />}
                              {isFailed && <AlertCircle className="w-3 h-3 mr-1" />}
                              {isCaptured && <Activity className="w-3 h-3 mr-1 animate-pulse" />}
                              {event.status || 'pending'}
                            </Badge>
                          </td>
                          <td className="p-3 text-sm font-medium text-foreground">
                            {event.latency_ms ? (
                              <span className={event.latency_ms > 5000 ? 'text-warning' : 'text-foreground'}>
                                {event.latency_ms.toFixed(0)}ms
                              </span>
                            ) : 'N/A'}
                          </td>
                          <td className="p-3 text-xs max-w-xs">
                            {isFailed && event.error_message ? (
                              <div className="flex items-center gap-2">
                                <div className="group relative flex-1">
                                  <div className="flex items-center gap-1 text-red-400 cursor-help">
                                    <AlertCircle className="w-4 h-4 flex-shrink-0" />
                                    <span className="truncate">View Error</span>
                                  </div>
                                  <div className="absolute left-0 top-full mt-1 z-50 hidden group-hover:block">
                                    <div className="bg-red-950/95 border border-red-500/50 rounded-lg p-3 shadow-xl max-w-md">
                                      <div className="text-red-200 font-semibold mb-1 text-xs">Replication Failed</div>
                                      <div className="text-red-300 text-xs whitespace-pre-wrap break-words">
                                        {event.error_message.length > 200
                                          ? event.error_message.substring(0, 200) + '...'
                                          : event.error_message}
                                      </div>
                                      <div className="text-red-400/70 text-xs mt-2">
                                        {event.table_name} | {event.event_type}
                                      </div>
                                    </div>
                                  </div>
                                </div>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 px-2 text-red-400 hover:text-red-300 hover:bg-red-500/10"
                                  onClick={async () => {
                                    if (!event.id) return
                                    setRetryingEventId(event.id)
                                    try {
                                      await apiClient.retryFailedEvent(event.id)
                                      // Refresh events
                                      if (pipelineId) {
                                        dispatch(fetchReplicationEvents({
                                          pipelineId: isNaN(Number(pipelineId)) ? String(pipelineId) : Number(pipelineId),
                                          limit: 1000
                                        }))
                                      }
                                    } catch (error: any) {
                                      alert(`Failed to retry event: ${error.message || 'Unknown error'}`)
                                    } finally {
                                      setRetryingEventId(null)
                                    }
                                  }}
                                  disabled={retryingEventId === event.id}
                                  title="Retry this failed event"
                                >
                                  {retryingEventId === event.id ? (
                                    <Loader2 className="w-3 h-3 animate-spin" />
                                  ) : (
                                    <RotateCw className="w-3 h-3" />
                                  )}
                                </Button>
                              </div>
                            ) : isApplied ? (
                              <div className="flex items-center gap-1 text-green-400">
                                <CheckCircle className="w-4 h-4" />
                                <span>Success</span>
                              </div>
                            ) : isCaptured ? (
                              <div className="flex items-center gap-1 text-cyan-400">
                                <Activity className="w-4 h-4 animate-pulse" />
                                <span>Captured</span>
                              </div>
                            ) : (
                              <span className="text-foreground-muted">-</span>
                            )}
                          </td>
                        </tr>
                      )
                    })}
                  </>
                )
              })()}
            </tbody>
          </table>
          {/* Pagination Controls - Enhanced hover states */}
          {pipelineEvents.length > 0 && (
            <div className="flex items-center justify-between mt-6 border-t border-border/50 pt-4">
              <span className="text-sm font-medium text-foreground-muted">
                Showing <span className="text-foreground font-bold">{((currentPage - 1) * itemsPerPage) + 1}</span> to <span className="text-foreground font-bold">{Math.min(currentPage * itemsPerPage, pipelineEvents.length)}</span> of <span className="text-foreground font-bold">{pipelineEvents.length}</span> entries
              </span>
              <div className="flex items-center gap-3">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(prev => Math.max(prev - 1, 1))}
                  disabled={currentPage === 1}
                  className="bg-transparent border-border hover:bg-teal-500/10 hover:border-teal-500/50 hover:text-teal-400 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200 font-semibold"
                >
                  Previous
                </Button>
                <span className="text-sm font-bold text-foreground px-3 py-1 bg-surface-hover rounded-md">
                  Page {currentPage} of {Math.max(Math.ceil(pipelineEvents.length / itemsPerPage), 1)}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(prev => Math.min(prev + 1, Math.ceil(pipelineEvents.length / itemsPerPage)))}
                  disabled={currentPage === Math.ceil(pipelineEvents.length / itemsPerPage)}
                  className="bg-transparent border-border hover:bg-teal-500/10 hover:border-teal-500/50 hover:text-teal-400 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200 font-semibold"
                >
                  Next
                </Button>
              </div>
            </div>
          )}
        </div>
      </Card>
    </div>
  )
}

