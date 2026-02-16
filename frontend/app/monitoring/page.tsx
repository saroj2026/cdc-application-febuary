"use client"

import { ProtectedPage } from "@/components/auth/ProtectedPage"

import { useState, useEffect, useMemo, useRef } from "react"
import { useRouter } from "next/navigation"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { fetchReplicationEvents, fetchMonitoringMetrics, setSelectedPipeline } from "@/lib/store/slices/monitoringSlice"
import { fetchPipelines } from "@/lib/store/slices/pipelineSlice"
import { wsClient } from "@/lib/websocket/client"
import { formatDistanceToNow } from "date-fns"
import { Loader2, Activity, Database, AlertCircle, CheckCircle, RefreshCw, Eye, RotateCw, ArrowRight, Zap, Clock, Trash2, Calendar, FileText } from "lucide-react"
import { apiClient } from "@/lib/api/client"
import { PageHeader } from "@/components/ui/page-header"
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from "recharts"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { cn } from "@/lib/utils"

export default function MonitoringPage() {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const { isAuthenticated, isLoading: authLoading } = useAppSelector((state) => state.auth)
  const { events, metrics, selectedPipelineId, isLoading } = useAppSelector((state) => state.monitoring)
  const { pipelines } = useAppSelector((state) => state.pipelines)
  const [mounted, setMounted] = useState(false)
  const [currentPage, setCurrentPage] = useState(1)
  const rowsPerPage = 10
  const [retryingEventId, setRetryingEventId] = useState<string | null>(null)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [wsConnected, setWsConnected] = useState(false)
  const [wsAvailable, setWsAvailable] = useState(true) // Default to true to show status
  const [searchTerm, setSearchTerm] = useState("")

  // Handle client-side mounting
  useEffect(() => {
    setMounted(true)
  }, [])

  useEffect(() => {
    if (!authLoading && !isAuthenticated) {
      router.push("/auth/login")
    }
  }, [isAuthenticated, authLoading, router])

  // Track if user has explicitly selected "All Pipelines" to prevent auto-selection
  const userSelectedAllRef = useRef(false)

  // Auto-select first pipeline when pipelines load and selectedPipelineId is null
  useEffect(() => {
    if (isAuthenticated && pipelines.length > 0 && !selectedPipelineId && !userSelectedAllRef.current) {
      const firstPipeline = pipelines[0]
      if (firstPipeline && firstPipeline.id) {
        const pipelineId = !isNaN(Number(firstPipeline.id)) ? Number(firstPipeline.id) : String(firstPipeline.id)
        dispatch(setSelectedPipeline(pipelineId))
      }
    }
  }, [dispatch, isAuthenticated, pipelines, selectedPipelineId])

  // Fetch data on mount
  useEffect(() => {
    if (isAuthenticated) {
      dispatch(fetchPipelines())
      dispatch(fetchReplicationEvents({ limit: 100000, todayOnly: false }))

      wsClient.connect()
      setWsConnected(wsClient.isConnected())
      setWsAvailable(wsClient.isAvailable())
    }
  }, [dispatch, isAuthenticated])

  // Check WebSocket connection status
  useEffect(() => {
    if (!isAuthenticated) return

    const updateWsStatus = () => {
      const connected = wsClient.isConnected()
      const available = wsClient.isAvailable()
      setWsConnected(connected)
      setWsAvailable(available)
    }

    const wsStatusInterval = setInterval(updateWsStatus, 2000)
    const unsubscribeStatus = wsClient.onStatusChange(updateWsStatus)
    updateWsStatus()

    return () => {
      clearInterval(wsStatusInterval)
      unsubscribeStatus()
    }
  }, [isAuthenticated])

  // Track last fetched pipeline ID to prevent unnecessary refetches
  const lastFetchedPipelineIdRef = useRef<string | number | null>(null)
  const isInitialMountRef = useRef(true)

  const stableSelectedPipelineId = useMemo(() => {
    if (!selectedPipelineId) return null
    return String(selectedPipelineId)
  }, [selectedPipelineId])

  // Fetch events and metrics when pipeline is selected
  useEffect(() => {
    if (!isAuthenticated) return

    const normalizedId = stableSelectedPipelineId ? stableSelectedPipelineId : null

    if (isInitialMountRef.current) {
      isInitialMountRef.current = false
    } else {
      if (lastFetchedPipelineIdRef.current === normalizedId && normalizedId !== null) {
        return
      }
    }

    lastFetchedPipelineIdRef.current = normalizedId

    if (normalizedId) {
      const pipelineId = !isNaN(Number(normalizedId)) ? Number(normalizedId) : normalizedId
      if (pipelineId && pipelineId !== 'null' && pipelineId !== 'undefined') {
        dispatch(fetchReplicationEvents({ pipelineId, limit: 100000, todayOnly: false }))
        dispatch(fetchMonitoringMetrics({ pipelineId })).catch(err => {
          console.error("Error fetching metrics:", err)
        })
      }
    } else {
      dispatch(fetchReplicationEvents({ limit: 100000, todayOnly: false }))
    }
  }, [dispatch, isAuthenticated, stableSelectedPipelineId])

  // Auto-refresh events
  const selectedPipelineIdRef = useRef<string | number | null>(null)

  useEffect(() => {
    selectedPipelineIdRef.current = stableSelectedPipelineId
  }, [stableSelectedPipelineId])

  useEffect(() => {
    if (!isAuthenticated) return

    const interval = setInterval(() => {
      const currentPipelineId = selectedPipelineIdRef.current
      if (currentPipelineId) {
        const pipelineId = !isNaN(Number(currentPipelineId)) ? Number(currentPipelineId) : String(currentPipelineId)
        if (pipelineId && pipelineId !== 'null' && pipelineId !== 'undefined') {
          dispatch(fetchReplicationEvents({ pipelineId, limit: 1000, todayOnly: false }))
          dispatch(fetchMonitoringMetrics({ pipelineId }))
        }
      } else {
        dispatch(fetchReplicationEvents({ limit: 1000, todayOnly: false }))
      }
    }, 5000)

    return () => clearInterval(interval)
  }, [dispatch, isAuthenticated])

  // Subscribe to pipeline room
  useEffect(() => {
    if (!isAuthenticated || !wsClient.isConnected()) return

    if (selectedPipelineId) {
      const pipelineId = !isNaN(Number(selectedPipelineId)) ? Number(selectedPipelineId) : String(selectedPipelineId)
      wsClient.subscribePipeline(pipelineId)
      return () => wsClient.unsubscribePipeline(pipelineId)
    } else {
      const pipelinesArray = Array.isArray(pipelines) ? pipelines : []
      if (pipelinesArray.length > 0) {
        const pipelineIds = pipelinesArray
          .filter(p => p.id)
          .map(p => !isNaN(Number(p.id)) ? Number(p.id) : String(p.id))

        pipelineIds.forEach(id => wsClient.subscribePipeline(id))
        return () => pipelineIds.forEach(id => wsClient.unsubscribePipeline(id))
      }
    }
  }, [isAuthenticated, selectedPipelineId, pipelines, wsClient])

  const filteredEvents = useMemo(() => {
    const eventsArray = Array.isArray(events) ? events : []
    let filtered = eventsArray

    if (selectedPipelineId) {
      filtered = filtered.filter(e => String(e.pipeline_id) === String(selectedPipelineId))
    }

    if (searchTerm) {
      const term = searchTerm.toLowerCase()
      filtered = filtered.filter(e =>
        (e.table_name || "").toLowerCase().includes(term) ||
        (e.event_type || "").toLowerCase().includes(term) ||
        (e.status || "").toLowerCase().includes(term)
      )
    }

    return filtered
  }, [events, selectedPipelineId, searchTerm])

  const chartData = useMemo(() => {
    const eventsArray = Array.isArray(events) ? events : []
    const _metricsArray = Array.isArray(metrics) ? metrics : []

    const now = new Date()
    // Create 12 buckets of 5 minutes for the last hour
    const buckets = Array.from({ length: 12 }, (_, i) => {
      const bucketTime = new Date(now.getTime() - (11 - i) * 5 * 60 * 1000)
      return bucketTime
    })

    return buckets.map(bucket => {
      const bucketStart = new Date(bucket)
      const bucketEnd = new Date(bucket.getTime() + 5 * 60 * 1000)

      const bucketEvents = eventsArray.filter(e => {
        try {
          const eventTime = new Date(e.created_at || e.source_commit_time || Date.now())
          return eventTime >= bucketStart && eventTime < bucketEnd
        } catch { return false }
      })

      return {
        time: bucket.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        events: bucketEvents.length,
        latency: bucketEvents.reduce((sum, e) => sum + (e.latency_ms || 0), 0) / (bucketEvents.length || 1)
      }
    })
  }, [events, metrics])

  const stats = useMemo(() => {
    const totalEvents = filteredEvents.length
    const appliedEvents = filteredEvents.filter(e => e.status === 'applied' || e.status === 'success').length
    const pendingEvents = filteredEvents.filter(e => e.status === 'captured' || e.status === 'pending').length
    const failedEvents = filteredEvents.filter(e => e.status === 'failed' || e.status === 'error').length

    // Calculate average latency
    const eventsWithLatency = filteredEvents.filter(e => typeof e.latency_ms === 'number')
    const avgLatency = eventsWithLatency.length > 0
      ? Math.round(eventsWithLatency.reduce((sum, e) => sum + (e.latency_ms || 0), 0) / eventsWithLatency.length)
      : 0

    return { totalEvents, appliedEvents, pendingEvents, failedEvents, avgLatency }
  }, [filteredEvents])

  const handleRefresh = () => {
    setIsRefreshing(true)
    const pipelineId = selectedPipelineId
      ? (!isNaN(Number(selectedPipelineId)) ? Number(selectedPipelineId) : String(selectedPipelineId))
      : undefined

    Promise.all([
      dispatch(fetchReplicationEvents({
        pipelineId,
        limit: 100000,
        todayOnly: false
      })),
      pipelineId ? dispatch(fetchMonitoringMetrics({ pipelineId })) : Promise.resolve()
    ]).finally(() => {
      setTimeout(() => setIsRefreshing(false), 500)
    })
  }

  const handleDownloadCSV = () => {
    const headers = ["Timestamp", "Operation", "Table", "LSN", "Latency (ms)", "Status"]
    const rows = filteredEvents.map(e => [
      e.created_at ? new Date(e.created_at).toISOString() : "",
      e.event_type || "UNKNOWN",
      e.table_name || "N/A",
      e.source_lsn || "-",
      e.latency_ms || 0,
      e.status
    ])

    const csvContent = [headers, ...rows].map(row => row.join(",")).join("\n")
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" })
    const url = URL.createObjectURL(blob)
    const link = document.body.appendChild(document.createElement("a"))
    link.href = url
    link.download = `cdc_events_${new Date().toISOString()}.csv`
    link.click()
    document.body.removeChild(link)
  }

  // Pagination logic
  const totalPages = Math.ceil(filteredEvents.length / rowsPerPage)
  const currentEvents = filteredEvents.slice((currentPage - 1) * rowsPerPage, currentPage * rowsPerPage)

  if (!mounted) return null

  return (
    <ProtectedPage path="/monitoring" requiredPermission="view_monitoring">
      <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
        <PageHeader
          title="Real-time Monitoring"
          subtitle="Live CDC event stream and performance metrics"
          icon={Activity}
          action={
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-2 px-3 py-1.5 bg-card border border-border rounded-md shadow-sm">
                <div className={`w-2 h-2 rounded-full ${wsConnected ? 'bg-success animate-pulse' : 'bg-error'}`} />
                <span className="text-xs font-medium text-foreground-muted">
                  {wsConnected ? 'Live Updates' : 'Disconnected'}
                </span>
              </div>
              <Select
                value={selectedPipelineId ? String(selectedPipelineId) : "all"}
                onValueChange={(value) => {
                  if (value === "all") {
                    userSelectedAllRef.current = true
                    dispatch(setSelectedPipeline(null))
                  } else {
                    userSelectedAllRef.current = false
                    const id = !isNaN(Number(value)) ? Number(value) : value
                    dispatch(setSelectedPipeline(id))
                  }
                }}
              >
                <SelectTrigger className="w-[200px] h-9 text-xs">
                  <SelectValue placeholder="All Pipelines" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Pipelines</SelectItem>
                  {pipelines.map(p => (
                    <SelectItem key={p.id} value={String(p.id)}>{p.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button
                variant="outline"
                size="sm" // Use small button
                className="h-9 gap-2" // Adjust height
                onClick={handleRefresh}
                disabled={isRefreshing || isLoading}
              >
                <RefreshCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
          }
        />

        {/* Stats Grid - Premium & Deep */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-6">
          {/* Total Events */}
          <Card className="relative overflow-hidden p-6 border-none bg-primary/5 shadow-[0_20px_40px_-10px_rgba(59,130,246,0.2)] dark:shadow-[0_20px_40px_-15px_rgba(0,0,0,0.5)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-4px] hover:bg-primary/10">
            <div className="absolute -right-8 -bottom-8 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.15] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
              <Database className="w-32 h-32 text-primary" />
            </div>
            <div className="flex items-center gap-3 mb-4 relative z-10">
              <div className="p-2.5 bg-primary/10 rounded-xl group-hover:rotate-6 transition-transform">
                <Database className="w-5 h-5 text-primary" />
              </div>
              <span className="text-[10px] font-black text-primary/80 uppercase tracking-[0.2em]">Total Events</span>
            </div>
            <div className="flex items-end justify-between relative z-10">
              <span className="text-4xl font-black text-foreground tracking-tighter drop-shadow-sm">{stats.totalEvents.toLocaleString()}</span>
              {wsConnected && (
                <div className="relative flex h-3 w-3 mb-1">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-3 w-3 bg-primary shadow-[0_0_10px_rgba(59,130,246,0.8)]"></span>
                </div>
              )}
            </div>
          </Card>

          {/* Applied */}
          <Card className="relative overflow-hidden p-6 border-none bg-emerald-500/5 shadow-[0_20px_40px_-10px_rgba(16,185,129,0.2)] dark:shadow-[0_20px_40px_-15px_rgba(0,0,0,0.5)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-4px] hover:bg-emerald-500/10">
            <div className="absolute -right-8 -bottom-8 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.15] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
              <CheckCircle className="w-32 h-32 text-emerald-500" />
            </div>
            <div className="flex items-center gap-3 mb-4 relative z-10">
              <div className="p-2.5 bg-emerald-500/10 rounded-xl group-hover:rotate-6 transition-transform">
                <CheckCircle className="w-5 h-5 text-emerald-500" />
              </div>
              <span className="text-[10px] font-black text-emerald-500/80 uppercase tracking-[0.2em]">Applied</span>
            </div>
            <div className="flex items-end justify-between relative z-10">
              <span className="text-4xl font-black text-foreground tracking-tighter drop-shadow-sm">{stats.appliedEvents.toLocaleString()}</span>
              <Badge className="bg-emerald-500 text-white border-0 text-[10px] font-black tracking-tighter px-1.5 h-5 mb-1">
                {stats.totalEvents > 0 ? Math.round((stats.appliedEvents / stats.totalEvents) * 100) : 0}%
              </Badge>
            </div>
          </Card>

          {/* Pending */}
          <Card className="relative overflow-hidden p-6 border-none bg-amber-500/5 shadow-[0_20px_40px_-10px_rgba(245,158,11,0.2)] dark:shadow-[0_20px_40px_-15px_rgba(0,0,0,0.5)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-4px] hover:bg-amber-500/10">
            <div className="absolute -right-8 -bottom-8 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.15] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
              <RotateCw className="w-32 h-32 text-amber-500" />
            </div>
            <div className="flex items-center gap-3 mb-4 relative z-10">
              <div className="p-2.5 bg-amber-500/10 rounded-xl group-hover:rotate-6 transition-transform">
                <RotateCw className="w-5 h-5 text-amber-500" />
              </div>
              <span className="text-[10px] font-black text-amber-500/80 uppercase tracking-[0.2em]">Pending</span>
            </div>
            <div className="flex items-end relative z-10">
              <span className="text-4xl font-black text-foreground tracking-tighter drop-shadow-sm">{stats.pendingEvents.toLocaleString()}</span>
            </div>
          </Card>

          {/* Latency */}
          <Card className="relative overflow-hidden p-6 border-none bg-info/5 shadow-[0_20px_40px_-10px_rgba(34,211,238,0.2)] dark:shadow-[0_20px_40px_-15px_rgba(0,0,0,0.5)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-4px] hover:bg-info/10">
            <div className="absolute -right-8 -bottom-8 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.15] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
              <Clock className="w-32 h-32 text-info" />
            </div>
            <div className="flex items-center gap-3 mb-4 relative z-10">
              <div className="p-2.5 bg-info/10 rounded-xl group-hover:rotate-6 transition-transform">
                <Clock className="w-5 h-5 text-info" />
              </div>
              <span className="text-[10px] font-black text-info/80 uppercase tracking-[0.2em]">Avg Latency</span>
            </div>
            <div className="flex items-end relative z-10">
              <span className="text-4xl font-black text-foreground tracking-tighter drop-shadow-sm">{stats.avgLatency}<span className="text-xl ml-1 opacity-60">ms</span></span>
            </div>
          </Card>

          {/* Failed */}
          <Card className="relative overflow-hidden p-6 border-none bg-red-500/5 shadow-[0_20px_40px_-10px_rgba(239,68,68,0.2)] dark:shadow-[0_20px_40px_-15px_rgba(0,0,0,0.5)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-4px] hover:bg-red-500/10">
            <div className="absolute -right-8 -bottom-8 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.15] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
              <AlertCircle className="w-32 h-32 text-red-500" />
            </div>
            <div className="flex items-center gap-3 mb-4 relative z-10">
              <div className="p-2.5 bg-red-500/10 rounded-xl group-hover:rotate-6 transition-transform">
                <AlertCircle className="w-5 h-5 text-red-500" />
              </div>
              <span className="text-[10px] font-black text-red-500/80 uppercase tracking-[0.2em]">Failed</span>
            </div>
            <div className="flex items-end justify-between relative z-10">
              <span className="text-4xl font-black text-foreground tracking-tighter drop-shadow-sm">{stats.failedEvents.toLocaleString()}</span>
              {stats.failedEvents > 0 && (
                <Badge variant="destructive" className="animate-pulse font-black text-[10px] px-1.5 h-5 mb-1">CRITICAL</Badge>
              )}
            </div>
          </Card>
        </div>

        {/* Charts & Feed */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card className="p-5 border-border shadow-sm bg-gradient-to-br from-card to-card/50 overflow-hidden relative group">
            <div className="absolute -right-8 -top-8 p-4 opacity-[0.03] group-hover:opacity-[0.06] transition-opacity pointer-events-none group-hover:scale-110 duration-500">
              <Zap className="w-32 h-32 text-primary" />
            </div>
            <div className="flex items-center justify-between mb-6 relative z-10">
              <div>
                <h3 className="text-sm font-bold text-foreground flex items-center gap-2">
                  <Zap className="w-4 h-4 text-primary" />
                  Event Throughput
                </h3>
                <p className="text-xs text-foreground-muted">Events processed per 5 minutes (last hour)</p>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="text-[10px] font-normal bg-primary/5 text-primary border-primary/20">Real-time Stream</Badge>
              </div>
            </div>
            <div className="h-[250px] relative z-10">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <defs>
                    <linearGradient id="colorEvents" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="var(--primary)" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="var(--primary)" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(var(--popover-rgb), 0.9)',
                      backdropFilter: 'blur(8px)',
                      borderColor: 'var(--border)',
                      fontSize: '12px',
                      borderRadius: '12px',
                      boxShadow: '0 8px 16px rgba(0,0,0,0.2)',
                      padding: '12px'
                    }}
                    cursor={{ stroke: 'var(--primary)', strokeWidth: 1, strokeDasharray: '4 4' }}
                  />
                  <XAxis dataKey="time" stroke="var(--foreground-muted)" tickLine={false} axisLine={false} tick={{ fontSize: 10, fill: 'var(--foreground-muted)' }} dy={10} />
                  <YAxis stroke="var(--foreground-muted)" tickLine={false} axisLine={false} tick={{ fontSize: 10, fill: 'var(--foreground-muted)' }} />
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" opacity={0.2} />
                  <Area
                    type="monotone"
                    dataKey="events"
                    stroke="var(--primary)"
                    fill="url(#colorEvents)"
                    strokeWidth={3}
                    activeDot={{ r: 6, strokeWidth: 0, fill: 'var(--primary)' }}
                    animationDuration={1500}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </Card>

          <Card className="p-5 border-border shadow-sm bg-gradient-to-br from-card to-card/50 overflow-hidden relative group">
            <div className="absolute -right-8 -top-8 p-4 opacity-[0.03] group-hover:opacity-[0.06] transition-opacity pointer-events-none group-hover:scale-110 duration-500">
              <Clock className="w-32 h-32 text-amber-500" />
            </div>
            <div className="flex items-center justify-between mb-6 relative z-10">
              <div>
                <h3 className="text-sm font-bold text-foreground flex items-center gap-2">
                  <Clock className="w-4 h-4 text-amber-500" />
                  Processing Latency
                </h3>
                <p className="text-xs text-foreground-muted">Average message processing time (ms)</p>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="text-[10px] font-normal bg-amber-500/5 text-amber-500 border-amber-500/20">System Performance</Badge>
              </div>
            </div>
            <div className="h-[250px] relative z-10">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <defs>
                    <linearGradient id="colorLatency" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.4} />
                      <stop offset="95%" stopColor="#f59e0b" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(var(--popover-rgb), 0.9)',
                      backdropFilter: 'blur(12px)',
                      borderColor: 'var(--border)',
                      fontSize: '12px',
                      borderRadius: '12px',
                      boxShadow: '0 10px 20px rgba(0,0,0,0.3)',
                      padding: '12px'
                    }}
                    cursor={{ stroke: '#f59e0b', strokeWidth: 1, strokeDasharray: '4 4' }}
                  />
                  <XAxis dataKey="time" stroke="var(--foreground-muted)" tickLine={false} axisLine={false} tick={{ fontSize: 10, fill: 'var(--foreground-muted)' }} dy={10} />
                  <YAxis stroke="var(--foreground-muted)" tickLine={false} axisLine={false} tick={{ fontSize: 10, fill: 'var(--foreground-muted)' }} />
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" opacity={0.15} />
                  <Area
                    type="monotone"
                    dataKey="latency"
                    stroke="#f59e0b"
                    fill="url(#colorLatency)"
                    strokeWidth={3}
                    activeDot={{ r: 6, strokeWidth: 0, fill: '#f59e0b' }}
                    animationDuration={2000}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </Card>
        </div>

        {/* CDC Event Summary & Live Feed */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Event Summary Side Panel */}
          <Card className="lg:col-span-1 p-0 border-border shadow-md bg-surface h-fit sticky top-6">
            <div className="p-4 border-b border-border bg-muted/20">
              <h3 className="font-bold text-foreground flex items-center gap-2">
                <Activity className="w-4 h-4 text-primary" />
                Event Summary
              </h3>
              <p className="text-xs text-foreground-muted mt-1">Breakdown of recent operations</p>
            </div>
            <div className="p-4 space-y-4">
              {/* Summary Stats */}
              <div className="grid grid-cols-1 gap-3">
                <div className="p-3 bg-cyan-500/10 border border-cyan-500/20 rounded-lg flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-cyan-500 animate-pulse"></div>
                    <span className="text-sm font-medium text-foreground">Inserts</span>
                  </div>
                  <span className="text-lg font-bold text-cyan-500">
                    {events.filter(e => (e.event_type || '').toLowerCase() === 'insert').length}
                  </span>
                </div>
                <div className="p-3 bg-blue-500/10 border border-blue-500/20 rounded-lg flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-blue-500"></div>
                    <span className="text-sm font-medium text-foreground">Updates</span>
                  </div>
                  <span className="text-lg font-bold text-blue-500">
                    {events.filter(e => (e.event_type || '').toLowerCase() === 'update').length}
                  </span>
                </div>
                <div className="p-3 bg-amber-500/10 border border-amber-500/20 rounded-lg flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-amber-500"></div>
                    <span className="text-sm font-medium text-foreground">Deletes</span>
                  </div>
                  <span className="text-lg font-bold text-amber-500">
                    {events.filter(e => (e.event_type || '').toLowerCase() === 'delete').length}
                  </span>
                </div>
              </div>

              {/* Recent Activity Mini-List */}
              <div className="mt-4 pt-4 border-t border-border">
                <h4 className="text-xs font-bold text-foreground-muted uppercase tracking-wider mb-3">Latest Activity</h4>
                <div className="space-y-3">
                  {events.slice(0, 5).map((event, i) => {
                    const type = (event.event_type || 'unknown').toLowerCase()
                    return (
                      <div key={i} className="flex items-start gap-2 text-xs">
                        <Badge variant="outline" className={`px-1 py-0 text-[10px] h-4 ${type === 'insert' ? 'border-cyan-500/30 text-cyan-500' :
                          type === 'update' ? 'border-blue-500/30 text-blue-500' :
                            'border-amber-500/30 text-amber-500'
                          }`}>
                          {type ? type.substring(0, 1).toUpperCase() : '?'}
                        </Badge>
                        <div className="flex-1 min-w-0">
                          <div className="truncate text-foreground font-medium">{event.table_name || 'Unknown Table'}</div>
                          <div className="text-foreground-muted text-[10px]">{event.created_at ? formatDistanceToNow(new Date(event.created_at), { addSuffix: true }) : 'Just now'}</div>
                        </div>
                      </div>
                    )
                  })}
                  {events.length === 0 && (
                    <div className="text-center text-foreground-muted text-xs py-2">No recent events</div>
                  )}
                </div>
              </div>
            </div>
          </Card>

          {/* Main Live Feed Table */}
          <Card className="lg:col-span-3 p-0 border-border shadow-md bg-surface overflow-hidden flex flex-col h-[600px]">
            <div className="p-4 border-b border-border bg-muted/20 flex flex-col sm:flex-row items-center justify-between flex-shrink-0 gap-4">
              <div className="flex items-center gap-2 w-full sm:w-auto">
                <div className={`w-2 h-2 rounded-full ${wsConnected ? "bg-success animate-pulse" : "bg-error"}`} />
                <h3 className="font-bold text-foreground">Live Event Feed</h3>
                <div className="relative ml-4 flex-1 sm:flex-initial">
                  <input
                    type="text"
                    placeholder="Search events or tables..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="h-8 w-full sm:w-64 bg-background border border-border rounded-md px-3 text-xs focus:ring-1 focus:ring-primary outline-none transition-all"
                  />
                </div>
              </div>
              <div className="flex items-center gap-4 w-full sm:w-auto justify-between">
                <div className="flex items-center gap-3 text-xs text-foreground-muted pr-4 border-r border-border h-8">
                  <div className="flex items-center gap-1.5"><span className="w-1.5 h-1.5 rounded-full bg-success"></span> App</div>
                  <div className="flex items-center gap-1.5"><span className="w-1.5 h-1.5 rounded-full bg-warning"></span> Pen</div>
                  <div className="flex items-center gap-1.5"><span className="w-1.5 h-1.5 rounded-full bg-error"></span> Err</div>
                </div>
                <Button variant="outline" size="sm" className="h-8 gap-2 border-border hover:bg-surface-hover" onClick={handleDownloadCSV}>
                  <FileText className="w-3 h-3" />
                  Export
                </Button>
              </div>
            </div>

            <div className="overflow-x-auto flex-1">
              <Table>
                <TableHeader className="bg-muted/10 sticky top-0 z-10 backdrop-blur-sm">
                  <TableRow className="hover:bg-transparent border-border">
                    <TableHead className="w-[180px] font-bold text-xs uppercase tracking-wider text-foreground-muted pl-4">Timestamp</TableHead>
                    <TableHead className="w-[100px] font-bold text-xs uppercase tracking-wider text-foreground-muted">Operation</TableHead>
                    <TableHead className="font-bold text-xs uppercase tracking-wider text-foreground-muted">Table</TableHead>
                    <TableHead className="w-[120px] font-bold text-xs uppercase tracking-wider text-foreground-muted">LSN</TableHead>
                    <TableHead className="w-[100px] font-bold text-xs uppercase tracking-wider text-foreground-muted">Latency</TableHead>
                    <TableHead className="w-[120px] font-bold text-xs uppercase tracking-wider text-foreground-muted text-right pr-6">Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {currentEvents.length > 0 ? (
                    currentEvents.map((event) => {
                      const type = (event.event_type || 'unknown').toLowerCase()
                      return (
                        <TableRow key={event.id} className="hover:bg-muted/5 border-border/50 group transition-colors">
                          <TableCell className="font-mono text-xs text-foreground-muted whitespace-nowrap pl-4">
                            {event.created_at ? new Date(event.created_at).toLocaleString() : 'Just now'}
                          </TableCell>
                          <TableCell>
                            <span className={`text-[10px] uppercase font-bold px-2 py-1 rounded-full border ${type === 'insert' ? 'bg-cyan-500/10 text-cyan-500 border-cyan-500/20' :
                              type === 'update' ? 'bg-blue-500/10 text-blue-500 border-blue-500/20' :
                                type === 'delete' ? 'bg-amber-500/10 text-amber-500 border-amber-500/20' :
                                  'bg-muted text-foreground-muted border-border'
                              }`}
                            >
                              {event.event_type || 'UNKNOWN'}
                            </span>
                          </TableCell>
                          <TableCell className="font-medium text-foreground text-sm">
                            {event.table_name || 'N/A'}
                          </TableCell>
                          <TableCell className="font-mono text-xs text-foreground-muted">
                            {event.source_lsn || '-'}
                          </TableCell>
                          <TableCell>
                            <span className={`text-xs font-medium ${(event.latency_ms || 0) > 1000 ? 'text-warning' : 'text-success'
                              }`}>
                              {event.latency_ms != null ? `${event.latency_ms}ms` : '<1ms'}
                            </span>
                          </TableCell>
                          <TableCell className="text-right pr-6">
                            <div className="flex items-center justify-end gap-2">
                              <span className={`w-1.5 h-1.5 rounded-full ${event.status === 'applied' || event.status === 'success' ? 'bg-success shadow-[0_0_4px_rgba(34,197,94,0.4)]' :
                                event.status === 'failed' || event.status === 'error' ? 'bg-error shadow-[0_0_4px_rgba(239,68,68,0.4)]' : 'bg-warning'
                                }`} />
                              <span className={`text-[10px] uppercase tracking-wider font-semibold ${event.status === 'applied' || event.status === 'success' ? 'text-success' :
                                event.status === 'failed' || event.status === 'error' ? 'text-error' : 'text-warning'
                                }`}>{event.status}</span>
                            </div>
                          </TableCell>
                        </TableRow>
                      )
                    })
                  ) : (
                    <TableRow>
                      <TableCell colSpan={6} className="h-64 text-center text-foreground-muted text-sm">
                        <div className="flex flex-col items-center justify-center gap-3 opacity-50">
                          <Activity className="w-10 h-10" />
                          <p>Waiting for live events...</p>
                          <span className="text-xs">Action on the source database to see events here</span>
                        </div>
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>

            <div className="p-4 border-t border-border bg-muted/10 flex justify-between items-center text-xs flex-shrink-0">
              <span className="text-foreground-muted">
                Page <span className="font-bold text-foreground">{currentPage}</span> of <span className="font-bold text-foreground">{Math.max(1, totalPages)}</span>
              </span>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 px-3 text-xs border-border hover:bg-surface-hover hover:text-primary hover:border-primary/30 transition-colors"
                  onClick={() => setCurrentPage(c => Math.max(1, c - 1))}
                  disabled={currentPage === 1}
                >
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 px-3 text-xs border-border hover:bg-surface-hover hover:text-primary hover:border-primary/30 transition-colors"
                  onClick={() => setCurrentPage(c => Math.min(totalPages, c + 1))}
                  disabled={currentPage >= totalPages}
                >
                  Next
                </Button>
              </div>
            </div>
          </Card>
        </div>
      </div>
    </ProtectedPage>
  )
}
