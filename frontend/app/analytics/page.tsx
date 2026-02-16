"use client"

import { useState, useEffect, useMemo, useRef } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts"
import { Calendar, Download, BarChart as BarChartIcon, Activity, CheckCircle2, Zap, Clock, Database } from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { fetchReplicationEvents, fetchMonitoringMetrics } from "@/lib/store/slices/monitoringSlice"
import { fetchPipelines } from "@/lib/store/slices/pipelineSlice"
import { apiClient } from "@/lib/api/client"
import { format, subDays, startOfDay } from "date-fns"
import { ProtectedPage } from "@/components/auth/ProtectedPage"
import jsPDF from "jspdf"
import autoTable from "jspdf-autotable"

const COLORS = {
  insert: "#22d3ee", // Cyan
  update: "#3b82f6", // Blue  
  delete: "#f59e0b", // Amber
}

export default function AnalyticsPage() {
  const dispatch = useAppDispatch()
  const { events, metrics } = useAppSelector((state) => state.monitoring)
  const { pipelines } = useAppSelector((state) => state.pipelines)

  const [timeRange, setTimeRange] = useState("all")
  const [selectedPipelineId, setSelectedPipelineId] = useState<string>("all")
  const [lsnLatencyData, setLsnLatencyData] = useState<any[]>([])
  const [lsnData, setLsnData] = useState<Record<string, { sourceLsn?: string; processedLsn?: string; gapBytes?: number; gapMB?: number }>>({})
  const [loadingLsn, setLoadingLsn] = useState(false)
  const [checkpoints, setCheckpoints] = useState<any[]>([])
  const [loadingCheckpoints, setLoadingCheckpoints] = useState(false)

  useEffect(() => {
    dispatch(fetchPipelines())
    // Fetch all events (no date filter) for analytics - let client-side filtering handle time ranges
    // Also fetch metrics for latency trend fallback
    dispatch(fetchReplicationEvents({ limit: 10000, todayOnly: false }))
    // If we have pipelines, we can fetch metrics for all or a specific one
    // But fetchMonitoringMetrics requires a specific pipelineId
    if (pipelines.length > 0) {
      dispatch(fetchMonitoringMetrics({ pipelineId: pipelines[0].id }))
    }
  }, [dispatch])

  // Track last fetched values to prevent unnecessary refetches
  const lastFetchedPipelineRef = useRef<string>("")
  const lastFetchedTimeRangeRef = useRef<string>("")

  // Fetch LSN latency data for selected pipeline
  useEffect(() => {
    // Skip if values haven't changed
    if (selectedPipelineId === lastFetchedPipelineRef.current &&
      timeRange === lastFetchedTimeRangeRef.current &&
      pipelines.length > 0) {
      return
    }

    const fetchLsnData = async () => {
      if (selectedPipelineId === "all" || !pipelines.length) {
        setLsnLatencyData([])
        setLsnData({})
        lastFetchedPipelineRef.current = selectedPipelineId
        lastFetchedTimeRangeRef.current = timeRange
        return
      }

      setLoadingLsn(true)
      try {
        const pipelineId = selectedPipelineId
        console.log('[Analytics] Fetching LSN latency for pipeline:', pipelineId)

        // Fetch current LSN latency
        const currentLsn = await apiClient.getLsnLatency(pipelineId)
        console.log('[Analytics] Current LSN data:', currentLsn)

        setLsnData(prev => ({
          ...prev,
          [pipelineId]: {
            sourceLsn: currentLsn.source_lsn,
            processedLsn: currentLsn.processed_lsn,
            gapBytes: currentLsn.lsn_gap_bytes,
            gapMB: currentLsn.lsn_gap_mb
          }
        }))

        // Fetch LSN latency trend (last 24 hours)
        const hours = timeRange === "24h" ? 24 : timeRange === "7d" ? 168 : timeRange === "30d" ? 720 : 24
        console.log('[Analytics] Fetching LSN trend for', hours, 'hours')
        try {
          const trendData = await apiClient.getLsnLatencyTrend(pipelineId, undefined, hours)
          console.log('[Analytics] LSN trend data:', trendData)

          if (trendData && trendData.trend && Array.isArray(trendData.trend) && trendData.trend.length > 0) {
            // Transform trend data for chart
            const chartData = trendData.trend.map((point: any) => ({
              timestamp: format(new Date(point.timestamp), "HH:mm"),
              latency: point.lsn_gap_mb ? Math.round(point.lsn_gap_mb * 1024) : 0, // Convert MB to KB for display
              gapBytes: point.lsn_gap_bytes || 0,
              gapMB: point.lsn_gap_mb || 0,
              sourceLsn: point.source_lsn,
              processedLsn: point.processed_lsn
            }))
            console.log('[Analytics] Chart data prepared:', chartData.length, 'points')
            setLsnLatencyData(chartData)
          } else {
            console.log('[Analytics] No trend data available - LSN tracking may not be running or no data collected yet')
            setLsnLatencyData([])
          }
        } catch (trendError: any) {
          console.warn('[Analytics] Error fetching LSN trend (will use event latency as fallback):', trendError)
          setLsnLatencyData([])
        }

        // Update refs after successful fetch
        lastFetchedPipelineRef.current = selectedPipelineId
        lastFetchedTimeRangeRef.current = timeRange
      } catch (error) {
        console.error("[Analytics] Error fetching LSN latency data:", error)
        setLsnLatencyData([])
        setLsnData({})
      } finally {
        setLoadingLsn(false)
      }
    }

    fetchLsnData()
  }, [selectedPipelineId, timeRange, pipelines.length]) // Use pipelines.length instead of pipelines array

  // Track last fetched checkpoint pipeline to prevent unnecessary refetches
  const lastFetchedCheckpointPipelineRef = useRef<string>("")

  // Fetch checkpoints for selected pipeline
  useEffect(() => {
    // Skip if pipeline hasn't changed
    if (selectedPipelineId === lastFetchedCheckpointPipelineRef.current && pipelines.length > 0) {
      return
    }

    const fetchCheckpoints = async () => {
      if (selectedPipelineId === "all" || !pipelines.length) {
        setCheckpoints([])
        lastFetchedCheckpointPipelineRef.current = selectedPipelineId
        return
      }

      setLoadingCheckpoints(true)
      try {
        const pipelineId = selectedPipelineId
        console.log('[Analytics] Fetching checkpoints for pipeline:', pipelineId)
        const data = await apiClient.getPipelineCheckpoints(pipelineId)
        console.log('[Analytics] Checkpoints data:', data)

        // Handle both response formats: { checkpoints: [...] } or direct array
        if (data) {
          if (Array.isArray(data)) {
            setCheckpoints(data)
          } else if (data.checkpoints && Array.isArray(data.checkpoints)) {
            setCheckpoints(data.checkpoints)
          } else {
            setCheckpoints([])
          }
        } else {
          setCheckpoints([])
        }

        // Update ref after successful fetch
        lastFetchedCheckpointPipelineRef.current = selectedPipelineId
      } catch (error) {
        console.error("[Analytics] Error fetching checkpoints:", error)
        setCheckpoints([])
      } finally {
        setLoadingCheckpoints(false)
      }
    }

    fetchCheckpoints()
  }, [selectedPipelineId, pipelines.length]) // Use pipelines.length instead of pipelines array

  // Calculate time range filter
  const timeRangeFilter = useMemo(() => {
    if (timeRange === "all") {
      return null // No date filter - show all events
    }
    const now = new Date()
    switch (timeRange) {
      case "24h":
        return subDays(now, 1)
      case "7d":
        return subDays(now, 7)
      case "30d":
        return subDays(now, 30)
      case "90d":
        return subDays(now, 90)
      default:
        return null // Show all events by default
    }
  }, [timeRange])

  // Filter events by time range and pipeline
  const filteredEvents = useMemo(() => {
    let filtered = events
    // Apply time range filter if specified
    if (timeRangeFilter) {
      filtered = filtered.filter((event) => {
        const eventDate = new Date(event.created_at || event.source_commit_time || Date.now())
        return eventDate >= timeRangeFilter
      })
    }

    if (selectedPipelineId !== "all") {
      filtered = filtered.filter((event) => String(event.pipeline_id) === selectedPipelineId)
    }

    return filtered
  }, [events, timeRangeFilter, selectedPipelineId])

  // Calculate KPIs from real data
  const kpis = useMemo(() => {
    const totalEvents = filteredEvents.length
    const successfulEvents = filteredEvents.filter((e) => e.status === "success" || e.status === "applied").length
    const failedEvents = filteredEvents.filter((e) => e.status === "failed" || e.status === "error").length
    const syncRateNum = totalEvents > 0 ? (successfulEvents / totalEvents) * 100 : 0
    const syncRate = syncRateNum % 1 === 0 ? syncRateNum.toFixed(0) : syncRateNum.toFixed(1)

    // Calculate average latency only from events that have been applied (have latency_ms > 0)
    const latencies = filteredEvents
      .map((e) => e.latency_ms)
      .filter((l): l is number => l != null && l !== undefined && l > 0)
    const avgLatencyNum = latencies.length > 0
      ? (latencies.reduce((a, b) => a + b, 0) / latencies.length)
      : 0
    const avgLatency = avgLatencyNum % 1 === 0 ? avgLatencyNum.toFixed(0) : avgLatencyNum.toFixed(1)

    // Debug logging (only in development)
    if (process.env.NODE_ENV === 'development') {
      console.log('[Analytics] Total events in Redux:', events.length)
      console.log('[Analytics] Filtered events:', filteredEvents.length)
      console.log('[Analytics] Time range filter:', timeRange, 'from', timeRangeFilter)
      console.log('[Analytics] Selected pipeline:', selectedPipelineId)
      if (events.length > 0) {
        console.log('[Analytics] First event:', events[0])
        console.log('[Analytics] First event date:', events[0].created_at)
      }
    }

    return {
      totalReplicated: totalEvents.toLocaleString(),
      successfullySynced: successfulEvents.toLocaleString(),
      syncRate: `${syncRate}%`,
      avgLatency: `${avgLatency}ms`,
    }
  }, [filteredEvents, events, timeRange, timeRangeFilter, selectedPipelineId])

  // Group events by date for time series chart
  const timeSeriesData = useMemo(() => {
    const now = new Date()
    let daysToShow = 7 // Default to 7 days

    // Determine number of days based on time range
    if (timeRange === "24h") {
      daysToShow = 1
    } else if (timeRange === "7d") {
      daysToShow = 7
    } else if (timeRange === "30d") {
      daysToShow = 30
    } else if (timeRange === "90d") {
      daysToShow = 90
    } else {
      // For "all", show last 30 days or all available dates
      daysToShow = 30
    }

    // Create date buckets for the time range - grouped by event type (INSERT, UPDATE, DELETE)
    const dateBuckets: Record<string, { date: string; dateObj: Date; insert: number; update: number; delete: number; total: number }> = {}

    // Initialize all days in the range with 0 values
    for (let i = daysToShow - 1; i >= 0; i--) {
      const date = new Date(now)
      date.setDate(date.getDate() - i)
      date.setHours(0, 0, 0, 0)
      const dateKey = format(date, "MMM d")
      const dateStr = format(date, "yyyy-MM-dd")
      dateBuckets[dateKey] = {
        date: dateKey,
        dateObj: date,
        insert: 0,
        update: 0,
        delete: 0,
        total: 0
      }
    }

    // Group events by date and event type
    filteredEvents.forEach((event) => {
      try {
        const eventDate = new Date(event.created_at || event.source_commit_time || Date.now())
        eventDate.setHours(0, 0, 0, 0)
        const dateKey = format(eventDate, "MMM d")

        // Only process events within our time range
        if (dateBuckets[dateKey]) {
          // Normalize event type to lowercase for comparison
          const eventType = (event.event_type || "").toLowerCase()

          // Count by event type
          if (eventType === "insert" || eventType === "i") {
            dateBuckets[dateKey].insert++
          } else if (eventType === "update" || eventType === "u") {
            dateBuckets[dateKey].update++
          } else if (eventType === "delete" || eventType === "d") {
            dateBuckets[dateKey].delete++
          } else {
            // If event type is unknown, count as insert (most common)
            dateBuckets[dateKey].insert++
          }

          // Increment total count
          dateBuckets[dateKey].total++
        }
      } catch (err) {
        console.warn("Error processing event date:", err, event)
      }
    })

    // Sort by date object (not string) and return
    return Object.values(dateBuckets)
      .sort((a, b) => a.dateObj.getTime() - b.dateObj.getTime())
      .map(({ dateObj, ...rest }) => rest) // Remove dateObj before returning
  }, [filteredEvents, timeRange])

  // Calculate event type distribution
  const eventTypeData = useMemo(() => {
    const inserts = filteredEvents.filter((e) => e.event_type === "insert").length
    const updates = filteredEvents.filter((e) => e.event_type === "update").length
    const deletes = filteredEvents.filter((e) => e.event_type === "delete").length

    const data = [
      { name: "Inserts", value: inserts },
      { name: "Updates", value: updates },
      { name: "Deletes", value: deletes },
    ]

    // Always return all types, even if 0, so the graph structure is consistent
    // But filter out zeros for display purposes
    return data.filter((item) => item.value > 0)
  }, [filteredEvents])

  // Calculate latency trend (group by hour) - use LSN data if available, otherwise use event latency
  const latencyData = useMemo(() => {
    // If we have LSN latency data, use it (more accurate for replication lag)
    if (lsnLatencyData.length > 0) {
      return lsnLatencyData.map(point => ({
        timestamp: point.timestamp,
        latency: point.gapMB ? Math.round(point.gapMB * 1024) : 0, // Convert MB to KB for display
        gapBytes: point.gapBytes,
        gapMB: point.gapMB,
        sourceLsn: point.sourceLsn,
        processedLsn: point.processedLsn
      }))
    }

    // Fallback to event latency if no LSN data
    const grouped: Record<string, { timestamp: string; latency: number; count: number }> = {}

    filteredEvents.forEach((event) => {
      if (event.latency_ms != null && event.latency_ms !== undefined && event.latency_ms > 0) {
        try {
          const eventDate = new Date(event.created_at || event.source_commit_time || Date.now())
          const hour = format(eventDate, "HH:00")
          if (!grouped[hour]) {
            grouped[hour] = { timestamp: hour, latency: 0, count: 0 }
          }
          grouped[hour].latency += event.latency_ms
          grouped[hour].count++
        } catch (err) {
          console.warn("Error processing event for latency trend:", err, event)
        }
      }
    })

    // If we have grouped data, return it
    if (Object.keys(grouped).length > 0) {
      return Object.values(grouped)
        .map((g) => ({
          timestamp: g.timestamp,
          latency: Math.round(g.latency / g.count) || 0,
        }))
        .sort((a, b) => a.timestamp.localeCompare(b.timestamp))
    }

    // If no event data, try to use metrics data as fallback
    const metricsArray = Array.isArray(metrics) ? metrics : []

    if (metricsArray.length > 0) {
      // Group metrics by hour
      const metricsGrouped: Record<string, { timestamp: string; latency: number; count: number }> = {}

      metricsArray.forEach((metric: any) => {
        if (metric.lag_seconds != null && metric.lag_seconds > 0) {
          try {
            const metricDate = new Date(metric.timestamp || Date.now())
            const hour = format(metricDate, "HH:00")
            if (!metricsGrouped[hour]) {
              metricsGrouped[hour] = { timestamp: hour, latency: 0, count: 0 }
            }
            const latencyMs = (metric.lag_seconds || 0) * 1000
            metricsGrouped[hour].latency += latencyMs
            metricsGrouped[hour].count++
          } catch (err) {
            console.warn("Error processing metric for latency trend:", err, metric)
          }
        }
      })

      if (Object.keys(metricsGrouped).length > 0) {
        return Object.values(metricsGrouped)
          .map((g) => ({
            timestamp: g.timestamp,
            latency: Math.round(g.latency / g.count) || 0,
          }))
          .sort((a, b) => a.timestamp.localeCompare(b.timestamp))
      }
    }

    // If no data at all, create empty buckets for the last 24 hours to show structure
    const now = new Date()
    return Array.from({ length: 24 }, (_, i) => {
      const hour = new Date(now.getTime() - (23 - i) * 60 * 60 * 1000)
      return {
        timestamp: format(hour, "HH:00"),
        latency: 0,
      }
    })
  }, [filteredEvents, lsnLatencyData, metrics])

  // Calculate table-level metrics
  const tableMetrics = useMemo(() => {
    const tableStats: Record<
      string,
      { table: string; inserted: number; updated: number; deleted: number; latencies: number[] }
    > = {}

    filteredEvents.forEach((event) => {
      const tableName = event.table_name || "unknown"
      if (!tableStats[tableName]) {
        tableStats[tableName] = { table: tableName, inserted: 0, updated: 0, deleted: 0, latencies: [] }
      }

      if (event.event_type === "insert") tableStats[tableName].inserted++
      else if (event.event_type === "update") tableStats[tableName].updated++
      else if (event.event_type === "delete") tableStats[tableName].deleted++

      if (event.latency_ms && event.latency_ms > 0) {
        tableStats[tableName].latencies.push(event.latency_ms)
      }
    })

    return Object.values(tableStats)
      .map((stat) => ({
        table: stat.table,
        inserted: stat.inserted,
        updated: stat.updated,
        deleted: stat.deleted,
        avg_latency: stat.latencies.length > 0
          ? Math.round(stat.latencies.reduce((a, b) => a + b, 0) / stat.latencies.length)
          : 0,
      }))
      .sort((a, b) => (b.inserted + b.updated + b.deleted) - (a.inserted + a.updated + a.deleted))
      .slice(0, 10) // Top 10 tables
  }, [filteredEvents])

  const handleExportPDF = () => {
    const doc = new jsPDF()
    const pageWidth = doc.internal.pageSize.getWidth()
    const timestamp = format(new Date(), "yyyy-MM-dd HH:mm:ss")

    // Set vibrant theme colors
    const primaryColor = [34, 211, 238] // Cyan-400
    const textColor = [30, 41, 59] // Slate-800
    const secondaryTextColor = [100, 116, 139] // Slate-500

    // Title & Header
    doc.setFontSize(22)
    doc.setTextColor(primaryColor[0], primaryColor[1], primaryColor[2])
    doc.text("CDC Replication Analytics Report", 14, 22)

    doc.setFontSize(10)
    doc.setTextColor(secondaryTextColor[0], secondaryTextColor[1], secondaryTextColor[2])
    doc.text(`Generated on: ${timestamp}`, 14, 30)
    doc.text(`Pipeline: ${selectedPipelineId === "all" ? "All Pipelines" : pipelines.find(p => String(p.id) === selectedPipelineId)?.name || selectedPipelineId}`, 14, 35)
    doc.text(`Time Range: ${timeRange === "all" ? "All Time" : timeRange}`, 14, 40)

    // Divider
    doc.setDrawColor(226, 232, 240)
    doc.line(14, 45, pageWidth - 14, 45)

    // 1. KPI Section
    doc.setFontSize(14)
    doc.setTextColor(textColor[0], textColor[1], textColor[2])
    doc.text("Performance Overview", 14, 55)

    const kpiData = [
      ["Metric", "Value"],
      ["Total Events Replicated", kpis.totalReplicated],
      ["Successfully Synced", kpis.successfullySynced],
      ["Sync Accuracy Rate", kpis.syncRate],
      ["Average Latency", kpis.avgLatency]
    ]

    autoTable(doc, {
      startY: 60,
      head: [kpiData[0]],
      body: kpiData.slice(1),
      theme: 'grid',
      headStyles: { fillColor: primaryColor as [number, number, number], textColor: [255, 255, 255], fontStyle: 'bold' },
      styles: { fontSize: 10, cellPadding: 5 },
      columnStyles: { 0: { fontStyle: 'bold', cellWidth: 60 } }
    })

    // 2. Operation Breakdown (from eventTypeData)
    const finalY = (doc as any).lastAutoTable.finalY + 15
    doc.setFontSize(14)
    doc.setTextColor(textColor[0], textColor[1], textColor[2])
    doc.text("Operational Breakdown", 14, finalY)

    const pieDataRows = eventTypeData.map((item: any) => [item.name, item.value.toLocaleString()])
    autoTable(doc, {
      startY: finalY + 5,
      head: [["Operation Type", "Count"]],
      body: pieDataRows,
      theme: 'striped',
      headStyles: { fillColor: [59, 130, 246], textColor: [255, 255, 255] }, // Blue-500
      styles: { fontSize: 10 },
    })

    // 3. Table Latency & Throughput
    const nextY = (doc as any).lastAutoTable.finalY + 15
    doc.setFontSize(14)
    doc.text("Top 10 Active Tables", 14, nextY)

    const tableRows = tableMetrics.map(t => [
      t.table,
      t.inserted.toLocaleString(),
      t.updated.toLocaleString(),
      t.deleted.toLocaleString(),
      `${t.avg_latency}ms`
    ])

    autoTable(doc, {
      startY: nextY + 5,
      head: [["Table Name", "Inserts", "Updates", "Deletes", "Avg Latency"]],
      body: tableRows,
      theme: 'grid',
      headStyles: { fillColor: [245, 158, 11], textColor: [255, 255, 255] }, // Amber-500
      styles: { fontSize: 9 },
    })

    // Footer
    const totalPages = (doc as any).internal.getNumberOfPages()
    for (let i = 1; i <= totalPages; i++) {
      doc.setPage(i)
      doc.setFontSize(8)
      doc.setTextColor(secondaryTextColor[0], secondaryTextColor[1], secondaryTextColor[2])
      doc.text(
        `Page ${i} of ${totalPages} - CONFIDENTIAL CDC REPLICATION REPORT`,
        pageWidth / 2,
        doc.internal.pageSize.getHeight() - 10,
        { align: 'center' }
      )
    }

    doc.save(`CDC_Analytics_Report_${format(new Date(), "yyyyMMdd_HHmm")}.pdf`)
  }

  return (
    <ProtectedPage path="/analytics" requiredPermission="view_metrics">
      <div className="p-6 space-y-6">
        <PageHeader
          title="Analytics & Metrics"
          subtitle="Detailed replication performance analysis and insights"
          icon={BarChartIcon}
          action={
            <Button
              variant="outline"
              onClick={handleExportPDF}
              className="bg-transparent border-border hover:bg-cyan-500/10 hover:border-cyan-500/50 hover:text-cyan-400 gap-2"
            >
              <Download className="w-4 h-4" />
              Export Report
            </Button>
          }
        />

        {/* Filters */}
        <div className="flex gap-4">
          <div className="w-48">
            <Select value={timeRange} onValueChange={setTimeRange}>
              <SelectTrigger className="bg-surface border-border">
                <Calendar className="w-4 h-4 mr-2" />
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-surface border-border">
                <SelectItem value="all">All Time</SelectItem>
                <SelectItem value="24h">Last 24 Hours</SelectItem>
                <SelectItem value="7d">Last 7 Days</SelectItem>
                <SelectItem value="30d">Last 30 Days</SelectItem>
                <SelectItem value="90d">Last 90 Days</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="w-48">
            <Select value={selectedPipelineId} onValueChange={setSelectedPipelineId}>
              <SelectTrigger className="bg-surface border-border">
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-surface border-border">
                <SelectItem value="all">All Pipelines</SelectItem>
                {pipelines.map((pipeline) => (
                  <SelectItem key={pipeline.id} value={String(pipeline.id)}>
                    {pipeline.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* Key Performance Indicators - Enhanced */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <Card className="p-5 bg-card border-l-4 border-l-cyan-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
            <Activity className="absolute -right-4 -bottom-4 w-24 h-24 text-cyan-500/10 group-hover:text-cyan-500/15 transition-colors transform rotate-12" />
            <div className="flex items-center justify-between mb-2 relative z-10">
              <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide">Total Replicated</p>
            </div>
            <p className="text-3xl font-extrabold text-cyan-500 relative z-10">{kpis.totalReplicated}</p>
            <p className="text-xs text-foreground-muted mt-1 relative z-10">Events captured</p>
          </Card>
          <Card className="p-5 bg-card border-l-4 border-l-green-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
            <CheckCircle2 className="absolute -right-4 -bottom-4 w-24 h-24 text-green-500/10 group-hover:text-green-500/15 transition-colors transform rotate-12" />
            <div className="flex items-center justify-between mb-2 relative z-10">
              <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide">Successfully Synced</p>
            </div>
            <p className="text-3xl font-extrabold text-green-500 relative z-10">{kpis.successfullySynced}</p>
            <p className="text-xs text-foreground-muted mt-1 relative z-10">Applied to target</p>
          </Card>
          <Card className="p-5 bg-card border-l-4 border-l-blue-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
            <Zap className="absolute -right-4 -bottom-4 w-24 h-24 text-blue-500/10 group-hover:text-blue-500/15 transition-colors transform rotate-12" />
            <div className="flex items-center justify-between mb-2 relative z-10">
              <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide">Sync Rate</p>
            </div>
            <p className="text-3xl font-extrabold text-blue-500 relative z-10">{kpis.syncRate}</p>
            <p className="text-xs text-foreground-muted mt-1 relative z-10">Success rate</p>
          </Card>
          <Card className="p-5 bg-card border-l-4 border-l-purple-500 shadow-sm hover:shadow-md transition-all group relative overflow-hidden">
            <Clock className="absolute -right-4 -bottom-4 w-24 h-24 text-purple-500/10 group-hover:text-purple-500/15 transition-colors transform rotate-12" />
            <div className="flex items-center justify-between mb-2 relative z-10">
              <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide">Avg Latency</p>
            </div>
            <p className="text-3xl font-extrabold text-purple-500 relative z-10">{kpis.avgLatency}</p>
            <p className="text-xs text-foreground-muted mt-1 relative z-10">Average replication delay</p>
          </Card>
        </div>

        {/* Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card className="bg-card border-l-4 border-l-blue-500 shadow-sm p-6 hover:shadow-md transition-all">
            <div className="flex items-center justify-between mb-6 flex-wrap gap-4">
              <h3 className="text-xl font-bold text-foreground flex items-center gap-2">
                <BarChartIcon className="w-5 h-5 text-blue-500" />
                Replication Events ({timeRange})
              </h3>
              <div className="flex items-center gap-4 text-xs text-foreground-muted">
                <div className="flex items-center gap-1.5">
                  <div className="w-3 h-3 rounded bg-gradient-to-br from-cyan-400 to-cyan-600"></div>
                  <span className="font-medium">INSERT (Top)</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <div className="w-3 h-3 rounded bg-gradient-to-br from-blue-500 to-blue-700"></div>
                  <span className="font-medium">UPDATE (Middle)</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <div className="w-3 h-3 rounded bg-gradient-to-br from-amber-500 to-amber-700"></div>
                  <span className="font-medium">DELETE (Bottom)</span>
                </div>
              </div>
            </div>
            {timeSeriesData.length > 0 ? (
              <ResponsiveContainer width="100%" height={350}>
                <BarChart
                  data={timeSeriesData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 70 }}
                >
                  <defs>
                    <linearGradient id="insertGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#22d3ee" stopOpacity={0.95} />
                      <stop offset="100%" stopColor="#06b6d4" stopOpacity={0.8} />
                    </linearGradient>
                    <linearGradient id="updateGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.95} />
                      <stop offset="100%" stopColor="#2563eb" stopOpacity={0.8} />
                    </linearGradient>
                    <linearGradient id="deleteGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#f59e0b" stopOpacity={0.95} />
                      <stop offset="100%" stopColor="#d97706" stopOpacity={0.8} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3448" opacity={0.3} />
                  <XAxis
                    dataKey="date"
                    stroke="#9ca3af"
                    angle={-45}
                    textAnchor="end"
                    height={80}
                    interval={timeSeriesData.length > 14 ? "preserveStartEnd" : 0}
                    tick={{ fill: "#9ca3af", fontSize: 11, fontWeight: 600 }}
                    label={{ value: 'Date', position: 'insideBottom', offset: -5, fill: '#9ca3af', fontSize: 12, fontWeight: 600 }}
                  />
                  <YAxis
                    stroke="#9ca3af"
                    tick={{ fill: "#9ca3af", fontSize: 11, fontWeight: 600 }}
                    label={{ value: 'Total Events', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 12, fontWeight: 600 }}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "rgba(15, 23, 42, 0.95)",
                      border: "1px solid rgb(59, 130, 246)",
                      borderRadius: "12px",
                      padding: "12px",
                      boxShadow: "0 10px 15px -3px rgba(0, 0, 0, 0.3)"
                    }}
                    labelStyle={{ color: "#60a5fa", fontWeight: "bold", fontSize: "14px", marginBottom: "8px" }}
                    itemStyle={{ color: "#e0e7ff", fontSize: "13px", padding: "4px 0" }}
                    formatter={(value: any, name: string, props: any) => {
                      // Show both individual count and percentage
                      const payload = props.payload
                      const total = payload?.total || (payload?.insert || 0) + (payload?.update || 0) + (payload?.delete || 0)
                      const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0
                      return [`${value} (${percentage}%)`, name]
                    }}
                    cursor={{ fill: 'rgba(59, 130, 246, 0.1)' }}
                  />
                  <Legend
                    wrapperStyle={{ paddingTop: "20px", color: "rgb(148, 163, 184)", fontWeight: 600 }}
                    iconType="square"
                    iconSize={14}
                  />
                  <Bar
                    dataKey="delete"
                    stackId="a"
                    fill="url(#deleteGradient)"
                    name="DELETE"
                    radius={[0, 0, 0, 0]}
                    stroke="#f59e0b"
                    strokeWidth={1}
                  />
                  <Bar
                    dataKey="update"
                    stackId="a"
                    fill="url(#updateGradient)"
                    name="UPDATE"
                    radius={[0, 0, 0, 0]}
                    stroke="#3b82f6"
                    strokeWidth={1}
                  />
                  <Bar
                    dataKey="insert"
                    stackId="a"
                    fill="url(#insertGradient)"
                    name="INSERT"
                    radius={[8, 8, 0, 0]}
                    stroke="#22d3ee"
                    strokeWidth={1}
                  />
                </BarChart>
              </ResponsiveContainer>
            ) : events.length === 0 ? (
              <div className="h-[300px] flex flex-col items-center justify-center text-foreground-muted">
                <p className="text-lg mb-2">No events found in database</p>
                <p className="text-sm">Make sure:</p>
                <ul className="text-sm list-disc list-inside mt-2 space-y-1">
                  <li>Pipelines are active and CDC is enabled</li>
                  <li>Data changes are being made in source databases</li>
                  <li>Check the Monitoring page to see if events are being captured</li>
                </ul>
              </div>
            ) : (
              <div className="h-[300px] flex flex-col items-center justify-center text-foreground-muted">
                <p>No data available for the selected time range ({timeRange})</p>
                <p className="text-xs mt-2">Total events in database: {events.length}</p>
                <p className="text-xs">Try selecting "All Time" to see all events</p>
              </div>
            )}
          </Card>

          <Card className="bg-card border-l-4 border-l-cyan-500 shadow-sm p-6 hover:shadow-md transition-all">
            <h3 className="text-lg font-semibold text-foreground mb-4">Event Distribution</h3>
            {eventTypeData.length > 0 ? (
              <>
                <div className="relative h-[300px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart margin={{ top: 20, right: 20, bottom: 20, left: 40 }}>
                      <defs>
                        <linearGradient id="insertGradient" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="#22d3ee" stopOpacity={0.9} />
                          <stop offset="100%" stopColor="#06b6d4" stopOpacity={0.7} />
                        </linearGradient>
                        <linearGradient id="updateGradient" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.9} />
                          <stop offset="100%" stopColor="#2563eb" stopOpacity={0.7} />
                        </linearGradient>
                        <linearGradient id="deleteGradient" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="#f59e0b" stopOpacity={0.9} />
                          <stop offset="100%" stopColor="#d97706" stopOpacity={0.7} />
                        </linearGradient>
                      </defs>
                      <Pie
                        data={eventTypeData}
                        cx="50%"
                        cy="50%"
                        innerRadius={60}
                        outerRadius={80}
                        paddingAngle={5}
                        dataKey="value"
                      >
                        {eventTypeData.map((entry, index) => {
                          const gradientId = entry.name === "Inserts" ? "insertGradient"
                            : entry.name === "Updates" ? "updateGradient"
                              : "deleteGradient";
                          return (
                            <Cell
                              key={`cell-${index}`}
                              fill={`url(#${gradientId})`}
                              stroke="none"
                            />
                          );
                        })}
                      </Pie>
                      <Tooltip
                        contentStyle={{
                          backgroundColor: "rgba(15, 23, 42, 0.95)",
                          border: "1px solid rgba(255, 255, 255, 0.1)",
                          borderRadius: "12px",
                          boxShadow: "0 10px 15px -3px rgba(0, 0, 0, 0.3)",
                          padding: "12px"
                        }}
                        itemStyle={{ color: 'rgb(203, 213, 225)', fontSize: '13px' }}
                      />
                    </PieChart>
                  </ResponsiveContainer>
                  {/* Center Label for Donut */}
                  <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 text-center pointer-events-none" style={{ marginTop: "-20px" }}>
                    <p className="text-2xl font-black text-foreground">
                      {eventTypeData.reduce((sum, d) => sum + d.value, 0).toLocaleString()}
                    </p>
                    <p className="text-[10px] text-foreground-muted uppercase tracking-widest font-bold">Total Events</p>
                  </div>
                </div>
                <div className="mt-6 grid grid-cols-3 gap-4">
                  {eventTypeData.map((item, i) => {
                    const total = eventTypeData.reduce((sum, d) => sum + d.value, 0);
                    const percentage = ((item.value / total) * 100).toFixed(1);
                    const colorMap = {
                      "Inserts": { bg: "from-cyan-500/10 to-cyan-600/5", border: "border-cyan-500/30", text: "text-cyan-400", icon: "+" },
                      "Updates": { bg: "from-blue-500/10 to-blue-600/5", border: "border-blue-500/30", text: "text-blue-400", icon: "↻" },
                      "Deletes": { bg: "from-amber-500/10 to-amber-600/5", border: "border-amber-500/30", text: "text-amber-400", icon: "−" }
                    };
                    const style = colorMap[item.name as keyof typeof colorMap];

                    return (
                      <div key={i} className={`p-4 bg-card border-l-4 ${style.border.replace('border-', 'border-l-')} shadow-sm hover:shadow-md transition-all`}>
                        <div className="flex items-center gap-2 mb-2">
                          <div className={`w-8 h-8 rounded-full ${style.bg} border ${style.border} flex items-center justify-center ${style.text} font-bold text-lg`}>
                            {style.icon}
                          </div>
                          <span className="text-sm font-semibold text-foreground">{item.name}</span>
                        </div>
                        <div className="mt-2">
                          <span className={`text-2xl font-extrabold ${style.text}`}>{item.value.toLocaleString()}</span>
                          <span className="text-xs text-foreground-muted ml-2">({percentage}%)</span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </>
            ) : (
              <div className="h-[300px] flex items-center justify-center text-foreground-muted">
                No event data available
              </div>
            )}
          </Card>
        </div>

        {/* Latency Trend & Health - Enhanced */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <Card className="bg-card border-l-4 border-l-green-500 shadow-sm p-6 hover:shadow-md transition-all flex flex-col items-center justify-center relative overflow-hidden group">
            <Activity className="absolute -right-4 -bottom-4 w-24 h-24 text-green-500/10 group-hover:text-green-500/15 transition-colors transform rotate-12" />
            <h3 className="text-lg font-bold text-foreground mb-4 flex items-center gap-2 self-start relative z-10">
              <Activity className="w-5 h-5 text-green-500" />
              Real-time Health
            </h3>
            <div className="relative w-full h-[220px] flex items-center justify-center z-10">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <defs>
                    <linearGradient id="healthGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#22c55e" stopOpacity={1} />
                      <stop offset="100%" stopColor="#16a34a" stopOpacity={0.8} />
                    </linearGradient>
                  </defs>
                  <Pie
                    data={[
                      { name: 'Progress', value: parseFloat(kpis.syncRate) },
                      { name: 'Remaining', value: Math.max(0, 100 - parseFloat(kpis.syncRate)) }
                    ]}
                    cx="50%"
                    cy="80%"
                    startAngle={200}
                    endAngle={-20}
                    innerRadius={75}
                    outerRadius={95}
                    paddingAngle={0}
                    dataKey="value"
                    stroke="none"
                  >
                    <Cell fill="url(#healthGradient)" />
                    <Cell fill="rgba(34, 197, 94, 0.1)" />
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <div className="absolute top-[45%] text-center pointer-events-none">
                <p className="text-4xl font-black text-foreground drop-shadow-sm">
                  {kpis.syncRate}
                </p>
                <p className="text-[10px] text-foreground-muted uppercase tracking-[0.2em] font-bold mt-1">Health Index</p>
              </div>
            </div>
          </Card>

          <Card className="lg:col-span-2 bg-card border-l-4 border-l-purple-500 shadow-sm p-6 hover:shadow-md transition-all">
            <div className="flex items-center justify-between mb-6 flex-wrap gap-4">
              <h3 className="text-xl font-bold text-foreground flex items-center gap-2">
                <Clock className="w-5 h-5 text-purple-500" />
                Replication Latency Trend
              </h3>
              {selectedPipelineId !== "all" && lsnData[selectedPipelineId] && (
                <div className="flex gap-4 text-sm flex-wrap">
                  <div className="bg-muted/30 px-3 py-1 rounded border border-border">
                    <span className="text-foreground-muted">Source: </span>
                    <span className="text-purple-500 font-mono text-xs font-bold">{lsnData[selectedPipelineId].sourceLsn || "N/A"}</span>
                  </div>
                  <div className="bg-muted/30 px-3 py-1 rounded border border-border">
                    <span className="text-foreground-muted">Processed: </span>
                    <span className="text-purple-500 font-mono text-xs font-bold">{lsnData[selectedPipelineId].processedLsn || "N/A"}</span>
                  </div>
                  {lsnData[selectedPipelineId].gapMB !== undefined && (
                    <div className="bg-muted/30 px-3 py-1 rounded border border-border">
                      <span className="text-foreground-muted">Gap: </span>
                      <span className="text-purple-500 font-bold">{lsnData[selectedPipelineId].gapMB?.toFixed(2) || "0.00"} MB</span>
                    </div>
                  )}
                </div>
              )}
            </div>
            {loadingLsn ? (
              <div className="h-[320px] flex items-center justify-center">
                <div className="text-foreground-muted">Loading LSN latency data...</div>
              </div>
            ) : latencyData.length > 0 ? (
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={latencyData}>
                  <defs>
                    <linearGradient id="latencyGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#a855f7" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#a855f7" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2d3448" opacity={0.3} />
                  <XAxis
                    dataKey="timestamp"
                    stroke="#9ca3af"
                    style={{ fontSize: '12px', fontWeight: 500 }}
                  />
                  <YAxis
                    stroke="#9ca3af"
                    label={{ value: lsnLatencyData.length > 0 ? "Replication Gap (KB)" : "Latency (ms)", angle: -90, position: "insideLeft", style: { fill: '#9ca3af', fontWeight: 600 } }}
                    style={{ fontSize: '12px', fontWeight: 500 }}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "rgba(15, 23, 42, 0.95)",
                      border: "1px solid rgb(168, 85, 247)",
                      borderRadius: "12px",
                      boxShadow: "0 10px 15px -3px rgba(0, 0, 0, 0.3)",
                      padding: "12px"
                    }}
                    labelStyle={{ color: '#a855f7', fontWeight: 'bold', fontSize: '14px' }}
                    itemStyle={{ color: '#e9d5ff', fontSize: '13px' }}
                  />
                  <Line
                    type="monotone"
                    dataKey="latency"
                    stroke="#a855f7"
                    strokeWidth={4}
                    dot={{ fill: "#a855f7", r: 6, strokeWidth: 2, stroke: "#fff" }}
                    activeDot={{ r: 8, stroke: "#a855f7", strokeWidth: 3 }}
                    name={lsnLatencyData.length > 0 ? "Replication Gap (KB)" : "Latency (ms)"}
                    fill="url(#latencyGradient)"
                  />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-[320px] flex flex-col items-center justify-center text-foreground-muted">
                <svg className="w-16 h-16 mb-4 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                </svg>
                <p className="text-sm font-medium">No latency data available</p>
                {selectedPipelineId === "all" && (
                  <p className="text-xs mt-2">Select a specific pipeline to view LSN replication latency</p>
                )}
                {selectedPipelineId !== "all" && !loadingLsn && (
                  <div className="text-xs mt-2 space-y-1 text-center">
                    <p>LSN tracking may not be enabled for this pipeline or no data available yet</p>
                    <p className="text-foreground-muted/70">Note: LSN tracking is only available for PostgreSQL pipelines</p>
                    <p className="text-foreground-muted/70">The chart will show event latency if LSN data is not available</p>
                  </div>
                )}
                {loadingLsn && (
                  <p className="text-xs mt-2">Loading LSN data...</p>
                )}
              </div>
            )}
          </Card>
        </div>

        {/* Table-Level Metrics */}
        <Card className="bg-card border-l-4 border-l-blue-500 shadow-sm p-6 hover:shadow-md transition-all">
          <h3 className="text-lg font-semibold text-foreground mb-4">Table-Level Metrics</h3>
          {tableMetrics.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-3 px-4 font-semibold text-foreground-muted">Table</th>
                    <th className="text-right py-3 px-4 font-semibold text-foreground-muted">Inserts</th>
                    <th className="text-right py-3 px-4 font-semibold text-foreground-muted">Updates</th>
                    <th className="text-right py-3 px-4 font-semibold text-foreground-muted">Deletes</th>
                    <th className="text-right py-3 px-4 font-semibold text-foreground-muted">Avg Latency</th>
                    <th className="text-right py-3 px-4 font-semibold text-foreground-muted">Total</th>
                  </tr>
                </thead>
                <tbody>
                  {tableMetrics.map((metric) => (
                    <tr key={metric.table} className="border-b border-border hover:bg-surface-hover">
                      <td className="py-3 px-4 text-foreground font-medium">{metric.table}</td>
                      <td className="py-3 px-4 text-right text-foreground">{metric.inserted.toLocaleString()}</td>
                      <td className="py-3 px-4 text-right text-foreground">{metric.updated.toLocaleString()}</td>
                      <td className="py-3 px-4 text-right text-foreground">{metric.deleted.toLocaleString()}</td>
                      <td className="py-3 px-4 text-right text-info font-medium">{metric.avg_latency}ms</td>
                      <td className="py-3 px-4 text-right text-foreground font-semibold">
                        {(metric.inserted + metric.updated + metric.deleted).toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="py-12 text-center text-foreground-muted">No table metrics available</div>
          )}
        </Card>

        {/* Performance Insights */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Top Performing Tables - Enhanced */}
          <Card className="bg-card border-l-4 border-l-green-500 shadow-sm p-6 hover:shadow-md transition-all">
            <h3 className="text-xl font-bold text-foreground mb-6 flex items-center gap-2">
              <Activity className="w-5 h-5 text-green-500" />
              Top Performing Tables
            </h3>
            <div className="space-y-3">
              {tableMetrics
                .filter((m) => m.avg_latency > 0)
                .sort((a, b) => a.avg_latency - b.avg_latency)
                .slice(0, 3)
                .map((table, index) => {
                  const medals = ['🥇', '🥈', '🥉'];
                  const maxLatency = Math.max(...tableMetrics.filter(m => m.avg_latency > 0).map(m => m.avg_latency));
                  const performancePercent = maxLatency > 0 ? ((maxLatency - table.avg_latency) / maxLatency * 100) : 100;

                  return (
                    <div key={table.table} className="p-4 bg-card border-l-4 border-l-green-500 shadow-sm hover:shadow-md transition-all duration-300 hover:scale-[1.02]">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                          <span className="text-2xl">{medals[index]}</span>
                          <span className="text-foreground font-bold">{table.table}</span>
                        </div>
                        <span className="text-sm font-bold text-green-400">{table.avg_latency}ms</span>
                      </div>
                      <div className="w-full bg-muted/30 rounded-full h-2 overflow-hidden">
                        <div
                          className="h-full bg-green-500 rounded-full transition-all duration-500"
                          style={{ width: `${performancePercent}%` }}
                        />
                      </div>
                      <p className="text-xs text-foreground-muted mt-1">Performance: {performancePercent.toFixed(0)}%</p>
                    </div>
                  );
                })}
              {tableMetrics.filter((m) => m.avg_latency > 0).length === 0 && (
                <div className="text-center py-8 text-foreground-muted">
                  <svg className="w-12 h-12 mx-auto mb-3 opacity-30" fill="currentColor" viewBox="0 0 20 20">
                    <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                  </svg>
                  <p className="text-sm font-medium">No latency data available</p>
                </div>
              )}
            </div>
          </Card>

          <Card className="bg-card border-l-4 border-l-amber-500 shadow-sm p-6 hover:shadow-md transition-all">
            <h3 className="text-xl font-bold text-foreground mb-6 flex items-center gap-2">
              <Zap className="w-5 h-5 text-amber-500" />
              Optimization Recommendations
            </h3>
            <div className="space-y-3">
              {tableMetrics
                .filter((m) => m.avg_latency > 15)
                .slice(0, 3)
                .map((table) => {
                  const severity = table.avg_latency > 50 ? 'high' : table.avg_latency > 30 ? 'medium' : 'low';
                  const severityColors = {
                    high: { bg: 'bg-red-500/10', border: 'border-red-500/30', text: 'text-red-400', badge: 'bg-red-500/20 text-red-400' },
                    medium: { bg: 'bg-amber-500/10', border: 'border-amber-500/30', text: 'text-amber-400', badge: 'bg-amber-500/20 text-amber-400' },
                    low: { bg: 'bg-yellow-500/10', border: 'border-yellow-500/30', text: 'text-yellow-400', badge: 'bg-yellow-500/20 text-yellow-400' }
                  };
                  const colors = severityColors[severity];

                  return (
                    <div key={table.table} className={`p-4 bg-card border-l-4 ${colors.border.replace('border-', 'border-l-')} shadow-sm hover:shadow-md transition-all duration-300`}>
                      <div className="flex items-start justify-between mb-2">
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-1">
                            <svg className={`w-5 h-5 ${colors.text}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                            </svg>
                            <p className="text-sm text-foreground font-bold">{table.table}</p>
                          </div>
                          <p className={`text-xs ${colors.text} font-semibold`}>High latency: {table.avg_latency}ms</p>
                        </div>
                        <span className={`px-3 py-1 text-xs font-bold rounded-full ${colors.badge} uppercase`}>
                          {severity}
                        </span>
                      </div>
                      <div className="mt-3 pt-3 border-t border-border/50">
                        <p className="text-[10px] text-foreground-muted font-bold uppercase tracking-wider mb-2">Recommended actions:</p>
                        <ul className="text-xs text-foreground-muted space-y-1">
                          <li>• Increase batch size for better throughput</li>
                          <li>• Review table indexes and query performance</li>
                          <li>• Consider connection pooling optimization</li>
                        </ul>
                      </div>
                    </div>
                  );
                })}
              {tableMetrics.filter((m) => m.avg_latency > 15).length === 0 && (
                <div className="text-center py-8">
                  <svg className="w-12 h-12 mx-auto mb-3 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <p className="text-sm font-semibold text-green-400">All tables performing well!</p>
                  <p className="text-xs text-foreground-muted mt-1">No optimization needed at this time</p>
                </div>
              )}
            </div>
          </Card>
        </div>

        {/* CDC Checkpoints Section */}
        {selectedPipelineId !== "all" && (
          <Card className="bg-card border-l-4 border-l-indigo-500 shadow-sm p-6 hover:shadow-md transition-all">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-bold text-foreground flex items-center gap-2">
                <Database className="w-5 h-5 text-indigo-500" />
                CDC Checkpoints
              </h3>
              <Button
                variant="outline"
                size="sm"
                onClick={async () => {
                  if (selectedPipelineId !== "all") {
                    setLoadingCheckpoints(true)
                    try {
                      const data = await apiClient.getPipelineCheckpoints(selectedPipelineId)
                      if (data) {
                        if (Array.isArray(data)) {
                          setCheckpoints(data)
                        } else if (data.checkpoints && Array.isArray(data.checkpoints)) {
                          setCheckpoints(data.checkpoints)
                        }
                      }
                    } catch (error) {
                      console.error("Error refreshing checkpoints:", error)
                    } finally {
                      setLoadingCheckpoints(false)
                    }
                  }
                }}
                className="bg-transparent border-indigo-500/20 hover:bg-indigo-500/10 gap-2"
                disabled={loadingCheckpoints}
              >
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                Refresh
              </Button>
            </div>

            {loadingCheckpoints ? (
              <div className="h-[200px] flex items-center justify-center">
                <div className="text-foreground-muted">Loading checkpoints...</div>
              </div>
            ) : checkpoints.length === 0 ? (
              <div className="h-[200px] flex flex-col items-center justify-center text-foreground-muted">
                <svg className="w-16 h-16 mb-4 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                <p className="text-sm font-medium">No checkpoints found</p>
                <p className="text-xs mt-2">Checkpoints will be created automatically when CDC replication starts</p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-indigo-500/20">
                      <th className="text-left py-3 px-4 font-semibold text-foreground-muted">Table</th>
                      <th className="text-left py-3 px-4 font-semibold text-foreground-muted">Checkpoint Type</th>
                      <th className="text-left py-3 px-4 font-semibold text-foreground-muted">Position (LSN/SCN/Binlog)</th>
                      <th className="text-right py-3 px-4 font-semibold text-foreground-muted">Rows Processed</th>
                      <th className="text-left py-3 px-4 font-semibold text-foreground-muted">Last Updated</th>
                    </tr>
                  </thead>
                  <tbody>
                    {checkpoints.map((checkpoint: any, index: number) => {
                      const getCheckpointValue = () => {
                        if (checkpoint.lsn) return { type: "LSN", value: checkpoint.lsn }
                        if (checkpoint.scn) return { type: "SCN", value: checkpoint.scn.toString() }
                        if (checkpoint.binlog_file) return { type: "Binlog", value: `${checkpoint.binlog_file}:${checkpoint.binlog_position || 0}` }
                        if (checkpoint.sql_server_lsn) return { type: "LSN", value: checkpoint.sql_server_lsn }
                        if (checkpoint.checkpoint_value) return { type: checkpoint.checkpoint_type || "Checkpoint", value: checkpoint.checkpoint_value }
                        return { type: "N/A", value: "Not set" }
                      }

                      const cpValue = getCheckpointValue()
                      const tableName = checkpoint.schema_name
                        ? `${checkpoint.schema_name}.${checkpoint.table_name}`
                        : checkpoint.table_name

                      return (
                        <tr key={index} className="border-b border-indigo-500/10 hover:bg-indigo-500/5 transition-colors">
                          <td className="py-3 px-4 text-foreground font-medium">{tableName}</td>
                          <td className="py-3 px-4">
                            <span className="px-2 py-1 bg-indigo-500/20 text-indigo-400 rounded text-xs font-medium">
                              {cpValue.type}
                            </span>
                          </td>
                          <td className="py-3 px-4">
                            <span className="font-mono text-xs text-foreground-muted bg-surface/50 px-2 py-1 rounded">
                              {cpValue.value}
                            </span>
                          </td>
                          <td className="py-3 px-4 text-right text-foreground">
                            {checkpoint.rows_processed ? checkpoint.rows_processed.toLocaleString() : "0"}
                          </td>
                          <td className="py-3 px-4 text-foreground-muted text-xs">
                            {checkpoint.last_updated_at
                              ? format(new Date(checkpoint.last_updated_at), "MMM d, yyyy HH:mm:ss")
                              : checkpoint.last_event_timestamp
                                ? format(new Date(checkpoint.last_event_timestamp), "MMM d, yyyy HH:mm:ss")
                                : "N/A"}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
        )}
      </div>
    </ProtectedPage>
  )
}
