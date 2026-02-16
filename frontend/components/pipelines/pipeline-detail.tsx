"use client"

import { useState, useEffect, useMemo, useRef } from "react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { Badge } from "@/components/ui/badge"
import { ArrowLeft, Play, Pause, Square, Download, CheckCircle, Eye, X, Loader2, ChevronLeft, ChevronRight, Activity, Database, AlertCircle, RefreshCw, RotateCw, Zap, Clock } from "lucide-react"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Label } from "@/components/ui/label"
import { useRouter } from "next/navigation"
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"
import { apiClient } from "@/lib/api/client"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { fetchMonitoringMetrics, fetchReplicationEvents } from "@/lib/store/slices/monitoringSlice"
import { triggerPipeline, pausePipeline, stopPipeline, fetchPipelines, updatePipeline, setSelectedPipeline } from "@/lib/store/slices/pipelineSlice"
import { wsClient } from "@/lib/websocket/client"
import { CheckpointManager } from "./checkpoint-manager"
import { useConfirmDialog } from "@/components/ui/confirm-dialog"
import { useErrorToast } from "@/components/ui/error-toast"
import { cn } from "@/lib/utils"

interface Pipeline {
  id: string | number
  name: string
  description?: string
  source_connection_id?: string | number
  target_connection_id?: string | number
  source_connection_uuid?: string  // Actual UUID for connection lookup
  target_connection_uuid?: string  // Actual UUID for connection lookup
  sourceConnection?: string
  targetConnection?: string
  table_mappings?: Array<{
    source_schema?: string
    source_table: string
    target_schema?: string
    target_table: string
  }>
  tables?: string[] // Legacy support
  mode?: string
  full_load_type?: string
  cdc_enabled?: boolean
  cdc_filters?: any[]
  status: string
  lastRun?: string
  nextRun?: string
  tableCount?: number
  recordsProcessed?: number
  lagSeconds?: number
  consumerLag?: any
  created_at?: string
  updated_at?: string
  current_lsn?: string
  current_offset?: string
  current_scn?: string
  last_offset_updated?: string
  // Nested structures from pipeline status API
  full_load?: {
    lsn?: string  // Format: "SCN:17026643" or "LSN:..."
    message?: string
    success?: boolean
    tables_transferred?: any[]
    total_rows?: number
  }
  debezium_connector?: {
    name?: string
    status?: string
    offset?: string | object
  }
  sink_connector?: {
    name?: string
    status?: string
  }
  kafka_topics?: string[]
}

// Performance data will be calculated from real metrics

export function PipelineDetail({ pipeline, onBack }: { pipeline?: Pipeline; onBack: () => void }) {
  const router = useRouter()
  const dispatch = useAppDispatch()
  const { metrics, events } = useAppSelector((state) => state.monitoring)
  const { isLoading: pipelineLoading } = useAppSelector((state) => state.pipelines)
  const { connections } = useAppSelector((state) => state.connections)
  const { showConfirm, ConfirmDialogComponent } = useConfirmDialog()
  const { showError, ErrorToastComponent } = useErrorToast()
  const [selectedTable, setSelectedTable] = useState<{ source: string, target: string, sourceSchema?: string, targetSchema?: string } | null>(null)
  const [sourceData, setSourceData] = useState<any>(null)
  const [targetData, setTargetData] = useState<any>(null)
  const [loadingData, setLoadingData] = useState(false)
  const [dataError, setDataError] = useState<string | null>(null)
  const [sourcePage, setSourcePage] = useState(1)
  const [targetPage, setTargetPage] = useState(1)
  const [retryingTarget, setRetryingTarget] = useState(false)
  const recordsPerPage = 10
  const [isComparing, setIsComparing] = useState(false) // Guard to prevent multiple simultaneous compare calls
  const [replicationProgress, setReplicationProgress] = useState<{
    full_load?: {
      status: string
      progress_percent: number
      records_loaded: number
      total_records: number
      current_table?: string
      error_message?: string
    }
    cdc?: {
      status: string
      events_captured: number
      events_applied: number
      events_failed: number
      last_event_time?: string
      error_message?: string
    }
    consumer_lag?: Record<string, any>
  } | null>(null)
  const progressPollIntervalRef = useRef<NodeJS.Timeout | null>(null)
  const [retryingEventId, setRetryingEventId] = useState<string | null>(null)

  // Fetch connections if not loaded
  useEffect(() => {
    if (connections.length === 0) {
      import("@/lib/store/slices/connectionSlice").then(({ fetchConnections }) => {
        dispatch(fetchConnections())
      })
    }
  }, [connections.length, dispatch])

  // Always refresh pipeline on mount to get latest status (including on page refresh/navigation)
  useEffect(() => {
    if (pipeline?.id) {
      // Always fetch latest pipeline status from backend
      dispatch(fetchPipelines())

      import("@/lib/api/client").then(({ apiClient }) => {
        // Fetch individual pipeline to get latest status and UUIDs
        apiClient.getPipeline(pipeline.id).then((updatedPipeline) => {
          if (updatedPipeline) {
            console.log("[PipelineDetail] Refreshed pipeline:", {
              id: updatedPipeline.id,
              status: updatedPipeline.status,
              source_uuid: updatedPipeline.source_connection_uuid,
              target_uuid: updatedPipeline.target_connection_uuid,
            })
            dispatch(setSelectedPipeline(updatedPipeline))
            dispatch(fetchPipelines()) // Refresh all pipelines to keep list updated
          }
        }).catch((err) => {
          console.warn("Failed to refresh pipeline:", err)
          // Still refresh pipelines list even if individual fetch fails
          dispatch(fetchPipelines())
        })
      })
    }
  }, [pipeline?.id, dispatch])

  // Auto-refresh pipeline status every 10 seconds to keep it updated
  useEffect(() => {
    if (!pipeline?.id) return

    const interval = setInterval(() => {
      // Refresh pipeline status from backend
      dispatch(fetchPipelines())

      import("@/lib/api/client").then(({ apiClient }) => {
        apiClient.getPipeline(pipeline.id).then((updatedPipeline) => {
          if (updatedPipeline) {
            dispatch(setSelectedPipeline(updatedPipeline))
          }
        }).catch((err) => {
          console.warn("Failed to refresh pipeline status:", err)
        })
      })
    }, 10000) // Refresh every 10 seconds

    return () => clearInterval(interval)
  }, [pipeline?.id, dispatch])

  // Extract stable pipeline ID and status to prevent unnecessary re-renders
  const stablePipelineId = useMemo(() => {
    if (!pipeline?.id) return null
    const pipelineIdStr = String(pipeline.id)
    const numId = pipeline.id
    const pipelineId = !isNaN(Number(numId)) && isFinite(Number(numId)) ? numId : pipelineIdStr
    // Validate pipeline ID is not NaN or undefined
    if (!pipelineId || pipelineId === 'NaN' || pipelineId === 'undefined' || (typeof pipelineId === 'number' && isNaN(pipelineId))) {
      return null
    }
    return pipelineId
  }, [pipeline?.id])

  const pipelineStatus = useMemo(() => pipeline?.status || null, [pipeline?.status])

  // Fetch real-time metrics and events for this pipeline
  useEffect(() => {
    if (!stablePipelineId) {
      return
    }

    console.log('[PipelineDetail] Using pipeline ID:', stablePipelineId, 'type:', typeof stablePipelineId)

    dispatch(fetchMonitoringMetrics({ pipelineId: stablePipelineId }))
    dispatch(fetchReplicationEvents({ pipelineId: stablePipelineId, limit: 10000, todayOnly: false })) // Fetch more events to get accurate count

    // Subscribe to WebSocket for real-time updates
    wsClient.subscribePipeline(stablePipelineId)

    // Auto-refresh events every 5 seconds for real-time updates
    const refreshInterval = setInterval(() => {
      dispatch(fetchReplicationEvents({ pipelineId: stablePipelineId, limit: 10000, todayOnly: false }))
    }, 5000)

    return () => {
      wsClient.unsubscribePipeline(stablePipelineId)
      clearInterval(refreshInterval)
    }
  }, [stablePipelineId, dispatch]) // Use stable memoized value instead of pipeline?.id

  // Poll for replication progress
  const progressRetryCountRef = useRef(0)
  const lastPipelineIdRef = useRef<string | number | null>(null)
  const lastPipelineStatusRef = useRef<string | null>(null)
  const fetchProgressRef = useRef<(() => Promise<void>) | null>(null)

  useEffect(() => {
    if (!stablePipelineId) return

    // Get current status directly from pipeline to avoid stale closures
    const currentStatus = pipeline?.status || null

    // Skip if pipeline ID and status haven't changed
    if (lastPipelineIdRef.current === stablePipelineId && lastPipelineStatusRef.current === currentStatus) {
      return
    }

    // Update refs
    lastPipelineIdRef.current = stablePipelineId
    lastPipelineStatusRef.current = currentStatus

    // Create fetchProgress function and store in ref for use in onClick handlers
    const fetchProgress = async () => {
      try {
        const progress = await apiClient.getPipelineProgress(stablePipelineId)

        // Map backend response to expected format (handle both nested and flat formats)
        const mappedProgress: any = {
          full_load: progress.full_load || {
            status: progress.full_load_status?.toLowerCase() || "unknown",
            progress_percent: progress.progress_percentage || 0,
            records_loaded: progress.records_loaded || progress.records_processed || 0,
            total_records: progress.total_records || progress.records_total || 0,
            current_table: progress.current_table,
            error_message: progress.error_message
          },
          cdc: progress.cdc || {
            status: progress.cdc_status?.toLowerCase() || "unknown"
          }
        }

        // Normalize status values
        if (mappedProgress.full_load.status === "completed" || mappedProgress.full_load.status === "COMPLETED") {
          mappedProgress.full_load.status = "completed"
          mappedProgress.full_load.progress_percent = Math.max(mappedProgress.full_load.progress_percent, 100)
        } else if (mappedProgress.full_load.status === "in_progress" || mappedProgress.full_load.status === "IN_PROGRESS" || mappedProgress.full_load.status === "running") {
          mappedProgress.full_load.status = "running"
        } else if (mappedProgress.full_load.status === "failed" || mappedProgress.full_load.status === "FAILED") {
          mappedProgress.full_load.status = "failed"
        }

        setReplicationProgress(mappedProgress)
        progressRetryCountRef.current = 0 // Reset retry count on success
      } catch (error: any) {
        // Handle timeout errors gracefully
        if (error?.isTimeout || error?.code === 'ECONNABORTED') {
          progressRetryCountRef.current++
          // Only log occasionally to avoid spam
          if (progressRetryCountRef.current % 5 === 0) {
            console.warn(`[PipelineProgress] Timeout (attempt ${progressRetryCountRef.current}) - endpoint may be slow`)
          }
        } else {
          console.error("Error fetching progress:", error)
        }
        // Don't update state on error - keep last known progress
      }
    }

    // Store fetchProgress in ref for use in onClick handlers
    fetchProgressRef.current = fetchProgress

    // Clear any existing interval first
    if (progressPollIntervalRef.current) {
      clearInterval(progressPollIntervalRef.current)
      progressPollIntervalRef.current = null
    }

    // Fetch immediately
    fetchProgress()

    // Poll every 3 seconds if pipeline is active/running/starting (reduced from 5s for better responsiveness)
    // Use currentStatus from closure instead of pipelineStatus to avoid stale values
    if (currentStatus === "active" || currentStatus === "running" || currentStatus === "starting") {
      progressPollIntervalRef.current = setInterval(fetchProgress, 3000)
    }

    return () => {
      if (progressPollIntervalRef.current) {
        clearInterval(progressPollIntervalRef.current)
        progressPollIntervalRef.current = null
      }
    }
  }, [stablePipelineId, pipeline?.status]) // Use pipeline?.status directly instead of memoized value

  // Calculate records synced from actual replication events
  const recordsSynced = useMemo(() => {
    if (!pipeline?.id) return 0

    const pipelineIdStr = String(pipeline.id)
    // Count all events for this pipeline (insert, update, delete)
    // Only count successful events (status === "applied" or status !== "failed")
    const pipelineEvents = events.filter(e => {
      const eventPipelineId = String(e.pipeline_id || '')
      return eventPipelineId === pipelineIdStr &&
        e.status !== "failed" &&
        e.status !== "error"
    })
    return pipelineEvents.length
  }, [events, pipeline?.id])

  // Calculate replication lag (average latency in seconds)
  const replicationLag = useMemo(() => {
    if (!pipeline?.id) return null

    const pipelineIdStr = String(pipeline.id)
    const pipelineId = !isNaN(Number(pipeline.id)) ? pipeline.id : pipelineIdStr
    const pipelineEvents = events.filter(e => {
      const eventPipelineId = String(e.pipeline_id || '')
      const currentPipelineId = String(pipelineId)
      return eventPipelineId === currentPipelineId &&
        e.latency_ms &&
        e.latency_ms > 0
    })

    if (pipelineEvents.length === 0) return null

    // Only calculate from events that have been applied (have latency_ms > 0)
    const eventsWithLatency = pipelineEvents.filter(e => e.latency_ms != null && e.latency_ms !== undefined && e.latency_ms > 0)
    if (eventsWithLatency.length === 0) return null // No latency data available

    const avgLatencyMs = eventsWithLatency.reduce((sum, e) => sum + (e.latency_ms || 0), 0) / eventsWithLatency.length
    return Math.round(avgLatencyMs / 1000) // Convert to seconds
  }, [events, pipeline?.id])

  // Calculate performance data from real metrics
  const performanceData = useMemo(() => {
    // Ensure metrics is always an array
    const safeMetrics = Array.isArray(metrics) ? metrics : []
    const safeEvents = Array.isArray(events) ? events : []
    const pipelineIdStr = String(pipeline?.id || '')

    // Filter events by pipeline ID first
    const pipelineEvents = safeEvents.filter(e => {
      if (!e || !e.created_at) return false
      const eventPipelineId = String(e.pipeline_id || '')
      return eventPipelineId === pipelineIdStr
    })

    // Filter metrics by pipeline ID
    const pipelineMetrics = safeMetrics.filter(m => {
      const metricPipelineId = String(m.pipeline_id || '')
      return metricPipelineId === pipelineIdStr
    })

    // If we have metrics, use them (preferred source)
    if (pipelineMetrics.length > 0) {
      const metricsData = pipelineMetrics.slice(-7).map(metric => {
        const date = new Date(metric.timestamp)
        const time = date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
        return {
          time,
          records: (metric as any).total_events || (metric as any).throughput_events_per_sec || 0,
          latency: Math.round((metric as any).avg_latency_ms || ((metric as any).lag_seconds || 0) * 1000 || 0),
        }
      })

      // If we have data, return it
      if (metricsData.length > 0) {
        return metricsData
      }
    }

    // If no metrics, create from events grouped by hour
    if (pipelineEvents.length > 0) {
      const now = new Date()
      const hourlyData = Array.from({ length: 7 }, (_, i) => {
        const hour = new Date(now.getTime() - (6 - i) * 60 * 60 * 1000)
        const time = hour.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })

        const hourEvents = pipelineEvents.filter(e => {
          if (!e || !e.created_at) return false
          const eventTime = new Date(e.created_at)
          return eventTime >= hour && eventTime < new Date(hour.getTime() + 60 * 60 * 1000)
        })

        // Count all event types (insert, update, delete) for records
        const records = hourEvents.filter(e => {
          const eventType = String(e.event_type || '').toLowerCase()
          return eventType === 'insert' || eventType === 'update' || eventType === 'delete'
        }).length

        const latency = hourEvents.length > 0
          ? hourEvents.reduce((sum, e) => sum + (e.latency_ms || 0), 0) / hourEvents.length
          : 0

        return { time, records, latency: Math.round(latency) }
      })

      // If we have any data, return it
      if (hourlyData.some(d => d.records > 0 || d.latency > 0)) {
        return hourlyData
      }
    }

    // Fallback: Return empty data structure so graph still renders
    const now = new Date()
    return Array.from({ length: 7 }, (_, i) => {
      const hour = new Date(now.getTime() - (6 - i) * 60 * 60 * 1000)
      const time = hour.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
      return { time, records: 0, latency: 0 }
    })
  }, [metrics, events, pipeline?.id]) // Add pipeline.id to dependencies for stability

  if (!pipeline) return null

  // Extract table names from table_mappings or use tables array
  const tableNames = pipeline.table_mappings
    ? pipeline.table_mappings.map(tm => `${tm.source_table} → ${tm.target_table}`)
    : (pipeline.tables || [])

  const tableCount = pipeline.table_mappings?.length || pipeline.tables?.length || pipeline.tableCount || 0
  const sourceConnObj = connections.find(c => String(c.id) === String(pipeline.source_connection_id))
  const sourceConn = sourceConnObj?.name || pipeline.sourceConnection || `Connection ${pipeline.source_connection_id || 'N/A'}`
  const targetConnObj = connections.find(c => String(c.id) === String(pipeline.target_connection_id))
  const targetConn = targetConnObj?.name || pipeline.targetConnection || `Connection ${pipeline.target_connection_id || 'N/A'}`

  const handleCompareTable = async (tm: { source_table: string, target_table: string, source_schema?: string, target_schema?: string }) => {
    // Prevent multiple simultaneous calls - check early and return immediately
    if (isComparing || loadingData) {
      console.log("[Compare] Already comparing, ignoring duplicate call")
      return
    }

    // Set flags immediately to prevent duplicate calls
    setIsComparing(true)
    setLoadingData(true)
    setDataError(null)
    setSourceData(null)
    setTargetData(null)

    // Check if pipeline has orphaned connections and fix them automatically before comparing
    if (pipeline?.id && (pipeline.source_connection_uuid || pipeline.target_connection_uuid)) {
      try {
        // Try to verify connections exist by attempting to get connection details
        const sourceConn = pipeline.source_connection_uuid
          ? connections.find(c => String(c.id) === String(pipeline.source_connection_uuid))
          : null
        const targetConn = pipeline.target_connection_uuid
          ? connections.find(c => String(c.id) === String(pipeline.target_connection_uuid))
          : null

        // If connections don't exist in the local list, try to fix orphaned connections
        if ((pipeline.source_connection_uuid && !sourceConn) || (pipeline.target_connection_uuid && !targetConn)) {
          console.log("[Compare] Detected orphaned connections, attempting to fix...")
          try {
            const result = await apiClient.fixOrphanedConnections()
            console.log("[Compare] Fixed orphaned connections:", result)
            // Refresh pipeline after fixing
            const updatedPipeline = await apiClient.getPipeline(pipeline.id)
            dispatch(setSelectedPipeline(updatedPipeline))
            dispatch(fetchPipelines())
            // Update local pipeline reference
            Object.assign(pipeline, updatedPipeline)
          } catch (fixErr) {
            console.warn("[Compare] Failed to fix orphaned connections automatically:", fixErr)
          }
        }
      } catch (err) {
        console.warn("[Compare] Error checking connections:", err)
      }
    }

    // Set selected table after state is set
    // Clean table names and schemas before setting selectedTable
    // Extract schema from table name if it's included (e.g., "public.projects_simple" -> schema: "public", table: "projects_simple")
    let cleanSourceTable = tm.source_table
    let cleanSourceSchema = tm.source_schema
    if (cleanSourceTable.includes('.') && !cleanSourceSchema) {
      const parts = cleanSourceTable.split('.')
      if (parts.length === 2) {
        cleanSourceSchema = parts[0]
        cleanSourceTable = parts[1]
      }
    }

    let cleanTargetTable = tm.target_table
    let cleanTargetSchema = tm.target_schema
    if (cleanTargetTable.includes('.') && !cleanTargetSchema) {
      const parts = cleanTargetTable.split('.')
      if (parts.length === 2) {
        cleanTargetSchema = parts[0]
        cleanTargetTable = parts[1]
      }
    }

    // Also remove schema from table name if it's already in the schema field
    if (cleanTargetTable.includes('.') && cleanTargetSchema) {
      const parts = cleanTargetTable.split('.')
      if (parts.length === 2 && parts[0] === cleanTargetSchema) {
        cleanTargetTable = parts[1]
        console.log(`[Compare] Removed duplicate schema from target table name: "${tm.target_table}" -> "${cleanTargetTable}"`)
      }
    }

    if (cleanSourceTable.includes('.') && cleanSourceSchema) {
      const parts = cleanSourceTable.split('.')
      if (parts.length === 2 && parts[0] === cleanSourceSchema) {
        cleanSourceTable = parts[1]
        console.log(`[Compare] Removed duplicate schema from source table name: "${tm.source_table}" -> "${cleanSourceTable}"`)
      }
    }

    setSelectedTable({
      source: cleanSourceTable,
      target: cleanTargetTable,
      sourceSchema: cleanSourceSchema,
      targetSchema: cleanTargetSchema
    })
    setSourcePage(1) // Reset to first page
    setTargetPage(1) // Reset to first page

    const errors: string[] = []

    try {
      // Get connection types to determine if Oracle (for timeout adjustments)
      // Try to find by UUID first, then by numeric ID
      const sourceConn = pipeline.source_connection_uuid
        ? connections.find(c => String(c.id) === String(pipeline.source_connection_uuid))
        : connections.find(c => String(c.id) === String(pipeline.source_connection_id))
      const targetConn = pipeline.target_connection_uuid
        ? connections.find(c => String(c.id) === String(pipeline.target_connection_uuid))
        : connections.find(c => String(c.id) === String(pipeline.target_connection_id))
      const isSourceOracle = sourceConn?.connection_type?.toLowerCase() === 'oracle'
      const isTargetOracle = targetConn?.connection_type?.toLowerCase() === 'oracle'

      // Fetch source data first (sequential to avoid overwhelming backend)
      let sourceTableName = tm.source_table
      let sourceSchema = tm.source_schema && tm.source_schema !== "undefined" && tm.source_schema.trim() !== ""
        ? tm.source_schema.trim()
        : undefined
      const sourceLimit = isSourceOracle ? 100 : 1000

      // Split table name if it contains schema (e.g., "public.employees" -> schema: "public", table: "employees")
      // Check if table name contains schema (format: "schema.table")
      if (sourceTableName.includes('.') && !sourceSchema) {
        const parts = sourceTableName.split('.')
        if (parts.length === 2) {
          sourceSchema = parts[0]
          sourceTableName = parts[1]
          console.log(`[Compare] Split table name: "${tm.source_table}" -> schema: "${sourceSchema}", table: "${sourceTableName}"`)
        }
      }

      // Handle "undefined" schema string - convert to undefined/null
      if (sourceSchema === "undefined" || sourceSchema?.trim() === "") {
        sourceSchema = undefined
      }

      try {
        console.log(`[Compare] Fetching source table: ${sourceTableName} (schema: ${sourceSchema || 'default'}, connection type: ${sourceConn?.connection_type || 'unknown'})`)

        // Use UUID if available, otherwise fall back to numeric ID
        const sourceConnectionId = pipeline.source_connection_uuid || pipeline.source_connection_id!
        console.log(`[Compare] Using source connection ID: ${sourceConnectionId} (UUID: ${pipeline.source_connection_uuid || 'not available'}, numeric: ${pipeline.source_connection_id})`)
        try {
          const sourceResult = await apiClient.getTableData(
            sourceConnectionId,
            sourceTableName, // Use cleaned table name (without schema prefix)
            sourceSchema, // Use cleaned schema (undefined if "undefined" string)
            sourceLimit,
            isSourceOracle ? 1 : 2, // Fewer retries for Oracle
            isSourceOracle // Pass Oracle flag
          )
          setSourceData(sourceResult)
          console.log(`[Compare] Source table loaded: ${sourceResult?.records?.length || 0} records`, {
            fullResponse: sourceResult,
            records: sourceResult?.records,
            columns: sourceResult?.columns,
            count: sourceResult?.count
          })
        } catch (sourceError: any) {
          const errorMsg = sourceError?.message || "Failed to fetch source table"
          // Check if it's a connection not found error
          if (errorMsg.includes("Connection not found")) {
            setDataError(`⚠️ Source connection not found. The pipeline's source connection (${pipeline.source_connection_uuid || pipeline.source_connection_id}) doesn't exist. Please update the pipeline to use a valid connection.`)
          } else {
            setDataError(`⚠️ Source table unavailable: ${errorMsg}`)
          }
          throw sourceError
        }
      } catch (sourceError: any) {
        console.error("[Compare] Failed to fetch source table:", sourceError)
        let sourceErrMsg = sourceError?.message || sourceError?.response?.data?.detail || "Failed to fetch source table data"

        // Improve error messages for common error codes
        // 08001 is PostgreSQL connection error code
        if (sourceErrMsg.includes('08001') || (sourceError?.code && String(sourceError.code).includes('08001'))) {
          sourceErrMsg = `Database connection error (08001): Cannot connect to source database. ` +
            `This usually means:\n` +
            `1. Database server is not running or unreachable\n` +
            `2. Connection host/port is incorrect\n` +
            `3. Network connectivity issues (firewall blocking)\n` +
            `4. Database credentials are incorrect\n` +
            `Please verify the source database connection configuration and ensure the database server is accessible.`
        }

        // Remove misleading prefixes like "Oracle Database Error:" if it's not actually an Oracle error
        const sourceErrMsgLower = sourceErrMsg.toLowerCase()
        if (sourceErrMsgLower.includes('oracle database error:') &&
          !sourceErrMsgLower.includes('ora-') &&
          (sourceErrMsgLower.includes('08001') ||
            sourceErrMsgLower.includes('postgresql') ||
            sourceErrMsgLower.includes('connection refused'))) {
          // This is incorrectly labeled as Oracle error - remove the prefix
          sourceErrMsg = sourceErrMsg.replace(/^Oracle Database Error:\s*/i, '').trim()
        }

        // Check if it's a connection not found error - try to fix automatically
        if (sourceErrMsg.includes('Connection not found') && pipeline?.id) {
          console.log("[Compare] Source connection not found, attempting to fix orphaned connections...")
          try {
            const result = await apiClient.fixOrphanedConnections()
            console.log("[Compare] Fixed orphaned connections:", result)
            // Refresh pipeline
            const updatedPipeline = await apiClient.getPipeline(pipeline.id)
            dispatch(setSelectedPipeline(updatedPipeline))
            dispatch(fetchPipelines())
            // Update local pipeline reference
            Object.assign(pipeline, updatedPipeline)
            // Retry the source fetch with updated connection
            try {
              const retrySourceConnectionId = updatedPipeline.source_connection_uuid || updatedPipeline.source_connection_id!
              const retrySourceResult = await apiClient.getTableData(
                retrySourceConnectionId,
                sourceTableName,
                sourceSchema,
                sourceLimit,
                isSourceOracle ? 1 : 2,
                isSourceOracle
              )
              setSourceData(retrySourceResult)
              console.log("[Compare] Source table loaded after fix:", retrySourceResult?.records?.length || 0, "records")
              // Don't add to errors since we successfully fetched after fix
              return // Exit early, will continue to target fetch
            } catch (retryErr) {
              console.error("[Compare] Retry after fix failed:", retryErr)
              errors.push(`Source: ${sourceErrMsg} (fix attempted but failed)`)
            }
          } catch (fixErr) {
            console.error("[Compare] Failed to fix orphaned connections:", fixErr)
            errors.push(`Source: ${sourceErrMsg}`)
          }
        } else {
          errors.push(`Source: ${sourceErrMsg}`)
        }
        // Continue to try target even if source fails
      }

      // Fetch target data after source (sequential)
      try {
        // Split table name if it contains schema (e.g., "dbo.employees" -> schema: "dbo", table: "employees")
        let targetTableName = tm.target_table
        let targetSchema = tm.target_schema && tm.target_schema !== "undefined" && tm.target_schema.trim() !== ""
          ? tm.target_schema.trim()
          : undefined

        // Check if table name contains schema (format: "schema.table")
        if (targetTableName.includes('.') && !targetSchema) {
          const parts = targetTableName.split('.')
          if (parts.length === 2) {
            targetSchema = parts[0]
            targetTableName = parts[1]
            console.log(`[Compare] Split target table name: "${tm.target_table}" -> schema: "${targetSchema}", table: "${targetTableName}"`)
          }
        }

        // Handle "undefined" schema string - convert to undefined/null
        if (targetSchema === "undefined" || targetSchema?.trim() === "") {
          targetSchema = undefined
        }

        // SQL Server uses "dbo" as default schema, not "public"
        // If target is SQL Server and schema is "public", convert to undefined (let backend use "dbo")
        const isTargetSQLServer = targetConn?.connection_type?.toLowerCase() === 'sqlserver' || targetConn?.connection_type?.toLowerCase() === 'sql_server'
        if (isTargetSQLServer && targetSchema?.toLowerCase() === 'public') {
          console.log(`[Compare] SQL Server detected: Converting 'public' schema to undefined (will use 'dbo' default)`)
          targetSchema = undefined
        }

        console.log(`[Compare] Fetching target table: ${targetTableName} (schema: ${targetSchema || 'default'}, connection type: ${targetConn?.connection_type || 'unknown'})`)

        // Fetch all records (backend supports up to 1000)
        // For Oracle connections, use smaller limit to avoid timeouts, but still fetch more records
        const targetLimit = isTargetOracle ? 100 : 1000 // Increased from 20/25 to 100/1000
        // Use UUID if available, otherwise fall back to numeric ID
        const targetConnectionId = pipeline.target_connection_uuid || pipeline.target_connection_id!
        console.log(`[Compare] Using target connection ID: ${targetConnectionId} (UUID: ${pipeline.target_connection_uuid || 'not available'}, numeric: ${pipeline.target_connection_id})`)
        console.log(`[Compare] Calling getTableData with:`, {
          connectionId: targetConnectionId,
          tableName: targetTableName,
          schema: targetSchema,
          limit: targetLimit
        })

        const targetResult = await apiClient.getTableData(
          targetConnectionId,
          targetTableName, // Use cleaned table name (without schema prefix)
          targetSchema, // Use cleaned schema (undefined if "undefined" string or SQL Server "public")
          targetLimit,
          isTargetOracle ? 1 : 2, // Fewer retries for Oracle
          isTargetOracle // Pass Oracle flag
        )

        console.log(`[Compare] Raw target result:`, targetResult)

        // Validate and set target data
        if (targetResult && typeof targetResult === 'object') {
          // Check if it's an error response
          if (targetResult.success === false) {
            console.error('[Compare] Target result indicates failure:', targetResult)
            throw new Error(targetResult.error || 'Failed to fetch target table data')
          }

          // Ensure records is an array (default to empty array if missing)
          if (!Array.isArray(targetResult.records)) {
            console.warn('[Compare] Target result records is not an array, setting to empty array:', targetResult.records)
            targetResult.records = []
          }

          // Ensure columns is an array (default to empty array if missing)
          if (!Array.isArray(targetResult.columns)) {
            console.warn('[Compare] Target result columns is not an array, setting to empty array:', targetResult.columns)
            targetResult.columns = []
          }

          // Ensure count is a number
          if (typeof targetResult.count !== 'number') {
            targetResult.count = targetResult.records?.length || 0
          }

          // Validate that we actually got data
          if (targetResult.success === true && Array.isArray(targetResult.records) && targetResult.records.length === 0 && targetResult.count > 0) {
            console.warn(`[Compare] WARNING: Target table has count=${targetResult.count} but records array is empty! This may indicate a data fetching issue.`)
            console.warn(`[Compare] Target table details:`, {
              tableName: targetTableName,
              schema: targetSchema,
              connectionId: targetConnectionId,
              connectionType: targetConn?.connection_type,
              result: targetResult
            })
          }

          // If records are empty and count is 0, check if table exists
          if (targetResult?.count === 0 && (!Array.isArray(targetResult?.records) || targetResult.records.length === 0)) {
            console.warn(`[Compare] Target table appears to be empty. This could mean: 1) Table doesn't exist, 2) Table exists but has no data, 3) Wrong table/schema name`)
            console.warn(`[Compare] Table mapping: source="${tm.source_table}" -> target="${tm.target_table}" (schema: ${tm.target_schema || 'default'})`)
            console.warn(`[Compare] Using table name: "${targetTableName}" with schema: "${targetSchema || 'default'}"`)
          }

          setTargetData(targetResult)
          console.log(`[Compare] Target table loaded: ${targetResult?.records?.length || 0} records (count: ${targetResult?.count || 0})`, {
            fullResponse: targetResult,
            records: targetResult?.records,
            recordsLength: targetResult?.records?.length,
            columns: targetResult?.columns,
            columnsLength: targetResult?.columns?.length,
            count: targetResult?.count,
            success: targetResult?.success,
            hasRecords: Array.isArray(targetResult?.records) && targetResult.records.length > 0,
            hasColumns: Array.isArray(targetResult?.columns) && targetResult.columns.length > 0,
            tableName: targetTableName,
            schema: targetSchema,
            connectionId: targetConnectionId,
            originalMapping: {
              source_table: tm.source_table,
              target_table: tm.target_table,
              target_schema: tm.target_schema
            }
          })

          // Log warning if records are empty but count > 0
          if (targetResult?.count > 0 && (!Array.isArray(targetResult?.records) || targetResult.records.length === 0)) {
            console.warn(`[Compare] Target table has ${targetResult.count} records but records array is empty. This may indicate a data fetching issue.`)
            console.warn(`[Compare] Please check: 1) Table name is correct (${targetTableName}), 2) Schema is correct (${targetSchema || 'default'}), 3) Connection is valid (${targetConnectionId})`)
          }
        } else {
          console.error('[Compare] Invalid target result (not an object):', targetResult, typeof targetResult)
          // Even if invalid, set empty structure to show the table card
          setTargetData({
            success: false,
            records: [],
            columns: [],
            count: 0,
            error: 'Target table data format is invalid. Backend returned: ' + (typeof targetResult === 'string' ? targetResult : JSON.stringify(targetResult))
          })
          // Don't throw error, just set empty data so UI can show error message
          console.error('[Compare] Target data is invalid, but continuing to show empty table')
        }
      } catch (targetError: any) {
        console.error("[Compare] Failed to fetch target table:", targetError)
        // Extract error details more carefully
        const errorDetails: any = {
          message: targetError?.message,
          name: targetError?.name,
          code: targetError?.code,
          table: tm.target_table,
          schema: tm.target_schema,
          connectionId: pipeline.target_connection_id
        }
        if (targetError?.response) {
          errorDetails.response = {
            status: targetError.response.status,
            statusText: targetError.response.statusText,
            data: targetError.response.data
          }
        }
        if (targetError?.isTimeout) errorDetails.isTimeout = true
        if (targetError?.isNetworkError) errorDetails.isNetworkError = true
        console.error("[Compare] Target error details:", errorDetails)

        // Extract more specific error message
        let targetErrMsg = "Failed to fetch target table data"
        if (targetError?.response?.data?.detail) {
          targetErrMsg = targetError.response.data.detail
        } else if (targetError?.message) {
          targetErrMsg = targetError.message
        }

        // Remove misleading prefixes like "Oracle Database Error:" if it's not actually an Oracle error
        // This can happen if error detection is too broad
        const targetErrMsgLower = targetErrMsg.toLowerCase()
        if (targetErrMsgLower.includes('oracle database error:') &&
          !targetErrMsgLower.includes('ora-') &&
          (targetErrMsgLower.includes('aws_s3') ||
            targetErrMsgLower.includes('s3') ||
            targetErrMsgLower.includes('object storage') ||
            targetErrMsgLower.includes('table comparison is not supported'))) {
          // This is incorrectly labeled as Oracle error - remove the prefix
          targetErrMsg = targetErrMsg.replace(/^Oracle Database Error:\s*/i, '').trim()
        }

        // Simplify long error messages but keep important parts
        if (targetErrMsg.length > 400) {
          // Keep the first part and important keywords
          const importantKeywords = ['timeout', 'connection', 'network', 'database', 'unreachable', 'oracle', 'not found', 'does not exist', 's3', 'aws_s3', 'object storage']
          const hasImportant = importantKeywords.some(kw => targetErrMsg.toLowerCase().includes(kw))
          if (hasImportant) {
            targetErrMsg = targetErrMsg.substring(0, 400) + "..."
          } else {
            targetErrMsg = targetErrMsg.substring(0, 200) + "..."
          }
        }

        errors.push(`Target: ${targetErrMsg}`)
        // Set targetData to null on error to prevent showing stale data
        setTargetData(null)
        // Continue even if target fails - at least show source if available
      }

      // If both failed, show error
      if (errors.length === 2) {
        const hasOrphanedConnections = errors.some(err => err.includes('Connection not found'))
        let errorMsg = `Failed to fetch both tables:\n${errors.join('\n\n')}\n\nPlease check:\n1. Database connections are configured correctly\n2. Database servers are running and accessible\n3. Network connectivity is available`
        if (hasOrphanedConnections) {
          errorMsg += `\n\n⚠️ This pipeline references connections that don't exist. The system will attempt to fix this automatically.`
        }
        setDataError(errorMsg)
      } else if (errors.length === 1) {
        // Partial success - show warning but allow viewing the successful table
        // Make it less alarming since we're showing available data
        const errorType = errors[0].startsWith('Target:') ? 'target' : 'source'
        const simplifiedError = errors[0].replace(/^Target: |^Source: /, '')

        // Check if it's a network/timeout error
        if (simplifiedError.includes('Network Error') || simplifiedError.includes('timeout')) {
          setDataError(`⚠️ ${errorType === 'target' ? 'Target' : 'Source'} table unavailable: ${simplifiedError}\n\nShowing available data. Please check the ${errorType} database connection.`)
        } else {
          setDataError(`⚠️ ${errorType === 'target' ? 'Target' : 'Source'} table: ${simplifiedError}\n\nShowing available data.`)
        }
      }
      // If no errors, both tables loaded successfully

    } catch (error: any) {
      // Only log and set error if not already handled
      if (!error?.handled) {
        console.error("[Compare] Unexpected error in handleCompareTable:", error)
        let errorMessage = "Failed to fetch table data"
        if (error?.message?.includes('timeout') || error?.isTimeout) {
          errorMessage = "Request timeout: The database connection is taking too long. Please check:\n1. Database servers are running and accessible\n2. Network connectivity is stable\n3. Connection credentials are correct"
        } else if (error?.isNetworkError || error?.message?.includes('Network Error')) {
          errorMessage = "Network error: Cannot connect to backend or database. Please check:\n1. Backend is running on http://localhost:8000\n2. Database servers are accessible\n3. Firewall/network settings allow connections"
        } else if (error?.response?.data?.detail) {
          errorMessage = error.response.data.detail
        } else if (error?.message) {
          errorMessage = error.message
        }
        setDataError(errorMessage)
      }
    } finally {
      // Always reset flags, even on error
      setLoadingData(false)
      setIsComparing(false) // Reset guard
    }
  }

  const closeComparison = () => {
    setSelectedTable(null)
    setSourceData(null)
    setTargetData(null)
    setDataError(null)
    setSourcePage(1)
    setTargetPage(1)
  }

  // Separate pagination calculations for source and target
  // Handle different response formats: records array or data array
  const sourceRecords = Array.isArray(sourceData?.records)
    ? sourceData.records
    : Array.isArray(sourceData?.data)
      ? sourceData.data
      : []
  // Extract records from targetData - backend returns {success, records, columns, count}
  const targetRecords = Array.isArray(targetData?.records)
    ? targetData.records
    : Array.isArray(targetData?.data)
      ? targetData.data
      : []

  // Calculate pagination
  const sourceTotalPages = Math.max(1, Math.ceil(sourceRecords.length / recordsPerPage))
  const targetTotalPages = Math.max(1, Math.ceil(targetRecords.length / recordsPerPage))
  const sourceStartIndex = (sourcePage - 1) * recordsPerPage
  const sourceEndIndex = sourceStartIndex + recordsPerPage
  const targetStartIndex = (targetPage - 1) * recordsPerPage
  const targetEndIndex = targetStartIndex + recordsPerPage
  const paginatedSourceRecords = sourceRecords.slice(sourceStartIndex, sourceEndIndex)
  const paginatedTargetRecords = targetRecords.slice(targetStartIndex, targetEndIndex)

  // Debug logging - after variables are defined
  if (process.env.NODE_ENV === 'development' && (sourceData || targetData)) {
    console.log('[Table Data] Source data structure:', {
      hasRecords: !!sourceData?.records,
      recordsLength: sourceData?.records?.length,
      hasData: !!sourceData?.data,
      dataLength: sourceData?.data?.length,
      sourceRecordsLength: sourceRecords.length,
      paginatedLength: paginatedSourceRecords.length,
      sourcePage: sourcePage,
      sourceStartIndex: sourceStartIndex,
      sourceEndIndex: sourceEndIndex,
      firstRecord: sourceRecords[0],
      fullSourceData: sourceData
    })
    console.log('[Table Data] Target data structure:', {
      hasRecords: Array.isArray(targetData?.records) && targetData.records.length > 0,
      recordsLength: Array.isArray(targetData?.records) ? targetData.records.length : 0,
      hasData: Array.isArray(targetData?.records) && targetData.records.length > 0,
      dataLength: Array.isArray(targetData?.records) ? targetData.records.length : 0,
      targetRecordsLength: targetRecords.length,
      paginatedLength: paginatedTargetRecords.length,
      targetPage: targetPage,
      targetStartIndex: targetStartIndex,
      targetEndIndex: targetEndIndex,
      firstRecord: targetRecords && targetRecords.length > 0 ? targetRecords[0] : (Array.isArray(targetData?.records) && targetData.records.length > 0 ? targetData.records[0] : null),
      fullTargetData: targetData,
      targetRecords: targetRecords,
      targetDataRecords: targetData?.records,
      targetDataRecordsType: Array.isArray(targetData?.records) ? 'array' : typeof targetData?.records
    })
    console.log('[Pagination] Source:', {
      totalRecords: sourceRecords.length,
      page: sourcePage,
      totalPages: sourceTotalPages,
      startIndex: sourceStartIndex,
      endIndex: sourceEndIndex,
      paginatedCount: paginatedSourceRecords.length,
      firstRecord: paginatedSourceRecords[0]
    })
    console.log('[Pagination] Target:', {
      totalRecords: targetRecords.length,
      page: targetPage,
      totalPages: targetTotalPages,
      startIndex: targetStartIndex,
      endIndex: targetEndIndex,
      paginatedCount: paginatedTargetRecords.length,
      firstRecord: paginatedTargetRecords[0]
    })
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="space-y-4">
        {/* First Line: Back Button, Title, Connections, Status Badge */}
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-6 pb-2">
          <div className="flex items-center gap-4">
            <Button
              variant="outline"
              size="sm"
              onClick={onBack}
              className="bg-white/5 border-border hover:bg-white/10 text-foreground transition-all h-10 px-4"
            >
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back
            </Button>
            <div className="space-y-1">
              <h1 className="text-4xl font-black text-foreground tracking-tighter drop-shadow-sm">{pipeline.name}</h1>
              <div className="flex items-center gap-3 text-foreground-muted">
                <div className="flex items-center gap-2 px-2 py-0.5 bg-muted rounded-md border border-border/50">
                  <Database className="w-3.5 h-3.5 text-primary/70" />
                  <span className="text-xs font-bold">{sourceConn}</span>
                </div>
                <Zap className="w-3.5 h-3.5 text-info animate-pulse" />
                <div className="flex items-center gap-2 px-2 py-0.5 bg-muted rounded-md border border-border/50">
                  <Database className="w-3.5 h-3.5 text-info/70" />
                  <span className="text-xs font-bold">{targetConn}</span>
                </div>
              </div>
            </div>
          </div>

          {/* Deep Status Card - From User Request */}
          <Card className={cn(
            "relative overflow-hidden border-none shadow-[0_15px_30px_-5px_rgba(0,0,0,0.2)] dark:shadow-[0_15px_40px_-10px_rgba(0,0,0,0.5)]",
            "rounded-3xl min-w-[240px] h-24 flex flex-col justify-center px-8 transition-all duration-500 group",
            (pipeline.status === "active" || pipeline.status === "running") ? "bg-emerald-500/5 ring-1 ring-emerald-500/20" :
              (pipeline.status === "paused") ? "bg-amber-500/5 ring-1 ring-amber-500/20" :
                "bg-red-500/5 ring-1 ring-red-500/20"
          )}>
            {/* Background Heartbeat Animation */}
            <div className="absolute inset-0 opacity-[0.15] group-hover:opacity-[0.25] transition-opacity">
              <svg viewBox="0 0 100 40" className="w-full h-full" preserveAspectRatio="none">
                {/* Static Base Line */}
                <path d="M0,20 L15,20 L20,20 L22,10 L28,30 L30,20 L45,20 L50,20 L52,-5 L58,45 L60,20 L80,20 L82,15 L88,25 L90,20 L100,20"
                  fill="none"
                  stroke={(pipeline.status === "active" || pipeline.status === "running") ? "rgb(16, 185, 129)" :
                    (pipeline.status === "paused") ? "rgb(245, 158, 11)" : "rgb(239, 68, 68)"}
                  strokeWidth="0.5"
                  className="opacity-20"
                />
                {/* Animated Pulse Line */}
                <path d="M0,20 L15,20 L20,20 L22,10 L28,30 L30,20 L45,20 L50,20 L52,-5 L58,45 L60,20 L80,20 L82,15 L88,25 L90,20 L100,20"
                  fill="none"
                  stroke={(pipeline.status === "active" || pipeline.status === "running") ? "rgb(16, 185, 129)" :
                    (pipeline.status === "paused") ? "rgb(245, 158, 11)" : "rgb(239, 68, 68)"}
                  strokeWidth="0.8"
                  className="animate-heart-pulse"
                />
              </svg>
            </div>

            {/* Very Deep Background Icon */}
            <div className="absolute -right-6 -bottom-6 transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
              <Activity className={cn(
                "w-36 h-36 opacity-[0.03] dark:opacity-[0.05] group-hover:opacity-[0.1] transition-opacity",
                (pipeline.status === "active" || pipeline.status === "running") ? "text-emerald-500" :
                  (pipeline.status === "paused") ? "text-amber-500" : "text-red-500"
              )} />
            </div>

            <div className="relative z-10 flex flex-col">
              <span className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-[0.25em] mb-1">STATUS</span>
              <div className="flex items-center gap-3">
                <span className={cn(
                  "text-3xl font-black tracking-tighter drop-shadow-md transition-transform duration-500 group-hover:scale-105 origin-left",
                  (pipeline.status === "active" || pipeline.status === "running") ? "text-emerald-500" :
                    (pipeline.status === "paused") ? "text-amber-500" : "text-red-500"
                )}>
                  {(pipeline.status || 'UNKNOWN').toUpperCase()}
                </span>
                <div className={cn(
                  "w-3 h-3 rounded-full animate-ping shadow-[0_0_10px_currentColor]",
                  (pipeline.status === "active" || pipeline.status === "running") ? "bg-emerald-500 text-emerald-400" :
                    (pipeline.status === "paused") ? "bg-amber-500 text-amber-400" : "bg-red-500 text-red-400"
                )}></div>
              </div>
            </div>

            {/* Reflection Shine */}
            <div className="absolute inset-0 bg-gradient-to-tr from-white/5 via-transparent to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-1000"></div>
          </Card>
        </div>

        {/* Second Line: Replication Mode and Action Buttons */}
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <Label className="text-sm text-foreground-muted whitespace-nowrap">Replication Mode:</Label>
            <Select
              value={
                pipeline.cdc_enabled
                  ? (pipeline.full_load_type === "overwrite" ? "full_load_cdc" : "cdc_only")
                  : "full_load"
              }
              onValueChange={async (value) => {
                // Only allow changes when pipeline is stopped, paused, or draft
                const canChange = pipeline.status === "stopped" || pipeline.status === "paused" || pipeline.status === "draft" || pipeline.status === "deleted" || !pipeline.status || (pipeline.status !== "active" && pipeline.status !== "running")

                if (!canChange) {
                  showError("Please stop the pipeline before changing replication mode. Running pipelines cannot be modified.", "Cannot Change Mode")
                  return
                }

                if (!pipeline.id) return

                try {
                  // Determine cdc_enabled and full_load_type based on mode
                  const cdc_enabled = value !== "full_load"
                  const full_load_type = value === "full_load_cdc" ? "overwrite" : value === "cdc_only" ? "append" : "overwrite"

                  // Build update data, only include fields that are provided
                  const updateData: any = {
                    name: pipeline.name,
                    full_load_type: full_load_type,
                    cdc_enabled: cdc_enabled,
                  }

                  // Only include optional fields if they have values
                  if (pipeline.description !== undefined && pipeline.description !== null) {
                    updateData.description = pipeline.description
                  }

                  if (pipeline.source_connection_id !== undefined && pipeline.source_connection_id !== null) {
                    updateData.source_connection_id = String(pipeline.source_connection_id)
                  }

                  if (pipeline.target_connection_id !== undefined && pipeline.target_connection_id !== null) {
                    updateData.target_connection_id = String(pipeline.target_connection_id)
                  }

                  if (pipeline.cdc_filters && Array.isArray(pipeline.cdc_filters) && pipeline.cdc_filters.length > 0) {
                    updateData.cdc_filters = pipeline.cdc_filters
                  }

                  if (pipeline.table_mappings && Array.isArray(pipeline.table_mappings) && pipeline.table_mappings.length > 0) {
                    updateData.table_mappings = pipeline.table_mappings
                  }

                  const updatedPipeline = await dispatch(updatePipeline({
                    id: pipeline.id,
                    data: updateData
                  })).unwrap()

                  // Update selected pipeline with the response
                  if (updatedPipeline) {
                    dispatch(setSelectedPipeline(updatedPipeline))
                  }

                  // Also refresh the pipelines list
                  await dispatch(fetchPipelines())
                } catch (error: any) {
                  console.error("Failed to update replication mode:", error)
                  // Extract detailed error message
                  let errorMessage = "Failed to update replication mode"

                  // Handle Pydantic validation errors (array format)
                  if (error?.response?.data?.detail) {
                    const detail = error.response.data.detail
                    if (Array.isArray(detail)) {
                      // Pydantic validation errors are arrays
                      errorMessage = detail.map((err: any) => {
                        if (typeof err === 'object' && err.loc && err.msg) {
                          return `${err.loc.join('.')}: ${err.msg}`
                        } else if (typeof err === 'object' && err.msg) {
                          return err.msg
                        } else if (typeof err === 'string') {
                          return err
                        } else {
                          return JSON.stringify(err)
                        }
                      }).join(', ')
                    } else if (typeof detail === 'string') {
                      errorMessage = detail
                    } else if (typeof detail === 'object') {
                      errorMessage = JSON.stringify(detail)
                    }
                  } else if (error?.response?.data?.message) {
                    errorMessage = error.response.data.message
                  } else if (error?.message) {
                    errorMessage = error.message
                  } else if (typeof error === 'string') {
                    errorMessage = error
                  } else if (Array.isArray(error)) {
                    // Handle array of errors
                    errorMessage = error.map((e: any) => {
                      if (typeof e === 'object' && e.message) {
                        return e.message
                      } else if (typeof e === 'object') {
                        return JSON.stringify(e)
                      } else {
                        return String(e)
                      }
                    }).join(', ')
                  } else if (typeof error === 'object') {
                    errorMessage = JSON.stringify(error)
                  }

                  showError(errorMessage, "Error")
                }
              }}
              disabled={pipeline.status === "active" || pipeline.status === "running" || pipelineLoading}
            >
              <SelectTrigger className="w-[200px] bg-input border-border text-foreground">
                <SelectValue />
              </SelectTrigger>
              <SelectContent className="bg-surface border-border">
                <SelectItem value="full_load" className="text-foreground focus:bg-surface-hover focus:text-foreground">
                  Full Load Only
                </SelectItem>
                <SelectItem value="cdc_only" className="text-foreground focus:bg-surface-hover focus:text-foreground">
                  CDC Only
                </SelectItem>
                <SelectItem value="full_load_cdc" className="text-foreground focus:bg-surface-hover focus:text-foreground">
                  Full Load + CDC
                </SelectItem>
              </SelectContent>
            </Select>
            {(pipeline.status === "active" || pipeline.status === "running") && (
              <span className="text-xs text-foreground-muted italic">
                (Stop pipeline to change mode)
              </span>
            )}
          </div>

          {/* Action Buttons */}
          <div className="flex items-center gap-2">
            {(pipeline.status === "active" || pipeline.status === "running" || pipeline.status === "starting") ? (
              <>
                <Button
                  variant="outline"
                  className="bg-transparent border-border hover:bg-surface-hover text-foreground hover:text-foreground gap-2"
                  onClick={async () => {
                    if (pipeline.id) {
                      try {
                        await dispatch(pausePipeline(pipeline.id)).unwrap()
                        await dispatch(fetchPipelines())
                        // Also fetch the specific pipeline to update selectedPipeline
                        if (pipeline.id) {
                          try {
                            const updatedPipeline = await apiClient.getPipeline(pipeline.id)
                            dispatch(setSelectedPipeline(updatedPipeline))
                          } catch (error) {
                            console.error("Failed to fetch updated pipeline:", error)
                          }
                        }
                      } catch (error: any) {
                        console.error("Failed to pause pipeline:", error)
                        const errorMessage = error?.payload ||
                          error?.message ||
                          error?.response?.data?.detail ||
                          "Failed to pause pipeline"
                        showError(errorMessage, "Failed to Pause Pipeline")
                      }
                    }
                  }}
                  disabled={pipelineLoading}
                >
                  <Pause className="w-4 h-4" />
                  Pause
                </Button>
                <Button
                  variant="outline"
                  className="bg-transparent border-border hover:bg-error/10 text-error hover:text-error border-error/30 hover:border-error/50 gap-2"
                  onClick={() => {
                    if (pipeline.id) {
                      showConfirm(
                        "Stop Pipeline",
                        `Are you sure you want to stop "${pipeline.name}"? This will pause the pipeline and stop all ongoing replication processes. Any in-progress operations will be halted.`,
                        async () => {
                          try {
                            await dispatch(stopPipeline(pipeline.id)).unwrap()
                            await dispatch(fetchPipelines())
                            // Also fetch the specific pipeline to update selectedPipeline
                            if (pipeline.id) {
                              try {
                                const updatedPipeline = await apiClient.getPipeline(pipeline.id)
                                dispatch(setSelectedPipeline(updatedPipeline))
                              } catch (error) {
                                console.error("Failed to fetch updated pipeline:", error)
                              }
                            }
                            // Clear replication progress when pipeline is stopped
                            setReplicationProgress(null)
                          } catch (error: any) {
                            console.error("Failed to stop pipeline:", error)
                            // Show more detailed error message
                            const errorMessage = error?.message ||
                              error?.payload ||
                              (typeof error === 'string' ? error : 'Failed to stop pipeline')
                            showError(errorMessage, "Failed to Stop Pipeline")
                          }
                        },
                        "danger",
                        "Stop Pipeline",
                        "Cancel"
                      )
                    }
                  }}
                  disabled={pipelineLoading}
                >
                  <Square className="w-4 h-4" />
                  Stop
                </Button>
              </>
            ) : (
              <Button
                className="bg-primary hover:bg-primary/90 text-foreground gap-2"
                onClick={async () => {
                  if (pipeline.id) {
                    try {
                      // Determine runType based on pipeline configuration
                      let runType = "full_load"
                      if (pipeline.cdc_enabled) {
                        // If CDC is enabled, use full_load_cdc to do both
                        runType = "full_load_cdc"
                      }
                      // Reset progress before starting
                      setReplicationProgress(null)

                      await dispatch(triggerPipeline({ id: pipeline.id, runType })).unwrap()

                      // Refresh pipelines to get updated status
                      await dispatch(fetchPipelines())

                      // Also fetch the specific pipeline to update selectedPipeline
                      if (pipeline.id) {
                        try {
                          const updatedPipeline = await apiClient.getPipeline(pipeline.id)
                          dispatch(setSelectedPipeline(updatedPipeline))
                        } catch (error) {
                          console.error("Failed to fetch updated pipeline:", error)
                        }
                      }

                      // Progress polling is handled by the useEffect hook
                      // Trigger an immediate fetch after pipeline starts
                      if (fetchProgressRef.current) {
                        // Small delay to let backend update pipeline status
                        setTimeout(() => {
                          if (fetchProgressRef.current) {
                            fetchProgressRef.current().catch((err) => {
                              console.debug("Initial progress fetch after trigger:", err)
                            })
                          }
                        }, 1000)
                      }
                    } catch (error: any) {
                      console.error("Failed to trigger pipeline:", error)
                      // Extract detailed error message
                      let errorMessage = "Failed to trigger pipeline"
                      if (error?.message) {
                        errorMessage = error.message
                      } else if (error?.response?.data?.detail) {
                        errorMessage = error.response.data.detail
                      } else if (error?.response?.data?.message) {
                        errorMessage = error.response.data.message
                      } else if (typeof error === 'string') {
                        errorMessage = error
                      }
                      // Extract actual error from Debezium connector error
                      let displayError = errorMessage

                      // Remove common wrapper messages
                      if (displayError.includes("Failed to create Debezium connector")) {
                        // Extract the actual Kafka Connect error after the colon
                        const match = displayError.match(/Failed to create Debezium connector[^:]*:\s*(.+)/)
                        if (match && match[1]) {
                          displayError = match[1].trim()
                        }
                      }

                      // Remove HTTP error wrapper if present
                      if (displayError.includes("400 Client Error") || displayError.includes("Bad Request")) {
                        // Try to extract the actual error message
                        const parts = displayError.split("for url:")
                        if (parts.length > 1) {
                          // The actual error should be before "for url:" or in the response data
                          displayError = parts[0].replace("400 Client Error:", "").replace("Bad Request", "").trim()
                        }
                        // Also check response data
                        if (error?.response?.data?.detail) {
                          displayError = error.response.data.detail
                        }
                      }

                      // If still generic, try to get from response
                      if (displayError.includes("400") && error?.response?.data?.detail) {
                        displayError = error.response.data.detail
                      }

                      showError(displayError, "Failed to Start Pipeline")
                    }
                  }
                }}
                disabled={pipelineLoading}
              >
                <Play className="w-4 h-4" />
                Run Pipeline
              </Button>
            )}
            <Button
              variant="outline"
              className="bg-transparent border-border hover:bg-surface-hover text-foreground hover:text-foreground gap-2"
              onClick={() => router.push(`/pipelines/${pipeline.id}/monitor`)}
            >
              <Activity className="w-4 h-4" />
              Monitor
            </Button>
            <Button
              variant="outline"
              className="bg-transparent border-border hover:bg-surface-hover text-foreground hover:text-foreground gap-2"
              onClick={async () => {
                if (!pipeline?.id) return
                try {
                  const blob = await apiClient.exportPipelineDag(pipeline.id)
                  const url = window.URL.createObjectURL(blob)
                  const a = document.createElement('a')
                  a.href = url
                  a.download = `pipeline_${pipeline.id}_dag.py`
                  document.body.appendChild(a)
                  a.click()
                  window.URL.revokeObjectURL(url)
                  document.body.removeChild(a)
                } catch (error: any) {
                  console.error("Failed to export DAG:", error)
                  showError(error?.message || "Failed to export DAG file", "Export Failed")
                }
              }}
              disabled={pipelineLoading || !pipeline?.id}
            >
              <Download className="w-4 h-4" />
              Export DAG
            </Button>
          </div>
        </div>
      </div>

      {/* Offset/LSN/SCN Information */}
      {(() => {
        // Extract SCN/LSN from various possible locations in the pipeline data
        let currentLsn: string | undefined = pipeline?.current_lsn
        let currentOffset: string | undefined = pipeline?.current_offset
        let currentScn: string | undefined = pipeline?.current_scn

        // Check full_load.lsn (format: "SCN:17026643" or "LSN:...")
        if ((pipeline as any)?.full_load?.lsn) {
          const fullLoadLsn = (pipeline as any).full_load.lsn
          if (typeof fullLoadLsn === 'string') {
            // Parse formats like "SCN:17026643" or "LSN:..."
            if (fullLoadLsn.startsWith('SCN:')) {
              currentScn = fullLoadLsn.substring(4).trim() // Extract "17026643"
            } else if (fullLoadLsn.startsWith('LSN:')) {
              currentLsn = fullLoadLsn.substring(4).trim() // Extract LSN value
            } else {
              // If no prefix, check if it looks like SCN (numeric) or LSN (hex)
              if (/^\d+$/.test(fullLoadLsn)) {
                currentScn = fullLoadLsn
              } else {
                currentLsn = fullLoadLsn
              }
            }
          }
        }

        // Check debezium_connector for offset information
        if ((pipeline as any)?.debezium_connector?.offset) {
          const offset = (pipeline as any).debezium_connector.offset
          if (typeof offset === 'string') {
            currentOffset = offset
          } else if (typeof offset === 'object') {
            // Debezium offset might be an object with scn, lsn, etc.
            if (offset.scn) {
              currentScn = String(offset.scn)
            }
            if (offset.lsn) {
              currentLsn = String(offset.lsn)
            }
            if (offset.binlog_file && offset.binlog_position) {
              currentOffset = `${offset.binlog_file}:${offset.binlog_position}`
            }
          }
        }

        // Only show if we have at least one value
        if (!currentLsn && !currentOffset && !currentScn) {
          return null
        }

        return (
          <Card className="bg-surface border-border p-6">
            <h3 className="text-lg font-semibold text-foreground mb-4">Replication Offset</h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {currentLsn && (
                <div className="space-y-2">
                  <Label className="text-sm text-foreground-muted">Current LSN</Label>
                  <div className="p-3 bg-surface-hover rounded-lg border border-border">
                    <code className="text-sm text-foreground font-mono break-all">{currentLsn}</code>
                  </div>
                  {pipeline?.last_offset_updated && (
                    <p className="text-xs text-foreground-muted">
                      Updated: {new Date(pipeline.last_offset_updated).toLocaleString()}
                    </p>
                  )}
                </div>
              )}
              {currentOffset && (
                <div className="space-y-2">
                  <Label className="text-sm text-foreground-muted">Binlog Position</Label>
                  <div className="p-3 bg-surface-hover rounded-lg border border-border">
                    <code className="text-sm text-foreground font-mono break-all">{currentOffset}</code>
                  </div>
                  {pipeline?.last_offset_updated && (
                    <p className="text-xs text-foreground-muted">
                      Updated: {new Date(pipeline.last_offset_updated).toLocaleString()}
                    </p>
                  )}
                </div>
              )}
              {currentScn && (
                <div className="space-y-2">
                  <Label className="text-sm text-foreground-muted">Current SCN</Label>
                  <div className="p-3 bg-surface-hover rounded-lg border border-border">
                    <code className="text-sm text-foreground font-mono break-all">{currentScn}</code>
                  </div>
                  {pipeline?.last_offset_updated && (
                    <p className="text-xs text-foreground-muted">
                      Updated: {new Date(pipeline.last_offset_updated).toLocaleString()}
                    </p>
                  )}
                </div>
              )}
            </div>
          </Card>
        )
      })()}

      {/* Replication Progress - Always Show */}
      <Card className="bg-surface border-border p-6">
        <h3 className="text-lg font-semibold text-foreground mb-6">Replication Progress</h3>
        <div className="space-y-6">
          {/* Full Load Progress with Animation */}
          {replicationProgress?.full_load || pipeline?.status === "active" || pipeline?.status === "running" || pipeline?.status === "starting" ? (
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium text-foreground">Full Load</span>
                  {(replicationProgress?.full_load?.status === "running" || pipeline?.status === "starting" || pipeline?.status === "running") && (
                    <Loader2 className="w-4 h-4 animate-spin text-primary" />
                  )}
                  {replicationProgress?.full_load?.status === "completed" && (
                    <CheckCircle className="w-4 h-4 text-success" />
                  )}
                  {replicationProgress?.full_load?.status === "failed" && (
                    <AlertCircle className="w-4 h-4 text-error" />
                  )}
                </div>
                <span className="text-2xl font-extrabold bg-gradient-to-r from-cyan-400 to-blue-500 bg-clip-text text-transparent">
                  {replicationProgress?.full_load ? (
                    replicationProgress.full_load.status === "completed" || Math.min(100, Math.max(0, replicationProgress.full_load.progress_percent || 0)) >= 100
                      ? '100%'
                      : `${Math.min(100, Math.max(0, replicationProgress.full_load.progress_percent || 0)).toFixed(1)}%`
                  ) : (
                    pipeline?.status === "starting" || pipeline?.status === "running" ? "0%" : "N/A"
                  )}
                </span>
              </div>

              {/* Enhanced Animated Transfer Visualization */}
              <div className="relative flex items-center justify-between py-6">
                {/* Source Database - Enhanced */}
                <div className="flex flex-col items-center gap-3 z-10">
                  <div className="relative group">
                    <div className="p-4 bg-gradient-to-br from-blue-500/20 to-blue-600/10 rounded-2xl border-2 border-blue-500/30 shadow-lg transition-all duration-300 group-hover:shadow-2xl group-hover:scale-105">
                      <Database className="w-14 h-14 text-blue-400" />
                    </div>
                    {(replicationProgress?.full_load?.status === "running" || pipeline?.status === "starting" || pipeline?.status === "running") && (
                      <>
                        <div className="absolute -top-1 -right-1 w-4 h-4 bg-blue-400 rounded-full animate-ping" />
                        <div className="absolute -top-1 -right-1 w-4 h-4 bg-blue-400 rounded-full shadow-lg shadow-blue-400/50" />
                      </>
                    )}
                  </div>
                  <div className="text-xs font-bold text-foreground text-center max-w-[120px] truncate px-3 py-1.5 bg-surface-hover rounded-lg border border-border">
                    {sourceConn}
                  </div>
                </div>

                {/* Transfer Animation Path - Enhanced */}
                <div className="flex-1 mx-6 relative">
                  {/* Progress Bar with Gradient */}
                  <div className="relative h-2.5 bg-gradient-to-r from-border via-border to-border rounded-full overflow-hidden shadow-inner">
                    <div
                      className={`h-full transition-all duration-500 ease-out rounded-full relative ${(replicationProgress?.full_load?.status === "completed" || ((replicationProgress?.full_load?.progress_percent ?? 0) >= 100 && replicationProgress?.full_load?.status !== "failed"))
                        ? 'bg-gradient-to-r from-green-400 via-emerald-500 to-green-400'
                        : 'bg-gradient-to-r from-cyan-400 via-blue-500 to-cyan-400'
                        }`}
                      style={{ width: `${Math.min(100, Math.max(0, replicationProgress?.full_load?.progress_percent || 0))}%` }}
                    >
                      {(replicationProgress?.full_load?.status === "running" || pipeline?.status === "starting" || pipeline?.status === "running") && !((replicationProgress?.full_load?.progress_percent ?? 0) >= 100) && (
                        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/40 to-transparent animate-[shimmer_1.5s_infinite]"
                          style={{ backgroundSize: '200% 100%' }}
                        />
                      )}
                      {(replicationProgress?.full_load?.status === "completed" || ((replicationProgress?.full_load?.progress_percent ?? 0) >= 100 && replicationProgress?.full_load?.status !== "failed")) && (
                        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/30 to-transparent animate-[shimmer_2s_infinite]"
                          style={{ backgroundSize: '200% 100%' }}
                        />
                      )}
                    </div>
                  </div>

                  {/* Animated File Icons Transferring */}
                  {(replicationProgress?.full_load?.status === "running" || pipeline?.status === "starting" || pipeline?.status === "running") && !((replicationProgress?.full_load?.progress_percent ?? 0) >= 100) && (
                    <div className="absolute top-1/2 left-0 right-0 -translate-y-1/2 pointer-events-none">
                      {[...Array(5)].map((_, i) => (
                        <div
                          key={i}
                          className="absolute"
                          style={{
                            animation: `fileTransfer 3s infinite ease-in-out`,
                            animationDelay: `${i * 0.6}s`,
                            left: '0%'
                          }}
                        >
                          <div className="relative">
                            <div className="w-7 h-7 bg-gradient-to-br from-cyan-400 to-blue-500 rounded-lg shadow-xl flex items-center justify-center">
                              <svg className="w-4 h-4 text-white" fill="currentColor" viewBox="0 0 20 20">
                                <path d="M4 4a2 2 0 012-2h4.586A2 2 0 0112 2.586L15.414 6A2 2 0 0116 7.414V16a2 2 0 01-2 2H6a2 2 0 01-2-2V4z" />
                              </svg>
                            </div>
                            <div className="absolute inset-0 bg-cyan-400 rounded-lg blur-md opacity-60 animate-pulse" />
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                  {/* Success animation when completed */}
                  {(replicationProgress?.full_load?.status === "completed" || ((replicationProgress?.full_load?.progress_percent ?? 0) >= 100 && replicationProgress?.full_load?.status !== "failed")) && (
                    <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 pointer-events-none">
                      <div className="relative">
                        <div className="w-12 h-12 bg-gradient-to-br from-green-400 to-emerald-500 rounded-full shadow-xl flex items-center justify-center animate-bounce">
                          <CheckCircle className="w-8 h-8 text-white" />
                        </div>
                        <div className="absolute inset-0 bg-green-400 rounded-full blur-xl opacity-60 animate-pulse" />
                      </div>
                    </div>
                  )}

                  {/* Speed Indicator */}
                  {(replicationProgress?.full_load?.status === "running" || pipeline?.status === "starting" || pipeline?.status === "running") && !((replicationProgress?.full_load?.progress_percent ?? 0) >= 100) && (
                    <div className="absolute -bottom-7 left-1/2 -translate-x-1/2 text-xs font-bold text-cyan-400 flex items-center gap-1.5 animate-pulse px-3 py-1 bg-cyan-500/10 rounded-full border border-cyan-500/30">
                      <Activity className="w-3.5 h-3.5" />
                      <span>Transferring Data...</span>
                    </div>
                  )}
                  {(replicationProgress?.full_load?.status === "completed" || ((replicationProgress?.full_load?.progress_percent ?? 0) >= 100 && replicationProgress?.full_load?.status !== "failed")) && (
                    <div className="absolute -bottom-7 left-1/2 -translate-x-1/2 text-xs font-bold text-green-400 flex items-center gap-1.5 animate-pulse px-3 py-1 bg-green-500/10 rounded-full border border-green-500/30">
                      <CheckCircle className="w-3.5 h-3.5" />
                      <span>Transfer Complete - 100%</span>
                    </div>
                  )}
                </div>

                {/* Target Database - Enhanced */}
                <div className="flex flex-col items-center gap-3 z-10">
                  <div className="relative group">
                    <div className={`p-4 rounded-2xl border-2 shadow-lg transition-all duration-500 group-hover:scale-105 ${(replicationProgress?.full_load?.status === "completed" || ((replicationProgress?.full_load?.progress_percent ?? 0) >= 100 && replicationProgress?.full_load?.status !== "failed"))
                      ? 'bg-gradient-to-br from-green-500/20 to-emerald-600/10 border-green-500/30 group-hover:shadow-2xl group-hover:shadow-green-500/20'
                      : (replicationProgress?.full_load?.status === "running" || pipeline?.status === "starting" || pipeline?.status === "running")
                        ? 'bg-gradient-to-br from-blue-500/20 to-cyan-600/10 border-blue-500/30 group-hover:shadow-2xl group-hover:shadow-blue-500/20'
                        : 'bg-gradient-to-br from-gray-500/20 to-gray-600/10 border-gray-500/30'
                      }`}>
                      <Database className={`w-14 h-14 transition-colors duration-500 ${(replicationProgress?.full_load?.status === "completed" || ((replicationProgress?.full_load?.progress_percent ?? 0) >= 100 && replicationProgress?.full_load?.status !== "failed"))
                        ? 'text-green-400'
                        : (replicationProgress?.full_load?.status === "running" || pipeline?.status === "starting" || pipeline?.status === "running")
                          ? 'text-blue-400 animate-pulse'
                          : 'text-gray-400'
                        }`} />
                    </div>
                    {(replicationProgress?.full_load?.status === "completed" || ((replicationProgress?.full_load?.progress_percent ?? 0) >= 100 && replicationProgress?.full_load?.status !== "failed")) && (
                      <>
                        <div className="absolute -top-2 -right-2 w-9 h-9 bg-gradient-to-br from-green-400 to-emerald-500 rounded-full flex items-center justify-center shadow-lg shadow-green-500/50 animate-bounce">
                          <CheckCircle className="w-6 h-6 text-white" />
                        </div>
                        <div className="absolute inset-0 bg-green-400 rounded-2xl blur-xl opacity-40 animate-pulse" />
                      </>
                    )}
                    {(replicationProgress?.full_load?.status === "running" || pipeline?.status === "starting" || pipeline?.status === "running") && !((replicationProgress?.full_load?.progress_percent ?? 0) >= 100) && (
                      <>
                        <div className="absolute -top-1 -right-1 w-4 h-4 bg-blue-400 rounded-full animate-ping" />
                        <div className="absolute -top-1 -right-1 w-4 h-4 bg-blue-400 rounded-full shadow-lg shadow-blue-400/50" />
                      </>
                    )}
                  </div>
                  <div className="text-xs font-bold text-foreground text-center max-w-[120px] truncate px-3 py-1.5 bg-surface-hover rounded-lg border border-border">
                    {targetConn}
                  </div>
                </div>
              </div>

              {/* Add CSS Keyframes for File Transfer Animation */}
              <style jsx>{`
                @keyframes fileTransfer {
                  0% {
                    left: 0%;
                    opacity: 0;
                    transform: translateY(0) scale(0.5) rotate(0deg);
                  }
                  10% {
                    opacity: 1;
                    transform: translateY(0) scale(1) rotate(0deg);
                  }
                  50% {
                    transform: translateY(-10px) scale(1.15) rotate(5deg);
                  }
                  90% {
                    opacity: 1;
                    transform: translateY(0) scale(1) rotate(0deg);
                  }
                  100% {
                    left: 100%;
                    opacity: 0;
                    transform: translateY(0) scale(0.5) rotate(0deg);
                  }
                }
                @keyframes shimmer {
                  0% {
                    background-position: -200% 0;
                  }
                  100% {
                    background-position: 200% 0;
                  }
                }
              `}</style>

              {/* Progress Details */}
              {replicationProgress?.full_load ? (
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-xs text-foreground-muted">
                    <span>
                      {replicationProgress.full_load.records_loaded?.toLocaleString() || 0} / {replicationProgress.full_load.total_records?.toLocaleString() || 0} records
                    </span>
                    {replicationProgress.full_load.current_table && (
                      <span className="text-primary">Processing: {replicationProgress.full_load.current_table}</span>
                    )}
                  </div>
                  {replicationProgress.full_load.status === "completed" && (
                    <div className="flex items-center gap-2 text-sm text-success bg-success/10 p-2 rounded">
                      <CheckCircle className="w-4 h-4" />
                      <span>Full load completed successfully - {Math.min(100, Math.max(0, replicationProgress.full_load.progress_percent || 100)).toFixed(1)}%</span>
                    </div>
                  )}
                  {/* Also show success message if progress is 100% even if status is not "completed" */}
                  {replicationProgress.full_load.progress_percent >= 100 && replicationProgress.full_load.status !== "completed" && (
                    <div className="flex items-center gap-2 text-sm text-success bg-success/10 p-2 rounded">
                      <CheckCircle className="w-4 h-4" />
                      <span>Full load completed successfully - 100%</span>
                    </div>
                  )}
                  {replicationProgress.full_load.status === "failed" && replicationProgress.full_load.error_message && (
                    <div className="flex items-start gap-2 text-sm text-error bg-error/10 p-2 rounded">
                      <AlertCircle className="w-4 h-4 mt-0.5" />
                      <span>{replicationProgress.full_load.error_message}</span>
                    </div>
                  )}
                </div>
              ) : (pipeline?.status === "starting" || pipeline?.status === "running") ? (
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-xs text-foreground-muted">
                    <span>Initializing pipeline...</span>
                    <Loader2 className="w-4 h-4 animate-spin text-primary" />
                  </div>
                </div>
              ) : null}
            </div>
          ) : (
            <div className="text-center py-8 text-foreground-muted">
              <Database className="w-12 h-12 mx-auto mb-2 opacity-50" />
              <p className="text-sm">Full load not started yet</p>
            </div>
          )}

          {/* CDC Progress - Enhanced - Always show when CDC is enabled */}
          {(pipeline.cdc_enabled || replicationProgress?.cdc) ? (
            <div className="space-y-4 border-t border-border/50 pt-6">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <span className="text-base font-bold text-foreground">CDC Monitoring</span>
                  {(replicationProgress?.cdc?.status === "watching" ||
                    replicationProgress?.cdc?.status === "active" ||
                    (pipeline.cdc_enabled && !replicationProgress?.cdc) ||
                    (pipeline.status === "active" || pipeline.status === "running")) && (
                      <div className="relative flex items-center justify-center">
                        {/* Animated Eye/Watching Indicator */}
                        <div className="relative">
                          <Eye className="w-6 h-6 text-cyan-400 animate-pulse" />
                          {/* Pulsing rings around the eye */}
                          <div className="absolute inset-0 -m-2">
                            <div className="w-10 h-10 rounded-full border-2 border-cyan-400/30 animate-ping" />
                          </div>
                          <div className="absolute inset-0 -m-2">
                            <div className="w-10 h-10 rounded-full border-2 border-cyan-400/20 animate-ping" style={{ animationDelay: '0.5s' }} />
                          </div>
                          {/* Scanning beam effect */}
                          <div className="absolute inset-0 -m-1">
                            <div className="w-8 h-8 rounded-full border border-cyan-400/40" style={{
                              clipPath: 'polygon(50% 50%, 50% 0%, 100% 0%, 100% 100%, 50% 100%)',
                              animation: 'radar-scan 3s linear infinite',
                              transformOrigin: '50% 50%'
                            }} />
                          </div>
                        </div>
                      </div>
                    )}
                  {replicationProgress?.cdc?.status === "stopped" && (
                    <Square className="w-5 h-5 text-foreground-muted" />
                  )}
                  {replicationProgress?.cdc?.status === "error" && (
                    <AlertCircle className="w-5 h-5 text-red-400" />
                  )}
                </div>
                <span className="text-sm font-bold text-green-400 capitalize px-3 py-1 bg-green-500/10 rounded-full border border-green-500/30 flex items-center gap-2">
                  {(replicationProgress?.cdc?.status === "active" ||
                    replicationProgress?.cdc?.status === "watching" ||
                    (pipeline.cdc_enabled && !replicationProgress?.cdc) ||
                    (pipeline.status === "active" || pipeline.status === "running")) && (
                      <div className="relative">
                        <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse" />
                        <div className="absolute inset-0 w-2 h-2 bg-green-400/50 rounded-full animate-ping" />
                      </div>
                    )}
                  {replicationProgress?.cdc?.status === "active" || replicationProgress?.cdc?.status === "watching" ||
                    (pipeline.cdc_enabled && !replicationProgress?.cdc) ||
                    (pipeline.status === "active" || pipeline.status === "running")
                    ? "Watching CDC Events"
                    : replicationProgress?.cdc?.status || "Ready"}
                </span>
              </div>

              {/* CDC Status Bar - Enhanced with continuous watching indicator */}
              {(replicationProgress?.cdc?.status === "active" ||
                replicationProgress?.cdc?.status === "watching" ||
                (pipeline.cdc_enabled && !replicationProgress?.cdc) ||
                (pipeline.status === "active" || pipeline.status === "running")) && (
                  <div className="relative h-4 bg-gradient-to-r from-cyan-500/10 via-cyan-400/20 to-cyan-500/10 rounded-full overflow-hidden border border-cyan-500/30">
                    {/* Animated shimmer effect */}
                    <div className="absolute inset-0 bg-gradient-to-r from-transparent via-cyan-400/40 to-transparent animate-[shimmer_2s_infinite]"
                      style={{ backgroundSize: '200% 100%' }} />
                    {/* Pulsing dots to show continuous activity */}
                    <div className="absolute inset-0 flex items-center justify-center gap-1">
                      {[...Array(3)].map((_, i) => (
                        <div
                          key={i}
                          className="w-1.5 h-1.5 bg-cyan-400 rounded-full animate-pulse"
                          style={{ animationDelay: `${i * 0.3}s` }}
                        />
                      ))}
                    </div>
                    <div className="absolute inset-0 flex items-center justify-center">
                      <span className="text-xs font-bold text-cyan-400 drop-shadow-lg flex items-center gap-2">
                        <div className="relative">
                          <div className="w-1.5 h-1.5 bg-cyan-400 rounded-full animate-pulse" />
                          <div className="absolute inset-0 w-1.5 h-1.5 bg-cyan-400/50 rounded-full animate-ping" />
                        </div>
                        Real-time CDC Active - Always Watching
                      </span>
                    </div>
                  </div>
                )}

              <div className="grid grid-cols-3 gap-4">
                <div className="bg-gradient-to-br from-blue-500/10 to-blue-600/5 border border-blue-500/20 p-4 rounded-lg shadow-md hover:shadow-lg transition-all relative group overflow-hidden">
                  {/* Animated background indicator when capturing */}
                  {(replicationProgress?.cdc?.status === "active" || replicationProgress?.cdc?.status === "watching") && (
                    <div className="absolute inset-0 bg-gradient-to-r from-transparent via-blue-400/10 to-transparent animate-[shimmer_2s_infinite]"
                      style={{ backgroundSize: '200% 100%' }} />
                  )}

                  <Button
                    variant="ghost"
                    size="sm"
                    className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity h-6 w-6 p-0 z-10"
                    onClick={async () => {
                      if (pipeline.id) {
                        try {
                          await apiClient.syncPipelineStats(pipeline.id)
                          // Refresh progress after sync
                          const progress = await apiClient.getPipelineProgress(pipeline.id)
                          setReplicationProgress(progress)
                        } catch (error) {
                          console.error("Failed to sync stats:", error)
                        }
                      }
                    }}
                    title="Sync stats from database - Counts all events (captured, applied, and failed)"
                  >
                    <RefreshCw className="w-3 h-3" />
                  </Button>

                  <div className="flex items-center gap-2 mb-2 relative z-10">
                    <span className="text-foreground-muted text-xs font-semibold uppercase tracking-wide" title="Total events captured from PostgreSQL (includes applied and failed)">
                      Captured
                    </span>
                    {(replicationProgress?.cdc?.status === "active" || replicationProgress?.cdc?.status === "watching") && (
                      <div className="relative flex items-center justify-center">
                        <div className="w-2 h-2 bg-cyan-400 rounded-full animate-pulse" />
                        <div className="absolute inset-0 w-2 h-2 bg-cyan-400/50 rounded-full animate-ping" />
                      </div>
                    )}
                  </div>

                  <div className="flex items-baseline gap-2 relative z-10">
                    <span className="text-foreground font-extrabold text-2xl bg-gradient-to-r from-blue-400 to-blue-600 bg-clip-text text-transparent">
                      {replicationProgress?.cdc?.events_captured || 0}
                    </span>
                    {(replicationProgress?.cdc?.status === "active" || replicationProgress?.cdc?.status === "watching") && (
                      <span className="text-xs text-cyan-400 font-medium animate-pulse">Live</span>
                    )}
                  </div>

                  <p className="text-xs text-foreground-muted mt-1 relative z-10">All events from source</p>
                  <Database className="absolute -right-4 -bottom-4 w-20 h-20 text-blue-500/10 group-hover:text-blue-500/15 transition-colors transform rotate-12" />
                </div>

                <div className="bg-gradient-to-br from-green-500/10 to-green-600/5 border border-green-500/20 p-4 rounded-lg shadow-md hover:shadow-lg transition-all relative group overflow-hidden">
                  <span className="text-foreground-muted block mb-2 text-xs font-semibold uppercase tracking-wide relative z-10" title="Events successfully applied to target database">
                    Applied
                  </span>
                  <span className="text-green-400 font-extrabold text-2xl relative z-10">
                    {replicationProgress?.cdc?.events_applied || 0}
                  </span>
                  <p className="text-xs text-foreground-muted mt-1 relative z-10">Successfully replicated</p>
                  <CheckCircle className="absolute -right-4 -bottom-4 w-20 h-20 text-green-500/10 group-hover:text-green-500/15 transition-colors transform rotate-12" />
                </div>

                <div className={`bg-gradient-to-br from-red-500/10 to-red-600/5 border border-red-500/20 p-4 rounded-lg shadow-md hover:shadow-lg transition-all relative group overflow-hidden ${(replicationProgress?.cdc?.events_failed || 0) > 0 ? 'border-red-500/40' : ''
                  }`}>
                  {/* Pulsing indicator when there are failures */}
                  {(replicationProgress?.cdc?.events_failed || 0) > 0 && (
                    <>
                      <div className="absolute inset-0 bg-gradient-to-r from-transparent via-red-500/10 to-transparent animate-[shimmer_2s_infinite]"
                        style={{ backgroundSize: '200% 100%' }} />
                      <div className="absolute top-2 right-2 z-20">
                        <div className="relative">
                          <AlertCircle className="w-4 h-4 text-red-400 animate-pulse" />
                          <div className="absolute inset-0 w-4 h-4 text-red-400/50 animate-ping" />
                        </div>
                      </div>
                    </>
                  )}

                  <div className="flex items-center gap-2 mb-2 relative z-10">
                    <span className="text-foreground-muted text-xs font-semibold uppercase tracking-wide" title="Events that failed to apply to target database (but were still captured)">
                      Failed
                    </span>
                    {(replicationProgress?.cdc?.events_failed || 0) > 0 && (
                      <div className="relative flex items-center justify-center">
                        <div className="w-2 h-2 bg-red-400 rounded-full animate-pulse" />
                        <div className="absolute inset-0 w-2 h-2 bg-red-400/50 rounded-full animate-ping" />
                      </div>
                    )}
                  </div>

                  <span className="text-red-400 font-extrabold text-2xl relative z-10">
                    {replicationProgress?.cdc?.events_failed || 0}
                  </span>
                  <p className="text-xs text-foreground-muted mt-1 relative z-10">
                    {(replicationProgress?.cdc?.events_failed || 0) > 0
                      ? 'Needs attention'
                      : 'All events applied'}
                  </p>
                  <AlertCircle className="absolute -right-4 -bottom-4 w-20 h-20 text-red-500/10 group-hover:text-red-500/15 transition-colors transform rotate-12" />
                </div>
              </div>
              {replicationProgress?.cdc?.last_event_time && (
                <div className="text-xs font-medium text-foreground-muted bg-surface-hover px-3 py-2 rounded-md">
                  Last event: {new Date(replicationProgress.cdc.last_event_time).toLocaleString()}
                </div>
              )}
              {replicationProgress?.cdc?.status === "error" && replicationProgress.cdc.error_message && (
                <div className="flex items-start gap-2 text-sm text-red-400 bg-red-500/10 p-3 rounded-lg border border-red-500/30">
                  <AlertCircle className="w-5 h-5 mt-0.5 flex-shrink-0" />
                  <span>{replicationProgress.cdc.error_message}</span>
                </div>
              )}

              {/* Recent Failed Events Section */}
              {(() => {
                if (!pipeline?.id) return null
                const pipelineIdStr = String(pipeline.id)
                const failedEvents = events
                  .filter(e => {
                    const eventPipelineId = String(e.pipeline_id || '')
                    return eventPipelineId === pipelineIdStr &&
                      (e.status === 'failed' || e.status === 'error') &&
                      e.error_message
                  })
                  .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())
                  .slice(0, 5) // Show top 5 recent failures

                if (failedEvents.length === 0) return null

                return (
                  <div className="mt-4 space-y-2 border-t border-red-500/20 pt-4">
                    <div className="flex items-center gap-2 mb-3">
                      <AlertCircle className="w-5 h-5 text-red-400" />
                      <h4 className="text-sm font-bold text-red-400">Recent Failures ({failedEvents.length})</h4>
                    </div>
                    <div className="space-y-2 max-h-64 overflow-y-auto">
                      {failedEvents.map((event) => (
                        <div
                          key={event.id}
                          className="bg-red-500/5 border border-red-500/20 rounded-lg p-3 hover:bg-red-500/10 transition-colors group"
                        >
                          <div className="flex items-start justify-between gap-2 mb-1">
                            <div className="flex items-center gap-2 flex-1 min-w-0">
                              <Badge className="bg-red-500/20 text-red-400 border-red-500/30 text-xs">
                                {event.event_type?.toUpperCase() || 'UNKNOWN'}
                              </Badge>
                              <span className="text-xs font-semibold text-foreground truncate">
                                {event.table_name || 'Unknown Table'}
                              </span>
                            </div>
                            <div className="flex items-center gap-2">
                              <span className="text-xs text-foreground-muted whitespace-nowrap">
                                {new Date(event.created_at).toLocaleTimeString()}
                              </span>
                              <Button
                                variant="ghost"
                                size="sm"
                                className="h-6 px-2 text-red-400 hover:text-red-300 hover:bg-red-500/10"
                                onClick={async () => {
                                  if (!event.id) return
                                  setRetryingEventId(event.id)
                                  try {
                                    await apiClient.retryFailedEvent(event.id)
                                    // Refresh progress and events
                                    if (pipeline.id) {
                                      const progress = await apiClient.getPipelineProgress(pipeline.id)
                                      setReplicationProgress(progress)
                                      dispatch(fetchReplicationEvents({
                                        pipelineId: String(pipeline.id),
                                        limit: 1000
                                      }))
                                    }
                                  } catch (error: any) {
                                    showError(error.message || 'Unknown error', "Failed to Retry Event")
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
                          </div>
                          <div className="text-xs text-red-300 mt-2 line-clamp-2 group-hover:line-clamp-none transition-all">
                            {event.error_message}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )
              })()}
            </div>
          ) : (
            <div className="text-center py-8 text-foreground-muted border-t border-border/50 pt-6">
              <Activity className="w-12 h-12 mx-auto mb-3 opacity-30" />
              <p className="text-sm font-medium">CDC monitoring not active</p>
            </div>
          )}
        </div>
      </Card>

      {/* Status Cards - Enhanced with gradients */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          {
            label: "Status",
            value: pipeline.status || "Unknown",
            gradient: (pipeline.status === "active" || pipeline.status === "running" || pipeline.status === "starting")
              ? "from-cyan-500/10 to-cyan-600/5"
              : (pipeline.status === "paused")
                ? "from-amber-500/10 to-amber-600/5"
                : (pipeline.status === "stopped" || pipeline.status === "failed" || pipeline.status === "error")
                  ? "from-red-500/10 to-red-600/5"
                  : "from-emerald-500/10 to-green-600/5",
            borderColor: (pipeline.status === "active" || pipeline.status === "running" || pipeline.status === "starting")
              ? "border-cyan-500/20"
              : (pipeline.status === "paused")
                ? "border-amber-500/20"
                : (pipeline.status === "stopped" || pipeline.status === "failed" || pipeline.status === "error")
                  ? "border-red-500/20"
                  : "border-emerald-500/20",
            textGradient: (pipeline.status === "active" || pipeline.status === "running" || pipeline.status === "starting")
              ? "from-cyan-400 to-cyan-600"
              : (pipeline.status === "paused")
                ? "from-amber-400 to-amber-600"
                : (pipeline.status === "stopped" || pipeline.status === "failed" || pipeline.status === "error")
                  ? "from-red-400 to-red-600"
                  : "from-emerald-400 to-green-500"
          },
          {
            label: "Tables",
            value: tableCount,
            gradient: "from-blue-500/10 to-blue-600/5",
            borderColor: "border-blue-500/20",
            textGradient: "from-blue-400 to-blue-600"
          },
          {
            label: "Records Synced",
            value: recordsSynced > 0 ? (recordsSynced >= 1000 ? (recordsSynced / 1000).toFixed(1) + "K" : recordsSynced.toString()) : "0",
            gradient: "from-purple-500/10 to-purple-600/5",
            borderColor: "border-purple-500/20",
            textGradient: "from-purple-400 to-purple-600"
          },
          {
            label: "Replication Lag",
            value: replicationLag !== null ? replicationLag + "s" : (pipeline.lagSeconds !== undefined && pipeline.lagSeconds !== null ? pipeline.lagSeconds + "s" : "N/A"),
            gradient: "from-cyan-500/10 to-cyan-600/5",
            borderColor: "border-cyan-500/20",
            textGradient: "from-cyan-400 to-cyan-600",
            // Add Consumer Lag details if available
            subValue: replicationProgress?.consumer_lag?.['cdc-event-logger'] !== undefined
              ? `Consumer Lag: ${replicationProgress.consumer_lag['cdc-event-logger']}`
              : (pipeline.consumerLag ? `Consumer Lag: ${pipeline.consumerLag}` : null)
          },
        ].map((stat, i) => {
          const Icon = i === 0 ? Activity : i === 1 ? Database : i === 2 ? Zap : Clock;
          return (
            <Card key={i} className={`bg-gradient-to-br ${stat.gradient} ${stat.borderColor} p-5 shadow-lg hover:shadow-xl transition-all duration-300 hover:scale-105 group relative overflow-hidden`}>
              <div className="relative z-10">
                <p className="text-sm font-semibold text-foreground-muted uppercase tracking-wide mb-2">{stat.label}</p>
                <p className={`text-3xl font-extrabold bg-gradient-to-r ${stat.textGradient} bg-clip-text text-transparent`}>{stat.value}</p>
                {/* Display subValue if it exists (e.g. Consumer Lag) */}
                {stat.subValue && (
                  <p className="text-xs font-medium text-foreground-muted mt-1 opacity-80">
                    {stat.subValue}
                  </p>
                )}
              </div>
              <Icon className={`absolute -right-4 -bottom-4 w-24 h-24 ${stat.borderColor.replace('border-', 'text-').replace('/20', '/10')} group-hover:${stat.borderColor.replace('border-', 'text-').replace('/20', '/15')} transition-colors transform rotate-12`} />
            </Card>
          );
        })}
      </div>

      {/* Charts - Enhanced visibility */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card className="bg-surface border-border p-6 shadow-xl">
          <h3 className="text-lg font-bold text-foreground mb-6 flex items-center gap-2">
            <Database className="w-6 h-6 text-blue-400" />
            Records Per Hour
          </h3>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={performanceData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.15)" />
              <XAxis
                dataKey="time"
                stroke="rgb(148, 163, 184)"
                tick={{ fill: 'rgb(148, 163, 184)', fontSize: 12, fontWeight: '600' }}
              />
              <YAxis
                stroke="rgb(148, 163, 184)"
                tick={{ fill: 'rgb(148, 163, 184)', fontSize: 12, fontWeight: '600' }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "rgba(15, 23, 42, 0.95)",
                  border: "1px solid rgb(59, 130, 246)",
                  borderRadius: "8px",
                  boxShadow: "0 4px 6px -1px rgba(0, 0, 0, 0.1)"
                }}
                labelStyle={{ color: 'rgb(59, 130, 246)', fontWeight: 'bold' }}
                itemStyle={{ color: 'rgb(148, 163, 184)' }}
              />
              <Bar
                dataKey="records"
                fill="url(#recordsGradient)"
                radius={[10, 10, 0, 0]}
              >
                <defs>
                  <linearGradient id="recordsGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="rgb(59, 130, 246)" stopOpacity={0.9} />
                    <stop offset="100%" stopColor="rgb(37, 99, 235)" stopOpacity={0.7} />
                  </linearGradient>
                </defs>
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Card>

        <Card className="bg-surface border-border p-6 shadow-xl">
          <h3 className="text-lg font-bold text-foreground mb-6 flex items-center gap-2">
            <Activity className="w-6 h-6 text-cyan-400" />
            Replication Latency (seconds)
          </h3>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={performanceData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(100, 116, 139, 0.15)" />
              <XAxis
                dataKey="time"
                stroke="rgb(148, 163, 184)"
                tick={{ fill: 'rgb(148, 163, 184)', fontSize: 12, fontWeight: '600' }}
              />
              <YAxis
                stroke="rgb(148, 163, 184)"
                tick={{ fill: 'rgb(148, 163, 184)', fontSize: 12, fontWeight: '600' }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "rgba(15, 23, 42, 0.95)",
                  border: "1px solid rgb(6, 182, 212)",
                  borderRadius: "8px",
                  boxShadow: "0 4px 6px -1px rgba(0, 0, 0, 0.1)"
                }}
                labelStyle={{ color: 'rgb(6, 182, 212)', fontWeight: 'bold' }}
                itemStyle={{ color: 'rgb(148, 163, 184)' }}
              />
              <Line
                type="monotone"
                dataKey="latency"
                stroke="rgb(6, 182, 212)"
                strokeWidth={3}
                dot={{ fill: "rgb(6, 182, 212)", r: 5 }}
                activeDot={{ r: 7, fill: "rgb(14, 165, 233)" }}
              />
            </LineChart>
          </ResponsiveContainer>
        </Card>
      </div>

      {/* Continuous Replication Status Section */}
      {pipeline.cdc_enabled && pipeline.id && (
        <Card className="bg-gradient-to-br from-indigo-500/10 to-purple-500/5 border-indigo-500/20 shadow-xl p-6">
          <div className="flex items-center justify-between mb-6">
            <h3 className="text-xl font-bold text-foreground flex items-center gap-2">
              <Activity className="w-6 h-6 text-indigo-400" />
              Continuous Replication Status
            </h3>
            <Badge
              variant="outline"
              className={`${pipeline.status === "active"
                ? "bg-success/20 text-success border-success/30"
                : "bg-warning/20 text-warning border-warning/30"
                }`}
            >
              {pipeline.status === "active" ? (
                <>
                  <div className="w-2 h-2 rounded-full bg-success mr-2 animate-pulse"></div>
                  Active
                </>
              ) : (
                pipeline.status
              )}
            </Badge>
          </div>

          {useMemo(() => {
            if (!events || !Array.isArray(events)) {
              return null
            }
            const pipelineIdStr = String(pipeline.id)
            const pipelineEvents = events.filter(e => String(e.pipeline_id) === pipelineIdStr)
            const totalEvents = pipelineEvents.length
            const successEvents = pipelineEvents.filter(e => e.status === 'applied' || e.status === 'success').length
            const failedEvents = pipelineEvents.filter(e => e.status === 'failed' || e.status === 'error').length
            const pendingEvents = totalEvents - successEvents - failedEvents
            const successRate = totalEvents > 0 ? Math.round((successEvents / totalEvents) * 100) : 0

            // Get latest LSN/Offset from events
            const latestEvent = pipelineEvents.length > 0
              ? pipelineEvents.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0]
              : null

            const sourceConn = connections.find(c => String(c.id) === String(pipeline.source_connection_id))
            const dbType = sourceConn?.connection_type?.toLowerCase() || ""

            // Calculate data transfer (estimate based on events)
            const insertEvents = pipelineEvents.filter(e => e.event_type === 'insert').length
            const updateEvents = pipelineEvents.filter(e => e.event_type === 'update').length
            const deleteEvents = pipelineEvents.filter(e => e.event_type === 'delete').length

            // Get LSN/Offset display
            const getLsnDisplay = () => {
              if (!latestEvent) return null

              if (dbType === "postgresql" && latestEvent.source_lsn) {
                return { type: "LSN", value: latestEvent.source_lsn, label: "PostgreSQL LSN" }
              } else if (dbType === "oracle" && latestEvent.source_scn) {
                return { type: "SCN", value: latestEvent.source_scn.toString(), label: "Oracle SCN" }
              } else if (dbType === "mysql" && latestEvent.source_binlog_file) {
                return {
                  type: "Binlog",
                  value: `${latestEvent.source_binlog_file}:${latestEvent.source_binlog_position || 0}`,
                  label: "MySQL Binlog"
                }
              } else if (dbType === "sqlserver" && latestEvent.sql_server_lsn) {
                return { type: "LSN", value: latestEvent.sql_server_lsn, label: "SQL Server LSN" }
              }
              return null
            }

            const lsnDisplay = getLsnDisplay()

            return (
              <div className="space-y-6">
                {/* Animated Data Transfer Progress Bar */}
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium text-foreground">Data Transfer Progress</span>
                    <span className="text-lg font-bold bg-gradient-to-r from-indigo-400 via-purple-500 to-pink-500 bg-clip-text text-transparent animate-pulse">
                      {successRate}% Success Rate
                    </span>
                  </div>

                  {/* Enhanced Animated Progress Bar with Data Flow */}
                  <div className="relative h-12 bg-gradient-to-r from-surface-hover via-surface-hover/80 to-surface-hover rounded-xl overflow-hidden border-2 border-border/50 shadow-inner">
                    {/* Animated background shimmer */}
                    <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent animate-[shimmer_3s_infinite] bg-[length:200%_100%]"></div>

                    {/* Success segment with animated data flow */}
                    {successEvents > 0 && (
                      <div
                        className="absolute left-0 top-0 h-full bg-gradient-to-r from-green-500 via-emerald-400 to-green-500 flex items-center justify-end pr-3 transition-all duration-700 ease-out relative overflow-hidden"
                        style={{ width: `${totalEvents > 0 ? (successEvents / totalEvents) * 100 : 0}%` }}
                      >
                        {/* Animated data particles flowing */}
                        <div className="absolute inset-0 overflow-hidden">
                          {[...Array(8)].map((_, i) => (
                            <div
                              key={i}
                              className="absolute w-3 h-3 bg-white/40 rounded-full shadow-lg"
                              style={{
                                left: `${i * 12.5}%`,
                                animation: `flow 2.5s infinite ease-in-out`,
                                animationDelay: `${i * 0.3}s`,
                                top: '50%',
                              }}
                            ></div>
                          ))}
                          {/* Additional smaller particles */}
                          {[...Array(12)].map((_, i) => (
                            <div
                              key={`small-${i}`}
                              className="absolute w-1.5 h-1.5 bg-white/20 rounded-full"
                              style={{
                                left: `${i * 8.33}%`,
                                animation: `flow 3s infinite ease-in-out`,
                                animationDelay: `${i * 0.25}s`,
                                top: `${30 + (i % 3) * 20}%`,
                              }}
                            ></div>
                          ))}
                        </div>

                        {/* Shimmer effect */}
                        <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent animate-[shimmer_2s_infinite] bg-[length:200%_100%]"></div>

                        {/* Success count badge */}
                        <div className="relative z-10 flex items-center gap-2">
                          <CheckCircle className="w-4 h-4 text-white animate-pulse" />
                          <span className="text-sm font-bold text-white drop-shadow-lg">{successEvents.toLocaleString()}</span>
                        </div>
                      </div>
                    )}

                    {/* Failed segment with warning animation */}
                    {failedEvents > 0 && (
                      <div
                        className="absolute h-full bg-gradient-to-r from-red-500 via-rose-400 to-red-500 flex items-center justify-end pr-3 transition-all duration-700 ease-out relative overflow-hidden"
                        style={{
                          left: `${totalEvents > 0 ? (successEvents / totalEvents) * 100 : 0}%`,
                          width: `${totalEvents > 0 ? (failedEvents / totalEvents) * 100 : 0}%`
                        }}
                      >
                        {/* Pulsing error indicator */}
                        <div className="absolute inset-0 bg-red-600/30 animate-pulse"></div>

                        {/* Failed count badge */}
                        <div className="relative z-10 flex items-center gap-2">
                          <AlertCircle className="w-4 h-4 text-white animate-pulse" />
                          <span className="text-sm font-bold text-white drop-shadow-lg">{failedEvents.toLocaleString()}</span>
                        </div>
                      </div>
                    )}

                    {/* Pending segment with loading animation */}
                    {pendingEvents > 0 && (
                      <div
                        className="absolute h-full bg-gradient-to-r from-amber-500/60 via-yellow-400/50 to-amber-500/60 flex items-center justify-end pr-3 transition-all duration-700 ease-out relative overflow-hidden"
                        style={{
                          left: `${totalEvents > 0 ? ((successEvents + failedEvents) / totalEvents) * 100 : 0}%`,
                          width: `${totalEvents > 0 ? (pendingEvents / totalEvents) * 100 : 0}%`
                        }}
                      >
                        {/* Animated loading dots */}
                        <div className="absolute inset-0 flex items-center justify-center gap-1">
                          {[...Array(3)].map((_, i) => (
                            <div
                              key={i}
                              className="w-1.5 h-1.5 bg-white/40 rounded-full animate-bounce"
                              style={{ animationDelay: `${i * 0.2}s` }}
                            ></div>
                          ))}
                        </div>

                        {/* Pending count badge */}
                        <div className="relative z-10 flex items-center gap-2">
                          <Loader2 className="w-4 h-4 text-white animate-spin" />
                          <span className="text-sm font-bold text-white drop-shadow-lg">{pendingEvents.toLocaleString()}</span>
                        </div>
                      </div>
                    )}

                    {/* Empty state with animated waiting indicator */}
                    {totalEvents === 0 && (
                      <div className="absolute inset-0 flex items-center justify-center">
                        <div className="flex items-center gap-2">
                          <div className="w-2 h-2 bg-indigo-400 rounded-full animate-pulse"></div>
                          <span className="text-xs text-foreground-muted">Waiting for replication events...</span>
                          <div className="w-2 h-2 bg-indigo-400 rounded-full animate-pulse" style={{ animationDelay: '0.3s' }}></div>
                          <div className="w-2 h-2 bg-indigo-400 rounded-full animate-pulse" style={{ animationDelay: '0.6s' }}></div>
                        </div>
                      </div>
                    )}

                    {/* Animated border glow when active */}
                    {pipeline.status === "active" && totalEvents > 0 && (
                      <div className="absolute inset-0 rounded-xl border-2 border-indigo-400/50 animate-pulse pointer-events-none"></div>
                    )}
                  </div>

                  {/* Data Transfer Speed Indicator */}
                  {pipeline.status === "active" && totalEvents > 0 && (
                    <div className="flex items-center gap-2 text-xs text-foreground-muted">
                      <div className="flex items-center gap-1">
                        <div className="w-1.5 h-1.5 bg-indigo-400 rounded-full animate-ping"></div>
                        <span>Live Data Transfer</span>
                      </div>
                      <span>•</span>
                      <span>{totalEvents.toLocaleString()} events processed</span>
                    </div>
                  )}

                  {/* Progress labels */}
                  <div className="flex items-center justify-between text-xs text-foreground-muted">
                    <div className="flex items-center gap-4">
                      <div className="flex items-center gap-1.5">
                        <div className="w-3 h-3 rounded bg-success"></div>
                        <span>Success: {successEvents.toLocaleString()}</span>
                      </div>
                      <div className="flex items-center gap-1.5">
                        <div className="w-3 h-3 rounded bg-error"></div>
                        <span>Failed: {failedEvents.toLocaleString()}</span>
                      </div>
                      {pendingEvents > 0 && (
                        <div className="flex items-center gap-1.5">
                          <div className="w-3 h-3 rounded bg-warning/50"></div>
                          <span>Pending: {pendingEvents.toLocaleString()}</span>
                        </div>
                      )}
                    </div>
                    <span className="font-semibold">Total: {totalEvents.toLocaleString()}</span>
                  </div>
                </div>

                {/* LSN/Offset Display */}
                {lsnDisplay && (
                  <div className="p-4 bg-surface/50 rounded-lg border border-indigo-500/20">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-foreground-muted">{lsnDisplay.label}</span>
                      <Badge variant="outline" className="bg-indigo-500/20 text-indigo-400 border-indigo-500/30">
                        {lsnDisplay.type}
                      </Badge>
                    </div>
                    <div className="flex items-center gap-2">
                      <Database className="w-4 h-4 text-indigo-400" />
                      <span className="font-mono text-sm text-foreground bg-surface px-2 py-1 rounded">
                        {lsnDisplay.value}
                      </span>
                      {latestEvent && (
                        <span className="text-xs text-foreground-muted ml-auto">
                          Last: {new Date(latestEvent.created_at).toLocaleTimeString()}
                        </span>
                      )}
                    </div>
                  </div>
                )}

                {/* Data Transfer Metrics */}
                {/* Data Transfer Metrics */}
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
                  {/* Captured */}
                  <Card className="relative overflow-hidden p-6 border-none bg-blue-500/5 shadow-[0_15px_30px_-5px_rgba(59,130,246,0.15)] dark:shadow-[0_15px_30px_-5px_rgba(0,0,0,0.3)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-2px] hover:bg-blue-500/10">
                    <div className="absolute -right-6 -bottom-6 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.12] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
                      <Database className="w-32 h-32 text-blue-500" />
                    </div>
                    <div className="relative z-10">
                      <div className="flex items-center gap-2 mb-4">
                        <div className="p-2 bg-blue-400/10 rounded-lg">
                          <Database className="w-4 h-4 text-blue-400" />
                        </div>
                        <span className="text-[10px] font-black text-blue-400/80 uppercase tracking-[0.2em]">Captured</span>
                      </div>
                      <p className="text-4xl font-black text-blue-500 tracking-tighter drop-shadow-sm">{totalEvents.toLocaleString()}</p>
                      <p className="text-[10px] text-foreground-muted font-black uppercase mt-2 opacity-60">Total Pipeline Events</p>
                    </div>
                  </Card>

                  {/* Applied */}
                  <Card className="relative overflow-hidden p-6 border-none bg-emerald-500/5 shadow-[0_15px_30px_-5px_rgba(16,185,129,0.15)] dark:shadow-[0_15px_30px_-5px_rgba(0,0,0,0.3)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-2px] hover:bg-emerald-500/10">
                    <div className="absolute -right-6 -bottom-6 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.12] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
                      <CheckCircle className="w-32 h-32 text-emerald-500" />
                    </div>
                    <div className="relative z-10">
                      <div className="flex items-center gap-2 mb-4">
                        <div className="p-2 bg-emerald-400/10 rounded-lg">
                          <CheckCircle className="w-4 h-4 text-emerald-400" />
                        </div>
                        <span className="text-[10px] font-black text-emerald-400/80 uppercase tracking-[0.2em]">Applied</span>
                      </div>
                      <p className="text-4xl font-black text-emerald-500 tracking-tighter drop-shadow-sm">{successEvents.toLocaleString()}</p>
                      <p className="text-[10px] text-foreground-muted font-black uppercase mt-2 opacity-60">Successfully Synced</p>
                    </div>
                  </Card>

                  {/* Errors */}
                  <Card className="relative overflow-hidden p-6 border-none bg-red-500/5 shadow-[0_15px_30px_-5px_rgba(239,68,68,0.15)] dark:shadow-[0_15px_30px_-5px_rgba(0,0,0,0.3)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-2px] hover:bg-red-500/10">
                    <div className="absolute -right-6 -bottom-6 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.12] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
                      <AlertCircle className="w-32 h-32 text-red-500" />
                    </div>
                    <div className="relative z-10">
                      <div className="flex items-center gap-2 mb-4">
                        <div className="p-2 bg-red-400/10 rounded-lg">
                          <AlertCircle className="w-4 h-4 text-red-400" />
                        </div>
                        <span className="text-[10px] font-black text-red-400/80 uppercase tracking-[0.2em]">Errors</span>
                      </div>
                      <p className="text-4xl font-black text-red-500 tracking-tighter drop-shadow-sm">{failedEvents.toLocaleString()}</p>
                      <p className="text-[10px] text-foreground-muted font-black uppercase mt-2 opacity-60">Failed Event Batches</p>
                    </div>
                  </Card>

                  {/* Operations */}
                  <Card className="relative overflow-hidden p-6 border-none bg-purple-500/5 shadow-[0_15px_30px_-5px_rgba(168,85,247,0.15)] dark:shadow-[0_15px_30px_-5px_rgba(0,0,0,0.3)] rounded-[2rem] group transition-all duration-500 hover:translate-y-[-2px] hover:bg-purple-500/10">
                    <div className="absolute -right-6 -bottom-6 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.12] transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
                      <Activity className="w-32 h-32 text-purple-500" />
                    </div>
                    <div className="relative z-10">
                      <div className="flex items-center gap-2 mb-4">
                        <div className="p-2 bg-purple-400/10 rounded-lg">
                          <Activity className="w-4 h-4 text-purple-400" />
                        </div>
                        <span className="text-[10px] font-black text-purple-400/80 uppercase tracking-[0.2em]">Ops Mix</span>
                      </div>
                      <div className="flex flex-col gap-1.5 pt-1">
                        <div className="flex justify-between items-baseline">
                          <span className="text-[10px] font-black text-foreground-muted opacity-80 uppercase">INS</span>
                          <span className="text-xl font-black text-purple-500 tracking-tighter">{insertEvents.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between items-baseline">
                          <span className="text-[10px] font-black text-foreground-muted opacity-80 uppercase">UPD</span>
                          <span className="text-xl font-black text-purple-500 tracking-tighter">{updateEvents.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between items-baseline">
                          <span className="text-[10px] font-black text-foreground-muted opacity-80 uppercase">DEL</span>
                          <span className="text-xl font-black text-purple-500 tracking-tighter">{deleteEvents.toLocaleString()}</span>
                        </div>
                      </div>
                    </div>
                  </Card>
                </div>

                {/* Real-time Status Indicator */}
                {pipeline.status === "active" && (
                  <div className="flex items-center justify-between p-3 bg-success/10 border border-success/20 rounded-lg">
                    <div className="flex items-center gap-2">
                      <div className="w-2 h-2 rounded-full bg-success animate-pulse"></div>
                      <span className="text-sm font-medium text-foreground">Replication Active</span>
                    </div>
                    {replicationProgress?.cdc && (
                      <div className="text-xs text-foreground-muted">
                        Last event: {replicationProgress.cdc.last_event_time
                          ? new Date(replicationProgress.cdc.last_event_time).toLocaleTimeString()
                          : "N/A"}
                      </div>
                    )}
                  </div>
                )}
              </div>
            )
          }, [events, pipeline?.id, pipeline?.source_connection_id, connections, pipeline?.status])}
        </Card>
      )}

      {/* CDC Checkpoints */}
      {pipeline.cdc_enabled && pipeline.id && (
        <CheckpointManager
          pipelineId={pipeline.id}
          sourceConnectionType={connections.find(c => String(c.id) === String(pipeline.source_connection_id))?.connection_type}
          tableMappings={pipeline.table_mappings || []}
        />
      )}

      {/* Tables */}
      <Card className="bg-surface border-border p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Replicated Tables ({tableCount})</h3>
        {tableNames.length > 0 ? (
          <div className="space-y-2">
            {pipeline.table_mappings ? (
              // Show source → target mapping
              pipeline.table_mappings.map((tm, idx) => (
                <div key={idx} className="flex items-center justify-between p-3 bg-surface-hover rounded-lg hover:bg-surface-hover/80 transition-colors">
                  <div className="flex-1">
                    <span className="text-foreground font-medium">{tm.source_table}</span>
                    {tm.source_table !== tm.target_table && (
                      <>
                        <span className="text-foreground-muted mx-2">→</span>
                        <span className="text-foreground-muted">{tm.target_table}</span>
                      </>
                    )}
                    {tm.source_schema && (
                      <span className="text-xs text-foreground-muted ml-2">({tm.source_schema})</span>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={(e) => {
                        e.preventDefault()
                        e.stopPropagation()
                        if (!isComparing && !loadingData) {
                          handleCompareTable(tm).catch((err) => {
                            console.error("[Compare] Unhandled error:", err)
                            // Error is already handled in handleCompareTable
                          })
                        }
                      }}
                      disabled={isComparing || loadingData}
                      className="bg-transparent border-border hover:bg-surface-hover hover:text-teal-500 active:text-teal-600 text-foreground gap-1 disabled:opacity-50 disabled:cursor-not-allowed"
                      title={isComparing || loadingData ? "Comparing..." : "Compare source and target data"}
                    >
                      {isComparing || loadingData ? (
                        <>
                          <Loader2 className="w-4 h-4 animate-spin" />
                          Comparing...
                        </>
                      ) : (
                        <>
                          <Eye className="w-4 h-4" />
                          Compare
                        </>
                      )}
                    </Button>
                    <CheckCircle className="w-5 h-5 text-success flex-shrink-0" />
                  </div>
                </div>
              ))
            ) : (
              // Fallback to simple table names
              tableNames.map((table, idx) => (
                <div key={idx} className="flex items-center justify-between p-3 bg-surface-hover rounded-lg">
                  <span className="text-foreground font-medium">{table}</span>
                  <CheckCircle className="w-5 h-5 text-success" />
                </div>
              ))
            )}
          </div>
        ) : (
          <div className="text-center py-8 text-foreground-muted">
            <p>No tables configured for this pipeline.</p>
          </div>
        )}
      </Card>

      {/* Data Comparison Section - Inline below tables */}
      {selectedTable && (
        <Card className="bg-surface border-border p-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-lg font-semibold text-foreground">Data Comparison</h3>
              <p className="text-sm text-foreground-muted">
                {selectedTable.source} → {selectedTable.target}
              </p>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={closeComparison}
              className="bg-transparent border-border hover:bg-surface-hover"
            >
              <X className="w-4 h-4 mr-2" />
              Close
            </Button>
          </div>

          {loadingData ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
              <span className="ml-2 text-foreground-muted">Loading table data...</span>
            </div>
          ) : (!sourceData && !targetData) ? (
            // Only show error if both tables failed
            <div className={`p-4 rounded-lg border ${dataError?.includes('⚠️') || dataError?.includes('Warning') || dataError?.includes('Showing available data')
              ? 'bg-warning/10 border-warning/30 text-warning'
              : 'bg-error/10 border-error/30 text-error'
              }`}>
              <div className="flex items-start justify-between gap-4">
                <p className="whitespace-pre-line text-sm flex-1">{dataError || 'Failed to fetch table data'}</p>
                {dataError?.includes('connections that don\'t exist') && (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={async () => {
                      try {
                        const result = await apiClient.fixOrphanedConnections()
                        // Success - could show success toast if needed, but for now just log
                        console.log(`Successfully fixed ${result.count} pipeline(s).`)
                        // Refresh the pipeline
                        if (pipeline?.id) {
                          const updatedPipeline = await apiClient.getPipeline(pipeline.id)
                          dispatch(setSelectedPipeline(updatedPipeline))
                          dispatch(fetchPipelines())
                        }
                        // Clear error and retry
                        setDataError(null)
                        if (selectedTable) {
                          handleCompareTable({
                            source_table: selectedTable.source,
                            target_table: selectedTable.target,
                            source_schema: selectedTable.sourceSchema,
                            target_schema: selectedTable.targetSchema
                          }).catch(console.error)
                        }
                      } catch (err: any) {
                        showError(err?.message || 'Unknown error', "Failed to Fix Connections")
                      }
                    }}
                    className="ml-4 shrink-0"
                  >
                    <RefreshCw className="w-4 h-4 mr-2" />
                    Fix Connections
                  </Button>
                )}
              </div>
            </div>
          ) : (
            <div className="space-y-4">
              {/* Show error banner if there's a partial failure */}
              {dataError && (sourceData || targetData) && (
                <div className={`p-4 rounded-lg border ${dataError.includes('⚠️') || dataError.includes('Warning') || dataError.includes('Showing available data')
                  ? 'bg-warning/10 border-warning/30 text-warning'
                  : 'bg-error/10 border-error/30 text-error'
                  }`}>
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1">
                      <p className="whitespace-pre-line text-sm">{dataError}</p>
                      {dataError.includes('Target') && !targetData && selectedTable && (
                        <p className="text-xs mt-2 opacity-75">
                          Table: {(() => {
                            const tableName = selectedTable.target
                            const schema = selectedTable.targetSchema
                            if (schema && tableName.startsWith(`${schema}.`)) {
                              return tableName
                            }
                            return schema ? `${schema}.${tableName}` : tableName
                          })()}
                        </p>
                      )}
                    </div>
                    {dataError.includes('Target') && !targetData && (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={async () => {
                          if (!selectedTable) return
                          setRetryingTarget(true)
                          setDataError(null)
                          try {
                            const tm = {
                              source_table: selectedTable.source,
                              target_table: selectedTable.target,
                              source_schema: selectedTable.sourceSchema,
                              target_schema: selectedTable.targetSchema
                            }
                            // Handle "undefined" schema string - convert to undefined/null
                            let targetSchema = tm.target_schema && tm.target_schema !== "undefined" && tm.target_schema.trim() !== ""
                              ? tm.target_schema.trim()
                              : undefined

                            // SQL Server uses "dbo" as default schema, not "public"
                            const targetConn = connections.find(c => String(c.id) === String(pipeline.target_connection_id))
                            const isTargetSQLServer = targetConn?.connection_type?.toLowerCase() === 'sqlserver' || targetConn?.connection_type?.toLowerCase() === 'sql_server'
                            if (isTargetSQLServer && targetSchema?.toLowerCase() === 'public') {
                              targetSchema = undefined
                            }

                            const targetConnectionId = pipeline.target_connection_uuid || pipeline.target_connection_id!
                            const targetResult = await apiClient.getTableData(
                              targetConnectionId,
                              tm.target_table,
                              targetSchema, // Use cleaned schema (undefined if "undefined" string or SQL Server "public")
                              1000 // Increased from 50 to fetch all records
                            )
                            setTargetData(targetResult)
                            setDataError(null)
                            console.log(`[Compare] Target table retry successful: ${targetResult?.records?.length || 0} records`)
                          } catch (retryError: any) {
                            console.error("[Compare] Retry failed:", retryError)
                            const retryErrMsg = retryError.message || "Retry failed. Please check the target database connection."
                            setDataError(`⚠️ Target table unavailable: ${retryErrMsg}\n\nShowing available data. Please check the target database connection.`)
                          } finally {
                            setRetryingTarget(false)
                          }
                        }}
                        disabled={retryingTarget || loadingData}
                        className="bg-transparent border-border hover:bg-surface-hover whitespace-nowrap"
                      >
                        {retryingTarget ? (
                          <>
                            <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                            Retrying...
                          </>
                        ) : (
                          <>
                            <Activity className="w-4 h-4 mr-2" />
                            Retry Target
                          </>
                        )}
                      </Button>
                    )}
                  </div>
                </div>
              )}

              {/* Table Comparison Header */}
              <div className="flex items-center justify-between mb-4 flex-shrink-0">
                <h4 className="text-sm font-bold text-foreground uppercase tracking-wide">Table Data Comparison</h4>
                <div className="flex items-center gap-4 text-xs text-foreground-muted">
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-3 rounded bg-surface-hover border border-border"></div>
                    <span>Source</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-3 rounded bg-primary/20 border border-primary/50"></div>
                    <span>Target</span>
                  </div>
                </div>
              </div>

              {/* Side by side tables in card format - Always show both side by side */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-6">
                {/* Source Table Card - Always show */}
                {sourceData ? (
                  <Card className="bg-surface border-2 border-border p-6 shadow-lg">
                    <div className="space-y-4">
                      <div className="flex items-center gap-2 pb-2 border-b border-border">
                        <Database className="w-4 h-4 text-foreground-muted" />
                        <div className="flex-1">
                          <p className="text-sm font-bold text-foreground">Source Table</p>
                          <p className="text-xs text-foreground-muted mt-1">
                            {(() => {
                              // Prevent duplicate schema in display (e.g., "public.public.table" -> "public.table")
                              const tableName = selectedTable.source
                              const schema = selectedTable.sourceSchema
                              if (schema && tableName.startsWith(`${schema}.`)) {
                                return tableName // Table already includes schema, don't duplicate
                              }
                              return schema ? `${schema}.${tableName}` : tableName
                            })()}
                          </p>
                        </div>
                        <div className="text-right">
                          <div className="text-xs text-foreground-muted">Records</div>
                          <div className="text-lg font-semibold text-foreground">
                            {sourceData?.count || sourceRecords.length || 0}
                          </div>
                        </div>
                      </div>
                      <div className="text-sm p-4 bg-surface-hover rounded-lg border-2 border-border shadow-inner">
                        <div className="overflow-x-auto overflow-y-visible">
                          <table className="w-full text-sm table-auto">
                            <thead className="bg-surface-hover">
                              <tr>
                                {(() => {
                                  // Use columns if available, otherwise use first record keys
                                  if (sourceData?.columns && sourceData.columns.length > 0) {
                                    return sourceData.columns.map((col: any, idx: number) => (
                                      <th key={`source-header-${idx}`} className="px-3 py-2 text-left text-foreground-muted font-medium border-b border-border whitespace-nowrap">
                                        {col.name}
                                      </th>
                                    ))
                                  } else if (sourceRecords.length > 0 && sourceRecords[0]) {
                                    // Fallback: use first record keys as headers
                                    return Object.keys(sourceRecords[0]).map((key: string, idx: number) => (
                                      <th key={`source-header-${idx}`} className="px-3 py-2 text-left text-foreground-muted font-medium border-b border-border whitespace-nowrap">
                                        {key}
                                      </th>
                                    ))
                                  } else {
                                    return (
                                      <th className="px-3 py-2 text-left text-foreground-muted font-medium border-b border-border whitespace-nowrap">
                                        No columns available
                                      </th>
                                    )
                                  }
                                })()}
                              </tr>
                            </thead>
                            <tbody>
                              {(() => {
                                // Debug logging (only in development)
                                if (process.env.NODE_ENV === 'development') {
                                  console.log('[Source Table Render]', {
                                    paginatedCount: paginatedSourceRecords.length,
                                    totalRecords: sourceRecords.length,
                                    hasColumns: !!sourceData?.columns,
                                    columnsCount: sourceData?.columns?.length || 0,
                                    firstRecord: paginatedSourceRecords[0],
                                    sourceData: sourceData
                                  })
                                }

                                if (paginatedSourceRecords.length > 0 && sourceData?.columns && sourceData.columns.length > 0) {
                                  return paginatedSourceRecords.map((record: any, rowIdx: number) => {
                                    if (!record || typeof record !== 'object') {
                                      console.warn('[Source] Invalid record at index', rowIdx, ':', record)
                                      return null
                                    }

                                    const recordKeys = Object.keys(record)

                                    return (
                                      <tr key={`source-row-${rowIdx}`} className="border-b border-border/50 hover:bg-surface transition-colors">
                                        {sourceData.columns.map((col: any, colIdx: number) => {
                                          // Try multiple ways to access the value
                                          let value = record[col.name]

                                          // If not found, try case variations
                                          if (value === undefined || value === null) {
                                            value = record[col.name.toLowerCase()] ?? record[col.name.toUpperCase()] ?? null
                                          }

                                          // If still null and record is array-like, try by index
                                          if (value === null && Array.isArray(record)) {
                                            value = record[colIdx] ?? null
                                          }

                                          return (
                                            <td key={`source-cell-${rowIdx}-${colIdx}`} className="px-3 py-2 text-foreground text-xs whitespace-nowrap">
                                              {value !== null && value !== undefined
                                                ? String(value).substring(0, 50)
                                                : <span className="text-foreground-muted">NULL</span>}
                                            </td>
                                          )
                                        })}
                                      </tr>
                                    )
                                  }).filter(Boolean)
                                } else if (paginatedSourceRecords.length > 0 && (!sourceData?.columns || sourceData.columns.length === 0)) {
                                  // If we have records but no columns, use record keys as columns
                                  const firstRecord = paginatedSourceRecords[0]
                                  const recordKeys = firstRecord ? Object.keys(firstRecord) : []

                                  return paginatedSourceRecords.map((record: any, rowIdx: number) => (
                                    <tr key={`source-row-${rowIdx}`} className="border-b border-border/50 hover:bg-surface transition-colors">
                                      {recordKeys.map((key: string, colIdx: number) => (
                                        <td key={`source-cell-${rowIdx}-${colIdx}`} className="px-3 py-2 text-foreground text-xs whitespace-nowrap">
                                          {record[key] !== null && record[key] !== undefined
                                            ? String(record[key]).substring(0, 50)
                                            : <span className="text-foreground-muted">NULL</span>}
                                        </td>
                                      ))}
                                    </tr>
                                  ))
                                } else {
                                  return (
                                    <tr>
                                      <td colSpan={sourceData?.columns?.length || 1} className="px-3 py-8 text-center text-foreground-muted">
                                        {sourceRecords.length > 0
                                          ? `No records on page ${sourcePage} of ${sourceTotalPages} (Total: ${sourceRecords.length} records)`
                                          : "No data available"}
                                      </td>
                                    </tr>
                                  )
                                }
                              })()}
                            </tbody>
                          </table>
                        </div>
                      </div>
                      {/* Source Table Pagination */}
                      {sourceTotalPages > 1 && (
                        <div className="flex items-center justify-between pt-3 border-t border-border">
                          <div className="text-xs text-foreground-muted">
                            Page {sourcePage} of {sourceTotalPages} ({sourceStartIndex + 1}-{Math.min(sourceEndIndex, sourceRecords.length)} of {sourceRecords.length})
                          </div>
                          <div className="flex items-center gap-1">
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setSourcePage(prev => Math.max(1, prev - 1))}
                              disabled={sourcePage === 1 || loadingData}
                              className="bg-transparent border-border hover:bg-surface-hover h-7 px-2"
                            >
                              <ChevronLeft className="w-3 h-3" />
                            </Button>
                            <div className="flex items-center gap-1">
                              {Array.from({ length: Math.min(sourceTotalPages, 5) }, (_, i) => {
                                let pageNum;
                                if (sourceTotalPages <= 5) {
                                  pageNum = i + 1;
                                } else if (sourcePage <= 3) {
                                  pageNum = i + 1;
                                } else if (sourcePage >= sourceTotalPages - 2) {
                                  pageNum = sourceTotalPages - 4 + i;
                                } else {
                                  pageNum = sourcePage - 2 + i;
                                }
                                return (
                                  <Button
                                    key={pageNum}
                                    variant={sourcePage === pageNum ? "default" : "outline"}
                                    size="sm"
                                    onClick={() => setSourcePage(pageNum)}
                                    disabled={loadingData}
                                    className={
                                      sourcePage === pageNum
                                        ? "bg-primary hover:bg-primary/90 text-foreground h-7 px-2"
                                        : "bg-transparent border-border hover:bg-surface-hover h-7 px-2"
                                    }
                                  >
                                    {pageNum}
                                  </Button>
                                );
                              })}
                            </div>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setSourcePage(prev => Math.min(sourceTotalPages, prev + 1))}
                              disabled={sourcePage === sourceTotalPages || loadingData}
                              className="bg-transparent border-border hover:bg-surface-hover h-7 px-2"
                            >
                              <ChevronRight className="w-3 h-3" />
                            </Button>
                          </div>
                        </div>
                      )}
                    </div>
                  </Card>
                ) : (
                  <Card className="bg-surface border-2 border-border p-6 shadow-lg">
                    <div className="space-y-4">
                      <div className="flex items-center gap-2 pb-2 border-b border-border">
                        <Database className="w-4 h-4 text-foreground-muted" />
                        <div className="flex-1">
                          <p className="text-sm font-bold text-foreground">Source Table</p>
                          <p className="text-xs text-foreground-muted mt-1">
                            {(() => {
                              // Prevent duplicate schema in display (e.g., "public.public.table" -> "public.table")
                              const tableName = selectedTable.source
                              const schema = selectedTable.sourceSchema
                              if (schema && tableName.startsWith(`${schema}.`)) {
                                return tableName // Table already includes schema, don't duplicate
                              }
                              return schema ? `${schema}.${tableName}` : tableName
                            })()}
                          </p>
                        </div>
                      </div>
                      <div className="text-sm p-6 bg-surface-hover rounded-lg border-2 border-dashed border-border text-center">
                        {loadingData ? (
                          <div className="text-foreground-muted">
                            <Loader2 className="w-5 h-5 animate-spin mx-auto mb-2" />
                            Loading source data...
                          </div>
                        ) : (
                          <div className="text-foreground-muted">
                            <p className="font-medium">Source data not available</p>
                            <p className="text-xs mt-1">Please check the source database connection</p>
                          </div>
                        )}
                      </div>
                    </div>
                  </Card>
                )}

                {/* Target Table Card - Always show */}
                {targetData ? (
                  <Card className="bg-surface border-2 border-primary/50 p-6 shadow-lg">
                    <div className="space-y-4">
                      <div className="flex items-center gap-2 pb-2 border-b border-primary/50">
                        <Database className="w-4 h-4 text-primary" />
                        <div className="flex-1">
                          <p className="text-sm font-bold text-foreground">Target Table</p>
                          <p className="text-xs text-foreground-muted mt-1">
                            {(() => {
                              // Prevent duplicate schema in display (e.g., "public.public.table" -> "public.table")
                              const tableName = selectedTable.target
                              const schema = selectedTable.targetSchema
                              if (schema && tableName.startsWith(`${schema}.`)) {
                                return tableName // Table already includes schema, don't duplicate
                              }
                              return schema ? `${schema}.${tableName}` : tableName
                            })()}
                          </p>
                        </div>
                        <div className="text-right">
                          <div className="text-xs text-foreground-muted">Records</div>
                          <div className="text-lg font-semibold text-foreground">
                            {targetData?.count || targetRecords.length || 0}
                          </div>
                        </div>
                      </div>
                      <div className="text-sm p-4 bg-primary/5 rounded-lg border-2 border-primary/40 shadow-inner">
                        <div className="overflow-x-auto overflow-y-visible">
                          <table className="w-full text-sm table-auto">
                            <thead className="bg-primary/10">
                              <tr>
                                {targetData?.columns?.map((col: any, idx: number) => (
                                  <th key={idx} className="px-3 py-2 text-left text-foreground-muted font-medium border-b border-primary/30 whitespace-nowrap">
                                    {col.name}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {paginatedTargetRecords.length > 0 ? (
                                paginatedTargetRecords.map((record: any, rowIdx: number) => {
                                  // Debug: log record structure
                                  if (process.env.NODE_ENV === 'development' && rowIdx === 0) {
                                    console.log('[Target Record] First record structure:', record)
                                    console.log('[Target Record] Record keys:', Object.keys(record))
                                    console.log('[Target Record] Columns:', targetData.columns)
                                  }

                                  // Get all record keys if columns don't match
                                  const recordKeys = Object.keys(record)
                                  const columnsToUse = targetData.columns.length > 0
                                    ? targetData.columns
                                    : recordKeys.map((key, idx) => ({ name: key, type: 'unknown', index: idx }))

                                  return (
                                    <tr key={rowIdx} className="border-b border-primary/20 hover:bg-primary/10 transition-colors">
                                      {columnsToUse.map((col: any, colIdx: number) => {
                                        // Try multiple ways to access the value
                                        let value = record[col.name] ?? record[col.name.toLowerCase()] ?? record[col.name.toUpperCase()] ?? null

                                        // If still null, try accessing by index if record is array-like
                                        if (value === null && Array.isArray(record)) {
                                          value = record[colIdx] ?? null
                                        }

                                        return (
                                          <td key={colIdx} className="px-3 py-2 text-foreground text-xs whitespace-nowrap">
                                            {value !== null && value !== undefined
                                              ? String(value).substring(0, 50)
                                              : <span className="text-foreground-muted">NULL</span>}
                                          </td>
                                        )
                                      })}
                                    </tr>
                                  )
                                })
                              ) : targetRecords.length > 0 ? (
                                // If we have records but pagination is wrong, show all records
                                targetRecords.map((record: any, rowIdx: number) => {
                                  // Debug: log record structure
                                  if (process.env.NODE_ENV === 'development' && rowIdx === 0) {
                                    console.log('[Target Record] First record (fallback) structure:', record)
                                    console.log('[Target Record] Record keys:', Object.keys(record))
                                    console.log('[Target Record] Columns:', targetData.columns)
                                  }

                                  // Get all record keys if columns don't match
                                  const recordKeys = Object.keys(record)
                                  const columnsToUse = targetData.columns.length > 0
                                    ? targetData.columns
                                    : recordKeys.map((key, idx) => ({ name: key, type: 'unknown', index: idx }))

                                  return (
                                    <tr key={rowIdx} className="border-b border-primary/20 hover:bg-primary/10 transition-colors">
                                      {columnsToUse.map((col: any, colIdx: number) => {
                                        // Try multiple ways to access the value
                                        let value = record[col.name] ?? record[col.name.toLowerCase()] ?? record[col.name.toUpperCase()] ?? null

                                        // If still null, try accessing by index if record is array-like
                                        if (value === null && Array.isArray(record)) {
                                          value = record[colIdx] ?? null
                                        }

                                        return (
                                          <td key={colIdx} className="px-3 py-2 text-foreground text-xs whitespace-nowrap">
                                            {value !== null && value !== undefined
                                              ? String(value).substring(0, 50)
                                              : <span className="text-foreground-muted">NULL</span>}
                                          </td>
                                        )
                                      })}
                                    </tr>
                                  )
                                })
                              ) : (
                                <tr>
                                  <td colSpan={targetData?.columns?.length || 1} className="px-3 py-8 text-center text-foreground-muted">
                                    {targetData?.count === 0 || (targetRecords.length === 0 && (!targetData?.count || targetData.count === 0))
                                      ? "No data available in target table"
                                      : targetRecords.length === 0 && targetData?.count > 0
                                        ? `Table exists with ${targetData.count} records, but none loaded. Please check the table structure.`
                                        : `Showing ${targetRecords.length} records (page ${targetPage} of ${targetTotalPages})`}
                                  </td>
                                </tr>
                              )}
                            </tbody>
                          </table>
                        </div>
                      </div>
                      {/* Target Table Pagination */}
                      {targetTotalPages > 1 && (
                        <div className="flex items-center justify-between pt-3 border-t border-primary/30">
                          <div className="text-xs text-foreground-muted">
                            Page {targetPage} of {targetTotalPages} ({targetStartIndex + 1}-{Math.min(targetEndIndex, targetRecords.length)} of {targetRecords.length})
                          </div>
                          <div className="flex items-center gap-1">
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setTargetPage(prev => Math.max(1, prev - 1))}
                              disabled={targetPage === 1 || loadingData}
                              className="bg-transparent border-border hover:bg-surface-hover h-7 px-2"
                            >
                              <ChevronLeft className="w-3 h-3" />
                            </Button>
                            <div className="flex items-center gap-1">
                              {Array.from({ length: Math.min(targetTotalPages, 5) }, (_, i) => {
                                let pageNum;
                                if (targetTotalPages <= 5) {
                                  pageNum = i + 1;
                                } else if (targetPage <= 3) {
                                  pageNum = i + 1;
                                } else if (targetPage >= targetTotalPages - 2) {
                                  pageNum = targetTotalPages - 4 + i;
                                } else {
                                  pageNum = targetPage - 2 + i;
                                }
                                return (
                                  <Button
                                    key={pageNum}
                                    variant={targetPage === pageNum ? "default" : "outline"}
                                    size="sm"
                                    onClick={() => setTargetPage(pageNum)}
                                    disabled={loadingData}
                                    className={
                                      targetPage === pageNum
                                        ? "bg-primary hover:bg-primary/90 text-foreground h-7 px-2"
                                        : "bg-transparent border-border hover:bg-surface-hover h-7 px-2"
                                    }
                                  >
                                    {pageNum}
                                  </Button>
                                );
                              })}
                            </div>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => setTargetPage(prev => Math.min(targetTotalPages, prev + 1))}
                              disabled={targetPage === targetTotalPages || loadingData}
                              className="bg-transparent border-border hover:bg-surface-hover h-7 px-2"
                            >
                              <ChevronRight className="w-3 h-3" />
                            </Button>
                          </div>
                        </div>
                      )}
                    </div>
                  </Card>
                ) : (
                  <Card className="bg-surface border-2 border-primary/50 p-6 shadow-lg">
                    <div className="space-y-4">
                      <div className="flex items-center gap-2 pb-2 border-b border-primary/50">
                        <Database className="w-4 h-4 text-primary" />
                        <div className="flex-1">
                          <p className="text-sm font-bold text-foreground">Target Table</p>
                          <p className="text-xs text-foreground-muted mt-1">
                            {(() => {
                              // Prevent duplicate schema in display (e.g., "public.public.table" -> "public.table")
                              const tableName = selectedTable.target
                              const schema = selectedTable.targetSchema
                              if (schema && tableName.startsWith(`${schema}.`)) {
                                return tableName // Table already includes schema, don't duplicate
                              }
                              return schema ? `${schema}.${tableName}` : tableName
                            })()}
                          </p>
                        </div>
                      </div>
                      <div className="text-sm p-6 bg-primary/5 rounded-lg border-2 border-dashed border-primary/40 text-center">
                        {loadingData ? (
                          <div className="text-foreground-muted">
                            <Loader2 className="w-5 h-5 animate-spin mx-auto mb-2" />
                            Loading target data...
                          </div>
                        ) : (
                          <div className="text-foreground-muted">
                            <p className="font-medium">Target data not available</p>
                            <p className="text-xs mt-1">Please check the target database connection</p>
                          </div>
                        )}
                        {!loadingData && !targetData && (
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={async () => {
                              if (!selectedTable) return
                              setRetryingTarget(true)
                              setDataError(null)
                              try {
                                const tm = {
                                  source_table: selectedTable.source,
                                  target_table: selectedTable.target,
                                  source_schema: selectedTable.sourceSchema,
                                  target_schema: selectedTable.targetSchema
                                }
                                // Handle "undefined" schema string - convert to undefined/null
                                let targetSchema = tm.target_schema && tm.target_schema !== "undefined" && tm.target_schema.trim() !== ""
                                  ? tm.target_schema.trim()
                                  : undefined

                                // SQL Server uses "dbo" as default schema, not "public"
                                const targetConn = connections.find(c => String(c.id) === String(pipeline.target_connection_id))
                                const isTargetSQLServer = targetConn?.connection_type?.toLowerCase() === 'sqlserver' || targetConn?.connection_type?.toLowerCase() === 'sql_server'
                                if (isTargetSQLServer && targetSchema?.toLowerCase() === 'public') {
                                  targetSchema = undefined
                                }

                                const targetConnectionId = pipeline.target_connection_uuid || pipeline.target_connection_id!
                                const targetResult = await apiClient.getTableData(
                                  targetConnectionId,
                                  tm.target_table,
                                  targetSchema, // Use cleaned schema (undefined if "undefined" string or SQL Server "public")
                                  1000 // Increased from 50 to fetch all records
                                )
                                setTargetData(targetResult)
                                setDataError(null)
                              } catch (retryError: any) {
                                console.error("[Compare] Retry failed:", retryError)
                                const retryErrMsg = retryError.message || "Retry failed. Please check the target database connection."
                                setDataError(`⚠️ Target table unavailable: ${retryErrMsg}`)
                              } finally {
                                setRetryingTarget(false)
                              }
                            }}
                            disabled={retryingTarget}
                            className="mt-4 bg-primary/10 border-primary/30 text-primary hover:bg-primary/20 hover:border-primary/50"
                          >
                            {retryingTarget ? (
                              <>
                                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                                Retrying...
                              </>
                            ) : (
                              <>
                                <Activity className="w-4 h-4 mr-2" />
                                Retry Target
                              </>
                            )}
                          </Button>
                        )}
                      </div>
                    </div>
                  </Card>
                )}
              </div>
            </div>
          )}
        </Card>
      )}
      <ConfirmDialogComponent />
      <ErrorToastComponent />
    </div>
  )
}
