"use client"

import { useState, useEffect, useRef, useMemo } from "react"
import { ViewToggle } from "@/components/ui/view-toggle"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Plus, Settings, Play, Pause, Square, Trash2, AlertCircle, Clock, Loader2, Edit2, Database, GitBranch, Activity, Workflow, ChevronLeft, ChevronRight, MoreHorizontal, ArrowRight } from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"
import { Badge } from "@/components/ui/badge"
import { PipelineModal } from "@/components/pipelines/pipeline-modal"
import { PipelineWizard } from "@/components/pipelines/pipeline-wizard"
import { PipelineDetail } from "@/components/pipelines/pipeline-detail"
import { useAppDispatch, useAppSelector } from "@/lib/store/hooks"
import { useErrorToast } from "@/components/ui/error-toast"
import {
  fetchPipelines,
  createPipeline,
  updatePipeline,
  deletePipeline,
  triggerPipeline,
  pausePipeline,
  stopPipeline,
  setSelectedPipeline,
} from "@/lib/store/slices/pipelineSlice"
import { fetchReplicationEvents } from "@/lib/store/slices/monitoringSlice"
import { formatDistanceToNow } from "date-fns"
import { ProtectedPage } from "@/components/auth/ProtectedPage"
import { useConfirmDialog } from "@/components/ui/confirm-dialog"
import { cn } from "@/lib/utils"

export default function PipelinesPage() {
  const dispatch = useAppDispatch()
  const { pipelines, isLoading, error } = useAppSelector((state) => state.pipelines)
  const { connections } = useAppSelector((state) => state.connections)
  const { events } = useAppSelector((state) => state.monitoring)
  const { showError, ErrorToastComponent } = useErrorToast()
  const { showConfirm, ConfirmDialogComponent } = useConfirmDialog()
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [editingPipeline, setEditingPipeline] = useState<any>(null)
  const [selectedPipelineId, setSelectedPipelineId] = useState<number | null>(null)
  const [view, setView] = useState<"grid" | "list">("grid")
  const [currentPage, setCurrentPage] = useState(1)
  const pipelinesPerPage = 8

  // Fetch pipelines on mount and when navigating back to this page
  useEffect(() => {
    // Always fetch pipelines to get latest status (including on refresh)
    dispatch(fetchPipelines())

    // Fetch replication events to calculate real progress
    dispatch(fetchReplicationEvents({ limit: 1000, todayOnly: false }))

    // Also fetch connections for pipeline wizard (only if not already loaded)
    if (connections.length === 0) {
      import("@/lib/store/slices/connectionSlice").then(({ fetchConnections }) => {
        dispatch(fetchConnections())
      })
    }
  }, [dispatch, connections.length])

  // Auto-refresh pipelines every 10 seconds to keep status updated
  useEffect(() => {
    const interval = setInterval(() => {
      dispatch(fetchPipelines())
    }, 10000) // Refresh every 10 seconds

    return () => clearInterval(interval)
  }, [dispatch])

  // Auto-refresh events every 5 seconds to update progress bars in real-time
  useEffect(() => {
    const interval = setInterval(() => {
      dispatch(fetchReplicationEvents({ limit: 10000, todayOnly: false }))
    }, 5000) // Refresh every 5 seconds

    return () => clearInterval(interval)
  }, [dispatch])

  // Helper function to calculate replication stats for a pipeline
  const getPipelineReplicationStats = (pipelineId: string | number) => {
    const pipelineIdStr = String(pipelineId)
    const pipelineEvents = events.filter(e => String(e.pipeline_id) === pipelineIdStr)
    const totalEvents = pipelineEvents.length
    const successEvents = pipelineEvents.filter(e => e.status === 'applied' || e.status === 'success').length
    const failedEvents = pipelineEvents.filter(e => e.status === 'failed' || e.status === 'error').length
    const successRate = totalEvents > 0 ? Math.round((successEvents / totalEvents) * 100) : 0

    return {
      totalEvents,
      successEvents,
      failedEvents,
      successRate
    }
  }

  // Helper function to parse table name (handles schema.table format)
  const parseTableName = (tableName: string): { schema?: string; table: string } => {
    if (!tableName) return { table: tableName }

    // Check if table name contains a dot (schema.table format)
    const parts = tableName.split('.')
    if (parts.length === 2) {
      const schema = parts[0].trim()
      const table = parts[1].trim()
      // Don't treat "undefined" as a valid schema name
      if (schema && schema !== "undefined") {
        return {
          schema: schema,
          table: table
        }
      }
      // If schema is "undefined", treat as no schema
      return { table: table }
    }
    return { table: tableName.trim() }
  }

  const handleAddPipeline = async (pipelineData: any) => {
    try {
      // Find connection IDs by name
      const sourceConn = connections.find(c => c.name === pipelineData.sourceConnection)
      const targetConn = connections.find(c => c.name === pipelineData.targetConnection)

      if (!sourceConn || !targetConn) {
        alert("Please select valid source and target connections")
        return
      }

      // Parse table mappings - handle both tableMapping array and tables array
      const tableMappings = []

      if (pipelineData.tableMapping && Array.isArray(pipelineData.tableMapping)) {
        // Use tableMapping if available
        for (const tm of pipelineData.tableMapping) {
          const sourceName = tm.source || tm.sourceTable || ""
          const targetName = tm.target || tm.targetTable || sourceName

          if (!sourceName || !targetName) {
            console.warn("Skipping invalid table mapping:", tm)
            continue
          }

          const sourceParsed = parseTableName(sourceName)
          const targetParsed = parseTableName(targetName)

          if (!sourceParsed.table || !targetParsed.table) {
            console.warn("Skipping table mapping with empty table name:", { sourceParsed, targetParsed })
            continue
          }

          const mapping: any = {
            source_table: sourceParsed.table,
            target_table: targetParsed.table,
          }

          // Only include schema if it exists
          if (sourceParsed.schema) {
            mapping.source_schema = sourceParsed.schema
          }
          if (targetParsed.schema) {
            mapping.target_schema = targetParsed.schema
          }

          tableMappings.push(mapping)
        }
      } else if (pipelineData.tables && Array.isArray(pipelineData.tables)) {
        // Fallback to tables array if tableMapping is not available
        for (const tableName of pipelineData.tables) {
          if (!tableName) {
            console.warn("Skipping empty table name")
            continue
          }

          const sourceParsed = parseTableName(tableName)
          // Use tableMapping object if available, otherwise use same name
          const targetName = pipelineData.tableMapping?.[tableName] || tableName
          const targetParsed = parseTableName(targetName)

          if (!sourceParsed.table || !targetParsed.table) {
            console.warn("Skipping table mapping with empty table name:", { sourceParsed, targetParsed })
            continue
          }

          const mapping: any = {
            source_table: sourceParsed.table,
            target_table: targetParsed.table,
          }

          // Only include schema if it exists
          if (sourceParsed.schema) {
            mapping.source_schema = sourceParsed.schema
          }
          if (targetParsed.schema) {
            mapping.target_schema = targetParsed.schema
          }

          tableMappings.push(mapping)
        }
      }

      if (tableMappings.length === 0) {
        alert("Please select at least one table for replication")
        return
      }

      // Validate all table mappings have required fields
      const invalidMappings = tableMappings.filter(tm => !tm.source_table || !tm.target_table)
      if (invalidMappings.length > 0) {
        console.error("[Pipeline] Invalid table mappings found:", invalidMappings)
        showError("Invalid table mappings detected. Please check:\n- All mappings must have source_table and target_table\n- Table names cannot be empty", "Validation Error")
        return
      }

      // Build payload with validated data
      const pipelinePayload = {
        name: (pipelineData.name || "").trim(),
        description: (pipelineData.description || "").trim(),
        source_connection_id: String(sourceConn.id), // Ensure it's a string
        target_connection_id: String(targetConn.id), // Ensure it's a string
        full_load_type: pipelineData.mode === "full_load" ? "overwrite" :
          pipelineData.mode === "cdc_only" ? "append" : "overwrite",
        cdc_enabled: pipelineData.mode !== "full_load",
        cdc_filters: pipelineData.cdc_filters || [],
        table_mappings: tableMappings.map(tm => {
          const mapping: { source_schema?: string; source_table: string; target_schema?: string; target_table: string } = {
            source_table: tm.source_table.trim(),
            target_table: tm.target_table.trim(),
          }
          // Only include schema if it exists, is not empty, and is not "undefined"
          if (tm.source_schema && tm.source_schema.trim() !== "" && tm.source_schema.trim() !== "undefined") {
            mapping.source_schema = tm.source_schema.trim()
          }
          if (tm.target_schema && tm.target_schema.trim() !== "" && tm.target_schema.trim() !== "undefined") {
            mapping.target_schema = tm.target_schema.trim()
          }
          return mapping
        }),
      }

      // Validate payload before sending
      if (!pipelinePayload.name || pipelinePayload.name.trim() === "") {
        showError("Pipeline name is required", "Validation Error")
        return
      }

      console.log("[Pipeline] Creating pipeline with validated payload:", {
        name: pipelinePayload.name,
        source_connection_id: pipelinePayload.source_connection_id,
        target_connection_id: pipelinePayload.target_connection_id,
        table_mappings_count: pipelinePayload.table_mappings.length,
        table_mappings: pipelinePayload.table_mappings,
      })

      await dispatch(createPipeline(pipelinePayload)).unwrap()
      setIsModalOpen(false)
    } catch (err: any) {
      // Define pipelinePayload in catch scope for error logging
      let pipelinePayload: any = null
      try {
        // Try to reconstruct payload for logging
        const sourceConn = connections.find(c => c.name === pipelineData?.sourceConnection)
        const targetConn = connections.find(c => c.name === pipelineData?.targetConnection)
        if (sourceConn && targetConn) {
          pipelinePayload = {
            name: pipelineData?.name,
            source_connection_id: String(sourceConn.id),
            target_connection_id: String(targetConn.id),
          }
        }
      } catch { }

      console.error("Failed to create pipeline:", err)
      console.error("Error type:", typeof err)
      console.error("Error constructor:", err?.constructor?.name)

      // Handle string errors (from Redux rejectWithValue)
      const errorString = typeof err === 'string' ? err : err?.message || err?.payload || String(err)
      console.error("Error string:", errorString)

      console.error("Error details:", {
        message: typeof err === 'string' ? err : err?.message,
        name: err?.name,
        code: err?.code,
        status: err?.status || err?.response?.status,
        response: err?.response?.data,
        payload: pipelinePayload
      })

      // Extract detailed error message from various possible locations
      let errorMessage = "Failed to create pipeline"

      // Check Redux error format
      if (err?.payload) {
        errorMessage = err.payload
      }
      // Check axios response
      else if (err?.response?.data?.detail) {
        errorMessage = err.response.data.detail
      }
      // Check if error has message
      else if (err?.message) {
        errorMessage = err.message
      }
      // Check if error is a string
      else if (typeof err === 'string') {
        errorMessage = err
      }
      // Check for validation errors
      else if (err?.response?.data?.errors) {
        const validationErrors = err.response.data.errors.map((e: any) =>
          `${e.loc?.join('.') || 'field'}: ${e.msg || e.message}`
        ).join('\n')
        errorMessage = `Validation errors:\n${validationErrors}`
      }

      // Show user-friendly error with troubleshooting steps
      showError(errorMessage, "Failed to Create Pipeline")
    }
  }

  const handleEditPipeline = async (pipelineData: any) => {
    if (!editingPipeline) return

    try {
      // Find connection IDs by name
      const sourceConn = connections.find(c => c.name === pipelineData.sourceConnection)
      const targetConn = connections.find(c => c.name === pipelineData.targetConnection)

      if (!sourceConn || !targetConn) {
        alert("Please select valid source and target connections")
        return
      }

      // Parse table mappings - handle both tableMapping array and tables array
      const tableMappings = []

      if (pipelineData.tableMapping && Array.isArray(pipelineData.tableMapping)) {
        // Use tableMapping if available
        for (const tm of pipelineData.tableMapping) {
          const sourceName = tm.source || tm.sourceTable || ""
          const targetName = tm.target || tm.targetTable || sourceName

          if (!sourceName || !targetName) {
            console.warn("Skipping invalid table mapping:", tm)
            continue
          }

          const sourceParsed = parseTableName(sourceName)
          const targetParsed = parseTableName(targetName)

          if (!sourceParsed.table || !targetParsed.table) {
            console.warn("Skipping table mapping with empty table name:", { sourceParsed, targetParsed })
            continue
          }

          const mapping: any = {
            source_table: sourceParsed.table,
            target_table: targetParsed.table,
          }

          // Only include schema if it exists
          if (sourceParsed.schema) {
            mapping.source_schema = sourceParsed.schema
          }
          if (targetParsed.schema) {
            mapping.target_schema = targetParsed.schema
          }

          tableMappings.push(mapping)
        }
      } else if (pipelineData.tables && Array.isArray(pipelineData.tables)) {
        // Fallback to tables array if tableMapping is not available
        for (const tableName of pipelineData.tables) {
          if (!tableName) {
            console.warn("Skipping empty table name")
            continue
          }

          const sourceParsed = parseTableName(tableName)
          // Use tableMapping object if available, otherwise use same name
          const targetName = pipelineData.tableMapping?.[tableName] || tableName
          const targetParsed = parseTableName(targetName)

          if (!sourceParsed.table || !targetParsed.table) {
            console.warn("Skipping table mapping with empty table name:", { sourceParsed, targetParsed })
            continue
          }

          const mapping: any = {
            source_table: sourceParsed.table,
            target_table: targetParsed.table,
          }

          // Only include schema if it exists
          if (sourceParsed.schema) {
            mapping.source_schema = sourceParsed.schema
          }
          if (targetParsed.schema) {
            mapping.target_schema = targetParsed.schema
          }

          tableMappings.push(mapping)
        }
      }

      if (tableMappings.length === 0) {
        alert("Please select at least one table for replication")
        return
      }

      // Ensure connection IDs are strings
      const pipelineDataPayload = {
        name: pipelineData.name,
        description: pipelineData.description || "",
        source_connection_id: String(sourceConn.id),
        target_connection_id: String(targetConn.id),
        full_load_type: pipelineData.mode === "full_load" ? "overwrite" :
          pipelineData.mode === "cdc_only" ? "append" : "overwrite",
        cdc_enabled: pipelineData.mode !== "full_load",
        cdc_filters: pipelineData.cdc_filters || [],
        table_mappings: tableMappings,
      }

      console.log("Updating pipeline with payload:", {
        id: editingPipeline.id,
        data: {
          ...pipelineDataPayload,
          table_mappings: tableMappings,
        }
      })

      await dispatch(updatePipeline({
        id: editingPipeline.id,
        data: pipelineDataPayload,
      })).unwrap()
      setIsEditModalOpen(false)
      setEditingPipeline(null)
      dispatch(fetchPipelines())
    } catch (err: any) {
      console.error("Failed to update pipeline:", err)
      const errorMessage = err.response?.data?.detail || err.message || "Failed to update pipeline"
      showError(errorMessage, "Failed to Update Pipeline")
    }
  }

  const handleOpenEdit = (pipeline: any) => {
    // Allow editing if pipeline is stopped, paused, or draft
    const canEdit = pipeline.status === "paused" || pipeline.status === "draft" || pipeline.status === "deleted" || !pipeline.status || (pipeline.status !== "active" && pipeline.status !== "running")

    if (!canEdit) {
      showError("Please stop the pipeline before editing. Running pipelines cannot be modified.", "Cannot Edit Pipeline")
      return
    }

    // Find connection names for the wizard
    const sourceConn = connections.find(c => c.id === pipeline.source_connection_id)
    const targetConn = connections.find(c => c.id === pipeline.target_connection_id)

    setEditingPipeline({
      ...pipeline,
      sourceConnection: sourceConn?.name || "",
      targetConnection: targetConn?.name || "",
    })
    setIsEditModalOpen(true)
  }

  const handleDeletePipeline = async (id: number) => {
    showConfirm(
      "Delete Pipeline",
      "Are you sure you want to delete this pipeline? This action cannot be undone.",
      async () => {
        try {
          await dispatch(deletePipeline(id)).unwrap()
          // Refresh pipelines to remove deleted pipeline
          dispatch(fetchPipelines())
        } catch (err: any) {
          console.error("Failed to delete pipeline:", err)
          const errorMessage = err?.payload || err?.message || err?.response?.data?.detail || "Failed to delete pipeline"
          showError(errorMessage, "Failed to Delete Pipeline")
        }
      },
      "danger",
      "Delete Pipeline"
    )
  }

  const handleTriggerPipeline = async (id: number, runType: string = "full_load") => {
    try {
      // Optimistically update UI immediately (handled in Redux slice)
      const result = await dispatch(triggerPipeline({ id, runType })).unwrap()
      console.log("Pipeline triggered successfully:", result)
      // Refresh pipelines after longer delays to ensure backend status is updated
      setTimeout(() => {
        dispatch(fetchPipelines())
      }, 3000)
      setTimeout(() => {
        dispatch(fetchPipelines())
      }, 6000)
    } catch (err: any) {
      console.error("Failed to trigger pipeline:", err)
      // Extract error message from various possible locations
      let errorMessage = "Failed to start pipeline"

      if (err?.payload) {
        errorMessage = err.payload
      } else if (err?.response?.data?.detail) {
        errorMessage = err.response.data.detail
      } else if (err?.response?.data?.message) {
        errorMessage = err.response.data.message
      } else if (err?.message) {
        errorMessage = err.message
      } else if (typeof err === 'string') {
        errorMessage = err
      }

      // Remove redundant "Failed to trigger pipeline" prefix if present
      if (errorMessage.includes("Failed to trigger pipeline")) {
        errorMessage = errorMessage.replace("Failed to trigger pipeline", "").trim()
        if (errorMessage.startsWith(":")) {
          errorMessage = errorMessage.substring(1).trim()
        }
      }

      // Show user-friendly error with full details
      let displayError = errorMessage

      // Handle timeout errors specially
      if (errorMessage.includes("timeout") || errorMessage.includes("took too long")) {
        displayError = "Pipeline start is taking longer than expected (over 2 minutes). This may indicate:\n\n" +
          "1. Database connection issues\n" +
          "2. Kafka Connect server is slow or unresponsive\n" +
          "3. Large schema creation taking time\n" +
          "4. Connector setup is hanging\n\n" +
          "Please check:\n" +
          "- Backend logs for detailed error information\n" +
          "- Kafka Connect server status at http://72.61.233.209:8083\n" +
          "- Database connectivity\n" +
          "- Network connectivity between backend and Kafka Connect"
        showError(displayError, "Pipeline Start Timeout")
        return // Don't continue with other error processing for timeouts
      }

      // Extract actual error from Debezium connector error
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
        if (err?.response?.data?.detail) {
          displayError = err.response.data.detail
        }
      }

      // If still generic, try to get from response
      if (displayError.includes("400") && err?.response?.data?.detail) {
        displayError = err.response.data.detail
      }

      showError(displayError, "Failed to Start Pipeline")
    }
    // Still refresh to get current status even on error (will revert optimistic update)
    setTimeout(() => {
      dispatch(fetchPipelines())
    }, 1000)
  }

  const handlePausePipeline = async (id: number) => {
    try {
      await dispatch(pausePipeline(id)).unwrap()
      // Refresh pipelines to get updated status
      setTimeout(() => {
        dispatch(fetchPipelines())
      }, 1000)
    } catch (err: any) {
      console.error("Failed to pause pipeline:", err)
      const errorMessage = err?.payload || err?.message || err?.response?.data?.detail || "Failed to pause pipeline"
      showError(errorMessage, "Failed to Pause Pipeline")
    }
  }

  const handleStopPipeline = async (id: number) => {
    showConfirm(
      "Stop Pipeline",
      "Are you sure you want to stop this pipeline? This will stop all replication tasks.",
      async () => {
        try {
          await dispatch(stopPipeline(id)).unwrap()
          // Refresh pipelines to get updated status
          setTimeout(() => {
            dispatch(fetchPipelines())
          }, 1000)
        } catch (err: any) {
          console.error("Failed to stop pipeline:", err)
          const errorMessage = err?.payload || err?.message || err?.response?.data?.detail || "Failed to stop pipeline"
          showError(errorMessage, "Failed to Stop Pipeline")
        }
      },
      "warning",
      "Stop Pipeline"
    )
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "active":
        return <div className="w-3 h-3 rounded-full bg-success animate-pulse"></div>
      case "paused":
        return <Pause className="w-4 h-4 text-warning" />
      case "error":
        return <AlertCircle className="w-4 h-4 text-error" />
      default:
        return <Clock className="w-4 h-4 text-muted-foreground" />
    }
  }

  const getSourceConnectionName = (sourceId: number) => {
    const conn = connections.find(c => c.id === sourceId)
    return conn?.name || `Connection ${sourceId}`
  }

  const getTargetConnectionName = (targetId: number) => {
    const conn = connections.find(c => c.id === targetId)
    return conn?.name || `Connection ${targetId}`
  }

  // Pagination logic
  const totalPages = Math.ceil(pipelines.length / pipelinesPerPage)
  const startIndex = (currentPage - 1) * pipelinesPerPage
  const endIndex = startIndex + pipelinesPerPage
  const paginatedPipelines = useMemo(() => {
    const pipelinesArray = Array.isArray(pipelines) ? pipelines : []
    return pipelinesArray.slice(startIndex, endIndex)
  }, [pipelines, startIndex, endIndex])

  // Reset to page 1 when pipelines change
  useEffect(() => {
    if (currentPage > totalPages && totalPages > 0) {
      setCurrentPage(1)
    }
  }, [pipelines.length, totalPages, currentPage])

  if (selectedPipelineId) {
    const pipeline = pipelines.find((p) => p.id === selectedPipelineId)
    if (pipeline) {
      return (
        <>
          <PipelineDetail pipeline={pipeline} onBack={() => setSelectedPipelineId(null)} />
          <ErrorToastComponent />
        </>
      )
    }
  }

  return (
    <>
      <ErrorToastComponent />
      <ConfirmDialogComponent />
      <ProtectedPage path="/pipelines" requiredPermission="create_pipeline">
        <div className="p-6 space-y-6">
          <PageHeader
            title="Replication Pipelines"
            subtitle="Create and manage data replication pipelines"
            icon={Workflow}
            action={
              <div className="flex items-center gap-3">
                <ViewToggle view={view} onViewChange={setView} />
                <Button onClick={() => setIsModalOpen(true)} className="bg-primary hover:bg-primary/90 text-foreground gap-2">
                  <Plus className="w-4 h-4" />
                  New Pipeline
                </Button>
              </div>
            }
          />

          {/* Error Message */}
          {error && (
            <div className="p-4 bg-error/10 border border-error/30 rounded-lg">
              <p className="text-sm text-error">{error}</p>
            </div>
          )}

          {/* Loading State */}
          {isLoading && pipelines.length === 0 && (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
              <span className="ml-2 text-foreground-muted">Loading pipelines...</span>
            </div>
          )}

          {/* Empty State */}
          {!isLoading && pipelines.length === 0 && (
            <div className="text-center py-12">
              <p className="text-foreground-muted mb-4">No pipelines found</p>
              <Button
                onClick={() => setIsModalOpen(true)}
                className="bg-primary hover:bg-primary/90 text-foreground gap-2"
              >
                <Plus className="w-4 h-4" />
                Create First Pipeline
              </Button>
            </div>
          )}

          {/* Pipelines Grid */}
          {pipelines.length > 0 && (
            <>
              {view === "grid" ? (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                  {paginatedPipelines.map((pipeline) => {
                    const isActive = pipeline.status === "active" || pipeline.status === "running"
                    const isPaused = pipeline.status === "paused"
                    const isError = pipeline.status === "failed" || pipeline.status === "error"
                    const stats = getPipelineReplicationStats(pipeline.id)

                    // Status-based color configs
                    const statusColors = isActive
                      ? { accent: "from-emerald-400 to-cyan-500", accentDark: "dark:from-emerald-500 dark:to-cyan-600", glow: "hover:shadow-[0_8px_30px_rgba(0,210,182,0.18)]", glowDark: "dark:hover:shadow-[0_8px_30px_rgba(0,210,182,0.25)]", bg: "bg-gradient-to-br from-emerald-50/80 via-cyan-50/60 to-sky-50/80", bgDark: "dark:from-[#0f2620] dark:via-[#0e2229] dark:to-[#0f1f2d]", dot: "bg-emerald-500", ring: "ring-emerald-500/20 dark:ring-emerald-400/30", iconBg: "bg-emerald-100 dark:bg-emerald-500/15", iconText: "text-emerald-600 dark:text-emerald-400", statBg: "bg-emerald-50/60 dark:bg-emerald-500/[0.06]", borderAccent: "border-emerald-200/60 dark:border-emerald-500/15" }
                      : isPaused
                        ? { accent: "from-amber-400 to-orange-500", accentDark: "dark:from-amber-500 dark:to-orange-600", glow: "hover:shadow-[0_8px_30px_rgba(245,158,11,0.15)]", glowDark: "dark:hover:shadow-[0_8px_30px_rgba(245,158,11,0.2)]", bg: "bg-gradient-to-br from-amber-50/80 via-orange-50/60 to-yellow-50/80", bgDark: "dark:from-[#201a0e] dark:via-[#221c10] dark:to-[#1e1a0f]", dot: "bg-amber-500", ring: "ring-amber-500/20 dark:ring-amber-400/30", iconBg: "bg-amber-100 dark:bg-amber-500/15", iconText: "text-amber-600 dark:text-amber-400", statBg: "bg-amber-50/60 dark:bg-amber-500/[0.06]", borderAccent: "border-amber-200/60 dark:border-amber-500/15" }
                        : isError
                          ? { accent: "from-rose-400 to-pink-500", accentDark: "dark:from-rose-500 dark:to-pink-600", glow: "hover:shadow-[0_8px_30px_rgba(244,63,94,0.15)]", glowDark: "dark:hover:shadow-[0_8px_30px_rgba(244,63,94,0.2)]", bg: "bg-gradient-to-br from-rose-50/80 via-pink-50/60 to-fuchsia-50/80", bgDark: "dark:from-[#200e14] dark:via-[#220e18] dark:to-[#1e0f1a]", dot: "bg-rose-500", ring: "ring-rose-500/20 dark:ring-rose-400/30", iconBg: "bg-rose-100 dark:bg-rose-500/15", iconText: "text-rose-600 dark:text-rose-400", statBg: "bg-rose-50/60 dark:bg-rose-500/[0.06]", borderAccent: "border-rose-200/60 dark:border-rose-500/15" }
                          : { accent: "from-blue-400 to-indigo-500", accentDark: "dark:from-blue-500 dark:to-indigo-600", glow: "hover:shadow-[0_8px_30px_rgba(59,130,246,0.12)]", glowDark: "dark:hover:shadow-[0_8px_30px_rgba(59,130,246,0.18)]", bg: "bg-gradient-to-br from-blue-50/80 via-indigo-50/60 to-violet-50/80", bgDark: "dark:from-[#0e1420] dark:via-[#101524] dark:to-[#0f1322]", dot: "bg-slate-400 dark:bg-slate-500", ring: "ring-blue-500/10 dark:ring-blue-400/20", iconBg: "bg-blue-100 dark:bg-blue-500/15", iconText: "text-blue-600 dark:text-blue-400", statBg: "bg-blue-50/60 dark:bg-blue-500/[0.06]", borderAccent: "border-blue-200/60 dark:border-blue-500/15" }

                    return (
                      <div
                        key={pipeline.id}
                        className={cn(
                          "relative group cursor-pointer rounded-xl overflow-hidden h-full flex flex-col",
                          "border border-black/[0.07] dark:border-white/[0.08]",
                          "shadow-sm transition-all duration-500 ease-out",
                          statusColors.glow, statusColors.glowDark,
                          "hover:-translate-y-0.5",
                          // Light mode bg
                          statusColors.bg,
                          // Dark mode bg
                          "dark:bg-gradient-to-br", statusColors.bgDark,
                        )}
                        onClick={() => setSelectedPipelineId(pipeline.id)}
                      >
                        {/* Top Accent Gradient Bar */}
                        <div className={cn(
                          "h-1 w-full bg-gradient-to-r",
                          statusColors.accent, statusColors.accentDark,
                          "relative overflow-hidden"
                        )}>
                          {isActive && (
                            <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/60 to-transparent animate-[shimmer_2s_infinite] translate-x-[-100%]" />
                          )}
                        </div>

                        {/* Decorative Background Orb */}
                        <div className={cn(
                          "absolute -right-10 -top-10 w-32 h-32 rounded-full blur-3xl opacity-[0.08] group-hover:opacity-[0.15] transition-opacity duration-700 pointer-events-none",
                          isActive ? "bg-emerald-500" : isPaused ? "bg-amber-500" : isError ? "bg-rose-500" : "bg-blue-500"
                        )} />

                        {/* Watermark Icon */}
                        <div className="absolute -right-4 -bottom-4 transition-all duration-700 group-hover:scale-110 group-hover:-rotate-12 pointer-events-none">
                          <Workflow className={cn(
                            "w-28 h-28 opacity-[0.03] dark:opacity-[0.06] group-hover:opacity-[0.08] transition-opacity duration-500",
                            isActive ? "text-emerald-500" : isPaused ? "text-amber-500" : isError ? "text-rose-500" : "text-blue-500"
                          )} />
                        </div>

                        {/* === Card Body === */}
                        <div className="flex flex-col flex-1 relative z-10">

                          {/* Header Section */}
                          <div className="p-4 pb-3">
                            {/* Top Row: Status + Icon Badge */}
                            <div className="flex items-start justify-between mb-3">
                              <div className="flex items-center gap-2.5 flex-1 min-w-0 pr-2">
                                {/* Status Orb */}
                                <div className={cn("relative flex-shrink-0")}>
                                  <div className={cn(
                                    "w-2.5 h-2.5 rounded-full",
                                    statusColors.dot,
                                    isActive && "animate-pulse"
                                  )} />
                                  {isActive && (
                                    <div className={cn(
                                      "absolute -inset-1 rounded-full animate-ping opacity-30",
                                      statusColors.dot
                                    )} />
                                  )}
                                </div>
                                <h3 className="text-sm font-bold text-foreground truncate group-hover:text-primary transition-colors duration-300">
                                  {pipeline.name}
                                </h3>
                              </div>
                              {/* Pipeline Type Icon */}
                              <div className={cn(
                                "p-1.5 rounded-lg transition-all duration-300 flex-shrink-0",
                                statusColors.iconBg,
                                "group-hover:scale-110 group-hover:rotate-3"
                              )}>
                                <Workflow className={cn("w-3.5 h-3.5", statusColors.iconText)} />
                              </div>
                            </div>

                            {/* Description */}
                            {pipeline.description && (
                              <p className="text-[11px] text-foreground-muted/80 line-clamp-1 mb-2.5 pl-5">{pipeline.description}</p>
                            )}

                            {/* Badges */}
                            <div className="flex items-center gap-1.5 flex-wrap pl-5">
                              {isActive && (
                                <span className={cn(
                                  "inline-flex items-center gap-1.5 text-[10px] font-semibold px-2 py-0.5 rounded-full",
                                  "bg-emerald-100 text-emerald-700 dark:bg-emerald-500/15 dark:text-emerald-400",
                                  "ring-1 ring-emerald-500/20 dark:ring-emerald-400/20"
                                )}>
                                  <span className="w-1 h-1 rounded-full bg-emerald-500 animate-pulse" />
                                  Live
                                </span>
                              )}
                              {isPaused && (
                                <span className={cn(
                                  "inline-flex items-center gap-1.5 text-[10px] font-semibold px-2 py-0.5 rounded-full",
                                  "bg-amber-100 text-amber-700 dark:bg-amber-500/15 dark:text-amber-400",
                                  "ring-1 ring-amber-500/20 dark:ring-amber-400/20"
                                )}>
                                  <Pause className="w-2.5 h-2.5" />
                                  Paused
                                </span>
                              )}
                              {isError && (
                                <span className={cn(
                                  "inline-flex items-center gap-1.5 text-[10px] font-semibold px-2 py-0.5 rounded-full",
                                  "bg-rose-100 text-rose-700 dark:bg-rose-500/15 dark:text-rose-400",
                                  "ring-1 ring-rose-500/20 dark:ring-rose-400/20"
                                )}>
                                  <AlertCircle className="w-2.5 h-2.5" />
                                  Error
                                </span>
                              )}
                              <span className={cn(
                                "inline-flex items-center text-[10px] font-semibold px-2 py-0.5 rounded-full",
                                pipeline.full_load_type === "overwrite" && pipeline.cdc_enabled
                                  ? "bg-blue-100 text-blue-700 ring-1 ring-blue-500/20 dark:bg-blue-500/15 dark:text-blue-400 dark:ring-blue-400/20"
                                  : pipeline.cdc_enabled
                                    ? "bg-sky-100 text-sky-700 ring-1 ring-sky-500/20 dark:bg-sky-500/15 dark:text-sky-400 dark:ring-sky-400/20"
                                    : "bg-violet-100 text-violet-700 ring-1 ring-violet-500/20 dark:bg-violet-500/15 dark:text-violet-400 dark:ring-violet-400/20"
                              )}>
                                {pipeline.full_load_type === "overwrite" && pipeline.cdc_enabled
                                  ? "Full + CDC"
                                  : pipeline.cdc_enabled ? "CDC" : "Full Load"}
                              </span>
                            </div>
                          </div>

                          {/* Connection Flow Strip */}
                          <div className={cn(
                            "mx-4 mb-3 flex items-center gap-2 p-2.5 rounded-lg",
                            "bg-white/50 dark:bg-white/[0.03]",
                            "border", statusColors.borderAccent,
                            "backdrop-blur-sm"
                          )}>
                            <div className="flex items-center gap-1.5 flex-1 min-w-0">
                              <div className="p-1 rounded bg-primary/10 dark:bg-primary/20">
                                <Database className="w-3 h-3 text-primary" />
                              </div>
                              <span className="text-[11px] font-medium text-foreground truncate">{getSourceConnectionName(pipeline.source_connection_id)}</span>
                            </div>
                            <div className="flex items-center gap-0.5 flex-shrink-0">
                              <div className="w-4 h-[1.5px] bg-foreground-muted/30 rounded-full" />
                              <ArrowRight className={cn("w-3.5 h-3.5", statusColors.iconText)} />
                              <div className="w-4 h-[1.5px] bg-foreground-muted/30 rounded-full" />
                            </div>
                            <div className="flex items-center gap-1.5 flex-1 min-w-0 justify-end">
                              <span className="text-[11px] font-medium text-foreground truncate text-right">{getTargetConnectionName(pipeline.target_connection_id)}</span>
                              <div className="p-1 rounded bg-info/10 dark:bg-info/20">
                                <Database className="w-3 h-3 text-info" />
                              </div>
                            </div>
                          </div>

                          {/* Stats Grid */}
                          <div className="mx-4 mb-3 grid grid-cols-4 gap-1.5">
                            {[
                              { icon: GitBranch, label: "Tables", value: pipeline.table_mappings?.length || 0 },
                              { icon: Activity, label: "Events", value: stats.totalEvents.toLocaleString() },
                              { icon: Clock, label: "Success", value: stats.successEvents.toLocaleString() },
                              { icon: Database, label: "Rate", value: `${stats.successRate}%` }
                            ].map(({ icon: Icon, label, value }) => (
                              <div key={label} className={cn(
                                "flex flex-col items-center justify-center py-2 px-1 rounded-lg",
                                statusColors.statBg,
                                "border", statusColors.borderAccent
                              )}>
                                <Icon className={cn("w-3 h-3 mb-1", statusColors.iconText, "opacity-70")} />
                                <span className="text-[9px] text-foreground-muted/70 uppercase tracking-widest font-medium">{label}</span>
                                <span className="text-xs font-bold text-foreground mt-0.5">{value}</span>
                              </div>
                            ))}
                          </div>

                          {/* Actions Bar */}
                          <div className={cn(
                            "px-3 py-2 flex items-center justify-between mt-auto",
                            "border-t", statusColors.borderAccent,
                            "bg-white/40 dark:bg-black/15 backdrop-blur-sm"
                          )}>
                            <div className="flex gap-0.5">
                              {isActive ? (
                                <>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    className={cn("h-7 w-7 p-0 rounded-lg text-foreground-muted hover:text-amber-600 dark:hover:text-amber-400 hover:bg-amber-100/80 dark:hover:bg-amber-500/15 transition-colors")}
                                    onClick={(e) => { e.stopPropagation(); handlePausePipeline(pipeline.id) }}
                                    disabled={isLoading}
                                    title="Pause"
                                  >
                                    <Pause className="w-3.5 h-3.5" />
                                  </Button>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    className="h-7 w-7 p-0 rounded-lg text-foreground-muted hover:text-rose-600 dark:hover:text-rose-400 hover:bg-rose-100/80 dark:hover:bg-rose-500/15 transition-colors"
                                    onClick={(e) => { e.stopPropagation(); handleStopPipeline(pipeline.id) }}
                                    disabled={isLoading}
                                    title="Stop"
                                  >
                                    <Square className="w-3.5 h-3.5" />
                                  </Button>
                                </>
                              ) : (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 w-7 p-0 rounded-lg text-foreground-muted hover:text-emerald-600 dark:hover:text-emerald-400 hover:bg-emerald-100/80 dark:hover:bg-emerald-500/15 transition-colors"
                                  onClick={(e) => { e.stopPropagation(); handleTriggerPipeline(pipeline.id, "full_load") }}
                                  disabled={isLoading}
                                  title="Run"
                                >
                                  <Play className="w-3.5 h-3.5" />
                                </Button>
                              )}
                            </div>
                            <div className="flex gap-0.5">
                              <Button
                                variant="ghost" size="sm"
                                className="h-7 w-7 p-0 rounded-lg text-foreground-muted hover:text-foreground hover:bg-black/5 dark:hover:bg-white/10 transition-colors"
                                onClick={(e) => { e.stopPropagation(); handleOpenEdit(pipeline) }}
                                title="Edit"
                              >
                                <Edit2 className="w-3.5 h-3.5" />
                              </Button>
                              <Button
                                variant="ghost" size="sm"
                                className="h-7 w-7 p-0 rounded-lg text-foreground-muted hover:text-foreground hover:bg-black/5 dark:hover:bg-white/10 transition-colors"
                                onClick={(e) => { e.stopPropagation(); setSelectedPipelineId(pipeline.id) }}
                                title="Details"
                              >
                                <Settings className="w-3.5 h-3.5" />
                              </Button>
                              <Button
                                variant="ghost" size="sm"
                                className="h-7 w-7 p-0 rounded-lg text-foreground-muted hover:text-rose-600 dark:hover:text-rose-400 hover:bg-rose-100/80 dark:hover:bg-rose-500/15 transition-colors"
                                onClick={(e) => { e.stopPropagation(); handleDeletePipeline(pipeline.id) }}
                                disabled={isLoading}
                                title="Delete"
                              >
                                <Trash2 className="w-3.5 h-3.5" />
                              </Button>
                            </div>
                          </div>
                        </div>
                      </div>
                    )
                  })}
                </div>
              ) : (
                <div className="space-y-2.5">
                  {paginatedPipelines.map((pipeline) => {
                    const isActive = pipeline.status === "active" || pipeline.status === "running"
                    const isPaused = pipeline.status === "paused"
                    const isError = pipeline.status === "failed" || pipeline.status === "error"
                    const stats = getPipelineReplicationStats(pipeline.id)

                    const listColors = isActive
                      ? { bg: "from-emerald-50/70 via-cyan-50/50 to-sky-50/70", bgDark: "dark:from-[#0f2620] dark:via-[#0e2229] dark:to-[#0f1f2d]", accent: "from-emerald-400 to-cyan-500", accentDark: "dark:from-emerald-500 dark:to-cyan-600", dot: "bg-emerald-500", glow: "hover:shadow-[0_4px_24px_rgba(0,210,182,0.14)]", glowDark: "dark:hover:shadow-[0_4px_24px_rgba(0,210,182,0.22)]", badge: "bg-emerald-100 text-emerald-700 ring-1 ring-emerald-500/20 dark:bg-emerald-500/15 dark:text-emerald-400", iconText: "text-emerald-600 dark:text-emerald-400" }
                      : isPaused
                        ? { bg: "from-amber-50/70 via-orange-50/50 to-yellow-50/70", bgDark: "dark:from-[#201a0e] dark:via-[#221c10] dark:to-[#1e1a0f]", accent: "from-amber-400 to-orange-500", accentDark: "dark:from-amber-500 dark:to-orange-600", dot: "bg-amber-500", glow: "hover:shadow-[0_4px_24px_rgba(245,158,11,0.12)]", glowDark: "dark:hover:shadow-[0_4px_24px_rgba(245,158,11,0.18)]", badge: "bg-amber-100 text-amber-700 ring-1 ring-amber-500/20 dark:bg-amber-500/15 dark:text-amber-400", iconText: "text-amber-600 dark:text-amber-400" }
                        : isError
                          ? { bg: "from-rose-50/70 via-pink-50/50 to-fuchsia-50/70", bgDark: "dark:from-[#200e14] dark:via-[#220e18] dark:to-[#1e0f1a]", accent: "from-rose-400 to-pink-500", accentDark: "dark:from-rose-500 dark:to-pink-600", dot: "bg-rose-500", glow: "hover:shadow-[0_4px_24px_rgba(244,63,94,0.12)]", glowDark: "dark:hover:shadow-[0_4px_24px_rgba(244,63,94,0.18)]", badge: "bg-rose-100 text-rose-700 ring-1 ring-rose-500/20 dark:bg-rose-500/15 dark:text-rose-400", iconText: "text-rose-600 dark:text-rose-400" }
                          : { bg: "from-blue-50/70 via-indigo-50/50 to-violet-50/70", bgDark: "dark:from-[#0e1420] dark:via-[#101524] dark:to-[#0f1322]", accent: "from-blue-400 to-indigo-500", accentDark: "dark:from-blue-500 dark:to-indigo-600", dot: "bg-slate-400 dark:bg-slate-500", glow: "hover:shadow-[0_4px_24px_rgba(59,130,246,0.1)]", glowDark: "dark:hover:shadow-[0_4px_24px_rgba(59,130,246,0.15)]", badge: "bg-blue-100 text-blue-700 ring-1 ring-blue-500/20 dark:bg-blue-500/15 dark:text-blue-400", iconText: "text-blue-600 dark:text-blue-400" }

                    return (
                      <div
                        key={pipeline.id}
                        className={cn(
                          "relative flex items-center justify-between p-3 rounded-xl overflow-hidden transition-all duration-300 cursor-pointer group",
                          "border border-black/[0.07] dark:border-white/[0.08]",
                          "bg-gradient-to-r", listColors.bg,
                          "dark:bg-gradient-to-r", listColors.bgDark,
                          listColors.glow, listColors.glowDark,
                          "hover:-translate-y-px"
                        )}
                        onClick={() => setSelectedPipelineId(pipeline.id)}
                      >
                        {/* Left Accent Bar */}
                        <div className={cn(
                          "absolute left-0 top-0 bottom-0 w-1 bg-gradient-to-b",
                          listColors.accent, listColors.accentDark
                        )} />

                        <div className="flex items-center gap-4 flex-1 min-w-0 pl-2">
                          <div className="flex items-center gap-3 min-w-[200px]">
                            {/* Status Dot */}
                            <div className="relative flex-shrink-0">
                              <div className={cn("w-2.5 h-2.5 rounded-full", listColors.dot, isActive && "animate-pulse")} />
                              {isActive && <div className={cn("absolute -inset-1 rounded-full animate-ping opacity-30", listColors.dot)} />}
                            </div>
                            <div>
                              <h3 className="text-sm font-bold text-foreground truncate group-hover:text-primary transition-colors">{pipeline.name}</h3>
                              <div className="flex items-center gap-2 mt-0.5">
                                <span className={cn("text-[10px] font-semibold px-2 py-0.5 rounded-full", listColors.badge)}>
                                  {pipeline.full_load_type === "overwrite" && pipeline.cdc_enabled ? "Full + CDC" : pipeline.cdc_enabled ? "CDC Only" : "Full Load"}
                                </span>
                              </div>
                            </div>
                          </div>

                          <div className="flex items-center gap-2 text-xs text-foreground-muted px-4 border-l border-black/[0.06] dark:border-white/[0.06] min-w-[250px]">
                            <div className="flex items-center gap-1.5 max-w-[120px]">
                              <div className="p-0.5 rounded bg-primary/10"><Database className="w-2.5 h-2.5 text-primary" /></div>
                              <span className="truncate font-medium">{getSourceConnectionName(pipeline.source_connection_id)}</span>
                            </div>
                            <div className="flex items-center gap-0.5">
                              <div className="w-3 h-[1.5px] bg-foreground-muted/30 rounded-full" />
                              <ArrowRight className={cn("w-3 h-3", listColors.iconText)} />
                              <div className="w-3 h-[1.5px] bg-foreground-muted/30 rounded-full" />
                            </div>
                            <div className="flex items-center gap-1.5 max-w-[120px]">
                              <span className="truncate font-medium">{getTargetConnectionName(pipeline.target_connection_id)}</span>
                              <div className="p-0.5 rounded bg-info/10"><Database className="w-2.5 h-2.5 text-info" /></div>
                            </div>
                          </div>

                          <div className="flex items-center gap-6 px-4 border-l border-black/[0.06] dark:border-white/[0.06] hidden lg:flex">
                            <div className="text-center">
                              <p className="text-[10px] text-foreground-muted uppercase tracking-wider">Tables</p>
                              <p className="text-xs font-bold text-foreground">{pipeline.table_mappings?.length}</p>
                            </div>
                            <div className="text-center">
                              <p className="text-[10px] text-foreground-muted uppercase tracking-wider">Events</p>
                              <p className="text-xs font-bold text-foreground">{stats.totalEvents}</p>
                            </div>
                            <div className="text-center">
                              <p className="text-[10px] text-foreground-muted uppercase tracking-wider">Rate</p>
                              <p className="text-xs font-bold text-foreground">{stats.successRate}%</p>
                            </div>
                          </div>
                        </div>

                        <div className="flex items-center gap-0.5 pl-3 border-l border-black/[0.06] dark:border-white/[0.06] ml-3">
                          <Button
                            variant="ghost" size="sm"
                            className="h-8 w-8 p-0 rounded-lg text-foreground-muted hover:text-foreground hover:bg-black/5 dark:hover:bg-white/10 transition-colors"
                            onClick={(e) => { e.stopPropagation(); setSelectedPipelineId(pipeline.id) }}
                          >
                            <Settings className="w-4 h-4" />
                          </Button>
                          {isActive ? (
                            <Button
                              variant="ghost" size="sm"
                              className="h-8 w-8 p-0 rounded-lg text-foreground-muted hover:text-rose-600 dark:hover:text-rose-400 hover:bg-rose-100/80 dark:hover:bg-rose-500/15 transition-colors"
                              onClick={(e) => { e.stopPropagation(); handleStopPipeline(pipeline.id) }}
                            >
                              <Square className="w-4 h-4" />
                            </Button>
                          ) : (
                            <Button
                              variant="ghost" size="sm"
                              className="h-8 w-8 p-0 rounded-lg text-foreground-muted hover:text-emerald-600 dark:hover:text-emerald-400 hover:bg-emerald-100/80 dark:hover:bg-emerald-500/15 transition-colors"
                              onClick={(e) => { e.stopPropagation(); handleTriggerPipeline(pipeline.id, "full_load") }}
                            >
                              <Play className="w-4 h-4" />
                            </Button>
                          )}
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}

              {/* Pagination Controls */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between pt-6 border-t border-border">
                  <div className="text-sm text-foreground-muted">
                    Showing <span className="font-semibold text-foreground">{startIndex + 1}</span> to{" "}
                    <span className="font-semibold text-foreground">
                      {Math.min(endIndex, pipelines.length)}
                    </span>{" "}
                    of <span className="font-semibold text-foreground">{pipelines.length}</span> pipelines
                  </div>

                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                      disabled={currentPage === 1}
                      className="border-border hover:bg-surface-hover disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <ChevronLeft className="w-4 h-4 mr-1" />
                      Previous
                    </Button>

                    <div className="flex items-center gap-1">
                      {Array.from({ length: totalPages }, (_, i) => i + 1).map((page) => {
                        // Show first page, last page, current page, and pages around current
                        if (
                          page === 1 ||
                          page === totalPages ||
                          (page >= currentPage - 1 && page <= currentPage + 1)
                        ) {
                          return (
                            <Button
                              key={page}
                              variant={currentPage === page ? "default" : "outline"}
                              size="sm"
                              onClick={() => setCurrentPage(page)}
                              className={`min-w-[40px] ${currentPage === page
                                ? "bg-primary text-white"
                                : "border-border hover:bg-surface-hover"
                                }`}
                            >
                              {page}
                            </Button>
                          )
                        } else if (
                          page === currentPage - 2 ||
                          page === currentPage + 2
                        ) {
                          return (
                            <span key={page} className="px-2 text-foreground-muted">
                              ...
                            </span>
                          )
                        }
                        return null
                      })}
                    </div>

                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                      disabled={currentPage === totalPages}
                      className="border-border hover:bg-surface-hover disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      Next
                      <ChevronRight className="w-4 h-4 ml-1" />
                    </Button>
                  </div>
                </div>
              )}
            </>
          )}

          {/* Modals */}
          <PipelineModal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} onSave={handleAddPipeline} />
          <PipelineWizard
            isOpen={isEditModalOpen}
            onClose={() => {
              setIsEditModalOpen(false)
              setEditingPipeline(null)
            }}
            onSave={handleEditPipeline}
            editingPipeline={editingPipeline}
          />
        </div>
      </ProtectedPage>
      <ErrorToastComponent />
    </>
  )
}
