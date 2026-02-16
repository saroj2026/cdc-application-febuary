"use client"

import { useState, useEffect } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Database, RefreshCw, Trash2, Save, Loader2, AlertCircle } from "lucide-react"
import { apiClient } from "@/lib/api/client"
import { useAppSelector } from "@/lib/store/hooks"

interface Checkpoint {
  id: string
  pipeline_id: string
  table_name: string
  schema_name?: string
  lsn?: string
  scn?: number
  binlog_file?: string
  binlog_position?: number
  sql_server_lsn?: string
  resume_token?: string
  checkpoint_value?: string
  checkpoint_type?: string
  rows_processed: number
  last_event_timestamp?: string
  last_updated_at: string
}

interface CheckpointManagerProps {
  pipelineId: string | number
  sourceConnectionType?: string
  tableMappings?: Array<{
    source_table: string
    target_table: string
    source_schema?: string
    target_schema?: string
  }>
}

export function CheckpointManager({ pipelineId, sourceConnectionType, tableMappings = [] }: CheckpointManagerProps) {
  const { connections } = useAppSelector((state) => state.connections)
  const [checkpoints, setCheckpoints] = useState<Checkpoint[]>([])
  const [loading, setLoading] = useState(false)
  const [editingCheckpoint, setEditingCheckpoint] = useState<string | null>(null)
  const [editData, setEditData] = useState<Partial<Checkpoint>>({})
  const [error, setError] = useState<string | null>(null)

  // Ensure checkpoints is always an array
  const safeCheckpoints = Array.isArray(checkpoints) ? checkpoints : []

  // Get source connection to determine database type
  const sourceConn = connections.find(c => c.id === pipelineId) // This might need adjustment based on how pipeline stores connection info

  useEffect(() => {
    fetchCheckpoints()
  }, [pipelineId])

  const fetchCheckpoints = async (retryCount = 0) => {
    setLoading(true)
    setError(null)
    try {
      const data = await apiClient.getPipelineCheckpoints(pipelineId)
      console.log('[CheckpointManager] Received data:', data)
      
      // Handle both response formats: { checkpoints: [...] } or direct array
      if (data) {
        if (Array.isArray(data)) {
          setCheckpoints(data)
        } else if (data.checkpoints && Array.isArray(data.checkpoints)) {
          setCheckpoints(data.checkpoints)
        } else {
          console.warn('[CheckpointManager] Unexpected data format:', data)
          setCheckpoints([])
        }
      } else {
        setCheckpoints([])
      }
    } catch (err: any) {
      // Check if error is a timeout - but getPipelineCheckpoints should handle this now
      const isTimeout = err?.isTimeout || err?.code === 'ECONNABORTED' || err?.message?.includes('timeout')
      
      // If getPipelineCheckpoints handled it, data should be returned, so this shouldn't happen
      // But if it does, just set empty checkpoints
      if (isTimeout && retryCount < 1) {
        // Retry once after a short delay
        console.log('[CheckpointManager] Timeout, retrying...')
        setTimeout(() => fetchCheckpoints(retryCount + 1), 1000)
        return
      }
      
      // After retry or if not timeout, just set empty checkpoints
      // Don't show error to user - empty checkpoints are acceptable
      console.warn('[CheckpointManager] Error fetching checkpoints, using empty list:', err?.message || err)
      setCheckpoints([])
      setError(null) // Don't show error to user
    } finally {
      setLoading(false)
    }
  }

  const handleEdit = (checkpoint: Checkpoint) => {
    setEditingCheckpoint(checkpoint.id)
    setEditData({
      lsn: checkpoint.lsn,
      scn: checkpoint.scn,
      binlog_file: checkpoint.binlog_file,
      binlog_position: checkpoint.binlog_position,
      sql_server_lsn: checkpoint.sql_server_lsn,
      resume_token: checkpoint.resume_token,
      checkpoint_value: checkpoint.checkpoint_value,
      checkpoint_type: checkpoint.checkpoint_type,
    })
  }

  const handleSave = async (checkpoint: Checkpoint) => {
    try {
      await apiClient.updatePipelineCheckpoint(
        pipelineId,
        checkpoint.table_name,
        {
          lsn: editData.lsn,
          scn: editData.scn,
          binlog_file: editData.binlog_file,
          binlog_position: editData.binlog_position,
          sql_server_lsn: editData.sql_server_lsn,
          resume_token: editData.resume_token,
          checkpoint_value: editData.checkpoint_value,
          checkpoint_type: editData.checkpoint_type,
        },
        checkpoint.schema_name
      )
      setEditingCheckpoint(null)
      setEditData({})
      fetchCheckpoints()
    } catch (err: any) {
      console.error("Failed to update checkpoint:", err)
      alert(err?.message || "Failed to update checkpoint")
    }
  }

  const handleReset = async (checkpoint: Checkpoint) => {
    if (!confirm(`Are you sure you want to reset the checkpoint for ${checkpoint.table_name}? This will delete the checkpoint and replication will start from the beginning.`)) {
      return
    }
    try {
      await apiClient.resetPipelineCheckpoint(pipelineId, checkpoint.table_name, checkpoint.schema_name)
      fetchCheckpoints()
    } catch (err: any) {
      console.error("Failed to reset checkpoint:", err)
      alert(err?.message || "Failed to reset checkpoint")
    }
  }

  const getCheckpointDisplay = (checkpoint: Checkpoint) => {
    const dbType = sourceConnectionType?.toLowerCase() || ""
    
    if (dbType === "postgresql" && checkpoint.lsn) {
      return { label: "LSN", value: checkpoint.lsn }
    } else if (dbType === "oracle" && checkpoint.scn) {
      return { label: "SCN", value: checkpoint.scn.toString() }
    } else if (dbType === "mysql" && checkpoint.binlog_file) {
      return { 
        label: "Binlog Position", 
        value: `${checkpoint.binlog_file}:${checkpoint.binlog_position || 0}` 
      }
    } else if (dbType === "sqlserver" && checkpoint.sql_server_lsn) {
      return { label: "LSN", value: checkpoint.sql_server_lsn }
    } else if (dbType === "mongodb" && checkpoint.resume_token) {
      return { label: "Resume Token", value: checkpoint.resume_token.substring(0, 50) + "..." }
    } else if (checkpoint.checkpoint_value) {
      return { label: checkpoint.checkpoint_type || "Checkpoint", value: checkpoint.checkpoint_value }
    }
    return { label: "No Checkpoint", value: "Not set" }
  }

  const getCheckpointFields = (dbType: string) => {
    const type = dbType?.toLowerCase() || ""
    if (type === "postgresql") {
      return [{ key: "lsn", label: "LSN (Log Sequence Number)", type: "text" }]
    } else if (type === "oracle") {
      return [{ key: "scn", label: "SCN (System Change Number)", type: "number" }]
    } else if (type === "mysql") {
      return [
        { key: "binlog_file", label: "Binlog File", type: "text" },
        { key: "binlog_position", label: "Binlog Position", type: "number" }
      ]
    } else if (type === "sqlserver") {
      return [{ key: "sql_server_lsn", label: "LSN", type: "text" }]
    } else if (type === "mongodb") {
      return [{ key: "resume_token", label: "Resume Token", type: "text" }]
    }
    return [{ key: "checkpoint_value", label: "Checkpoint Value", type: "text" }]
  }

  if (loading && safeCheckpoints.length === 0) {
    return (
      <Card className="bg-surface border-border p-6">
        <div className="flex items-center justify-center py-8">
          <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
          <span className="ml-2 text-foreground-muted">Loading checkpoints...</span>
        </div>
      </Card>
    )
  }

  return (
    <Card className="bg-surface border-border p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-lg font-semibold text-foreground">CDC Checkpoints</h3>
          <p className="text-sm text-foreground-muted mt-1">
            Track replication position (LSN/SCN/Binlog) for each table
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={fetchCheckpoints}
          className="bg-transparent border-border hover:bg-surface-hover gap-2"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </Button>
      </div>

      {error && (
        <div className={`p-3 rounded-lg mb-4 ${
          error.includes("slowly") || error.includes("timeout")
            ? "bg-warning/10 border border-warning/30"
            : "bg-error/10 border border-error/30"
        }`}>
          <div className={`flex items-center gap-2 ${
            error.includes("slowly") || error.includes("timeout")
              ? "text-warning"
              : "text-error"
          }`}>
            <AlertCircle className="w-4 h-4" />
            <span className="text-sm">{error}</span>
          </div>
        </div>
      )}

      {safeCheckpoints.length === 0 ? (
        <div className="text-center py-8 text-foreground-muted">
          <Database className="w-12 h-12 mx-auto mb-3 opacity-50" />
          <p>No checkpoints found. Checkpoints will be created automatically when CDC replication starts.</p>
        </div>
      ) : (
        <div className="space-y-3">
          {safeCheckpoints.map((checkpoint) => {
            const isEditing = editingCheckpoint === checkpoint.id
            const display = getCheckpointDisplay(checkpoint)
            const fields = getCheckpointFields(sourceConnectionType || "")

            return (
              <div
                key={checkpoint.id}
                className="p-4 bg-surface-hover rounded-lg border border-border"
              >
                <div className="flex items-start justify-between mb-3">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-1">
                      <Database className="w-4 h-4 text-foreground-muted" />
                      <span className="font-medium text-foreground">
                        {checkpoint.schema_name ? `${checkpoint.schema_name}.` : ""}{checkpoint.table_name}
                      </span>
                      <Badge variant="outline" className="text-xs">
                        {display.label}
                      </Badge>
                    </div>
                    {!isEditing && (
                      <div className="mt-2">
                        <span className="text-sm text-foreground-muted">{display.label}: </span>
                        <span className="text-sm font-mono text-foreground">{display.value}</span>
                        {checkpoint.rows_processed > 0 && (
                          <span className="text-xs text-foreground-muted ml-3">
                            • {checkpoint.rows_processed.toLocaleString()} rows processed
                          </span>
                        )}
                        {checkpoint.last_event_timestamp && (
                          <span className="text-xs text-foreground-muted ml-3">
                            • Last event: {new Date(checkpoint.last_event_timestamp).toLocaleString()}
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    {isEditing ? (
                      <>
                        <Button
                          size="sm"
                          onClick={() => handleSave(checkpoint)}
                          className="bg-primary hover:bg-primary/90 text-foreground gap-1"
                        >
                          <Save className="w-3 h-3" />
                          Save
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => {
                            setEditingCheckpoint(null)
                            setEditData({})
                          }}
                          className="bg-transparent border-border hover:bg-surface-hover"
                        >
                          Cancel
                        </Button>
                      </>
                    ) : (
                      <>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => handleEdit(checkpoint)}
                          className="bg-transparent border-border hover:bg-surface-hover gap-1"
                        >
                          Edit
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => handleReset(checkpoint)}
                          className="bg-transparent border-border hover:bg-error/10 text-error hover:text-error gap-1"
                        >
                          <Trash2 className="w-3 h-3" />
                          Reset
                        </Button>
                      </>
                    )}
                  </div>
                </div>

                {isEditing && (
                  <div className="mt-3 pt-3 border-t border-border space-y-3">
                    {fields.map((field) => (
                      <div key={field.key}>
                        <Label className="text-sm text-foreground-muted">{field.label}</Label>
                        <Input
                          type={field.type}
                          value={
                            field.type === "number"
                              ? (editData[field.key as keyof typeof editData] as number) || ""
                              : (editData[field.key as keyof typeof editData] as string) || ""
                          }
                          onChange={(e) =>
                            setEditData({
                              ...editData,
                              [field.key]: field.type === "number" ? parseInt(e.target.value) || 0 : e.target.value,
                            })
                          }
                          className="mt-1 bg-input border-border text-foreground"
                          placeholder={`Enter ${field.label.toLowerCase()}`}
                        />
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </Card>
  )
}

