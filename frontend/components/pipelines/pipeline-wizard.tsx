"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Card } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { ChevronRight, ChevronLeft, Copy, GripVertical, Loader2, Database, X, Trash2, Plus, Check, Edit2 } from "lucide-react"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { fetchConnections } from "@/lib/store/slices/connectionSlice"
import { apiClient } from "@/lib/api/client"
import { DatabaseLogo } from "@/lib/database-logo-loader"
import { getDatabaseByConnectionType } from "@/lib/database-icons"

interface PipelineWizardProps {
  isOpen: boolean
  onClose: () => void
  onSave: (data: any) => void
  editingPipeline?: any | null
}

// Mock data removed - using real data from API

export function PipelineWizard({ isOpen, onClose, onSave, editingPipeline }: PipelineWizardProps) {
  const dispatch = useAppDispatch()
  const { connections } = useAppSelector((state) => state.connections)
  const [step, setStep] = useState(1)
  const [formData, setFormData] = useState({
    name: "",
    description: "",
    sourceConnection: "",
    targetConnection: "",
    mode: "full_load_cdc",
    schedule: "hourly",
  })
  
  // Initialize form data when editing
  useEffect(() => {
    if (editingPipeline && isOpen) {
      setFormData({
        name: editingPipeline.name || "",
        description: editingPipeline.description || "",
        sourceConnection: editingPipeline.sourceConnection || "",
        targetConnection: editingPipeline.targetConnection || "",
        mode: editingPipeline.cdc_enabled 
          ? (editingPipeline.full_load_type === "overwrite" ? "full_load_cdc" : "cdc_only")
          : "full_load",
        schedule: "hourly",
      })
      // Load existing table mappings if available
      if (editingPipeline.table_mappings && editingPipeline.table_mappings.length > 0) {
        const mapping: Record<string, string> = {}
        const tables: string[] = []
        editingPipeline.table_mappings.forEach((tm: any) => {
          const sourceKey = tm.source_schema ? `${tm.source_schema}.${tm.source_table}` : tm.source_table
          mapping[sourceKey] = tm.target_table
          tables.push(sourceKey)
        })
        setTableMapping(mapping)
        setSelectedTables(tables)
        setTargetTables(Object.values(mapping))
      }
      setStep(1)
    } else if (!editingPipeline && isOpen) {
      // Reset form for new pipeline
      setFormData({
        name: "",
        description: "",
        sourceConnection: "",
        targetConnection: "",
        mode: "full_load_cdc",
        schedule: "hourly",
      })
      setSelectedTables([])
      setTableMapping({})
      setTargetTables([])
      setTargetTableSchemas({})
      setStep(1)
    }
  }, [editingPipeline, isOpen])
  const [selectedTables, setSelectedTables] = useState<string[]>([])
  const [tableMapping, setTableMapping] = useState<Record<string, string>>({})
  const [tableSchemas, setTableSchemas] = useState<Record<string, any>>({})
  const [targetTableSchemas, setTargetTableSchemas] = useState<Record<string, any>>({})
  const [newFieldInputs, setNewFieldInputs] = useState<Record<string, { name: string; type: string; nullable: boolean }>>({})
  const [draggedTable, setDraggedTable] = useState<string | null>(null)
  const [availableTables, setAvailableTables] = useState<any[]>([])
  const [loadingTables, setLoadingTables] = useState(false)
  const [targetTables, setTargetTables] = useState<string[]>([])
  const [showSchemaConfirm, setShowSchemaConfirm] = useState(false)
  const [schemaConfirmData, setSchemaConfirmData] = useState<{
    sourceTable: string
    targetTable: string
    sourceSchema: any
  } | null>(null)

  useEffect(() => {
    if (isOpen && connections.length === 0) {
      dispatch(fetchConnections())
    }
  }, [isOpen, connections.length, dispatch])

  // Get only successful/tested connections - user can choose any as source or target
  const successfulConnections = connections.filter(c => 
    c.last_test_status === "success" || c.last_test_status === true
  )

  // Get source connection object
  const sourceConnection = connections.find(c => c.name === formData.sourceConnection)

  const handleInputChange = (field: string, value: string) => {
    setFormData((prev) => ({ ...prev, [field]: value }))
  }

  // Helper function to parse table name (handles schema.table format)
  const parseTableName = (tableName: string): { schema?: string; table: string } => {
    if (!tableName) return { table: tableName }
    
    // Check if table name contains a dot (schema.table format)
    const parts = tableName.split('.')
    if (parts.length === 2) {
      return {
        schema: parts[0].trim(),
        table: parts[1].trim()
      }
    }
    return { table: tableName.trim() }
  }

  const handleTableRename = (originalTable: string, newName: string) => {
    setTableMapping((prev) => ({ ...prev, [originalTable]: newName }))
    // Also update target schema if it exists
    if (targetTableSchemas[originalTable]) {
      setTargetTableSchemas((prev) => {
        const newMap = { ...prev }
        const schema = newMap[originalTable]
        if (schema) {
          newMap[newName] = {
            ...schema,
            full_name: newName
          }
          delete newMap[originalTable]
        }
        return newMap
      })
    }
  }

  // Helper function to get default schema based on database type
  const getDefaultSchemaForDbType = (dbType: string | undefined): string => {
    if (!dbType) return "public"
    const normalizedType = dbType.toLowerCase()
    if (normalizedType === "sqlserver" || normalizedType === "mssql" || normalizedType === "sql_server") {
      return "dbo"
    } else if (normalizedType === "snowflake") {
      return "PUBLIC"
    } else if (normalizedType === "oracle") {
      return "" // Oracle uses username as schema
    }
    return "public" // PostgreSQL, MySQL, etc.
  }

  // Helper function to automatically copy schema when mapping a table
  const copySchemaToTarget = (sourceTable: string, targetTable: string) => {
    const sourceSchema = tableSchemas[sourceTable]
    if (sourceSchema) {
      const sourceParsed = parseTableName(sourceTable)
      const targetParsed = parseTableName(targetTable)
      
      // Get target connection to determine default schema
      const targetConn = connections.find(c => c.name === formData.targetConnection)
      const targetDbType = targetConn?.connection_type || targetConn?.database_type
      const defaultTargetSchema = getDefaultSchemaForDbType(targetDbType)
      
      // Determine target schema name - use target DB default if source doesn't have one
      let targetSchemaName: string | undefined
      if (targetParsed.schema && targetParsed.schema !== "undefined" && targetParsed.schema.trim() !== "") {
        // Target already has a schema specified
        targetSchemaName = targetParsed.schema
      } else if (sourceParsed.schema && sourceParsed.schema !== "undefined" && sourceParsed.schema.trim() !== "") {
        // Use source schema, but only if it makes sense for target (same DB type or both PostgreSQL-like)
        const sourceConn = connections.find(c => c.name === formData.sourceConnection)
        const sourceDbType = sourceConn?.connection_type || sourceConn?.database_type
        
        // If source and target are same DB type, use source schema
        // Otherwise, use target's default schema
        if (sourceDbType?.toLowerCase() === targetDbType?.toLowerCase()) {
          targetSchemaName = sourceParsed.schema
        } else {
          // Different DB types: use target's default schema
          targetSchemaName = defaultTargetSchema
        }
      } else if (sourceSchema.schema && sourceSchema.schema !== "undefined" && sourceSchema.schema.trim() !== "") {
        targetSchemaName = sourceSchema.schema
      } else {
        // No schema from source, use target's default
        targetSchemaName = defaultTargetSchema
      }
      
      // Construct full target name with schema if needed
      let fullTargetName = targetTable
      if (targetSchemaName && !targetTable.includes('.')) {
        // Only add schema if target doesn't already have one
        fullTargetName = `${targetSchemaName}.${targetParsed.table || targetTable}`
      } else if (targetTable.includes('.') && !targetParsed.schema) {
        // If target has a dot but no valid schema, reconstruct it
        if (targetSchemaName) {
          fullTargetName = `${targetSchemaName}.${targetParsed.table || targetTable.split('.')[1]}`
        }
      }
      
      // Copy schema to target - ensure schema is never "undefined"
      const finalSchemaName = targetSchemaName && targetSchemaName !== "undefined" ? targetSchemaName : undefined
      
      setTargetTableSchemas((prev) => ({
        ...prev,
        [fullTargetName]: {
          ...sourceSchema,
          schema: finalSchemaName,
          full_name: fullTargetName
        }
      }))
      
      // Update table mapping to use full name if schema was added
      if (fullTargetName !== targetTable) {
        setTableMapping((prev) => {
          const newMap = { ...prev }
          newMap[sourceTable] = fullTargetName
          return newMap
        })
        return fullTargetName
      }
    }
    return targetTable
  }

  const handleLoadTables = async () => {
    if (!sourceConnection) {
      alert("Please select a source connection first")
      return
    }

    setLoadingTables(true)
    try {
      const result = await apiClient.getConnectionTables(sourceConnection.id)
      setAvailableTables(result.tables || [])
      
      // Store schemas for later use - use full_name for Oracle tables with schema
      const schemas: Record<string, any> = {}
      result.tables?.forEach((table: any) => {
        // Use full_name if available (for Oracle schema.table format), otherwise use name
        const key = table.full_name || table.name
        schemas[key] = table
        // Also store by name for backward compatibility
        if (table.full_name && table.full_name !== table.name) {
          schemas[table.name] = table
        }
      })
      setTableSchemas(schemas)
    } catch (error: any) {
      console.error("Failed to load tables:", error)
      const errorMessage = error.response?.data?.detail || error.response?.data?.message || error.message || "Unknown error"
      // Format multi-line error messages for better readability
      const formattedMessage = errorMessage.split('\n').join('\n')
      alert(`Failed to load tables:\n\n${formattedMessage}`)
    } finally {
      setLoadingTables(false)
    }
  }

  // Load target connection tables
  const targetConnection = connections.find(c => c.name === formData.targetConnection)
  const [targetAvailableTables, setTargetAvailableTables] = useState<any[]>([])
  const [loadingTargetTables, setLoadingTargetTables] = useState(false)

  const handleLoadTargetTables = async () => {
    if (!targetConnection) {
      alert("Please select a target connection first")
      return
    }

    setLoadingTargetTables(true)
    try {
      const result = await apiClient.getConnectionTables(targetConnection.id)
      setTargetAvailableTables(result.tables || [])
      
      // Store target table schemas
      const targetSchemas: Record<string, any> = {}
      result.tables?.forEach((table: any) => {
        const key = table.full_name || table.name
        targetSchemas[key] = table
        if (table.full_name && table.full_name !== table.name) {
          targetSchemas[table.name] = table
        }
      })
      // Merge with existing target schemas
      setTargetTableSchemas((prev) => ({ ...prev, ...targetSchemas }))
    } catch (error: any) {
      console.error("Failed to load target tables:", error)
      const errorMessage = error.response?.data?.detail || error.response?.data?.message || error.message || "Unknown error"
      alert(`Failed to load target tables:\n\n${errorMessage}`)
    } finally {
      setLoadingTargetTables(false)
    }
  }

  const handleCopySchema = (table: string) => {
    const schema = tableSchemas[table]
    if (schema && schema.columns) {
      const schemaText = `Table: ${schema.full_name || table}\nColumns:\n${schema.columns.map((col: any) => `  - ${col.name} (${col.type})`).join("\n")}`
      navigator.clipboard.writeText(schemaText)
      alert("Schema copied to clipboard!")
    } else {
      alert("Schema information not available for this table")
    }
  }

  const handleDragStart = (table: string) => {
    setDraggedTable(table)
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    if (draggedTable) {
      // Ensure the table is in selected tables
      if (!selectedTables.includes(draggedTable)) {
        setSelectedTables((prev) => [...prev, draggedTable])
      }
      
      // Map source table to target table (default to same name, can be edited in Step 4)
      let targetName = draggedTable // Default: use same name, user can edit in Step 4
      
      // Automatically copy schema when mapping
      targetName = copySchemaToTarget(draggedTable, targetName)
      
      setTableMapping((prev) => ({ ...prev, [draggedTable]: targetName }))
      
      // Add to target tables if not already there
      if (!targetTables.includes(targetName)) {
        setTargetTables((prev) => [...prev, targetName])
      }
      
      setDraggedTable(null)
    }
  }

  const handleTableSelect = (table: string) => {
    if (!selectedTables.includes(table)) {
      setSelectedTables([...selectedTables, table])
      let targetName = table
      // Automatically copy schema when selecting
      targetName = copySchemaToTarget(table, targetName)
      setTableMapping((prev) => ({ ...prev, [table]: targetName }))
      if (!targetTables.includes(targetName)) {
        setTargetTables([...targetTables, targetName])
      }
    }
  }

  const handleSave = async () => {
    if (!formData.name || !formData.sourceConnection || !formData.targetConnection || selectedTables.length === 0) {
      alert("Please complete all required fields and select at least one table")
      return
    }

    const mappedTables = selectedTables.map((table) => ({
      source: table,
      target: tableMapping[table] || table,
    }))

    try {
      // Call onSave and wait for it to complete
      await onSave({
        ...formData,
        tables: selectedTables,
        tableMapping: mappedTables,
      })
      // Only reset and close if save was successful
      resetWizard()
      onClose() // Close the wizard after successful save
    } catch (error) {
      // If save fails, don't close the wizard so user can fix errors
      console.error("Failed to save pipeline:", error)
      // Error handling is done in the onSave callback (handleAddPipeline/handleEditPipeline)
    }
  }

  const resetWizard = () => {
    setStep(1)
    setFormData({
      name: "",
      description: "",
      sourceConnection: "",
      targetConnection: "",
      mode: "full_load_cdc",
      schedule: "hourly",
    })
    setSelectedTables([])
    setTableMapping({})
    setTableSchemas({})
    setTargetTableSchemas({})
    setAvailableTables([])
    setTargetTables([])
  }

  // Reset when modal closes
  // Reset wizard when closing (but not when editing)
  useEffect(() => {
    if (!isOpen && !editingPipeline) {
      resetWizard()
    }
  }, [isOpen, editingPipeline])

  const canProceedToNext = () => {
    switch (step) {
      case 1:
        return formData.name && formData.sourceConnection && formData.targetConnection && formData.mode
      case 2:
        return selectedTables.length > 0
      case 3:
        return selectedTables.length > 0
      case 4:
        return selectedTables.length > 0
      case 5:
        return formData.schedule
      default:
        return true
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-[95%] w-[95%] lg:max-w-[80%] lg:w-[80%] bg-surface border-border max-h-[96vh] h-[96vh] overflow-hidden p-6 flex flex-col">
        <DialogHeader>
          <DialogTitle className="text-foreground">
            {editingPipeline ? "Edit Replication Pipeline" : "Create Replication Pipeline"}
          </DialogTitle>
        </DialogHeader>

        {/* Progress Bar */}
        <div className="flex items-center justify-between gap-2 mb-4 flex-shrink-0">
          {[1, 2, 3, 4, 5].map((stepNum) => (
            <div key={stepNum} className="flex items-center flex-1">
              <div
                className={`w-8 h-8 rounded-full flex items-center justify-center font-semibold transition-all ${
                  stepNum <= step
                    ? "bg-primary text-foreground"
                    : "bg-surface-hover text-foreground-muted border border-border"
                }`}
              >
                {stepNum}
              </div>
              {stepNum < 5 && (
                <div className={`flex-1 h-1 mx-2 transition-all ${stepNum < step ? "bg-primary" : "bg-border"}`}></div>
              )}
            </div>
          ))}
        </div>

        {/* Step Content Container */}
        <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
        {/* Step 1: Basic Configuration */}
        {step === 1 && (
          <div className="space-y-4 flex-1 overflow-y-auto min-h-0">
            <h3 className="text-lg font-semibold text-foreground">Step 1: Basic Configuration</h3>

            <div>
              <Label className="text-foreground">Pipeline Name *</Label>
              <Input
                placeholder="e.g., Orders Replication"
                value={formData.name}
                onChange={(e) => handleInputChange("name", e.target.value)}
                className="mt-1"
                required
              />
            </div>

            <div>
              <Label className="text-foreground">Description</Label>
              <Input
                placeholder="Optional description"
                value={formData.description}
                onChange={(e) => handleInputChange("description", e.target.value)}
                className="mt-1"
              />
            </div>

            <div className="space-y-4">
              <div className="p-4 bg-primary/5 border border-primary/20 rounded-lg">
                <p className="text-sm text-foreground-muted mb-4">
                  Select which connection will be the <strong className="text-foreground">source</strong> (where data comes from) 
                  and which will be the <strong className="text-foreground">target</strong> (where data goes to). 
                  Only successfully tested connections are shown.
                </p>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label className="text-foreground flex items-center gap-2">
                    <span className="w-2 h-2 rounded-full bg-primary"></span>
                    Source Connection
                  </Label>
                  <Select
                    value={formData.sourceConnection}
                    onValueChange={(value) => {
                      handleInputChange("sourceConnection", value)
                      setSelectedTables([])
                      setTableMapping({})
                    }}
                  >
                  <SelectTrigger className="mt-1 bg-input border-border">
                    <SelectValue placeholder="Select source database">
                      {formData.sourceConnection && (() => {
                        const selectedConn = successfulConnections.find(c => c.name === formData.sourceConnection)
                        if (selectedConn) {
                          const dbInfo = getDatabaseByConnectionType(selectedConn.connection_type)
                          return (
                            <div className="flex items-center gap-2">
                              <DatabaseLogo
                                connectionType={selectedConn.connection_type}
                                databaseId={dbInfo?.id}
                                displayName={dbInfo?.displayName || selectedConn.connection_type}
                                size={20}
                                className="w-5 h-5 flex-shrink-0"
                              />
                              <span>{selectedConn.name}</span>
                            </div>
                          )
                        }
                        return formData.sourceConnection
                      })()}
                    </SelectValue>
                  </SelectTrigger>
                    <SelectContent className="bg-surface border-border">
                      {successfulConnections.length === 0 ? (
                        <div className="px-2 py-1.5 text-sm text-foreground-muted">
                          No successful connections available. Please test your connections first.
                        </div>
                      ) : (
                        successfulConnections
                          .filter(conn => conn.name !== formData.targetConnection)
                          .map((conn) => {
                            const dbInfo = getDatabaseByConnectionType(conn.connection_type)
                            return (
                              <SelectItem key={conn.id} value={conn.name}>
                                <div className="flex items-center gap-2">
                                  <DatabaseLogo
                                    connectionType={conn.connection_type}
                                    databaseId={dbInfo?.id}
                                    displayName={dbInfo?.displayName || conn.connection_type}
                                    size={20}
                                    className="w-5 h-5 flex-shrink-0"
                                  />
                                  <span>{conn.name}</span>
                                  <span className="text-xs text-foreground-muted">({conn.connection_type.toUpperCase()})</span>
                                </div>
                              </SelectItem>
                            )
                          })
                      )}
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label className="text-foreground flex items-center gap-2">
                    <span className="w-2 h-2 rounded-full bg-green-500"></span>
                    Target Connection
                  </Label>
                  <Select
                    value={formData.targetConnection}
                    onValueChange={(value) => handleInputChange("targetConnection", value)}
                  >
                    <SelectTrigger className="mt-1 bg-input border-border">
                      <SelectValue placeholder="Select target database">
                        {formData.targetConnection && (() => {
                          const selectedConn = successfulConnections.find(c => c.name === formData.targetConnection)
                          if (selectedConn) {
                            const dbInfo = getDatabaseByConnectionType(selectedConn.connection_type)
                            return (
                              <div className="flex items-center gap-2">
                                <DatabaseLogo
                                  connectionType={selectedConn.connection_type}
                                  databaseId={dbInfo?.id}
                                  displayName={dbInfo?.displayName || selectedConn.connection_type}
                                  size={20}
                                  className="w-5 h-5 flex-shrink-0"
                                />
                                <span>{selectedConn.name}</span>
                              </div>
                            )
                          }
                          return formData.targetConnection
                        })()}
                      </SelectValue>
                    </SelectTrigger>
                    <SelectContent className="bg-surface border-border">
                      {successfulConnections.length === 0 ? (
                        <div className="px-2 py-1.5 text-sm text-foreground-muted">
                          No successful connections available. Please test your connections first.
                        </div>
                      ) : (
                        successfulConnections
                          .filter(conn => conn.name !== formData.sourceConnection)
                          .map((conn) => {
                            const dbInfo = getDatabaseByConnectionType(conn.connection_type)
                            return (
                              <SelectItem key={conn.id} value={conn.name}>
                                <div className="flex items-center gap-2">
                                  <DatabaseLogo
                                    connectionType={conn.connection_type}
                                    databaseId={dbInfo?.id}
                                    displayName={dbInfo?.displayName || conn.connection_type}
                                    size={20}
                                    className="w-5 h-5 flex-shrink-0"
                                  />
                                  <span>{conn.name}</span>
                                  <span className="text-xs text-foreground-muted">({conn.connection_type.toUpperCase()})</span>
                                </div>
                              </SelectItem>
                            )
                          })
                      )}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>

            <div>
              <Label className="text-foreground">Replication Mode</Label>
              <Select value={formData.mode} onValueChange={(value) => handleInputChange("mode", value)}>
                <SelectTrigger className="mt-1 bg-input border-border">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-surface border-border">
                  <SelectItem value="full_load">Full Load Only</SelectItem>
                  <SelectItem value="cdc_only">CDC Only</SelectItem>
                  <SelectItem value="full_load_cdc">Full Load + CDC</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        )}

        {/* Step 2: Table Selection */}
        {step === 2 && (
          <div className="space-y-4 flex flex-col h-full overflow-hidden">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-lg font-semibold text-foreground">Step 2: Select Tables</h3>
                <p className="text-sm text-foreground-muted">
                  Load tables from source connection or manually add table names.
                </p>
              </div>
              {sourceConnection && (
                <Button
                  onClick={handleLoadTables}
                  disabled={loadingTables}
                  className="bg-primary hover:bg-primary/90 text-foreground gap-2"
                >
                  {loadingTables ? (
                    <>
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Loading...
                    </>
                  ) : (
                    <>
                      <Database className="w-4 h-4" />
                      Load Tables
                    </>
                  )}
                </Button>
              )}
            </div>

            {!sourceConnection && (
              <div className="p-4 bg-warning/10 border border-warning/30 rounded text-warning">
                Please select a source connection in Step 1 to load tables automatically.
              </div>
            )}

            <div className="space-y-3 flex-1 min-h-0 flex flex-col">
              {/* Available Tables from Database */}
              {availableTables.length > 0 && (
                <div className="space-y-2 flex-1 min-h-0 flex flex-col">
                  <Label className="text-foreground font-semibold">Available Tables ({availableTables.length})</Label>
                  <div className="grid grid-cols-2 gap-2 flex-1 min-h-0 overflow-y-auto p-2 bg-surface-hover rounded">
                    {availableTables.map((table: any) => (
                      <Card
                        key={table.name}
                        className={`bg-surface border-border p-3 cursor-pointer hover:border-primary/50 transition-colors ${
                          selectedTables.includes(table.name) ? "border-primary bg-primary/10" : ""
                        }`}
                        onClick={() => handleTableSelect(table.name)}
                      >
                        <div className="flex items-center justify-between">
                          <div className="flex-1">
                            <p className="font-semibold text-foreground text-sm">{table.full_name || table.name}</p>
                            <p className="text-xs text-foreground-muted">
                              {table.columns?.length || 0} columns
                            </p>
                          </div>
                          {selectedTables.includes(table.name) && (
                            <div className="w-2 h-2 rounded-full bg-primary"></div>
                          )}
                        </div>
                      </Card>
                    ))}
                  </div>
                </div>
              )}

              {/* Manual Table Input */}
              <div className="space-y-2">
                <Label className="text-foreground">Or Add Table Manually</Label>
                <div className="flex gap-2">
                  <Input
                    placeholder="Enter table name (e.g., orders)"
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault()
                        const input = e.currentTarget
                        const tableName = input.value.trim()
                        if (tableName && !selectedTables.includes(tableName)) {
                          setSelectedTables([...selectedTables, tableName])
                          setTableMapping((prev) => ({ ...prev, [tableName]: tableName }))
                          if (!targetTables.includes(tableName)) {
                            setTargetTables([...targetTables, tableName])
                          }
                          input.value = ""
                        }
                      }
                    }}
                    className="flex-1"
                  />
                  <Button
                    onClick={() => {
                      const input = document.querySelector('input[placeholder*="table name"]') as HTMLInputElement
                      if (input) {
                        const tableName = input.value.trim()
                        if (tableName && !selectedTables.includes(tableName)) {
                          setSelectedTables([...selectedTables, tableName])
                          setTableMapping((prev) => ({ ...prev, [tableName]: tableName }))
                          if (!targetTables.includes(tableName)) {
                            setTargetTables([...targetTables, tableName])
                          }
                          input.value = ""
                        }
                      }
                    }}
                  >
                    Add
                  </Button>
                </div>
              </div>

              {/* Selected Tables */}
              {selectedTables.length > 0 && (
                <div className="space-y-2 flex-1 min-h-0 flex flex-col">
                  <Label className="text-foreground font-semibold">Selected Tables ({selectedTables.length})</Label>
                  <div className="space-y-2 flex-1 min-h-0 overflow-y-auto">
                    {selectedTables.map((table) => (
                      <Card
                        key={table}
                        className="bg-surface border-border p-3 flex items-center justify-between"
                      >
                        <div className="flex-1">
                          <span className="font-semibold text-foreground">{table}</span>
                          {tableSchemas[table] && (
                            <p className="text-xs text-foreground-muted">
                              {tableSchemas[table].columns?.length || 0} columns
                            </p>
                          )}
                        </div>
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => {
                            setSelectedTables(selectedTables.filter(t => t !== table))
                            setTableMapping((prev) => {
                              const newMap = { ...prev }
                              delete newMap[table]
                              return newMap
                            })
                            setTargetTables(targetTables.filter(t => t !== table))
                          }}
                          className="text-error hover:text-error"
                        >
                          Remove
                        </Button>
                      </Card>
                    ))}
                  </div>
                </div>
              )}

              {selectedTables.length === 0 && availableTables.length === 0 && (
                <div className="text-center py-8 text-foreground-muted">
                  <p>No tables selected. Click "Load Tables" to fetch from source or add manually.</p>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Step 3: Drag & Drop Table Mapping */}
        {step === 3 && (
          <div className="space-y-4 flex flex-col h-full overflow-hidden">
            <div className="flex items-center justify-between flex-shrink-0">
              <div>
                <h3 className="text-lg font-semibold text-foreground">Step 3: Map Tables Source to Target</h3>
                <p className="text-sm text-foreground-muted">
                  Review and manage table mappings. Source and target tables are displayed in separate sections.
                </p>
              </div>
              {selectedTables.length > 0 && targetTables.length < selectedTables.length && (
                <Button
                  variant="outline"
                  onClick={() => {
                    selectedTables.forEach((table) => {
                      if (!targetTables.includes(table)) {
                        let targetName = table
                        // Automatically copy schema when auto-mapping
                        targetName = copySchemaToTarget(table, targetName)
                        setTableMapping((prev) => ({ ...prev, [table]: targetName }))
                        if (!targetTables.includes(targetName)) {
                          setTargetTables((prev) => [...prev, targetName])
                        }
                      }
                    })
                  }}
                  className="bg-transparent border-border hover:bg-surface-hover"
                >
                  Auto-map All Tables
                </Button>
              )}
            </div>

            {selectedTables.length === 0 ? (
              <div className="text-center py-12 text-foreground-muted bg-surface-hover rounded-lg border border-border flex-shrink-0">
                <p className="text-sm">No tables selected. Go back to Step 2 to select tables.</p>
              </div>
            ) : (
              <div className="grid grid-cols-2 gap-6 flex-1 min-h-0 overflow-hidden">
                {/* Source Tables Section */}
                <div className="flex flex-col border-2 border-border rounded-lg overflow-hidden bg-surface-hover/30">
                  <div className="bg-surface-hover border-b-2 border-border px-4 py-3 flex-shrink-0">
                    <div className="flex items-center gap-2">
                      <Database className="w-5 h-5 text-primary" />
                      <h4 className="font-bold text-foreground text-base">Source Tables</h4>
                      <span className="ml-auto text-xs text-foreground-muted bg-surface px-2 py-1 rounded">
                        {selectedTables.length} table{selectedTables.length !== 1 ? 's' : ''}
                      </span>
                    </div>
                    {formData.sourceConnection && (
                      <p className="text-xs text-foreground-muted mt-1">
                        Connection: <span className="font-semibold text-foreground">{formData.sourceConnection}</span>
                      </p>
                    )}
                  </div>
                  <div className="flex-1 overflow-y-auto p-4 space-y-3 min-h-0">
                    {selectedTables.map((sourceTable) => {
                      const sourceSchema = tableSchemas[sourceTable]
                      const mappedTarget = tableMapping[sourceTable]
                      const isMapped = !!mappedTarget
                      
                      return (
                        <Card
                          key={sourceTable}
                          draggable
                          onDragStart={(e) => {
                            handleDragStart(sourceTable)
                            e.dataTransfer.effectAllowed = 'move'
                          }}
                          className={`p-4 border-2 transition-all cursor-move ${
                            isMapped 
                              ? 'bg-primary/10 border-primary/50 shadow-md' 
                              : 'bg-surface border-border hover:border-primary/30 hover:shadow-md'
                          }`}
                        >
                            <div className="space-y-2">
                            <div className="flex items-start justify-between gap-2">
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2">
                                  <GripVertical className="w-4 h-4 text-foreground-muted flex-shrink-0" />
                                  <div className="font-bold text-foreground text-sm mb-1">{sourceTable}</div>
                                </div>
                                {sourceSchema && (
                                  <div className="text-xs text-foreground-muted space-y-0.5">
                                    <div>
                                      {sourceSchema.columns?.length || 0} columns
                                      {sourceSchema.schema && (
                                        <span className="ml-2">• Schema: <span className="font-semibold">{sourceSchema.schema}</span></span>
                                      )}
                                    </div>
                                  </div>
                                )}
                              </div>
                              {isMapped && (
                                <div className="flex-shrink-0">
                                  <div className="w-2 h-2 rounded-full bg-primary animate-pulse"></div>
                                </div>
                              )}
                            </div>
                            {isMapped && (
                              <div className="pt-2 border-t border-border/50">
                                <div className="text-xs text-primary font-semibold flex items-center gap-1">
                                  <ChevronRight className="w-3 h-3" />
                                  Mapped to: <span className="font-bold">{mappedTarget}</span>
                                </div>
                              </div>
                            )}
                          </div>
                        </Card>
                      )
                    })}
                  </div>
                </div>

                {/* Target Tables Section */}
                <div 
                  className="flex flex-col border-2 border-primary/50 rounded-lg overflow-hidden bg-primary/5"
                  onDragOver={handleDragOver}
                  onDrop={handleDrop}
                >
                  <div className="bg-primary/10 border-b-2 border-primary/50 px-4 py-3 flex-shrink-0">
                    <div className="flex items-center gap-2">
                      <Database className="w-5 h-5 text-primary" />
                      <h4 className="font-bold text-foreground text-base">Target Tables</h4>
                      <span className="ml-auto text-xs text-foreground-muted bg-primary/20 px-2 py-1 rounded">
                        {targetTables.length} table{targetTables.length !== 1 ? 's' : ''}
                      </span>
                    </div>
                    {formData.targetConnection && (
                      <p className="text-xs text-foreground-muted mt-1">
                        Connection: <span className="font-semibold text-foreground">{formData.targetConnection}</span>
                      </p>
                    )}
                    {draggedTable && (
                      <p className="text-xs text-primary font-semibold mt-2 animate-pulse">
                        Drop "{draggedTable}" here to map
                      </p>
                    )}
                  </div>
                  <div 
                    className={`flex-1 overflow-y-auto p-4 space-y-3 min-h-0 transition-all ${
                      draggedTable ? 'bg-primary/10 border-2 border-dashed border-primary/50' : ''
                    }`}
                    onDragOver={handleDragOver}
                    onDrop={handleDrop}
                  >
                    {targetTables.length === 0 ? (
                      <div className="text-center py-12 text-foreground-muted">
                        <p className="text-sm italic">No target tables mapped yet</p>
                        <p className="text-xs mt-2">Use "Auto-map All Tables" or map individually</p>
                      </div>
                    ) : (
                      targetTables.map((targetTable) => {
                        // Find the source table that maps to this target
                        const sourceTable = Object.keys(tableMapping).find(
                          (key) => tableMapping[key] === targetTable || key === targetTable
                        ) || targetTable
                        const targetParsed = parseTableName(targetTable)
                        
                        return (
                          <Card
                            key={targetTable}
                            className="p-4 border-2 border-primary/50 bg-primary/10 hover:border-primary/70 transition-all shadow-sm"
                          >
                            <div className="space-y-2">
                              <div className="flex items-start justify-between gap-2">
                                <div className="flex-1 min-w-0">
                                  <div className="font-bold text-foreground text-sm mb-1">{targetTable}</div>
                                  {targetParsed.schema && (
                                    <div className="text-xs text-foreground-muted">
                                      Schema: <span className="font-semibold">{targetParsed.schema}</span>
                                    </div>
                                  )}
                                  {targetTable !== sourceTable && (
                                    <div className="text-xs text-primary mt-1 font-medium">
                                      ← From: {sourceTable}
                                    </div>
                                  )}
                                </div>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() => {
                                    // Remove from target tables
                                    setTargetTables(targetTables.filter(t => {
                                      const mappedTarget = tableMapping[sourceTable]
                                      return t !== mappedTarget && t !== sourceTable
                                    }))
                                    // Remove from table mapping
                                    setTableMapping((prev) => {
                                      const newMap = { ...prev }
                                      delete newMap[sourceTable]
                                      return newMap
                                    })
                                  }}
                                  className="bg-error/10 border-error/30 text-error hover:bg-error/20 hover:border-error/50 hover:text-error flex-shrink-0"
                                  title="Remove mapping"
                                >
                                  <Trash2 className="w-4 h-4" />
                                </Button>
                              </div>
                            </div>
                          </Card>
                        )
                      })
                    )}
                  </div>
                </div>
              </div>
            )}

            {draggedTable && (
              <div className="p-3 bg-primary/10 border border-primary/30 rounded text-sm text-primary flex-shrink-0">
                Dragging "{draggedTable}" - Drop it in the target area
              </div>
            )}
          </div>
        )}

        {/* Step 4: Configure Target Tables */}
        {step === 4 && (
          <div className="space-y-4 flex flex-col h-full overflow-hidden">
            <div className="flex-shrink-0">
              <div className="flex items-center justify-between mb-2">
                <div>
                  <h3 className="text-xl font-bold text-foreground mb-2">Step 4: Configure Target Tables</h3>
                  <p className="text-sm text-foreground-muted">
                    Edit target table names and review source and target table schemas side by side.
                  </p>
                </div>
                {targetConnection && (
                  <Button
                    onClick={handleLoadTargetTables}
                    disabled={loadingTargetTables}
                    variant="outline"
                    className="bg-primary/10 border-primary/30 hover:bg-primary/20 gap-2"
                    title="Load existing tables from target connection"
                  >
                    {loadingTargetTables ? (
                      <>
                        <Loader2 className="w-4 h-4 animate-spin" />
                        Loading...
                      </>
                    ) : (
                      <>
                        <Database className="w-4 h-4" />
                        Load Target Tables
                      </>
                    )}
                  </Button>
                )}
              </div>
              {targetAvailableTables.length > 0 && (
                <div className="mt-3 p-3 bg-info/10 border border-info/30 rounded text-sm text-foreground">
                  <p className="font-semibold">Found {targetAvailableTables.length} existing table{targetAvailableTables.length !== 1 ? 's' : ''} in target connection</p>
                  <p className="text-xs text-foreground-muted mt-1">
                    These tables are available in your target database. You can map source tables to these existing tables or create new ones.
                  </p>
                </div>
              )}
            </div>

            <div className="space-y-4 flex-1 overflow-y-auto pr-2 min-h-0">
              {selectedTables.length === 0 ? (
                <div className="text-center py-12 text-foreground-muted bg-surface-hover rounded-lg border border-border">
                  <p>No tables selected. Go back to Step 2 to select tables.</p>
                </div>
              ) : (
                selectedTables.map((sourceTable) => {
                  // Get source schema - check both full_name and name for Oracle tables
                  const schema = tableSchemas[sourceTable] || tableSchemas[sourceTable.split('.').pop() || sourceTable]
                  const mappedTarget = tableMapping[sourceTable] || sourceTable
                  
                  // Get target schema - check both full_name and name, and also check if target table exists in target connection
                  let targetSchema = targetTableSchemas[mappedTarget]
                  if (!targetSchema) {
                    // Try to find by just table name (without schema)
                    const targetParsed = parseTableName(mappedTarget)
                    targetSchema = targetTableSchemas[targetParsed.table] || targetTableSchemas[mappedTarget]
                  }
                  
                  // Use source schema for target if target schema doesn't exist yet
                  const displayTargetSchema = targetSchema || schema
                  
                  return (
                    <Card key={sourceTable} className="bg-surface border-2 border-border p-6 shadow-lg">
                      <div className="space-y-6">
                        {/* Header Section */}
                        <div className="flex items-start justify-between pb-4 border-b border-border">
                          <div className="flex-1 space-y-2">
                            <div className="flex items-center gap-3">
                              <Database className="w-5 h-5 text-primary" />
                              <div>
                                <Label className="text-base font-bold text-foreground">
                                  Source Table
                                </Label>
                                <p className="text-sm font-medium text-foreground mt-1">{sourceTable}</p>
                              </div>
                            </div>
                            {schema && (
                              <div className="flex items-center gap-4 text-xs text-foreground-muted ml-8">
                                <span>Schema: <span className="font-semibold text-foreground">{schema.schema || "default"}</span></span>
                                <span>•</span>
                                <span>Columns: <span className="font-semibold text-foreground">{schema.columns?.length || 0}</span></span>
                                {formData.sourceConnection && (
                                  <>
                                    <span>•</span>
                                    <span>Connection: <span className="font-semibold text-foreground">{formData.sourceConnection}</span></span>
                                  </>
                                )}
                              </div>
                            )}
                          </div>
                          <div className="flex items-center gap-2">
                            <Button
                              size="sm"
                              variant="outline"
                              className="bg-transparent border-border hover:bg-surface-hover gap-2"
                              onClick={() => handleCopySchema(sourceTable)}
                              title="Copy schema to clipboard"
                            >
                              <Copy className="w-4 h-4" />
                              Copy to Clipboard
                            </Button>
                            <Button
                              size="sm"
                              variant="ghost"
                              className="text-error hover:text-error hover:bg-error/10 gap-2"
                              onClick={() => {
                                const mappedTarget = tableMapping[sourceTable] || sourceTable
                                setSelectedTables(selectedTables.filter(t => t !== sourceTable))
                                setTargetTables(targetTables.filter(t => {
                                  return t !== mappedTarget && t !== sourceTable
                                }))
                                setTableMapping((prev) => {
                                  const newMap = { ...prev }
                                  delete newMap[sourceTable]
                                  return newMap
                                })
                                setTargetTableSchemas((prev) => {
                                  const newMap = { ...prev }
                                  delete newMap[mappedTarget]
                                  return newMap
                                })
                              }}
                              title="Remove this table mapping"
                            >
                              <Trash2 className="w-4 h-4" />
                              Remove
                            </Button>
                          </div>
                        </div>

                        {/* Target Table Configuration */}
                        <div className="space-y-3 bg-surface-hover/50 p-4 rounded-lg border border-border">
                          <div>
                            <Label className="text-sm font-semibold text-foreground mb-2 block">
                              Target Table Name *
                            </Label>
                            <div className="flex gap-3">
                              {/* Show dropdown if target tables are available */}
                              {targetAvailableTables.length > 0 && (
                                <Select
                                  value={(() => {
                                    const mapped = tableMapping[sourceTable] || sourceTable
                                    const parsed = parseTableName(mapped)
                                    // Try to find matching table in available tables
                                    const matchingTable = targetAvailableTables.find(t => 
                                      (t.full_name || t.name) === mapped || 
                                      t.name === parsed.table ||
                                      (t.full_name && t.full_name.includes(parsed.table))
                                    )
                                    return matchingTable ? (matchingTable.full_name || matchingTable.name) : ""
                                  })()}
                                  onValueChange={(value) => {
                                    if (value === "__new__") {
                                      // User wants to create new table - clear selection
                                      return
                                    }
                                    const selectedTable = targetAvailableTables.find(t => 
                                      (t.full_name || t.name) === value
                                    )
                                    if (selectedTable) {
                                      const newTarget = selectedTable.full_name || selectedTable.name
                                      handleTableRename(sourceTable, newTarget)
                                      // Update target schema if available
                                      if (selectedTable.columns) {
                                        setTargetTableSchemas((prev) => ({
                                          ...prev,
                                          [newTarget]: selectedTable
                                        }))
                                      }
                                    }
                                  }}
                                >
                                  <SelectTrigger className="flex-1 text-base">
                                    <SelectValue placeholder="Select existing table or create new" />
                                  </SelectTrigger>
                                  <SelectContent className="bg-surface border-border max-h-60">
                                    <SelectItem value="__new__">+ Create New Table</SelectItem>
                                    {targetAvailableTables.map((table: any) => (
                                      <SelectItem 
                                        key={table.full_name || table.name} 
                                        value={table.full_name || table.name}
                                      >
                                        {table.full_name || table.name} 
                                        {table.columns && ` (${table.columns.length} columns)`}
                                      </SelectItem>
                                    ))}
                                  </SelectContent>
                                </Select>
                              )}
                              <Input
                                value={(() => {
                                  const mapped = tableMapping[sourceTable] || sourceTable
                                  const parsed = parseTableName(mapped)
                                  // If dropdown is shown and a table is selected, show empty for new table input
                                  if (targetAvailableTables.length > 0) {
                                    const matchingTable = targetAvailableTables.find(t => 
                                      (t.full_name || t.name) === mapped
                                    )
                                    if (matchingTable) {
                                      return "" // Show empty when existing table is selected
                                    }
                                  }
                                  return parsed.table
                                })()}
                                onChange={(e) => {
                                  const newTableName = e.target.value.trim()
                                  const oldTarget = tableMapping[sourceTable] || sourceTable
                                  
                                  // Determine target schema based on target connection type
                                  const targetConn = connections.find(c => c.name === formData.targetConnection)
                                  const targetDbType = targetConn?.connection_type || targetConn?.database_type
                                  const defaultTargetSchema = getDefaultSchemaForDbType(targetDbType)
                                  
                                  // Check if user typed a schema (contains '.')
                                  let newTarget = newTableName
                                  const targetParsed = parseTableName(newTableName)
                                  if (targetParsed.schema && targetParsed.schema !== "undefined") {
                                    // User specified a schema - use it as-is
                                    newTarget = newTableName
                                  } else if (newTableName && !newTableName.includes('.')) {
                                    // No schema specified - use target's default schema
                                    newTarget = `${defaultTargetSchema}.${newTableName}`
                                  }
                                  
                                  handleTableRename(sourceTable, newTarget)
                                  if (targetTableSchemas[oldTarget]) {
                                    setTargetTableSchemas((prev) => {
                                      const newMap = { ...prev }
                                      newMap[newTarget] = newMap[oldTarget]
                                      delete newMap[oldTarget]
                                      return newMap
                                    })
                                  }
                                }}
                                className="flex-1 text-base"
                                placeholder={targetAvailableTables.length > 0 ? "Or enter new table name" : "Enter target table name"}
                              />
                              {(() => {
                                // Only show Copy Schema button if schema hasn't been copied yet
                                const sourceParsed = parseTableName(sourceTable)
                                if (sourceParsed.schema && sourceParsed.schema !== "undefined" && schema && !targetSchema) {
                                  return (
                                    <Button
                                      type="button"
                                      variant="outline"
                                      size="default"
                                      onClick={() => {
                                        const sourceParsed = parseTableName(sourceTable)
                                        const currentTarget = tableMapping[sourceTable] || sourceTable
                                        const targetParsed = parseTableName(currentTarget)
                                        
                                        // Construct target name with schema
                                        let newTarget = currentTarget
                                        if (sourceParsed.schema && sourceParsed.schema !== "undefined") {
                                          if (!targetParsed.schema || targetParsed.schema === "undefined") {
                                            newTarget = `${sourceParsed.schema}.${targetParsed.table || sourceParsed.table}`
                                          }
                                        }
                                        
                                        setSchemaConfirmData({
                                          sourceTable: sourceTable,
                                          targetTable: newTarget,
                                          sourceSchema: schema
                                        })
                                        setShowSchemaConfirm(true)
                                      }}
                                      className="bg-primary/10 border-primary/30 text-primary hover:bg-primary/20 hover:border-primary/50 whitespace-nowrap gap-2"
                                      title="Manually copy schema (schema is automatically copied when mapping tables)"
                                    >
                                      <Copy className="w-4 h-4" />
                                      Copy Schema
                                    </Button>
                                  )
                                }
                                return null
                              })()}
                            </div>
                            {(() => {
                              const mapped = tableMapping[sourceTable] || sourceTable
                              const sourceParsed = parseTableName(sourceTable)
                              const targetParsed = parseTableName(mapped)
                              if (sourceParsed.schema && targetParsed.schema) {
                                return (
                                  <p className="text-xs text-foreground-muted mt-2 ml-1">
                                    Full target name: <span className="font-mono font-semibold text-foreground">{mapped}</span>
                                    <span className="ml-2">(Schema: {targetParsed.schema}, Table: {targetParsed.table})</span>
                                  </p>
                                )
                              } else if (targetParsed.schema) {
                                return (
                                  <p className="text-xs text-foreground-muted mt-2 ml-1">
                                    Full target name: <span className="font-mono font-semibold text-foreground">{mapped}</span>
                                    <span className="ml-2">(Schema: {targetParsed.schema})</span>
                                  </p>
                                )
                              }
                              return null
                            })()}
                          </div>
                        </div>

                        {/* Source and Target Schema Display Side by Side */}
                        <div>
                          <div className="flex items-center justify-between mb-4">
                            <h4 className="text-sm font-bold text-foreground uppercase tracking-wide">Schema Comparison</h4>
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
                          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-6">
                            {/* Source Schema */}
                            <div className="space-y-3">
                              <div className="flex items-center gap-2 pb-2 border-b border-border">
                                <Database className="w-4 h-4 text-foreground-muted" />
                                <p className="text-sm font-bold text-foreground">Source Table Schema</p>
                              </div>
                              {schema && schema.columns && schema.columns.length > 0 ? (
                                <div className="text-sm p-4 bg-surface-hover rounded-lg border-2 border-border shadow-inner">
                                  <div className="space-y-2 max-h-80 overflow-y-auto">
                                    {schema.columns.map((col: any, idx: number) => (
                                      <div key={idx} className="flex items-start gap-3 text-foreground py-2 px-3 bg-surface rounded border border-border/50 hover:border-border transition-colors">
                                        <span className="font-mono text-sm font-semibold text-primary flex-shrink-0 w-32">{col.name}</span>
                                        <div className="flex-1 flex items-center gap-2 flex-wrap">
                                          <span className="text-xs px-2 py-1 bg-surface-hover rounded border border-border/50">{col.type}</span>
                                          {col.nullable && (
                                            <span className="text-xs px-2 py-1 bg-primary/10 text-primary rounded border border-primary/30">nullable</span>
                                          )}
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                  <div className="mt-3 pt-3 border-t border-border text-xs text-foreground-muted flex-shrink-0">
                                    <p className="font-semibold text-foreground">Total: {schema.columns.length} columns</p>
                                  </div>
                                </div>
                              ) : (
                                <div className="text-sm text-foreground-muted p-6 bg-surface-hover rounded-lg border-2 border-dashed border-border text-center">
                                  <p className="font-medium">Schema information not available</p>
                                  <p className="text-xs mt-1">Will be detected during pipeline execution</p>
                                </div>
                              )}
                            </div>

                            {/* Target Schema */}
                            <div className="space-y-3 flex flex-col min-h-0">
                              <div className="flex items-center justify-between gap-2 pb-2 border-b border-border flex-shrink-0 flex-wrap">
                                <div className="flex items-center gap-2">
                                  <Database className="w-4 h-4 text-primary" />
                                  <p className="text-sm font-bold text-foreground">Target Table Schema</p>
                                  {targetSchema && (
                                    <span className="text-xs px-2 py-1 bg-primary/20 text-primary rounded border border-primary/50 font-semibold">
                                      ✓ Copied
                                    </span>
                                  )}
                                </div>
                                <div className="flex items-center gap-2 flex-wrap">
                                  {!targetSchema && schema && (
                                    <Button
                                      type="button"
                                      variant="outline"
                                      size="sm"
                                      onClick={() => {
                                        const sourceParsed = parseTableName(sourceTable)
                                        const currentTarget = tableMapping[sourceTable] || sourceTable
                                        const targetParsed = parseTableName(currentTarget)
                                        
                                        // Construct target table name - only include schema if source has one
                                        let newTarget: string
                                        if (sourceParsed.schema) {
                                          // Use source schema with target table name
                                          newTarget = `${sourceParsed.schema}.${targetParsed.table || sourceParsed.table}`
                                        } else {
                                          // No schema, just use table name
                                          newTarget = targetParsed.table || sourceParsed.table
                                        }
                                        
                                        // Confirm and copy schema
                                        if (schema) {
                                          const targetSchemaName = sourceParsed.schema || targetParsed.schema || schema.schema
                                          setTargetTableSchemas((prev) => ({
                                            ...prev,
                                            [newTarget]: {
                                              ...schema,
                                              schema: targetSchemaName || undefined,
                                              full_name: newTarget
                                            }
                                          }))
                                          handleTableRename(sourceTable, newTarget)
                                        }
                                      }}
                                      className="bg-primary hover:bg-primary/90 text-foreground gap-1"
                                      title="Confirm schema copy"
                                    >
                                      <Check className="w-3 h-3" />
                                      Confirm
                                    </Button>
                                  )}
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const mappedTarget = tableMapping[sourceTable] || sourceTable
                                      setNewFieldInputs((prev) => ({
                                        ...prev,
                                        [mappedTarget]: { name: '', type: 'VARCHAR', nullable: false }
                                      }))
                                    }}
                                    className="bg-transparent border-border hover:bg-surface-hover gap-1"
                                    title="Add new field to target schema"
                                  >
                                    <Plus className="w-3 h-3" />
                                    Add Field
                                  </Button>
                                </div>
                              </div>
                              {displayTargetSchema && displayTargetSchema.columns && displayTargetSchema.columns.length > 0 ? (
                                <div className={`text-sm p-4 rounded-lg border-2 shadow-inner flex-1 min-h-0 flex flex-col ${
                                  targetSchema 
                                    ? 'bg-primary/5 border-primary/40' 
                                    : 'bg-surface-hover border-border'
                                }`}>
                                  <div className="space-y-2 flex-1 overflow-y-auto min-h-0">
                                    {displayTargetSchema.columns.map((col: any, idx: number) => (
                                      <div key={idx} className={`flex items-start gap-3 text-foreground py-2 px-3 rounded border transition-colors ${
                                        targetSchema
                                          ? 'bg-primary/10 border-primary/30 hover:border-primary/50'
                                          : 'bg-surface border-border/50 hover:border-border'
                                      }`}>
                                        <span className={`font-mono text-sm font-semibold flex-shrink-0 w-32 ${
                                          targetSchema ? 'text-primary' : 'text-foreground-muted'
                                        }`}>{col.name}</span>
                                        <div className="flex-1 flex items-center gap-2 flex-wrap">
                                          <span className={`text-xs px-2 py-1 rounded border ${
                                            targetSchema
                                              ? 'bg-primary/20 border-primary/40 text-primary'
                                              : 'bg-surface-hover border-border/50'
                                          }`}>{col.type}</span>
                                          {col.nullable && (
                                            <span className="text-xs px-2 py-1 bg-primary/20 text-primary rounded border border-primary/40">nullable</span>
                                          )}
                                        </div>
                                      </div>
                                    ))}
                                    {/* Add Field Input */}
                                    {(() => {
                                      const mappedTarget = tableMapping[sourceTable] || sourceTable
                                      const newField = newFieldInputs[mappedTarget]
                                      if (newField) {
                                        return (
                                          <div className="flex items-start gap-3 text-foreground py-2 px-3 rounded border-2 border-dashed border-primary/50 bg-primary/5">
                                            <Input
                                              placeholder="Field name"
                                              value={newField.name}
                                              onChange={(e) => {
                                                setNewFieldInputs((prev) => ({
                                                  ...prev,
                                                  [mappedTarget]: { ...prev[mappedTarget], name: e.target.value }
                                                }))
                                              }}
                                              className="flex-1 text-xs h-8"
                                            />
                                            <Select
                                              value={newField.type}
                                              onValueChange={(value) => {
                                                setNewFieldInputs((prev) => ({
                                                  ...prev,
                                                  [mappedTarget]: { ...prev[mappedTarget], type: value }
                                                }))
                                              }}
                                            >
                                              <SelectTrigger className="w-32 h-8 text-xs">
                                                <SelectValue />
                                              </SelectTrigger>
                                              <SelectContent>
                                                <SelectItem value="VARCHAR">VARCHAR</SelectItem>
                                                <SelectItem value="INTEGER">INTEGER</SelectItem>
                                                <SelectItem value="BIGINT">BIGINT</SelectItem>
                                                <SelectItem value="DECIMAL">DECIMAL</SelectItem>
                                                <SelectItem value="DATE">DATE</SelectItem>
                                                <SelectItem value="TIMESTAMP">TIMESTAMP</SelectItem>
                                                <SelectItem value="BOOLEAN">BOOLEAN</SelectItem>
                                                <SelectItem value="TEXT">TEXT</SelectItem>
                                              </SelectContent>
                                            </Select>
                                            <label className="flex items-center gap-1 px-2 py-1 border border-border rounded text-xs cursor-pointer hover:bg-surface-hover">
                                              <input
                                                type="checkbox"
                                                id={`nullable-${mappedTarget}`}
                                                checked={newField.nullable}
                                                onChange={(e) => {
                                                  setNewFieldInputs((prev) => ({
                                                    ...prev,
                                                    [mappedTarget]: { ...prev[mappedTarget], nullable: e.target.checked }
                                                  }))
                                                }}
                                                className="mr-1"
                                              />
                                              Nullable
                                            </label>
                                            <Button
                                              type="button"
                                              variant="outline"
                                              size="sm"
                                              onClick={() => {
                                                if (newField.name.trim()) {
                                                  const mappedTarget = tableMapping[sourceTable] || sourceTable
                                                  const currentSchema = targetTableSchemas[mappedTarget] || displayTargetSchema
                                                  const updatedColumns = [...(currentSchema.columns || []), {
                                                    name: newField.name.trim(),
                                                    type: newField.type,
                                                    nullable: newField.nullable
                                                  }]
                                                  setTargetTableSchemas((prev) => ({
                                                    ...prev,
                                                    [mappedTarget]: {
                                                      ...currentSchema,
                                                      columns: updatedColumns
                                                    }
                                                  }))
                                                  setNewFieldInputs((prev) => {
                                                    const newInputs = { ...prev }
                                                    delete newInputs[mappedTarget]
                                                    return newInputs
                                                  })
                                                }
                                              }}
                                              className="bg-primary hover:bg-primary/90 text-foreground h-8"
                                              disabled={!newField.name.trim()}
                                            >
                                              <Check className="w-3 h-3" />
                                            </Button>
                                            <Button
                                              type="button"
                                              variant="outline"
                                              size="sm"
                                              onClick={() => {
                                                setNewFieldInputs((prev) => {
                                                  const newInputs = { ...prev }
                                                  delete newInputs[mappedTarget]
                                                  return newInputs
                                                })
                                              }}
                                              className="h-8"
                                            >
                                              <X className="w-3 h-3" />
                                            </Button>
                                          </div>
                                        )
                                      }
                                      return null
                                    })()}
                                  </div>
                                  <div className={`mt-3 pt-3 border-t flex-shrink-0 ${
                                    targetSchema ? 'border-primary/30' : 'border-border'
                                  } text-xs`}>
                                    <p className={`font-semibold ${
                                      targetSchema ? 'text-primary' : 'text-foreground-muted'
                                    }`}>
                                      Total: {displayTargetSchema.columns.length} columns
                                      {!targetSchema && schema && (
                                        <span className="ml-2 text-xs font-normal italic">
                                          (showing source schema - click "Copy Schema" to confirm)
                                        </span>
                                      )}
                                    </p>
                                  </div>
                                </div>
                              ) : (
                                <div className="text-sm text-foreground-muted p-6 bg-surface-hover rounded-lg border-2 border-dashed border-border text-center">
                                  <p className="font-medium">No schema available</p>
                                  <p className="text-xs mt-1">Click "Copy Schema" to copy from source</p>
                                </div>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    </Card>
                  )
                })
              )}
            </div>
          </div>
        )}

        {/* Step 5: Schedule */}
        {step === 5 && (
          <div className="space-y-4 flex-1 overflow-y-auto min-h-0">
            <h3 className="text-lg font-semibold text-foreground">Step 5: Configure Schedule</h3>

            <Card className="bg-surface border-border p-4 space-y-3">
              <div>
                <Label className="text-foreground">Replication Schedule</Label>
                <Select value={formData.schedule} onValueChange={(value) => handleInputChange("schedule", value)}>
                  <SelectTrigger className="mt-1 bg-input border-border">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent className="bg-surface border-border">
                    <SelectItem value="immediate">Immediate Start</SelectItem>
                    <SelectItem value="hourly">Every Hour</SelectItem>
                    <SelectItem value="every_6h">Every 6 Hours</SelectItem>
                    <SelectItem value="daily">Daily at 2 AM</SelectItem>
                    <SelectItem value="weekly">Weekly (Monday)</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="p-3 bg-primary/10 border border-primary/30 rounded">
                <p className="text-sm text-foreground-muted">
                  <span className="font-semibold text-foreground">Pipeline Summary:</span>
                </p>
                <ul className="text-sm text-foreground mt-2 space-y-1">
                  <li>
                    • <span className="font-medium">{formData.name}</span>
                  </li>
                  <li>
                    • Tables: <span className="font-medium">{selectedTables.length}</span>
                  </li>
                  <li>
                    • Mode: <span className="font-medium">{formData.mode.replace(/_/g, " ")}</span>
                  </li>
                  <li>
                    • Schedule: <span className="font-medium">{formData.schedule}</span>
                  </li>
                </ul>
              </div>
            </Card>
          </div>
        )}
        </div>

        {/* Navigation Buttons */}
        <div className="flex justify-between gap-3 pt-4 flex-shrink-0 border-t border-border">
          <Button
            variant="outline"
            onClick={() => (step === 1 ? onClose() : setStep(step - 1))}
            className="bg-transparent border-border hover:bg-surface-hover gap-2"
          >
            <ChevronLeft className="w-4 h-4" />
            {step === 1 ? "Cancel" : "Back"}
          </Button>

          <div className="flex gap-2">
            {step < 5 && (
              <Button
                onClick={() => setStep(step + 1)}
                disabled={!canProceedToNext()}
                className="bg-primary hover:bg-primary/90 text-foreground gap-2"
              >
                Next
                <ChevronRight className="w-4 h-4" />
              </Button>
            )}
            {step === 5 && (
              <Button onClick={handleSave} className="bg-primary hover:bg-primary/90 text-foreground">
                {editingPipeline ? "Update Pipeline" : "Create Pipeline"}
              </Button>
            )}
          </div>
        </div>
      </DialogContent>

      {/* Schema Copy Confirmation Dialog */}
      <Dialog open={showSchemaConfirm} onOpenChange={setShowSchemaConfirm}>
        <DialogContent className="max-w-[95%] w-[95%] lg:max-w-[80%] lg:w-[80%] bg-surface border-border max-h-[94vh] h-[94vh] overflow-hidden p-6 flex flex-col">
          <DialogHeader className="flex-shrink-0">
            <DialogTitle className="text-foreground text-xl">Confirm Schema Copy</DialogTitle>
          </DialogHeader>
          
          {schemaConfirmData && (
            <div className="space-y-4 flex-1 min-h-0 flex flex-col">
              <p className="text-sm text-foreground-muted flex-shrink-0">
                Please review the source and target table information before confirming the schema copy.
              </p>
              
              {/* Side by side comparison table */}
              <div className="border border-border rounded-lg overflow-hidden flex-1 min-h-0 flex flex-col">
                <table className="w-full border-collapse">
                  <thead>
                    <tr className="bg-surface-hover border-b border-border">
                      <th className="px-4 py-3 text-left text-sm font-semibold text-foreground border-r border-border w-1/2">
                        Source Table
                      </th>
                      <th className="px-4 py-3 text-left text-sm font-semibold text-foreground w-1/2">
                        Target Table
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {/* Table Name Row */}
                    <tr className="border-b border-border">
                      <td className="px-4 py-3 text-sm text-foreground border-r border-border bg-surface-hover/50">
                        <div className="font-medium">{schemaConfirmData.sourceTable}</div>
                        <div className="text-xs text-foreground-muted mt-1">
                          {(() => {
                            const parsed = parseTableName(schemaConfirmData.sourceTable)
                            return parsed.schema ? `Schema: ${parsed.schema}` : 'No schema'
                          })()}
                        </div>
                      </td>
                      <td className="px-4 py-3 text-sm text-foreground bg-surface-hover/50">
                        <div className="font-medium">{schemaConfirmData.targetTable}</div>
                        <div className="text-xs text-foreground-muted mt-1">
                          {(() => {
                            const parsed = parseTableName(schemaConfirmData.targetTable)
                            return parsed.schema ? `Schema: ${parsed.schema}` : 'No schema'
                          })()}
                        </div>
                      </td>
                    </tr>
                    
                    {/* Connection Info Row */}
                    <tr className="border-b border-border">
                      <td className="px-4 py-3 text-sm text-foreground border-r border-border">
                        <div className="text-xs text-foreground-muted mb-1">Connection:</div>
                        <div className="font-medium">{formData.sourceConnection || 'N/A'}</div>
                      </td>
                      <td className="px-4 py-3 text-sm text-foreground">
                        <div className="text-xs text-foreground-muted mb-1">Connection:</div>
                        <div className="font-medium">{formData.targetConnection || 'N/A'}</div>
                      </td>
                    </tr>
                    
                    {/* Column Count Row */}
                    <tr className="border-b border-border">
                      <td className="px-4 py-3 text-sm text-foreground border-r border-border">
                        <div className="text-xs text-foreground-muted mb-1">Columns:</div>
                        <div className="font-medium">
                          {schemaConfirmData.sourceSchema?.columns?.length || 0} columns
                        </div>
                      </td>
                      <td className="px-4 py-3 text-sm text-foreground">
                        <div className="text-xs text-foreground-muted mb-1">Columns (will be copied):</div>
                        <div className="font-medium">
                          {schemaConfirmData.sourceSchema?.columns?.length || 0} columns
                        </div>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              {/* Column Details Table */}
              {schemaConfirmData.sourceSchema?.columns && schemaConfirmData.sourceSchema.columns.length > 0 && (
                <div className="space-y-2 flex-1 min-h-0 flex flex-col">
                  <h4 className="text-sm font-semibold text-foreground flex-shrink-0">Column Details</h4>
                  <div className="border border-border rounded-lg overflow-hidden flex-1 min-h-0 flex flex-col">
                    <div className="flex-1 overflow-y-auto min-h-0">
                      <table className="w-full border-collapse">
                        <thead className="sticky top-0 bg-surface-hover">
                          <tr className="border-b border-border">
                            <th className="px-4 py-2 text-left text-xs font-semibold text-foreground border-r border-border w-1/2">
                              Source Column
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-semibold text-foreground w-1/2">
                              Target Column (will be created)
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {schemaConfirmData.sourceSchema.columns.map((col: any, idx: number) => (
                            <tr key={idx} className="border-b border-border hover:bg-surface-hover/50">
                              <td className="px-4 py-2 text-xs text-foreground border-r border-border">
                                <div className="font-medium">{col.name}</div>
                                <div className="text-foreground-muted mt-0.5">
                                  {col.type}
                                  {col.nullable && <span className="ml-1">(nullable)</span>}
                                </div>
                              </td>
                              <td className="px-4 py-2 text-xs text-foreground">
                                <div className="font-medium">{col.name}</div>
                                <div className="text-foreground-muted mt-0.5">
                                  {col.type}
                                  {col.nullable && <span className="ml-1">(nullable)</span>}
                                </div>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}

              {/* Action Buttons */}
              <div className="flex justify-end gap-3 pt-4 border-t border-border flex-shrink-0 mt-auto">
                <Button
                  variant="outline"
                  onClick={() => {
                    setShowSchemaConfirm(false)
                    setSchemaConfirmData(null)
                  }}
                  className="bg-transparent border-border hover:bg-surface-hover"
                >
                  Cancel
                </Button>
                <Button
                  onClick={() => {
                    if (schemaConfirmData) {
                      handleTableRename(schemaConfirmData.sourceTable, schemaConfirmData.targetTable)
                      // Store the target schema (copy of source schema)
                      if (schemaConfirmData.sourceSchema) {
                        const targetParsed = parseTableName(schemaConfirmData.targetTable)
                        const sourceParsed = parseTableName(schemaConfirmData.sourceTable)
                        // Use target schema if it exists and is valid, otherwise use source schema, but never "undefined"
                        const targetSchemaName = targetParsed.schema && targetParsed.schema !== "undefined" 
                          ? targetParsed.schema 
                          : (sourceParsed.schema && sourceParsed.schema !== "undefined" ? sourceParsed.schema : undefined)
                        
                        setTargetTableSchemas((prev) => ({
                          ...prev,
                          [schemaConfirmData.targetTable]: {
                            ...schemaConfirmData.sourceSchema,
                            schema: targetSchemaName,
                            full_name: schemaConfirmData.targetTable
                          }
                        }))
                      }
                      setShowSchemaConfirm(false)
                      setSchemaConfirmData(null)
                    }
                  }}
                  className="bg-primary hover:bg-primary/90 text-foreground"
                >
                  Confirm & Copy Schema
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </Dialog>
  )
}
