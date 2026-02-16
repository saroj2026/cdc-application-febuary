"use client"

import { useState, useEffect, useMemo, useRef } from "react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Plus, Edit2, Trash2, TestTube, CheckCircle, AlertCircle, Loader2, AlertTriangle, Database, Search, ChevronLeft, ChevronRight, LayoutGrid, List } from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"
import { ViewToggle } from "@/components/ui/view-toggle"
import { ConnectionModal } from "@/components/connections/connection-modal"
import { ProtectedPage } from "@/components/auth/ProtectedPage"
import { useAppDispatch, useAppSelector } from "@/lib/store/hooks"
import {
  fetchConnections,
  createConnection,
  updateConnection,
  deleteConnection,
  testConnection,
  setSelectedConnection,
} from "@/lib/store/slices/connectionSlice"
import { formatDistanceToNow } from "date-fns"
import { getDatabaseByConnectionType } from "@/lib/database-icons"
import { getDatabaseColor } from "@/lib/database-colors"
import { DatabaseLogo } from "@/lib/database-logo-loader"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"

export default function ConnectionsPage() {
  const dispatch = useAppDispatch()
  const { connections, isLoading, error } = useAppSelector((state) => state.connections)
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [editingConnection, setEditingConnection] = useState<number | null>(null)
  const [testingConnectionId, setTestingConnectionId] = useState<number | null>(null)
  const [searchQuery, setSearchQuery] = useState("")
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false)
  const [connectionToDelete, setConnectionToDelete] = useState<{ id: number; name: string } | null>(null)
  const [currentPage, setCurrentPage] = useState(1)
  const [viewMode, setViewMode] = useState<"grid" | "list">("grid")
  const connectionsPerPage = viewMode === "grid" ? 10 : 20

  const hasFetchedRef = useRef(false)
  useEffect(() => {
    // Prevent multiple simultaneous calls
    if (hasFetchedRef.current || isLoading) return
    hasFetchedRef.current = true

    dispatch(fetchConnections())
  }, [dispatch, isLoading])

  // Filter connections based on search query
  const filteredConnections = useMemo(() => {
    // Ensure connections is an array
    const connectionsArray = Array.isArray(connections) ? connections : []

    if (!searchQuery.trim()) {
      return connectionsArray
    }

    const query = searchQuery.toLowerCase().trim()
    return connectionsArray.filter(connection => {
      const dbInfo = getDatabaseByConnectionType(connection.connection_type)
      const dbName = dbInfo?.displayName || connection.connection_type

      return (
        connection.name?.toLowerCase().includes(query) ||
        connection.connection_type?.toLowerCase().includes(query) ||
        dbName?.toLowerCase().includes(query) ||
        connection.host?.toLowerCase().includes(query) ||
        connection.database?.toLowerCase().includes(query)
      )
    })
  }, [connections, searchQuery])

  // Pagination logic
  const totalPages = Math.ceil(filteredConnections.length / connectionsPerPage)
  const startIndex = (currentPage - 1) * connectionsPerPage
  const endIndex = startIndex + connectionsPerPage
  const paginatedConnections = filteredConnections.slice(startIndex, endIndex)

  // Reset to page 1 when search changes
  useEffect(() => {
    setCurrentPage(1)
  }, [searchQuery, filteredConnections.length])

  const handleAddConnection = async (connectionData: any) => {
    // ... [Original handleAddConnection implementation remains same] ...
    // Note: Copied from original file for brevity in this thought, 
    // but in write_to_file I must include the full logic.
    // Since I'm overwriting, I'll paste the logic back from previous read.

    try {
      if (!connectionData || typeof connectionData !== 'object') {
        alert("Invalid connection data")
        return
      }

      if (!connectionData.name || !String(connectionData.name).trim()) {
        alert("Connection name is required")
        return
      }
      const defaultRole = "source"
      if (!connectionData.engine || !String(connectionData.engine).trim()) {
        alert("Database engine is required")
        return
      }
      if (!connectionData.host || !String(connectionData.host).trim()) {
        alert("Host is required")
        return
      }
      if (!connectionData.database || !String(connectionData.database).trim()) {
        alert("Database name is required")
        return
      }
      if (!connectionData.username || !String(connectionData.username).trim()) {
        alert("Username is required")
        return
      }
      if (!connectionData.password || !String(connectionData.password)) {
        alert("Password is required")
        return
      }

      const databaseTypeMap: Record<string, string> = {
        "mysql": "mysql", "mariadb": "mysql", "postgresql": "postgresql", "postgres": "postgresql",
        "mongodb": "mongodb", "mssql": "sqlserver", "sqlserver": "sqlserver", "azuresql": "sqlserver",
        "oracle": "oracle", "as400": "as400", "aws_s3": "aws_s3", "snowflake": "snowflake", "s3": "s3",
      }

      const engineValue = String(connectionData?.engine || "").toLowerCase().trim()
      const mappedDatabaseType = databaseTypeMap[engineValue] || engineValue

      if (!mappedDatabaseType || !mappedDatabaseType.trim()) {
        alert(`Invalid database engine: ${engineValue}`)
        return
      }

      const mappedRole = "source"
      const nameValue = String(connectionData?.name || "").trim()
      const databaseTypeValue = String(mappedDatabaseType || "").trim()

      const payload: any = {
        name: nameValue,
        database_type: databaseTypeValue,
        connection_type: mappedRole,
        host: String(connectionData.host).trim(),
        port: parseInt(String(connectionData.port || "3306")) || 3306,
        database: String(connectionData.database).trim(),
        username: String(connectionData.username).trim(),
        password: String(connectionData.password),
        ssl_enabled: Boolean(connectionData.ssl_enabled || false),
      }

      if (connectionData?.description) payload.description = String(connectionData.description).trim()
      if (connectionData?.schema_name) payload.schema_name = String(connectionData.schema_name).trim()

      await dispatch(createConnection(payload)).unwrap()
      setIsModalOpen(false)
      setEditingConnection(null)
    } catch (err) {
      console.error("[handleAddConnection] Failed to create connection:", err)
    }
  }

  const handleUpdateConnection = async (connectionData: any) => {
    if (!editingConnection) return
    try {
      const databaseTypeMap: Record<string, string> = {
        "mysql": "mysql", "mariadb": "mysql", "postgresql": "postgresql", "postgres": "postgresql",
        "mongodb": "mongodb", "mssql": "sqlserver", "sqlserver": "sqlserver", "azuresql": "sqlserver",
        "oracle": "oracle", "as400": "as400", "aws_s3": "aws_s3", "snowflake": "snowflake", "s3": "s3",
      }

      const engineValue = String(connectionData?.engine || "").toLowerCase().trim()
      const mappedDatabaseType = databaseTypeMap[engineValue] || engineValue

      await dispatch(updateConnection({
        id: editingConnection,
        data: {
          name: connectionData.name,
          database_type: mappedDatabaseType,
          connection_type: "source",
          description: connectionData.description || "",
          host: connectionData.host,
          port: parseInt(connectionData.port) || 3306,
          database: connectionData.database,
          username: connectionData.username,
          password: connectionData.password,
          ssl_enabled: connectionData.ssl_enabled || false,
        }
      })).unwrap()
      setIsModalOpen(false)
      setEditingConnection(null)
    } catch (err) {
      console.error("Failed to update connection:", err)
    }
  }

  const handleDeleteClick = (id: number, name: string) => {
    setConnectionToDelete({ id, name })
    setDeleteConfirmOpen(true)
  }

  const handleDeleteConnection = async () => {
    if (!connectionToDelete) return
    try {
      await dispatch(deleteConnection(connectionToDelete.id)).unwrap()
      setDeleteConfirmOpen(false)
      setConnectionToDelete(null)
    } catch (err) {
      console.error("Failed to delete connection:", err)
      alert("Failed to delete connection. Please try again.")
    }
  }

  const handleTestConnection = async (id: number) => {
    setTestingConnectionId(id)
    try {
      const result = await dispatch(testConnection(id)).unwrap()
      await new Promise(resolve => setTimeout(resolve, 1000))
      await dispatch(fetchConnections())
      if (result?.message) alert(`Connection test successful: ${result.message}`)
      else alert('Connection test successful!')
    } catch (err: any) {
      await new Promise(resolve => setTimeout(resolve, 1000))
      await dispatch(fetchConnections())
      const errorMessage = typeof err === 'string' ? err : err?.message || 'Connection test failed.'
      alert(`Connection test failed:\n\n${errorMessage}`)
    } finally {
      setTestingConnectionId(null)
    }
  }

  return (
    <ProtectedPage path="/connections" requiredPermission="create_connection">
      <div className="p-6 space-y-6">
        <PageHeader
          title="Database Connections"
          subtitle={`${connections.length} connection${connections.length !== 1 ? 's' : ''} configured`}
          icon={Database}
          action={
            <div className="flex items-center gap-2">
              <ViewToggle view={viewMode} onViewChange={setViewMode} />
              <div className="h-6 w-px bg-border mx-1" />
              <Button
                onClick={() => {
                  setEditingConnection(null)
                  setIsModalOpen(true)
                }}
                className="bg-primary hover:bg-primary/90 text-foreground gap-2"
              >
                <Plus className="w-4 h-4" />
                New Connection
              </Button>
            </div>
          }
        />

        {/* Search Box */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-foreground-muted" />
          <Input
            type="text"
            placeholder="Search connections..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10 bg-surface border-border focus:border-primary w-full max-w-md"
          />
        </div>

        {/* Error Message */}
        {error && (
          <div className="p-4 bg-error/10 border border-error/30 rounded-lg">
            <p className="text-sm text-error">{error}</p>
          </div>
        )}

        {/* Loading State */}
        {isLoading && connections.length === 0 && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
            <span className="ml-2 text-foreground-muted">Loading connections...</span>
          </div>
        )}

        {/* Empty State */}
        {!isLoading && filteredConnections.length === 0 && (
          <div className="text-center py-12">
            <div className="p-4 bg-muted/30 rounded-full w-fit mx-auto mb-4">
              <Database className="w-8 h-8 text-foreground-muted/50" />
            </div>
            <p className="text-foreground-muted mb-4 font-medium">
              {searchQuery.trim() ? "No matching connections found" : "No connections configured yet"}
            </p>
            {connections.length === 0 && (
              <Button
                onClick={() => {
                  setEditingConnection(null)
                  setIsModalOpen(true)
                }}
                variant="outline"
                className="gap-2"
              >
                <Plus className="w-4 h-4" />
                Create First Connection
              </Button>
            )}
          </div>
        )}

        {/* Connection Views */}
        {paginatedConnections.length > 0 && (
          <>
            {viewMode === "grid" ? (
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 2xl:grid-cols-6 gap-4">
                {paginatedConnections.map((connection) => {
                  const isConnected = connection.last_test_status === "success"
                  const isTesting = testingConnectionId === connection.id
                  const dbType = (connection as any).database_type || connection.connection_type
                  const dbInfo = getDatabaseByConnectionType(dbType)
                  const dbColor = getDatabaseColor(dbType)

                  return (
                    <Card
                      key={connection.id}
                      className="group relative overflow-hidden border-2 hover:scale-[1.02] transition-all duration-300 bg-card"
                      style={{
                        borderColor: `${dbColor.primary}30`,
                      }}
                    >
                      {/* Colorful Header Strip */}
                      <div
                        className="h-1.5 w-full"
                        style={{
                          background: `linear-gradient(90deg, ${dbColor.primary}, ${dbColor.secondary})`
                        }}
                      />

                      {/* Connection Indicator */}
                      {isConnected && (
                        <div className="absolute top-4 right-4 z-10">
                          <span className="absolute inline-flex h-2 w-2 rounded-full bg-success opacity-75 animate-ping"></span>
                          <span className="relative inline-flex h-2 w-2 rounded-full bg-success"></span>
                        </div>
                      )}

                      <div className="p-4">
                        <div className="flex items-start gap-3 mb-4">
                          <div
                            className="w-12 h-12 rounded-xl flex items-center justify-center flex-shrink-0 shadow-md border border-white/10"
                            style={{
                              background: `linear-gradient(135deg, ${dbColor.primary}15, ${dbColor.secondary}10)`,
                              borderColor: `${dbColor.primary}20`
                            }}
                          >
                            <DatabaseLogo
                              connectionType={dbType}
                              databaseId={dbInfo?.id}
                              databaseName={connection.name}
                              displayName={dbInfo?.displayName}
                              size={28}
                            />
                          </div>
                          <div className="flex-1 min-w-0 pt-0.5">
                            <h3 className="text-sm font-bold text-foreground truncate group-hover:text-primary transition-colors">
                              {connection.name}
                            </h3>
                            <p className="text-[10px] text-foreground-muted font-medium uppercase tracking-wider mt-0.5">
                              {dbInfo?.displayName || connection.connection_type}
                            </p>
                          </div>
                        </div>

                        {/* Connection Details */}
                        <div className="space-y-1.5 mb-4 text-xs bg-muted/30 p-2.5 rounded-lg border border-border/50">
                          <div className="flex items-center justify-between">
                            <span className="text-foreground-muted">Host</span>
                            <span className="text-foreground font-medium truncate ml-2 max-w-[100px]" title={connection.host}>
                              {connection.host}
                            </span>
                          </div>
                          <div className="flex items-center justify-between">
                            <span className="text-foreground-muted">DB</span>
                            <span className="text-foreground font-medium truncate ml-2 max-w-[100px]" title={connection.database}>
                              {connection.database}
                            </span>
                          </div>
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-1.5">
                              {connection.last_test_status === "success" ? (
                                <span className="text-[10px] text-success font-medium bg-success/10 px-1 rounded border border-success/20">Active</span>
                              ) : (
                                <span className="text-[10px] text-error font-medium bg-error/10 px-1 rounded border border-error/20">Error</span>
                              )}
                            </div>
                            {isTesting && <Loader2 className="w-3 h-3 animate-spin text-primary" />}
                          </div>
                        </div>

                        {/* Actions */}
                        <div className="flex gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            className="flex-1 h-8 text-xs bg-transparent hover:bg-primary/5 hover:text-primary hover:border-primary/30 transition-all border-border"
                            onClick={() => handleTestConnection(connection.id)}
                            disabled={isLoading || testingConnectionId !== null}
                          >
                            <TestTube className="w-3.5 h-3.5 mr-1.5" />
                            Test
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 w-8 p-0"
                            onClick={() => {
                              setEditingConnection(connection.id)
                              setIsModalOpen(true)
                            }}
                          >
                            <Edit2 className="w-3.5 h-3.5 text-foreground-muted hover:text-foreground" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 w-8 p-0 hover:bg-error/10 hover:text-error"
                            onClick={() => handleDeleteClick(connection.id, connection.name)}
                          >
                            <Trash2 className="w-3.5 h-3.5 text-foreground-muted hover:text-error" />
                          </Button>
                        </div>
                      </div>
                    </Card>
                  )
                })}
              </div>
            ) : (
              <Card className="border-border shadow-sm overflow-hidden bg-card">
                <div className="w-full overflow-x-auto">
                  <Table>
                    <TableHeader className="bg-muted/50">
                      <TableRow>
                        <TableHead className="w-[200px]">Name</TableHead>
                        <TableHead className="w-[150px]">Type</TableHead>
                        <TableHead>Host</TableHead>
                        <TableHead>Database</TableHead>
                        <TableHead className="w-[120px]">Status</TableHead>
                        <TableHead className="text-right">Actions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {paginatedConnections.map((connection) => {
                        const isConnected = connection.last_test_status === "success"
                        const isTesting = testingConnectionId === connection.id
                        const dbType = (connection as any).database_type || connection.connection_type
                        const dbInfo = getDatabaseByConnectionType(dbType)
                        const dbColor = getDatabaseColor(dbType)

                        return (
                          <TableRow key={connection.id} className="hover:bg-surface-hover group">
                            <TableCell className="font-medium">
                              <div className="flex items-center gap-3">
                                <div
                                  className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 shadow-sm border border-border"
                                  style={{ background: `${dbColor.primary}10`, borderColor: `${dbColor.primary}30` }}
                                >
                                  <DatabaseLogo
                                    connectionType={dbType}
                                    databaseId={dbInfo?.id}
                                    databaseName={connection.name}
                                    displayName={dbInfo?.displayName}
                                    size={16}
                                  />
                                </div>
                                <span className="font-semibold">{connection.name}</span>
                              </div>
                            </TableCell>
                            <TableCell>
                              <span className="text-xs font-medium px-2 py-1 rounded-md bg-muted text-foreground-muted">
                                {dbInfo?.displayName || connection.connection_type}
                              </span>
                            </TableCell>
                            <TableCell className="font-mono text-xs text-foreground-muted">{connection.host}</TableCell>
                            <TableCell className="font-mono text-xs text-foreground-muted">{connection.database}</TableCell>
                            <TableCell>
                              {isConnected ? (
                                <div className="flex items-center gap-1.5 text-success text-xs font-medium">
                                  <CheckCircle className="w-3.5 h-3.5" />
                                  Connected
                                </div>
                              ) : connection.last_test_status === "failed" ? (
                                <div className="flex items-center gap-1.5 text-error text-xs font-medium">
                                  <AlertCircle className="w-3.5 h-3.5" />
                                  Failed
                                </div>
                              ) : (
                                <div className="flex items-center gap-1.5 text-warning text-xs font-medium">
                                  <AlertTriangle className="w-3.5 h-3.5" />
                                  Not Tested
                                </div>
                              )}
                            </TableCell>
                            <TableCell className="text-right">
                              <div className="flex items-center justify-end gap-1 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
                                <Button
                                  variant="outline"
                                  size="sm"
                                  className="h-7 px-2 text-xs border-border hover:bg-surface-hover"
                                  onClick={() => handleTestConnection(connection.id)}
                                  disabled={isLoading || testingConnectionId !== null}
                                >
                                  {isTesting ? <Loader2 className="w-3 h-3 animate-spin" /> : <TestTube className="w-3.5 h-3.5" />}
                                  <span className="ml-1.5">Test</span>
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 w-7 p-0"
                                  onClick={() => {
                                    setEditingConnection(connection.id)
                                    setIsModalOpen(true)
                                  }}
                                >
                                  <Edit2 className="w-3.5 h-3.5 text-foreground-muted" />
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 w-7 p-0 hover:bg-error/10 hover:text-error"
                                  onClick={() => handleDeleteClick(connection.id, connection.name)}
                                >
                                  <Trash2 className="w-3.5 h-3.5 text-foreground-muted hover:text-error" />
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        )
                      })}
                    </TableBody>
                  </Table>
                </div>
              </Card>
            )}

            {/* Pagination */}
            <div className="flex items-center justify-between pt-6 border-t border-border">
              <div className="text-sm text-foreground-muted">
                Showing <span className="font-semibold text-foreground">{startIndex + 1}</span> to{" "}
                <span className="font-semibold text-foreground">
                  {Math.min(endIndex, filteredConnections.length)}
                </span>{" "}
                of <span className="font-semibold text-foreground">{filteredConnections.length}</span> connections
              </div>

              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                  disabled={currentPage === 1}
                  className="border-border hover:bg-surface-hover disabled:opacity-50"
                >
                  <ChevronLeft className="w-4 h-4 mr-1" />
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                  disabled={currentPage === totalPages}
                  className="border-border hover:bg-surface-hover disabled:opacity-50"
                >
                  Next
                  <ChevronRight className="w-4 h-4 ml-1" />
                </Button>
              </div>
            </div>
          </>
        )}

        <ConnectionModal
          isOpen={isModalOpen}
          onClose={() => {
            setIsModalOpen(false)
            setEditingConnection(null)
          }}
          onSave={editingConnection ? handleUpdateConnection : handleAddConnection}
          editingConnection={editingConnection ? connections.find(c => c.id === editingConnection) || null : null}
        />

        <Dialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
          <DialogContent className="bg-surface border-border max-w-md">
            <DialogHeader>
              <div className="flex items-center gap-3 mb-2">
                <div className="p-2 bg-error/10 rounded-full">
                  <AlertTriangle className="w-6 h-6 text-error" />
                </div>
                <DialogTitle className="text-foreground text-xl">Delete Connection</DialogTitle>
              </div>
              <DialogDescription className="text-foreground-muted pt-2">
                Are you sure you want to delete this connection? This action cannot be undone.
              </DialogDescription>
            </DialogHeader>

            <div className="py-4">
              <div className="p-4 bg-surface-hover rounded-lg border border-border">
                <p className="text-sm text-foreground-muted mb-1">Connection Name:</p>
                <p className="text-lg font-semibold text-foreground">
                  {connectionToDelete?.name || "Unknown"}
                </p>
              </div>
              <div className="mt-4 p-3 bg-warning/10 border border-warning/30 rounded-lg">
                <p className="text-sm text-warning flex items-start gap-2">
                  <AlertCircle className="w-4 h-4 mt-0.5 flex-shrink-0" />
                  <span>
                    <strong>Warning:</strong> Deleting this connection will also remove any pipelines
                    that use it. Make sure no active pipelines depend on this connection.
                  </span>
                </p>
              </div>
            </div>

            <DialogFooter className="gap-2">
              <Button
                variant="outline"
                onClick={() => {
                  setDeleteConfirmOpen(false)
                  setConnectionToDelete(null)
                }}
                className="bg-transparent border-border hover:bg-surface-hover"
              >
                Cancel
              </Button>
              <Button
                onClick={handleDeleteConnection}
                className="bg-error hover:bg-error/90 text-white"
                disabled={isLoading}
              >
                {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : "Delete Connection"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </ProtectedPage>
  )
}
