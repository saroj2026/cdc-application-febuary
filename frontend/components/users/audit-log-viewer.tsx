"use client"

import { useState, useEffect, useRef, useCallback } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Button } from "@/components/ui/button"
import { 
  Search, 
  Filter, 
  Calendar, 
  User, 
  Activity, 
  Loader2, 
  ChevronLeft, 
  ChevronRight,
  AlertCircle,
  Download,
  RefreshCw
} from "lucide-react"
import { formatDistanceToNow, format } from "date-fns"
import { apiClient } from "@/lib/api/client"

interface AuditLog {
  id: string
  tenant_id?: string
  user_id?: string
  user_email?: string
  action: string
  resource_type?: string
  resource_id?: string
  old_value?: any
  new_value?: any
  ip_address?: string
  user_agent?: string
  created_at: string
}

const ACTION_COLORS: Record<string, string> = {
  create: "bg-green-500/20 text-green-400 border-green-500/30",
  update: "bg-blue-500/20 text-blue-400 border-blue-500/30",
  delete: "bg-red-500/20 text-red-400 border-red-500/30",
  start: "bg-cyan-500/20 text-cyan-400 border-cyan-500/30",
  stop: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30",
  login: "bg-purple-500/20 text-purple-400 border-purple-500/30",
  logout: "bg-gray-500/20 text-gray-400 border-gray-500/30",
}

export function AuditLogViewer() {
  const [auditLogs, setAuditLogs] = useState<AuditLog[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [searchQuery, setSearchQuery] = useState("")
  const [actionFilter, setActionFilter] = useState<string>("all")
  const [resourceFilter, setResourceFilter] = useState<string>("all")
  const [currentPage, setCurrentPage] = useState(1)
  const [totalLogs, setTotalLogs] = useState(0)
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date())
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [availableActions, setAvailableActions] = useState<string[]>([])
  const [availableResourceTypes, setAvailableResourceTypes] = useState<string[]>([])
  const logsPerPage = 20
  const intervalRef = useRef<NodeJS.Timeout | null>(null)

  // Fetch available filters
  useEffect(() => {
    const fetchFilters = async () => {
      try {
        const response = await apiClient.client.get("/api/v1/audit-logs/filters", {
          timeout: 10000
        })
        if (response.data) {
          setAvailableActions(response.data.actions || [])
          setAvailableResourceTypes(response.data.resource_types || [])
        }
      } catch (err: any) {
        // If filters can't be fetched, use defaults
        console.warn("Could not fetch filter options:", err)
      }
    }
    fetchFilters()
  }, [])

  // Use ref to store the latest fetch function to avoid dependency issues
  const fetchAuditLogsRef = useRef<((silent?: boolean) => Promise<void>) | null>(null)

  const fetchAuditLogs = useCallback(async (silent = false) => {
    try {
      if (!silent) {
        setIsLoading(true)
      } else {
        setIsRefreshing(true)
      }
      // Use longer timeout for audit logs (30 seconds)
      const response = await apiClient.client.get("/api/v1/audit-logs", {
        timeout: 30000, // 30 seconds timeout
        params: {
          skip: (currentPage - 1) * logsPerPage,
          limit: logsPerPage,
          action: actionFilter !== "all" ? actionFilter : undefined,
          resource_type: resourceFilter !== "all" ? resourceFilter : undefined,
        }
      })
      const logs = Array.isArray(response.data) ? response.data : []
      
      // Only update if we got new data or it's not a silent refresh
      if (!silent || logs.length > 0) {
        setAuditLogs(logs)
        setLastUpdate(new Date())
      }
      
      // Estimate total (in a real app, API would return total count)
      setTotalLogs(logs.length >= logsPerPage ? (currentPage * logsPerPage) + 1 : logs.length)
    } catch (err: any) {
      // Don't log connection errors in silent mode
      if (!silent) {
        // Check if it's a connection error
        if (err?.code === 'ERR_CONNECTION_REFUSED' || 
            err?.code === 'ERR_NETWORK' || 
            err?.message?.includes('Cannot connect to server') ||
            err?.message?.includes('Network Error')) {
          console.warn("Backend server connection issue. Please ensure the backend is running on http://localhost:8000")
        } else {
          console.error("Error fetching audit logs:", err)
        }
        setAuditLogs([])
      }
    } finally {
      setIsLoading(false)
      setIsRefreshing(false)
    }
  }, [currentPage, actionFilter, resourceFilter, logsPerPage])

  // Update ref whenever fetchAuditLogs changes
  useEffect(() => {
    fetchAuditLogsRef.current = fetchAuditLogs
  }, [fetchAuditLogs])
  
  const handleManualRefresh = useCallback(() => {
    fetchAuditLogs()
  }, [fetchAuditLogs])

  useEffect(() => {
    // Only fetch if we have a connection (check if backend is available)
    let mounted = true
    
    const fetchWithRetry = async (retries = 0) => {
      if (!mounted) return
      
      try {
        // Use ref to get the latest function
        if (fetchAuditLogsRef.current) {
          await fetchAuditLogsRef.current()
        }
      } catch (err: any) {
        // If connection error and we haven't retried too many times, retry after delay
        if ((err?.code === 'ERR_CONNECTION_REFUSED' || err?.code === 'ERR_NETWORK') && retries < 2) {
          setTimeout(() => {
            if (mounted) fetchWithRetry(retries + 1)
          }, 2000)
        }
      }
    }
    
    fetchWithRetry()
    
    // Set up polling for real-time updates (every 10 seconds - increased from 5)
    // Only poll if we're on the first page and no filters are applied
    if (currentPage === 1 && actionFilter === "all" && resourceFilter === "all" && !searchQuery) {
      intervalRef.current = setInterval(() => {
        if (mounted && fetchAuditLogsRef.current) {
          fetchAuditLogsRef.current(true) // Silent refresh
        }
      }, 10000) // Increased to 10 seconds to reduce load
    }
    
    return () => {
      mounted = false
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
      }
    }
  }, [currentPage, actionFilter, resourceFilter, searchQuery])

  // Filter logs client-side for search (server already filters by action/resource_type)
  const filteredLogs = auditLogs.filter(log => {
    if (searchQuery && !log.action.toLowerCase().includes(searchQuery.toLowerCase()) && 
        !log.resource_type?.toLowerCase().includes(searchQuery.toLowerCase()) &&
        !log.user_email?.toLowerCase().includes(searchQuery.toLowerCase())) {
      return false
    }
    return true
  })

  // Calculate total pages (we'll need to track total from API response)
  const totalPages = Math.ceil(totalLogs / logsPerPage)
  const paginatedLogs = filteredLogs

  const getActionColor = (action: string) => {
    const actionLower = action.toLowerCase()
    for (const [key, color] of Object.entries(ACTION_COLORS)) {
      if (actionLower.includes(key)) {
        return color
      }
    }
    return "bg-gray-500/20 text-gray-400 border-gray-500/30"
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
        <span className="ml-2 text-foreground-muted">Loading audit logs...</span>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Filters */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex flex-col sm:flex-row gap-4">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-foreground-muted" />
              <Input
                placeholder="Search by action or resource type..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10"
              />
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={handleManualRefresh}
              disabled={isRefreshing}
              className="w-auto"
            >
              <RefreshCw className={`w-4 h-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
            <Select value={actionFilter} onValueChange={setActionFilter}>
              <SelectTrigger className="w-[180px]">
                <Filter className="w-4 h-4 mr-2" />
                <SelectValue placeholder="All Actions" />
              </SelectTrigger>
              <SelectContent className="max-h-[300px]">
                <SelectItem value="all">All Actions</SelectItem>
                {availableActions.length > 0 ? (
                  availableActions.map((action) => (
                    <SelectItem key={action} value={action}>
                      {action.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                    </SelectItem>
                  ))
                ) : (
                  <>
                    <SelectItem value="create">Create</SelectItem>
                    <SelectItem value="update">Update</SelectItem>
                    <SelectItem value="delete">Delete</SelectItem>
                    <SelectItem value="start">Start</SelectItem>
                    <SelectItem value="stop">Stop</SelectItem>
                    <SelectItem value="login">Login</SelectItem>
                  </>
                )}
              </SelectContent>
            </Select>
            <Select value={resourceFilter} onValueChange={setResourceFilter}>
              <SelectTrigger className="w-[180px]">
                <SelectValue placeholder="All Resources" />
              </SelectTrigger>
              <SelectContent className="max-h-[300px] z-50">
                <SelectItem value="all">All Resources</SelectItem>
                {/* Always show default options, plus any additional ones from API */}
                <SelectItem value="pipeline">Pipeline</SelectItem>
                <SelectItem value="connection">Connection</SelectItem>
                <SelectItem value="user">User</SelectItem>
                {availableResourceTypes.length > 0 && availableResourceTypes
                  .filter(rt => !["pipeline", "connection", "user"].includes(rt.toLowerCase()))
                  .map((resourceType) => (
                    <SelectItem key={resourceType} value={resourceType}>
                      {resourceType.charAt(0).toUpperCase() + resourceType.slice(1)}
                    </SelectItem>
                  ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Last Update Indicator */}
      {paginatedLogs.length > 0 && (
        <div className="flex items-center justify-between text-xs text-foreground-muted">
          <span>
            Last updated: {formatDistanceToNow(lastUpdate, { addSuffix: true })}
            {isRefreshing && <span className="ml-2">(Refreshing...)</span>}
          </span>
          <span className="flex items-center gap-1">
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
            Live updates enabled
          </span>
        </div>
      )}

      {/* Audit Logs List */}
      {paginatedLogs.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center">
            <AlertCircle className="w-16 h-16 mx-auto mb-4 opacity-20 text-foreground-muted" />
            <p className="text-foreground-muted mb-2">
              {searchQuery || actionFilter !== "all" || resourceFilter !== "all"
                ? "No audit logs match your filters"
                : "No audit logs available yet"}
            </p>
            <p className="text-sm text-foreground-muted mb-4">
              Audit logs will appear here as users perform actions in the system
            </p>
            <Button variant="outline" size="sm" onClick={handleManualRefresh} disabled={isRefreshing}>
              <RefreshCw className={`w-4 h-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
              Refresh Now
            </Button>
          </CardContent>
        </Card>
      ) : (
        <>
          <div className="space-y-4">
            {paginatedLogs.map((log) => {
              const actionColor = getActionColor(log.action)
              const logDate = new Date(log.created_at)
              
              return (
                <Card key={log.id} className="hover:border-cyan-400/50 transition-colors">
                  <CardContent className="pt-6">
                    <div className="flex items-start justify-between gap-4">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-3 mb-2">
                          <Badge variant="outline" className={actionColor}>
                            <Activity className="w-3 h-3 mr-1" />
                            {log.action.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                          </Badge>
                          {log.resource_type && (
                            <Badge variant="outline" className="bg-cyan-500/20 text-cyan-400 border-cyan-500/30">
                              {log.resource_type}
                            </Badge>
                          )}
                          <span className="text-xs text-foreground-muted">
                            {formatDistanceToNow(logDate, { addSuffix: true })}
                          </span>
                        </div>
                        <div className="space-y-1 text-sm">
                          {log.user_email && (
                            <div className="flex items-center gap-2 text-foreground-muted">
                              <User className="w-3 h-3" />
                              <span>{log.user_email}</span>
                            </div>
                          )}
                          {!log.user_email && log.user_id && (
                            <div className="flex items-center gap-2 text-foreground-muted">
                              <User className="w-3 h-3" />
                              <span>User ID: {log.user_id}</span>
                            </div>
                          )}
                          {log.resource_id && (
                            <div className="flex items-center gap-2 text-foreground-muted">
                              <span>Resource ID: {log.resource_id}</span>
                            </div>
                          )}
                          {log.ip_address && (
                            <div className="flex items-center gap-2 text-foreground-muted">
                              <span>IP: {log.ip_address}</span>
                            </div>
                          )}
                          <div className="flex items-center gap-2 text-foreground-muted">
                            <Calendar className="w-3 h-3" />
                            <span>{format(logDate, "PPpp")}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )
            })}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between">
              <p className="text-sm text-foreground-muted">
                Showing {filteredLogs.length > 0 ? (currentPage - 1) * logsPerPage + 1 : 0} to {Math.min(currentPage * logsPerPage, totalLogs)} of {totalLogs} logs
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                  disabled={currentPage === 1}
                >
                  <ChevronLeft className="w-4 h-4" />
                  Previous
                </Button>
                <span className="text-sm text-foreground-muted">
                  Page {currentPage} of {totalPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                  disabled={currentPage === totalPages}
                >
                  Next
                  <ChevronRight className="w-4 h-4" />
                </Button>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

