"use client"

import { useState, useEffect, useRef, useCallback } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Badge } from "@/components/ui/badge"
import { apiClient } from "@/lib/api/client"
import { format } from "date-fns"
import { Search, RefreshCw, Download, Filter, X, Loader2, FileText, ChevronLeft, ChevronRight } from "lucide-react"

interface LogEntry {
  id: string
  level: string
  logger: string
  message: string
  timestamp: string
  module?: string
  function?: string
  line?: number
  extra?: any
}

export function ApplicationLogs() {
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedLevel, setSelectedLevel] = useState<string>("all")
  const [availableLevels, setAvailableLevels] = useState<string[]>([])
  const [currentPage, setCurrentPage] = useState(1)
  const [totalLogs, setTotalLogs] = useState(0)
  const logsPerPage = 50
  const scrollAreaRef = useRef<HTMLDivElement>(null)
  const autoRefreshIntervalRef = useRef<NodeJS.Timeout | null>(null)

  // Fetch log levels
  useEffect(() => {
    const fetchLevels = async () => {
      try {
        const levels = await apiClient.getLogLevels()
        setAvailableLevels(levels)
      } catch (error) {
        console.error("Error fetching log levels:", error)
        setAvailableLevels(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
      }
    }
    fetchLevels()
  }, [])

  // Fetch logs
  const fetchLogs = useCallback(async (silent = false) => {
    if (!silent) {
      setIsLoading(true)
    } else {
      setIsRefreshing(true)
    }

    try {
      const skip = (currentPage - 1) * logsPerPage
      const level = selectedLevel !== "all" ? selectedLevel : undefined
      const search = searchTerm.trim() || undefined

      const response = await apiClient.getApplicationLogs(
        skip,
        logsPerPage,
        level,
        search
      )

      // Handle both array response (backward compatibility) and object response
      if (Array.isArray(response)) {
        setLogs(response)
        setTotalLogs(response.length >= logsPerPage ? currentPage * logsPerPage + 1 : response.length)
      } else if (response && typeof response === 'object' && 'logs' in response) {
        const logsArray = response.logs || []
        const total = response.total || logsArray.length
        setLogs(logsArray)
        setTotalLogs(total)
      } else {
        setLogs([])
        setTotalLogs(0)
      }
    } catch (error: any) {
      console.error("Error fetching logs:", error)
      setLogs([])
      setTotalLogs(0)
    } finally {
      setIsLoading(false)
      setIsRefreshing(false)
    }
  }, [currentPage, selectedLevel, searchTerm, logsPerPage])

  // Initial fetch
  useEffect(() => {
    fetchLogs()
  }, [fetchLogs])

  // Auto-refresh every 10 seconds
  useEffect(() => {
    autoRefreshIntervalRef.current = setInterval(() => {
      fetchLogs(true) // Silent refresh
    }, 10000)

    return () => {
      if (autoRefreshIntervalRef.current) {
        clearInterval(autoRefreshIntervalRef.current)
      }
    }
  }, [fetchLogs])

  // Reset to page 1 when filters change
  useEffect(() => {
    setCurrentPage(1)
  }, [searchTerm, selectedLevel])

  const getLevelColor = (level: string) => {
    const upperLevel = level.toUpperCase()
    if (upperLevel === "ERROR" || upperLevel === "CRITICAL") {
      return "bg-error/10 text-error border-error/20"
    } else if (upperLevel === "WARNING" || upperLevel === "WARN") {
      return "bg-warning/10 text-warning border-warning/20"
    } else if (upperLevel === "INFO") {
      return "bg-info/10 text-info border-info/20"
    } else if (upperLevel === "DEBUG") {
      return "bg-muted text-foreground-muted border-border"
    }
    return "bg-muted text-foreground-muted border-border"
  }

  const handleExport = () => {
    const logText = logs
      .map(log => {
        const timestamp = format(new Date(log.timestamp), "yyyy-MM-dd HH:mm:ss")
        return `[${timestamp}] [${log.level}] [${log.logger}] ${log.message}`
      })
      .join("\n")

    const blob = new Blob([logText], { type: "text/plain" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `application-logs-${format(new Date(), "yyyy-MM-dd-HH-mm-ss")}.txt`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  return (
    <Card className="flex flex-col border-border shadow-sm bg-card p-0 overflow-hidden">
      <div className="p-4 border-b border-border flex flex-col sm:flex-row sm:items-center justify-between gap-4 bg-muted/30">
        <div>
          <h3 className="text-base font-bold text-foreground flex items-center gap-2">
            <FileText className="w-4 h-4 text-primary" />
            Application Logs
          </h3>
          <p className="text-xs text-foreground-muted mt-0.5">System events and error tracking</p>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={handleExport}
            className="h-8 text-xs bg-background hover:bg-surface-hover hover:text-primary border-border"
          >
            <Download className="w-3.5 h-3.5 mr-1.5" />
            Export
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => fetchLogs()}
            disabled={isRefreshing}
            className="h-8 text-xs bg-background hover:bg-surface-hover hover:text-primary border-border"
          >
            <RefreshCw className={`w-3.5 h-3.5 mr-1.5 ${isRefreshing ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        </div>
      </div>

      {/* Filters */}
      <div className="p-4 border-b border-border bg-card flex flex-col sm:flex-row gap-3">
        <div className="flex-1 relative">
          <Search className="absolute left-2.5 top-1/2 transform -translate-y-1/2 w-4 h-4 text-foreground-muted" />
          <Input
            placeholder="Search logs..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-9 h-9 text-sm bg-background border-border focus-visible:ring-primary"
          />
          {searchTerm && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSearchTerm("")}
              className="absolute right-1 top-1/2 transform -translate-y-1/2 h-7 w-7 p-0"
            >
              <X className="w-3.5 h-3.5" />
            </Button>
          )}
        </div>
        <Select value={selectedLevel} onValueChange={setSelectedLevel}>
          <SelectTrigger className="w-full sm:w-[150px] h-9 text-sm bg-background border-border">
            <Filter className="w-3.5 h-3.5 mr-2 text-foreground-muted" />
            <SelectValue placeholder="All Levels" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Levels</SelectItem>
            {availableLevels.map((level) => (
              <SelectItem key={level} value={level}>
                {level}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Logs Display */}
      <div className="h-[400px] w-full overflow-y-auto bg-surface/50" ref={scrollAreaRef}>
        {isLoading && logs.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full">
            <Loader2 className="w-8 h-8 animate-spin text-primary mb-2" />
            <span className="text-xs text-foreground-muted">Loading logs...</span>
          </div>
        ) : logs.length > 0 ? (
          <div className="divide-y divide-border/50">
            {logs.map((log) => (
              <div
                key={log.id}
                className="p-3 hover:bg-surface-hover transition-colors font-mono text-xs group"
              >
                <div className="flex items-start gap-3">
                  <div className="flex flex-col items-center gap-1 min-w-[60px]">
                    <Badge
                      variant="outline"
                      className={`${getLevelColor(log.level)} text-[10px] px-1.5 py-0 rounded h-5 border font-semibold w-full justify-center`}
                    >
                      {log.level}
                    </Badge>
                  </div>

                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-0.5">
                      <span className="text-foreground-muted">
                        {format(new Date(log.timestamp), "HH:mm:ss.SSS")}
                      </span>
                      {log.logger && (
                        <span className="text-primary/80 font-medium">[{log.logger}]</span>
                      )}
                      {log.module && (
                        <span className="text-foreground-muted truncate">
                          {log.module}
                          {log.function && `::${log.function}`}
                          {log.line && `:${log.line}`}
                        </span>
                      )}
                    </div>
                    <p className="text-foreground whitespace-pre-wrap break-words leading-relaxed pl-1 border-l-2 border-transparent group-hover:border-border/50 transition-colors">
                      {log.message}
                    </p>
                    {log.extra && Object.keys(log.extra).length > 0 && (
                      <details className="mt-1.5">
                        <summary className="text-[10px] text-foreground-muted cursor-pointer hover:text-primary inline-flex items-center gap-1 select-none">
                          View details
                        </summary>
                        <pre className="mt-1 p-2 bg-muted rounded border border-border text-[10px] text-foreground-muted overflow-x-auto">
                          {JSON.stringify(log.extra, null, 2)}
                        </pre>
                      </details>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center h-full text-foreground-muted">
            <div className="p-4 bg-muted/50 rounded-full mb-3">
              <FileText className="w-6 h-6 opacity-40" />
            </div>
            <p className="text-sm font-medium">No logs found</p>
            <p className="text-xs mt-1">
              {searchTerm || selectedLevel !== "all"
                ? "Try adjusting your filters"
                : "Logs will appear here"}
            </p>
          </div>
        )}
      </div>

      {/* Pagination */}
      {logs.length > 0 && (
        <div className="p-3 border-t border-border bg-card flex items-center justify-between text-xs">
          <div className="text-foreground-muted">
            Showing <span className="font-medium text-foreground">{((currentPage - 1) * logsPerPage) + 1}</span> - <span className="font-medium text-foreground">{Math.min(currentPage * logsPerPage, totalLogs)}</span> of {totalLogs}
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
              disabled={currentPage === 1 || isLoading}
              className="h-7 px-2 text-xs border-border hover:bg-surface-hover"
            >
              <ChevronLeft className="w-3.5 h-3.5 mr-1" />
              Prev
            </Button>
            <span className="px-2 min-w-[3rem] text-center font-medium">
              Page {currentPage}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCurrentPage(prev => prev + 1)}
              disabled={logs.length < logsPerPage || isLoading}
              className="h-7 px-2 text-xs border-border hover:bg-surface-hover"
            >
              Next
              <ChevronRight className="w-3.5 h-3.5 ml-1" />
            </Button>
          </div>
        </div>
      )}
    </Card>
  )
}
