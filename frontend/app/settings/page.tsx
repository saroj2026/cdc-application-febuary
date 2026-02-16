"use client"

import { useState, useEffect, useMemo } from "react"
import { useAppDispatch, useAppSelector } from "@/lib/store/hooks"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card } from "@/components/ui/card"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import { apiClient } from "@/lib/api/client"
import {
  Settings, User, Mail, Lock, Key, Send, X, CheckCircle,
  AlertCircle, RefreshCw, ShieldCheck, Activity, Terminal,
  Bell, Database, Cpu, Globe, Search, Filter, ChevronLeft, ChevronRight, Clock
} from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"
import { useToast } from "@/components/ui/use-toast"
import { getCurrentUser } from "@/lib/store/slices/authSlice"
import { ProtectedPage } from "@/components/auth/ProtectedPage"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Badge } from "@/components/ui/badge"
import { format } from "date-fns"
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow
} from "@/components/ui/table"
import { Switch } from "@/components/ui/switch"
import { cn } from "@/lib/utils"

interface UserData {
  id: string
  email: string
  full_name: string
  is_active: boolean
  is_superuser: boolean
}

interface AuditLog {
  id: string
  user_id: string
  action: string
  resource_type: string
  resource_id: string
  created_at: string
  ip_address: string
  old_value?: any
  new_value?: any
}

interface AppLog {
  id: string
  level: string
  message: string
  timestamp: string
  logger: string
  module: string
}

export default function SettingsPage() {
  const dispatch = useAppDispatch()
  const { user: currentUser } = useAppSelector((state) => state.auth)
  const { toast } = useToast()

  // States
  const [activeTab, setActiveTab] = useState("users")
  const [mounted, setMounted] = useState(false)
  const [loading, setLoading] = useState(false)

  // User Management States
  const [users, setUsers] = useState<UserData[]>([])
  const [selectedUser, setSelectedUser] = useState<UserData | null>(null)
  const [showChangePasswordDialog, setShowChangePasswordDialog] = useState(false)
  const [newPassword, setNewPassword] = useState("")
  const [confirmPassword, setConfirmPassword] = useState("")
  const [sendEmail, setSendEmail] = useState(true)
  const [changingPassword, setChangingPassword] = useState(false)
  const [passwordChangeSuccess, setPasswordChangeSuccess] = useState(false)
  const [generatedPassword, setGeneratedPassword] = useState("")

  // Audit Logs States
  const [auditLogs, setAuditLogs] = useState<AuditLog[]>([])
  const [auditSkip, setAuditSkip] = useState(0)
  const [auditLimit] = useState(15)
  const [auditAction, setAuditAction] = useState("")

  // App Logs States
  const [appLogs, setAppLogs] = useState<AppLog[]>([])
  const [appLogLevel, setAppLogLevel] = useState("INFO")
  const [appLogSearch, setAppLogSearch] = useState("")

  // System Health States
  const [systemHealth, setSystemHealth] = useState<any>(null)
  const [eventLoggerStatus, setEventLoggerStatus] = useState<any>(null)

  // Notifications Settings (Mocked)
  const [notifications, setNotifications] = useState({
    emailAlerts: true,
    slackAlerts: false,
    webhookAlerts: true,
    thresholdLatency: 1000,
    thresholdErrorRate: 5
  })

  // Handle client-side mounting
  useEffect(() => {
    setMounted(true)
  }, [])

  useEffect(() => {
    if (mounted && currentUser?.is_superuser) {
      if (activeTab === "users") loadUsers()
      if (activeTab === "audit") loadAuditLogs()
      if (activeTab === "applogs") loadAppLogs()
      if (activeTab === "health") loadSystemStatus()
    }
  }, [mounted, currentUser?.is_superuser, activeTab, auditSkip, appLogLevel])

  const loadUsers = async () => {
    try {
      setLoading(true)
      const data = await apiClient.getUsers(0, 1000)
      setUsers(data || [])
    } catch (error: any) {
      console.error("Failed to load users:", error)
    } finally {
      setLoading(false)
    }
  }

  const loadAuditLogs = async () => {
    try {
      setLoading(true)
      const data = await apiClient.getAuditLogs(auditSkip, auditLimit, auditAction)
      setAuditLogs(data || [])
    } catch (error: any) {
      console.error("Failed to load audit logs:", error)
    } finally {
      setLoading(false)
    }
  }

  const loadAppLogs = async () => {
    try {
      setLoading(true)
      const data = await apiClient.getApplicationLogs(0, 50, appLogLevel, appLogSearch)
      setAppLogs(data.logs || [])
    } catch (error: any) {
      console.error("Failed to load app logs:", error)
    } finally {
      setLoading(false)
    }
  }

  const loadSystemStatus = async () => {
    try {
      setLoading(true)
      const health = await apiClient.getSystemHealth()
      const loggerStatus = await apiClient.getEventLoggerStatus()
      setSystemHealth(health)
      setEventLoggerStatus(loggerStatus)
    } catch (error: any) {
      console.error("Failed to load system status:", error)
    } finally {
      setLoading(false)
    }
  }

  const handleChangePassword = (user: UserData) => {
    setSelectedUser(user)
    setNewPassword("")
    setConfirmPassword("")
    setSendEmail(true)
    setPasswordChangeSuccess(false)
    setGeneratedPassword("")
    setShowChangePasswordDialog(true)
  }

  const handleSubmitPasswordChange = async () => {
    if (!selectedUser || !newPassword) return
    if (newPassword !== confirmPassword) {
      toast({ title: "Error", description: "Passwords do not match", variant: "destructive" })
      return
    }

    setChangingPassword(true)
    try {
      const response = await apiClient.adminChangePassword(selectedUser.id, newPassword, sendEmail)
      setPasswordChangeSuccess(true)
      setGeneratedPassword(response.new_password || "")
      toast({ title: "Success", description: "Password changed successfully!" })
    } catch (error: any) {
      toast({ title: "Error", description: error.response?.data?.detail || "Failed to change password", variant: "destructive" })
    } finally {
      setChangingPassword(false)
    }
  }

  const isAdmin = currentUser?.is_superuser

  if (!mounted || !currentUser) return <div className="p-6">Loading...</div>

  if (!isAdmin) {
    return (
      <div className="p-6">
        <Card className="p-6 border-error/50 bg-error/5">
          <div className="flex items-center gap-3 text-error">
            <AlertCircle className="w-6 h-6" />
            <div>
              <p className="font-bold text-lg">Access Denied</p>
              <p className="text-sm opacity-90">Only administrators can access the settings portal.</p>
            </div>
          </div>
        </Card>
      </div>
    )
  }

  return (
    <ProtectedPage path="/settings" requiredPermission="manage_roles">
      <div className="p-6 space-y-6">
        <PageHeader title="System Settings" subtitle="Configure platform parameters and manage access" icon={Settings} />

        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid grid-cols-5 w-full max-w-4xl bg-surface/50 border border-border p-1">
            <TabsTrigger value="users" className="gap-2"><User className="w-4 h-4" /> Users</TabsTrigger>
            <TabsTrigger value="audit" className="gap-2"><ShieldCheck className="w-4 h-4" /> Audit</TabsTrigger>
            <TabsTrigger value="applogs" className="gap-2"><Terminal className="w-4 h-4" /> Logs</TabsTrigger>
            <TabsTrigger value="health" className="gap-2"><Activity className="w-4 h-4" /> Health</TabsTrigger>
            <TabsTrigger value="notifications" className="gap-2"><Bell className="w-4 h-4" /> Alerts</TabsTrigger>
          </TabsList>

          {/* User Management */}
          <TabsContent value="users" className="mt-6">
            <Card className="p-6 border-purple-500/20 bg-gradient-to-br from-card to-purple-500/5">
              <div className="flex items-center justify-between mb-6">
                <div>
                  <h3 className="text-lg font-bold flex items-center gap-2">
                    <User className="text-purple-400" /> User Directory
                  </h3>
                  <p className="text-sm text-foreground-muted">Manage system users, roles, and security credentials</p>
                </div>
                <Button onClick={loadUsers} variant="outline" size="sm" className="gap-2">
                  <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} /> Sync
                </Button>
              </div>

              <div className="space-y-3">
                {users.map((user) => (
                  <div key={user.id} className="flex items-center justify-between p-4 bg-surface rounded-lg border border-border hover:border-purple-500/30 transition-all">
                    <div className="flex items-center gap-4">
                      <div className="w-10 h-10 rounded-full bg-purple-500/10 flex items-center justify-center">
                        <User className="w-5 h-5 text-purple-400" />
                      </div>
                      <div>
                        <p className="font-semibold text-foreground">{user.full_name || user.email}</p>
                        <div className="flex items-center gap-3 text-xs text-foreground-muted">
                          <span className="flex items-center gap-1"><Mail className="w-3 h-3" /> {user.email}</span>
                          {user.is_superuser && <Badge variant="secondary" className="bg-purple-500/10 text-purple-400 border-purple-500/20 text-[10px] h-4">ADMIN</Badge>}
                          {!user.is_active && <Badge variant="destructive" className="text-[10px] h-4">INACTIVE</Badge>}
                        </div>
                      </div>
                    </div>
                    <Button onClick={() => handleChangePassword(user)} variant="ghost" size="sm" className="text-purple-400 hover:text-purple-300 hover:bg-purple-500/10 gap-2">
                      <Key className="w-4 h-4" /> Password
                    </Button>
                  </div>
                ))}
              </div>
            </Card>
          </TabsContent>

          {/* Audit Logs */}
          <TabsContent value="audit" className="mt-6">
            <Card className="p-6 border-blue-500/20 bg-gradient-to-br from-card to-blue-500/5">
              <div className="flex items-center justify-between mb-6">
                <div>
                  <h3 className="text-lg font-bold flex items-center gap-2">
                    <ShieldCheck className="text-blue-400" /> Governance & Audit Trail
                  </h3>
                  <p className="text-sm text-foreground-muted">Historical record of all administrative actions and system modifications</p>
                </div>
                <div className="flex gap-2">
                  <div className="relative">
                    <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                    <Input placeholder="Filter actions..." className="pl-8 h-9 w-[200px]" value={auditAction} onChange={(e) => setAuditAction(e.target.value)} />
                  </div>
                  <Button onClick={loadAuditLogs} variant="outline" size="sm">
                    <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
                  </Button>
                </div>
              </div>

              <div className="border rounded-lg overflow-hidden bg-surface">
                <Table>
                  <TableHeader className="bg-muted/50">
                    <TableRow>
                      <TableHead className="w-[180px]">Timestamp</TableHead>
                      <TableHead>User ID</TableHead>
                      <TableHead>Action</TableHead>
                      <TableHead>Resource</TableHead>
                      <TableHead>IP Address</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {auditLogs.length > 0 ? auditLogs.map((log) => (
                      <TableRow key={log.id} className="hover:bg-muted/30">
                        <TableCell className="text-xs font-mono text-muted-foreground">
                          {format(new Date(log.created_at || Date.now()), "MMM dd, HH:mm:ss")}
                        </TableCell>
                        <TableCell className="text-xs truncate max-w-[120px]">{log.user_id}</TableCell>
                        <TableCell>
                          <Badge variant="outline" className="border-blue-500/30 text-blue-400 font-mono text-[10px]">
                            {log.action}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-xs">{log.resource_type}</TableCell>
                        <TableCell className="text-xs text-muted-foreground font-mono">{log.ip_address}</TableCell>
                      </TableRow>
                    )) : (
                      <TableRow>
                        <TableCell colSpan={5} className="text-center py-12 text-muted-foreground">
                          No audit records found
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </div>

              <div className="flex items-center justify-between mt-4">
                <p className="text-xs text-muted-foreground text-center">
                  Showing entries {auditSkip + 1} to {auditSkip + auditLogs.length}
                </p>
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" onClick={() => setAuditSkip(Math.max(0, auditSkip - auditLimit))} disabled={auditSkip === 0}>
                    <ChevronLeft className="w-4 h-4" />
                  </Button>
                  <Button variant="outline" size="sm" onClick={() => setAuditSkip(auditSkip + auditLimit)} disabled={auditLogs.length < auditLimit}>
                    <ChevronRight className="w-4 h-4" />
                  </Button>
                </div>
              </div>
            </Card>
          </TabsContent>

          {/* Application Logs */}
          <TabsContent value="applogs" className="mt-6">
            <Card className="p-6 border-teal-500/20 bg-gradient-to-br from-card to-teal-500/5">
              <div className="flex items-center justify-between mb-6">
                <div>
                  <h3 className="text-lg font-bold flex items-center gap-2">
                    <Terminal className="text-teal-400" /> System Runtime Logs
                  </h3>
                  <p className="text-sm text-foreground-muted">Live telemetry and event logs from the backend engine</p>
                </div>
                <div className="flex gap-2">
                  <select
                    value={appLogLevel}
                    onChange={(e) => setAppLogLevel(e.target.value)}
                    className="bg-background border rounded px-3 py-1 text-sm outline-none focus:ring-1 ring-teal-500"
                  >
                    <option value="DEBUG">DEBUG</option>
                    <option value="INFO">INFO</option>
                    <option value="WARNING">WARNING</option>
                    <option value="ERROR">ERROR</option>
                    <option value="CRITICAL">CRITICAL</option>
                  </select>
                  <Button onClick={loadAppLogs} variant="outline" size="sm" className="gap-2">
                    <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} /> Tail
                  </Button>
                </div>
              </div>

              <div className="bg-slate-950 p-4 rounded-lg font-mono text-[11px] leading-relaxed border border-slate-800 h-[500px] overflow-y-auto custom-scrollbar">
                {appLogs.map((log, i) => (
                  <div key={log.id || i} className="flex gap-4 mb-2 group">
                    <span className="text-slate-600 shrink-0">[{format(new Date(log.timestamp), "HH:mm:ss")}]</span>
                    <span className={`shrink-0 w-16 font-bold ${log.level === 'ERROR' ? 'text-red-400' :
                      log.level === 'WARNING' ? 'text-yellow-400' :
                        'text-teal-400'
                      }`}>{log.level}</span>
                    <span className="text-slate-400 shrink-0 w-24 truncate">[{log.logger.split('.').pop()}]</span>
                    <span className="text-slate-200 group-hover:text-white transition-colors">{log.message}</span>
                  </div>
                ))}
                {appLogs.length === 0 && <p className="text-slate-500 text-center italic mt-12">No recent logs found for level {appLogLevel}</p>}
              </div>
            </Card>
          </TabsContent>

          {/* System Health */}
          <TabsContent value="health" className="mt-6">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8 pb-10">
              {/* API Core - Very Deep Visual Card */}
              <Card className="relative overflow-hidden p-0 border-none bg-white dark:bg-slate-900 shadow-[0_30px_60px_rgba(0,0,0,0.12)] dark:shadow-[0_30px_60px_rgba(0,0,0,0.4)] rounded-[2.5rem] group min-h-[320px] ring-1 ring-slate-100 dark:ring-slate-800/50 transition-all duration-500 hover:translate-y-[-4px]">
                <div className="absolute inset-0 bg-gradient-to-br from-emerald-500/10 via-transparent to-transparent opacity-50 group-hover:opacity-100 transition-opacity duration-1000"></div>

                {/* 3D Glass Surface Shine */}
                <div className="absolute -top-24 -left-24 w-64 h-64 bg-white/20 dark:bg-white/5 rounded-full blur-3xl group-hover:scale-150 transition-transform duration-1000"></div>

                {/* Heartbeat Visualization */}
                <div className="absolute right-0 bottom-0 left-0 h-32 opacity-[0.2] group-hover:opacity-[0.35] transition-all duration-700 pointer-events-none">
                  <svg viewBox="0 0 100 40" className="h-full w-full" preserveAspectRatio="none">
                    <path d="M0,20 L15,20 L20,20 L22,10 L28,30 L30,20 L45,20 L50,20 L52,-5 L58,45 L60,20 L80,20 L82,15 L88,25 L90,20 L100,20"
                      fill="none" stroke="rgb(16, 185, 129)" strokeWidth="1"
                      className="animate-[shimmer_3s_linear_infinite]"
                    />
                  </svg>
                </div>

                <div className="p-10 relative z-10 flex flex-col h-full">
                  <div className="flex items-center gap-5 mb-10">
                    <div className="p-5 bg-gradient-to-br from-emerald-400 to-emerald-600 rounded-2xl shadow-[0_10px_20px_-5px_rgba(16,185,129,0.4)] transform group-hover:rotate-6 transition-transform">
                      <Cpu className="w-8 h-8 text-white" />
                    </div>
                    <div>
                      <h4 className="text-2xl font-black tracking-tighter text-slate-800 dark:text-white">Backend Core</h4>
                      <p className="text-[10px] font-black text-emerald-500 uppercase tracking-[0.3em]">Engine Cluster V1</p>
                    </div>
                  </div>

                  <div className="mt-auto space-y-8">
                    <div className="relative">
                      <span className="text-[11px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-[0.25em] block mb-2">STATUS</span>
                      <div className="flex items-center gap-4">
                        <span className="text-5xl font-black text-emerald-500 tracking-tighter drop-shadow-[0_8px_15px_rgba(16,185,129,0.4)] group-hover:scale-105 transition-transform origin-left">RUNNING</span>
                        <div className="relative flex h-4 w-4">
                          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                          <span className="relative inline-flex rounded-full h-4 w-4 bg-emerald-500 shadow-[0_0_10px_rgba(16,185,129,0.8)]"></span>
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-8 pt-6 border-t border-slate-100 dark:border-white/5">
                      <div className="space-y-1">
                        <span className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest">UPTIME</span>
                        <p className="text-sm font-bold text-slate-700 dark:text-slate-300 tabular-nums">14h 22m 11s</p>
                      </div>
                      <div className="space-y-1">
                        <span className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest">BUILD</span>
                        <p className="text-sm font-bold text-slate-700 dark:text-slate-300">V1.0.4-S</p>
                      </div>
                    </div>
                  </div>
                </div>
              </Card>

              {/* Event Logger - Very Deep Visual Card */}
              <Card className="relative overflow-hidden p-0 border-none bg-white dark:bg-slate-900 shadow-[0_30px_60px_rgba(0,0,0,0.12)] dark:shadow-[0_30px_60px_rgba(0,0,0,0.4)] rounded-[2.5rem] group min-h-[320px] ring-1 ring-slate-100 dark:ring-slate-800/50 transition-all duration-500 hover:translate-y-[-4px]">
                <div className="absolute inset-0 bg-gradient-to-br from-amber-500/10 via-transparent to-transparent opacity-50 group-hover:opacity-100 transition-opacity duration-1000"></div>

                {/* Heartbeat Visualization */}
                <div className="absolute right-0 bottom-0 left-0 h-32 opacity-[0.2] group-hover:opacity-[0.35] transition-all duration-700 pointer-events-none">
                  <svg viewBox="0 0 100 40" className="h-full w-full" preserveAspectRatio="none">
                    <path d="M0,20 L15,20 L20,20 L22,12 L28,28 L30,20 L45,20 L50,20 L52,5 L58,35 L60,20 L80,20 L82,18 L88,22 L90,20 L100,20"
                      fill="none" stroke="rgb(245, 158, 11)" strokeWidth="1"
                      className="animate-[shimmer_4s_linear_infinite]"
                    />
                  </svg>
                </div>

                <div className="p-10 relative z-10 flex flex-col h-full">
                  <div className="flex items-center gap-5 mb-10">
                    <div className="p-5 bg-gradient-to-br from-amber-400 to-amber-600 rounded-2xl shadow-[0_10px_20px_-5px_rgba(245,158,11,0.4)] transform group-hover:-rotate-6 transition-transform">
                      <Database className="w-8 h-8 text-white" />
                    </div>
                    <div>
                      <h4 className="text-2xl font-black tracking-tighter text-slate-800 dark:text-white">Log Engine</h4>
                      <p className="text-[10px] font-black text-amber-500 uppercase tracking-[0.3em]">Kafka Event Bus</p>
                    </div>
                  </div>

                  <div className="mt-auto space-y-8">
                    <div className="relative">
                      <span className="text-[11px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-[0.25em] block mb-2">STATUS</span>
                      <div className="flex items-center gap-4">
                        <span className="text-5xl font-black text-amber-500 tracking-tighter drop-shadow-[0_8px_15px_rgba(245,158,11,0.4)] group-hover:scale-105 transition-transform origin-left">
                          {eventLoggerStatus ? "ACTIVE" : "RUNNING"}
                        </span>
                        <div className="relative flex h-4 w-4">
                          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75"></span>
                          <span className="relative inline-flex rounded-full h-4 w-4 bg-amber-500 shadow-[0_0_10px_rgba(245,158,11,0.8)]"></span>
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-8 pt-6 border-t border-slate-100 dark:border-white/5">
                      <div className="space-y-1">
                        <span className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest">LOAD</span>
                        <p className="text-sm font-bold text-slate-700 dark:text-slate-300">0% (Empty)</p>
                      </div>
                      <div className="space-y-1">
                        <span className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest">BUS</span>
                        <p className="text-sm font-bold text-slate-700 dark:text-slate-300">Healthy Flow</p>
                      </div>
                    </div>
                  </div>
                </div>
              </Card>

              {/* Infrastructure - Very Deep Visual Card */}
              <Card className="relative overflow-hidden p-0 border-none bg-white dark:bg-slate-900 shadow-[0_30px_60px_rgba(0,0,0,0.12)] dark:shadow-[0_30px_60px_rgba(0,0,0,0.4)] rounded-[2.5rem] group min-h-[320px] ring-1 ring-slate-100 dark:ring-slate-800/50 transition-all duration-500 hover:translate-y-[-4px]">
                <div className="absolute inset-0 bg-gradient-to-br from-cyan-500/10 via-transparent to-transparent opacity-50 group-hover:opacity-100 transition-opacity duration-1000"></div>

                {/* Heartbeat Visualization */}
                <div className="absolute right-0 bottom-0 left-0 h-32 opacity-[0.2] group-hover:opacity-[0.35] transition-all duration-700 pointer-events-none">
                  <svg viewBox="0 0 100 40" className="h-full w-full" preserveAspectRatio="none">
                    <path d="M0,20 L15,20 L20,20 L22,18 L28,22 L30,20 L45,20 L50,20 L52,1 L58,39 L60,20 L80,20 L82,15 L88,25 L90,20 L100,20"
                      fill="none" stroke="rgb(6, 182, 212)" strokeWidth="1"
                      className="animate-[shimmer_2s_linear_infinite]"
                    />
                  </svg>
                </div>

                <div className="p-10 relative z-10 flex flex-col h-full">
                  <div className="flex items-center gap-5 mb-10">
                    <div className="p-5 bg-gradient-to-br from-cyan-400 to-cyan-600 rounded-2xl shadow-[0_10px_20px_-5px_rgba(6,182,212,0.4)] transform group-hover:scale-110 transition-transform">
                      <Globe className="w-8 h-8 text-white" />
                    </div>
                    <div>
                      <h4 className="text-2xl font-black tracking-tighter text-slate-800 dark:text-white">API Gateway</h4>
                      <p className="text-[10px] font-black text-cyan-500 uppercase tracking-[0.3em]">Network Edge</p>
                    </div>
                  </div>

                  <div className="mt-auto space-y-8">
                    <div className="relative">
                      <span className="text-[11px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-[0.25em] block mb-2">STATUS</span>
                      <div className="flex items-center gap-4">
                        <span className="text-5xl font-black text-cyan-500 tracking-tighter drop-shadow-[0_8px_15px_rgba(6,182,212,0.4)] group-hover:scale-105 transition-transform origin-left">HEALTHY</span>
                        <div className="relative flex h-4 w-4">
                          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-75"></span>
                          <span className="relative inline-flex rounded-full h-4 w-4 bg-cyan-500 shadow-[0_0_10px_rgba(6,182,212,0.8)]"></span>
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-8 pt-6 border-t border-slate-100 dark:border-white/5">
                      <div className="space-y-1">
                        <span className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest">LATENCY</span>
                        <p className="text-sm font-bold text-slate-700 dark:text-slate-300 tabular-nums">12ms (Avg)</p>
                      </div>
                      <div className="space-y-1">
                        <span className="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest">SECURITY</span>
                        <p className="text-sm font-bold text-emerald-500 dark:text-emerald-400 flex items-center gap-1 font-black">
                          <ShieldCheck className="w-3.5 h-3.5" /> SECURE
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              </Card>
            </div>

            <Card className="mt-6 p-6 border-border bg-card/50">
              <h4 className="text-sm font-bold mb-4 flex items-center gap-2"><Settings className="w-4 h-4" /> System Control</h4>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="p-4 border rounded-lg flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium">Re-initialize Monitoring Bus</p>
                    <p className="text-[10px] text-muted-foreground">Full restart of the Internal stats collector</p>
                  </div>
                  <Button variant="outline" size="sm">Execute</Button>
                </div>
                <div className="p-4 border rounded-lg flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium">Clear Data Cache</p>
                    <p className="text-[10px] text-muted-foreground">Flushes UI-state and dashboard metrics</p>
                  </div>
                  <Button variant="outline" size="sm" className="text-error border-error/20 hover:bg-error/5">Flush</Button>
                </div>
              </div>
            </Card>
          </TabsContent>

          {/* Alerts & Notifications */}
          <TabsContent value="notifications" className="mt-6">
            <Card className="p-6 border-orange-500/20 bg-gradient-to-br from-card to-orange-500/5">
              <div className="mb-8">
                <h3 className="text-lg font-bold flex items-center gap-2">
                  <Bell className="text-orange-400" /> Alerting Framework
                </h3>
                <p className="text-sm text-foreground-muted">Configure how and when you want to be notified about pipeline events</p>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                <div className="space-y-6">
                  <h4 className="text-sm font-bold uppercase tracking-wider text-muted-foreground">Notification Channels</h4>

                  <div className="flex items-center justify-between p-4 bg-surface rounded-lg border border-border">
                    <div className="flex items-center gap-3">
                      <Mail className="w-5 h-5 text-orange-400/70" />
                      <div>
                        <p className="text-sm font-medium">Email Notifications</p>
                        <p className="text-xs text-muted-foreground">Send alerts to admin team inbox</p>
                      </div>
                    </div>
                    <Switch checked={notifications.emailAlerts} onCheckedChange={(v) => setNotifications({ ...notifications, emailAlerts: v })} />
                  </div>

                  <div className="flex items-center justify-between p-4 bg-surface rounded-lg border border-border">
                    <div className="flex items-center gap-3">
                      <div className="w-5 h-5 flex items-center justify-center font-bold text-slate-400">#</div>
                      <div>
                        <p className="text-sm font-medium">Slack Integration</p>
                        <p className="text-xs text-muted-foreground">Broadcast alerts to #cdc-ops channel</p>
                      </div>
                    </div>
                    <Switch checked={notifications.slackAlerts} onCheckedChange={(v) => setNotifications({ ...notifications, slackAlerts: v })} />
                  </div>

                  <div className="flex items-center justify-between p-4 bg-surface rounded-lg border border-border">
                    <div className="flex items-center gap-3">
                      <Globe className="w-5 h-5 text-cyan-400/70" />
                      <div>
                        <p className="text-sm font-medium">Custom Webhooks</p>
                        <p className="text-xs text-muted-foreground">POST JSON payload to custom endpoint</p>
                      </div>
                    </div>
                    <Switch checked={notifications.webhookAlerts} onCheckedChange={(v) => setNotifications({ ...notifications, webhookAlerts: v })} />
                  </div>
                </div>

                <div className="space-y-6">
                  <h4 className="text-sm font-bold uppercase tracking-wider text-muted-foreground">Alerting Thresholds</h4>

                  <div className="p-4 bg-surface rounded-lg border border-border space-y-4">
                    <div className="space-y-2">
                      <div className="flex justify-between">
                        <Label className="text-xs">Latency Critical Threshold (ms)</Label>
                        <span className="text-xs font-mono font-bold text-orange-400">{notifications.thresholdLatency}ms</span>
                      </div>
                      <Input
                        type="range" min="100" max="5000" step="100"
                        value={notifications.thresholdLatency}
                        onChange={(e) => setNotifications({ ...notifications, thresholdLatency: parseInt(e.target.value) })}
                        className="accent-orange-500 h-2 p-0 bg-muted rounded-lg appearance-none cursor-pointer"
                      />
                    </div>

                    <div className="space-y-2">
                      <div className="flex justify-between">
                        <Label className="text-xs">Error Rate (%) Trigger</Label>
                        <span className="text-xs font-mono font-bold text-red-400">{notifications.thresholdErrorRate}%</span>
                      </div>
                      <Input
                        type="range" min="1" max="50" step="1"
                        value={notifications.thresholdErrorRate}
                        onChange={(e) => setNotifications({ ...notifications, thresholdErrorRate: parseInt(e.target.value) })}
                        className="accent-red-500 h-2 p-0 bg-muted rounded-lg appearance-none cursor-pointer"
                      />
                    </div>
                  </div>

                  <Button className="w-full bg-orange-600 hover:bg-orange-500 text-white" onClick={() => toast({ title: "Settings Saved", description: "Global alert thresholds have been updated." })}>
                    Save Configuration
                  </Button>
                </div>
              </div>
            </Card>
          </TabsContent>
        </Tabs>

        {/* Change Password Dialog (Maintained from original) */}
        <Dialog open={showChangePasswordDialog} onOpenChange={setShowChangePasswordDialog}>
          <DialogContent className="sm:max-w-md">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <Lock className="w-5 h-5 text-primary" /> Security Update
              </DialogTitle>
              <DialogDescription>Reset password for user <b>{selectedUser?.email}</b></DialogDescription>
            </DialogHeader>

            {passwordChangeSuccess ? (
              <div className="space-y-4 py-4 text-center">
                <div className="mx-auto w-12 h-12 bg-emerald-500/10 rounded-full flex items-center justify-center mb-2">
                  <CheckCircle className="text-emerald-500 w-8 h-8" />
                </div>
                <p className="text-sm font-medium">Policy Updated Successfully</p>
                {generatedPassword && !sendEmail && (
                  <div className="p-4 bg-muted rounded-lg font-mono text-xs break-all border select-all">{generatedPassword}</div>
                )}
                <Button onClick={() => setShowChangePasswordDialog(false)} className="w-full">Dismiss</Button>
              </div>
            ) : (
              <div className="space-y-4 py-2">
                <div className="space-y-2">
                  <Label>New Secure Password</Label>
                  <div className="flex gap-2">
                    <Input type="password" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} className="flex-1" placeholder="Min 8 characters" />
                    <Button type="button" variant="outline" onClick={() => {
                      const p = Math.random().toString(36).slice(-10) + "!" + Math.floor(Math.random() * 100);
                      setNewPassword(p); setConfirmPassword(p);
                    }}><Key className="w-4 h-4" /></Button>
                  </div>
                </div>
                <div className="space-y-2">
                  <Label>Confirm Password</Label>
                  <Input type="password" value={confirmPassword} onChange={(e) => setConfirmPassword(e.target.value)} />
                </div>
                <div className="flex items-center gap-2">
                  <Switch checked={sendEmail} onCheckedChange={setSendEmail} />
                  <Label className="text-xs">Dispatch credentials via Email</Label>
                </div>
                <div className="flex gap-3 pt-4">
                  <Button variant="outline" className="flex-1" onClick={() => setShowChangePasswordDialog(false)}>Cancel</Button>
                  <Button disabled={changingPassword || !newPassword || newPassword !== confirmPassword} onClick={handleSubmitPasswordChange} className="flex-1 bg-purple-600 hover:bg-purple-500 text-white">
                    {changingPassword ? "Updating..." : "Update Password"}
                  </Button>
                </div>
              </div>
            )}
          </DialogContent>
        </Dialog>
      </div>
    </ProtectedPage>
  )
}
