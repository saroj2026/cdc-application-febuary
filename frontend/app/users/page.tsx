"use client"

import { useState, useEffect, useMemo, useRef } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import {
  Users,
  Plus,
  Edit2,
  Trash2,
  Search,
  MoreVertical,
  Shield,
  ShieldCheck,
  UserCog,
  Eye,
  Loader2,
  CheckCircle,
  XCircle,
  Clock,
  AlertCircle,
  Filter,
  Zap,
  Circle,
  Activity,
  LayoutGrid,
  List,
  Mail,
  Upload,
  Copy,
  Check
} from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"
import { UserModal } from "@/components/users/user-modal"
import { InviteUserModal } from "@/components/users/invite-user-modal"
import { useAppDispatch, useAppSelector } from "@/lib/store/hooks"
import { fetchUsers, deleteUser } from "@/lib/store/slices/userSlice"
import { apiClient } from "@/lib/api/client"
import { formatDistanceToNow } from "date-fns"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger, DropdownMenuSeparator } from "@/components/ui/dropdown-menu"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { hasPermission } from "@/lib/store/slices/permissionSlice"
import { AuditLogViewer } from "@/components/users/audit-log-viewer"
import { store } from "@/lib/store/store"
import { ProtectedPage } from "@/components/auth/ProtectedPage"
import { useConfirmDialog } from "@/components/ui/confirm-dialog"

interface User {
  id: string
  email: string
  full_name: string
  role_name: string
  status?: string
  is_active: boolean
  is_superuser: boolean
  last_login?: string
  created_at: string
}

const ROLE_COLORS: Record<string, string> = {
  super_admin: "bg-purple-500/20 text-purple-400 border-purple-500/30",
  org_admin: "bg-blue-500/20 text-blue-400 border-blue-500/30",
  data_engineer: "bg-cyan-500/20 text-cyan-400 border-cyan-500/30",
  operator: "bg-green-500/20 text-green-400 border-green-500/30",
  viewer: "bg-gray-500/20 text-gray-400 border-gray-500/30",
}

const STATUS_COLORS: Record<string, string> = {
  active: "bg-green-500/20 text-green-400 border-green-500/30",
  invited: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30",
  suspended: "bg-red-500/20 text-red-400 border-red-500/30",
  deactivated: "bg-gray-500/20 text-gray-400 border-gray-500/30",
}

const ROLE_ICONS: Record<string, any> = {
  super_admin: ShieldCheck,
  org_admin: Shield,
  data_engineer: UserCog,
  operator: Eye,
  viewer: Eye,
}

function ImportResultRow({ index, row }: { index: number; row: { email: string; full_name?: string; role?: string; token: string; expires_at: string } }) {
  const [copiedLink, setCopiedLink] = useState(false)
  const [copiedEmail, setCopiedEmail] = useState(false)
  const inviteLink = typeof window !== "undefined" ? `${window.location.origin}/auth/accept-invite?token=${row.token}` : ""
  const copyLink = () => {
    if (inviteLink && typeof navigator !== "undefined") {
      navigator.clipboard.writeText(inviteLink)
      setCopiedLink(true)
      setTimeout(() => setCopiedLink(false), 2000)
    }
  }
  const copyEmail = () => {
    if (row.email && typeof navigator !== "undefined") {
      navigator.clipboard.writeText(row.email)
      setCopiedEmail(true)
      setTimeout(() => setCopiedEmail(false), 2000)
    }
  }
  const expiresDate = row.expires_at ? new Date(row.expires_at).toLocaleDateString(undefined, { dateStyle: "short" }) : "—"
  return (
    <TableRow>
      <TableCell className="text-muted-foreground w-12">{index}</TableCell>
      <TableCell className="font-medium whitespace-nowrap">{row.email}</TableCell>
      <TableCell className="whitespace-nowrap">{row.full_name || "—"}</TableCell>
      <TableCell className="whitespace-nowrap">{row.role ? row.role.charAt(0).toUpperCase() + row.role.slice(1) : "—"}</TableCell>
      <TableCell className="text-muted-foreground text-xs whitespace-nowrap">{expiresDate}</TableCell>
      <TableCell className="text-right">
        <div className="flex items-center justify-end gap-1">
          <Button variant="outline" size="sm" className="h-8 gap-1 text-xs" onClick={copyLink} title="Copy invite link">
            {copiedLink ? <Check className="w-3.5 h-3.5 text-green-500" /> : <Copy className="w-3.5 h-3.5" />}
            {copiedLink ? "Copied" : "Copy link"}
          </Button>
          <Button variant="ghost" size="sm" className="h-8 gap-1 text-xs" onClick={copyEmail} title="Copy email">
            {copiedEmail ? <Check className="w-3.5 h-3.5 text-green-500" /> : <Copy className="w-3.5 h-3.5" />}
            {copiedEmail ? "Copied" : "Email"}
          </Button>
        </div>
      </TableCell>
    </TableRow>
  )
}

export default function UsersPage() {
  const dispatch = useAppDispatch()
  const { user: currentUser } = useAppSelector((state) => state.auth)
  const { users, isLoading, error: usersError } = useAppSelector((state) => state.users)

  // Use state to prevent hydration mismatch
  const [mounted, setMounted] = useState(false)
  const [canCreateUser, setCanCreateUser] = useState(false)
  const [canManageRoles, setCanManageRoles] = useState(false)
  const [canViewAuditLogs, setCanViewAuditLogs] = useState(false)

  const [isModalOpen, setIsModalOpen] = useState(false)
  const [editingUser, setEditingUser] = useState<User | null>(null)
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false)
  const [userToDelete, setUserToDelete] = useState<User | null>(null)
  const [searchQuery, setSearchQuery] = useState("")
  const [roleFilter, setRoleFilter] = useState<string>("all")
  const [statusFilter, setStatusFilter] = useState<string>("all")
  const [currentPage, setCurrentPage] = useState(1)
  const [activeTab, setActiveTab] = useState<"users" | "import" | "audit">("users")
  const [viewMode, setViewMode] = useState<"grid" | "list">("grid")
  const [inviteModalOpen, setInviteModalOpen] = useState(false)
  const [importFile, setImportFile] = useState<File | null>(null)
  const [importResult, setImportResult] = useState<{ imported: number; skipped_duplicates: number; errors: string[]; invitation_tokens: Array<{ email: string; full_name?: string; role?: string; token: string; expires_at: string }> } | null>(null)
  const [importLoading, setImportLoading] = useState(false)
  const { showConfirm, ConfirmDialogComponent } = useConfirmDialog()
  const usersPerPage = 8

  const hasFetchedRef = useRef(false)

  // Set permissions on client side only
  useEffect(() => {
    setMounted(true)
    const state = store.getState()
    setCanCreateUser(hasPermission("create_user")(state))
    setCanManageRoles(hasPermission("manage_roles")(state))
    setCanViewAuditLogs(hasPermission("view_audit_logs")(state))
  }, [])

  useEffect(() => {
    if (hasFetchedRef.current || isLoading) return
    hasFetchedRef.current = true
    dispatch(fetchUsers())
  }, [dispatch, isLoading])

  // Filter users
  const filteredUsers = useMemo(() => {
    let filtered = Array.isArray(users) ? users : []

    // Search filter
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase()
      filtered = filtered.filter(u =>
        u.email?.toLowerCase().includes(query) ||
        u.full_name?.toLowerCase().includes(query) ||
        u.role_name?.toLowerCase().includes(query)
      )
    }

    // Role filter
    if (roleFilter !== "all") {
      filtered = filtered.filter(u => u.role_name === roleFilter)
    }

    // Status filter
    if (statusFilter !== "all") {
      if (statusFilter === "active") {
        filtered = filtered.filter(u => u.is_active && (!u.status || u.status === "active"))
      } else {
        filtered = filtered.filter(u => u.status === statusFilter)
      }
    }

    return filtered
  }, [users, searchQuery, roleFilter, statusFilter])

  // Pagination
  const totalPages = Math.ceil(filteredUsers.length / usersPerPage)
  const startIndex = (currentPage - 1) * usersPerPage
  const endIndex = startIndex + usersPerPage
  const paginatedUsers = filteredUsers.slice(startIndex, endIndex)

  useEffect(() => {
    setCurrentPage(1)
  }, [searchQuery, roleFilter, statusFilter])

  const handleCreateUser = () => {
    setEditingUser(null)
    setIsModalOpen(true)
  }

  const handleEditUser = (user: User) => {
    setEditingUser(user)
    setIsModalOpen(true)
  }

  const handleDeleteUser = (user: User) => {
    setUserToDelete(user)
    setDeleteConfirmOpen(true)
  }

  const confirmDelete = async (user: User) => {
    showConfirm(
      "Delete User",
      `Are you sure you want to delete ${user.full_name || user.email}? This action cannot be undone.`,
      async () => {
        try {
          await dispatch(deleteUser(user.id)).unwrap()
        } catch (err: any) {
          alert(err.message || "Failed to delete user")
        }
      },
      "danger",
      "Delete User"
    )
  }

  const handleSaveUser = async () => {
    await dispatch(fetchUsers())
    setIsModalOpen(false)
    setEditingUser(null)
  }

  const handleInviteSuccess = () => {
    dispatch(fetchUsers())
  }

  const handleImportCsv = async () => {
    if (!importFile) return
    setImportLoading(true)
    setImportResult(null)
    try {
      const res = await apiClient.importUsers(importFile)
      setImportResult(res)
      dispatch(fetchUsers())
      setImportFile(null)
    } catch (err: any) {
      setImportResult({
        imported: 0,
        skipped_duplicates: 0,
        errors: [err.response?.data?.detail || err.message || "Import failed"],
        invitation_tokens: [],
      })
    } finally {
      setImportLoading(false)
    }
  }

  const getRoleDisplayName = (role: string) => {
    return role.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')
  }

  const getStatusDisplayName = (status?: string, isActive?: boolean) => {
    if (!status && isActive) return "Active"
    if (!status) return "Inactive"
    return status.charAt(0).toUpperCase() + status.slice(1)
  }

  const getStatusIcon = (status?: string, isActive?: boolean) => {
    if (!status && isActive) return CheckCircle
    if (status === "active" || (!status && isActive)) return CheckCircle
    if (status === "invited") return Clock
    if (status === "suspended") return XCircle
    return AlertCircle
  }

  // Prevent hydration mismatch by not rendering action until mounted
  if (!mounted) {
    return (
      <div className="p-6 space-y-6">
        <PageHeader
          title="User Management"
          subtitle="Manage users, roles, and permissions for your CDC replication platform"
          icon={Users}
        />
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
        </div>
      </div>
    )
  }

  return (
    <ProtectedPage path="/users" requiredPermission="create_user">
      <ConfirmDialogComponent />
      <div className="p-6 space-y-6">
        <PageHeader
          title="User Management"
          subtitle="Manage users, roles, and permissions for your CDC replication platform"
          icon={Users}
          action={
            canCreateUser || canManageRoles ? (
              <div className="flex items-center gap-2">
                {canCreateUser && (
                  <>
                    <Button onClick={handleCreateUser} className="bg-primary hover:bg-primary/90 text-foreground gap-2">
                    <Plus className="w-4 h-4" />
                    New User
                  </Button>
                    <Button variant="outline" onClick={() => setInviteModalOpen(true)} className="gap-2">
                      <Mail className="w-4 h-4" />
                      Invite User
                    </Button>
                  </>
                )}
              </div>
            ) : undefined
          }
        />

        {/* Error Message */}
        {usersError && (
          <div className="p-4 bg-error/10 border border-error/30 rounded-lg">
            <p className="text-sm text-error">{usersError}</p>
          </div>
        )}

        <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as "users" | "import" | "audit")}>
          <TabsList>
            <TabsTrigger value="users">Users</TabsTrigger>
            {canCreateUser && <TabsTrigger value="import">Import CSV</TabsTrigger>}
            {canViewAuditLogs && <TabsTrigger value="audit">Audit Logs</TabsTrigger>}
          </TabsList>

          <TabsContent value="users" className="space-y-6">
            {/* Filters */}
            <Card>
              <CardContent className="pt-6">
                <div className="flex flex-col sm:flex-row gap-4">
                  <div className="flex-1 relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-foreground-muted" />
                    <Input
                      placeholder="Search users by name, email, or role..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="pl-10"
                    />
                  </div>
                  <div className="flex items-center gap-2 border border-border rounded-md p-1 bg-muted/30">
                    <Button
                      variant={viewMode === "grid" ? "secondary" : "ghost"}
                      size="sm"
                      onClick={() => setViewMode("grid")}
                      className="h-8 w-8 p-0"
                    >
                      <LayoutGrid className="w-4 h-4" />
                    </Button>
                    <Button
                      variant={viewMode === "list" ? "secondary" : "ghost"}
                      size="sm"
                      onClick={() => setViewMode("list")}
                      className="h-8 w-8 p-0"
                    >
                      <List className="w-4 h-4" />
                    </Button>
                  </div>
                  <Select value={roleFilter} onValueChange={setRoleFilter}>
                    <SelectTrigger className="w-[180px]">
                      <Filter className="w-4 h-4 mr-2" />
                      <SelectValue placeholder="All Roles" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Roles</SelectItem>
                      <SelectItem value="super_admin">Super Admin</SelectItem>
                      <SelectItem value="org_admin">Org Admin</SelectItem>
                      <SelectItem value="data_engineer">Data Engineer</SelectItem>
                      <SelectItem value="operator">Operator</SelectItem>
                      <SelectItem value="viewer">Viewer</SelectItem>
                    </SelectContent>
                  </Select>
                  <Select value={statusFilter} onValueChange={setStatusFilter}>
                    <SelectTrigger className="w-[180px]">
                      <SelectValue placeholder="All Status" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Status</SelectItem>
                      <SelectItem value="active">Active</SelectItem>
                      <SelectItem value="invited">Invited</SelectItem>
                      <SelectItem value="suspended">Suspended</SelectItem>
                      <SelectItem value="deactivated">Deactivated</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </CardContent>
            </Card>

            {/* Users List */}
            {isLoading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="w-6 h-6 animate-spin text-foreground-muted" />
                <span className="ml-2 text-foreground-muted">Loading users...</span>
              </div>
            ) : paginatedUsers.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center">
                  <Users className="w-16 h-16 mx-auto mb-4 opacity-20 text-foreground-muted" />
                  <p className="text-foreground-muted mb-4">
                    {searchQuery || roleFilter !== "all" || statusFilter !== "all"
                      ? "No users match your filters"
                      : "No users found"}
                  </p>
                  {canCreateUser && !searchQuery && roleFilter === "all" && statusFilter === "all" && (
                    <Button onClick={handleCreateUser} className="bg-primary hover:bg-primary/90 text-foreground gap-2">
                      <Plus className="w-4 h-4" />
                      Create First User
                    </Button>
                  )}
                </CardContent>
              </Card>
            ) : (
              <>
                {viewMode === "grid" ? (
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                    {paginatedUsers.map((user) => {
                      const RoleIcon = ROLE_ICONS[user.role_name] || Users
                      const StatusIcon = getStatusIcon(user.status, user.is_active)
                      const statusDisplay = getStatusDisplayName(user.status, user.is_active)
                      const statusColor = STATUS_COLORS[user.status || (user.is_active ? "active" : "inactive")] || STATUS_COLORS.active
                      const isActive = user.is_active && (!user.status || user.status === "active")

                      return (
                        <Card
                          key={user.id}
                          className="p-0 bg-card border-l-4 border-l-primary shadow-sm hover:shadow-md transition-all group relative overflow-hidden h-full flex flex-col"
                        >
                          {/* Watermark Icon */}
                          <RoleIcon className="absolute -right-4 -bottom-4 w-24 h-24 text-primary/10 group-hover:text-primary/15 transition-colors transform rotate-12" />

                          <CardContent className="p-6 relative z-10 flex flex-col items-center text-center flex-1">
                            {/* Centered Avatar */}
                            <div className="relative mb-4">
                              <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center text-primary font-bold text-xl transition-all duration-300 group-hover:scale-105">
                                {user.full_name?.charAt(0).toUpperCase() || user.email?.charAt(0).toUpperCase() || "U"}
                              </div>
                              {/* Status dot */}
                              <div className={`absolute -bottom-0.5 -right-0.5 w-4 h-4 rounded-full border-2 border-card ${isActive ? 'bg-success' :
                                  user.status === 'invited' ? 'bg-warning' :
                                    user.status === 'suspended' ? 'bg-error' : 'bg-muted-foreground'
                                }`} />
                            </div>

                            {/* User Name */}
                            <div className="mb-1 w-full text-center">
                              <h3 className="text-base font-bold text-foreground truncate px-2">
                                {user.full_name || user.email}
                              </h3>
                            </div>

                            {/* Email */}
                            <p className="text-xs text-foreground-muted mb-3 truncate w-full px-2">
                              {user.email}
                            </p>

                            {/* Role and Status Badges */}
                            <div className="flex flex-col items-center gap-1.5 mb-4 w-full">
                              <Badge
                                variant="outline"
                                className={`text-[10px] h-5 ${ROLE_COLORS[user.role_name] || ROLE_COLORS.viewer}`}
                              >
                                <RoleIcon className="w-3 h-3 mr-1" />
                                {getRoleDisplayName(user.role_name)}
                              </Badge>
                              <Badge
                                variant="outline"
                                className={`text-[10px] h-5 ${statusColor}`}
                              >
                                <StatusIcon className="w-3 h-3 mr-1" />
                                {statusDisplay}
                              </Badge>
                            </div>

                            {/* Action Buttons */}
                            <div className="flex items-center justify-center gap-2 mt-auto pt-3 border-t border-border/50 w-full">
                              {canManageRoles && (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => handleEditUser(user)}
                                  className="h-8 px-2 text-xs flex-1 hover:bg-primary/5"
                                >
                                  <Edit2 className="w-3 h-3 mr-1" />
                                  Edit
                                </Button>
                              )}
                              {canManageRoles && user.id !== currentUser?.id && (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => confirmDelete(user)}
                                  className="h-8 px-2 text-xs flex-1 hover:bg-error/5 text-error"
                                >
                                  <Trash2 className="w-3 h-3 mr-1" />
                                  Delete
                                </Button>
                              )}
                            </div>
                          </CardContent>
                        </Card>
                      )
                    })}
                  </div>
                ) : (
                  <div className="space-y-2">
                    {paginatedUsers.map((user) => {
                      const RoleIcon = ROLE_ICONS[user.role_name] || Users
                      const StatusIcon = getStatusIcon(user.status, user.is_active)
                      const statusDisplay = getStatusDisplayName(user.status, user.is_active)
                      const statusColor = STATUS_COLORS[user.status || (user.is_active ? "active" : "inactive")] || STATUS_COLORS.active
                      const isActive = user.is_active && (!user.status || user.status === "active")

                      return (
                        <Card
                          key={user.id}
                          className="p-0 bg-card border-l-4 border-l-primary shadow-sm hover:shadow-md transition-all group relative overflow-hidden"
                        >
                          <CardContent className="p-4 flex items-center justify-between gap-4">
                            <div className="flex items-center gap-4 flex-1 min-w-0">
                              <div className="relative flex-shrink-0">
                                <div className="w-10 h-10 rounded-full bg-primary/10 flex items-center justify-center text-primary font-bold">
                                  {user.full_name?.charAt(0).toUpperCase() || user.email?.charAt(0).toUpperCase() || "U"}
                                </div>
                                <div className={`absolute -bottom-0.5 -right-0.5 w-3 h-3 rounded-full border-2 border-card ${isActive ? 'bg-success' :
                                    user.status === 'invited' ? 'bg-warning' :
                                      user.status === 'suspended' ? 'bg-error' : 'bg-muted-foreground'
                                  }`} />
                              </div>
                              <div className="flex-1 min-w-0">
                                <h3 className="text-sm font-bold text-foreground truncate">
                                  {user.full_name || user.email}
                                </h3>
                                <p className="text-xs text-foreground-muted truncate">
                                  {user.email}
                                </p>
                              </div>
                              <div className="hidden md:flex flex-col gap-1 items-start min-w-[120px]">
                                <Badge
                                  variant="outline"
                                  className={`text-[10px] h-5 ${ROLE_COLORS[user.role_name] || ROLE_COLORS.viewer}`}
                                >
                                  <RoleIcon className="w-3 h-3 mr-1" />
                                  {getRoleDisplayName(user.role_name)}
                                </Badge>
                              </div>
                              <div className="hidden sm:flex flex-col gap-1 items-start min-w-[100px]">
                                <Badge
                                  variant="outline"
                                  className={`text-[10px] h-5 ${statusColor}`}
                                >
                                  <StatusIcon className="w-3 h-3 mr-1" />
                                  {statusDisplay}
                                </Badge>
                              </div>
                            </div>

                            <div className="flex items-center gap-1">
                              {canManageRoles && (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => handleEditUser(user)}
                                  className="h-8 w-8 p-0"
                                >
                                  <Edit2 className="w-4 h-4" />
                                </Button>
                              )}
                              {canManageRoles && user.id !== currentUser?.id && (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => confirmDelete(user)}
                                  className="h-8 w-8 p-0 text-error hover:text-error hover:bg-error/10"
                                >
                                  <Trash2 className="w-4 h-4" />
                                </Button>
                              )}
                            </div>
                          </CardContent>
                        </Card>
                      )
                    })}
                  </div>
                )}

                {/* Pagination */}
                {totalPages > 1 && (
                  <div className="flex items-center justify-between">
                    <p className="text-sm text-foreground-muted">
                      Showing {startIndex + 1} to {Math.min(endIndex, filteredUsers.length)} of {filteredUsers.length} users
                    </p>
                    <div className="flex items-center gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                        disabled={currentPage === 1}
                      >
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
                      </Button>
                    </div>
                  </div>
                )}
              </>
            )}
          </TabsContent>

          {canCreateUser && (
            <TabsContent value="import" className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Upload className="w-5 h-5" />
                    Import users from CSV
                  </CardTitle>
                  <CardDescription>
                    Upload a CSV with columns: email, full_name, role. Users are created as PENDING and invitation links are generated.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex flex-wrap items-center gap-3">
                    <Input
                      type="file"
                      accept=".csv"
                      className="max-w-xs"
                      onChange={(e) => {
                        const f = e.target.files?.[0]
                        setImportFile(f || null)
                        setImportResult(null)
                      }}
                    />
                    <Button
                      disabled={!importFile || importLoading}
                      onClick={handleImportCsv}
                    >
                      {importLoading ? (
                        <>
                          <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                          Uploading...
                        </>
                      ) : (
                        <>
                          <Upload className="w-4 h-4 mr-2" />
                          Upload & import
                        </>
                      )}
                    </Button>
                  </div>
                  {importResult && (
                    <div className="space-y-4">
                      <div className="flex flex-wrap gap-4 p-3 rounded-lg bg-muted/50 text-sm">
                        <span>Imported: <strong>{importResult.imported}</strong></span>
                        <span>Skipped (duplicates): <strong>{importResult.skipped_duplicates}</strong></span>
                        {importResult.errors.length > 0 && (
                          <span className="text-destructive">Errors: {importResult.errors.length}</span>
                        )}
                      </div>
                      {importResult.errors.length > 0 && (
                        <div className="p-3 rounded-lg border border-destructive/30 bg-destructive/5">
                          <p className="text-sm font-medium text-destructive mb-2">Row errors</p>
                          <ul className="text-xs text-destructive list-disc list-inside">
                            {importResult.errors.map((err, i) => (
                              <li key={i}>{err}</li>
                            ))}
                          </ul>
                        </div>
                      )}
                      {importResult.invitation_tokens.length > 0 && (
                        <div className="rounded-md border">
                          <p className="p-3 border-b text-sm font-medium bg-muted/30">
                            Imported users ({importResult.invitation_tokens.length}) — share invite links via Actions
                          </p>
                          <div className="max-h-[480px] overflow-auto">
                            <Table>
                              <TableHeader>
                                <TableRow>
                                  <TableHead className="whitespace-nowrap">#</TableHead>
                                  <TableHead className="whitespace-nowrap">Email</TableHead>
                                  <TableHead className="whitespace-nowrap">Full name</TableHead>
                                  <TableHead className="whitespace-nowrap">Role</TableHead>
                                  <TableHead className="whitespace-nowrap">Expires</TableHead>
                                  <TableHead className="whitespace-nowrap text-right w-[180px]">Actions</TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {importResult.invitation_tokens.map((row, idx) => (
                                  <ImportResultRow key={idx} index={idx + 1} row={row} />
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          )}

          {canViewAuditLogs && (
            <TabsContent value="audit">
              <AuditLogViewer />
            </TabsContent>
          )}
        </Tabs>

        {/* User Modal */}
        {canCreateUser || canManageRoles ? (
          <UserModal
            isOpen={isModalOpen}
            onClose={() => {
              setIsModalOpen(false)
              setEditingUser(null)
            }}
            onSave={handleSaveUser}
            editingUser={editingUser}
          />
        ) : null}

        {/* Invite User Modal */}
        {(canCreateUser || canManageRoles) && (
          <InviteUserModal
            isOpen={inviteModalOpen}
            onClose={() => setInviteModalOpen(false)}
            onSuccess={handleInviteSuccess}
          />
        )}

      </div>
    </ProtectedPage>
  )
}
