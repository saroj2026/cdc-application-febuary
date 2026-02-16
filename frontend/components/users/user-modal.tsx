"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { AlertCircle, Loader2, ShieldCheck, Shield, UserCog, Eye, Mail, User } from "lucide-react"
import { apiClient } from "@/lib/api/client"

interface UserModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: () => void
  editingUser?: any | null
}

const ROLES = [
  { value: "viewer", label: "Viewer", icon: Eye, description: "Read-only access to dashboards and logs" },
  { value: "operator", label: "Operator", icon: UserCog, description: "Start/stop pipelines, monitor health" },
  { value: "data_engineer", label: "Data Engineer", icon: UserCog, description: "Create pipelines, full load, CDC control" },
  { value: "org_admin", label: "Org Admin", icon: Shield, description: "Manages users, connections, pipelines" },
  { value: "super_admin", label: "Super Admin", icon: ShieldCheck, description: "Platform owner with full access" },
]

const STATUSES = [
  { value: "invited", label: "Invited" },
  { value: "active", label: "Active" },
  { value: "suspended", label: "Suspended" },
  { value: "deactivated", label: "Deactivated" },
]

export function UserModal({ isOpen, onClose, onSave, editingUser }: UserModalProps) {
  const [formData, setFormData] = useState({
    email: "",
    full_name: "",
    password: "",
    role_name: "viewer",
    status: "invited",
    is_active: true,
    is_superuser: false,
  })
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [showPassword, setShowPassword] = useState(false)

  useEffect(() => {
    if (editingUser) {
      setFormData({
        email: editingUser.email || "",
        full_name: editingUser.full_name || "",
        password: "", // Don't pre-fill password
        role_name: editingUser.role_name || "viewer",
        status: editingUser.status || (editingUser.is_active ? "active" : "invited"),
        is_active: editingUser.is_active ?? true,
        is_superuser: editingUser.is_superuser ?? false,
      })
      setShowPassword(false)
    } else {
      setFormData({
        email: "",
        full_name: "",
        password: "",
        role_name: "viewer",
        status: "invited",
        is_active: true,
        is_superuser: false,
      })
      setShowPassword(true)
    }
    setError(null)
  }, [editingUser, isOpen])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)

    // Validation
    if (!formData.email.trim()) {
      setError("Email is required")
      return
    }
    if (!formData.full_name.trim()) {
      setError("Full name is required")
      return
    }
    if (!editingUser && !formData.password.trim()) {
      setError("Password is required for new users")
      return
    }

    try {
      setIsSubmitting(true)

      if (editingUser) {
        // Update user
        const updateData: any = {
          email: formData.email,
          full_name: formData.full_name,
          role_name: formData.role_name,
          status: formData.status,
          is_active: formData.is_active,
          is_superuser: formData.is_superuser,
        }
        if (formData.password.trim()) {
          updateData.password = formData.password
        }
        await apiClient.client.put(`/api/v1/users/${editingUser.id}`, updateData)
      } else {
        // Create user
        await apiClient.client.post("/api/v1/users", {
          email: formData.email,
          full_name: formData.full_name,
          password: formData.password,
          role_name: formData.role_name,
          status: formData.status,
        })
      }

      onSave()
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || "Failed to save user")
      console.error("Error saving user:", err)
    } finally {
      setIsSubmitting(false)
    }
  }

  const selectedRole = ROLES.find(r => r.value === formData.role_name)

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <User className="w-5 h-5" />
            {editingUser ? "Edit User" : "Create New User"}
          </DialogTitle>
          <DialogDescription>
            {editingUser
              ? "Update user information and permissions"
              : "Add a new user to your CDC replication platform"}
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-6">
          {error && (
            <div className="p-3 bg-error/10 border border-error/30 rounded-lg flex items-start gap-2">
              <AlertCircle className="w-4 h-4 text-error mt-0.5 flex-shrink-0" />
              <p className="text-sm text-error">{error}</p>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="email" className="flex items-center gap-2">
                <Mail className="w-4 h-4" />
                Email Address
              </Label>
              <Input
                id="email"
                type="email"
                placeholder="user@example.com"
                value={formData.email}
                onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                required
                disabled={!!editingUser} // Email cannot be changed
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="full_name" className="flex items-center gap-2">
                <User className="w-4 h-4" />
                Full Name
              </Label>
              <Input
                id="full_name"
                type="text"
                placeholder="John Doe"
                value={formData.full_name}
                onChange={(e) => setFormData({ ...formData, full_name: e.target.value })}
                required
              />
            </div>
          </div>

          {(!editingUser || showPassword) && (
            <div className="space-y-2">
              <Label htmlFor="password">
                Password {editingUser && "(leave blank to keep current)"}
              </Label>
              <Input
                id="password"
                type="password"
                placeholder={editingUser ? "Enter new password" : "Enter password"}
                value={formData.password}
                onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                required={!editingUser}
              />
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="role_name">Role</Label>
              <Select value={formData.role_name} onValueChange={(value) => setFormData({ ...formData, role_name: value })}>
                <SelectTrigger id="role_name">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {ROLES.map((role) => {
                    const RoleIcon = role.icon
                    return (
                      <SelectItem key={role.value} value={role.value}>
                        <div className="flex items-center gap-2">
                          <RoleIcon className="w-4 h-4" />
                          <span>{role.label}</span>
                        </div>
                      </SelectItem>
                    )
                  })}
                </SelectContent>
              </Select>
              {selectedRole && (
                <p className="text-xs text-foreground-muted">{selectedRole.description}</p>
              )}
            </div>

            <div className="space-y-2">
              <Label htmlFor="status">Status</Label>
              <Select value={formData.status} onValueChange={(value) => setFormData({ ...formData, status: value })}>
                <SelectTrigger id="status">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STATUSES.map((status) => (
                    <SelectItem key={status.value} value={status.value}>
                      {status.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-4 border-t pt-4">
            <div className="flex items-center justify-between">
              <div className="space-y-0.5">
                <Label htmlFor="is_active">Active</Label>
                <p className="text-xs text-foreground-muted">User can log in and access the platform</p>
              </div>
              <Switch
                id="is_active"
                checked={formData.is_active}
                onCheckedChange={(checked) => setFormData({ ...formData, is_active: checked })}
              />
            </div>

            <div className="flex items-center justify-between">
              <div className="space-y-0.5">
                <Label htmlFor="is_superuser">Super User</Label>
                <p className="text-xs text-foreground-muted">Full platform access (use with caution)</p>
              </div>
              <Switch
                id="is_superuser"
                checked={formData.is_superuser}
                onCheckedChange={(checked) => setFormData({ ...formData, is_superuser: checked })}
              />
            </div>
          </div>

          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose} disabled={isSubmitting}>
              Cancel
            </Button>
            <Button type="submit" disabled={isSubmitting} className="bg-primary hover:bg-primary/90">
              {isSubmitting ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  Saving...
                </>
              ) : (
                editingUser ? "Update User" : "Create User"
              )}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

