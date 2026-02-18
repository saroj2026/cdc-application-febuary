"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { AlertCircle, Loader2, Mail, UserCog } from "lucide-react"
import { apiClient } from "@/lib/api/client"

interface InviteUserModalProps {
  isOpen: boolean
  onClose: () => void
  onSuccess: () => void
}

const ROLES = [
  { value: "viewer", label: "Viewer" },
  { value: "operator", label: "Operator" },
  { value: "data_engineer", label: "Data Engineer" },
  { value: "org_admin", label: "Org Admin" },
  { value: "super_admin", label: "Super Admin" },
]

export function InviteUserModal({ isOpen, onClose, onSuccess }: InviteUserModalProps) {
  const [email, setEmail] = useState("")
  const [role, setRole] = useState("viewer")
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [inviteLink, setInviteLink] = useState<string | null>(null)

  useEffect(() => {
    if (!isOpen) {
      setEmail("")
      setRole("viewer")
      setError(null)
      setInviteLink(null)
    }
  }, [isOpen])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    setInviteLink(null)
    if (!email.trim() || !email.includes("@")) {
      setError("Please enter a valid email address.")
      return
    }
    try {
      setIsSubmitting(true)
      const res = await apiClient.createInvitation({ email: email.trim().toLowerCase(), role })
      const baseUrl = typeof window !== "undefined" ? window.location.origin : ""
      setInviteLink(`${baseUrl}/auth/accept-invite?token=${res.token}`)
      onSuccess()
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || "Failed to send invitation.")
    } finally {
      setIsSubmitting(false)
    }
  }

  const handleClose = () => {
    setInviteLink(null)
    onClose()
  }

  const copyLink = () => {
    if (inviteLink && typeof navigator !== "undefined") {
      navigator.clipboard.writeText(inviteLink)
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && handleClose()}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Mail className="w-5 h-5" />
            Invite User
          </DialogTitle>
          <DialogDescription>
            Send an invitation link. The user will set their password when they accept.
          </DialogDescription>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          {error && (
            <div className="p-3 bg-destructive/10 border border-destructive/30 rounded-lg flex items-start gap-2">
              <AlertCircle className="w-4 h-4 text-destructive mt-0.5 flex-shrink-0" />
              <p className="text-sm text-destructive">{error}</p>
            </div>
          )}

          {inviteLink ? (
            <div className="space-y-3">
              <p className="text-sm text-muted-foreground">Invitation created. Share this link with the user:</p>
              <div className="flex gap-2">
                <Input readOnly value={inviteLink} className="font-mono text-xs" />
                <Button type="button" variant="outline" size="sm" onClick={copyLink}>
                  Copy
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Link expires in 7 days. User can set their password at the accept page.
              </p>
            </div>
          ) : (
            <>
              <div className="space-y-2">
                <Label htmlFor="invite-email">Email</Label>
                <Input
                  id="invite-email"
                  type="email"
                  placeholder="user@company.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="invite-role">Role</Label>
                <Select value={role} onValueChange={setRole}>
                  <SelectTrigger id="invite-role">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {ROLES.map((r) => (
                      <SelectItem key={r.value} value={r.value}>
                        {r.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </>
          )}

          <DialogFooter>
            {inviteLink ? (
              <Button type="button" onClick={handleClose}>
                Done
              </Button>
            ) : (
              <>
                <Button type="button" variant="outline" onClick={handleClose}>
                  Cancel
                </Button>
                <Button type="submit" disabled={isSubmitting}>
                  {isSubmitting ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Sending...
                    </>
                  ) : (
                    "Send invitation"
                  )}
                </Button>
              </>
            )}
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
