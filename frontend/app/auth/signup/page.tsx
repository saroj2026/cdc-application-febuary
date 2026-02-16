"use client"

import type React from "react"

import { useState, useEffect, useRef } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card } from "@/components/ui/card"
import { AlertCircle, Mail, Lock, User, Moon, Sun, ArrowRight, CheckCircle, Shield, GitBranch, Activity, Zap, Sparkles } from "lucide-react"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { ThemeProvider, useTheme } from "@/contexts/theme-context"
import { DatabaseLogo } from "@/lib/database-logo-loader"

// Popular databases to showcase with original logos - positioned on left and right sides
const featuredDatabases = [
  { id: "postgresql", name: "PostgreSQL", connectionType: "postgresql", displayName: "PostgreSQL", x: 8, y: 20, delay: 0, side: "left" },
  { id: "mysql", name: "MySQL", connectionType: "mysql", displayName: "MySQL", x: 8, y: 40, delay: 0.3, side: "left" },
  { id: "mongodb", name: "MongoDB", connectionType: "mongodb", displayName: "MongoDB", x: 8, y: 60, delay: 0.6, side: "left" },
  { id: "mariadb", name: "MariaDB", connectionType: "mysql", displayName: "MariaDB", databaseId: "mariadb", x: 8, y: 80, delay: 0.9, side: "left" },
  { id: "sqlserver", name: "SQL Server", connectionType: "sqlserver", displayName: "SQL Server", x: 92, y: 20, delay: 1.2, side: "right" },
  { id: "oracle", name: "Oracle", connectionType: "oracle", displayName: "Oracle", x: 92, y: 40, delay: 1.5, side: "right" },
  { id: "snowflake", name: "Snowflake", connectionType: "snowflake", displayName: "Snowflake", x: 92, y: 60, delay: 1.8, side: "right" },
  { id: "redshift", name: "Redshift", connectionType: "redshift", displayName: "Redshift", x: 92, y: 80, delay: 2.1, side: "right" },
]

function SignupContent() {
  const router = useRouter()
  const { theme, toggleTheme } = useTheme()
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState("")
  const [formData, setFormData] = useState({
    fullName: "",
    email: "",
    password: "",
    confirmPassword: "",
    role: "",
  })
  const [showPassword, setShowPassword] = useState(false)
  const [showConfirmPassword, setShowConfirmPassword] = useState(false)
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData((prev) => ({
      ...prev,
      [e.target.name]: e.target.value,
    }))
    setError("")
  }

  const handleRoleChange = (value: string) => {
    setFormData((prev) => ({
      ...prev,
      role: value,
    }))
    setError("")
  }

  const validateForm = () => {
    if (!formData.fullName.trim()) {
      setError("Full name is required")
      return false
    }

    if (!formData.email.trim()) {
      setError("Email is required")
      return false
    }

    if (!formData.role) {
      setError("Please select a role")
      return false
    }

    if (formData.password.length < 8) {
      setError("Password must be at least 8 characters")
      return false
    }

    if (formData.password !== formData.confirmPassword) {
      setError("Passwords do not match")
      return false
    }

    return true
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError("")

    if (!validateForm()) {
      return
    }

    setIsLoading(true)

    try {
      const apiUrl = `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}/api/v1/users/`;

      if (!formData.role) {
        setError("Please select a role")
        return
      }

      const payload = {
        full_name: formData.fullName,
        email: formData.email,
        password: formData.password,
        role_name: formData.role,
      };

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000);

      let response: Response;
      try {
        response = await fetch(apiUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
          },
          body: JSON.stringify(payload),
          signal: controller.signal,
        });
        clearTimeout(timeoutId);
      } catch (fetchError: any) {
        clearTimeout(timeoutId);
        if (fetchError.name === 'AbortError') {
          throw new Error('Request timeout: Backend did not respond within 10 seconds. Please ensure the backend server is running on http://localhost:8000');
        }
        if (fetchError.message?.includes('Failed to fetch') || fetchError.message?.includes('NetworkError')) {
          throw new Error(`Network error: Cannot reach backend server. Is it running on ${apiUrl}?`);
        }
        throw fetchError;
      }

      const contentType = response.headers.get('content-type');
      let data;

      if (contentType && contentType.includes('application/json')) {
        data = await response.json();
      } else {
        const responseText = await response.text();
        throw new Error(`Server returned non-JSON response: ${responseText.substring(0, 200)}`);
      }

      if (!response.ok) {
        const errorMessage = data.detail || data.message || `Server error: ${response.status}`;
        throw new Error(errorMessage);
      }

      router.push("/auth/login?registered=true")
    } catch (err: any) {
      let errorMessage = err.message || "Failed to create account. Please try again.";

      if (err.message?.includes('Failed to fetch') || err.message?.includes('NetworkError')) {
        errorMessage = `Cannot connect to backend server. Please ensure the backend is running on http://localhost:8000`;
      } else if (err.message?.includes('timeout')) {
        errorMessage = `Request timed out. The backend server may be slow or unresponsive.`;
      }

      setError(errorMessage);
    } finally {
      setIsLoading(false)
    }
  }

  const passwordStrength = () => {
    if (formData.password.length === 0) return { strength: 0, label: "" }
    if (formData.password.length < 8) return { strength: 1, label: "Weak" }
    if (formData.password.length < 12) return { strength: 2, label: "Medium" }
    return { strength: 3, label: "Strong" }
  }

  const strength = passwordStrength()

  // Animated network canvas background
  useEffect(() => {
    if (!canvasRef.current || !mounted) return

    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const resizeCanvas = () => {
      canvas.width = window.innerWidth
      canvas.height = window.innerHeight
    }
    resizeCanvas()
    window.addEventListener('resize', resizeCanvas)

    const nodes = featuredDatabases.map((db, idx) => ({
      x: (db.x / 100) * canvas.width,
      y: (db.y / 100) * canvas.height,
      side: db.side,
      idx
    }))

    let animationFrame: number
    let time = 0

    const draw = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height)
      time += 0.01

      // Connect left side databases to right side databases
      const leftNodes = nodes.filter(node => node.side === "left")
      const rightNodes = nodes.filter(node => node.side === "right")

      // Draw connection lines (spider web pattern)
      ctx.strokeStyle = `rgba(20, 184, 166, ${0.3 + Math.sin(time) * 0.15})`
      ctx.lineWidth = 2
      ctx.setLineDash([8, 4])

      const connections: Array<{ from: typeof rightNodes[0], to: typeof leftNodes[0], idx: number }> = []

      leftNodes.forEach((leftNode, leftIdx) => {
        rightNodes.forEach((rightNode, rightIdx) => {
          // Draw connection line
          ctx.beginPath()
          ctx.moveTo(rightNode.x, rightNode.y)
          ctx.lineTo(leftNode.x, leftNode.y)
          ctx.stroke()

          connections.push({ from: rightNode, to: leftNode, idx: leftIdx * rightNodes.length + rightIdx })
        })
      })

      ctx.setLineDash([])

      // Draw data replication packets moving from RIGHT to LEFT
      connections.forEach((conn, connIdx) => {
        // Multiple data packets per connection for continuous flow
        for (let packetNum = 0; packetNum < 3; packetNum++) {
          const baseProgress = (time * 0.5 + connIdx * 0.1 + packetNum * 0.33) % 1
          const progress = baseProgress // Data flows from right (0) to left (1)

          // Calculate position along the line
          const x = conn.from.x + (conn.to.x - conn.from.x) * progress
          const y = conn.from.y + (conn.to.y - conn.from.y) * progress

          // Draw data packet with glow effect
          const packetSize = 6 + Math.sin(time * 3 + connIdx) * 2
          const opacity = 0.9 - progress * 0.3

          // Outer glow
          const gradient = ctx.createRadialGradient(x, y, 0, x, y, packetSize * 2)
          gradient.addColorStop(0, `rgba(20, 184, 166, ${opacity * 0.6})`)
          gradient.addColorStop(0.5, `rgba(20, 184, 166, ${opacity * 0.3})`)
          gradient.addColorStop(1, `rgba(20, 184, 166, 0)`)
          ctx.fillStyle = gradient
          ctx.beginPath()
          ctx.arc(x, y, packetSize * 2, 0, Math.PI * 2)
          ctx.fill()

          // Main packet (data icon representation)
          ctx.fillStyle = `rgba(20, 184, 166, ${opacity})`
          ctx.beginPath()
          ctx.arc(x, y, packetSize, 0, Math.PI * 2)
          ctx.fill()

          // Inner highlight
          ctx.fillStyle = `rgba(255, 255, 255, ${opacity * 0.5})`
          ctx.beginPath()
          ctx.arc(x - packetSize * 0.3, y - packetSize * 0.3, packetSize * 0.4, 0, Math.PI * 2)
          ctx.fill()

          // Draw trail behind packet
          for (let i = 1; i <= 5; i++) {
            const trailProgress = progress - i * 0.05
            if (trailProgress > 0) {
              const trailX = conn.from.x + (conn.to.x - conn.from.x) * trailProgress
              const trailY = conn.from.y + (conn.to.y - conn.from.y) * trailProgress
              ctx.fillStyle = `rgba(20, 184, 166, ${opacity * (0.2 - i * 0.03)})`
              ctx.beginPath()
              ctx.arc(trailX, trailY, packetSize * (1 - i * 0.15), 0, Math.PI * 2)
              ctx.fill()
            }
          }
        }
      })

      // Draw pulsing nodes at database positions
      nodes.forEach((node, idx) => {
        const pulse = Math.sin(time * 2 + idx) * 0.2 + 1

        // Outer pulse ring
        ctx.fillStyle = `rgba(20, 184, 166, ${0.2 * pulse})`
        ctx.beginPath()
        ctx.arc(node.x, node.y, 14 * pulse, 0, Math.PI * 2)
        ctx.fill()

        // Inner node
        ctx.fillStyle = node.side === "right" ? 'rgba(20, 184, 166, 0.9)' : 'rgba(34, 197, 94, 0.9)'
        ctx.beginPath()
        ctx.arc(node.x, node.y, 7, 0, Math.PI * 2)
        ctx.fill()

        // Center highlight
        ctx.fillStyle = 'rgba(255, 255, 255, 0.6)'
        ctx.beginPath()
        ctx.arc(node.x - 2, node.y - 2, 2, 0, Math.PI * 2)
        ctx.fill()
      })

      // Draw "receiving" effect at left databases when data arrives
      leftNodes.forEach((leftNode, leftIdx) => {
        const receiveTime = (time * 0.5 + leftIdx * 0.2) % 1
        if (receiveTime < 0.1) {
          const pulseSize = receiveTime * 50
          ctx.strokeStyle = `rgba(34, 197, 94, ${1 - receiveTime * 10})`
          ctx.lineWidth = 3
          ctx.beginPath()
          ctx.arc(leftNode.x, leftNode.y, 20 + pulseSize, 0, Math.PI * 2)
          ctx.stroke()
        }
      })

      animationFrame = requestAnimationFrame(draw)
    }

    draw()

    return () => {
      window.removeEventListener('resize', resizeCanvas)
      cancelAnimationFrame(animationFrame)
    }
  }, [mounted])

  return (
    <div className="h-screen bg-background flex items-center justify-center p-4 relative overflow-hidden">
      {/* Animated Canvas Background - Spider Web Connections */}
      <canvas
        ref={canvasRef}
        className="absolute inset-0 w-full h-full opacity-60"
        style={{ zIndex: 0 }}
      />

      {/* Floating Database Logos - Left and Right Sides */}
      <div className="absolute inset-0 overflow-hidden" style={{ zIndex: 1 }}>
        {featuredDatabases.map((db, idx) => (
          <div
            key={db.id}
            className="absolute database-float"
            style={{
              left: `${db.x}%`,
              top: `${db.y}%`,
              animationDelay: `${db.delay}s`,
              transform: 'translate(-50%, -50%)',
              zIndex: 2, // Above canvas but below form
            }}
          >
            <div className="flex flex-col items-center group cursor-default">
              <div className="relative">
                <div className="absolute inset-0 bg-primary/30 rounded-2xl blur-xl opacity-0 group-hover:opacity-100 transition-opacity duration-500 database-glow" />

                <div className="relative w-16 h-16 rounded-2xl bg-gradient-to-br from-surface/90 via-surface/80 to-surface/70 backdrop-blur-md border-2 border-primary/30 flex items-center justify-center shadow-2xl shadow-primary/20 group-hover:shadow-primary/40 group-hover:scale-110 group-hover:border-primary/60 transition-all duration-500 database-card">
                  <DatabaseLogo
                    connectionType={db.connectionType}
                    databaseId={db.databaseId || db.id}
                    displayName={db.displayName}
                    size={40}
                    className="w-10 h-10"
                  />

                  <div className="absolute -top-1 -right-1 w-4 h-4 bg-primary rounded-full border-2 border-surface animate-ping-slow" />
                  <div className="absolute -top-1 -right-1 w-4 h-4 bg-primary rounded-full border-2 border-surface" />
                </div>
              </div>

              <div className="mt-3 px-3 py-1.5 bg-surface/80 backdrop-blur-sm rounded-lg border border-primary/20 shadow-lg opacity-0 group-hover:opacity-100 transition-all duration-300 transform translate-y-2 group-hover:translate-y-0">
                <span className="text-xs font-semibold text-primary whitespace-nowrap">
                  {db.displayName}
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Gradient Overlays */}
      <div className="absolute inset-0 bg-gradient-to-br from-primary/10 via-transparent to-primary/5" style={{ zIndex: 1 }} />
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_50%,rgba(20,184,166,0.15),transparent_70%)]" style={{ zIndex: 1 }} />

      {/* Theme Toggle */}
      <div className="absolute top-6 right-6 z-20" suppressHydrationWarning>
        <button
          onClick={toggleTheme}
          className="group relative p-3 bg-surface/80 backdrop-blur-sm hover:bg-surface-hover border border-border rounded-xl transition-all duration-300 shadow-lg hover:shadow-xl hover:scale-105"
          aria-label="Toggle theme"
          suppressHydrationWarning
        >
          <div className="relative w-5 h-5" suppressHydrationWarning>
            <Sun className={`w-5 h-5 text-foreground-muted transition-all duration-300 ${mounted && theme === "dark" ? "block" : "hidden"}`} />
            <Moon className={`w-5 h-5 text-foreground-muted transition-all duration-300 ${mounted && theme === "light" ? "block" : "hidden"}`} />
            {!mounted && <Moon className="w-5 h-5 text-foreground-muted transition-all duration-300" />}
          </div>
        </button>
      </div>

      <div className="w-full max-w-3xl relative z-10">
        {/* Signup Card */}
        <Card className="bg-surface/80 backdrop-blur-xl border-primary/30 shadow-2xl shadow-primary/20 relative overflow-hidden signup-card">
          {/* Animated border glow */}
          <div className="absolute inset-0 rounded-lg bg-gradient-to-r from-primary/0 via-primary/30 to-primary/0 animate-shimmer opacity-60" />

          <div className="relative z-10 flex flex-col lg:flex-row">
            {/* Logo Section - Left Side */}
            <div className="flex-shrink-0 p-6 lg:p-8 border-b lg:border-b-0 lg:border-r border-primary/30 bg-gradient-to-br from-primary/10 via-primary/5 to-transparent flex flex-col items-center justify-center min-w-[220px] logo-section">
              <div className="relative mb-4">
                <div className="w-20 h-20 bg-gradient-to-br from-primary via-primary/90 to-primary/80 rounded-2xl flex items-center justify-center shadow-2xl shadow-primary/40 animate-pulse-slow relative z-10">
                  <Sparkles className="w-10 h-10 text-white" />
                </div>

                <div className="absolute inset-0 animate-orbit" style={{ animationDuration: '15s' }}>
                  <div className="absolute top-0 left-1/2 -translate-x-1/2 -translate-y-full -mt-3">
                    <div className="w-8 h-8 rounded-lg bg-primary/20 backdrop-blur-sm border border-primary/30 flex items-center justify-center">
                      <User className="w-4 h-4 text-primary" />
                    </div>
                  </div>
                  <div className="absolute bottom-0 left-1/2 -translate-x-1/2 translate-y-full mt-3">
                    <div className="w-8 h-8 rounded-lg bg-primary/20 backdrop-blur-sm border border-primary/30 flex items-center justify-center">
                      <GitBranch className="w-4 h-4 text-primary" />
                    </div>
                  </div>
                  <div className="absolute left-0 top-1/2 -translate-y-1/2 -translate-x-full -ml-3">
                    <div className="w-8 h-8 rounded-lg bg-primary/20 backdrop-blur-sm border border-primary/30 flex items-center justify-center">
                      <Activity className="w-4 h-4 text-primary" />
                    </div>
                  </div>
                  <div className="absolute right-0 top-1/2 -translate-y-1/2 translate-x-full mr-3">
                    <div className="w-8 h-8 rounded-lg bg-primary/20 backdrop-blur-sm border border-primary/30 flex items-center justify-center">
                      <Zap className="w-4 h-4 text-primary" />
                    </div>
                  </div>
                </div>

                <div className="absolute inset-0 rounded-2xl border-2 border-primary/40 animate-ping-slow" />
                <div className="absolute inset-0 rounded-2xl border-2 border-primary/20 animate-ping-slow" style={{ animationDelay: '0.5s' }} />
              </div>

              <h1 className="text-3xl font-bold mb-2 bg-gradient-to-r from-primary via-primary/90 to-primary bg-clip-text text-transparent text-center">
                CDC Nexus
              </h1>
              <p className="text-primary/80 text-sm font-medium text-center mb-3">Create Your Account</p>

              <div className="flex items-center justify-center gap-2 px-4 py-2 bg-primary/10 rounded-full border border-primary/20">
                <div className="flex gap-1">
                  <div className="w-2 h-2 bg-primary rounded-full animate-pulse" />
                  <div className="w-2 h-2 bg-primary rounded-full animate-pulse" style={{ animationDelay: '0.2s' }} />
                  <div className="w-2 h-2 bg-primary rounded-full animate-pulse" style={{ animationDelay: '0.4s' }} />
                </div>
                <span className="text-xs font-semibold text-primary">Get Started</span>
              </div>
            </div>

            {/* Form Section - Right Side */}
            <div className="flex-1 p-6 lg:p-8">
              <div className="mb-6">
                <h2 className="text-2xl font-bold text-foreground mb-2">Create Account</h2>
                <p className="text-sm text-foreground-muted">Sign up to start using CDC Nexus</p>
              </div>

              <form onSubmit={handleSubmit} className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="block text-sm font-semibold text-foreground">Full Name</label>
                    <div className="relative">
                      <User className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-primary/60" />
                      <Input
                        type="text"
                        name="fullName"
                        placeholder="John Doe"
                        value={formData.fullName}
                        onChange={handleChange}
                        className="pl-10 h-12 text-sm bg-input/50 border-border focus:ring-2 focus:ring-primary/30 focus:border-primary/50 transition-all"
                        disabled={isLoading}
                        required
                      />
                    </div>
                  </div>

                  <div className="space-y-2">
                    <label className="block text-sm font-semibold text-foreground">Email Address</label>
                    <div className="relative">
                      <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-primary/60" />
                      <Input
                        type="email"
                        name="email"
                        placeholder="you@example.com"
                        value={formData.email}
                        onChange={handleChange}
                        className="pl-10 h-12 text-sm bg-input/50 border-border focus:ring-2 focus:ring-primary/30 focus:border-primary/50 transition-all"
                        disabled={isLoading}
                        required
                      />
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="block text-sm font-semibold text-foreground">Password</label>
                    <div className="relative">
                      <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-primary/60" />
                      <Input
                        type={showPassword ? "text" : "password"}
                        name="password"
                        placeholder="••••••••"
                        value={formData.password}
                        onChange={handleChange}
                        className="pl-10 pr-10 h-12 text-sm bg-input/50 border-border focus:ring-2 focus:ring-primary/30 focus:border-primary/50 transition-all"
                        disabled={isLoading}
                        autoComplete="new-password"
                        required
                      />
                      <button
                        type="button"
                        onClick={() => setShowPassword(!showPassword)}
                        className="absolute right-3 top-1/2 -translate-y-1/2 text-foreground-muted hover:text-primary transition-colors"
                      >
                        <Lock className="w-4 h-4" />
                      </button>
                    </div>
                    {formData.password && (
                      <div className="mt-2">
                        <div className="flex gap-1 mb-1">
                          {[1, 2, 3].map((level) => (
                            <div
                              key={level}
                              className={`h-1.5 flex-1 rounded-full transition-all duration-300 ${level <= strength.strength
                                  ? level === 1
                                    ? "bg-error"
                                    : level === 2
                                      ? "bg-warning"
                                      : "bg-success"
                                  : "bg-muted"
                                }`}
                            />
                          ))}
                        </div>
                        <p className="text-xs text-foreground-muted">
                          {strength.label && `Password strength: ${strength.label}`}
                        </p>
                      </div>
                    )}
                  </div>

                  <div className="space-y-2">
                    <label className="block text-sm font-semibold text-foreground">Confirm Password</label>
                    <div className="relative">
                      <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-primary/60" />
                      <Input
                        type={showConfirmPassword ? "text" : "password"}
                        name="confirmPassword"
                        placeholder="••••••••"
                        value={formData.confirmPassword}
                        onChange={handleChange}
                        className="pl-10 pr-10 h-12 text-sm bg-input/50 border-border focus:ring-2 focus:ring-primary/30 focus:border-primary/50 transition-all"
                        disabled={isLoading}
                        autoComplete="new-password"
                        required
                      />
                      <button
                        type="button"
                        onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                        className="absolute right-3 top-1/2 -translate-y-1/2 text-foreground-muted hover:text-primary transition-colors"
                      >
                        <Lock className="w-4 h-4" />
                      </button>
                    </div>
                    {formData.confirmPassword && formData.password === formData.confirmPassword && (
                      <div className="flex items-center gap-1.5 text-xs text-success mt-1">
                        <CheckCircle className="w-3.5 h-3.5" />
                        <span>Passwords match</span>
                      </div>
                    )}
                  </div>
                </div>

                <div className="space-y-2">
                  <label className="block text-sm font-semibold text-foreground">Role</label>
                  <div className="relative">
                    <Shield className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-primary/60 z-10 pointer-events-none" />
                    <Select
                      value={formData.role}
                      onValueChange={handleRoleChange}
                      disabled={isLoading}
                    >
                      <SelectTrigger className="w-full pl-10 h-12 text-sm bg-input/50 border-border focus:ring-2 focus:ring-primary/30">
                        <SelectValue placeholder="Select a role" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="user">User</SelectItem>
                        <SelectItem value="operator">Operator</SelectItem>
                        <SelectItem value="viewer">Viewer</SelectItem>
                        <SelectItem value="admin">Admin</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {formData.role && (
                    <p className="text-xs text-foreground-muted mt-1">
                      {formData.role === "admin" && "Full access to all features and settings"}
                      {formData.role === "operator" && "Can manage connections and pipelines"}
                      {formData.role === "viewer" && "Read-only access to view data"}
                      {formData.role === "user" && "Standard user access"}
                    </p>
                  )}
                </div>

                {error && (
                  <div className="flex gap-2 p-3 bg-error/10 border border-error/30 rounded-lg animate-in slide-in-from-top-2">
                    <AlertCircle className="w-4 h-4 text-error flex-shrink-0 mt-0.5" />
                    <p className="text-sm text-error">{error}</p>
                  </div>
                )}

                <Button
                  type="submit"
                  className="w-full h-12 bg-gradient-to-r from-primary via-primary/90 to-primary hover:from-primary/90 hover:to-primary/80 text-white text-sm font-semibold shadow-xl shadow-primary/30 hover:shadow-primary/50 transition-all duration-300 hover:scale-[1.02]"
                  disabled={isLoading || !formData.role}
                >
                  {isLoading ? (
                    <span className="flex items-center gap-2">
                      <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                      Creating account...
                    </span>
                  ) : (
                    <span className="flex items-center gap-2">
                      Create Account
                      <ArrowRight className="w-4 h-4" />
                    </span>
                  )}
                </Button>

                <div className="relative py-4">
                  <div className="absolute inset-0 flex items-center">
                    <div className="w-full border-t border-border" />
                  </div>
                  <div className="relative flex justify-center text-xs">
                    <span className="px-3 bg-surface text-foreground-muted">Already have an account?</span>
                  </div>
                </div>

                <Button
                  type="button"
                  variant="outline"
                  className="w-full h-12 bg-transparent border-primary/30 hover:bg-primary/10 hover:border-primary/50 text-primary hover:text-primary/90 transition-all text-sm font-medium"
                  onClick={() => router.push("/auth/login")}
                  disabled={isLoading}
                >
                  Sign In
                </Button>
              </form>
            </div>
          </div>
        </Card>
      </div>

      <style jsx global>{`
        @keyframes float {
          0%, 100% {
            transform: translate(-50%, -50%) translateY(0px) rotate(0deg);
          }
          50% {
            transform: translate(-50%, -50%) translateY(-20px) rotate(5deg);
          }
        }

        @keyframes shimmer {
          0% {
            transform: translateX(-100%);
          }
          100% {
            transform: translateX(100%);
          }
        }

        @keyframes orbit {
          from {
            transform: rotate(0deg);
          }
          to {
            transform: rotate(360deg);
          }
        }

        @keyframes ping-slow {
          0%, 100% {
            transform: scale(1);
            opacity: 1;
          }
          50% {
            transform: scale(1.5);
            opacity: 0.5;
          }
        }

        @keyframes pulse-slow {
          0%, 100% {
            opacity: 1;
            transform: scale(1);
          }
          50% {
            opacity: 0.8;
            transform: scale(1.05);
          }
        }

        .database-float {
          animation: float 6s ease-in-out infinite;
        }

        .database-glow {
          animation: pulse-slow 3s ease-in-out infinite;
        }

        .database-card {
          transition: all 0.5s cubic-bezier(0.4, 0, 0.2, 1);
        }

        .animate-shimmer {
          animation: shimmer 3s linear infinite;
        }

        .animate-orbit {
          animation: orbit 15s linear infinite;
        }

        .animate-ping-slow {
          animation: ping-slow 2s ease-in-out infinite;
        }

        .animate-pulse-slow {
          animation: pulse-slow 3s ease-in-out infinite;
        }

        .signup-card {
          animation: fade-in 0.6s ease-out;
        }

        @keyframes fade-in {
          from {
            opacity: 0;
            transform: translateY(20px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }
      `}</style>
    </div>
  )
}

export default function SignupPage() {
  return (
    <ThemeProvider>
      <SignupContent />
    </ThemeProvider>
  )
}
