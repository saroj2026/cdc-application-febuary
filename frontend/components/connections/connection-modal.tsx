"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { AlertCircle, CheckCircle, ChevronLeft } from "lucide-react"
import { DatabaseSelector } from "./database-selector"
import { DatabaseInfo, getDatabaseByConnectionType } from "@/lib/database-icons"
import { DatabaseLogo } from "@/lib/database-logo-loader"

interface ConnectionModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (data: any) => void
  editingConnection?: any | null
}

export function ConnectionModal({ isOpen, onClose, onSave, editingConnection }: ConnectionModalProps) {
  const [step, setStep] = useState<"select" | "configure">("select")
  const [selectedDatabase, setSelectedDatabase] = useState<DatabaseInfo | null>(null)
  const [formData, setFormData] = useState({
    name: "",
    description: "",
    engine: "",
    host: "",
    port: "",
    database: "",
    username: "",
    password: "",
    ssl_enabled: false,
    // Snowflake-specific fields
    account: "",
    warehouse: "",
    role: "",
    schema_name: "",
    private_key: "",
    private_key_passphrase: "",
    auth_method: "password", // "password" or "private_key"
    // S3-specific fields
    region: "",
    endpoint_url: "",
  })
  const [testStatus, setTestStatus] = useState<"idle" | "testing" | "success" | "error">("idle")
  const [testMessage, setTestMessage] = useState("")

  // Load editing connection data
  useEffect(() => {
    if (editingConnection) {
      const dbInfo = getDatabaseByConnectionType(editingConnection.connection_type)
      setSelectedDatabase(dbInfo || null)
      setStep("configure")
      const additionalConfig = editingConnection.additional_config || {}
      setFormData({
        name: editingConnection.name || "",
        description: editingConnection.description || "",
        engine: editingConnection.database_type || editingConnection.connection_type || "",
        host: editingConnection.host || "",
        port: String(editingConnection.port || ""),
        database: editingConnection.database || "",
        username: editingConnection.username || "",
        password: "", // Don't pre-fill password
        ssl_enabled: editingConnection.ssl_enabled || false,
        // Snowflake-specific fields
        account: editingConnection.host || additionalConfig.account || "",
        warehouse: additionalConfig.warehouse || "",
        role: additionalConfig.role || "",
        schema_name: editingConnection.schema_name || editingConnection.schema || "",
        private_key: "",
        private_key_passphrase: "",
        auth_method: additionalConfig.private_key ? "private_key" : "password",
        // S3-specific fields
        region: additionalConfig.region_name || "",
        endpoint_url: additionalConfig.endpoint_url || "",
      })
    } else {
      // Reset form when creating new
      setStep("select")
      setSelectedDatabase(null)
      setFormData({
        name: "",
        description: "",
        engine: "",
        host: "",
        port: "",
        database: "",
        username: "",
        password: "",
        ssl_enabled: false,
        // Snowflake-specific fields
        account: "",
        warehouse: "",
        role: "",
        schema_name: "",
        private_key: "",
        private_key_passphrase: "",
        auth_method: "password",
      })
    }
    setTestStatus("idle")
    setTestMessage("")
  }, [editingConnection, isOpen])

  // Handle database selection
  const handleDatabaseSelect = (database: DatabaseInfo) => {
    setSelectedDatabase(database)
    setFormData(prev => ({
      ...prev,
      engine: database.connectionType,
      port: String(database.defaultPort || ""),
    }))
    setStep("configure")
  }

  // Go back to database selection
  const handleBack = () => {
    setStep("select")
    setSelectedDatabase(null)
    setFormData({
      name: "",
      description: "",
      engine: "",
      host: "",
      port: "",
      database: "",
      username: "",
      password: "",
      ssl_enabled: false,
      // Snowflake-specific fields
      account: "",
      warehouse: "",
      role: "",
      schema_name: "",
      private_key: "",
      private_key_passphrase: "",
      auth_method: "password",
      // S3-specific fields
      region: "",
      endpoint_url: "",
    })
    setTestStatus("idle")
    setTestMessage("")
  }

  const handleInputChange = (field: string, value: string | boolean) => {
    setFormData((prev) => ({ ...prev, [field]: value }))
    setTestStatus("idle")
  }

  const handleTestConnection = async () => {
    const isSnowflake = formData.engine === "snowflake"
    const isS3 = formData.engine === "s3" || formData.engine === "aws_s3"
    
    // Validate required fields
    const missingFields = []
    if (isSnowflake) {
      if (!formData.account || !formData.account.trim()) missingFields.push("Account")
      if (!formData.database || !formData.database.trim()) missingFields.push("Database")
      if (!formData.username || !formData.username.trim()) missingFields.push("Username")
      if (!formData.password) missingFields.push("Password")
      if (!formData.private_key || !formData.private_key.trim()) missingFields.push("Private Key")
    } else if (isS3) {
      if (!formData.database || !formData.database.trim()) missingFields.push("Bucket Name")
      if (!formData.username || !formData.username.trim()) missingFields.push("AWS Access Key ID")
      if (!formData.password || formData.password.trim() === "") missingFields.push("AWS Secret Access Key")
    } else {
      if (!formData.host || !formData.host.trim()) missingFields.push("Host")
      if (!formData.port || !formData.port.trim() || isNaN(parseInt(formData.port))) missingFields.push("Port")
      if (!formData.database || !formData.database.trim()) missingFields.push("Database")
      if (!formData.username || !formData.username.trim()) missingFields.push("Username")
      if (!formData.password || formData.password.trim() === "") missingFields.push("Password")
    }
    
    if (missingFields.length > 0) {
      setTestStatus("error")
      setTestMessage(`Please fill in: ${missingFields.join(", ")}`)
      return
    }

    setTestStatus("testing")
    setTestMessage("")
    
    try {
      const testData: any = {
        name: formData.name?.trim() || "test_connection",
        database_type: formData.engine,
        connection_type: "source",
        database: formData.database.trim(),
        username: formData.username.trim(),
        password: formData.password,
        ssl_enabled: formData.ssl_enabled || false,
      }
      
      if (isSnowflake) {
        testData.host = formData.account.trim()
        testData.additional_config = {
          account: formData.account.trim(),
          warehouse: (formData.warehouse && formData.warehouse.trim()) || undefined,
          role: (formData.role && formData.role.trim()) || undefined,
          schema: (formData.schema_name && formData.schema_name.trim()) || undefined,
          private_key: formData.private_key, // Always include private_key
          private_key_passphrase: formData.private_key_passphrase || undefined,
        }
        // Remove undefined values
        Object.keys(testData.additional_config).forEach(key => {
          if (testData.additional_config[key] === undefined) {
            delete testData.additional_config[key]
          }
        })
      } else if (isS3) {
        // S3-specific fields
        if (formData.schema_name) {
          testData.schema_name = formData.schema_name.trim()
        }
        testData.additional_config = {
          region_name: (formData.region && formData.region.trim()) || undefined,
          endpoint_url: (formData.endpoint_url && formData.endpoint_url.trim()) || undefined,
        }
        // Remove undefined values
        Object.keys(testData.additional_config).forEach(key => {
          if (testData.additional_config[key] === undefined) {
            delete testData.additional_config[key]
          }
        })
      } else {
        testData.host = formData.host.trim()
        testData.port = parseInt(formData.port) || (selectedDatabase?.defaultPort || 3306)
        if (formData.schema_name) {
          testData.schema_name = formData.schema_name.trim()
        }
      }

      // If editing, use the connection ID endpoint, otherwise use test endpoint
      const endpoint = editingConnection 
        ? `/api/v1/connections/${editingConnection.id}/test`
        : `/api/v1/connections/test`

      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'}${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`,
        },
        body: JSON.stringify(testData),
      })

      const result = await response.json()

      if (response.ok && (result.success || result.status === 'success')) {
        setTestStatus("success")
        setTestMessage(result.message || "Connection successful")
      } else {
        setTestStatus("error")
        // Extract error message safely - check multiple possible locations
        let errorMsg = "Connection failed"
        
        // Check result.error first (common in test connection responses)
        if (result.error) {
          if (typeof result.error === 'string') {
            errorMsg = result.error
          } else if (typeof result.error === 'object') {
            errorMsg = result.error.message || result.error.msg || JSON.stringify(result.error)
          }
        }
        // Check result.detail (FastAPI error format)
        else if (result.detail) {
          if (typeof result.detail === 'string') {
            errorMsg = result.detail
          } else if (Array.isArray(result.detail)) {
            errorMsg = result.detail.map((err: any) => {
              const field = err.loc?.join('.') || 'field'
              return `${field}: ${err.msg || 'Invalid value'}`
            }).join(', ')
          } else if (typeof result.detail === 'object') {
            errorMsg = result.detail.message || result.detail.msg || JSON.stringify(result.detail)
          }
        }
        // Check result.message
        else if (result.message) {
          errorMsg = result.message
        }
        // Check response status text
        else if (!response.ok) {
          errorMsg = `Connection failed: ${response.statusText || `HTTP ${response.status}`}`
        }
        
        setTestMessage(errorMsg)
      }
    } catch (error: any) {
      setTestStatus("error")
      setTestMessage(error.message || "Failed to test connection")
    }
  }

  const handleSave = () => {
    // Validate required fields
    if (!formData.name || !formData.name.trim()) {
      alert("Connection name is required")
      return
    }
    if (!formData.engine) {
      alert("Database engine is required")
      return
    }
    // Database-specific validation
    const isSnowflake = formData.engine === "snowflake"
    const isS3 = formData.engine === "s3" || formData.engine === "aws_s3"
    
    if (isSnowflake) {
      if (!formData.account || !formData.account.trim()) {
        alert("Snowflake Account is required (e.g., xy12345.us-east-1)")
        return
      }
      if (!formData.database || !formData.database.trim()) {
        alert("Database name is required")
        return
      }
      if (!formData.username || !formData.username.trim()) {
        alert("Username is required")
        return
      }
      // For Snowflake: both password AND private_key are required
      if (!formData.password) {
        alert("Password is required")
        return
      }
      if (!formData.private_key || !formData.private_key.trim()) {
        alert("Private Key is required")
        return
      }
    } else if (isS3) {
      // S3-specific validation
      if (!formData.database || !formData.database.trim()) {
        alert("Bucket name is required")
        return
      }
      if (!formData.username || !formData.username.trim()) {
        alert("AWS Access Key ID is required")
        return
      }
      if (!formData.password) {
        alert("AWS Secret Access Key is required")
        return
      }
    } else {
      // Standard database validation
      if (!formData.host || !formData.host.trim()) {
        alert("Host is required")
        return
      }
      if (!formData.database || !formData.database.trim()) {
        alert("Database name is required")
        return
      }
      if (!formData.username || !formData.username.trim()) {
        alert("Username is required")
        return
      }
      if (!formData.password) {
        alert("Password is required")
        return
      }
    }
    
    // Build data to save (isSnowflake and isS3 already defined above)
    const dataToSave: any = {
      name: formData.name.trim(),
      description: formData.description?.trim() || "",
      engine: formData.engine,
      database: formData.database.trim(),
      username: formData.username.trim(),
      password: formData.password,
      ssl_enabled: formData.ssl_enabled || false,
    }
    
    if (isSnowflake) {
      // Snowflake uses account instead of host
      dataToSave.host = formData.account.trim()
      // Add Snowflake-specific fields to additional_config
      dataToSave.additional_config = {
        account: formData.account.trim(),
        warehouse: (formData.warehouse && formData.warehouse.trim()) || undefined,
        role: (formData.role && formData.role.trim()) || undefined,
        schema: (formData.schema_name && formData.schema_name.trim()) || undefined,
        private_key: formData.private_key, // Always include private_key
        private_key_passphrase: formData.private_key_passphrase || undefined,
      }
      // Remove undefined values
      Object.keys(dataToSave.additional_config).forEach(key => {
        if (dataToSave.additional_config[key] === undefined) {
          delete dataToSave.additional_config[key]
        }
      })
    } else if (isS3) {
      // S3-specific fields
      // database = bucket name (already set)
      // username = aws_access_key_id (already set)
      // password = aws_secret_access_key (already set)
      // schema_name = prefix
      // host and port are required by database model, but not used by S3 - set defaults
      dataToSave.host = "s3.amazonaws.com"  // Default S3 endpoint (required by DB model)
      dataToSave.port = 443  // HTTPS port (required by DB model)
      if (formData.schema_name) {
        dataToSave.schema_name = formData.schema_name.trim()
      }
      // Add S3-specific fields to additional_config
      dataToSave.additional_config = {
        region_name: (formData.region && formData.region.trim()) || undefined,
        endpoint_url: (formData.endpoint_url && formData.endpoint_url.trim()) || undefined,
      }
      // Remove undefined values
      Object.keys(dataToSave.additional_config).forEach(key => {
        if (dataToSave.additional_config[key] === undefined) {
          delete dataToSave.additional_config[key]
        }
      })
    } else {
      // Standard database fields
      // Ensure port is valid
      const port = parseInt(formData.port) || (selectedDatabase?.defaultPort || 3306)
      if (port < 1 || port > 65535) {
        alert("Port must be between 1 and 65535")
        return
      }
      dataToSave.host = formData.host.trim()
      dataToSave.port = String(port)
      if (formData.schema_name) {
        dataToSave.schema_name = formData.schema_name.trim()
      }
    }
    
    console.log("Saving connection data:", { ...dataToSave, password: "***" })
    onSave(dataToSave)
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className={`${step === "select" ? "max-w-6xl" : "max-w-md"} bg-surface border-border max-h-[90vh] flex flex-col p-0`}>
        {step === "select" ? (
          <>
            <DialogHeader className="px-6 pt-6 pb-4 flex-shrink-0">
              <DialogTitle className="text-foreground">
                Add New Service
              </DialogTitle>
              <DialogDescription className="text-foreground-muted">
                Select a database service to configure
              </DialogDescription>
            </DialogHeader>
            <div className="flex-1 overflow-y-auto px-6 py-6">
              <DatabaseSelector
                onSelect={handleDatabaseSelect}
                onCancel={onClose}
              />
            </div>
          </>
        ) : (
          <>
            <DialogHeader className="px-6 pt-6 pb-4 flex-shrink-0">
              <div className="flex items-center gap-3 mb-2">
                {!editingConnection && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={handleBack}
                    className="p-1 h-8 w-8"
                  >
                    <ChevronLeft className="w-4 h-4" />
                  </Button>
                )}
                <div className="flex-1">
                  <DialogTitle className="text-foreground">
                    {editingConnection ? "Edit Connection" : "Configure Service"}
                  </DialogTitle>
                  <DialogDescription className="text-foreground-muted">
                    {editingConnection
                      ? "Update database connection details"
                      : selectedDatabase
                        ? `Configure ${selectedDatabase.displayName} connection`
                        : "Configure database connection"}
                  </DialogDescription>
                </div>
              </div>
              
              {/* Progress Bar */}
              <div className="flex items-center gap-2 mt-4">
                <div className="flex-1 h-2 bg-surface-hover rounded-full overflow-hidden">
                  <div className="h-full bg-primary w-2/4 transition-all duration-300"></div>
                </div>
                <span className="text-xs text-foreground-muted">Step 2 of 2</span>
              </div>
            </DialogHeader>

            <div className="flex-1 overflow-y-auto px-6 pb-4">
          <Tabs defaultValue="basic" className="w-full">
          <TabsList className="grid w-full grid-cols-2 bg-surface-hover">
            <TabsTrigger value="basic">Basic</TabsTrigger>
            <TabsTrigger value="advanced">Advanced</TabsTrigger>
          </TabsList>

          <TabsContent value="basic" className="space-y-4 mt-4">
            {/* Connection Name */}
            <div>
              <Label className="text-foreground">Connection Name *</Label>
              <Input
                placeholder="e.g., Production MySQL"
                value={formData.name}
                onChange={(e) => handleInputChange("name", e.target.value)}
                className="mt-1"
                required
              />
            </div>

            {/* Description */}
            <div>
              <Label className="text-foreground">Description</Label>
              <Input
                placeholder="Optional description"
                value={formData.description}
                onChange={(e) => handleInputChange("description", e.target.value)}
                className="mt-1"
              />
            </div>

            {/* Selected Database Info */}
            {selectedDatabase && (
              <div className="mb-4 p-3 bg-surface-hover rounded-lg border border-border">
                <div className="flex items-center gap-3">
                  <div className="w-14 h-14 rounded-lg bg-gradient-to-br from-primary/20 to-primary/10 flex items-center justify-center">
                    <DatabaseLogo 
                      connectionType={selectedDatabase.connectionType}
                      databaseId={selectedDatabase.id}
                      displayName={selectedDatabase.displayName}
                      size={32}
                      className="w-8 h-8"
                    />
                  </div>
                  <div>
                    <p className="text-sm font-semibold text-foreground">{selectedDatabase.displayName}</p>
                    <p className="text-xs text-foreground-muted">Connection Type: {selectedDatabase.connectionType}</p>
                  </div>
                </div>
              </div>
            )}

            {/* Host / Account (Snowflake) / Not needed for S3 */}
            {formData.engine === "snowflake" ? (
              <div>
                <Label className="text-foreground">Account *</Label>
                <Input
                  placeholder="xy12345.us-east-1"
                  value={formData.account}
                  onChange={(e) => handleInputChange("account", e.target.value)}
                  className="mt-1"
                  required
                />
                <p className="text-xs text-foreground-muted mt-1.5 px-1">
                  Your Snowflake account identifier (e.g., xy12345.us-east-1)
                </p>
              </div>
            ) : formData.engine !== "s3" && formData.engine !== "aws_s3" ? (
              <div>
                <Label className="text-foreground">Host</Label>
                <Input
                  placeholder="localhost or IP address"
                  value={formData.host}
                  onChange={(e) => handleInputChange("host", e.target.value)}
                  className="mt-1"
                />
              </div>
            ) : null}

            {/* Port and Database / Bucket (S3) */}
            {formData.engine === "snowflake" ? (
              <div>
                <Label className="text-foreground">Database *</Label>
                <Input
                  placeholder="SNOWFLAKE_SAMPLE_DATA"
                  value={formData.database}
                  onChange={(e) => handleInputChange("database", e.target.value)}
                  className="mt-1"
                  required
                />
              </div>
            ) : formData.engine === "s3" || formData.engine === "aws_s3" ? (
              <div>
                <Label className="text-foreground">Bucket Name *</Label>
                <Input
                  placeholder="my-bucket-name"
                  value={formData.database}
                  onChange={(e) => handleInputChange("database", e.target.value)}
                  className="mt-1"
                  required
                />
                <p className="text-xs text-foreground-muted mt-1.5 px-1">
                  Your S3 bucket name (e.g., my-data-bucket)
                </p>
              </div>
            ) : (
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label className="text-foreground">Port</Label>
                  <Input
                    type="number"
                    placeholder={selectedDatabase?.defaultPort ? String(selectedDatabase.defaultPort) : "3306"}
                    value={formData.port}
                    onChange={(e) => handleInputChange("port", e.target.value)}
                    className="mt-1"
                  />
                </div>
                <div>
                  <Label className="text-foreground">
                    {formData.engine === "oracle" ? "Database/Service Name" : "Database"}
                  </Label>
                  <Input
                    placeholder={formData.engine === "oracle" ? "XE, ORCL, PDB1, etc." : "database name"}
                    value={formData.database}
                    onChange={(e) => handleInputChange("database", e.target.value)}
                    className="mt-1"
                  />
                  {formData.engine === "oracle" && (
                    <p className="text-xs text-foreground-muted mt-1.5 px-1">
                      <span className="font-medium text-warning">⚠️ Important:</span> For Oracle, enter the <span className="font-medium">Service Name or SID</span> (e.g., XE, ORCL, PDB1), <span className="font-medium">NOT your username</span>. Common service names: XE (Express), ORCL (Standard), PDB1/PDB2 (Pluggable DBs).
                    </p>
                  )}
                </div>
              </div>
            )}

            {/* Username / AWS Access Key ID (S3) */}
            <div>
              <Label className="text-foreground">
                {formData.engine === "s3" || formData.engine === "aws_s3" ? "AWS Access Key ID *" : "Username"}
              </Label>
              <Input
                placeholder={formData.engine === "s3" || formData.engine === "aws_s3" ? "AKIAIOSFODNN7EXAMPLE" : "database user"}
                value={formData.username}
                onChange={(e) => handleInputChange("username", e.target.value)}
                className="mt-1"
                required={formData.engine === "s3" || formData.engine === "aws_s3"}
              />
              {(formData.engine === "s3" || formData.engine === "aws_s3") && (
                <p className="text-xs text-foreground-muted mt-1.5 px-1">
                  Your AWS Access Key ID
                </p>
              )}
            </div>

            {/* Schema (Snowflake) */}
            {formData.engine === "snowflake" && (
              <div>
                <Label className="text-foreground">Schema</Label>
                <Input
                  placeholder="PUBLIC (default)"
                  value={formData.schema_name}
                  onChange={(e) => handleInputChange("schema_name", e.target.value)}
                  className="mt-1"
                />
                <p className="text-xs text-foreground-muted mt-1.5 px-1">
                  Schema name (defaults to PUBLIC if not specified)
                </p>
              </div>
            )}

            {/* Password / AWS Secret Access Key (S3) */}
            <div>
              <Label className="text-foreground">
                {formData.engine === "s3" || formData.engine === "aws_s3" ? "AWS Secret Access Key *" : "Password *"}
              </Label>
              <Input
                type="password"
                placeholder="••••••••"
                value={formData.password}
                onChange={(e) => handleInputChange("password", e.target.value)}
                className="mt-1"
                required
              />
              {(formData.engine === "s3" || formData.engine === "aws_s3") && (
                <p className="text-xs text-foreground-muted mt-1.5 px-1">
                  Your AWS Secret Access Key
                </p>
              )}
            </div>

            {/* Private Key (Snowflake) - Always required for Snowflake */}
            {formData.engine === "snowflake" && (
              <>
                <div>
                  <Label className="text-foreground">Private Key *</Label>
                  <textarea
                    placeholder="-----BEGIN PRIVATE KEY-----&#10;...&#10;-----END PRIVATE KEY-----"
                    value={formData.private_key}
                    onChange={(e) => handleInputChange("private_key", e.target.value)}
                    className="mt-1 w-full min-h-[120px] px-3 py-2 bg-surface border border-border rounded-md text-foreground text-sm font-mono resize-y"
                    required
                  />
                  <p className="text-xs text-foreground-muted mt-1.5 px-1">
                    Paste your PEM-formatted private key for key pair authentication (required)
                  </p>
                </div>
                <div>
                  <Label className="text-foreground">Private Key Passphrase (optional)</Label>
                  <Input
                    type="password"
                    placeholder="••••••••"
                    value={formData.private_key_passphrase}
                    onChange={(e) => handleInputChange("private_key_passphrase", e.target.value)}
                    className="mt-1"
                  />
                  <p className="text-xs text-foreground-muted mt-1.5 px-1">
                    Passphrase if your private key is encrypted
                  </p>
                </div>
              </>
            )}
          </TabsContent>

          <TabsContent value="advanced" className="space-y-4 mt-4">
            {/* Snowflake-specific advanced fields */}
            {formData.engine === "snowflake" && (
              <>
                <div>
                  <Label className="text-foreground">Warehouse</Label>
                  <Input
                    placeholder="COMPUTE_WH (optional but recommended)"
                    value={formData.warehouse}
                    onChange={(e) => handleInputChange("warehouse", e.target.value)}
                    className="mt-1"
                  />
                  <p className="text-xs text-foreground-muted mt-1.5 px-1">
                    Snowflake warehouse name (optional but recommended for better performance)
                  </p>
                </div>
                <div>
                  <Label className="text-foreground">Role</Label>
                  <Input
                    placeholder="ACCOUNTADMIN (optional)"
                    value={formData.role}
                    onChange={(e) => handleInputChange("role", e.target.value)}
                    className="mt-1"
                  />
                  <p className="text-xs text-foreground-muted mt-1.5 px-1">
                    Snowflake role to use (optional)
                  </p>
                </div>
              </>
            )}

            {/* S3-specific advanced fields */}
            {(formData.engine === "s3" || formData.engine === "aws_s3") && (
              <>
                <div>
                  <Label className="text-foreground">AWS Region</Label>
                  <Input
                    placeholder="us-east-1 (optional, defaults to us-east-1)"
                    value={formData.region}
                    onChange={(e) => handleInputChange("region", e.target.value)}
                    className="mt-1"
                  />
                  <p className="text-xs text-foreground-muted mt-1.5 px-1">
                    AWS region where your bucket is located (e.g., us-east-1, eu-west-1)
                  </p>
                </div>
                <div>
                  <Label className="text-foreground">Prefix/Folder Path</Label>
                  <Input
                    placeholder="folder/subfolder/ (optional)"
                    value={formData.schema_name}
                    onChange={(e) => handleInputChange("schema_name", e.target.value)}
                    className="mt-1"
                  />
                  <p className="text-xs text-foreground-muted mt-1.5 px-1">
                    S3 object key prefix/folder path (optional, e.g., data/raw/)
                  </p>
                </div>
                <div>
                  <Label className="text-foreground">Custom Endpoint URL</Label>
                  <Input
                    placeholder="https://s3.amazonaws.com (optional)"
                    value={formData.endpoint_url}
                    onChange={(e) => handleInputChange("endpoint_url", e.target.value)}
                    className="mt-1"
                  />
                  <p className="text-xs text-foreground-muted mt-1.5 px-1">
                    Custom S3 endpoint URL for S3-compatible services (optional)
                  </p>
                </div>
              </>
            )}
            
            {/* SSL/TLS (not applicable for Snowflake or S3) */}
            {formData.engine !== "snowflake" && formData.engine !== "s3" && formData.engine !== "aws_s3" && (
              <div>
                <Label className="text-foreground flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={formData.ssl_enabled}
                    onChange={(e) => handleInputChange("ssl_enabled", e.target.checked)}
                    className="w-4 h-4"
                  />
                  Enable SSL/TLS
                </Label>
              </div>
            )}
          </TabsContent>
          </Tabs>

          {/* Test Status */}
          {testStatus !== "idle" && (
            <div
              className={`flex gap-2 p-3 rounded-lg border mt-4 ${
                testStatus === "success"
                  ? "bg-success/10 border-success/30"
                  : testStatus === "error"
                    ? "bg-error/10 border-error/30"
                    : "bg-info/10 border-info/30"
              }`}
            >
              {testStatus === "success" && (
                <>
                  <CheckCircle className="w-4 h-4 text-success flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm text-success font-medium">Connection successful!</p>
                    {testMessage && <p className="text-xs text-success/80 mt-1">{testMessage}</p>}
                  </div>
                </>
              )}
              {testStatus === "error" && (
                <>
                  <AlertCircle className="w-4 h-4 text-error flex-shrink-0 mt-0.5" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-error font-medium">Connection failed</p>
                    {testMessage && (
                      <div className="mt-1 max-h-32 overflow-y-auto">
                        <p className="text-xs text-error/80 whitespace-pre-wrap break-words">{testMessage}</p>
                      </div>
                    )}
                  </div>
                </>
              )}
              {testStatus === "testing" && (
                <>
                  <div className="w-4 h-4 rounded-full border-2 border-info border-t-transparent animate-spin flex-shrink-0"></div>
                  <p className="text-sm text-info">Testing connection...</p>
                </>
              )}
            </div>
          )}
        </div>

            {/* Action Buttons - Fixed at bottom */}
            <div className="flex gap-3 justify-end px-6 py-4 border-t border-border bg-surface flex-shrink-0">
              <Button
                variant="outline"
                onClick={() => handleTestConnection()}
                className="bg-transparent border-border hover:bg-surface-hover"
                disabled={testStatus === "testing"}
              >
                {testStatus === "testing" ? "Testing..." : "Test Connection"}
              </Button>
              <Button variant="outline" onClick={onClose} className="bg-transparent border-border hover:bg-surface-hover">
                Cancel
              </Button>
              <Button 
                onClick={handleSave} 
                className="bg-primary hover:bg-primary/90 text-foreground"
                disabled={testStatus === "testing"}
              >
                Save Connection
              </Button>
            </div>
          </>
        )}
      </DialogContent>
    </Dialog>
  )
}
