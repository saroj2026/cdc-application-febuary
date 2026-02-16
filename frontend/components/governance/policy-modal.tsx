"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Switch } from "@/components/ui/switch"
import { Shield, Database, LineChart, AlertCircle } from "lucide-react"

interface PolicyModalProps {
  isOpen: boolean
  onClose: () => void
  onSave: (data: any) => void
}

export function PolicyModal({ isOpen, onClose, onSave }: PolicyModalProps) {
  const [formData, setFormData] = useState({
    // Data Encryption Policy
    requireSsl: true,
    sslThreshold: "100",
    
    // Connection Health Policy
    connectionHealthThreshold: "100",
    requireActiveConnections: true,
    
    // Pipeline Status Policy
    requireActivePipelines: true,
    minActivePipelines: "1",
    
    // Audit Logging Policy
    requireAuditLogging: true,
    minEventLogs: "100",
    
    // Quality Metrics Thresholds
    minCompleteness: "95",
    minAccuracy: "99",
    minTimeliness: "90",
    minOverallQuality: "95",
    
    // SLA Requirements
    defaultSla: "99.9",
    criticalSla: "99.99",
    standardSla: "99",
    
    // Data Classification
    autoClassifyCritical: true,
    autoClassifyAtRisk: true,
  })

  const handleInputChange = (field: string, value: string | boolean) => {
    setFormData((prev) => ({ ...prev, [field]: value }))
  }

  const handleSave = () => {
    onSave(formData)
    onClose()
  }

  const handleReset = () => {
    setFormData({
      requireSsl: true,
      sslThreshold: "100",
      connectionHealthThreshold: "100",
      requireActiveConnections: true,
      requireActivePipelines: true,
      minActivePipelines: "1",
      requireAuditLogging: true,
      minEventLogs: "100",
      minCompleteness: "95",
      minAccuracy: "99",
      minTimeliness: "90",
      minOverallQuality: "95",
      defaultSla: "99.9",
      criticalSla: "99.99",
      standardSla: "99",
      autoClassifyCritical: true,
      autoClassifyAtRisk: true,
    })
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-3xl bg-surface border-border max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-foreground flex items-center gap-2">
            <Shield className="w-5 h-5" />
            Configure Data Governance Policies
          </DialogTitle>
          <DialogDescription className="text-foreground-muted">
            Set policies for data encryption, connection health, quality metrics, and compliance standards
          </DialogDescription>
        </DialogHeader>

        <Tabs defaultValue="security" className="w-full">
          <TabsList className="grid w-full grid-cols-4 bg-surface-hover">
            <TabsTrigger value="security">Security</TabsTrigger>
            <TabsTrigger value="quality">Quality</TabsTrigger>
            <TabsTrigger value="sla">SLA</TabsTrigger>
            <TabsTrigger value="classification">Classification</TabsTrigger>
          </TabsList>

          {/* Security Policies */}
          <TabsContent value="security" className="space-y-6 mt-4">
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground flex items-center gap-2">
                <Database className="w-4 h-4" />
                Data Encryption
              </h3>
              
              <div className="space-y-4 p-4 bg-surface-hover rounded-lg">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <Label className="text-foreground">Require SSL/TLS Encryption</Label>
                    <p className="text-sm text-foreground-muted mt-1">
                      Enforce SSL encryption for all database connections
                    </p>
                  </div>
                  <Switch
                    checked={formData.requireSsl}
                    onCheckedChange={(checked) => handleInputChange("requireSsl", checked)}
                  />
                </div>

                <div>
                  <Label className="text-foreground">SSL Compliance Threshold (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Minimum percentage of connections that must use SSL
                  </p>
                  <Input
                    type="number"
                    min="0"
                    max="100"
                    value={formData.sslThreshold}
                    onChange={(e) => handleInputChange("sslThreshold", e.target.value)}
                    className="bg-input border-border"
                  />
                </div>
              </div>

              <h3 className="text-lg font-semibold text-foreground flex items-center gap-2 mt-6">
                <AlertCircle className="w-4 h-4" />
                Connection Health
              </h3>
              
              <div className="space-y-4 p-4 bg-surface-hover rounded-lg">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <Label className="text-foreground">Require Active Connections</Label>
                    <p className="text-sm text-foreground-muted mt-1">
                      All connections must be active for compliance
                    </p>
                  </div>
                  <Switch
                    checked={formData.requireActiveConnections}
                    onCheckedChange={(checked) => handleInputChange("requireActiveConnections", checked)}
                  />
                </div>

                <div>
                  <Label className="text-foreground">Connection Health Threshold (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Minimum percentage of connections that must be active
                  </p>
                  <Input
                    type="number"
                    min="0"
                    max="100"
                    value={formData.connectionHealthThreshold}
                    onChange={(e) => handleInputChange("connectionHealthThreshold", e.target.value)}
                    className="bg-input border-border"
                  />
                </div>
              </div>

              <h3 className="text-lg font-semibold text-foreground flex items-center gap-2 mt-6">
                <Database className="w-4 h-4" />
                Pipeline Status
              </h3>
              
              <div className="space-y-4 p-4 bg-surface-hover rounded-lg">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <Label className="text-foreground">Require Active Pipelines</Label>
                    <p className="text-sm text-foreground-muted mt-1">
                      At least one pipeline must be active
                    </p>
                  </div>
                  <Switch
                    checked={formData.requireActivePipelines}
                    onCheckedChange={(checked) => handleInputChange("requireActivePipelines", checked)}
                  />
                </div>

                <div>
                  <Label className="text-foreground">Minimum Active Pipelines</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Minimum number of pipelines that must be active
                  </p>
                  <Input
                    type="number"
                    min="0"
                    value={formData.minActivePipelines}
                    onChange={(e) => handleInputChange("minActivePipelines", e.target.value)}
                    className="bg-input border-border"
                  />
                </div>
              </div>

              <h3 className="text-lg font-semibold text-foreground flex items-center gap-2 mt-6">
                <AlertCircle className="w-4 h-4" />
                Audit Logging
              </h3>
              
              <div className="space-y-4 p-4 bg-surface-hover rounded-lg">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <Label className="text-foreground">Require Audit Logging</Label>
                    <p className="text-sm text-foreground-muted mt-1">
                      All replication events must be logged
                    </p>
                  </div>
                  <Switch
                    checked={formData.requireAuditLogging}
                    onCheckedChange={(checked) => handleInputChange("requireAuditLogging", checked)}
                  />
                </div>

                <div>
                  <Label className="text-foreground">Minimum Event Logs</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Minimum number of events that must be logged
                  </p>
                  <Input
                    type="number"
                    min="0"
                    value={formData.minEventLogs}
                    onChange={(e) => handleInputChange("minEventLogs", e.target.value)}
                    className="bg-input border-border"
                  />
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Quality Metrics */}
          <TabsContent value="quality" className="space-y-6 mt-4">
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground flex items-center gap-2">
                <LineChart className="w-4 h-4" />
                Quality Thresholds
              </h3>
              
              <p className="text-sm text-foreground-muted">
                Set minimum quality thresholds for data replication. Values below these thresholds will trigger alerts.
              </p>

              <div className="space-y-4 p-4 bg-surface-hover rounded-lg">
                <div>
                  <Label className="text-foreground">Minimum Data Completeness (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Minimum percentage of successful replications
                  </p>
                  <Input
                    type="number"
                    min="0"
                    max="100"
                    value={formData.minCompleteness}
                    onChange={(e) => handleInputChange("minCompleteness", e.target.value)}
                    className="bg-input border-border"
                  />
                </div>

                <div>
                  <Label className="text-foreground">Minimum Data Accuracy (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Minimum accuracy rate (inverse of error rate)
                  </p>
                  <Input
                    type="number"
                    min="0"
                    max="100"
                    value={formData.minAccuracy}
                    onChange={(e) => handleInputChange("minAccuracy", e.target.value)}
                    className="bg-input border-border"
                  />
                </div>

                <div>
                  <Label className="text-foreground">Minimum Data Timeliness (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Minimum percentage of events with low latency (&lt; 1000ms)
                  </p>
                  <Input
                    type="number"
                    min="0"
                    max="100"
                    value={formData.minTimeliness}
                    onChange={(e) => handleInputChange("minTimeliness", e.target.value)}
                    className="bg-input border-border"
                  />
                </div>

                <div>
                  <Label className="text-foreground">Minimum Overall Quality Score (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Minimum overall quality score (average of all metrics)
                  </p>
                  <Input
                    type="number"
                    min="0"
                    max="100"
                    value={formData.minOverallQuality}
                    onChange={(e) => handleInputChange("minOverallQuality", e.target.value)}
                    className="bg-input border-border"
                  />
                </div>
              </div>
            </div>
          </TabsContent>

          {/* SLA Requirements */}
          <TabsContent value="sla" className="space-y-6 mt-4">
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground flex items-center gap-2">
                <Shield className="w-4 h-4" />
                Service Level Agreements
              </h3>
              
              <p className="text-sm text-foreground-muted">
                Configure SLA requirements for different data classifications
              </p>

              <div className="space-y-4 p-4 bg-surface-hover rounded-lg">
                <div>
                  <Label className="text-foreground">Default SLA (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    Default SLA for standard data tables
                  </p>
                  <Select
                    value={formData.defaultSla}
                    onValueChange={(value) => handleInputChange("defaultSla", value)}
                  >
                    <SelectTrigger className="bg-input border-border">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-surface border-border">
                      <SelectItem value="90">90%</SelectItem>
                      <SelectItem value="95">95%</SelectItem>
                      <SelectItem value="99">99%</SelectItem>
                      <SelectItem value="99.9">99.9%</SelectItem>
                      <SelectItem value="99.99">99.99%</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label className="text-foreground">Critical Data SLA (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    SLA for business-critical data tables
                  </p>
                  <Select
                    value={formData.criticalSla}
                    onValueChange={(value) => handleInputChange("criticalSla", value)}
                  >
                    <SelectTrigger className="bg-input border-border">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-surface border-border">
                      <SelectItem value="99">99%</SelectItem>
                      <SelectItem value="99.9">99.9%</SelectItem>
                      <SelectItem value="99.99">99.99%</SelectItem>
                      <SelectItem value="99.999">99.999%</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label className="text-foreground">Standard Data SLA (%)</Label>
                  <p className="text-sm text-foreground-muted mt-1 mb-2">
                    SLA for standard/internal data tables
                  </p>
                  <Select
                    value={formData.standardSla}
                    onValueChange={(value) => handleInputChange("standardSla", value)}
                  >
                    <SelectTrigger className="bg-input border-border">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-surface border-border">
                      <SelectItem value="90">90%</SelectItem>
                      <SelectItem value="95">95%</SelectItem>
                      <SelectItem value="99">99%</SelectItem>
                      <SelectItem value="99.9">99.9%</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Data Classification */}
          <TabsContent value="classification" className="space-y-6 mt-4">
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground flex items-center gap-2">
                <Database className="w-4 h-4" />
                Automatic Classification
              </h3>
              
              <p className="text-sm text-foreground-muted">
                Configure automatic data classification based on pipeline status and performance
              </p>

              <div className="space-y-4 p-4 bg-surface-hover rounded-lg">
                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <Label className="text-foreground">Auto-Classify as Business Critical</Label>
                    <p className="text-sm text-foreground-muted mt-1">
                      Automatically classify tables from active pipelines as Business Critical
                    </p>
                  </div>
                  <Switch
                    checked={formData.autoClassifyCritical}
                    onCheckedChange={(checked) => handleInputChange("autoClassifyCritical", checked)}
                  />
                </div>

                <div className="flex items-center justify-between">
                  <div className="flex-1">
                    <Label className="text-foreground">Auto-Classify as At Risk</Label>
                    <p className="text-sm text-foreground-muted mt-1">
                      Automatically classify tables from pipelines with errors as At Risk
                    </p>
                  </div>
                  <Switch
                    checked={formData.autoClassifyAtRisk}
                    onCheckedChange={(checked) => handleInputChange("autoClassifyAtRisk", checked)}
                  />
                </div>
              </div>
            </div>
          </TabsContent>
        </Tabs>

        <div className="flex justify-end gap-2 mt-6 pt-4 border-t border-border">
          <Button
            variant="outline"
            onClick={handleReset}
            className="bg-transparent border-border hover:bg-surface-hover"
          >
            Reset to Defaults
          </Button>
          <Button
            variant="outline"
            onClick={onClose}
            className="bg-transparent border-border hover:bg-surface-hover"
          >
            Cancel
          </Button>
          <Button
            onClick={handleSave}
            className="bg-primary hover:bg-primary/90 text-foreground"
          >
            Save Policies
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}

