"use client"

import { useEffect, useMemo, useState } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { ArrowRight, Shield, Database, LineChart, FileText, CheckCircle2 } from "lucide-react"
import { PageHeader } from "@/components/ui/page-header"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { fetchConnections } from "@/lib/store/slices/connectionSlice"
import { fetchPipelines } from "@/lib/store/slices/pipelineSlice"
import { fetchReplicationEvents } from "@/lib/store/slices/monitoringSlice"
import { PolicyModal } from "@/components/governance/policy-modal"
import { ProtectedPage } from "@/components/auth/ProtectedPage"

export default function GovernancePage() {
  const dispatch = useAppDispatch()
  const { connections } = useAppSelector((state) => state.connections)
  const { pipelines } = useAppSelector((state) => state.pipelines)
  const { events } = useAppSelector((state) => state.monitoring)
  const [isPolicyModalOpen, setIsPolicyModalOpen] = useState(false)

  useEffect(() => {
    dispatch(fetchConnections())
    dispatch(fetchPipelines())
    dispatch(fetchReplicationEvents({ limit: 10000 }))
  }, [dispatch])

  // Calculate data lineage from pipelines
  const dataLineage = useMemo(() => {
    return pipelines.map(pipeline => {
      const sourceConn = connections.find(c => String(c.id) === String(pipeline.source_connection_id))
      const targetConn = connections.find(c => String(c.id) === String(pipeline.target_connection_id))

      const tables = pipeline.table_mappings?.map(tm => tm.target_table || tm.source_table) || []

      return {
        id: pipeline.id,
        source: sourceConn?.name || `Connection ${pipeline.source_connection_id}`,
        target: targetConn?.name || `Connection ${pipeline.target_connection_id}`,
        tables: tables.slice(0, 10), // Limit to 10 tables for display
        pipelineName: pipeline.name,
      }
    })
  }, [pipelines, connections])

  // Calculate quality metrics from events
  const qualityMetrics = useMemo(() => {
    const totalEvents = events.length
    if (totalEvents === 0) {
      return {
        completeness: 100,
        accuracy: 100,
        timeliness: 100,
        overall: 100,
      }
    }

    const successfulEvents = events.filter(e => e.status === 'applied' || e.status === 'success').length
    const failedEvents = events.filter(e => e.status === 'failed' || e.status === 'error').length

    // Data Completeness: percentage of successful replications
    const completeness = totalEvents > 0 ? (successfulEvents / totalEvents) * 100 : 100

    // Data Accuracy: inverse of error rate
    const accuracy = totalEvents > 0 ? ((totalEvents - failedEvents) / totalEvents) * 100 : 100

    // Data Timeliness: events with low latency (< 1000ms)
    const timelyEvents = events.filter(e => e.latency_ms && e.latency_ms < 1000).length
    const timeliness = totalEvents > 0 ? (timelyEvents / totalEvents) * 100 : 100

    // Overall Quality Score: average of all metrics
    const overall = (completeness + accuracy + timeliness) / 3

    return {
      completeness: Math.round(completeness * 10) / 10,
      accuracy: Math.round(accuracy * 10) / 10,
      timeliness: Math.round(timeliness * 10) / 10,
      overall: Math.round(overall * 10) / 10,
    }
  }, [events])

  // Calculate compliance status
  const complianceStatus = useMemo(() => {
    const activeConnections = connections.filter(c => c.is_active).length
    const totalConnections = connections.length
    const activePipelines = pipelines.filter(p => p.status === 'active').length
    const totalPipelines = pipelines.length

    // SSL enabled connections
    const sslEnabled = connections.filter(c => c.ssl_enabled).length
    const sslCompliance = totalConnections > 0 ? (sslEnabled / totalConnections) * 100 : 100

    return [
      {
        policy: "Data Encryption",
        status: sslCompliance >= 100 ? "compliant" : sslCompliance >= 80 ? "review_needed" : "non_compliant",
        details: `${sslEnabled}/${totalConnections} connections use SSL`,
      },
      {
        policy: "Connection Health",
        status: totalConnections > 0 && activeConnections === totalConnections ? "compliant" : "review_needed",
        details: `${activeConnections}/${totalConnections} connections active`,
      },
      {
        policy: "Pipeline Status",
        status: totalPipelines > 0 && activePipelines > 0 ? "compliant" : "review_needed",
        details: `${activePipelines}/${totalPipelines} pipelines active`,
      },
      {
        policy: "Audit Logging",
        status: events.length > 0 ? "compliant" : "review_needed",
        details: `${events.length} events logged`,
      },
    ]
  }, [connections, pipelines, events])

  // Calculate table ownership from pipeline table mappings
  const tableOwnership = useMemo(() => {
    const ownershipMap: Record<string, {
      table: string
      pipeline: string
      classification: string
      sla: string
    }> = {}

    pipelines.forEach(pipeline => {
      pipeline.table_mappings?.forEach(tm => {
        const tableName = tm.target_table || tm.source_table
        if (tableName && !ownershipMap[tableName]) {
          // Determine classification based on pipeline status
          const classification = pipeline.status === 'active' ? 'Business Critical' :
            pipeline.status === 'error' ? 'At Risk' : 'Internal'

          // Calculate SLA based on pipeline performance
          const pipelineEvents = events.filter(e => String(e.pipeline_id) === String(pipeline.id))
          const successRate = pipelineEvents.length > 0
            ? (pipelineEvents.filter(e => e.status === 'applied' || e.status === 'success').length / pipelineEvents.length) * 100
            : 100

          const sla = successRate >= 99.9 ? '99.9%' :
            successRate >= 99 ? '99%' :
              successRate >= 95 ? '95%' : '90%'

          ownershipMap[tableName] = {
            table: tableName,
            pipeline: pipeline.name,
            classification,
            sla,
          }
        }
      })
    })

    return Object.values(ownershipMap).slice(0, 20) // Limit to 20 tables
  }, [pipelines, events])

  return (
    <ProtectedPage path="/governance" requiredPermission="view_metrics">
      <div className="p-6 space-y-6">
        <PageHeader
          title="Data Governance"
          subtitle="Manage data lineage, compliance, and quality standards"
          icon={Shield}
          action={
            <Button
              onClick={() => setIsPolicyModalOpen(true)}
              className="bg-primary hover:bg-primary/90 text-foreground"
            >
              Configure Policies
            </Button>
          }
        />

        {/* Data Lineage */}
        <Card className="bg-card border-l-4 border-l-primary shadow-sm p-6 hover:shadow-md transition-all group relative overflow-hidden">
          <Database className="absolute -right-4 -bottom-4 w-24 h-24 text-primary/10 group-hover:text-primary/15 transition-colors transform rotate-12" />
          <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2 relative z-10">
            <Database className="w-5 h-5 text-primary" />
            Data Lineage
          </h3>
          {dataLineage.length > 0 ? (
            <div className="space-y-4">
              {dataLineage.map((lineage) => (
                <div key={lineage.id} className="flex items-center justify-between p-4 bg-surface-hover rounded-lg">
                  <div className="flex-1">
                    <p className="text-sm text-foreground-muted">Source</p>
                    <p className="text-foreground font-semibold">{lineage.source}</p>
                    <p className="text-xs text-foreground-muted mt-1">{lineage.pipelineName}</p>
                    {lineage.tables.length > 0 && (
                      <div className="flex gap-2 mt-2 flex-wrap">
                        {lineage.tables.map((table) => (
                          <Badge key={table} className="bg-primary/20 text-primary">
                            {table}
                          </Badge>
                        ))}
                        {(() => {
                          const pipeline = pipelines.find(p => p.id === lineage.id)
                          return pipeline?.table_mappings && pipeline.table_mappings.length > 10 && (
                            <Badge className="bg-primary/20 text-primary">
                              +{pipeline.table_mappings.length - 10} more
                            </Badge>
                          )
                        })()}
                      </div>
                    )}
                  </div>
                  <ArrowRight className="w-6 h-6 text-foreground-muted mx-4" />
                  <div>
                    <p className="text-sm text-foreground-muted">Target</p>
                    <p className="text-foreground font-semibold">{lineage.target}</p>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-8 text-foreground-muted">
              <p>No data lineage available. Create pipelines to see data flow.</p>
            </div>
          )}
        </Card>

        {/* Compliance & Policies */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card className="bg-card border-l-4 border-l-warning shadow-sm p-6 hover:shadow-md transition-all group relative overflow-hidden">
            <Shield className="absolute -right-4 -bottom-4 w-24 h-24 text-warning/10 group-hover:text-warning/15 transition-colors transform rotate-12" />
            <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2 relative z-10">
              <Shield className="w-5 h-5 text-warning" />
              Compliance Status
            </h3>
            <div className="space-y-3">
              {complianceStatus.map((item) => (
                <div key={item.policy} className="flex items-center justify-between p-3 bg-surface-hover rounded">
                  <div className="flex-1">
                    <span className="text-foreground text-sm font-medium">{item.policy}</span>
                    <p className="text-xs text-foreground-muted mt-1">{item.details}</p>
                  </div>
                  <Badge
                    className={
                      item.status === "compliant"
                        ? "bg-success/20 text-success"
                        : item.status === "review_needed"
                          ? "bg-warning/20 text-warning"
                          : "bg-error/20 text-error"
                    }
                  >
                    {item.status === "compliant"
                      ? "Compliant"
                      : item.status === "review_needed"
                        ? "Review Needed"
                        : "Non-Compliant"}
                  </Badge>
                </div>
              ))}
            </div>
          </Card>

          <Card className="bg-card border-l-4 border-l-success shadow-sm p-6 hover:shadow-md transition-all group relative overflow-hidden">
            <LineChart className="absolute -right-4 -bottom-4 w-24 h-24 text-success/10 group-hover:text-success/15 transition-colors transform rotate-12" />
            <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2 relative z-10">
              <LineChart className="w-5 h-5 text-success" />
              Quality Metrics
            </h3>
            <div className="space-y-3">
              {[
                { metric: "Data Completeness", value: `${qualityMetrics.completeness}%` },
                { metric: "Data Accuracy", value: `${qualityMetrics.accuracy}%` },
                { metric: "Data Timeliness", value: `${qualityMetrics.timeliness}%` },
                { metric: "Overall Quality Score", value: `${qualityMetrics.overall}%` },
              ].map((item) => (
                <div key={item.metric} className="flex items-center justify-between">
                  <span className="text-foreground-muted text-sm">{item.metric}</span>
                  <span className="text-foreground font-semibold">{item.value}</span>
                </div>
              ))}
            </div>
          </Card>
        </div>

        {/* Data Owner Assignment */}
        <Card className="bg-card border-l-4 border-l-blue-500 shadow-sm p-6 hover:shadow-md transition-all group relative overflow-hidden">
          <FileText className="absolute -right-4 -bottom-4 w-24 h-24 text-blue-500/10 group-hover:text-blue-500/15 transition-colors transform rotate-12" />
          <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2 relative z-10">
            <FileText className="w-5 h-5 text-blue-500" />
            Data Owner Assignment
          </h3>
          {tableOwnership.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-3 px-4 font-semibold text-foreground-muted">Database / Table</th>
                    <th className="text-left py-3 px-4 font-semibold text-foreground-muted">Pipeline</th>
                    <th className="text-left py-3 px-4 font-semibold text-foreground-muted">Classification</th>
                    <th className="text-center py-3 px-4 font-semibold text-foreground-muted">SLA</th>
                  </tr>
                </thead>
                <tbody>
                  {tableOwnership.map((row) => (
                    <tr key={row.table} className="border-b border-border hover:bg-surface-hover">
                      <td className="py-3 px-4 text-foreground font-medium">{row.table}</td>
                      <td className="py-3 px-4 text-foreground">{row.pipeline}</td>
                      <td className="py-3 px-4">
                        <Badge
                          className={
                            row.classification === "Business Critical"
                              ? "bg-error/20 text-error"
                              : row.classification === "At Risk"
                                ? "bg-warning/20 text-warning"
                                : "bg-primary/20 text-primary"
                          }
                        >
                          {row.classification}
                        </Badge>
                      </td>
                      <td className="py-3 px-4 text-center text-foreground font-semibold">{row.sla}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center py-8 text-foreground-muted">
              <p>No table ownership data available. Create pipelines with table mappings to see ownership.</p>
            </div>
          )}
        </Card>

        {/* Policy Configuration Modal */}
        <PolicyModal
          isOpen={isPolicyModalOpen}
          onClose={() => setIsPolicyModalOpen(false)}
          onSave={(policyData) => {
            // Save policies (in a real app, this would call an API)
            console.log("Saving policies:", policyData)
            // You can dispatch to Redux store or call API here
            alert("Policies saved successfully!")
          }}
        />
      </div>
    </ProtectedPage>
  )
}
