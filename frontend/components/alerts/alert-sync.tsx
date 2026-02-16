/**
 * Component to sync errors from connections, pipelines, and events to alerts
 */
"use client"

import { useEffect } from "react"
import { useAppSelector, useAppDispatch } from "@/lib/store/hooks"
import { addAlert, addAlerts } from "@/lib/store/slices/alertsSlice"
import { format } from "date-fns"

export function AlertSync() {
  const dispatch = useAppDispatch()
  const { connections, error: connectionError } = useAppSelector((state) => state.connections)
  const { pipelines, error: pipelineError } = useAppSelector((state) => state.pipelines)
  const { events } = useAppSelector((state) => state.monitoring)

  // Sync connection errors
  useEffect(() => {
    if (connectionError) {
      dispatch(addAlert({
        id: `conn_error_${Date.now()}`,
        type: 'error',
        source: 'connection',
        message: connectionError,
        timestamp: new Date().toISOString(),
        status: 'unresolved',
        severity: 'high',
      }))
    }
  }, [connectionError, dispatch])

  // Sync pipeline errors
  useEffect(() => {
    if (pipelineError) {
      dispatch(addAlert({
        id: `pipeline_error_${Date.now()}`,
        type: 'error',
        source: 'pipeline',
        message: pipelineError,
        timestamp: new Date().toISOString(),
        status: 'unresolved',
        severity: 'high',
      }))
    }
  }, [pipelineError, dispatch])

  // Sync connection test failures
  useEffect(() => {
    const connectionsArray = Array.isArray(connections) ? connections : []
    connectionsArray.forEach(conn => {
      if (conn.last_test_status === 'failed' || (!conn.is_active && conn.last_tested_at)) {
        dispatch(addAlert({
          id: `conn_test_${conn.id}_${conn.last_tested_at}`,
          type: 'error',
          source: 'connection',
          sourceId: conn.id,
          sourceName: conn.name,
          message: `Connection test failed: ${conn.name}`,
          details: conn.last_test_status || 'Connection inactive',
          timestamp: conn.last_tested_at || conn.updated_at || conn.created_at,
          status: 'unresolved',
          severity: 'high',
        }))
      }
    })
  }, [connections, dispatch])

  // Sync pipeline errors
  useEffect(() => {
    const pipelinesArray = Array.isArray(pipelines) ? pipelines : []
    pipelinesArray.forEach(pipeline => {
      if (pipeline.status === 'error') {
        dispatch(addAlert({
          id: `pipeline_status_${pipeline.id}_${pipeline.updated_at || pipeline.created_at}`,
          type: 'error',
          source: 'pipeline',
          sourceId: pipeline.id,
          sourceName: pipeline.name,
          message: `Pipeline error: ${pipeline.name}`,
          details: `Pipeline status: ${pipeline.status}`,
          timestamp: pipeline.updated_at || pipeline.created_at,
          status: 'unresolved',
          severity: 'critical',
        }))
      }
    })
  }, [pipelines, dispatch])

  // Sync replication event errors
  useEffect(() => {
    const eventsArray = Array.isArray(events) ? events : []
    const failedEvents = eventsArray.filter(e => 
      e.status === 'failed' || e.status === 'error'
    ).slice(0, 100) // Limit to recent 100 failed events

    if (failedEvents.length > 0) {
      const alerts = failedEvents.map(event => ({
        id: `replication_${event.id}`,
        type: 'error' as const,
        source: 'replication' as const,
        sourceId: event.pipeline_id,
        message: `Replication failed: ${event.event_type || 'unknown'}${event.table_name ? ` on ${event.table_name}` : ''}`,
        details: `Status: ${event.status}${event.error_message ? ` - ${event.error_message}` : ''}`,
        timestamp: event.created_at || new Date().toISOString(),
        status: 'unresolved' as const,
        severity: (event.latency_ms && event.latency_ms > 10000 ? 'critical' : 'high') as 'critical' | 'high',
        table: event.table_name,
      }))
      dispatch(addAlerts(alerts))
    }
  }, [events, dispatch])

  return null // This is a sync component, no UI
}

