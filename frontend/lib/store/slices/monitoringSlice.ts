/**
 * Monitoring Redux slice
 */
import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit';
import { apiClient } from '@/lib/api/client';

interface ReplicationEvent {
  id: string;  // Database ID as string
  pipeline_id: string;  // Database ID as string
  pipeline_run_id?: string;  // Database ID as string (optional)
  event_type: 'insert' | 'update' | 'delete';
  table_name: string;
  schema_name?: string;
  source_data?: any;
  target_data?: any;
  changed_columns?: string[];
  source_commit_time?: string;
  target_apply_time?: string;
  latency_ms?: number;
  // Replication Position (LSN/SCN/Binlog)
  source_lsn?: string;  // PostgreSQL LSN
  source_scn?: number;  // Oracle SCN
  source_binlog_file?: string;  // MySQL binlog file
  source_binlog_position?: number;  // MySQL binlog position
  sql_server_lsn?: string;  // SQL Server LSN
  status: string;
  error_message?: string;
  created_at: string;
}

interface MonitoringMetric {
  id: string;  // Database ID as string
  pipeline_id: string;  // Database ID as string
  timestamp: string;
  events_per_second: number;
  avg_latency_ms: number;
  total_events: number;
  insert_count: number;
  update_count: number;
  delete_count: number;
  error_count: number;
}

interface MonitoringState {
  events: ReplicationEvent[];
  metrics: MonitoringMetric[];
  selectedPipelineId: number | string | null;
  isLoading: boolean;
  error: string | null;
  realTimeEnabled: boolean;
}

const initialState: MonitoringState = {
  events: [],
  metrics: [],
  selectedPipelineId: null,
  isLoading: false,
  error: null,
  realTimeEnabled: false,
};

// Async thunks
export const fetchReplicationEvents = createAsyncThunk(
  'monitoring/fetchEvents',
  async ({
    pipelineId,
    skip,
    limit,
    todayOnly,
    startDate,
    endDate,
    tableName,
  }: {
    pipelineId?: number | string;
    skip?: number;
    limit?: number;
    todayOnly?: boolean;
    startDate?: string | Date;
    endDate?: string | Date;
    tableName?: string; // Added table name filter
  }) => {
    return await apiClient.getReplicationEvents(
      pipelineId,
      skip,
      limit,
      todayOnly || false,
      startDate,
      endDate,
      tableName
    );
  }
);

export const fetchMonitoringMetrics = createAsyncThunk(
  'monitoring/fetchMetrics',
  async ({
    pipelineId,
    startTime,
    endTime
  }: {
    pipelineId: number | string;
    startTime?: string | Date;
    endTime?: string | Date;
  }) => {
    // Convert to string if it's a number, or use as-is if it's already a string
    const id = typeof pipelineId === 'number' && !isNaN(pipelineId) ? pipelineId : String(pipelineId)
    // Don't proceed if ID is invalid
    if (!id || id === 'NaN' || id === 'undefined') {
      throw new Error('Invalid pipeline ID')
    }
    return await apiClient.getMonitoringMetrics(id, startTime, endTime);
  }
);

const monitoringSlice = createSlice({
  name: 'monitoring',
  initialState,
  reducers: {
    setSelectedPipeline: (state, action: PayloadAction<number | string | null>) => {
      state.selectedPipelineId = action.payload;
    },
    addReplicationEvent: (state, action: PayloadAction<ReplicationEvent>) => {
      // Check if event already exists (avoid duplicates)
      const existingIndex = state.events.findIndex(e => e.id === action.payload.id);
      if (existingIndex === -1) {
        state.events.unshift(action.payload);
        // Keep only last 1000 events
        if (state.events.length > 1000) {
          state.events = state.events.slice(0, 1000);
        }
      }
    },
    addMonitoringMetric: (state, action: PayloadAction<MonitoringMetric>) => {
      state.metrics.push(action.payload);
      // Keep only last 1000 metrics
      if (state.metrics.length > 1000) {
        state.metrics = state.metrics.slice(0, 1000);
      }
    },
    setRealTimeEnabled: (state, action: PayloadAction<boolean>) => {
      state.realTimeEnabled = action.payload;
    },
    clearEvents: (state) => {
      state.events = [];
    },
    clearError: (state) => {
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder

      .addCase(fetchReplicationEvents.pending, (state) => {
        if (state.events.length === 0) {
          state.isLoading = true;
        }
        state.error = null;
      })
      .addCase(fetchReplicationEvents.fulfilled, (state, action) => {
        state.isLoading = false;
        // CRITICAL FIX: Ensure events is always an array and log what we received
        const eventsArray = Array.isArray(action.payload) ? action.payload : [];
        console.log('[Redux] fetchReplicationEvents.fulfilled - Received', eventsArray.length, 'events');
        if (eventsArray.length > 0) {
          console.log('[Redux] Sample event:', {
            id: eventsArray[0].id,
            pipeline_id: eventsArray[0].pipeline_id,
            event_type: eventsArray[0].event_type,
            status: eventsArray[0].status
          });
        }
        state.events = eventsArray;
      })
      .addCase(fetchReplicationEvents.rejected, (state, action) => {
        state.isLoading = false;
        state.error = action.error.message || 'Failed to fetch events';
        // Ensure events remains an array even on error
        if (!Array.isArray(state.events)) {
          state.events = [];
        }
      })

      .addCase(fetchMonitoringMetrics.pending, (state) => {
        if (state.metrics.length === 0) {
          state.isLoading = true;
        }
      })
      .addCase(fetchMonitoringMetrics.fulfilled, (state, action) => {
        state.isLoading = false;
        // Ensure metrics is always an array
        state.metrics = Array.isArray(action.payload) ? action.payload : [];
      })
      .addCase(fetchMonitoringMetrics.rejected, (state, action) => {
        state.isLoading = false;
        state.error = action.error.message || 'Failed to fetch metrics';
        // Ensure metrics remains an array even on error
        if (!Array.isArray(state.metrics)) {
          state.metrics = [];
        }
      });
  },
});

export const {
  setSelectedPipeline,
  addReplicationEvent,
  addMonitoringMetric,
  setRealTimeEnabled,
  clearEvents,
  clearError,
} = monitoringSlice.actions;
export default monitoringSlice.reducer;

