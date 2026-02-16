/**
 * Pipeline Redux slice
 */
import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit';
import { apiClient } from '@/lib/api/client';

interface Pipeline {
  id: number;
  name: string;
  description?: string;
  source_connection_id: number;
  target_connection_id: number;
  source_connection_uuid?: string;  // Actual UUID for connection lookup
  target_connection_uuid?: string;  // Actual UUID for connection lookup
  full_load_type: 'overwrite' | 'append' | 'merge';
  cdc_enabled: boolean;
  cdc_filters?: any[];
  table_mappings: Array<{
    source_schema?: string;
    source_table: string;
    target_schema?: string;
    target_table: string;
  }>;
  airflow_dag_id?: string;
  status: 'draft' | 'active' | 'paused' | 'error' | 'deleted' | 'running' | 'failed' | 'stopped' | 'starting';
  created_at: string;
  updated_at?: string;
}

interface PipelineState {
  pipelines: Pipeline[];
  selectedPipeline: Pipeline | null;
  isLoading: boolean;
  error: string | null;
}

const initialState: PipelineState = {
  pipelines: [],
  selectedPipeline: null,
  isLoading: false,
  error: null,
};

// Async thunks
export const fetchPipelines = createAsyncThunk(
  'pipelines/fetchAll',
  async (_, { rejectWithValue }) => {
    try {
      return await apiClient.getPipelines();
    } catch (error: any) {
      // Provide more helpful error messages for timeouts
      if (error.isTimeout) {
        return rejectWithValue('Request timeout: The server took too long to respond. This may indicate a database connection issue. Please check if PostgreSQL is running and the backend is accessible.');
      }
      if (error.isNetworkError) {
        return rejectWithValue('Network error: Cannot connect to the backend server. Please ensure it is running on http://localhost:8000');
      }
      return rejectWithValue(error.message || 'Failed to fetch pipelines');
    }
  }
);

export const fetchPipeline = createAsyncThunk('pipelines/fetchOne', async (id: number) => {
  return await apiClient.getPipeline(id);
});

export const createPipeline = createAsyncThunk(
  'pipelines/create',
  async (pipelineData: any, { rejectWithValue }) => {
    try {
      return await apiClient.createPipeline(pipelineData);
    } catch (error: any) {
      // Extract error message from various possible locations
      let errorMessage = 'Failed to create pipeline';

      if (error.response?.data?.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.response?.data?.message) {
        errorMessage = error.response.data.message;
      } else if (error.message) {
        errorMessage = error.message;
      } else if (typeof error === 'string') {
        errorMessage = error;
      }

      // Include full error info for debugging
      const errorInfo = {
        message: errorMessage,
        status: error.response?.status,
        data: error.response?.data,
        code: error.code,
        isTimeout: error.isTimeout,
        isNetworkError: error.isNetworkError,
      };

      console.error('[Redux] createPipeline error:', errorInfo);
      return rejectWithValue(errorMessage);
    }
  }
);

export const updatePipeline = createAsyncThunk(
  'pipelines/update',
  async ({ id, data }: { id: string | number; data: any }, { rejectWithValue }) => {
    try {
      return await apiClient.updatePipeline(String(id), data);
    } catch (error: any) {
      // Extract error message properly
      let errorMessage = 'Failed to update pipeline';

      if (error?.response?.data?.detail) {
        const detail = error.response.data.detail;
        if (Array.isArray(detail)) {
          // Pydantic validation errors
          errorMessage = detail.map((err: any) => {
            if (typeof err === 'object' && err.loc && err.msg) {
              return `${err.loc.join('.')}: ${err.msg}`;
            } else if (typeof err === 'object' && err.msg) {
              return err.msg;
            } else if (typeof err === 'string') {
              return err;
            } else {
              return JSON.stringify(err);
            }
          }).join(', ');
        } else if (typeof detail === 'string') {
          errorMessage = detail;
        } else {
          errorMessage = JSON.stringify(detail);
        }
      } else if (error?.response?.data?.message) {
        errorMessage = error.response.data.message;
      } else if (error?.message) {
        errorMessage = error.message;
      }

      return rejectWithValue(errorMessage);
    }
  }
);

export const deletePipeline = createAsyncThunk(
  'pipelines/delete',
  async (id: number, { rejectWithValue }) => {
    try {
      await apiClient.deletePipeline(id);
      return id;
    } catch (error: any) {
      return rejectWithValue(error.response?.data?.detail || 'Failed to delete pipeline');
    }
  }
);

export const triggerPipeline = createAsyncThunk(
  'pipelines/trigger',
  async ({ id, runType }: { id: string | number; runType?: string }, { rejectWithValue, dispatch }) => {
    try {
      const result = await apiClient.triggerPipeline(String(id), runType);
      // Wait longer before refreshing to allow backend to fully update status
      await new Promise(resolve => setTimeout(resolve, 2000));
      // Refresh pipelines to get updated status
      dispatch(fetchPipelines());
      // Refresh again after a longer delay to ensure status is persisted
      setTimeout(() => {
        dispatch(fetchPipelines());
      }, 4000);
      return result;
    } catch (error: any) {
      // Extract detailed error message
      let errorMessage = 'Failed to trigger pipeline'

      if (error?.response?.data?.detail) {
        errorMessage = error.response.data.detail
      } else if (error?.response?.data?.message) {
        errorMessage = error.response.data.message
      } else if (error?.message) {
        errorMessage = error.message
      } else if (typeof error === 'string') {
        errorMessage = error
      }

      // Remove redundant wrapping if present
      if (errorMessage.includes("Failed to trigger pipeline") && errorMessage.length > 25) {
        // Keep the actual error, remove the wrapper
        const parts = errorMessage.split("Failed to trigger pipeline")
        if (parts.length > 1) {
          errorMessage = parts[parts.length - 1].trim()
          if (errorMessage.startsWith(":")) {
            errorMessage = errorMessage.substring(1).trim()
          }
        }
      }

      // Remove HTTP error wrapper if present
      if (errorMessage.includes("400 Client Error") || errorMessage.includes("Bad Request")) {
        // Extract the actual error message
        if (errorMessage.includes("for url:")) {
          const parts = errorMessage.split("for url:")
          if (parts.length > 0) {
            errorMessage = parts[0].replace("400 Client Error:", "").replace("Bad Request", "").trim()
          }
        }
        // Also check if there's a detail in the response
        if (error?.response?.data?.detail) {
          errorMessage = error.response.data.detail
        }
      }

      return rejectWithValue(errorMessage);
    }
  }
);

export const pausePipeline = createAsyncThunk(
  'pipelines/pause',
  async (id: string | number, { rejectWithValue, dispatch }) => {
    try {
      const result = await apiClient.pausePipeline(String(id));
      // Refresh pipelines to get updated status
      dispatch(fetchPipelines());
      return result;
    } catch (error: any) {
      return rejectWithValue(error.response?.data?.detail || 'Failed to pause pipeline');
    }
  }
);

export const stopPipeline = createAsyncThunk(
  'pipelines/stop',
  async (id: string | number, { rejectWithValue, dispatch }) => {
    try {
      const result = await apiClient.stopPipeline(String(id));
      // Refresh pipelines to get updated status
      dispatch(fetchPipelines());
      return result;
    } catch (error: any) {
      // Provide more detailed error message
      const errorMessage = error.response?.data?.detail ||
        error.message ||
        error.response?.data?.message ||
        'Failed to stop pipeline';
      const errorData = error.response?.data;
      console.error('[PipelineSlice] Stop pipeline error:', {
        status: error.response?.status,
        data: errorData,
        message: errorMessage,
        stack: error.stack
      });
      return rejectWithValue(errorMessage);
    }
  }
);

export const fetchPipelineStatus = createAsyncThunk(
  'pipelines/fetchStatus',
  async (id: string | number) => {
    return await apiClient.getPipelineStatus(id);
  }
);

const pipelineSlice = createSlice({
  name: 'pipelines',
  initialState,
  reducers: {
    setSelectedPipeline: (state, action: PayloadAction<Pipeline | null>) => {
      state.selectedPipeline = action.payload;
    },
    clearError: (state) => {
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder

      .addCase(fetchPipelines.pending, (state) => {
        if (state.pipelines.length === 0) {
          state.isLoading = true;
        }
        state.error = null;
      })
      .addCase(fetchPipelines.fulfilled, (state, action) => {
        state.isLoading = false;
        // Merge with existing pipelines to preserve optimistic updates
        // Only update if the backend status is more recent or different
        const newPipelines = action.payload;
        if (Array.isArray(newPipelines)) {
          // Create a map of new pipelines by ID
          const newPipelinesMap = new Map(newPipelines.map(p => [String(p.id), p]));

          // Update existing pipelines, but preserve optimistic 'active' status if backend hasn't caught up
          state.pipelines = state.pipelines.map(existingPipeline => {
            const newPipeline = newPipelinesMap.get(String(existingPipeline.id));
            if (newPipeline) {
              // If we optimistically set it to active and backend still shows stopped/starting,
              // keep it as active for a bit longer (backend might be slow to update)
              if (existingPipeline.status === 'active' &&
                newPipeline.status !== 'active' &&
                newPipeline.status !== 'running') {
                // Keep optimistic status for now, but update other fields
                return { ...newPipeline, status: 'active' };
              }
              return newPipeline;
            }
            return existingPipeline;
          });

          // Add any new pipelines that weren't in the existing list
          newPipelines.forEach(newPipeline => {
            const exists = state.pipelines.some(p => String(p.id) === String(newPipeline.id));
            if (!exists) {
              state.pipelines.push(newPipeline);
            }
          });
        } else {
          state.pipelines = newPipelines;
        }
      })
      .addCase(fetchPipelines.rejected, (state, action) => {
        state.isLoading = false;
        state.error = action.error.message || 'Failed to fetch pipelines';
      })
      .addCase(fetchPipeline.fulfilled, (state, action) => {
        state.selectedPipeline = action.payload;
      })
      .addCase(createPipeline.fulfilled, (state, action) => {
        state.pipelines.push(action.payload);
      })
      .addCase(updatePipeline.fulfilled, (state, action) => {
        const index = state.pipelines.findIndex((p) => p.id === action.payload.id);
        if (index !== -1) {
          state.pipelines[index] = action.payload;
        }
        if (state.selectedPipeline?.id === action.payload.id) {
          state.selectedPipeline = action.payload;
        }
      })
      .addCase(deletePipeline.fulfilled, (state, action) => {
        state.pipelines = state.pipelines.filter((p) => p.id !== action.payload);
        if (state.selectedPipeline?.id === action.payload) {
          state.selectedPipeline = null;
        }
      })
      .addCase(triggerPipeline.pending, (state, action) => {
        // Optimistically update status to active immediately
        const pipelineId = action.meta.arg.id;
        const pipeline = state.pipelines.find((p) => String(p.id) === String(pipelineId));
        if (pipeline) {
          pipeline.status = 'active';
        }
        if (state.selectedPipeline && String(state.selectedPipeline.id) === String(pipelineId)) {
          state.selectedPipeline.status = 'active';
        }
      })
      .addCase(triggerPipeline.fulfilled, (state, action) => {
        // Update pipeline status to active (in case pending didn't catch it)
        // The response might have 'id' field or 'pipeline_id'
        const pipelineId = action.payload?.id || action.payload?.pipeline_id || action.meta.arg.id;
        const pipeline = state.pipelines.find((p) => String(p.id) === String(pipelineId));
        if (pipeline) {
          pipeline.status = 'active';
        }
        if (state.selectedPipeline && String(state.selectedPipeline.id) === String(pipelineId)) {
          state.selectedPipeline.status = 'active';
        }
      })
      .addCase(triggerPipeline.rejected, (state, action) => {
        // Revert optimistic update on error
        const pipelineId = action.meta.arg.id;
        const pipeline = state.pipelines.find((p) => String(p.id) === String(pipelineId));
        if (pipeline && pipeline.status === 'active') {
          // Only revert if we optimistically set it - check if it was actually started
          // Don't revert if backend says it's active but request failed
          pipeline.status = 'stopped';
        }
      })
      .addCase(pausePipeline.fulfilled, (state, action) => {
        // Update pipeline status to paused
        const pipelineId = action.payload?.id || action.payload?.pipeline_id || action.meta.arg;
        const pipeline = state.pipelines.find((p) => String(p.id) === String(pipelineId));
        if (pipeline) {
          pipeline.status = 'paused';
        }
        if (state.selectedPipeline && String(state.selectedPipeline.id) === String(pipelineId)) {
          state.selectedPipeline.status = 'paused';
        }
      })
      .addCase(stopPipeline.fulfilled, (state, action) => {
        // Update pipeline status to stopped
        const pipelineId = action.payload?.id || action.payload?.pipeline_id || action.meta.arg;
        const pipeline = state.pipelines.find((p) => String(p.id) === String(pipelineId));
        if (pipeline) {
          pipeline.status = 'stopped';
        }
        if (state.selectedPipeline && String(state.selectedPipeline.id) === String(pipelineId)) {
          state.selectedPipeline.status = 'stopped';
        }
      });
  },
});

export const { setSelectedPipeline, clearError } = pipelineSlice.actions;
export default pipelineSlice.reducer;

