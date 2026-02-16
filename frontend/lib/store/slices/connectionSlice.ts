/**
 * Connection Redux slice
 */
import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit';
import { apiClient } from '@/lib/api/client';

interface Connection {
  id: number;
  name: string;
  description?: string;
  connection_type: string; // This is now database_type (postgresql, mysql, etc.) from backend
  database_type?: string; // Explicit database_type field
  role: 'source' | 'target';
  host: string;
  port: number;
  database: string;
  username: string;
  ssl_enabled: boolean;
  is_active: boolean;
  last_tested_at?: string;
  last_test_status?: string;
  created_at: string;
  updated_at?: string;
}

interface ConnectionState {
  connections: Connection[];
  selectedConnection: Connection | null;
  isLoading: boolean;
  error: string | null;
}

// Utility to ensure error is always a string
const ensureStringError = (error: any): string => {
  if (!error) return 'An error occurred';
  if (typeof error === 'string') return error;
  if (Array.isArray(error)) {
    return error.map((err: any) => {
      if (typeof err === 'string') return err;
      const field = err.loc?.join('.') || 'field';
      return `${field}: ${err.msg || err.message || 'Invalid value'}`;
    }).join(', ');
  }
  if (typeof error === 'object') {
    return error.message || error.msg || error.detail || JSON.stringify(error);
  }
  return String(error);
};

const initialState: ConnectionState = {
  connections: [],
  selectedConnection: null,
  isLoading: false,
  error: null,
};

// Async thunks
export const fetchConnections = createAsyncThunk(
  'connections/fetchAll',
  async (_, { rejectWithValue }) => {
    try {
      return await apiClient.getConnections();
    } catch (error: any) {
      // Provide more helpful error messages for timeouts
      if (error.isTimeout) {
        return rejectWithValue('Request timeout: The server took too long to respond. This may indicate a database connection issue. Please check if PostgreSQL is running and the backend is accessible.');
      }
      if (error.isNetworkError) {
        return rejectWithValue('Network error: Cannot connect to the backend server. Please ensure it is running on http://localhost:8000');
      }
      return rejectWithValue(error.message || 'Failed to fetch connections');
    }
  }
);

export const fetchConnection = createAsyncThunk('connections/fetchOne', async (id: number) => {
  return await apiClient.getConnection(id);
});

// Helper function to extract error message from FastAPI error response
const extractErrorMessage = (error: any): string => {
  if (!error?.response?.data) {
    return error?.message || 'An unexpected error occurred';
  }
  
  const detail = error.response.data.detail;
  
  // If detail is a string, return it
  if (typeof detail === 'string') {
    return detail;
  }
  
  // If detail is an array (validation errors), format them
  if (Array.isArray(detail)) {
    return detail.map((err: any) => {
      const field = err.loc?.join('.') || 'field';
      return `${field}: ${err.msg || 'Invalid value'}`;
    }).join(', ');
  }
  
  // If detail is an object, try to get message
  if (typeof detail === 'object' && detail !== null) {
    return detail.message || detail.msg || JSON.stringify(detail);
  }
  
  return 'Failed to create connection';
};

export const createConnection = createAsyncThunk(
  'connections/create',
  async (connectionData: any, { rejectWithValue }) => {
    try {
      return await apiClient.createConnection(connectionData);
    } catch (error: any) {
      return rejectWithValue(extractErrorMessage(error));
    }
  }
);

export const updateConnection = createAsyncThunk(
  'connections/update',
  async ({ id, data }: { id: number; data: any }, { rejectWithValue }) => {
    try {
      return await apiClient.updateConnection(id, data);
    } catch (error: any) {
      return rejectWithValue(extractErrorMessage(error));
    }
  }
);

export const deleteConnection = createAsyncThunk(
  'connections/delete',
  async (id: number, { rejectWithValue }) => {
    try {
      await apiClient.deleteConnection(id);
      return id;
    } catch (error: any) {
      return rejectWithValue(extractErrorMessage(error));
    }
  }
);

export const testConnection = createAsyncThunk(
  'connections/test',
  async (id: number, { rejectWithValue }) => {
    try {
      return await apiClient.testConnection(id);
    } catch (error: any) {
      // Handle timeout errors specifically
      if (error.isTimeout || error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
        return rejectWithValue('Connection test timed out. The database server may be unreachable or the connection is taking too long. Please check network connectivity and firewall settings.');
      }
      // Handle network errors
      if (error.isNetworkError || error.code === 'ERR_NETWORK' || error.code === 'ECONNREFUSED') {
        return rejectWithValue('Network error: Cannot connect to the backend server. Please ensure it is running on http://localhost:8000');
      }
      // Extract error message from backend response
      return rejectWithValue(extractErrorMessage(error));
    }
  }
);

const connectionSlice = createSlice({
  name: 'connections',
  initialState,
  reducers: {
    setSelectedConnection: (state, action: PayloadAction<Connection | null>) => {
      state.selectedConnection = action.payload;
    },
    clearError: (state) => {
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchConnections.pending, (state) => {
        state.isLoading = true;
        state.error = null;
      })
      .addCase(fetchConnections.fulfilled, (state, action) => {
        state.isLoading = false;
        state.connections = action.payload;
      })
      .addCase(fetchConnections.rejected, (state, action) => {
        state.isLoading = false;
        state.error = ensureStringError(action.payload || action.error.message || 'Failed to fetch connections');
      })
      .addCase(createConnection.rejected, (state, action) => {
        state.isLoading = false;
        state.error = ensureStringError(action.payload || 'Failed to create connection');
      })
      .addCase(updateConnection.rejected, (state, action) => {
        state.isLoading = false;
        state.error = ensureStringError(action.payload || 'Failed to update connection');
      })
      .addCase(deleteConnection.rejected, (state, action) => {
        state.isLoading = false;
        state.error = ensureStringError(action.payload || 'Failed to delete connection');
      })
      .addCase(testConnection.pending, (state) => {
        state.isLoading = true;
        state.error = null;
      })
      .addCase(testConnection.fulfilled, (state, action) => {
        state.isLoading = false;
        // Update the connection's test status in the list
        const connectionId = action.meta.arg;
        // Convert to string for comparison (backend uses string UUIDs, frontend might use numbers)
        const connectionIdStr = String(connectionId);
        const index = state.connections.findIndex((c) => String(c.id) === connectionIdStr);
        if (index !== -1) {
          const result = action.payload;
          // Determine status: check result.success first, then result.status
          let testStatus = 'failed';
          if (result.success === true || result.success === 'true') {
            testStatus = 'success';
          } else if (result.status === 'SUCCESS' || result.status === 'success') {
            testStatus = 'success';
          } else if (result.success === false || result.status === 'FAILED' || result.status === 'failed') {
            testStatus = 'failed';
          }
          
          console.log('[testConnection.fulfilled] Updating connection status:', {
            connectionId,
            connectionIdStr,
            index,
            result,
            testStatus,
            currentStatus: state.connections[index].last_test_status
          });
          
          state.connections[index] = {
            ...state.connections[index],
            last_test_status: testStatus,
            last_tested_at: result.tested_at || new Date().toISOString(),
          };
        } else {
          console.warn('[testConnection.fulfilled] Connection not found in state:', {
            connectionId,
            connectionIdStr,
            availableIds: state.connections.map(c => ({ id: c.id, idType: typeof c.id }))
          });
        }
      })
      .addCase(testConnection.rejected, (state, action) => {
        state.isLoading = false;
        state.error = ensureStringError(action.payload || 'Connection test failed');
        // Update the connection's test status even on failure
        const connectionId = action.meta.arg;
        // Convert to string for comparison (backend uses string UUIDs, frontend might use numbers)
        const connectionIdStr = String(connectionId);
        const index = state.connections.findIndex((c) => String(c.id) === connectionIdStr);
        if (index !== -1) {
          state.connections[index] = {
            ...state.connections[index],
            last_test_status: 'failed',
            last_tested_at: new Date().toISOString(),
          };
        }
      })
      .addCase(fetchConnection.fulfilled, (state, action) => {
        state.selectedConnection = action.payload;
      })
      .addCase(createConnection.fulfilled, (state, action) => {
        state.connections.push(action.payload);
      })
      .addCase(updateConnection.fulfilled, (state, action) => {
        const index = state.connections.findIndex((c) => c.id === action.payload.id);
        if (index !== -1) {
          state.connections[index] = action.payload;
        }
        if (state.selectedConnection?.id === action.payload.id) {
          state.selectedConnection = action.payload;
        }
      })
      .addCase(deleteConnection.fulfilled, (state, action) => {
        state.connections = state.connections.filter((c) => c.id !== action.payload);
        if (state.selectedConnection?.id === action.payload) {
          state.selectedConnection = null;
        }
      });
  },
});

export const { setSelectedConnection, clearError } = connectionSlice.actions;
export default connectionSlice.reducer;

