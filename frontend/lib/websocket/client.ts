/**
 * WebSocket client for real-time monitoring
 */
import { io, Socket } from 'socket.io-client';
import { store } from '@/lib/store/store';
import { addReplicationEvent, addMonitoringMetric, fetchReplicationEvents } from '@/lib/store/slices/monitoringSlice';

const WS_URL = process.env.NEXT_PUBLIC_WS_URL || 'http://localhost:8000';
const WS_ENABLED = process.env.NEXT_PUBLIC_WS_ENABLED !== 'false'; // Default to true unless explicitly disabled

class WebSocketClient {
  private socket: Socket | null = null;
  private subscribedPipelines: Set<number | string> = new Set();
  private isConnecting: boolean = false;
  private connectionFailed: boolean = false; // Track if connection has failed to prevent retries
  private errorCount: number = 0; // Track consecutive errors to suppress spam
  private maxErrorCount: number = 3; // Stop logging after 3 errors
  private statusListeners: Set<() => void> = new Set(); // Listeners for status changes

  connect() {
    // Check if WebSocket is enabled
    if (!WS_ENABLED) {
      console.log('[WebSocket] WebSocket is disabled via configuration');
      this.connectionFailed = true;
      return;
    }

    // If connection has permanently failed, don't retry automatically
    // User can still manually retry via retryConnection()
    if (this.connectionFailed && this.errorCount >= 5) {
      console.log('[WebSocket] Connection permanently failed. Use retryConnection() to manually retry.');
      return;
    }

    // Prevent multiple connection attempts
    if (this.socket?.connected) {
      console.log('[WebSocket] Already connected');
      return;
    }
    
    if (this.isConnecting) {
      console.log('[WebSocket] Connection already in progress');
      return;
    }

    // Reset failed state to allow retry (if error count is low)
    if (this.connectionFailed && this.errorCount < 5) {
      console.log('[WebSocket] Resetting failed state and attempting reconnection');
      this.connectionFailed = false;
      // Clean up old socket if exists
      if (this.socket) {
        this.socket.removeAllListeners();
        this.socket.disconnect();
        this.socket = null;
      }
    }

    this.isConnecting = true;
    console.log('[WebSocket] Attempting to connect to:', WS_URL);

    this.socket = io(WS_URL, {
      path: '/socket.io',
      transports: ['polling', 'websocket'], // Try polling first (more reliable fallback)
      reconnection: this.errorCount < 3, // Only enable reconnection if we haven't failed too many times
      reconnectionDelay: 2000, // Start with 2 second delay
      reconnectionDelayMax: 10000, // Max 10 second delay
      reconnectionAttempts: 2, // Reduced to 2 attempts to fail faster if backend doesn't support it
      timeout: 10000, // 10 second timeout (increased from 3s to handle slow connections)
      forceNew: false, // Reuse existing connection if available
      autoConnect: true,
      // Suppress default error logging
      withCredentials: false,
    });

    this.socket.on('connect', () => {
      console.log('========================================');
      console.log('[Frontend] WEBSOCKET CONNECTED');
      console.log('========================================');
      console.log('[Frontend] Socket ID:', this.socket?.id);
      console.log('[Frontend] Previously subscribed pipelines:', Array.from(this.subscribedPipelines));
      this.isConnecting = false;
      this.connectionFailed = false; // Reset failure flag on successful connection
      this.errorCount = 0; // Reset error count on successful connection
      // Notify all listeners about status change
      this.notifyStatusListeners();
      // Re-subscribe to previously subscribed pipelines
      this.subscribedPipelines.forEach((pipelineId) => {
        if (this.socket?.connected) {
          console.log(`[Frontend] Re-subscribing to pipeline: ${pipelineId}`);
          this.socket.emit('subscribe_pipeline', { pipeline_id: pipelineId });
        }
      });
      console.log('========================================');
    });

    this.socket.on('disconnect', (reason) => {
      console.log('WebSocket disconnected:', reason);
      this.isConnecting = false;
      // Notify all listeners about status change
      this.notifyStatusListeners();
      if (reason === 'io server disconnect') {
        // Server disconnected, try to reconnect manually
        this.socket?.connect();
      }
    });

    this.socket.on('connect_error', (error) => {
      this.errorCount++;
      this.isConnecting = false;
      
      // Only log first few errors to avoid console spam
      if (this.errorCount <= this.maxErrorCount) {
        console.warn(`[WebSocket] Connection error (${this.errorCount}/${this.maxErrorCount}):`, error.message);
        if (this.errorCount === this.maxErrorCount) {
          console.warn('[WebSocket] Suppressing further connection errors. Backend may not have Socket.IO configured.');
        }
      }
      
      // If we've had multiple errors quickly, mark as failed to stop retries
      // This prevents infinite retry loops when backend doesn't support WebSocket
      if (this.errorCount >= 3) {
        this.connectionFailed = true;
        if (this.socket) {
          this.socket.io.reconnecting = false;
          this.socket.disconnect();
          this.socket = null; // Clean up socket to prevent further attempts
        }
        console.warn('[WebSocket] Stopped connection attempts. Backend does not appear to support WebSocket. Using polling mode.');
      }
    });

    this.socket.on('reconnect_attempt', (attemptNumber) => {
      // Only log first few attempts to avoid spam
      if (attemptNumber <= 2) {
        console.log(`[WebSocket] Reconnection attempt ${attemptNumber}`);
      }
    });

    this.socket.on('reconnect_failed', () => {
      if (this.errorCount <= this.maxErrorCount) {
        console.warn('[WebSocket] All reconnection attempts failed. WebSocket unavailable. Using polling mode.');
      }
      this.connectionFailed = true;
      this.isConnecting = false;
      // Stop reconnection attempts
      if (this.socket) {
        this.socket.io.reconnecting = false;
      }
    });

    this.socket.on('reconnect', (attemptNumber) => {
      console.log(`[WebSocket] Successfully reconnected after ${attemptNumber} attempts`);
      this.connectionFailed = false;
      this.isConnecting = false;
      this.errorCount = 0; // Reset error count on reconnect
      // Notify all listeners about status change
      this.notifyStatusListeners();
      // Re-subscribe to all previously subscribed pipelines
      this.subscribedPipelines.forEach((pipelineId) => {
        if (this.socket?.connected) {
          console.log(`[WebSocket] Re-subscribing to pipeline: ${pipelineId}`);
          this.socket.emit('subscribe_pipeline', { pipeline_id: pipelineId });
        }
      });
    });

    this.socket.on('error', (error) => {
      // Suppress WebSocket errors - backend may not have Socket.IO configured
      // This is not critical for the application to function
      // Disable reconnection to prevent spam
      if (this.socket) {
        this.socket.io.reconnecting = false;
      }
      // Don't log to console to avoid cluttering logs
    });

    this.socket.on('replication_event', (data: any) => {
      try {
        console.log('========================================');
        console.log('[Frontend] RECEIVED REPLICATION EVENT');
        console.log('========================================');
        console.log('[Frontend] Event ID:', data.id);
        console.log('[Frontend] Pipeline ID:', data.pipeline_id);
        console.log('[Frontend] Event Type:', data.event_type);
        console.log('[Frontend] Table:', data.table_name);
        console.log('[Frontend] Status:', data.status);
        console.log('[Frontend] Full event data:', data);
        console.log('========================================');
        
        store.dispatch(addReplicationEvent(data));
        console.log('[Frontend] ✓ Event added to Redux store');
        
        // Refresh events from API with correct parameters when new event is received
        // This ensures the events list is up-to-date with all parameters applied
        const state = store.getState();
        const monitoringState = state.monitoring;
        const selectedPipelineId = monitoringState.selectedPipelineId;
        
        // Prepare fetch parameters based on current state
        const fetchParams: {
          pipelineId?: number | string;
          limit?: number;
          todayOnly?: boolean;
          startDate?: string | Date;
          endDate?: string | Date;
        } = {
          limit: 500,
          todayOnly: false, // Fetch all events, not just today's
        };
        
        // Add pipeline_id if a specific pipeline is selected, or use the event's pipeline_id
        if (selectedPipelineId) {
          fetchParams.pipelineId = selectedPipelineId;
        } else if (data.pipeline_id) {
          // If no pipeline is selected but event has pipeline_id, refresh with that pipeline
          fetchParams.pipelineId = data.pipeline_id;
        }
        
        // Dispatch fetch to refresh events list with correct parameters
        store.dispatch(fetchReplicationEvents(fetchParams));
        
        // Show browser notification for new events
        if (typeof window !== 'undefined' && 'Notification' in window && Notification.permission === 'granted') {
          new Notification('CDC Event Captured', {
            body: `${data.event_type?.toUpperCase() || 'EVENT'} on ${data.table_name || 'table'} - ${data.status || 'unknown'}`,
            icon: '/icon-dark-32x32.png',
          });
        }
      } catch (error) {
        console.error('Error handling replication event:', error);
      }
    });

    this.socket.on('monitoring_metric', (data: any) => {
      try {
        console.log('Received monitoring metric:', data);
        store.dispatch(addMonitoringMetric(data));
      } catch (error) {
        console.error('Error handling monitoring metric:', error);
      }
    });

    this.socket.on('pipeline_status', (data: any) => {
      try {
        console.log('Pipeline status update:', data);
        // Handle pipeline status updates
      } catch (error) {
        console.error('Error handling pipeline status:', error);
      }
    });
  }

  disconnect() {
    if (this.socket) {
      this.socket.disconnect();
      this.socket = null;
      this.subscribedPipelines.clear();
      this.isConnecting = false;
    }
  }

  subscribePipeline(pipelineId: number | string) {
    // Validate pipeline ID
    if (!pipelineId || pipelineId === 'NaN' || pipelineId === 'undefined' || 
        (typeof pipelineId === 'number' && (isNaN(pipelineId) || !isFinite(pipelineId)))) {
      console.error(`[Frontend] ✗ Invalid pipeline ID for subscription: ${pipelineId}`);
      return;
    }
    
    // Skip if already subscribed
    if (this.subscribedPipelines.has(pipelineId)) {
      console.log(`[Frontend] Already subscribed to pipeline: ${pipelineId}`);
      return;
    }

    console.log(`[Frontend] ========================================`);
    console.log(`[Frontend] SUBSCRIBING TO PIPELINE`);
    console.log(`[Frontend] ========================================`);
    console.log(`[Frontend] Pipeline ID: ${pipelineId} (type: ${typeof pipelineId})`);
    console.log(`[Frontend] WebSocket connected: ${this.socket?.connected || false}`);
    console.log(`[Frontend] Socket ID: ${this.socket?.id || 'N/A'}`);

    // Add to set first to prevent duplicate subscriptions
    this.subscribedPipelines.add(pipelineId);

    if (!this.socket?.connected) {
      console.log(`[Frontend] WebSocket not connected, connecting...`);
      this.connect();
      // Wait for connection before subscribing
      if (this.socket) {
        this.socket.once('connect', () => {
          if (this.socket?.connected) {
            console.log(`[Frontend] WebSocket connected, subscribing to pipeline: ${pipelineId}`);
            this.socket.emit('subscribe_pipeline', { pipeline_id: pipelineId });
            console.log(`[Frontend] ✓ Subscription request sent for pipeline: ${pipelineId}`);
          }
        });
      }
    } else if (this.socket.connected) {
      // Already connected, subscribe immediately
      console.log(`[Frontend] WebSocket already connected, subscribing immediately`);
      this.socket.emit('subscribe_pipeline', { pipeline_id: pipelineId });
      console.log(`[Frontend] ✓ Subscription request sent for pipeline: ${pipelineId}`);
    }
    console.log(`[Frontend] ========================================`);
  }

  unsubscribePipeline(pipelineId: number | string) {
    if (this.socket && this.subscribedPipelines.has(pipelineId)) {
      this.socket.emit('unsubscribe_pipeline', { pipeline_id: pipelineId });
      this.subscribedPipelines.delete(pipelineId);
    }
  }

  isConnected(): boolean {
    // Check actual connection state - if socket is connected, always return true
    return this.socket?.connected === true;
  }

  isAvailable(): boolean {
    // WebSocket is available if:
    // 1. It's connected (always available if connected, regardless of connectionFailed flag)
    // 2. It's still trying to connect (isConnecting)
    // 3. Socket exists and hasn't permanently failed (for initial connection attempts)
    // Priority: connected > connecting > not failed
    if (this.socket?.connected === true) {
      return true; // Always available if connected
    }
    if (this.isConnecting) {
      return true; // Available if connecting
    }
    // If not connected and not connecting, only available if not permanently failed
    return !this.connectionFailed && this.socket !== null;
  }

  reset(): void {
    // Reset connection state to allow retry
    console.log('[WebSocket] Resetting connection state');
    this.connectionFailed = false;
    this.isConnecting = false;
    this.errorCount = 0; // Reset error count
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.disconnect();
      this.socket = null;
    }
  }

  retryConnection(): void {
    // Manually trigger reconnection
    console.log('[WebSocket] Manual reconnection requested');
    this.reset();
    
    // Check if WebSocket is enabled
    if (!WS_ENABLED) {
      console.warn('[WebSocket] WebSocket is disabled via configuration. Enable it in environment variables.');
      this.connectionFailed = true;
      return;
    }
    
    this.connect();
  }

  disable(): void {
    // Manually disable WebSocket (useful for testing or when backend doesn't support it)
    console.log('[WebSocket] Manually disabling WebSocket');
    this.connectionFailed = true;
    this.isConnecting = false;
    if (this.socket) {
      this.socket.io.reconnecting = false;
      this.socket.removeAllListeners();
      this.socket.disconnect();
      this.socket = null;
    }
  }

  isDisabled(): boolean {
    // Check if WebSocket is disabled or permanently failed
    return !WS_ENABLED || (this.connectionFailed && this.errorCount >= 3);
  }

  hasFailed(): boolean {
    // Check if WebSocket connection has permanently failed (backend doesn't support Socket.IO)
    return this.connectionFailed;
  }

  // Status change notification system
  onStatusChange(callback: () => void): () => void {
    // Add listener
    this.statusListeners.add(callback);
    // Return unsubscribe function
    return () => {
      this.statusListeners.delete(callback);
    };
  }

  private notifyStatusListeners(): void {
    // Notify all listeners about status change
    this.statusListeners.forEach(listener => {
      try {
        listener();
      } catch (error) {
        console.error('Error in status listener:', error);
      }
    });
  }
}

export const wsClient = new WebSocketClient();

