/**
 * Alerts and Errors Redux slice for real-time error tracking
 */
import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface Alert {
  id: string;
  type: 'error' | 'warning' | 'critical' | 'info';
  source: 'connection' | 'pipeline' | 'replication' | 'system';
  sourceId?: string | number;
  sourceName?: string;
  message: string;
  details?: string;
  timestamp: string;
  status: 'unresolved' | 'acknowledged' | 'resolved';
  severity: 'low' | 'medium' | 'high' | 'critical';
  table?: string;
  rowsAffected?: number;
}

interface AlertsState {
  alerts: Alert[];
  unreadCount: number;
  lastNotificationTime: string | null;
}

const initialState: AlertsState = {
  alerts: [],
  unreadCount: 0,
  lastNotificationTime: null,
};

const alertsSlice = createSlice({
  name: 'alerts',
  initialState,
  reducers: {
    addAlert: (state, action: PayloadAction<Alert>) => {
      // Check if alert already exists (prevent duplicates)
      const exists = state.alerts.some(a => a.id === action.payload.id);
      if (!exists) {
        state.alerts.unshift(action.payload); // Add to beginning
        if (action.payload.status === 'unresolved') {
          state.unreadCount++;
        }
        // Keep only last 1000 alerts
        if (state.alerts.length > 1000) {
          state.alerts = state.alerts.slice(0, 1000);
        }
      }
    },
    addAlerts: (state, action: PayloadAction<Alert[]>) => {
      action.payload.forEach(alert => {
        const exists = state.alerts.some(a => a.id === alert.id);
        if (!exists) {
          state.alerts.unshift(alert);
          if (alert.status === 'unresolved') {
            state.unreadCount++;
          }
        }
      });
      // Keep only last 1000 alerts
      if (state.alerts.length > 1000) {
        state.alerts = state.alerts.slice(0, 1000);
      }
    },
    updateAlertStatus: (state, action: PayloadAction<{ id: string; status: Alert['status'] }>) => {
      const alert = state.alerts.find(a => a.id === action.payload.id);
      if (alert) {
        const wasUnresolved = alert.status === 'unresolved';
        alert.status = action.payload.status;
        if (wasUnresolved && action.payload.status !== 'unresolved') {
          state.unreadCount = Math.max(0, state.unreadCount - 1);
        } else if (!wasUnresolved && action.payload.status === 'unresolved') {
          state.unreadCount++;
        }
      }
    },
    markAllAsRead: (state) => {
      state.unreadCount = 0;
      state.lastNotificationTime = new Date().toISOString();
    },
    clearAlerts: (state) => {
      state.alerts = [];
      state.unreadCount = 0;
    },
    removeAlert: (state, action: PayloadAction<string>) => {
      const alert = state.alerts.find(a => a.id === action.payload);
      if (alert && alert.status === 'unresolved') {
        state.unreadCount = Math.max(0, state.unreadCount - 1);
      }
      state.alerts = state.alerts.filter(a => a.id !== action.payload);
    },
  },
});

export const { addAlert, addAlerts, updateAlertStatus, markAllAsRead, clearAlerts, removeAlert } = alertsSlice.actions;
export default alertsSlice.reducer;

