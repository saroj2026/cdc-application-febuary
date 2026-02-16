/**
 * Redux store configuration
 */
import { configureStore } from '@reduxjs/toolkit';
import authReducer from './slices/authSlice';
import connectionReducer from './slices/connectionSlice';
import pipelineReducer from './slices/pipelineSlice';
import monitoringReducer from './slices/monitoringSlice';
import alertsReducer from './slices/alertsSlice';
import permissionReducer from './slices/permissionSlice';
import userReducer from './slices/userSlice';

export const store = configureStore({
  reducer: {
    auth: authReducer,
    connections: connectionReducer,
    pipelines: pipelineReducer,
    monitoring: monitoringReducer,
    alerts: alertsReducer,
    permissions: permissionReducer,
    users: userReducer,
  },
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;

