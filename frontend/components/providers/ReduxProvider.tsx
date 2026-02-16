/**
 * Redux Provider component
 */
'use client';

import { Provider } from 'react-redux';
import { store } from '@/lib/store/store';
import { useEffect } from 'react';
import { wsClient } from '@/lib/websocket/client';

export function ReduxProvider({ children }: { children: React.ReactNode }) {
  useEffect(() => {
    // Connect WebSocket on mount
    wsClient.connect();

    // Cleanup on unmount
    return () => {
      wsClient.disconnect();
    };
  }, []);

  return <Provider store={store}>{children}</Provider>;
}

