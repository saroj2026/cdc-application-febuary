/**
 * Auth Redux slice
 */
import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit';
import { apiClient } from '@/lib/api/client';

interface User {
  id: string | number;
  email: string;
  full_name: string;
  is_active: boolean;
  is_superuser: boolean;
  role_name?: string; // Added to match backend response
  roles?: Array<{ id: number; name: string }>; // Optional, may not be in response
}

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
}

// Helper to safely get from localStorage
const getInitialToken = (): string | null => {
  if (typeof window === 'undefined') return null;
  try {
    return localStorage.getItem('access_token');
  } catch {
    return null;
  }
};

// Helper to safely get user from localStorage
const getInitialUser = (): User | null => {
  if (typeof window === 'undefined') return null;
  try {
    const userStr = localStorage.getItem('user');
    if (userStr) {
      return JSON.parse(userStr);
    }
  } catch {
    return null;
  }
  return null;
};

const initialState: AuthState = {
  user: getInitialUser(),
  token: getInitialToken(),
  isAuthenticated: !!getInitialToken(),
  isLoading: false,
  error: null,
};

// Async thunks
export const login = createAsyncThunk(
  'auth/login',
  async ({ email, password }: { email: string; password: string }, { rejectWithValue }) => {
    try {
      // Step 1: Login and get token
      const data = await apiClient.login(email, password);
      
      if (!data.access_token) {
        return rejectWithValue('No access token received from server');
      }
      
      // Step 2: Set token in API client and localStorage
      apiClient.setToken(data.access_token);
      
      // Step 3: Use user from login response (it already has all the data we need)
      // The login response includes the user object with is_superuser and role_name
      // IMPORTANT: Always use the user from login response, don't call getCurrentUser()
      if (data.user) {
        // Normalize user data - ensure is_superuser is boolean
        const user = { ...data.user }; // Create a copy to avoid mutating the original
        
        // Ensure is_superuser is a boolean
        if (typeof user.is_superuser !== 'boolean') {
          // Infer from role_name if available
          user.is_superuser = user.role_name === 'super_admin' || 
                             user.role_name === 'admin' || 
                             user.is_superuser === true ||
                             user.is_superuser === 'true' ||
                             String(user.is_superuser).toLowerCase() === 'true' ||
                             false;
        }
        
        // Ensure role_name is set
        if (!user.role_name && user.is_superuser) {
          user.role_name = 'super_admin';
        }
        
        console.log('[Auth] Login response user (BEFORE normalization):', data.user);
        console.log('[Auth] Login response user (AFTER normalization):', {
          email: user.email,
          is_superuser: user.is_superuser,
          role_name: user.role_name,
          full_user: user
        });
        
        return { token: data.access_token, user };
      }
      
      // This should never happen if backend is working correctly
      console.error('[Auth] Login response missing user object!', data);
      throw new Error('Login response missing user object');
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || error.message || 'Login failed';
      return rejectWithValue(errorMessage);
    }
  }
);

export const logout = createAsyncThunk('auth/logout', async () => {
  await apiClient.logout();
});

export const getCurrentUser = createAsyncThunk('auth/getCurrentUser', async (_, { rejectWithValue }) => {
  try {
    const user = await apiClient.getCurrentUser();
    console.log('[Auth] getCurrentUser RAW response:', user);
    
    // Normalize user data - ensure is_superuser is boolean
    if (user) {
      // Ensure is_superuser is a boolean
      if (typeof user.is_superuser !== 'boolean') {
        // Infer from role_name if available
        user.is_superuser = user.role_name === 'super_admin' || 
                           user.role_name === 'admin' || 
                           user.is_superuser === true ||
                           user.is_superuser === 'true' ||
                           String(user.is_superuser).toLowerCase() === 'true' ||
                           false;
      }
      
      // Ensure role_name is set
      if (!user.role_name && user.is_superuser) {
        user.role_name = 'super_admin';
      }
      
      console.log('[Auth] getCurrentUser NORMALIZED response:', {
        email: user.email,
        is_superuser: user.is_superuser,
        role_name: user.role_name,
        full_user: user
      });
    }
    
    if (typeof window !== 'undefined' && user) {
      localStorage.setItem('user', JSON.stringify(user));
    }
    return user;
  } catch (error: any) {
    console.error('[Auth] Failed to fetch current user:', error);
    // Don't clear auth state on error - keep cached user if available
    // Only clear if it's an authentication error
    const isAuthError = error?.response?.status === 401 || error?.response?.status === 403;
    if (isAuthError && typeof window !== 'undefined') {
      localStorage.removeItem('user');
      localStorage.removeItem('access_token');
    }
    return rejectWithValue(error.response?.data?.detail || 'Failed to fetch user info');
  }
});

export const createUser = createAsyncThunk(
  'auth/createUser',
  async (userData: { full_name: string; email: string; password: string }, { rejectWithValue }) => {
    try {
      const user = await apiClient.createUser(userData);
      return user;
    } catch (error: any) {
      return rejectWithValue(error.response?.data?.detail || 'Failed to create account');
    }
  }
);

const authSlice = createSlice({
  name: 'auth',
  initialState,
  reducers: {
    clearError: (state) => {
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder
      // Login
      .addCase(login.pending, (state) => {
        state.isLoading = true;
        state.error = null;
      })
      .addCase(login.fulfilled, (state, action) => {
        state.isLoading = false;
        state.token = action.payload.token;
        
        // Normalize user data - ensure is_superuser is boolean
        const user = action.payload.user;
        if (user) {
          // Ensure is_superuser is a boolean
          if (typeof user.is_superuser !== 'boolean') {
            // Infer from role_name if available
            user.is_superuser = user.role_name === 'super_admin' || 
                               user.role_name === 'admin' || 
                               user.is_superuser === true ||
                               user.is_superuser === 'true' ||
                               false;
          }
          
          // Ensure role_name is set
          if (!user.role_name && user.is_superuser) {
            user.role_name = 'super_admin';
          }
          
          // Log for debugging
          console.log('[Auth] Setting user after login:', {
            email: user.email,
            is_superuser: user.is_superuser,
            role_name: user.role_name,
            full_user: user
          });
          
          state.user = user;
        } else {
          state.user = null;
        }
        
        state.isAuthenticated = true;
        state.error = null;
        
        if (typeof window !== 'undefined' && user) {
          localStorage.setItem('user', JSON.stringify(user));
        }
      })
      .addCase(login.rejected, (state, action) => {
        state.isLoading = false;
        state.error = action.payload as string;
      })
      // Logout
      .addCase(logout.fulfilled, (state) => {
        state.user = null;
        state.token = null;
        state.isAuthenticated = false;
        if (typeof window !== 'undefined') {
          localStorage.removeItem('user');
        }
      })
      // Get current user
      .addCase(getCurrentUser.fulfilled, (state, action) => {
        const userData = action.payload;
        // Ensure is_superuser is properly set
        if (userData && typeof userData.is_superuser === 'boolean') {
          state.user = userData;
        } else {
          // If is_superuser is missing or invalid, set it to false and log warning
          console.warn('[Auth] User data missing is_superuser, setting to false:', userData);
          state.user = userData ? { ...userData, is_superuser: false } : null;
        }
        state.isAuthenticated = true;
        if (typeof window !== 'undefined' && userData) {
          localStorage.setItem('user', JSON.stringify(state.user));
        }
      })
      .addCase(getCurrentUser.rejected, (state, action) => {
        // Don't clear user data on error - keep cached user if available
        // Only clear if it's an authentication error
        const error = action.payload as any;
        const isAuthError = error?.response?.status === 401 || error?.response?.status === 403;
        
        if (isAuthError) {
          // Authentication error - clear everything
          state.user = null;
          state.isAuthenticated = false;
          state.token = null;
          if (typeof window !== 'undefined') {
            localStorage.removeItem('user');
            localStorage.removeItem('access_token');
          }
        } else {
          // Other errors (network, server) - keep cached user data
          console.warn('[Auth] getCurrentUser failed but keeping cached user data:', error);
          // Don't clear state - keep existing user data
        }
      })
      // Create user
      .addCase(createUser.pending, (state) => {
        state.isLoading = true;
        state.error = null;
      })
      .addCase(createUser.fulfilled, (state) => {
        state.isLoading = false;
        // User created successfully, redirect to login
      })
      .addCase(createUser.rejected, (state, action) => {
        state.isLoading = false;
        state.error = action.payload as string;
      });
  },
});

export const { clearError } = authSlice.actions;
export default authSlice.reducer;

