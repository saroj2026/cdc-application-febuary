import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit'
import { apiClient } from '@/lib/api/client'

interface User {
  id: string
  email: string
  full_name: string
  role_name: string
  status?: string
  is_active: boolean
  is_superuser: boolean
  last_login?: string
  created_at: string
}

interface UserState {
  users: User[]
  selectedUser: User | null
  isLoading: boolean
  error: string | null
}

const initialState: UserState = {
  users: [],
  selectedUser: null,
  isLoading: false,
  error: null,
}

// Async thunks
export const fetchUsers = createAsyncThunk('users/fetchUsers', async () => {
  const response = await apiClient.client.get('/api/v1/users')
  return response.data
})

export const createUser = createAsyncThunk('users/createUser', async (userData: any) => {
  const response = await apiClient.client.post('/api/v1/users', userData)
  return response.data
})

export const updateUser = createAsyncThunk('users/updateUser', async ({ id, userData }: { id: string; userData: any }) => {
  const response = await apiClient.client.put(`/api/v1/users/${id}`, userData)
  return response.data
})

export const deleteUser = createAsyncThunk('users/deleteUser', async (id: string) => {
  await apiClient.client.delete(`/api/v1/users/${id}`)
  return id
})

const userSlice = createSlice({
  name: 'users',
  initialState,
  reducers: {
    setSelectedUser: (state, action: PayloadAction<User | null>) => {
      state.selectedUser = action.payload
    },
    clearError: (state) => {
      state.error = null
    },
  },
  extraReducers: (builder) => {
    builder
      // Fetch users
      .addCase(fetchUsers.pending, (state) => {
        state.isLoading = true
        state.error = null
      })
      .addCase(fetchUsers.fulfilled, (state, action) => {
        state.isLoading = false
        state.users = Array.isArray(action.payload) ? action.payload : []
      })
      .addCase(fetchUsers.rejected, (state, action) => {
        state.isLoading = false
        state.error = action.error.message || 'Failed to fetch users'
      })
      // Create user
      .addCase(createUser.fulfilled, (state, action) => {
        state.users.push(action.payload)
      })
      .addCase(createUser.rejected, (state, action) => {
        state.error = action.error.message || 'Failed to create user'
      })
      // Update user
      .addCase(updateUser.fulfilled, (state, action) => {
        const index = state.users.findIndex(u => u.id === action.payload.id)
        if (index !== -1) {
          state.users[index] = action.payload
        }
      })
      .addCase(updateUser.rejected, (state, action) => {
        state.error = action.error.message || 'Failed to update user'
      })
      // Delete user
      .addCase(deleteUser.fulfilled, (state, action) => {
        state.users = state.users.filter(u => u.id !== action.payload)
      })
      .addCase(deleteUser.rejected, (state, action) => {
        state.error = action.error.message || 'Failed to delete user'
      })
  },
})

export const { setSelectedUser, clearError } = userSlice.actions
export default userSlice.reducer

