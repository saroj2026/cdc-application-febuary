/**
 * API client for Python backend
 */
import axios, { AxiosInstance, AxiosError } from 'axios';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

class ApiClient {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: API_BASE_URL,
      headers: {
        'Content-Type': 'application/json',
      },
      timeout: 10000, // 10 seconds timeout for API requests (reduced from 45s)
    });

    // Add auth token to requests
    this.client.interceptors.request.use((config) => {
      const token = this.getToken();
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    });

    // Handle auth errors and timeouts
    this.client.interceptors.response.use(
      (response) => response,
      (error: AxiosError) => {
        // Handle timeout errors - but don't log for endpoints with their own retry logic
        if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
          // Don't log for endpoints with retry logic - they handle their own errors silently
          const url = error.config?.url || '';
          const hasRetryLogic = url.includes('/api/v1/connections/') ||
            url.includes('/api/v1/pipelines/') ||
            url.includes('/api/v1/monitoring/') ||
            url.includes('/api/v1/audit-logs') ||
            url.includes('/api/v1/logs/');
          // Table discovery endpoints can be slow, don't log timeout errors
          const isTableDiscovery = url.includes('/tables') && !url.includes('/data');
          // Suppress all timeout logging for endpoints with retry logic or table discovery
          // Only log in development for endpoints without retry logic
          if (!hasRetryLogic && !isTableDiscovery && process.env.NODE_ENV === 'development') {
            console.error('Request timeout:', url);
          }
          // Create a more helpful error message based on the endpoint
          let errorMessage = 'Request timeout: The server took too long to respond.';
          if (url.includes('/tables') && !url.includes('/data')) {
            errorMessage = 'Request timeout: Table discovery is taking longer than expected. This may indicate a database connection issue. Please check if the database is running and accessible.';
          } else if (url.includes('/test')) {
            errorMessage = 'Request timeout: Connection test is taking longer than expected. This may indicate a database connection issue. Please check if the database is running and the backend is accessible.';
          } else if (url.includes('/trigger') || url.includes('/start')) {
            errorMessage = 'Request timeout: Pipeline start is taking longer than expected. Please check backend logs for details.';
          } else {
            errorMessage = 'Request timeout: The server took too long to respond. This may indicate a database connection issue. Please check if PostgreSQL is running and the backend is accessible.';
          }

          const timeoutError = new Error(errorMessage);
          (timeoutError as any).isTimeout = true;
          (timeoutError as any).code = 'ECONNABORTED';
          return Promise.reject(timeoutError);
        }

        // Handle network errors - but skip for table data endpoints which have their own error handling
        const url = error.config?.url || error.request?.responseURL || '';
        const isTableDataEndpoint = url.includes('/tables/') && url.includes('/data');

        // Log for debugging
        if (process.env.NODE_ENV === 'development') {
          console.log('[Axios Interceptor] Error caught:', {
            url,
            isTableDataEndpoint,
            errorCode: error.code,
            errorMessage: error.message,
            hasResponse: !!error.response,
            responseStatus: error.response?.status
          });
        }

        // Only handle network errors for non-table-data endpoints
        // Table data endpoints have longer timeouts and better error handling
        if (!isTableDataEndpoint && (error.code === 'ECONNREFUSED' || error.message?.includes('Network Error'))) {
          const networkError = new Error('Cannot connect to server. Please ensure the backend is running on http://localhost:8000');
          (networkError as any).isNetworkError = true;
          return Promise.reject(networkError);
        }

        // For table data endpoints, preserve the original error so getTableData can handle it
        if (isTableDataEndpoint) {
          // Mark network errors but don't transform them - let getTableData handle it
          if (error.code === 'ECONNREFUSED' || error.message?.includes('Network Error') || error.code === 'ERR_NETWORK') {
            (error as any).isNetworkError = true;
          }
          // Don't transform the error - let getTableData handle it with proper error messages
          return Promise.reject(error);
        }

        if (error.response?.status === 401 || error.response?.status === 403) {
          // Clear token and redirect to login for auth errors
          this.clearToken();
          if (typeof window !== 'undefined') {
            const currentPath = window.location.pathname;
            // Only redirect if not already on a login/signup page
            if (!currentPath.startsWith('/auth/login') && !currentPath.startsWith('/auth/signup')) {
              window.location.href = '/auth/login';
            }
          }
        }
        return Promise.reject(error);
      }
    );
  }

  private getToken(): string | null {
    if (typeof window === 'undefined') return null;
    return localStorage.getItem('access_token');
  }

  private clearToken(): void {
    if (typeof window !== 'undefined') {
      localStorage.removeItem('access_token');
      localStorage.removeItem('user');
    }
  }

  setToken(token: string): void {
    if (typeof window !== 'undefined') {
      localStorage.setItem('access_token', token);
      // Force update the axios default headers
      this.client.defaults.headers.common['Authorization'] = `Bearer ${token}`;
    }
  }

  async backendHealthCheck(): Promise<void> {
    try {
      const response = await this.client.get('/api/health', { timeout: 3000 });
      if (!response.data || response.data.status !== 'healthy') {
        throw new Error('Backend health check failed');
      }
    } catch (error: any) {
      throw new Error('Backend is not available. Please ensure it is running on http://localhost:8000');
    }
  }

  // Auth endpoints
  async login(email: string, password: string) {
    try {
      const response = await this.client.post('/api/v1/auth/login', {
        email,
        password,
      });
      return response.data;
    } catch (error: any) {
      // Handle network errors
      if (error.code === 'ECONNREFUSED' || error.message?.includes('Network Error')) {
        throw new Error('Cannot connect to server. Please ensure the backend is running on http://localhost:8000');
      }
      // Re-throw to let the caller handle it
      throw error;
    }
  }

  async logout() {
    await this.client.post('/api/v1/auth/logout');
    this.clearToken();
  }

  async getCurrentUser() {
    const response = await this.client.get('/api/v1/auth/me');
    return response.data;
  }

  async forgotPassword(email: string) {
    const response = await this.client.post('/api/v1/auth/forgot-password', { email });
    return response.data;
  }

  async resetPassword(token: string, newPassword: string) {
    const response = await this.client.post('/api/v1/auth/reset-password', {
      token,
      new_password: newPassword
    });
    return response.data;
  }

  async adminChangePassword(userId: string, newPassword: string, sendEmail: boolean = true) {
    const response = await this.client.post(`/api/v1/users/${userId}/change-password`, {
      new_password: newPassword,
      send_email: sendEmail
    });
    return response.data;
  }

  // User endpoints
  async getUsers(skip = 0, limit = 100) {
    const response = await this.client.get('/api/v1/users/', {
      params: { skip, limit },
    });
    return response.data;
  }

  async createUser(userData: any) {
    const response = await this.client.post('/api/v1/users/', userData);
    return response.data;
  }

  async updateUser(userId: string | number, userData: any) {
    const response = await this.client.put(`/api/v1/users/${String(userId)}`, userData);
    return response.data;
  }

  async deleteUser(userId: string | number) {
    await this.client.delete(`/api/v1/users/${String(userId)}`);
  }

  // Track pending requests to prevent duplicates
  private pendingRequests = new Map<string, Promise<any>>();

  // Helper method for retry logic with timeout and network error handling
  private async retryRequest<T>(
    requestFn: () => Promise<T>,
    endpointName: string,
    retries = 1, // Reduced from 2 to 1 (only 1 retry)
    timeout = 20000,
    requestKey?: string // Optional key for deduplication
  ): Promise<T> {
    // Deduplicate requests if key is provided
    if (requestKey && this.pendingRequests.has(requestKey)) {
      return this.pendingRequests.get(requestKey)!;
    }

    const makeRequest = async (): Promise<T> => {
      let lastError: any;
      for (let attempt = 0; attempt <= retries; attempt++) {
        try {
          const result = await requestFn();
          // Remove from pending on success
          if (requestKey) {
            this.pendingRequests.delete(requestKey);
          }
          return result;
        } catch (error: any) {
          lastError = error;

          // Don't retry on 401 (auth errors) or 4xx errors
          if (error.response?.status >= 400 && error.response?.status < 500) {
            if (requestKey) {
              this.pendingRequests.delete(requestKey);
            }
            throw error;
          }

          // Check for timeout or network errors
          const isTimeout = error.code === 'ECONNABORTED' ||
            error.message?.includes('timeout') ||
            error.isTimeout;
          const isNetworkError = error.code === 'ERR_NETWORK' ||
            error.message?.includes('Network Error') ||
            error.isNetworkError;

          // Don't retry on timeout - fail fast
          if (isTimeout) {
            if (requestKey) {
              this.pendingRequests.delete(requestKey);
            }
            const timeoutError = new Error(`Request timeout: The server took too long to respond for ${endpointName}. This may indicate a database connection issue. Please check if PostgreSQL is running and the backend is accessible.`);
            (timeoutError as any).isTimeout = true;
            (timeoutError as any).code = 'ECONNABORTED';
            throw timeoutError;
          }

          // Retry only on network errors (connection refused, etc.)
          if (attempt < retries && isNetworkError) {
            const delay = 1000 * (attempt + 1); // Linear backoff: 1s, 2s
            // Only log in development mode to reduce console noise
            if (process.env.NODE_ENV === 'development') {
              console.warn(`${endpointName} fetch attempt ${attempt + 1}/${retries + 1} failed, retrying in ${delay}ms...`);
            }
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }

          // If no more retries, mark error appropriately
          if (isNetworkError) {
            if (requestKey) {
              this.pendingRequests.delete(requestKey);
            }
            const networkError = new Error(`Network error: Cannot connect to the backend server for ${endpointName}. Please ensure it is running on http://localhost:8000`);
            (networkError as any).isNetworkError = true;
            (networkError as any).code = 'ERR_NETWORK';
            throw networkError;
          }

          // Remove from pending on error
          if (requestKey) {
            this.pendingRequests.delete(requestKey);
          }
          throw error;
        }
      }
      // If all retries failed, throw the last error with proper marking
      if (requestKey) {
        this.pendingRequests.delete(requestKey);
      }
      throw lastError || new Error(`Failed to fetch ${endpointName}`);
    };

    const requestPromise = makeRequest();

    // Store pending request if key is provided
    if (requestKey) {
      this.pendingRequests.set(requestKey, requestPromise);
    }

    return requestPromise;
  }

  // Connection endpoints
  async getConnections(skip = 0, limit = 100, retries = 1) {
    const requestKey = `connections-${skip}-${limit}`;
    return this.retryRequest(
      () => this.client.get('/api/v1/connections/', {
        params: { skip, limit },
        timeout: 10000, // Reduced to 10 seconds
      }).then(res => res.data),
      'connections',
      retries,
      10000,
      requestKey
    );
  }

  async getConnection(connectionId: number) {
    const response = await this.client.get(`/api/v1/connections/${connectionId}`);
    return response.data;
  }

  async createConnection(connectionData: any) {
    console.log('[API Client] createConnection called with:', { ...connectionData, password: '***' });
    const response = await this.client.post('/api/v1/connections/', connectionData);
    console.log('[API Client] createConnection response:', response.data);
    return response.data;
  }

  async updateConnection(connectionId: number, connectionData: any) {
    const response = await this.client.put(`/api/v1/connections/${connectionId}`, connectionData);
    return response.data;
  }

  async deleteConnection(connectionId: number) {
    await this.client.delete(`/api/v1/connections/${connectionId}`);
  }

  async testConnection(connectionId: number) {
    // Use longer timeout for connection tests (35s to account for 30s PostgreSQL timeout + overhead)
    const response = await this.client.post(
      `/api/v1/connections/${connectionId}/test`,
      {},
      { timeout: 35000 }
    );
    return response.data;
  }

  async getConnectionTables(connectionId: number | string) {
    // Use longer timeout for table discovery (AS400 can be slow)
    const response = await this.client.get(`/api/v1/connections/${connectionId}/tables`, {
      timeout: 60000, // 60 seconds for table discovery
    });
    return response.data;
  }

  async getTableData(connectionId: number | string, tableName: string, schema?: string, limit: number = 100, retries: number = 2, isOracleConnection?: boolean) {
    const params = new URLSearchParams({ limit: limit.toString() });
    // Only append schema if it's a valid non-empty string (not "undefined" string)
    if (schema && schema !== "undefined" && schema.trim() !== "") {
      params.append('schema', schema.trim());
    }
    const url = `/api/v1/connections/${connectionId}/tables/${tableName}/data?${params}`;
    const fullUrl = `${this.client.defaults.baseURL || ''}${url}`;

    // Enhanced logging for debugging
    if (process.env.NODE_ENV === 'development') {
      console.log('[API Client] getTableData request:', {
        connectionId,
        tableName,
        schema,
        limit,
        url,
        fullUrl,
        baseURL: this.client.defaults.baseURL,
        retries,
        isOracleConnection
      });
    }

    // Check backend health first (but don't fail if it's slow - just log)
    try {
      const healthController = new AbortController();
      const healthTimeout = setTimeout(() => healthController.abort(), 2000); // 2s timeout for health check
      try {
        await fetch(`${this.client.defaults.baseURL || 'http://localhost:8000'}/api/health`, {
          method: 'GET',
          signal: healthController.signal
        });
      } finally {
        clearTimeout(healthTimeout);
      }
    } catch (healthError: any) {
      // Don't throw - just log, as backend might be slow but still working
      if (process.env.NODE_ENV === 'development') {
        console.warn('[API Client] Backend health check failed, but continuing:', healthError.message);
      }
    }

    let lastError: any = null;

    // Determine if Oracle or SQL Server connection for timeout adjustment
    // Backend timeout is 120s for Oracle/SQL Server, 60s for others
    // Frontend timeout should be slightly longer to allow backend to complete
    const isOracle = isOracleConnection !== undefined ? isOracleConnection :
      (url.includes('oracle') || connectionId.toString().toLowerCase().includes('oracle'));
    const isSqlServer = url.includes('sqlserver') || url.includes('sql-server') ||
      connectionId.toString().toLowerCase().includes('sqlserver');
    // Use 130s for Oracle/SQL Server (slightly longer than backend 120s), 70s for others (slightly longer than backend 60s)
    const frontendTimeout = (isOracle || isSqlServer) ? 130000 : 70000;

    for (let attempt = 0; attempt <= retries; attempt++) {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => {
        controller.abort();
      }, frontendTimeout);

      try {
        const response = await this.client.get(url, {
          timeout: frontendTimeout,
          signal: controller.signal,
        });
        clearTimeout(timeoutId);

        // Validate response
        if (!response || !response.data) {
          throw new Error('Backend returned an empty or invalid response');
        }

        // Check if response has expected structure
        if (typeof response.data !== 'object') {
          throw new Error('Backend returned an invalid response format');
        }

        return response.data;
      } catch (err: any) {
        clearTimeout(timeoutId);
        lastError = err;

        // Enhanced error logging for debugging
        if (process.env.NODE_ENV === 'development') {
          console.error(`[API Client] getTableData error (attempt ${attempt + 1}/${retries + 1}):`, {
            name: err.name,
            code: err.code,
            message: err.message,
            response: err.response ? {
              status: err.response.status,
              statusText: err.response.statusText,
              data: err.response.data,
            } : null,
            isNetworkError: !err.response,
            fullUrl
          });
        }

        // IMPORTANT: Check for HTTP responses FIRST (before timeout/network checks)
        // This ensures 500 errors are properly handled as server errors, not network errors

        // Check for server errors (500+) - don't retry these
        if (err.response && err.response.status >= 500) {
          // Server error - don't retry, just store and break
          lastError = err;
          break;
        }

        // Check for client errors (400-499) - don't retry these
        if (err.response && err.response.status >= 400 && err.response.status < 500) {
          // Client error - don't retry, just store and break
          lastError = err;
          break;
        }

        // Don't retry on abort/timeout - fail fast
        if (err.name === 'AbortError' || err.code === 'ECONNABORTED' || err.message?.includes('aborted')) {
          if (isOracle) {
            lastError = new Error(
              'Oracle Connection Timeout: The request to Oracle database timed out.\n\n' +
              'This usually means:\n' +
              '1. Oracle server is not reachable\n' +
              '2. Oracle listener is not running\n' +
              '3. Network/firewall is blocking the connection\n' +
              '4. Service name is incorrect\n\n' +
              'Please check:\n' +
              '- Oracle server is running and accessible\n' +
              '- Oracle listener is active (run "lsnrctl status" on Oracle server)\n' +
              '- Network connectivity to Oracle server\n' +
              '- Service name is correct (should be XE, ORCL, PDB1, etc.)\n' +
              '- Firewall allows connections on Oracle port'
            );
            (lastError as any).isTimeout = true;
            (lastError as any).code = 'ECONNABORTED';
          } else {
            lastError = new Error('Request timeout: The database query took too long. Please try again or check your database connection.');
            (lastError as any).isTimeout = true;
            (lastError as any).code = 'ECONNABORTED';
          }
          break; // Don't retry timeouts
        }

        // Check for empty response or parsing errors - don't retry
        if (err.response && (!err.response.data || (typeof err.response.data === 'object' && Object.keys(err.response.data).length === 0))) {
          lastError = new Error('Server returned an empty response. The backend may have crashed or encountered an error. Please check the backend logs.');
          (lastError as any).isNetworkError = true;
          break; // Don't retry empty responses
        }

        // Check for network errors - retry if not last attempt
        // ONLY check for network errors if there's NO response (error.response is null/undefined)
        const isNetworkError = !err.response && (
          err.code === 'ECONNREFUSED' ||
          err.code === 'ERR_NETWORK' ||
          err.code === 'ERR_INTERNET_DISCONNECTED' ||
          err.message === 'Network Error' ||
          err.message?.includes('Network Error')
        );

        if (isNetworkError && attempt < retries) {
          // Wait before retry (exponential backoff)
          const delay = Math.min(1000 * Math.pow(2, attempt), 5000);
          console.log(`[API Client] Network error, retrying in ${delay}ms (attempt ${attempt + 1}/${retries + 1})`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue; // Retry
        }

        // If last attempt or not a network error, break immediately
        // Don't continue the loop - this prevents multiple error throws
        break;
      }
    }

    // Process final error - only if we have one
    if (!lastError) {
      throw new Error('Unexpected error: No error was captured but request failed');
    }

    const error = lastError;

    // Log the full error for debugging - handle different error types
    if (process.env.NODE_ENV === 'development') {
      const errorInfo: any = {
        errorType: typeof error,
        errorConstructor: error?.constructor?.name,
        isError: error instanceof Error,
      };

      // Try to extract all possible error properties
      if (error instanceof Error) {
        errorInfo.message = error.message;
        errorInfo.name = error.name;
        errorInfo.stack = error.stack;
      }

      // Check for axios error properties
      if (error.response) {
        errorInfo.response = {
          status: error.response.status,
          statusText: error.response.statusText,
          data: error.response.data,
          headers: error.response.headers,
        };
      }

      // Check for axios request properties
      if (error.request) {
        errorInfo.request = {
          method: error.config?.method,
          url: error.config?.url,
          timeout: error.config?.timeout,
        };
      }

      // Check for custom properties
      errorInfo.code = error.code;
      errorInfo.isTimeout = error.isTimeout;
      errorInfo.isNetworkError = error.isNetworkError;
      errorInfo.config = error.config;

      // If error is a string, include it
      if (typeof error === 'string') {
        errorInfo.stringValue = error;
      }

      // Try JSON.stringify to see the full object
      try {
        errorInfo.jsonString = JSON.stringify(error, Object.getOwnPropertyNames(error));
      } catch (e) {
        errorInfo.jsonError = String(e);
      }

      console.error('[API Client] getTableData error:', errorInfo);
      console.error('[API Client] Raw error object:', error);
    }

    // Extract error message - handle different error types
    // Also sanitize error messages to handle escaped % characters from backend
    let errorMessage = '';
    if (typeof error === 'string') {
      errorMessage = error;
    } else if (error?.message) {
      errorMessage = error.message;
    } else if (error?.response?.data?.detail) {
      errorMessage = error.response.data.detail;
    } else if (error?.response?.data?.message) {
      errorMessage = error.response.data.message;
    } else if (error?.response?.statusText) {
      errorMessage = error.response.statusText;
    } else {
      errorMessage = 'Unknown error occurred';
    }

    // Sanitize error message: convert escaped %% back to % for display
    // Backend escapes % to %% to prevent Python string formatting issues
    // Frontend should display the original % character
    if (typeof errorMessage === 'string') {
      errorMessage = errorMessage.replace(/%%/g, '%');
    }

    // Check for HTTP status codes FIRST (before network/timeout errors)
    // This ensures we properly handle validation errors, server errors, etc.

    // Check for validation errors (422) - these are NOT network errors
    if (error.response?.status === 422) {
      let validationError = error.response?.data?.detail || errorMessage || 'Validation error';
      // Sanitize error message: convert escaped %% back to % for display
      if (typeof validationError === 'string') {
        validationError = validationError.replace(/%%/g, '%');
      }
      // Check if it's a list of validation errors (Pydantic format)
      if (Array.isArray(validationError)) {
        const errorDetails = validationError.map((err: any) => {
          if (typeof err === 'object' && err.loc && err.msg) {
            return `${err.loc.join('.')}: ${err.msg}`;
          }
          return String(err);
        }).join('\n');
        throw new Error(`Validation Error: Invalid request parameters.\n\n${errorDetails}\n\nPlease check:\n- Connection ID is valid\n- Table name is correct\n- Schema name is correct (if provided)`);
      }
      throw new Error(`Validation Error: ${validationError}\n\nPlease check:\n- Connection ID is valid\n- Table name is correct\n- Schema name is correct (if provided)`);
    }

    // Check for other client errors (400-499) - but not 422 (already handled above)
    if (error.response?.status >= 400 && error.response?.status < 500 && error.response?.status !== 422) {
      const errorDetail = error.response?.data?.detail || errorMessage || 'Invalid request';
      throw new Error(`Request Error (${error.response.status}): ${errorDetail}`);
    }

    // Check for server errors (500+) - handle these with detailed messages
    if (error.response?.status >= 500) {
      let errorDetail = error.response?.data?.detail || error.response?.data || errorMessage || 'Server error occurred';

      // If errorDetail is a string, use it directly; if it's an object, try to extract message
      if (typeof errorDetail === 'object') {
        errorDetail = errorDetail.message || errorDetail.error || JSON.stringify(errorDetail);
      }

      // Sanitize error message: convert escaped %% back to % for display
      // Backend escapes % to %% to prevent Python string formatting issues
      // Frontend should display the original % character
      if (typeof errorDetail === 'string') {
        errorDetail = errorDetail.replace(/%%/g, '%');
      }

      // Check if this is an Oracle-related error (but exclude other database types)
      // Only treat as Oracle error if it's clearly an Oracle error, not S3, Snowflake, etc.
      const errorDetailLower = typeof errorDetail === 'string' ? errorDetail.toLowerCase() : '';
      const isOracleError = (
        typeof errorDetail === 'string' &&
        !errorDetailLower.includes('aws_s3') &&
        !errorDetailLower.includes('s3') &&
        !errorDetailLower.includes('snowflake') &&
        !errorDetailLower.includes('object storage') &&
        !errorDetailLower.includes('table comparison is not supported') &&
        (
          errorDetailLower.includes('ora-') ||  // Oracle error codes like ORA-00942
          (errorDetailLower.includes('oracle') && (
            errorDetailLower.includes('listener') ||
            errorDetailLower.includes('service name') ||
            errorDetailLower.includes('tns') ||
            errorDetailLower.includes('does not exist') ||
            errorDetailLower.includes('connection')
          ))
        )
      );

      if (isOracleError) {
        throw new Error(
          `Oracle Database Error:\n\n${errorDetail}\n\n` +
          `Troubleshooting Steps:\n` +
          `1. Verify Oracle server is running and accessible\n` +
          `2. Check Oracle listener: Run 'lsnrctl status' on Oracle server\n` +
          `3. Test network connectivity: ping the Oracle host and telnet to the Oracle port\n` +
          `4. Verify service name: Should be XE, ORCL, PDB1, etc. (NOT your username)\n` +
          `5. Check firewall: Ensure Oracle port (usually 1521) is not blocked\n` +
          `6. Verify credentials: Username and password are correct\n\n` +
          `Please check the backend terminal logs for more details.`
        );
      }

      // Check if this is an S3/object storage error
      const isS3Error = (
        typeof errorDetail === 'string' &&
        (
          errorDetailLower.includes('aws_s3') ||
          errorDetailLower.includes('s3') && errorDetailLower.includes('object storage') ||
          errorDetailLower.includes('table comparison is not supported') && errorDetailLower.includes('s3')
        )
      );

      // For S3 errors, use the backend message as-is (it's already clear and informative)
      // Don't add generic "Server Error" prefix
      if (isS3Error) {
        throw new Error(errorDetail);
      }

      // For other errors, include status code for debugging
      throw new Error(`Server Error (${error.response.status}): ${errorDetail}\n\nPlease check the backend terminal logs for more details.`);
    }

    // Check for gateway timeout (504)
    if (error.response?.status === 504) {
      throw new Error(`Request timeout: The backend took too long to fetch data from the database. The table "${tableName}" may be very large or the database connection is slow.`);
    }

    // Check for timeout (before network errors)
    if (error.isTimeout || error.code === 'ECONNABORTED' || errorMessage?.includes('timeout') || errorMessage?.includes('Timeout')) {
      // Check if this is likely an Oracle connection issue
      const isOracleIssue = errorMessage?.toLowerCase().includes('oracle') ||
        errorMessage?.toLowerCase().includes('ora-') ||
        tableName?.toLowerCase().includes('oracle');

      if (isOracleIssue) {
        throw new Error(
          `Oracle Connection Timeout: The request to fetch table data from Oracle timed out.\n\n` +
          `This usually means:\n` +
          `1. Oracle database server is not reachable\n` +
          `2. Oracle listener is not running\n` +
          `3. Network/firewall is blocking the connection\n` +
          `4. Service name is incorrect\n\n` +
          `Please check:\n` +
          `- Oracle server is running and accessible\n` +
          `- Oracle listener is active (run 'lsnrctl status' on Oracle server)\n` +
          `- Network connectivity to Oracle server\n` +
          `- Service name is correct (should be XE, ORCL, PDB1, etc., NOT your username)\n` +
          `- Firewall allows connections on Oracle port (usually 1521)\n\n` +
          `Try reducing the number of records or check the Oracle connection configuration.`
        );
      }
      throw new Error(`Timeout while fetching table data. The table "${tableName}" may be very large or the database connection is slow. Try reducing the limit.`);
    }

    // Check for empty response (backend may have crashed or returned invalid response)
    if (error.response && (!error.response.data || (typeof error.response.data === 'object' && Object.keys(error.response.data).length === 0))) {
      throw new Error('Server returned an empty response. The backend may have encountered an error. Please check the backend logs and try again.');
    }

    // Check for network errors - ONLY if there's NO response (error.response is null/undefined)
    // This means the request never reached the server or the server didn't respond
    // This can happen if: backend crashes, CORS blocks, connection refused, or request times out
    // IMPORTANT: Only treat as network error if there's NO response (error.response is null/undefined)
    const isNetworkError = !error.response && (
      error.isNetworkError ||
      error.code === 'ECONNREFUSED' ||
      error.code === 'ERR_NETWORK' ||
      error.code === 'ERR_INTERNET_DISCONNECTED' ||
      error.code === 'ERR_CONNECTION_REFUSED' ||
      error.code === 'ETIMEDOUT' ||
      errorMessage === 'Network Error' ||
      errorMessage?.includes('Network Error') ||
      errorMessage?.includes('Cannot connect to server') ||
      errorMessage?.includes('Failed to load response data') ||
      (error.name === 'AxiosError' && errorMessage === 'Network Error') ||
      (error.request && !error.response) // Request was made but no response received
    );

    if (isNetworkError) {
      // Check if backend is still alive
      let backendAlive = false;
      try {
        const healthController = new AbortController();
        const healthTimeout = setTimeout(() => healthController.abort(), 3000);
        try {
          const healthCheck = await fetch('http://localhost:8000/api/health', {
            method: 'GET',
            signal: healthController.signal
          });
          backendAlive = healthCheck.ok;
        } finally {
          clearTimeout(healthTimeout);
        }
      } catch (healthErr) {
        backendAlive = false;
      }

      // Provide a helpful error message with troubleshooting steps
      let troubleshootingMsg = `Network Error: The request to fetch table data failed.\n\n`;

      if (!backendAlive) {
        troubleshootingMsg += `⚠️ Backend is not responding. The server may have crashed.\n\n`;
      } else {
        troubleshootingMsg += `⚠️ Backend is running but the request timed out or failed.\n\n`;
      }

      troubleshootingMsg += `Possible causes:\n` +
        `1. Database connection is hanging (Oracle/SQL Server may be unreachable)\n` +
        `2. Backend server crashed or is not responding\n` +
        `3. Request timeout (table may be too large or database is slow)\n` +
        `4. Network connectivity issue\n\n` +
        `Please check:\n` +
        `- Backend is running on http://localhost:8000\n` +
        `- Check backend terminal for error logs\n` +
        `- Database servers are accessible and responding\n` +
        `- Try the "Retry Target" button\n` +
        `- Reduce the number of records if table is very large`;

      throw new Error(troubleshootingMsg);
    }

    // If we get here and have a response, it's an unhandled status code
    if (error.response) {
      const status = error.response.status;
      const errorDetail = error.response?.data?.detail || errorMessage || `HTTP ${status} error`;
      throw new Error(`Request failed with status ${status}: ${errorDetail}`);
    }

    // Re-throw with a meaningful message
    throw new Error(errorMessage || 'An unexpected error occurred while fetching table data');
  }

  // Pipeline endpoints
  async getPipelines(skip?: number, limit?: number, retries?: number) {
    const skipValue = skip ?? 0;
    const limitValue = limit ?? 10000;  // Increased default limit to fetch all pipelines
    const retriesValue = retries ?? 1;
    const requestKey = `pipelines-${skipValue}-${limitValue}`;
    return this.retryRequest(
      () => this.client.get('/api/v1/pipelines/', {
        params: { skip: skipValue, limit: limitValue },
        timeout: 10000, // Reduced to 10 seconds
      }).then(res => res.data),
      'pipelines',
      retriesValue,
      10000,
      requestKey
    );
  }

  async getPipeline(pipelineId: string | number) {
    try {
      const response = await this.client.get(`/api/v1/pipelines/${pipelineId}`, {
        timeout: 15000 // 15 seconds timeout (increased from 5s to handle slow backend responses)
      });
      return response.data;
    } catch (error: any) {
      // Handle all errors gracefully - return basic pipeline info to prevent UI breakage
      const isTimeout = error.isTimeout ||
        error.code === 'ECONNABORTED' ||
        error.message?.includes('timeout') ||
        error.message?.includes('took too long');
      const isServerError = error.response?.status >= 500;
      const isNetworkError = error.code === 'ECONNREFUSED' || error.message?.includes('Network Error');

      if (isTimeout || isServerError || isNetworkError) {
        // Return minimal pipeline info to prevent UI breakage
        console.warn(`Pipeline ${pipelineId} endpoint timeout or error, returning minimal info`);
        return {
          id: String(pipelineId),
          name: `Pipeline ${pipelineId}`,
          status: 'UNKNOWN',
          full_load_status: 'NOT_STARTED',
          cdc_status: 'NOT_STARTED',
          error: 'Endpoint timeout or unavailable'
        };
      }
      // For other errors (like 404), also return minimal info
      return {
        id: String(pipelineId),
        name: `Pipeline ${pipelineId}`,
        status: 'NOT_FOUND',
        full_load_status: 'NOT_STARTED',
        cdc_status: 'NOT_STARTED'
      };
    }
  }

  async fixOrphanedConnections() {
    const response = await this.client.post('/api/v1/pipelines/fix-orphaned-connections');
    return response.data;
  }

  async createPipeline(pipelineData: any) {
    try {
      console.log('[API Client] createPipeline called with:', {
        ...pipelineData,
        table_mappings: pipelineData.table_mappings?.length || 0
      });
      const response = await this.client.post('/api/v1/pipelines/', pipelineData);
      console.log('[API Client] createPipeline success:', response.data);
      return response.data;
    } catch (error: any) {
      // Log comprehensive error information
      const errorInfo: any = {
        message: error.message || 'Unknown error',
        name: error.name,
        code: error.code,
        isTimeout: error.isTimeout,
        isNetworkError: error.isNetworkError,
      };

      // Add response data if available
      if (error.response) {
        errorInfo.status = error.response.status;
        errorInfo.statusText = error.response.statusText;
        errorInfo.data = error.response.data;
        errorInfo.headers = error.response.headers;
      }

      // Add request config if available
      if (error.config) {
        errorInfo.url = error.config.url;
        errorInfo.method = error.config.method;
        errorInfo.data = error.config.data;
      }

      // Log the full error object
      console.error('[API Client] createPipeline error:', errorInfo);
      console.error('[API Client] Full error object:', error);

      // Re-throw with better error message
      if (error.response?.data?.detail) {
        const detailedError = new Error(error.response.data.detail);
        (detailedError as any).response = error.response;
        (detailedError as any).status = error.response.status;
        throw detailedError;
      }

      throw error;
    }
  }

  async updatePipeline(pipelineId: string | number, pipelineData: any) {
    const response = await this.client.put(`/api/v1/pipelines/${String(pipelineId)}`, pipelineData);
    return response.data;
  }

  async deletePipeline(pipelineId: number) {
    await this.client.delete(`/api/v1/pipelines/${pipelineId}`);
  }

  async exportPipelineDag(pipelineId: string | number): Promise<Blob> {
    const response = await this.client.get(`/api/v1/pipelines/${String(pipelineId)}/export-dag`, {
      responseType: 'blob',
    });
    return response.data;
  }

  // Checkpoint management
  async getPipelineCheckpoints(pipelineId: string | number) {
    // LSN metrics router is under /monitoring prefix, but endpoints start with /pipelines
    // Use shorter timeout and handle errors gracefully
    try {
      const response = await this.client.get(`/api/v1/monitoring/pipelines/${String(pipelineId)}/checkpoints`, {
        timeout: 5000 // 5 seconds timeout - backend should respond quickly
      });
      return response.data;
    } catch (error: any) {
      // Handle all errors gracefully - return empty checkpoints to prevent UI breakage
      const isTimeout = error.isTimeout ||
        error.code === 'ECONNABORTED' ||
        error.message?.includes('timeout') ||
        error.message?.includes('took too long');
      const isServerError = error.response?.status >= 500;
      const isNetworkError = error.code === 'ECONNREFUSED' || error.message?.includes('Network Error');

      // Always return empty checkpoints for any error - prevents UI breakage
      // This includes timeouts, network errors, server errors, and 404s
      return {
        checkpoints: [],
        pipeline_id: String(pipelineId),
        count: 0
      };
    }
  }

  async getPipelineCheckpoint(pipelineId: string | number, tableName: string, schemaName?: string) {
    const params = schemaName ? { schema_name: schemaName } : {};
    const response = await this.client.get(`/api/v1/pipelines/${String(pipelineId)}/checkpoints/${encodeURIComponent(tableName)}`, { params });
    return response.data;
  }

  async updatePipelineCheckpoint(
    pipelineId: string | number,
    tableName: string,
    checkpointData: {
      lsn?: string;
      scn?: number;
      binlog_file?: string;
      binlog_position?: number;
      sql_server_lsn?: string;
      resume_token?: string;
      checkpoint_value?: string;
      checkpoint_type?: string;
    },
    schemaName?: string
  ) {
    const params = schemaName ? { schema_name: schemaName } : {};
    const response = await this.client.put(
      `/api/v1/pipelines/${String(pipelineId)}/checkpoints/${encodeURIComponent(tableName)}`,
      checkpointData,
      { params }
    );
    return response.data;
  }

  async resetPipelineCheckpoint(pipelineId: string | number, tableName: string, schemaName?: string) {
    const params = schemaName ? { schema_name: schemaName } : {};
    const response = await this.client.delete(
      `/api/v1/pipelines/${String(pipelineId)}/checkpoints/${encodeURIComponent(tableName)}`,
      { params }
    );
    return response.data;
  }

  async triggerPipeline(pipelineId: string | number, runType = 'full_load') {
    // Pipeline start can take time (schema creation, connector setup, etc.)
    // Increase timeout to 120 seconds to allow for full initialization and connector setup
    const response = await this.client.post(`/api/v1/pipelines/${String(pipelineId)}/trigger`, {
      run_type: runType,
    }, {
      timeout: 180000 // 180 seconds (3 minutes) timeout for pipeline start (increased from 120s)
    });
    return response.data;
  }

  async syncPipelineStats(pipelineId: string | number) {
    const response = await this.client.post(`/api/v1/pipelines/${String(pipelineId)}/sync-stats`);
    return response.data;
  }

  async getPipelineProgress(pipelineId: string | number) {
    // Progress endpoint is optimized to return immediately from memory (no DB queries)
    // Using shorter timeout since endpoint should respond instantly
    try {
      const response = await this.client.get(`/api/v1/pipelines/${String(pipelineId)}/progress`, {
        timeout: 10000 // 10 seconds timeout (increased from 3s to handle slow backend responses)
      });
      return response.data;
    } catch (error: any) {
      // Handle timeout/errors gracefully - return empty progress
      const isTimeout = error.isTimeout || error.code === 'ECONNABORTED' || error.message?.includes('timeout');
      if (isTimeout || error.response?.status >= 500) {
        console.warn(`Pipeline progress endpoint timeout for ${pipelineId}, returning empty progress`);
        return {
          pipeline_id: String(pipelineId),
          progress: 0,
          status: 'UNKNOWN',
          message: 'Progress unavailable'
        };
      }
      // For other errors, return empty progress
      return {
        pipeline_id: String(pipelineId),
        progress: 0,
        status: 'UNKNOWN'
      };
    }
  }

  async pausePipeline(pipelineId: string | number) {
    const response = await this.client.post(`/api/v1/pipelines/${String(pipelineId)}/pause`);
    return response.data;
  }

  async stopPipeline(pipelineId: string | number) {
    // Increase timeout for stop pipeline as it may take time to stop connectors
    const response = await this.client.post(`/api/v1/pipelines/${String(pipelineId)}/stop`, {}, {
      timeout: 35000, // 35 seconds timeout for stopping pipeline
    });
    return response.data;
  }

  async getPipelineRuns(pipelineId: string | number, skip = 0, limit = 100) {
    const response = await this.client.get(`/api/v1/pipelines/${pipelineId}/runs`, {
      params: { skip, limit },
    });
    return response.data;
  }

  async getPipelineStatus(pipelineId: string | number) {
    const response = await this.client.get(`/api/v1/pipelines/${pipelineId}/status`);
    return response.data;
  }

  async getPipelineOffset(pipelineId: string | number, fetchFromDb = false) {
    const response = await this.client.get(`/api/v1/pipelines/${pipelineId}/offset`, {
      params: { fetch_from_db: fetchFromDb }
    });
    return response.data;
  }

  async updatePipelineOffset(pipelineId: string | number, offsetData: {
    current_lsn?: string;
    current_offset?: string;
    current_scn?: string;
  }) {
    const response = await this.client.put(`/api/v1/pipelines/${pipelineId}/offset`, offsetData);
    return response.data;
  }

  // Monitoring endpoints
  async getReplicationEvents(
    pipelineId?: number | string,
    skip = 0,
    limit = 100,
    todayOnly = false,
    startDate?: string | Date,
    endDate?: string | Date,
    tableName?: string,
    retries = 1
  ) {
    const requestKey = `events-${pipelineId || 'all'}-${skip}-${limit}-${todayOnly}-${startDate || ''}-${endDate || ''}-${tableName || ''}`;
    // Convert to string for UUIDs, or use as-is for numbers
    const pipelineIdParam = pipelineId ? String(pipelineId) : undefined;

    // Convert Date objects to ISO strings if needed
    const startDateParam = startDate instanceof Date ? startDate.toISOString() : startDate;
    const endDateParam = endDate instanceof Date ? endDate.toISOString() : endDate;

    return this.retryRequest(
      () => this.client.get('/api/v1/monitoring/events', {
        params: {
          pipeline_id: pipelineIdParam,
          table_name: tableName, // Added table name filter
          skip,
          limit,
          today_only: todayOnly,
          start_date: startDateParam,
          end_date: endDateParam,
        },
        timeout: 10000, // Reduced to 10 seconds
      }).then(res => {
        // CRITICAL: Log API response for debugging
        const eventsData = res.data;
        const eventsArray = Array.isArray(eventsData) ? eventsData : [];
        console.log('[API] getReplicationEvents response:', {
          pipelineId: pipelineIdParam || 'all',
          receivedCount: eventsArray.length,
          limit,
          isArray: Array.isArray(eventsData),
          sampleEvent: eventsArray.length > 0 ? {
            id: eventsArray[0].id,
            pipeline_id: eventsArray[0].pipeline_id,
            event_type: eventsArray[0].event_type
          } : null
        });
        return eventsData;
      }),
      'replication events',
      retries,
      10000,
      requestKey
    );
  }

  async getDashboardStats() {
    try {
      const response = await this.client.get('/api/v1/monitoring/dashboard', {
        timeout: 5000 // 5 second timeout
      });
      return response.data;
    } catch (error: any) {
      // Handle all errors gracefully - return empty dashboard data
      const isTimeout = error.isTimeout || error.code === 'ECONNABORTED' || error.message?.includes('timeout')
      const isNetworkError = error.code === 'ECONNREFUSED' || error.message?.includes('Network Error') || error.code === 'ERR_NETWORK'
      const isServerError = error.response?.status >= 500

      if (isTimeout || isNetworkError || isServerError) {
        // Return empty dashboard data on errors
        return {
          total_pipelines: 0,
          active_pipelines: 0,
          stopped_pipelines: 0,
          error_pipelines: 0,
          total_events: 0,
          failed_events: 0,
          success_events: 0,
          recent_metrics: [],
          timestamp: new Date().toISOString()
        }
      }
      // For other errors, also return empty dashboard
      return {
        total_pipelines: 0,
        active_pipelines: 0,
        stopped_pipelines: 0,
        error_pipelines: 0,
        total_events: 0,
        failed_events: 0,
        success_events: 0,
        recent_metrics: [],
        timestamp: new Date().toISOString()
      }
    }
  }

  async getMonitoringMetrics(
    pipelineId: number | string,
    startTime?: string | Date,
    endTime?: string | Date,
    retries = 1
  ) {
    // Convert to string to handle both numeric IDs and UUID strings
    const pipelineIdStr = String(pipelineId)
    // Don't send if it's NaN
    if (pipelineIdStr === 'NaN' || pipelineIdStr === 'undefined' || !pipelineIdStr) {
      throw new Error('Invalid pipeline ID')
    }

    // Convert Date objects to ISO strings if necessary
    const formattedStartTime = startTime instanceof Date ? startTime.toISOString() : startTime
    const formattedEndTime = endTime instanceof Date ? endTime.toISOString() : endTime

    const requestKey = `metrics-${pipelineIdStr}-${formattedStartTime || ''}-${formattedEndTime || ''}`;
    return this.retryRequest(
      () => this.client.get('/api/v1/monitoring/metrics', {
        params: {
          pipelineId: pipelineIdStr,  // Use pipelineId (camelCase) to match backend
          startTime: formattedStartTime,
          endTime: formattedEndTime
        },
        timeout: 10000, // Reduced to 10 seconds
      }).then(res => res.data),
      'monitoring metrics',
      retries,
      10000,
      requestKey
    );
  }

  async retryFailedEvent(eventId: string) {
    const response = await this.client.post(`/api/v1/monitoring/events/${eventId}/retry`);
    return response.data;
  }

  async createReplicationEvent(eventData: any) {
    const response = await this.client.post('/api/v1/monitoring/events', eventData);
    return response.data;
  }

  // LSN Latency methods
  async getLsnLatency(pipelineId: string | number, tableName?: string, schemaName?: string) {
    const params: any = {};
    if (tableName) params.table_name = tableName;
    if (schemaName) params.schema_name = schemaName;
    // LSN metrics router is under /monitoring prefix, but endpoints start with /pipelines
    const response = await this.client.get(`/api/v1/monitoring/pipelines/${String(pipelineId)}/lsn-latency`, { params });
    return response.data;
  }

  async getLsnLatencyTrend(pipelineId: string | number, tableName?: string, hours: number = 24) {
    const params: any = { hours };
    if (tableName) params.table_name = tableName;
    const response = await this.client.get(`/api/v1/monitoring/pipelines/${String(pipelineId)}/lsn-latency-trend`, { params });
    return response.data;
  }

  async getApplicationLogs(
    skip: number = 0,
    limit: number = 100,
    level?: string,
    search?: string,
    startDate?: string,
    endDate?: string
  ) {
    try {
      const params: any = { skip, limit }
      if (level) params.level = level
      if (search) params.search = search
      if (startDate) params.start_date = startDate
      if (endDate) params.end_date = endDate

      // Reduced timeout and limit for faster response
      const response = await this.client.get('/api/v1/logs/application-logs', {
        params,
        timeout: 5000 // 5 second timeout
      })
      return response.data
    } catch (error: any) {
      // Handle all errors gracefully - return empty logs to prevent UI breakage
      const isTimeout = error.isTimeout || error.code === 'ECONNABORTED' || error.message?.includes('timeout')
      const isNetworkError = error.code === 'ECONNREFUSED' || error.message?.includes('Network Error') || error.code === 'ERR_NETWORK'
      const isServerError = error.response?.status >= 500

      if (isTimeout || isNetworkError || isServerError) {
        // Silently return empty logs - don't log as error to avoid noise
        return {
          logs: [],
          total: 0,
          skip,
          limit
        }
      }
      // For other errors (like 404), also return empty logs
      return {
        logs: [],
        total: 0,
        skip,
        limit
      }
    }
  }

  async getLogLevels() {
    try {
      const response = await this.client.get('/api/v1/logs/application-logs/levels', {
        timeout: 5000 // 5 second timeout for log levels
      })
      return response.data
    } catch (error: any) {
      // On timeout or any error, return default levels
      if (error.isTimeout || error.code === 'ECONNABORTED') {
        console.warn('Log levels endpoint timed out, using default levels')
      } else {
        console.error('Error fetching log levels:', error)
      }
      return ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    }
  }
  async getLsnLatencyMetrics(pipelineId: string, startTime?: string, endTime?: string, limit: number = 100) {
    const params: any = { limit };
    if (startTime) params.start_time = startTime;
    if (endTime) params.end_time = endTime;
    const response = await this.client.get(`/api/v1/monitoring/lsn-latency`, {
      params: { pipeline_id: pipelineId, ...params }
    });
    return response.data;
  }

  async getAuditLogs(
    skip: number = 0,
    limit: number = 20,
    action?: string,
    resourceType?: string,
    startDate?: string,
    endDate?: string
  ) {
    const params: any = { skip, limit }
    if (action) params.action = action
    if (resourceType) params.resource_type = resourceType
    if (startDate) params.start_date = startDate
    if (endDate) params.end_date = endDate

    const response = await this.client.get('/api/v1/audit-logs', { params })
    return response.data
  }

  async getAuditLogFilters() {
    const response = await this.client.get('/api/v1/audit-logs/filters')
    return response.data
  }

  async getEventLoggerStatus() {
    const response = await this.client.get('/api/v1/monitoring/event-logger-status')
    return response.data
  }

  async getSystemHealth() {
    const response = await this.client.get('/api/monitoring/health')
    return response.data
  }
}

export const apiClient = new ApiClient();

