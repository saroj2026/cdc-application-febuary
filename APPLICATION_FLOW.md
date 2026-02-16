# Application Flow: Frontend and Backend Architecture

## Overview

This document describes the complete application flow between the frontend (Next.js/React) and backend (FastAPI/Python) for the CDC (Change Data Capture) pipeline management system.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND (Next.js)                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │   React UI   │  │  Redux Store │  │  API Client   │        │
│  │  Components  │◄─┤  (State)     │◄─┤  (Axios)      │        │
│  └──────────────┘  └──────────────┘  └──────┬─────────┘        │
│                                               │                  │
│  ┌───────────────────────────────────────────┴─────────┐       │
│  │         WebSocket Client (Socket.IO)                 │       │
│  └───────────────────────────────────────────────────────┘       │
└───────────────────────────────┬───────────────────────────────────┘
                                │ HTTP REST API
                                │ WebSocket (Socket.IO)
                                │
┌───────────────────────────────▼───────────────────────────────────┐
│                      BACKEND (FastAPI)                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  FastAPI    │  │  CDC Manager │  │  Event Logger │          │
│  │  Endpoints  │◄─┤  (Pipeline)  │◄─┤  (Kafka)      │          │
│  └──────────────┘  └──────────────┘  └──────┬───────┘          │
│                                               │                   │
│  ┌───────────────────────────────────────────┴─────────┐        │
│  │         Socket.IO Server (Real-time)                │        │
│  └───────────────────────────────────────────────────────┘        │
└───────────────────────────────┬───────────────────────────────────┘
                                │
                ┌───────────────┼───────────────┐
                │               │               │
        ┌───────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
        │  PostgreSQL  │ │   Kafka     │ │ Kafka       │
        │   Database   │ │  Connect    │ │  Topics     │
        └──────────────┘ └─────────────┘ └─────────────┘
```

## Core Components

### Frontend Components

1. **React UI Components**
   - Pipeline management pages
   - Monitoring dashboard
   - Connection management
   - Analytics and reporting

2. **Redux Store**
   - `pipelineSlice`: Pipeline state management
   - `monitoringSlice`: Event and metrics state
   - `connectionSlice`: Connection state
   - `authSlice`: Authentication state

3. **API Client** (`lib/api/client.ts`)
   - Axios-based HTTP client
   - Request/response interceptors
   - Error handling and retry logic
   - TypeScript type definitions

4. **WebSocket Client** (`lib/websocket/client.ts`)
   - Socket.IO client for real-time updates
   - Pipeline event subscriptions
   - Connection management

### Backend Components

1. **FastAPI Application** (`backend/ingestion/api.py`)
   - REST API endpoints
   - Request/response models
   - Authentication middleware
   - CORS configuration

2. **CDC Manager** (`backend/ingestion/cdc_manager.py`)
   - Pipeline lifecycle management
   - Full load orchestration
   - CDC connector management
   - Status tracking

3. **CDC Event Logger** (`backend/ingestion/cdc_event_logger.py`)
   - Kafka consumer for CDC events
   - Database event persistence
   - Real-time event emission

4. **Database Models** (`backend/ingestion/database/models_db.py`)
   - SQLAlchemy ORM models
   - Pipeline, Connection, Event models

5. **Socket.IO Server**
   - Real-time event broadcasting
   - Client connection management

## Data Flow

### 1. Pipeline Creation Flow

```
Frontend                    Backend                    Database
   │                           │                           │
   │── POST /api/v1/pipelines │                           │
   │   (pipeline config)       │                           │
   │──────────────────────────>│                           │
   │                           │── INSERT INTO pipelines  │
   │                           │──────────────────────────>│
   │                           │<──────────────────────────│
   │<── 201 Created            │                           │
   │   (pipeline_id)           │                           │
   │                           │                           │
```

**Frontend Code:**
```typescript
// frontend/lib/api/client.ts
async createPipeline(pipelineData: PipelineCreate) {
  const response = await this.client.post('/api/v1/pipelines', pipelineData);
  return response.data;
}
```

**Backend Code:**
```python
# backend/ingestion/api.py
@app.post("/api/v1/pipelines")
async def create_pipeline(pipeline_data: PipelineCreate, db: Session = Depends(get_db)):
    # Create pipeline in database
    pipeline_model = PipelineModel(...)
    db.add(pipeline_model)
    db.commit()
    return pipeline_model
```

### 2. Pipeline Start Flow

```
Frontend                    Backend                    Kafka Connect
   │                           │                           │
   │── POST /start             │                           │
   │   (pipeline_id)           │                           │
   │──────────────────────────>│                           │
   │                           │── 1. Auto-create schema  │
   │                           │── 2. Run full load        │
   │                           │── 3. Create Debezium     │
   │                           │──────────────────────────>│
   │                           │<──────────────────────────│
   │                           │── 4. Create Sink         │
   │                           │──────────────────────────>│
   │                           │<──────────────────────────│
   │<── 200 OK                 │                           │
   │   (status, connectors)    │                           │
   │                           │                           │
```

**Pipeline Modes:**

1. **FULL_LOAD_ONLY**
   - Step 0: Auto-create target schema/tables
   - Step 1: Run full data load
   - Step 2: Skip CDC setup (return immediately)

2. **CDC_ONLY**
   - Step 0.5: Ensure target schema exists (if needed)
   - Step 1: Skip full load
   - Step 2: Create Debezium connector (snapshot_mode='never')
   - Step 3: Create Sink connector

3. **FULL_LOAD_AND_CDC**
   - Step 0: Auto-create target schema/tables
   - Step 1: Run full load (if not completed)
   - Step 2: Create Debezium connector (snapshot_mode based on full load status)
   - Step 3: Create Sink connector

### 3. CDC Event Flow

```
Source DB    Debezium    Kafka Topic    Event Logger    Database    Frontend
   │            │            │               │              │            │
   │── INSERT   │            │               │              │            │
   │───────────>│            │               │              │            │
   │            │── Message  │               │              │            │
   │            │───────────>│               │              │            │
   │            │            │── Consume     │              │            │
   │            │            │──────────────>│              │            │
   │            │            │               │── Parse      │            │
   │            │            │               │── Save       │            │
   │            │            │               │─────────────>│            │
   │            │            │               │              │            │
   │            │            │               │── Emit       │            │
   │            │            │               │──────────────┼──────────>│
   │            │            │               │              │            │
```

**Event Logger Process:**
1. Consumes messages from Kafka topics
2. Parses Debezium message format
3. Extracts event type (insert/update/delete)
4. Saves to `pipeline_runs` table with `run_type='CDC'`
5. Emits via Socket.IO for real-time updates

### 4. Monitoring Dashboard Flow

```
Frontend                    Backend                    Database
   │                           │                           │
   │── GET /monitoring/dashboard│                          │
   │──────────────────────────>│                           │
   │                           │── SQL Aggregation:       │
   │                           │    SELECT COUNT(*)       │
   │                           │    WHERE run_type='CDC'   │
   │                           │    AND deleted_at IS NULL│
   │                           │──────────────────────────>│
   │                           │                           │
   │                           │── GROUP BY event_type:   │
   │                           │    SELECT                 │
   │                           │      CASE WHEN ...        │
   │                           │      COUNT(*)             │
   │                           │    GROUP BY event_type    │
   │                           │──────────────────────────>│
   │                           │<──────────────────────────│
   │<── 200 OK                 │                           │
   │   {                       │                           │
   │     total_events: 1234,   │                           │
   │     insert_count: 800,    │                           │
   │     update_count: 400,    │                           │
   │     delete_count: 34      │                           │
   │   }                       │                           │
   │                           │                           │
   │── Display counts directly │                           │
   │   (no frontend filtering)  │                           │
   │                           │                           │
```

**CRITICAL: Backend Aggregation**
- Event counts are calculated in the database using SQL aggregation
- Frontend receives pre-calculated counts (no filtering needed)
- Debezium op codes (c/u/d/r) are normalized to insert/update/delete in backend
- Soft-deleted events are excluded (`deleted_at IS NULL`)

**Frontend Processing:**
```typescript
// frontend/app/monitoring/page.tsx
// CRITICAL: Use backend-aggregated counts when available
// Fallback to frontend calculation only if backend doesn't provide counts
const eventStats = useMemo(() => {
  const eventsArray = Array.isArray(events) ? events : [];
  
  // If dashboard provides aggregated counts, use them (preferred)
  if (dashboardData?.insert_count !== undefined) {
    return {
      total: dashboardData.total_events || eventsArray.length,
      insert: dashboardData.insert_count || 0,
      update: dashboardData.update_count || 0,
      delete: dashboardData.delete_count || 0,
      // ...
    };
  }
  
  // Fallback: Calculate from events array (normalize Debezium codes)
  return {
    total: eventsArray.length,
    insert: eventsArray.filter(e => {
      const et = String(e.event_type || '').toLowerCase().trim();
      return et === 'insert' || et === 'i' || et === 'c' || et === 'r';
    }).length,
    update: eventsArray.filter(e => {
      const et = String(e.event_type || '').toLowerCase().trim();
      return et === 'update' || et === 'u';
    }).length,
    delete: eventsArray.filter(e => {
      const et = String(e.event_type || '').toLowerCase().trim();
      return et === 'delete' || et === 'd';
    }).length,
    // ...
  };
}, [events, dashboardData]);
```

## API Endpoints

### Pipeline Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/pipelines` | Create new pipeline |
| `GET` | `/api/v1/pipelines` | List all pipelines |
| `GET` | `/api/v1/pipelines/{id}` | Get pipeline details |
| `PUT` | `/api/v1/pipelines/{id}` | Update pipeline |
| `DELETE` | `/api/v1/pipelines/{id}` | Delete pipeline |
| `POST` | `/api/v1/pipelines/{id}/start` | Start pipeline |
| `POST` | `/api/v1/pipelines/{id}/stop` | Stop pipeline |
| `POST` | `/api/v1/pipelines/{id}/pause` | Pause pipeline |
| `GET` | `/api/v1/pipelines/{id}/status` | Get pipeline status |
| `GET` | `/api/v1/pipelines/{id}/progress` | Get pipeline progress |

### Monitoring

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/monitoring/events` | Get replication events |
| `GET` | `/api/v1/monitoring/dashboard` | Get dashboard statistics |
| `GET` | `/api/v1/monitoring/metrics` | Get pipeline metrics |
| `GET` | `/api/v1/monitoring/pipelines/{id}/lsn-latency` | Get LSN latency |
| `POST` | `/api/v1/pipelines/{id}/sync-stats` | Sync pipeline statistics |
| `GET` | `/api/v1/monitoring/event-logger-status` | Get event logger status |
| `POST` | `/api/v1/monitoring/event-logger/restart` | Restart event logger |

### Connections

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/connections` | Create connection |
| `GET` | `/api/v1/connections` | List connections |
| `GET` | `/api/v1/connections/{id}` | Get connection |
| `PUT` | `/api/v1/connections/{id}` | Update connection |
| `DELETE` | `/api/v1/connections/{id}` | Delete connection |
| `POST` | `/api/v1/connections/{id}/test` | Test connection |

## State Management (Redux)

### Pipeline Slice

```typescript
// frontend/lib/store/slices/pipelineSlice.ts
interface PipelineState {
  pipelines: Pipeline[];
  selectedPipeline: Pipeline | null;
  isLoading: boolean;
  error: string | null;
}

// Actions:
- fetchPipelines()
- createPipeline()
- updatePipeline()
- deletePipeline()
- setSelectedPipeline()
```

### Monitoring Slice

```typescript
// frontend/lib/store/slices/monitoringSlice.ts
interface MonitoringState {
  events: ReplicationEvent[];
  metrics: MonitoringMetric[];
  selectedPipelineId: string | null;
  isLoading: boolean;
}

// Actions:
- fetchReplicationEvents()
- fetchMonitoringMetrics()
- addReplicationEvent()  // From WebSocket
- setSelectedPipeline()
```

## WebSocket Communication

### Connection Flow

```
Frontend                    Backend
   │                           │
   │── connect()               │
   │──────────────────────────>│
   │<── connect event          │
   │                           │
   │── subscribe_pipeline      │
   │   (pipeline_id)           │
   │──────────────────────────>│
   │                           │
   │<── replication_event      │
   │   (event data)            │
   │                           │
```

### Event Emission

```python
# backend/ingestion/cdc_event_logger.py
# After committing events to database:
if self.socketio_server:
    await self.socketio_server.emit(
        'replication_event',
        formatted_event,
        room=f"pipeline_{pipeline_id}"
    )
```

## Database Schema

### Key Tables

1. **pipelines**
   - `id`: UUID
   - `name`: String
   - `source_connection_id`: UUID
   - `target_connection_id`: UUID
   - `mode`: Enum (full_load_only, cdc_only, full_load_and_cdc)
   - `status`: Enum (STOPPED, RUNNING, ERROR, etc.)
   - `full_load_status`: Enum (NOT_STARTED, COMPLETED, etc.)
   - `cdc_status`: Enum (NOT_STARTED, RUNNING, etc.)
   - `debezium_connector_name`: String
   - `sink_connector_name`: String
   - `kafka_topics`: JSON Array

2. **pipeline_runs**
   - `id`: UUID
   - `pipeline_id`: UUID
   - `run_type`: String ('CDC' or 'FULL_LOAD')
   - `status`: String
   - `started_at`: DateTime
   - `completed_at`: DateTime
   - `run_metadata`: JSONB (contains event details)
   - `deleted_at`: DateTime (soft delete)

3. **connections**
   - `id`: UUID
   - `name`: String
   - `connection_type`: Enum (source/target)
   - `database_type`: String
   - `host`, `port`, `database`, `username`, `password`
   - `schema`: String

## Error Handling

### Frontend Error Handling

```typescript
// frontend/lib/api/client.ts
try {
  const response = await this.client.get('/api/v1/pipelines');
  return response.data;
} catch (error) {
  if (error.response?.status === 404) {
    // Handle not found
  } else if (error.response?.status >= 500) {
    // Handle server error
  }
  throw error;
}
```

### Backend Error Handling

```python
# backend/ingestion/api.py
try:
    pipeline = cdc_manager.pipeline_store.get(pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail=str(e))
```

## Authentication Flow

```
Frontend                    Backend                    Database
   │                           │                           │
   │── POST /auth/login        │                           │
   │   (username, password)    │                           │
   │──────────────────────────>│                           │
   │                           │── SELECT * FROM users     │
   │                           │    WHERE username=...     │
   │                           │──────────────────────────>│
   │                           │<──────────────────────────│
   │                           │── Verify password         │
   │                           │── Generate JWT token       │
   │<── 200 OK                 │                           │
   │   (token)                 │                           │
   │                           │                           │
   │── Store token in          │                           │
   │   localStorage            │                           │
   │── Add to request headers  │                           │
   │   (Authorization: Bearer) │                           │
   │                           │                           │
```

## Real-time Updates Flow

### WebSocket Event Flow

1. **Event Logger** consumes from Kafka
2. **Event Logger** saves to database
3. **Event Logger** emits via Socket.IO
4. **Frontend** receives event
5. **Redux** updates state
6. **UI** re-renders with new data

### Polling Fallback

If WebSocket is unavailable:
- Frontend polls `/api/v1/monitoring/events` every 5 seconds
- Manual refresh button available
- Auto-refresh interval configurable

## Pipeline Lifecycle States

```
NOT_STARTED → STARTING → RUNNING → STOPPED
                │            │
                │            ├──→ PAUSED → RUNNING
                │            │
                └──→ ERROR ──┘
```

### Status Transitions

1. **Start Pipeline**
   - `NOT_STARTED` → `STARTING` → `RUNNING`
   - If error: `STARTING` → `ERROR`

2. **Stop Pipeline**
   - `RUNNING` → `STOPPING` → `STOPPED`

3. **Pause Pipeline**
   - `RUNNING` → `PAUSED`
   - Resume: `PAUSED` → `RUNNING`

## Data Synchronization

### Full Load Process

1. **Schema Creation**
   - Read source table schema
   - Create target schema/tables
   - Add CDC metadata columns (if needed)

2. **Data Transfer**
   - Batch read from source
   - Batch insert to target
   - Track progress and LSN/offset

3. **Completion**
   - Capture final LSN/offset
   - Update `full_load_status` to `COMPLETED`
   - Store LSN for CDC start point

### CDC Process

1. **Debezium Connector**
   - Captures changes from source database
   - Publishes to Kafka topics
   - Uses replication slot (PostgreSQL) or equivalent

2. **Sink Connector**
   - Consumes from Kafka topics
   - Applies changes to target database
   - Handles insert/update/delete operations

3. **Event Logger**
   - Consumes from Kafka topics
   - Logs events to `pipeline_runs` table
   - Emits real-time updates via Socket.IO

## Monitoring Metrics

### Event Statistics

**CRITICAL: Backend Aggregation (Not Frontend Filtering)**

Event counts are calculated in the backend using SQL aggregation for performance and accuracy:

```sql
-- Total events
SELECT COUNT(*) FROM pipeline_runs 
WHERE run_type='CDC' AND deleted_at IS NULL

-- Event type counts (aggregated from run_metadata)
SELECT 
  CASE 
    WHEN run_metadata->>'event_type' IN ('insert', 'i', 'c', 'create', 'r') THEN 'insert'
    WHEN run_metadata->>'event_type' IN ('update', 'u') THEN 'update'
    WHEN run_metadata->>'event_type' IN ('delete', 'd', 'remove') THEN 'delete'
  END as event_type,
  COUNT(*) as count
FROM pipeline_runs
WHERE run_type='CDC' AND deleted_at IS NULL
GROUP BY event_type
```

**Debezium Operation Code Mapping:**
- `c` (create) → `insert`
- `r` (read/snapshot) → `insert`
- `u` (update) → `update`
- `d` (delete) → `delete`

**Metrics:**
- **Total Events**: Count of all CDC events (from database aggregation)
- **Insert Count**: Aggregated from database (normalized from c/r codes)
- **Update Count**: Aggregated from database (normalized from u code)
- **Delete Count**: Aggregated from database (normalized from d code)
- **Success Rate**: (Applied events / Total events) * 100
- **Average Latency**: Average `latency_ms` from events

### Pipeline Metrics

- **Events Per Second**: Rate of event processing
- **Lag (LSN)**: Replication lag in bytes/seconds
- **Error Count**: Failed events count
- **Last Event Time**: Timestamp of most recent event

## Configuration

### Environment Variables

**Backend:**
- `DATABASE_URL`: PostgreSQL connection string
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_CONNECT_URL`: Kafka Connect REST API URL
- `JWT_SECRET`: JWT token secret key

**Frontend:**
- `NEXT_PUBLIC_API_URL`: Backend API base URL
- `NEXT_PUBLIC_WS_URL`: WebSocket server URL
- `NEXT_PUBLIC_WS_ENABLED`: Enable/disable WebSocket

## Troubleshooting

### Common Issues

1. **Events Not Showing / Counts Showing 0**
   
   **Root Causes:**
   - ❌ Debezium op codes (c/u/d/r) not normalized to insert/update/delete
   - ❌ Soft-deleted events not filtered (`deleted_at IS NULL` missing)
   - ❌ Redux reducer replacing events instead of appending
   - ❌ WebSocket room name mismatch
   - ❌ Frontend filtering instead of backend aggregation
   
   **Debug Steps:**
   ```sql
   -- Step 1: Check actual event_type values in database
   SELECT run_metadata->>'event_type' as event_type, COUNT(*)
   FROM pipeline_runs
   WHERE run_type='CDC' AND deleted_at IS NULL
   GROUP BY run_metadata->>'event_type';
   
   -- Step 2: Verify soft delete filter
   SELECT COUNT(*) FROM pipeline_runs 
   WHERE run_type='CDC' AND deleted_at IS NULL;
   
   -- Step 3: Check if events exist
   SELECT COUNT(*) FROM pipeline_runs WHERE run_type='CDC';
   ```
   
   **Fixes:**
   - ✅ Ensure Event Logger maps op codes: `c→insert, u→update, d→delete`
   - ✅ Add `deleted_at IS NULL` to all queries
   - ✅ Use backend SQL aggregation for counts
   - ✅ Verify Redux reducer uses `unshift()` not `=`
   - ✅ Check WebSocket room names match: `pipeline_{pipeline_id}`

2. **Pipeline Won't Start**
   - Check connection status
   - Verify Kafka Connect is accessible
   - Check database schema exists
   - Review backend logs for errors

3. **WebSocket Not Connecting**
   - Verify Socket.IO server is running
   - Check CORS configuration
   - Verify WebSocket URL is correct
   - Check firewall/network settings
   - Verify room subscription: `subscribe_pipeline` → `pipeline_{id}`

4. **Request Timeouts**
   - Check database connection
   - Verify query performance
   - Check network latency
   - Review statement timeout settings
   - Use SQL aggregation instead of loading all events

5. **Event Counts Incorrect**
   - **Check**: Are you using frontend filtering or backend aggregation?
   - **Fix**: Use `/api/v1/monitoring/dashboard` which provides aggregated counts
   - **Verify**: Event Logger normalizes Debezium op codes before saving
   - **Check**: Soft delete filter is applied (`deleted_at IS NULL`)

## Performance Considerations

1. **Database Queries**
   - Use pagination for large result sets
   - Index frequently queried columns
   - Set appropriate statement timeouts

2. **Kafka Consumption**
   - Batch events before committing
   - Use appropriate consumer group settings
   - Monitor consumer lag

3. **Frontend Rendering**
   - Use React memoization for expensive calculations
   - Implement virtual scrolling for large lists
   - Debounce/throttle API calls

4. **WebSocket**
   - Limit event emission frequency
   - Use rooms for targeted broadcasts
   - Handle connection failures gracefully

## Security

1. **Authentication**
   - JWT tokens for API access
   - Token expiration and refresh
   - Secure password storage (bcrypt)

2. **Authorization**
   - Role-based access control (RBAC)
   - Permission checks on endpoints
   - Resource-level permissions

3. **Data Protection**
   - Encrypted database connections
   - Secure credential storage
   - Input validation and sanitization

## Deployment

### Backend Deployment
- FastAPI with Uvicorn
- Environment variable configuration
- Database migrations (Alembic)
- Health check endpoints

### Frontend Deployment
- Next.js production build
- Static asset optimization
- API proxy configuration
- Environment variable injection

## Future Enhancements

1. **Caching**
   - Redis for frequently accessed data
   - Cache invalidation strategies

2. **Message Queue**
   - RabbitMQ for async processing
   - Task queue for long-running operations

3. **Monitoring**
   - Prometheus metrics
   - Grafana dashboards
   - Alerting system

4. **Scalability**
   - Horizontal scaling
   - Load balancing
   - Database replication

