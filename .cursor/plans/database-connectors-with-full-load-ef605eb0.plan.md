<!-- ef605eb0-b521-4e8a-b24d-959c8cf0e912 8127e4b8-af65-4131-aac5-071151339dbd -->
# Fix Frontend-Backend Integration Issues

## Problems Identified

1. **Pipeline Creation 422 Error**: Backend requires `source_database` and `source_schema` (required fields), but frontend payload doesn't include them
2. **Row Count Shows 0**: `get_table_row_count` method may not be using correct database connection
3. **Data Flow Mismatch**: Frontend sends data in one format, backend expects different format

## Root Cause Analysis

### Issue 1: Missing Pipeline Fields

**Backend** (`ingestion/api.py:111-127`):

- `PipelineCreate` model requires:
- `source_database: str` (required)
- `source_schema: str` (required)
- `target_database: Optional[str]` (optional)
- `target_schema: Optional[str]` (optional)

**Frontend** (`frontend/app/pipelines/page.tsx:204-227`):

- `handleAddPipeline` builds payload but doesn't include `source_database` or `source_schema`
- Only includes `source_connection_id`, `target_connection_id`, and `table_mappings`

**Frontend API Client** (`frontend/lib/api/client.ts:864-889`):

- `createPipeline` tries to use `pipelineData.source_database` and `pipelineData.source_schema` but they're undefined

### Issue 2: Row Count Display

**Backend** (`ingestion/connectors/postgresql.py:1151-1190`):

- `get_table_row_count` was fixed to connect to target database
- But may still have issues with connection reuse or database context

**Backend** (`ingestion/connection_service.py:266-308`):

- `discover_tables` calls `get_table_row_count` for each table
- Connection is established once, then `get_table_row_count` creates new connections
- May have database context issues

## Solution Architecture

```mermaid
graph TB
subgraph Frontend["Frontend Flow"]
PW[Pipeline Wizard]
HP[handleAddPipeline]
AC[API Client]
end

subgraph Backend["Backend Flow"]
API[POST /api/pipelines]
PS[Pipeline Service]
CS[Connection Service]
Conn[Connectors]
end

PW -->|1. User selects connections| HP
HP -->|2. Extract source_database/schema from sourceConn| HP
HP -->|3. Build payload with all required fields| AC
AC -->|4. POST with source_database, source_schema| API
API -->|5. Validate PipelineCreate| PS

PW -->|Load Tables| AC
AC -->|GET /api/v1/connections/{id}/tables| API
API -->|discover_tables| CS
CS -->|list_tables + get_table_row_count| Conn
Conn -->|Returns tables with ro

### To-dos

- [ ] Create BaseConnector abstract class with common interface methods (connect, extract_schema, extract_data, extract_lsn_offset, full_load)
- [ ] Add extract_lsn_offset() method to SQL Server connector to extract LSN from transaction log
- [ ] Add full_load() method to SQL Server connector for extracting schema + data for specified tables
- [ ] Refactor SQL Server connector to extend BaseConnector class
- [ ] Create PostgreSQL connector extending BaseConnector with schema extraction, data extraction, and LSN/offset extraction
- [ ] Create DataTransfer utility class for direct database-to-database data copying with type mapping support
- [ ] Create __init__.py to export all connectors and transfer utility
- [ ] Create example_usage.py demonstrating connector usage and data transfer
- [ ] Create BaseConnector abstract class with common interface methods (connect, extract_schema, extract_data, extract_lsn_offset, full_load)
- [ ] Add extract_lsn_offset() method to SQL Server connector to extract LSN from transaction log
- [ ] Add full_load() method to SQL Server connector for extracting schema + data for specified tables
- [ ] Refactor SQL Server connector to extend BaseConnector class
- [ ] Create PostgreSQL connector extending BaseConnector with schema extraction, data extraction, and LSN/offset extraction
- [ ] Create DataTransfer utility class for direct database-to-database data copying with type mapping support
- [ ] Create __init__.py to export all connectors and transfer utility
- [ ] Create example_usage.py demonstrating connector usage and data transfer
- [ ] Create BaseConnector abstract class with common interface methods (connect, extract_schema, extract_data, extract_lsn_offset, full_load)
- [ ] Add extract_lsn_offset() method to SQL Server connector to extract LSN from transaction log
- [ ] Add full_load() method to SQL Server connector for extracting schema + data for specified tables
- [ ] Refactor SQL Server connector to extend BaseConnector class
- [ ] Create PostgreSQL connector extending BaseConnector with schema extraction, data extraction, and LSN/offset extraction
- [ ] Create DataTransfer utility class for direct database-to-database data copying with type mapping support
- [ ] Create __init__.py to export all connectors and transfer utility
- [ ] Create example_usage.py demonstrating connector usage and data transfer
- [ ] Create BaseConnector abstract class with common interface methods (connect, extract_schema, extract_data, extract_lsn_offset, full_load)
- [ ] Add extract_lsn_offset() method to SQL Server connector to extract LSN from transaction log
- [ ] Add full_load() method to SQL Server connector for extracting schema + data for specified tables
- [ ] Refactor SQL Server connector to extend BaseConnector class
- [ ] Create PostgreSQL connector extending BaseConnector with schema extraction, data extraction, and LSN/offset extraction
- [ ] Create DataTransfer utility class for direct database-to-database data copying with type mapping support
- [ ] Create __init__.py to export all connectors and transfer utility
- [ ] Create example_usage.py demonstrating connector usage and data transfer