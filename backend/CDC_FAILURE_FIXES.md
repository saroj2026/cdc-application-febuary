# CDC Failure Fixes - Summary

## Problem Analysis

Based on the detailed analysis provided, the issue with `pipeline-feb-11` was:

1. **Full load succeeded** (Postgres → SQL Server)
2. **CDC setup failed** during Debezium connector creation
3. **No error was persisted** - connector names stayed `null` in database
4. **Backend keeps checking for non-existent connectors** (404s in Connect logs)
5. **Schema name inconsistency** - "databa" vs "public" causing different connector names

## Root Causes Identified

1. **Connector creation failure not captured**: When Kafka Connect rejected the connector creation (400 Bad Request, validation error, etc.), the backend got an exception but never stored:
   - The connector name that was attempted
   - The actual error message from Kafka Connect
   - The HTTP status code

2. **Schema name normalization missing**: Pipeline had incorrect schema values (like "databa" truncated from "database") which caused:
   - Different connector names being generated
   - Connectors created with wrong names or not at all

3. **No persistence on failure**: Connector names were only persisted on success, so on failure:
   - `debezium_connector_name` = null
   - `sink_connector_name` = null
   - `kafka_topics` = []
   - No way to know what was attempted

## Fixes Implemented

### 1. **Schema Name Normalization** ✅
- Added validation to detect truncated schema names ("databa", "datab", "dat")
- Automatically normalizes to "public" for source, "dbo"/"public" for target
- Applied before both config generation and connector name generation
- Logs warning when normalization occurs

**Location**: `cdc_manager.py` lines ~509-515, ~960-970

### 2. **Immediate Connector Name Persistence** ✅
- Store connector names **before** attempting creation
- Ensures we have the name even if creation fails
- Persists to database immediately

**Location**: `cdc_manager.py` lines ~530-534, ~980-984

### 3. **Enhanced Error Storage** ✅
- Store detailed error information in `debezium_config['_last_error']` and `sink_config['_last_error']`
- Includes:
  - Error message from Kafka Connect
  - HTTP status code (if available)
  - Connector name that was attempted
  - Timestamp
  - Error type
- Persists even on failure

**Location**: `cdc_manager.py` lines ~674-683, ~730-735, ~1020-1030

### 4. **Better Error Messages** ✅
- Error messages now include the connector name
- Logs clearly indicate what connector name was stored
- Makes debugging much easier

**Location**: Throughout error handling in `cdc_manager.py`

### 5. **Retry Logic for Transient Failures** ✅
- Retries connection errors and timeouts (2 retries with exponential backoff)
- Non-transient HTTP errors (400, 500, etc.) are not retried
- Helps with network issues

**Location**: `cdc_manager.py` lines ~571-608

### 6. **Connector Discovery in Status Retrieval** ✅
- If connector names are missing from database, attempts to discover them from Kafka Connect
- Uses expected connector names based on pipeline configuration
- Auto-recovers connector information
- Persists discovered information

**Location**: `cdc_manager.py` lines ~2230-2310 (in `get_pipeline_status`)

### 7. **Improved Logging** ✅
- Logs schema names used (pipeline vs connection vs default)
- Logs connector names before and after creation
- Logs LSN status
- Better visibility into what's happening

**Location**: Throughout `cdc_manager.py`

## How to Use These Fixes

### For Existing Failed Pipelines

1. **Check the error**: Query the database to see stored errors:
   ```sql
   SELECT 
     name,
     debezium_connector_name,
     sink_connector_name,
     debezium_config->>'_last_error' as debezium_error,
     sink_config->>'_last_error' as sink_error,
     cdc_status
   FROM pipelines 
   WHERE name = 'pipeline-feb-11';
   ```

2. **Check connector names**: Even if creation failed, the connector name should now be stored in `debezium_connector_name` and `sink_connector_name`

3. **Review error details**: The `_last_error` field in configs will show:
   - What error Kafka Connect returned
   - HTTP status code
   - Which connector name was attempted

### For New Pipeline Starts

The fixes will automatically:
- Normalize schema names
- Store connector names before creation
- Capture and persist detailed errors
- Retry transient failures
- Provide better error messages

### To Fix pipeline-feb-11

1. **Check stored error**:
   ```sql
   SELECT debezium_config->>'_last_error' FROM pipelines WHERE name = 'pipeline-feb-11';
   ```

2. **If connector name is stored**, check if it exists in Kafka Connect:
   ```bash
   curl http://72.61.233.209:8083/connectors/cdc-pipeline-feb-11-pg-public/status
   ```

3. **If connector doesn't exist**, the error message will tell you why (config validation, plugin missing, etc.)

4. **Fix the root cause** (config issue, plugin, etc.) and restart the pipeline

## Expected Behavior After Fixes

### On Success:
- Connector names stored immediately
- Topics discovered and stored
- All information persisted to database
- Pipeline status = RUNNING

### On Failure:
- Connector name stored (even though creation failed)
- Detailed error stored in `_last_error`
- Pipeline status = ERROR
- Error message includes connector name
- Can query database to see what went wrong

### On Status Check:
- If connector names missing, attempts discovery from Kafka Connect
- Auto-recovers connector information if connectors exist
- Provides detailed error information from `_last_error`

## Testing

To test these fixes:

1. **Create a test pipeline** with a schema name that might be truncated
2. **Start the pipeline** and let it fail (or succeed)
3. **Check database** - connector names should be stored even on failure
4. **Check error details** - `_last_error` should contain detailed information
5. **Check logs** - should see schema normalization warnings if applicable

## Files Modified

- `backend/ingestion/cdc_manager.py` - Main fixes for CDC setup, error handling, persistence

## Next Steps

1. **Monitor** new pipeline starts to see if errors are now properly captured
2. **Review** stored errors for existing failed pipelines
3. **Fix root causes** based on error messages (config issues, plugins, etc.)
4. **Restart** failed pipelines once root causes are fixed

