"""Connection service for testing and discovery operations."""

import uuid
import logging
import time
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from ingestion.database import SessionLocal
from ingestion.database.models_db import ConnectionModel, ConnectionTestModel
from ingestion.connectors.postgresql import PostgreSQLConnector
from ingestion.connectors.sqlserver import SQLServerConnector
from ingestion.connectors.s3 import S3Connector
from ingestion.connectors.as400 import AS400Connector
from ingestion.connectors.oracle import OracleConnector

logger = logging.getLogger(__name__)


def safe_error_message(msg: Any) -> str:
    """Safely convert error message to string and escape % characters to prevent formatting issues.
    
    This function is designed to handle errors that might themselves contain formatting issues,
    such as "not all arguments converted during string formatting".
    
    Args:
        msg: Error message (can be Exception, str, or any type)
        
    Returns:
        Safe error message string with % characters escaped
    """
    if msg is None:
        return "Unknown error"
    
    # Try multiple methods to safely get the error message
    error_str = None
    
    # Method 1: Try to get error message from exception args (safest)
    # This avoids calling __str__ on the exception which might use % formatting
    try:
        if hasattr(msg, 'args') and msg.args and len(msg.args) > 0:
            # Get the first argument, but handle it very carefully
            first_arg = msg.args[0]
            if isinstance(first_arg, str):
                error_str = first_arg
            elif first_arg is not None:
                # Use repr() first to avoid any __str__ formatting issues
                try:
                    error_str = repr(first_arg)
                    # Remove quotes if it's a string representation
                    if (error_str.startswith("'") and error_str.endswith("'")) or \
                       (error_str.startswith('"') and error_str.endswith('"')):
                        error_str = error_str[1:-1]
                except Exception:
                    # If repr() fails, try str() as last resort
                    try:
                        error_str = str(first_arg)
                    except Exception:
                        pass
    except Exception:
        pass
    
    # Method 2: Use repr() which is safer for formatting (doesn't use % formatting)
    if error_str is None:
        try:
            error_repr = repr(msg)
            # If it's an exception, try to extract just the message part
            if hasattr(msg, '__class__'):
                class_name = msg.__class__.__name__
                if error_repr.startswith(class_name + '(') and error_repr.endswith(')'):
                    # Extract content between parentheses
                    content = error_repr[len(class_name) + 1:-1]
                    # Remove quotes if present
                    if (content.startswith("'") and content.endswith("'")) or \
                       (content.startswith('"') and content.endswith('"')):
                        content = content[1:-1]
                    error_str = content
                else:
                    error_str = error_repr
            else:
                error_str = error_repr
        except Exception:
            pass
    
    # Method 3: Try str() conversion (last resort, might fail with formatting errors)
    # IMPORTANT: str() on exceptions can trigger "not all arguments converted during string formatting"
    # So we catch ValueError specifically which is what Python raises for formatting errors
    if error_str is None:
        try:
            error_str = str(msg)
        except (ValueError, TypeError) as format_error:
            # If str() fails due to formatting error, use exception type name
            # This happens when the exception's __str__ method uses % formatting incorrectly
            try:
                error_str = "Error type: " + type(msg).__name__
            except Exception:
                error_str = "Unknown error occurred"
        except Exception:
            # For any other exception, use safe fallback
            try:
                error_str = "Error type: " + type(msg).__name__
            except Exception:
                error_str = "Unknown error occurred"
    
    # If we still don't have an error string, use ultimate fallback
    if not error_str:
        error_str = "Unknown error occurred"
    
    # Escape % characters to prevent string formatting issues
    # This is critical - we need to escape ALL % characters
    try:
        # Replace %% temporarily to avoid double-escaping
        safe_str = error_str.replace('%%', '\x00\x00')  # Use null bytes as temporary marker
        # Escape all single % characters
        safe_str = safe_str.replace('%', '%%')
        # Restore the original %% (now escaped as %%%%)
        safe_str = safe_str.replace('\x00\x00', '%%')
        return safe_str
    except Exception:
        # If even escaping fails, return a safe message
        return "Error occurred (message formatting failed)"


class ConnectionService:
    """Service for managing database connections."""
    
    def __init__(self):
        """Initialize connection service."""
        self.session = SessionLocal()
    
    def test_connection(
        self,
        connection_id: str,
        save_history: bool = True
    ) -> Dict[str, Any]:
        """Test database connection and optionally save to history.
        
        Args:
            connection_id: Connection ID to test
            save_history: Whether to save test result to history
            
        Returns:
            Test result dictionary with status and details
        """
        try:
            connection = self.session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {
                    "success": False,
                    "error": f"Connection not found: {connection_id}",
                    "status": "NOT_FOUND"
                }
            
            start_time = time.time()
            
            # Check if this is AS400/DB2 - use Kafka Connect fallback if ODBC fails
            db_type = connection.database_type.value.lower() if hasattr(connection.database_type, 'value') else str(connection.database_type).lower()
            is_as400 = db_type in ["as400", "ibm_i", "db2"]
            
            if is_as400:
                # For AS400/DB2, try direct connection first, then fall back to Kafka Connect
                try:
                    connector = self._get_connector(connection)
                    connection_obj = connector.connect()
                    
                    if hasattr(connector, 'get_version'):
                        version = connector.get_version()
                    else:
                        test_result = connector.test_connection()
                        version = "Connection verified" if test_result else "Connection failed"
                    
                    if hasattr(connector, 'disconnect') and connection_obj:
                        connector.disconnect(connection_obj)
                    
                    response_time_ms = int((time.time() - start_time) * 1000)
                    
                    result = {
                        "success": True,
                        "status": "SUCCESS",
                        "response_time_ms": response_time_ms,
                        "version": version,
                        "tested_at": datetime.utcnow().isoformat()
                    }
                    
                    if save_history:
                        self._save_test_history(
                            connection_id=connection_id,
                            status="SUCCESS",
                            response_time_ms=response_time_ms
                        )
                        connection.last_tested_at = datetime.utcnow()
                        connection.last_test_status = "success"
                        self.session.commit()
                    
                    return result
                    
                except Exception as e:
                    # Direct connection failed, try Kafka Connect validation
                    error_msg = str(e)
                    logger.info(f"Direct AS400 connection failed ({error_msg}), trying Kafka Connect validation...")
                    
                    # Use Kafka Connect validation as fallback
                    kafka_result = self._test_as400_via_kafka_connect(connection, start_time, save_history=False)
                    
                    # If Kafka Connect validation succeeded, update the connection status
                    if kafka_result.get("success"):
                        if save_history:
                            self._save_test_history(
                                connection_id=connection_id,
                                status="SUCCESS",
                                response_time_ms=kafka_result.get("response_time_ms", 0)
                            )
                            connection.last_tested_at = datetime.utcnow()
                            connection.last_test_status = "success"
                            self.session.commit()
                        return kafka_result
                    else:
                        # Kafka Connect validation also failed
                        if save_history:
                            self._save_test_history(
                                connection_id=connection_id,
                                status="FAILED",
                                response_time_ms=kafka_result.get("response_time_ms", 0),
                                error_message=kafka_result.get("error", "Connection test failed")
                            )
                            connection.last_tested_at = datetime.utcnow()
                            connection.last_test_status = "failed"
                            self.session.commit()
                        return kafka_result
            
            # For non-AS400 connections, use normal test flow
            connector = self._get_connector(connection)
            connection_obj = None
            
            try:
                connection_obj = connector.connect()
                
                # Get version if method exists, otherwise use test_connection result
                if hasattr(connector, 'get_version'):
                    version = connector.get_version()
                else:
                    # For connectors without get_version (like S3), use test_connection
                    test_result = connector.test_connection()
                    version = "Connection verified" if test_result else "Connection failed"
                
                # Disconnect if method exists and we have a connection object
                if hasattr(connector, 'disconnect') and connection_obj:
                    connector.disconnect(connection_obj)
                
                response_time_ms = int((time.time() - start_time) * 1000)
                
                result = {
                    "success": True,
                    "status": "SUCCESS",
                    "response_time_ms": response_time_ms,
                    "version": version,
                    "tested_at": datetime.utcnow().isoformat()
                }
                
                if save_history:
                    self._save_test_history(
                        connection_id=connection_id,
                        status="SUCCESS",
                        response_time_ms=response_time_ms
                    )
                    
                    connection.last_tested_at = datetime.utcnow()
                    connection.last_test_status = "success"  # Use lowercase to match frontend
                    self.session.commit()
                
                return result
                
            except Exception as e:
                response_time_ms = int((time.time() - start_time) * 1000)
                error_msg = str(e)
                
                logger.error(f"Connection test failed for {connection_id}: {error_msg}")
                
                result = {
                    "success": False,
                    "status": "FAILED",
                    "error": error_msg,
                    "response_time_ms": response_time_ms,
                    "tested_at": datetime.utcnow().isoformat()
                }
                
                if save_history:
                    self._save_test_history(
                        connection_id=connection_id,
                        status="FAILED",
                        response_time_ms=response_time_ms,
                        error_message=error_msg
                    )
                    
                    connection.last_tested_at = datetime.utcnow()
                    connection.last_test_status = "failed"  # Use lowercase to match frontend
                    self.session.commit()
                
                return result
                
        except Exception as e:
            logger.error(f"Error testing connection {connection_id}: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "status": "ERROR"
            }
    
    def discover_databases(self, connection_id: str) -> Dict[str, Any]:
        """Discover available databases in a connection.
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Dictionary with list of databases
        """
        try:
            connection = self.session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {
                    "success": False,
                    "error": f"Connection not found: {connection_id}"
                }
            
            connector = self._get_connector(connection)
            connection_obj = connector.connect()
            
            databases = connector.list_databases()
            
            if hasattr(connector, 'disconnect') and connection_obj:
                connector.disconnect(connection_obj)
            
            return {
                "success": True,
                "databases": databases,
                "count": len(databases)
            }
            
        except Exception as e:
            logger.error(f"Error discovering databases: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def discover_schemas(
        self,
        connection_id: str,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Discover schemas in a database.
        
        Args:
            connection_id: Connection ID
            database: Target database (optional, uses connection default)
            
        Returns:
            Dictionary with list of schemas
        """
        try:
            connection = self.session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {
                    "success": False,
                    "error": f"Connection not found: {connection_id}"
                }
            
            target_db = database or connection.database
            
            connector = self._get_connector(connection)
            connection_obj = connector.connect()
            
            schemas = connector.list_schemas(database=target_db)
            
            if hasattr(connector, 'disconnect') and connection_obj:
                connector.disconnect(connection_obj)
            
            return {
                "success": True,
                "database": target_db,
                "schemas": schemas,
                "count": len(schemas)
            }
            
        except Exception as e:
            logger.error(f"Error discovering schemas: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def discover_tables(
        self,
        connection_id: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Discover tables in a schema.
        
        Args:
            connection_id: Connection ID
            database: Target database (optional)
            schema: Target schema (optional)
            
        Returns:
            Dictionary with list of tables and their metadata
        """
        try:
            connection = self.session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {
                    "success": False,
                    "error": f"Connection not found: {connection_id}"
                }
            
            target_db = database or connection.database
            # Default schema based on database type
            if not schema:
                db_type = connection.database_type.value.lower() if hasattr(connection.database_type, 'value') else str(connection.database_type).lower()
                if db_type in ["sqlserver", "mssql"]:
                    target_schema = connection.schema or "dbo"
                elif db_type == "snowflake":
                    target_schema = connection.schema or "PUBLIC"  # Snowflake default is PUBLIC (uppercase)
                else:
                    target_schema = connection.schema or "public"
            else:
                target_schema = schema
            
            connector = self._get_connector(connection)
            conn = connector.connect()
            
            try:
                tables = connector.list_tables(
                    database=target_db,
                    schema=target_schema
                )
                # Handle both list of strings and list of ConnectorTable objects
                if tables and len(tables) > 0:
                    # Check if first item is a ConnectorTable object (has 'name' attribute)
                    if hasattr(tables[0], 'name'):
                        # Convert ConnectorTable objects to table names
                        table_names = [table.name for table in tables]
                    elif isinstance(tables[0], dict) and 'name' in tables[0]:
                        # Handle list of dictionaries
                        table_names = [table['name'] for table in tables]
                    else:
                        # Assume it's already a list of strings
                        table_names = tables
                else:
                    table_names = []
            except AttributeError as e:
                # If list_tables doesn't exist, try alternative method
                if "list_tables" in str(e):
                    logger.warning(f"list_tables not available, trying alternative method: {e}")
                    # Try using extract_schema for PostgreSQL
                    if hasattr(connector, 'extract_schema'):
                        schema_info = connector.extract_schema(database=target_db, schema=target_schema)
                        table_names = [table["name"] for table in schema_info.get("tables", [])]
                    else:
                        raise
                else:
                    raise
            
            table_details = []
            for table_name in table_names:
                try:
                    # Use the existing connection for better performance and to ensure database context
                    if hasattr(connector, 'get_table_row_count'):
                        row_count = connector.get_table_row_count(
                            table=table_name,
                            database=target_db,
                            schema=target_schema
                        )
                    else:
                        # Fallback: query directly using existing connection
                        cursor = conn.cursor()
                        if connection.database_type.value.lower() in ["sqlserver", "mssql"]:
                            cursor.execute(f"USE [{target_db}]")
                            cursor.execute(f"SELECT COUNT(*) FROM [{target_schema}].[{table_name}]")
                        else:
                            from psycopg2 import sql
                            query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                                sql.Identifier(target_schema),
                                sql.Identifier(table_name)
                            )
                            cursor.execute(query)
                        row_count = cursor.fetchone()[0]
                        cursor.close()
                        row_count = int(row_count) if row_count else 0
                    
                    # Get column information
                    column_count = 0
                    columns = []
                    try:
                        if hasattr(connector, 'get_table_columns'):
                            columns = connector.get_table_columns(
                                table=table_name,
                                database=target_db,
                                schema=target_schema
                            )
                            column_count = len(columns) if columns else 0
                    except Exception as e:
                        logger.warning(f"Could not get columns for {table_name}: {e}")
                        # Continue without column info
                    
                    table_details.append({
                        "name": table_name,
                        "row_count": row_count,
                        "schema": target_schema,
                        "database": target_db,
                        "column_count": column_count,
                        "columns": columns if columns else []
                    })
                except Exception as e:
                    logger.error(f"Could not get row count for {table_name}: {e}", exc_info=True)
                    table_details.append({
                        "name": table_name,
                        "row_count": None,
                        "schema": target_schema,
                        "database": target_db,
                        "column_count": 0,
                        "columns": []
                    })
            
            if hasattr(connector, 'disconnect') and conn:
                connector.disconnect(conn)
            
            return {
                "success": True,
                "database": target_db,
                "schema": target_schema,
                "tables": table_details,
                "count": len(table_details)
            }
            
        except Exception as e:
            logger.error(f"Error discovering tables: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_table_schema(
        self,
        connection_id: str,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get detailed schema information for a table.
        
        Args:
            connection_id: Connection ID
            table_name: Table name
            database: Target database (optional)
            schema: Target schema (optional)
            
        Returns:
            Dictionary with table schema details
        """
        # Use a fresh session for this operation to avoid transaction state issues
        session = SessionLocal()
        try:
            connection = session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {
                    "success": False,
                    "error": f"Connection not found: {connection_id}"
                }
            
            target_db = database or connection.database
            # Default schema based on database type (especially for target connections in pipelines)
            if not schema:
                db_type = connection.database_type.value.lower() if hasattr(connection.database_type, 'value') else str(connection.database_type).lower()
                if db_type in ["sqlserver", "mssql"]:
                    target_schema = connection.schema or "dbo"  # SQL Server default is dbo
                elif db_type == "snowflake":
                    target_schema = connection.schema or "PUBLIC"  # Snowflake default is PUBLIC
                else:
                    target_schema = connection.schema or "public"  # PostgreSQL and others default to public
            else:
                target_schema = schema
            
            # For Oracle, if database parameter is provided and connection.database is empty/None,
            # create connector with database from parameter to avoid validation error
            if connection.database_type.value.lower() == "oracle":
                # Use database parameter if provided, otherwise use connection.database
                oracle_db = database if database else connection.database
                if not oracle_db or (isinstance(oracle_db, str) and oracle_db.strip() == ""):
                    # If both are empty, try to get from additional_config
                    if hasattr(connection, 'additional_config') and connection.additional_config:
                        oracle_db = connection.additional_config.get("service_name") or connection.additional_config.get("database")
                
                if not oracle_db:
                    return {
                        "success": False,
                        "error": "Either 'database' (SID) or 'service_name' must be provided for Oracle connection"
                    }
                
                # Create connector with proper database value
                from .connectors.oracle import OracleConnector
                config = {
                    "host": connection.host,
                    "port": connection.port,
                    "database": oracle_db,  # Use database/SID
                    "username": connection.username,
                    "password": connection.password,
                }
                if hasattr(connection, 'schema') and connection.schema:
                    config["schema"] = connection.schema
                if hasattr(connection, 'additional_config') and connection.additional_config:
                    config.update(connection.additional_config)
                connector = OracleConnector(config)
            else:
                connector = self._get_connector(connection)
            
            try:
                conn = connector.connect()
            except Exception as connect_error:
                safe_error = safe_error_message(connect_error)
                return {
                    "success": False,
                    "error": f"Failed to connect to database: {safe_error}"
                }
            
            try:
                # Clean table name - remove schema prefix if present (e.g., "public.projects_simple" -> "projects_simple")
                clean_table_name = table_name
                if '.' in clean_table_name and target_schema:
                    parts = clean_table_name.split('.', 1)
                    if len(parts) == 2 and parts[0] == target_schema:
                        clean_table_name = parts[1]
                        logger.info("[get_table_schema] Removed duplicate schema from table name: %r -> %r", table_name, clean_table_name)
                
                # Validate table name is not empty
                if not clean_table_name or not clean_table_name.strip():
                    return {
                        "success": False,
                        "error": "Table name cannot be empty"
                    }
                
                # Validate schema is a string
                if target_schema and not isinstance(target_schema, str):
                    target_schema = str(target_schema)
                
                # Check if connector supports get_table_columns
                if not hasattr(connector, 'get_table_columns'):
                    logger.warning("[get_table_schema] Connector %s does not support get_table_columns method", type(connector).__name__)
                    return {
                        "success": False,
                        "error": f"Connector {type(connector).__name__} does not support column extraction"
                    }
                
                # Log connection details for debugging BEFORE executing query
                db_type = connection.database_type.value if hasattr(connection.database_type, 'value') else connection.database_type
                logger.info("[get_table_schema] Getting table columns for: table=%r, schema=%r, database=%r, connection_type=%r", 
                           clean_table_name, target_schema, target_db, db_type)
                
                logger.info("[get_table_schema] Calling connector.get_table_columns with: table=%r, database=%r, schema=%r", 
                           clean_table_name, target_db, target_schema)
                
                columns = connector.get_table_columns(
                    table=clean_table_name,
                    database=target_db,
                    schema=target_schema
                )
                
                logger.info("[get_table_schema] get_table_columns returned %d columns", len(columns) if columns else 0)
                
                if not columns:
                    logger.warning("[get_table_schema] No columns returned for table=%r, schema=%r, database=%r. Table may not exist or user may not have permissions.", 
                                 clean_table_name, target_schema, target_db)
                    columns = []
            except Exception as columns_error:
                if hasattr(connector, 'disconnect') and conn:
                    try:
                        connector.disconnect(conn)
                    except Exception:
                        pass
                # Use safe_error_message function to handle all edge cases
                safe_error = safe_error_message(columns_error)
                
                # Use logger with explicit formatting to avoid any issues
                # Use %r (repr) instead of %s to avoid any formatting issues
                try:
                    logger.error("Error getting table columns: %r", safe_error, exc_info=True)
                except Exception as log_error:
                    # If logging fails, just print to avoid cascading errors
                    try:
                        # Use repr() to avoid any formatting issues
                        print("Error getting table columns:", repr(safe_error))
                    except Exception:
                        print("Error getting table columns: [Error message could not be formatted]")
                
                # Build error message using string concatenation instead of f-string to avoid any formatting issues
                # Ensure safe_error is a string and handle any conversion errors
                # Also escape % characters to prevent formatting issues
                try:
                    if isinstance(safe_error, str):
                        # Escape % characters to prevent formatting issues
                        escaped_error = safe_error.replace('%', '%%')
                        error_message = "Failed to get table columns: " + escaped_error
                    else:
                        # If safe_error is not a string, convert it safely
                        try:
                            error_str = str(safe_error)
                            # Escape % characters
                            escaped_error = error_str.replace('%', '%%')
                            error_message = "Failed to get table columns: " + escaped_error
                        except Exception:
                            # If str() fails, use repr() as fallback
                            try:
                                error_repr = repr(safe_error)
                                escaped_error = error_repr.replace('%', '%%')
                                error_message = "Failed to get table columns: " + escaped_error
                            except Exception:
                                error_message = "Failed to get table columns: Unknown error occurred"
                except Exception:
                    # Ultimate fallback if everything fails
                    error_message = "Failed to get table columns: Unknown error occurred"
                
                return {
                    "success": False,
                    "error": error_message
                }
            
            # Get primary keys - but don't fail if we can't get them
            # Some tables may not have PKs, or user may not have permission
            # We'll still return table schema successfully without PKs
            primary_keys = []
            try:
                primary_keys = connector.get_primary_keys(
                    table=table_name,
                    database=target_db,
                    schema=target_schema
                )
                # get_primary_keys now returns empty list on error instead of raising
                # So this should always succeed
            except Exception as pk_error:
                # Fallback: if get_primary_keys still raises (shouldn't happen after fix)
                # log warning but continue - empty PK list is acceptable
                safe_error = safe_error_message(pk_error)
                logger.warning(f"Could not get primary keys for table {target_schema}.{table_name}: {safe_error}. "
                             f"Continuing with empty primary key list. This may be due to insufficient permissions "
                             f"or the table may not have a primary key.")
                primary_keys = []  # Use empty list as fallback
            
            if hasattr(connector, 'disconnect') and conn:
                connector.disconnect(conn)
            
            return {
                "success": True,
                "table": table_name,
                "database": target_db,
                "schema": target_schema,
                "columns": columns,
                "primary_keys": primary_keys,
                "column_count": len(columns)
            }
            
        except Exception as e:
            logger.error(f"Error getting table schema: {e}", exc_info=True)
            # Ensure session is rolled back on error
            try:
                self.session.rollback()
            except Exception as rollback_error:
                logger.warning(f"Error rolling back session in get_table_schema: {rollback_error}")
                try:
                    self.session.close()
                    self.session = SessionLocal()
                except Exception:
                    pass
            # Escape any % characters in error message to prevent string formatting issues
            safe_error_msg = safe_error_message(e)
            return {
                "success": False,
                "error": safe_error_msg
            }
    
    def get_table_data(
        self,
        connection_id: str,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Get table data from a connection.
        
        Args:
            connection_id: Connection ID
            table_name: Table name
            database: Target database (optional)
            schema: Target schema (optional)
            limit: Maximum number of records to return
            
        Returns:
            Dictionary with table data (records, columns, count)
        """
        # Use a fresh session for this operation to avoid transaction state issues
        session = SessionLocal()
        try:
            connection = session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {
                    "success": False,
                    "error": f"Connection not found: {connection_id}"
                }
            
            target_db = database or connection.database
            # Default schema based on database type (especially for target connections in pipelines)
            # Always determine default based on database type to ensure SQL Server uses "dbo" not "public"
            db_type = connection.database_type.value.lower() if hasattr(connection.database_type, 'value') else str(connection.database_type).lower()
            if db_type in ["sqlserver", "mssql"]:
                default_schema = "dbo"  # SQL Server default is dbo
            elif db_type == "snowflake":
                default_schema = "PUBLIC"  # Snowflake default is PUBLIC
            else:
                default_schema = "public"  # PostgreSQL and others default to public
            
            # Use provided schema, or connection schema, or default
            # But if schema is provided and doesn't match database type default, validate it
            if schema:
                target_schema = schema
                # Warn if schema doesn't match expected default for database type
                if db_type in ["sqlserver", "mssql"] and schema.lower() == "public":
                    logger.warning(f"[get_table_data] SQL Server connection using 'public' schema. Expected 'dbo'. Using provided schema: {schema}")
            else:
                target_schema = connection.schema or default_schema
            
            # Clean table name - remove schema prefix if present (e.g., "public.projects_simple" -> "projects_simple")
            # Define in outer scope so it's available in error handling
            clean_table_name = table_name
            original_table_name = table_name  # Keep original for error messages
            logger.info(f"[get_table_data] Original table_name: {table_name}, target_schema: {target_schema}, database: {target_db}, db_type: {db_type}")
            if '.' in clean_table_name and target_schema:
                parts = clean_table_name.split('.', 1)
                logger.info(f"[get_table_data] Table name has dot, parts: {parts}, comparing '{parts[0]}' == '{target_schema}'")
                if len(parts) == 2 and parts[0] == target_schema:
                    clean_table_name = parts[1]
                    logger.info(f"[get_table_data] Removed duplicate schema from table name: {table_name} -> {clean_table_name}")
                else:
                    logger.info(f"[get_table_data] Schema prefix '{parts[0]}' does not match target_schema '{target_schema}', keeping original name")
            else:
                logger.info(f"[get_table_data] No schema prefix to clean, using table_name as-is: {clean_table_name}")
            
            # Ensure table name is not empty
            if not clean_table_name or not clean_table_name.strip():
                return {
                    "success": False,
                    "error": "Table name cannot be empty"
                }
            
            # Check if database type supports table queries (S3 is object storage, not queryable)
            db_type_for_check = connection.database_type.value.lower() if hasattr(connection.database_type, 'value') else str(connection.database_type).lower()
            # Normalize aws_s3 to s3
            if db_type_for_check == "aws_s3":
                db_type_for_check = "s3"
            
            # Non-queryable database types (object storage, etc.)
            non_queryable_types = ["s3", "aws_s3"]
            if db_type_for_check in non_queryable_types:
                return {
                    "success": False,
                    "error": f"Table comparison is not supported for {connection.database_type.value} connections. "
                            f"{connection.database_type.value} is object storage (S3), not a queryable database. "
                            f"Please use a database connection (PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, etc.) "
                            f"for table comparison operations."
                }
            
            # For Oracle, handle database parameter
            if connection.database_type.value.lower() == "oracle":
                oracle_db = database if database else connection.database
                if not oracle_db or (isinstance(oracle_db, str) and oracle_db.strip() == ""):
                    if hasattr(connection, 'additional_config') and connection.additional_config:
                        oracle_db = connection.additional_config.get("service_name") or connection.additional_config.get("database")
                
                if not oracle_db:
                    return {
                        "success": False,
                        "error": "Either 'database' (SID) or 'service_name' must be provided for Oracle connection"
                    }
            
            try:
                connector = self._get_connector(connection)
            except ImportError as import_error:
                # Handle missing connector library (e.g., snowflake-connector-python)
                error_msg = str(import_error)
                if "snowflake" in error_msg.lower():
                    return {
                        "success": False,
                        "error": "Snowflake connector library is not installed. Please install it with: pip install snowflake-connector-python"
                    }
                else:
                    return {
                        "success": False,
                        "error": f"Database connector library is not installed: {error_msg}"
                    }
            except Exception as connector_error:
                # Escape any % characters in error message
                safe_connector_error = safe_error_message(connector_error)
                return {
                    "success": False,
                    "error": f"Failed to create database connector: {safe_connector_error}"
                }
            
            conn = None
            
            try:
                # Ensure session is clean before calling get_table_schema
                try:
                    self.session.rollback()
                except Exception:
                    pass
                
                conn = connector.connect()
                
                # Get table schema first to get column names (use cleaned table name)
                logger.info(f"[get_table_data] Calling get_table_schema with: connection_id={connection_id}, table={clean_table_name}, database={database}, schema={schema or target_schema}")
                schema_result = self.get_table_schema(
                    connection_id,
                    clean_table_name,  # Use cleaned table name
                    database=database,
                    schema=schema or target_schema  # Use target_schema if schema not provided
                )
                
                logger.info(f"[get_table_data] get_table_schema result: success={schema_result.get('success')}, error={schema_result.get('error', 'None')}")
                
                if not schema_result.get("success"):
                    schema_error = schema_result.get("error", "Failed to get table schema")
                    logger.error(f"[get_table_data] Failed to get table schema: {schema_error}")
                    # Ensure error message is safe (already escaped in get_table_schema, but be safe)
                    safe_schema_error = safe_error_message(schema_error)
                    return {
                        "success": False,
                        "error": safe_schema_error
                    }
                
                columns = schema_result.get("columns", [])
                column_names = [col.get("name") for col in columns if col.get("name")]
                
                if not column_names:
                    return {
                        "success": False,
                        "error": "No columns found in table"
                    }
                
                # Build query based on database type
                db_type = connection.database_type.value.lower()
                
                if db_type in ["postgresql", "postgres"]:
                    # PostgreSQL - use cleaned table name
                    logger.info(f"[get_table_data] Building PostgreSQL query: schema={target_schema}, table={clean_table_name}, limit={limit}")
                    if target_schema and target_schema != "public":
                        query = f'SELECT * FROM "{target_schema}"."{clean_table_name}" LIMIT {limit}'
                    else:
                        # For public schema, we can omit schema or include it explicitly
                        query = f'SELECT * FROM "public"."{clean_table_name}" LIMIT {limit}'
                    logger.info(f"[get_table_data] PostgreSQL query: {query}")
                elif db_type in ["sqlserver", "mssql"]:
                    # SQL Server - use cleaned table name
                    logger.info(f"[get_table_data] Building SQL Server query: schema={target_schema}, table={clean_table_name}, limit={limit}")
                    if target_schema:
                        query = f'SELECT TOP {limit} * FROM [{target_schema}].[{clean_table_name}]'
                    else:
                        query = f'SELECT TOP {limit} * FROM [{clean_table_name}]'
                    logger.info(f"[get_table_data] SQL Server query: {query}")
                elif db_type == "oracle":
                    # Oracle: Unquoted identifiers are uppercase, quoted preserve case
                    # Try uppercase first (most common), then try with quotes if that fails
                    if target_schema:
                        # Try uppercase (unquoted) first - most common case
                        query = f'SELECT * FROM {target_schema.upper()}.{table_name.upper()} WHERE ROWNUM <= {limit}'
                    else:
                        # Try uppercase (unquoted) first
                        query = f'SELECT * FROM {table_name.upper()} WHERE ROWNUM <= {limit}'
                elif db_type == "mysql":
                    # MySQL
                    if target_schema:
                        query = f'SELECT * FROM `{target_schema}`.`{table_name}` LIMIT {limit}'
                    else:
                        query = f'SELECT * FROM `{table_name}` LIMIT {limit}'
                elif db_type == "snowflake":
                    # Snowflake: Use USE DATABASE/SCHEMA context, then use simple table name
                    # Strip any quotes from database/schema names before using
                    clean_db = (target_db or connection.database or "").strip('"\'')
                    clean_schema = (target_schema or "PUBLIC").strip('"\'')
                    clean_table = clean_table_name.strip('"\'')
                    
                    # Uppercase database and schema names (Snowflake convention)
                    db_upper = clean_db.upper() if clean_db else None
                    schema_upper = clean_schema.upper() if clean_schema else "PUBLIC"
                    
                    # For Snowflake, after setting USE DATABASE/SCHEMA context,
                    # we can use just the table name (not fully qualified)
                    # Try uppercase first (most common), then quoted if needed
                    # Store for use in execution block - will try uppercase first
                    snowflake_table_upper = clean_table.upper()
                    snowflake_table_quoted = clean_table  # Preserve case for quoted attempt
                    
                    # Build query using context (will be set in execution block)
                    # Start with uppercase unquoted (most common case in Snowflake)
                    query = f'SELECT * FROM {snowflake_table_upper} LIMIT {limit}'
                    # Store both for fallback
                    snowflake_query_upper = query
                    snowflake_query_quoted = f'SELECT * FROM "{snowflake_table_quoted}" LIMIT {limit}'
                    
                    logger.info(f"[get_table_data] Snowflake will use context: db={db_upper}, schema={schema_upper}, table={snowflake_table_upper} (will try uppercase first)")
                else:
                    return {
                        "success": False,
                        "error": f"Unsupported database type: {db_type}"
                    }
                
                # Execute query using cursor - handle different database types
                records = []
                total_count = 0
                
                if db_type in ["postgresql", "postgres"]:
                    from psycopg2.extras import RealDictCursor
                    cursor = conn.cursor(cursor_factory=RealDictCursor)
                    logger.info(f"[get_table_data] Executing PostgreSQL query: {query}")
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    logger.info(f"[get_table_data] PostgreSQL query returned {len(rows)} rows")
                    # RealDictCursor returns dict rows
                    for row in rows:
                        record = dict(row)
                        # Convert datetime and other types to strings for JSON serialization
                        for key, value in record.items():
                            if hasattr(value, 'isoformat'):
                                record[key] = value.isoformat()
                            elif value is None:
                                record[key] = None
                            else:
                                record[key] = str(value)
                        records.append(record)
                    logger.info(f"[get_table_data] Processed {len(records)} records from PostgreSQL")
                    cursor.close()
                    
                    # Get count - use cleaned table name
                    logger.info(f"[get_table_data] Building PostgreSQL count query: schema={target_schema}, table={clean_table_name}")
                    if target_schema and target_schema != "public":
                        count_query = f'SELECT COUNT(*) FROM "{target_schema}"."{clean_table_name}"'
                    else:
                        count_query = f'SELECT COUNT(*) FROM "public"."{clean_table_name}"'
                    logger.info(f"[get_table_data] PostgreSQL count query: {count_query}")
                    count_cursor = conn.cursor()
                    count_cursor.execute(count_query)
                    total_count = count_cursor.fetchone()[0]
                    logger.info(f"[get_table_data] PostgreSQL count query returned: {total_count} total records")
                    count_cursor.close()
                    
                elif db_type in ["sqlserver", "mssql"]:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    # Get column names from cursor description
                    column_names_from_cursor = [desc[0] for desc in cursor.description] if cursor.description else column_names
                    for row in rows:
                        record = {}
                        for i, col_name in enumerate(column_names_from_cursor):
                            if i < len(row):
                                value = row[i]
                                if hasattr(value, 'isoformat'):
                                    record[col_name] = value.isoformat()
                                elif value is None:
                                    record[col_name] = None
                                else:
                                    record[col_name] = str(value)
                        records.append(record)
                    cursor.close()
                    
                    # Get count - use cleaned table name
                    logger.info(f"[get_table_data] Building SQL Server count query: schema={target_schema}, table={clean_table_name}")
                    if target_schema:
                        count_query = f'SELECT COUNT(*) FROM [{target_schema}].[{clean_table_name}]'
                    else:
                        count_query = f'SELECT COUNT(*) FROM [{clean_table_name}]'
                    logger.info(f"[get_table_data] SQL Server count query: {count_query}")
                    count_cursor = conn.cursor()
                    count_cursor.execute(count_query)
                    total_count = count_cursor.fetchone()[0]
                    count_cursor.close()
                    
                elif db_type == "oracle":
                    cursor = conn.cursor()
                    try:
                        cursor.execute(query)
                        rows = cursor.fetchall()
                    except Exception as query_error:
                        error_msg = str(query_error)
                        # If uppercase query fails, try with quotes (preserve case)
                        if "ORA-00942" in error_msg or "does not exist" in error_msg.lower():
                            # Try with quoted identifiers (preserve case)
                            if target_schema:
                                query = f'SELECT * FROM "{target_schema}"."{table_name}" WHERE ROWNUM <= {limit}'
                            else:
                                query = f'SELECT * FROM "{table_name}" WHERE ROWNUM <= {limit}'
                            try:
                                cursor.execute(query)
                                rows = cursor.fetchall()
                            except Exception as retry_error:
                                # If both fail, provide helpful error message
                                cursor.close()
                                # Escape any % characters in error_msg to prevent formatting issues
                                safe_error_msg = safe_error_message(error_msg)
                                return {
                                    "success": False,
                                    "error": f"Oracle table or view does not exist: {target_schema or 'default'}.{table_name}. "
                                            f"Oracle table names are case-sensitive. "
                                            f"Tried: {target_schema.upper() if target_schema else 'default'}.{table_name.upper()} and \"{target_schema or 'default'}\".\"{table_name}\". "
                                            f"Please verify:\n"
                                            f"1. The schema name is correct (current: {target_schema or 'default'})\n"
                                            f"2. The table name matches exactly (including case)\n"
                                            f"3. You have SELECT permissions on the table\n"
                                            f"4. The table exists in the specified schema\n\n"
                                            f"Original error: {safe_error_msg}"
                                }
                        else:
                            cursor.close()
                            raise query_error
                    
                    # Get column names from cursor description
                    column_names_from_cursor = [desc[0] for desc in cursor.description] if cursor.description else column_names
                    for row in rows:
                        record = {}
                        for i, col_name in enumerate(column_names_from_cursor):
                            if i < len(row):
                                value = row[i]
                                if hasattr(value, 'isoformat'):
                                    record[col_name] = value.isoformat()
                                elif value is None:
                                    record[col_name] = None
                                else:
                                    record[col_name] = str(value)
                        records.append(record)
                    cursor.close()
                    
                    # Get count - try uppercase first, then quoted
                    try:
                        if target_schema:
                            count_query = f'SELECT COUNT(*) FROM {target_schema.upper()}.{table_name.upper()}'
                        else:
                            count_query = f'SELECT COUNT(*) FROM {table_name.upper()}'
                        count_cursor = conn.cursor()
                        count_cursor.execute(count_query)
                        total_count = count_cursor.fetchone()[0]
                        count_cursor.close()
                    except Exception:
                        # Try with quotes if uppercase fails
                        try:
                            if target_schema:
                                count_query = f'SELECT COUNT(*) FROM "{target_schema}"."{table_name}"'
                            else:
                                count_query = f'SELECT COUNT(*) FROM "{table_name}"'
                            count_cursor = conn.cursor()
                            count_cursor.execute(count_query)
                            total_count = count_cursor.fetchone()[0]
                            count_cursor.close()
                        except Exception as count_error:
                            logger.warning(f"Failed to get table count: {count_error}")
                            total_count = len(records)
                    
                elif db_type == "mysql":
                    cursor = conn.cursor()
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    # Get column names from cursor description
                    column_names_from_cursor = [desc[0] for desc in cursor.description] if cursor.description else column_names
                    for row in rows:
                        record = {}
                        for i, col_name in enumerate(column_names_from_cursor):
                            if i < len(row):
                                value = row[i]
                                if hasattr(value, 'isoformat'):
                                    record[col_name] = value.isoformat()
                                elif value is None:
                                    record[col_name] = None
                                else:
                                    record[col_name] = str(value)
                        records.append(record)
                    cursor.close()
                    
                    # Get count
                    count_query = f'SELECT COUNT(*) FROM `{target_schema}`.`{table_name}`' if target_schema else f'SELECT COUNT(*) FROM `{table_name}`'
                    count_cursor = conn.cursor()
                    count_cursor.execute(count_query)
                    total_count = count_cursor.fetchone()[0]
                    count_cursor.close()
                    
                elif db_type == "snowflake":
                    # Snowflake: Re-extract cleaned names for execution block
                    snowflake_clean_db = (target_db or connection.database or "").strip('"\'')
                    snowflake_clean_schema = (target_schema or "PUBLIC").strip('"\'')
                    snowflake_clean_table = clean_table_name.strip('"\'')
                    snowflake_db_upper = snowflake_clean_db.upper() if snowflake_clean_db else None
                    snowflake_schema_upper = snowflake_clean_schema.upper() if snowflake_clean_schema else "PUBLIC"
                    snowflake_table_upper = snowflake_clean_table.upper()
                    snowflake_table_quoted = snowflake_clean_table
                    
                    # Set database and schema context first
                    cursor = conn.cursor()
                    
                    try:
                        if snowflake_db_upper:
                            cursor.execute(f'USE DATABASE "{snowflake_db_upper}"')
                            logger.debug(f"[get_table_data] Set Snowflake database context: {snowflake_db_upper}")
                        if snowflake_schema_upper:
                            cursor.execute(f'USE SCHEMA "{snowflake_schema_upper}"')
                            logger.debug(f"[get_table_data] Set Snowflake schema context: {snowflake_schema_upper}")
                    except Exception as context_error:
                        logger.warning(f"[get_table_data] Could not set database/schema context: {context_error}, will try fully qualified query")
                        cursor.close()
                        # Fallback to fully qualified query
                        if snowflake_db_upper and snowflake_schema_upper:
                            snowflake_query_upper = f'SELECT * FROM "{snowflake_db_upper}"."{snowflake_schema_upper}".{snowflake_table_upper} LIMIT {limit}'
                            snowflake_query_quoted = f'SELECT * FROM "{snowflake_db_upper}"."{snowflake_schema_upper}"."{snowflake_table_quoted}" LIMIT {limit}'
                        elif snowflake_schema_upper:
                            snowflake_query_upper = f'SELECT * FROM "{snowflake_schema_upper}".{snowflake_table_upper} LIMIT {limit}'
                            snowflake_query_quoted = f'SELECT * FROM "{snowflake_schema_upper}"."{snowflake_table_quoted}" LIMIT {limit}'
                        else:
                            snowflake_query_upper = f'SELECT * FROM {snowflake_table_upper} LIMIT {limit}'
                            snowflake_query_quoted = f'SELECT * FROM "{snowflake_table_quoted}" LIMIT {limit}'
                        cursor = conn.cursor()
                    
                    # After setting USE DATABASE/SCHEMA context, use simple table name
                    # Try uppercase unquoted first (most common in Snowflake - unquoted identifiers are uppercase)
                    try:
                        query_to_use = f'SELECT * FROM {snowflake_table_upper} LIMIT {limit}'
                        logger.info(f"[get_table_data] Executing Snowflake query (uppercase, using context): {query_to_use}")
                        cursor.execute(query_to_use)
                        rows = cursor.fetchall()
                    except Exception as upper_error:
                        # If uppercase fails, try quoted (preserve case)
                        error_msg = str(upper_error).lower()
                        if "does not exist" in error_msg or "not authorized" in error_msg:
                            try:
                                query_to_use = f'SELECT * FROM "{snowflake_table_quoted}" LIMIT {limit}'
                                logger.info(f"[get_table_data] Uppercase failed, trying quoted: {query_to_use}")
                                cursor.execute(query_to_use)
                                rows = cursor.fetchall()
                            except Exception as quoted_error:
                                # If both fail, provide helpful error
                                cursor.close()
                                safe_error = safe_error_message(quoted_error)
                                return {
                                    "success": False,
                                    "error": f"Table does not exist: {snowflake_clean_table} in schema: {snowflake_schema_upper}. "
                                            f"Tried: {snowflake_table_upper} and \"{snowflake_table_quoted}\". "
                                            f"Please verify the table exists and you have SELECT permissions. "
                                            f"Original error: {safe_error}"
                                }
                        else:
                            cursor.close()
                            raise upper_error
                    
                    # Get column names from cursor description
                    column_names_from_cursor = [desc[0] for desc in cursor.description] if cursor.description else column_names
                    for row in rows:
                        record = {}
                        for i, col_name in enumerate(column_names_from_cursor):
                            if i < len(row):
                                value = row[i]
                                if hasattr(value, 'isoformat'):
                                    record[col_name] = value.isoformat()
                                elif value is None:
                                    record[col_name] = None
                                else:
                                    record[col_name] = str(value)
                        records.append(record)
                    cursor.close()
                    
                    # Get count - use same approach (try uppercase, then quoted)
                    try:
                        count_cursor = conn.cursor()
                        try:
                            if snowflake_db_upper:
                                count_cursor.execute(f'USE DATABASE "{snowflake_db_upper}"')
                            if snowflake_schema_upper:
                                count_cursor.execute(f'USE SCHEMA "{snowflake_schema_upper}"')
                        except Exception:
                            pass  # Context may already be set
                        
                        # Try uppercase first
                        try:
                            count_query = f'SELECT COUNT(*) FROM {snowflake_table_upper}'
                            logger.debug(f"[get_table_data] Executing Snowflake count query (uppercase): {count_query}")
                            count_cursor.execute(count_query)
                            total_count = count_cursor.fetchone()[0]
                        except Exception:
                            # Try quoted
                            count_query = f'SELECT COUNT(*) FROM "{snowflake_table_quoted}"'
                            logger.debug(f"[get_table_data] Executing Snowflake count query (quoted): {count_query}")
                            count_cursor.execute(count_query)
                            total_count = count_cursor.fetchone()[0]
                        count_cursor.close()
                    except Exception as count_error:
                        # If count query fails, use row count as fallback
                        logger.warning(f"[get_table_data] Failed to get table count: {count_error}, using row count as fallback")
                        total_count = len(records)
                else:
                    return {
                        "success": False,
                        "error": f"Unsupported database type: {db_type}"
                    }
                
                logger.info(f"[get_table_data] Returning result: success=True, records={len(records)}, columns={len(columns) if columns else 0}, count={total_count}, limit={limit}")
                return {
                    "success": True,
                    "records": records,
                    "columns": columns,
                    "count": total_count,
                    "limit": limit
                }
                
            finally:
                if conn and hasattr(connector, 'disconnect'):
                    try:
                        connector.disconnect(conn)
                    except Exception as disconnect_error:
                        logger.warning(f"Error disconnecting from database: {disconnect_error}")
                # Session will be closed in the outer finally block
                    
        except Exception as e:
            # Safely log error to avoid formatting issues
            safe_error_msg = safe_error_message(e)
            try:
                logger.error("Failed to get table data: %r", safe_error_msg, exc_info=True)
            except Exception as log_err:
                # If logging fails, just print to avoid cascading errors
                try:
                    print("Failed to get table data:", repr(safe_error_msg))
                except Exception:
                    print("Failed to get table data: [Error message could not be formatted]")
            
            error_msg = str(e) if hasattr(e, '__str__') else repr(e)
            
            # Get clean_table_name - it might not be defined if error occurred early
            try:
                table_name_for_error = clean_table_name if 'clean_table_name' in locals() else table_name
            except:
                table_name_for_error = table_name
            
            # Get target_schema - it might not be defined if error occurred early
            try:
                schema_for_error = target_schema if 'target_schema' in locals() else schema
            except:
                schema_for_error = schema
            
            # Provide more helpful error messages for common issues
            # Use string concatenation instead of f-strings to avoid formatting issues
            if "ORA-00942" in error_msg or "table or view does not exist" in error_msg.lower():
                error_text = "Oracle table or view does not exist: " + str(table_name_for_error) + ". "
                error_text += "Oracle table names are case-sensitive. "
                error_text += "Please verify:\n"
                error_text += "1. The schema name is correct (current: " + str(schema_for_error or 'default') + ")\n"
                error_text += "2. The table name matches exactly (including case)\n"
                error_text += "3. You have SELECT permissions on the table\n"
                error_text += "4. The table exists in the specified schema\n\n"
                error_text += "Original error: " + str(safe_error_msg)
                return {
                    "success": False,
                    "error": error_text
                }
            elif "snowflake" in error_msg.lower() and "not installed" in error_msg.lower():
                error_text = "Snowflake connector library is not installed. "
                error_text += "Please install it with: pip install snowflake-connector-python\n\n"
                error_text += "After installation, restart the backend server."
                return {
                    "success": False,
                    "error": error_text
                }
            elif "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or ("table" in error_msg.lower() and "not found" in error_msg.lower()):
                # PostgreSQL/SQL Server table doesn't exist
                error_text = "Table does not exist: " + str(table_name_for_error) + " in schema: " + str(schema_for_error or 'default') + ". "
                error_text += "Please verify:\n"
                error_text += "1. The table name is correct: " + str(table_name_for_error) + "\n"
                error_text += "2. The schema is correct: " + str(schema_for_error or 'default') + "\n"
                error_text += "3. The table was created during full load\n"
                error_text += "4. You have SELECT permissions on the table\n\n"
                error_text += "Original error: " + str(safe_error_msg)
                return {
                    "success": False,
                    "error": error_text
                }
            else:
                # Return safe error message (with % escaped)
                return {
                    "success": False,
                    "error": safe_error_msg
                }
        finally:
            # Always close the session
            try:
                session.close()
            except Exception:
                pass
    
    def get_test_history(
        self,
        connection_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get connection test history.
        
        Args:
            connection_id: Connection ID
            limit: Maximum number of records to return
            
        Returns:
            List of test history records
        """
        try:
            tests = self.session.query(ConnectionTestModel).filter_by(
                connection_id=connection_id
            ).order_by(
                ConnectionTestModel.tested_at.desc()
            ).limit(limit).all()
            
            return [
                {
                    "id": test.id,
                    "status": test.test_status,
                    "response_time_ms": test.response_time_ms,
                    "error_message": test.error_message,
                    "tested_at": test.tested_at.isoformat() if test.tested_at else None
                }
                for test in tests
            ]
            
        except Exception as e:
            logger.error(f"Error getting test history: {e}", exc_info=True)
            return []
    
    def validate_connection(self, connection_id: str) -> Tuple[bool, Optional[str]]:
        """Validate if a connection is usable.
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        result = self.test_connection(connection_id, save_history=False)
        
        if result["success"]:
            return True, None
        else:
            return False, result.get("error", "Connection test failed")
    
    def test_connection_data(
        self,
        connection: Any,  # Can be Connection model or ConnectionModel
        save_history: bool = False
    ) -> Dict[str, Any]:
        """Test database connection from connection data (without saving to DB).
        
        Args:
            connection: Connection object (Connection or ConnectionModel)
            save_history: Whether to save test result to history (only if connection has ID)
            
        Returns:
            Test result dictionary with status and details
        """
        try:
            start_time = time.time()
            
            # Check if this is AS400 and if we should use Kafka Connect fallback
            db_type = None
            if hasattr(connection, 'database_type'):
                db_type = connection.database_type
                if hasattr(db_type, 'value'):
                    db_type = db_type.value
                db_type = str(db_type).lower() if db_type else None
            
            # For AS400/DB2, try Kafka Connect validation if ODBC driver is not available
            if db_type in ["as400", "ibm_i", "db2"]:
                try:
                    connector = self._get_connector_from_data(connection)
                    connection_obj = connector.connect()
                    
                    # Get version if method exists
                    if hasattr(connector, 'get_version'):
                        version = connector.get_version()
                    else:
                        version = "Connection verified"
                    
                    # Disconnect if method exists
                    if hasattr(connector, 'disconnect') and connection_obj:
                        connector.disconnect(connection_obj)
                    
                    response_time_ms = int((time.time() - start_time) * 1000)
                    result = {
                        "success": True,
                        "status": "SUCCESS",
                        "response_time_ms": response_time_ms,
                        "version": version,
                        "tested_at": datetime.utcnow().isoformat()
                    }
                    
                    if save_history and hasattr(connection, 'id') and connection.id:
                        self._save_test_history(
                            connection_id=connection.id,
                            status="SUCCESS",
                            response_time_ms=response_time_ms
                        )
                        if isinstance(connection, ConnectionModel):
                            connection.last_tested_at = datetime.utcnow()
                            connection.last_test_status = "success"
                            self.session.commit()
                    
                    return result
                    
                except ValueError as e:
                    error_msg = str(e)
                    # Check if error is about missing ODBC driver
                    if "ODBC driver" in error_msg or "IBM i Access" in error_msg:
                        # Try Kafka Connect validation as fallback
                        logger.info("ODBC driver not available for AS400, trying Kafka Connect validation...")
                        return self._test_as400_via_kafka_connect(connection, start_time, save_history)
                    # Re-raise if it's a different ValueError
                    raise
                except Exception as e:
                    # For other exceptions, try Kafka Connect fallback
                    error_msg = str(e)
                    logger.info(f"Direct AS400 connection failed ({error_msg}), trying Kafka Connect validation...")
                    return self._test_as400_via_kafka_connect(connection, start_time, save_history)
            
            # For other database types, use direct connection
            connector = self._get_connector_from_data(connection)
            
            try:
                connection_obj = connector.connect()
                
                # Get version if method exists, otherwise use test_connection result
                if hasattr(connector, 'get_version'):
                    version = connector.get_version()
                else:
                    # For connectors without get_version (like S3), use test_connection
                    test_result = connector.test_connection()
                    version = "Connection verified" if test_result else "Connection failed"
                
                # Disconnect if method exists and we have a connection object
                if hasattr(connector, 'disconnect') and connection_obj:
                    connector.disconnect(connection_obj)
                
                response_time_ms = int((time.time() - start_time) * 1000)
                
                result = {
                    "success": True,
                    "status": "SUCCESS",
                    "response_time_ms": response_time_ms,
                    "version": version,
                    "tested_at": datetime.utcnow().isoformat()
                }
                
                # Only save history if connection has an ID (exists in DB)
                if save_history and hasattr(connection, 'id') and connection.id:
                    self._save_test_history(
                        connection_id=connection.id,
                        status="SUCCESS",
                        response_time_ms=response_time_ms
                    )
                    
                    if isinstance(connection, ConnectionModel):
                        connection.last_tested_at = datetime.utcnow()
                        connection.last_test_status = "success"  # Use lowercase to match frontend
                        self.session.commit()
                
                return result
                
            except Exception as e:
                response_time_ms = int((time.time() - start_time) * 1000)
                error_msg = str(e)
                
                logger.error(f"Connection test failed: {error_msg}")
                
                result = {
                    "success": False,
                    "status": "FAILED",
                    "error": error_msg,
                    "response_time_ms": response_time_ms,
                    "tested_at": datetime.utcnow().isoformat()
                }
                
                # Only save history if connection has an ID
                if save_history and hasattr(connection, 'id') and connection.id:
                    self._save_test_history(
                        connection_id=connection.id,
                        status="FAILED",
                        response_time_ms=response_time_ms,
                        error_message=error_msg
                    )
                    
                    if isinstance(connection, ConnectionModel):
                        connection.last_tested_at = datetime.utcnow()
                        connection.last_test_status = "failed"  # Use lowercase to match frontend
                        self.session.commit()
                
                return result
                
        except Exception as e:
            logger.error(f"Error testing connection: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "status": "ERROR"
            }
    
    def _test_as400_via_kafka_connect(
        self,
        connection: Any,
        start_time: float,
        save_history: bool
    ) -> Dict[str, Any]:
        """Test AS400 connection via Kafka Connect connector validation.
        
        Args:
            connection: Connection object
            start_time: Start time for response time calculation
            save_history: Whether to save test result
            
        Returns:
            Test result dictionary
        """
        try:
            from ingestion.kafka_connect_client import KafkaConnectClient
            import os
            
            # Get Kafka Connect URL from environment or use default (same as api.py)
            kafka_connect_url = os.getenv("KAFKA_CONNECT_URL", "http://72.61.233.209:8083")
            kafka_client = KafkaConnectClient(base_url=kafka_connect_url)
            
            # Check if AS400 connector plugin is available
            plugins = kafka_client.get_connector_plugins()
            as400_plugin = None
            for plugin in plugins:
                if plugin.get("class") == "io.debezium.connector.db2as400.As400RpcConnector":
                    as400_plugin = plugin
                    break
            
            if not as400_plugin:
                response_time_ms = int((time.time() - start_time) * 1000)
                return {
                    "success": False,
                    "status": "FAILED",
                    "error": (
                        "AS400 connection test requires either:\n"
                        "1. IBM i Access ODBC Driver installed on backend server, OR\n"
                        "2. Kafka Connect AS400 connector available (io.debezium.connector.db2as400.As400RpcConnector)\n\n"
                        "Currently: ODBC driver not found, and Kafka Connect AS400 connector not found."
                    ),
                    "response_time_ms": response_time_ms,
                    "tested_at": datetime.utcnow().isoformat()
                }
            
            # Build AS400 connector config for validation
            library = (
                connection.additional_config.get("library") if hasattr(connection, 'additional_config') and connection.additional_config 
                else connection.schema if hasattr(connection, 'schema') and connection.schema 
                else connection.database if hasattr(connection, 'database') else ""
            )
            
            # Generate a temporary connector name for validation
            # Use connection name if available, otherwise generate one
            connector_name = "test-as400-connector"
            if hasattr(connection, 'name') and connection.name:
                # Sanitize connection name for use as connector name (Kafka Connect has naming restrictions)
                connector_name = re.sub(r'[^a-zA-Z0-9_-]', '-', str(connection.name).lower())[:50]
                if not connector_name:
                    connector_name = "test-as400-connector"
            
            config = {
                "name": connector_name,  # Required by Kafka Connect for validation
                "connector.class": "io.debezium.connector.db2as400.As400RpcConnector",
                "database.hostname": connection.host,
                "database.port": str(connection.port or 446),
                "database.user": connection.username,
                "database.password": connection.password,
                "database.dbname": library or connection.database,
            }
            
            # Add additional config if present
            if hasattr(connection, 'additional_config') and connection.additional_config:
                if "journal_library" in connection.additional_config:
                    config["database.journal.library"] = connection.additional_config["journal_library"]
                if "tablename" in connection.additional_config:
                    config["table.include.list"] = connection.additional_config["tablename"]
            
            # Validate connector config
            try:
                validation_result = kafka_client.validate_connector_config(
                    "io.debezium.connector.db2as400.As400RpcConnector",
                    config
                )
            except Exception as e:
                # If validation fails, it might be due to missing jt400.jar
                error_msg = str(e)
                if "jt400" in error_msg.lower() or "toolbox" in error_msg.lower() or "classnotfound" in error_msg.lower():
                    response_time_ms = int((time.time() - start_time) * 1000)
                    return {
                        "success": False,
                        "status": "FAILED",
                        "error": (
                            "AS400 connector validation failed. The Debezium AS400 connector requires jt400.jar (IBM Toolbox for Java).\n\n"
                            "To fix this:\n"
                            "1. Download jt400.jar from IBM (or use Maven: com.ibm.as400:jt400)\n"
                            "2. Place it in Kafka Connect's plugin path or lib directory\n"
                            "3. Restart Kafka Connect\n\n"
                            f"Original error: {error_msg}"
                        ),
                        "response_time_ms": response_time_ms,
                        "tested_at": datetime.utcnow().isoformat()
                    }
                # Re-raise if it's a different error
                raise
            
            response_time_ms = int((time.time() - start_time) * 1000)
            
            # Check validation result
            errors = validation_result.get("error_count", 0)
            if errors == 0:
                result = {
                    "success": True,
                    "status": "SUCCESS",
                    "response_time_ms": response_time_ms,
                    "version": f"Kafka Connect AS400 Connector {as400_plugin.get('version', 'unknown')}",
                    "tested_at": datetime.utcnow().isoformat(),
                    "note": "Connection validated via Kafka Connect (ODBC driver not required)"
                }
                
                if save_history and hasattr(connection, 'id') and connection.id:
                    self._save_test_history(
                        connection_id=connection.id,
                        status="SUCCESS",
                        response_time_ms=response_time_ms
                    )
                    if isinstance(connection, ConnectionModel):
                        connection.last_tested_at = datetime.utcnow()
                        connection.last_test_status = "success"
                        self.session.commit()
                
                return result
            else:
                # Extract error messages from validation - check multiple possible formats
                error_messages = []
                
                # Format 1: errors in configs array
                configs = validation_result.get("configs", [])
                for cfg in configs:
                    # Check for 'errors' array
                    if cfg.get("value", {}).get("errors"):
                        error_messages.extend(cfg.get("value", {}).get("errors", []))
                    elif cfg.get("errors"):
                        error_messages.extend(cfg.get("errors", []))
                    # Check for 'value.errors' nested
                    if isinstance(cfg.get("value"), dict) and cfg.get("value", {}).get("errors"):
                        error_messages.extend(cfg.get("value", {}).get("errors", []))
                
                # Format 2: direct errors field
                if validation_result.get("errors"):
                    if isinstance(validation_result.get("errors"), list):
                        error_messages.extend(validation_result.get("errors", []))
                    else:
                        error_messages.append(str(validation_result.get("errors")))
                
                # Format 3: error field
                if validation_result.get("error"):
                    error_messages.append(str(validation_result.get("error")))
                
                # Format 4: message field
                if validation_result.get("message"):
                    error_messages.append(str(validation_result.get("message")))
                
                # If no errors found, include the full validation result for debugging
                if not error_messages:
                    error_messages.append(f"Validation failed (error_count: {errors}). Full response: {str(validation_result)[:500]}")
                
                # Join all error messages (show up to 10, not just 3)
                error_msg = "Kafka Connect validation failed: " + "; ".join(error_messages[:10])
                
                # Check for specific jt400.jar related errors
                full_error_text = " ".join(error_messages).lower()
                if any(keyword in full_error_text for keyword in ["classnotfound", "noclassdeffound", "jt400", "toolbox", "com.ibm.as400"]):
                    error_msg += (
                        "\n\nThis often indicates that 'jt400.jar' (IBM Toolbox for Java) is missing "
                        "from the Kafka Connect classpath. Please ensure 'jt400.jar' is installed "
                        "in your Kafka Connect plugin directory and restart Kafka Connect."
                    )
                
                result = {
                    "success": False,
                    "status": "FAILED",
                    "error": error_msg,
                    "response_time_ms": response_time_ms,
                    "tested_at": datetime.utcnow().isoformat()
                }
                
                if save_history and hasattr(connection, 'id') and connection.id:
                    self._save_test_history(
                        connection_id=connection.id,
                        status="FAILED",
                        response_time_ms=response_time_ms,
                        error_message=error_msg
                    )
                    if isinstance(connection, ConnectionModel):
                        connection.last_tested_at = datetime.utcnow()
                        connection.last_test_status = "failed"
                        self.session.commit()
                
                return result
                
        except Exception as e:
            response_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Kafka Connect validation failed: {e}", exc_info=True)
            return {
                "success": False,
                "status": "FAILED",
                "error": f"Kafka Connect validation error: {str(e)}",
                "response_time_ms": response_time_ms,
                "tested_at": datetime.utcnow().isoformat()
            }
    
    def _get_connector_from_data(self, connection: Any):
        """Get connector instance from connection data (Connection or ConnectionModel)."""
        from ingestion.models import Connection as ConnectionModel
        
        # Handle both Connection model and ConnectionModel
        if hasattr(connection, 'database_type'):
            db_type = connection.database_type
            if hasattr(db_type, 'value'):
                db_type = db_type.value
            db_type = str(db_type).lower()
            # Normalize aws_s3 to s3 for compatibility
            if db_type == "aws_s3":
                db_type = "s3"
            # Normalize db2 to as400 (IBM DB2 uses AS400 connector)
            if db_type == "db2":
                db_type = "as400"
        else:
            raise ValueError("Connection must have database_type")
        
        if db_type == "postgresql":
            # PostgreSQL connector expects config dict with 'host', 'user', etc.
            config = {
                "host": connection.host,
                "port": connection.port,
                "database": connection.database,
                "username": connection.username,  # Will be normalized to 'user' by connector
                "password": connection.password,
            }
            if hasattr(connection, 'schema') and connection.schema:
                config["schema"] = connection.schema
            if hasattr(connection, 'additional_config') and connection.additional_config:
                config.update(connection.additional_config)
            return PostgreSQLConnector(config)
        elif db_type in ["sqlserver", "mssql"]:
            # Check if pyodbc is available
            from .connectors.sqlserver import SQLSERVER_AVAILABLE
            if not SQLSERVER_AVAILABLE:
                raise ImportError(
                    "pyodbc is not installed. SQL Server connector requires pyodbc. "
                    "Please install it with: pip install pyodbc>=5.0.0"
                )
            
            # SQL Server connector expects 'server' instead of 'host', and 'user' instead of 'username'
            config = {
                "server": connection.host,  # Map 'host' to 'server'
                "port": connection.port,
                "database": connection.database,
                "username": connection.username,  # Will be normalized to 'user' by connector
                "password": connection.password,
                "trust_server_certificate": True,  # Default to True to avoid SSL certificate issues
            }
            if hasattr(connection, 'schema') and connection.schema:
                config["schema"] = connection.schema
            if hasattr(connection, 'additional_config') and connection.additional_config:
                # Allow additional_config to override defaults (e.g., if user explicitly sets trust_server_certificate=False)
                config.update(connection.additional_config)
            return SQLServerConnector(config)
        elif db_type == "s3":
            # S3 connector expects bucket, aws_access_key_id, aws_secret_access_key
            config = {
                "bucket": connection.database,  # Use database field for bucket name
                "aws_access_key_id": connection.username,  # Use username for AWS access key
                "aws_secret_access_key": connection.password,  # Use password for AWS secret key
            }
            if hasattr(connection, 'schema') and connection.schema:
                config["prefix"] = connection.schema  # Use schema field for S3 prefix
            if hasattr(connection, 'additional_config') and connection.additional_config:
                # Allow additional_config for region_name, endpoint_url, etc.
                config.update(connection.additional_config)
            return S3Connector(config)
        elif db_type == "oracle":
            # Oracle connector expects host, port, database (SID) or service_name, user, password
            from .connectors.oracle import OracleConnector
            config = {
                "host": connection.host,
                "port": connection.port,
                "database": connection.database,  # SID
                "username": connection.username,  # Will be normalized to 'user' by connector
                "password": connection.password,
            }
            if hasattr(connection, 'schema') and connection.schema:
                config["schema"] = connection.schema
            if hasattr(connection, 'additional_config') and connection.additional_config:
                # Allow additional_config for service_name, mode, etc.
                config.update(connection.additional_config)
            return OracleConnector(config)
        elif db_type == "snowflake":
            # Snowflake connector expects account, user, password, database, schema, warehouse, role
            from .connectors.snowflake import SnowflakeConnector
            account = connection.host or (connection.additional_config.get("account") if hasattr(connection, 'additional_config') and connection.additional_config else None)
            if not account:
                raise ValueError("Snowflake account is required. Provide it in 'host' field or 'additional_config.account'")
            
            schema = (connection.schema if hasattr(connection, 'schema') and connection.schema else None) or \
                     (connection.additional_config.get("schema") if hasattr(connection, 'additional_config') and connection.additional_config else None) or \
                     "PUBLIC"
            
            config = {
                "account": account,
                "user": connection.username,
                "password": connection.password,
                "database": connection.database,
                "schema": schema,
            }
            
            # Add optional parameters from additional_config
            if hasattr(connection, 'additional_config') and connection.additional_config:
                if "warehouse" in connection.additional_config:
                    config["warehouse"] = connection.additional_config["warehouse"]
                if "role" in connection.additional_config:
                    config["role"] = connection.additional_config["role"]
                if "private_key" in connection.additional_config:
                    config["private_key"] = connection.additional_config["private_key"]
                if "private_key_passphrase" in connection.additional_config:
                    config["private_key_passphrase"] = connection.additional_config["private_key_passphrase"]
            
            return SnowflakeConnector(config)
        elif db_type in ["as400", "ibm_i", "db2"]:
            # AS400 connector expects 'server' instead of 'host'
            from .connectors.as400 import AS400Connector
            # Check if pyodbc is available
            from .connectors.as400 import AS400_AVAILABLE
            if not AS400_AVAILABLE:
                raise ImportError(
                    "pyodbc is not installed. AS400 connector requires pyodbc. "
                    "Please install it with: pip install pyodbc>=5.0.0"
                )
            
            # Use library from additional_config or schema or database
            library = (
                connection.additional_config.get("library") if hasattr(connection, 'additional_config') and connection.additional_config 
                else connection.schema if hasattr(connection, 'schema') and connection.schema 
                else connection.database if hasattr(connection, 'database') else ""
            )
            
            config = {
                "server": connection.host,  # Map 'host' to 'server'
                "port": connection.port or 446,  # Default AS400 port
                "database": library or connection.database,  # Library name
                "username": connection.username,  # Will be normalized to 'user' by connector
                "password": connection.password,
                "additional_config": {
                    "journal_library": connection.additional_config.get("journal_library", "") if hasattr(connection, 'additional_config') and connection.additional_config else "",
                    "library": library or connection.database or "",
                    "tablename": connection.additional_config.get("tablename", "") if hasattr(connection, 'additional_config') and connection.additional_config else "",
                }
            }
            if hasattr(connection, 'schema') and connection.schema:
                config["schema"] = connection.schema
            if hasattr(connection, 'additional_config') and connection.additional_config:
                # Merge additional_config (driver, etc.) but preserve nested structure
                for key, value in connection.additional_config.items():
                    if key not in ["library", "journal_library", "tablename"]:
                        config[key] = value
            
            return AS400Connector(config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _get_connector(self, connection: ConnectionModel):
        """Get appropriate connector for connection type.
        
        Args:
            connection: Connection model
            
        Returns:
            Database connector instance
        """
        # Get database type value (handle enum)
        db_type = connection.database_type
        if hasattr(db_type, 'value'):
            db_type = db_type.value
        db_type = str(db_type).lower()
        
        # Normalize aws_s3 to s3 for compatibility
        if db_type == "aws_s3":
            db_type = "s3"
        # Normalize db2 to as400 (IBM DB2 uses AS400 connector)
        if db_type == "db2":
            db_type = "as400"
        
        if db_type == "postgresql":
            config = {
                "host": connection.host,
                "port": connection.port,
                "database": connection.database,
                "username": connection.username,
                "password": connection.password
            }
            if connection.schema:
                config["schema"] = connection.schema
            if connection.additional_config:
                config.update(connection.additional_config)
            return PostgreSQLConnector(config)
        elif db_type in ["sqlserver", "mssql"]:
            # Check if pyodbc is available
            from .connectors.sqlserver import SQLSERVER_AVAILABLE
            if not SQLSERVER_AVAILABLE:
                raise ImportError(
                    "pyodbc is not installed. SQL Server connector requires pyodbc. "
                    "Please install it with: pip install pyodbc>=5.0.0"
                )
            
            # SQL Server connector expects 'server' instead of 'host'
            config = {
                "server": connection.host,  # Map 'host' to 'server'
                "port": connection.port,
                "database": connection.database,
                "username": connection.username,  # Will be normalized to 'user' by connector
                "password": connection.password,
                "trust_server_certificate": True,  # Default to True to avoid SSL certificate issues
            }
            if connection.schema:
                config["schema"] = connection.schema
            if connection.additional_config:
                # Allow additional_config to override defaults (e.g., if user explicitly sets trust_server_certificate=False)
                # Also allow specifying 'driver' explicitly if auto-detection fails
                config.update(connection.additional_config)
            return SQLServerConnector(config)
        elif db_type == "s3":
            # S3 connector expects bucket, aws_access_key_id, aws_secret_access_key
            config = {
                "bucket": connection.database,  # Use database field for bucket name
                "aws_access_key_id": connection.username,  # Use username for AWS access key
                "aws_secret_access_key": connection.password,  # Use password for AWS secret key
            }
            if hasattr(connection, 'schema') and connection.schema:
                config["prefix"] = connection.schema  # Use schema field for S3 prefix
            if hasattr(connection, 'additional_config') and connection.additional_config:
                # Allow additional_config for region_name, endpoint_url, etc.
                config.update(connection.additional_config)
            return S3Connector(config)
        elif db_type in ["as400", "ibm_i", "db2"]:
            # AS400 connector expects 'server' instead of 'host'
            # Use library from additional_config or schema or database
            library = (
                connection.additional_config.get("library") if connection.additional_config 
                else connection.schema or connection.database or ""
            )
            
            config = {
                "server": connection.host,  # Map 'host' to 'server'
                "port": connection.port or 446,  # Default AS400 port
                "database": library,  # Library name (can be empty, will use additional_config)
                "username": connection.username,
                "password": connection.password,
                "additional_config": {
                    "journal_library": connection.additional_config.get("journal_library", "") if connection.additional_config else "",
                    "library": library,
                    "tablename": connection.additional_config.get("tablename", "") if connection.additional_config else "",
                }
            }
            if connection.schema:
                config["schema"] = connection.schema
            if connection.additional_config:
                # Merge additional_config (driver, etc.)
                config["additional_config"].update({
                    k: v for k, v in connection.additional_config.items() 
                    if k not in ["journal_library", "library", "tablename"]
                })
            from ingestion.connectors.as400 import AS400Connector
            return AS400Connector(config)
        elif db_type == "snowflake":
            # Snowflake connector expects account, user, password, database, schema, warehouse, role
            # Account can be provided in 'host' field or 'additional_config.account'
            account = connection.host or (connection.additional_config.get("account") if connection.additional_config else None)
            if not account:
                raise ValueError("Snowflake account is required. Provide it in 'host' field or 'additional_config.account'")
            
            # Schema can be from connection.schema or additional_config.schema
            schema = connection.schema or (connection.additional_config.get("schema") if connection.additional_config else None) or "PUBLIC"
            
            config = {
                "account": account,
                "user": connection.username,
                "password": connection.password,
                "database": connection.database,
                "schema": schema,
            }
            
            # Add optional parameters from additional_config
            if connection.additional_config:
                if "warehouse" in connection.additional_config:
                    config["warehouse"] = connection.additional_config["warehouse"]
                if "role" in connection.additional_config:
                    config["role"] = connection.additional_config["role"]
                if "private_key" in connection.additional_config:
                    config["private_key"] = connection.additional_config["private_key"]
                if "private_key_passphrase" in connection.additional_config:
                    config["private_key_passphrase"] = connection.additional_config["private_key_passphrase"]
            
            from ingestion.connectors.snowflake import SnowflakeConnector
            return SnowflakeConnector(config)
        elif db_type == "oracle":
            # Oracle connector expects host, port, database (SID) or service_name, user, password
            config = {
                "host": connection.host,
                "port": connection.port,
                "user": connection.username,
                "password": connection.password,
            }
            # Oracle connector accepts 'database' for SID or 'service_name' for service name
            # Check additional_config first for explicit service_name or sid
            if connection.additional_config:
                if 'service_name' in connection.additional_config:
                    config["service_name"] = connection.additional_config['service_name']
                elif 'sid' in connection.additional_config:
                    config["database"] = connection.additional_config['sid']
                # Add other additional_config items
                for key, value in connection.additional_config.items():
                    if key not in ['service_name', 'sid']:
                        config[key] = value
            
            # If not set from additional_config, use connection.database
            if 'service_name' not in config and 'database' not in config:
                if connection.database:
                    # Check if it looks like a service name (contains /) or is a SID
                    if '/' in connection.database:
                        config["service_name"] = connection.database
                    else:
                        config["database"] = connection.database  # Use 'database' for SID
            
            if connection.schema:
                config["schema"] = connection.schema
            return OracleConnector(config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _save_test_history(
        self,
        connection_id: str,
        status: str,
        response_time_ms: int,
        error_message: Optional[str] = None
    ):
        """Save connection test result to history.
        
        Args:
            connection_id: Connection ID
            status: Test status
            response_time_ms: Response time in milliseconds
            error_message: Error message if failed
        """
        try:
            test = ConnectionTestModel(
                id=str(uuid.uuid4()),
                connection_id=connection_id,
                test_status=status,
                response_time_ms=response_time_ms,
                error_message=error_message,
                tested_at=datetime.utcnow()
            )
            
            self.session.add(test)
            self.session.commit()
            
        except Exception as e:
            logger.error(f"Error saving test history: {e}", exc_info=True)
            self.session.rollback()
    
    def close(self):
        """Close database session."""
        self.session.close()

