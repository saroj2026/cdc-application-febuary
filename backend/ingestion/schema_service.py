"""Schema service for creating and managing target database schemas and tables."""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from ingestion.connection_service import ConnectionService
from ingestion.connectors.postgresql import PostgreSQLConnector
from ingestion.connectors.sqlserver import SQLServerConnector
from ingestion.connectors.snowflake import SnowflakeConnector
from ingestion.models import Connection

logger = logging.getLogger(__name__)


class SchemaService:
    """Service for managing target database schemas and tables."""
    
    def __init__(self):
        """Initialize schema service."""
        self.connection_service = ConnectionService()
    
    def create_target_schema(
        self,
        connection_id: str,
        schema_name: str,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create schema in target database.
        
        Args:
            connection_id: Connection ID
            schema_name: Schema name to create
            database: Database name (optional)
            
        Returns:
            Creation result
        """
        try:
            from ingestion.database.models_db import ConnectionModel
            connection = self.connection_service.session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {
                    "success": False,
                    "error": f"Connection not found: {connection_id}"
                }
            
            # Get database type (handle enum)
            db_type = connection.database_type
            if hasattr(db_type, 'value'):
                db_type = db_type.value
            db_type = str(db_type).lower()
            
            # Only create connector for supported database types (not S3)
            if db_type == "s3":
                return {
                    "success": False,
                    "error": f"Cannot create schema for S3 connection. S3 does not support schemas."
                }
            
            # Validate database type before creating connector
            if db_type not in ["postgresql", "sqlserver", "mssql", "snowflake"]:
                return {
                    "success": False,
                    "error": f"Unsupported database type for schema creation: {db_type}. Only PostgreSQL, SQL Server, and Snowflake are supported."
                }
            
            try:
                connector = self.connection_service._get_connector(connection)
            except ImportError as e:
                # Catch boto3 import errors (shouldn't happen for SQL Server/PostgreSQL)
                error_msg = str(e)
                if "boto3" in error_msg.lower():
                    return {
                        "success": False,
                        "error": f"Connection type mismatch: Attempted to use S3 connector for {db_type} connection. Please check the connection's database_type setting."
                    }
                raise  # Re-raise if it's a different ImportError
            
            # For Snowflake, use the database from connection config if not provided
            # Snowflake connector already has the database in its config
            if db_type == "snowflake":
                # Use database parameter if provided, otherwise use connection's database
                # Strip any quotes that might have been added
                target_db = (database or connection.database or "").strip('"\'')
                # If still empty, get from connector config
                if not target_db and hasattr(connector, 'config'):
                    target_db = connector.config.get('database', '').strip('"\'')
            else:
                target_db = database or connection.database
            
            if db_type == "postgresql":
                return self._create_postgresql_schema(connector, schema_name, target_db)
            elif db_type in ["sqlserver", "mssql"]:
                return self._create_sqlserver_schema(connector, schema_name, target_db)
            elif db_type == "snowflake":
                return self._create_snowflake_schema(connector, schema_name, target_db)
            elif db_type == "oracle":
                # Oracle doesn't use schemas the same way - schemas are users
                # For Oracle, we just verify the schema/user exists
                return {
                    "success": True,
                    "message": f"Oracle schema/user '{schema_name}' verification (Oracle uses users as schemas)",
                    "created": False
                }
            else:
                return {
                    "success": False,
                    "error": f"Unsupported database type: {db_type}"
                }
                
        except Exception as e:
            logger.error(f"Error creating schema: {e}", exc_info=True)
            error_msg = str(e)
            # Provide more helpful error message for boto3 issues
            if "boto3" in error_msg.lower():
                return {
                    "success": False,
                    "error": f"Connection configuration error: {error_msg}. This usually means the target connection's database_type is incorrectly set to 's3' instead of the actual database type (e.g., 'sqlserver' or 'postgresql')."
                }
            return {
                "success": False,
                "error": error_msg
            }
    
    def _create_postgresql_schema(
        self,
        connector: PostgreSQLConnector,
        schema_name: str,
        database: str
    ) -> Dict[str, Any]:
        """Create PostgreSQL schema."""
        try:
            conn = connector.connect()
            cursor = conn.cursor()
            
            # Check if schema exists
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (schema_name,))
            exists = cursor.fetchone() is not None
            
            if exists:
                return {
                    "success": True,
                    "message": f"Schema '{schema_name}' already exists",
                    "created": False
                }
            
            # Create schema
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            conn.commit()
            
            cursor.close()
            conn.close()
            
            return {
                "success": True,
                "message": f"Schema '{schema_name}' created successfully",
                "created": True
            }
            
        except Exception as e:
            logger.error(f"Error creating PostgreSQL schema: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _create_sqlserver_schema(
        self,
        connector: SQLServerConnector,
        schema_name: str,
        database: str
    ) -> Dict[str, Any]:
        """Create SQL Server schema."""
        try:
            conn = connector.connect()
            cursor = conn.cursor()
            
            cursor.execute(f"USE [{database}]")
            
            # Check if schema exists
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = ?
            """, schema_name)
            exists = cursor.fetchone() is not None
            
            if exists:
                return {
                    "success": True,
                    "message": f"Schema '{schema_name}' already exists",
                    "created": False
                }
            
            # Create schema
            cursor.execute(f"CREATE SCHEMA [{schema_name}]")
            conn.commit()
            
            cursor.close()
            conn.close()
            
            return {
                "success": True,
                "message": f"Schema '{schema_name}' created successfully",
                "created": True
            }
            
        except Exception as e:
            logger.error(f"Error creating SQL Server schema: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _create_snowflake_schema(
        self,
        connector: Any,  # SnowflakeConnector
        schema_name: str,
        database: str
    ) -> Dict[str, Any]:
        """Create Snowflake schema."""
        conn = None
        cursor = None
        try:
            conn = connector.connect()
            cursor = conn.cursor()
            
            # Strip any existing quotes from database and schema names
            # Snowflake identifiers are case-insensitive unless quoted
            db_name = database.strip('"\'') if database else ""
            schema_name_clean = schema_name.strip('"\'')
            
            # If database is not provided or empty, use the one from connector config
            if not db_name and hasattr(connector, 'config'):
                db_name = connector.config.get('database', '').strip('"\'')
            
            if not db_name:
                return {
                    "success": False,
                    "error": "Database name is required for Snowflake schema creation"
                }
            
            # First, list available databases to find the correct case
            existing_databases = []
            try:
                cursor.execute("SHOW DATABASES")
                existing_databases = [row[1] for row in cursor.fetchall()]  # Second column is database name
                logger.info(f"Available databases: {existing_databases}")
            except Exception as list_db_error:
                logger.warning(f"Could not list databases: {list_db_error}")
            
            # Determine which database name to use (handle case sensitivity)
            db_name_upper = db_name.upper()
            db_name_to_use = db_name_upper  # Default to uppercase
            
            if existing_databases:
                # Check for exact match (case-sensitive)
                if db_name in existing_databases:
                    db_name_to_use = db_name  # Use original case
                elif db_name_upper in existing_databases:
                    db_name_to_use = db_name_upper  # Use uppercase
                else:
                    # Try case-insensitive match
                    db_match = next((db for db in existing_databases if db.upper() == db_name_upper), None)
                    if db_match:
                        db_name_to_use = db_match
                    else:
                        return {
                            "success": False,
                            "error": f"Database '{db_name}' (or '{db_name_upper}') does not exist in Snowflake. Available databases: {', '.join(existing_databases[:10])} (showing first 10). Please verify the database name in your Snowflake connection configuration."
                        }
            
            # Use uppercase for schema (Snowflake standard)
            schema_name_upper = schema_name_clean.upper()
            
            # First, ensure we're using the correct database context
            db_switched = False
            for db_name_attempt in [db_name_to_use, db_name_upper, db_name]:
                try:
                    cursor.execute(f"USE DATABASE {db_name_attempt}")
                    logger.info(f"Switched to database context: {db_name_attempt}")
                    db_name_to_use = db_name_attempt
                    db_switched = True
                    break
                except Exception as use_db_error:
                    error_msg = str(use_db_error)
                    if "does not exist" in error_msg.lower():
                        continue  # Try next attempt
                    elif "not authorized" in error_msg.lower():
                        return {
                            "success": False,
                            "error": f"You don't have permission to access database '{db_name}'. Please check your Snowflake role and permissions."
                        }
                    else:
                        continue  # Try next attempt
            
            if not db_switched:
                error_details = f"Available databases: {', '.join(existing_databases[:10])}" if existing_databases else "Could not list databases"
                return {
                    "success": False,
                    "error": f"Database '{db_name}' does not exist or you don't have permission to access it. {error_details}. Please verify the database name in your Snowflake connection configuration."
                }
            
            # Check if schema exists - use SHOW SCHEMAS command which is more reliable
            try:
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                existing_schemas = [row[1] for row in cursor.fetchall()]  # Second column is schema name
                exists = schema_name_upper in existing_schemas
            except Exception as show_error:
                # Fallback to INFORMATION_SCHEMA query - use f-string to avoid parameterized query issues
                logger.warning(f"SHOW SCHEMAS failed, using INFORMATION_SCHEMA: {show_error}")
                # Escape single quotes in schema name to prevent SQL injection
                schema_escaped = schema_name_upper.replace("'", "''")
                cursor.execute(f"""
                    SELECT schema_name 
                    FROM INFORMATION_SCHEMA.SCHEMATA 
                    WHERE schema_name = '{schema_escaped}'
                """)
                exists = cursor.fetchone() is not None
            
            if exists:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
                return {
                    "success": True,
                    "message": f"Schema '{schema_name_upper}' already exists in database '{db_name_to_use}'",
                    "created": False
                }
            
            # Create schema - use simple name since we're already in the database context
            # In Snowflake, when you're in a database context, you can create schema with just the name
            create_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name_upper}"
            logger.info(f"Creating Snowflake schema with SQL: {create_sql} in database context: {db_name_to_use}")
            
            try:
                cursor.execute(create_sql)
                # Snowflake auto-commits DDL statements, but let's verify
                logger.info(f"Schema creation command executed successfully")
            except Exception as create_error:
                error_msg = str(create_error)
                logger.error(f"Failed to execute CREATE SCHEMA: {error_msg}")
                # Check if it's a permission error
                if "not authorized" in error_msg.lower() or "permission" in error_msg.lower():
                    return {
                        "success": False,
                        "error": f"Permission denied. You may not have permission to create schemas in database '{db_name_to_use}'. Please check your Snowflake role and permissions. Error: {error_msg}"
                    }
                return {
                    "success": False,
                    "error": f"Failed to create schema '{schema_name_upper}': {error_msg}"
                }
            
            # Verify schema was created - wait a moment and check again
            import time
            time.sleep(0.5)  # Brief delay to ensure schema is visible
            
            # Verify schema was created
            try:
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                existing_schemas = [row[1] for row in cursor.fetchall()]
                logger.info(f"Schemas in database {db_name_to_use} after creation: {existing_schemas}")
                
                if schema_name_upper not in existing_schemas:
                    # Try one more time with a longer delay
                    logger.warning(f"Schema {schema_name_upper} not found immediately after creation, waiting and retrying...")
                    time.sleep(1)
                    cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                    existing_schemas = [row[1] for row in cursor.fetchall()]
                    
                    if schema_name_upper not in existing_schemas:
                        return {
                            "success": False,
                            "error": f"Schema '{schema_name_upper}' was not created successfully in database '{db_name_to_use}'. Available schemas: {', '.join(existing_schemas[:10])}. This might be a permission issue or the schema name might be invalid."
                        }
            except Exception as verify_error:
                logger.error(f"Error verifying schema creation: {verify_error}")
                # Don't fail here - schema might have been created but we can't verify
                logger.warning(f"Could not verify schema creation, but assuming it succeeded: {verify_error}")
            
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            
            logger.info(f"✓ Schema '{schema_name_upper}' created/verified successfully in database '{db_name_to_use}'")
            return {
                "success": True,
                "message": f"Schema '{schema_name_upper}' created successfully in database '{db_name_to_use}'",
                "created": True
            }
        except Exception as e:
            logger.error(f"Error creating Snowflake schema: {e}", exc_info=True)
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
            return {
                "success": False,
                "error": str(e)
            }
    
    def create_target_table(
        self,
        source_connection_id: str,
        target_connection_id: str,
        table_name: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        target_table_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create table in target database with schema from source.
        
        Args:
            source_connection_id: Source connection ID
            target_connection_id: Target connection ID
            table_name: Source table name
            source_database: Source database (optional)
            source_schema: Source schema (optional)
            target_database: Target database (optional)
            target_schema: Target schema (optional)
            target_table_name: Target table name (optional, defaults to source name)
            
        Returns:
            Creation result
        """
        try:
            # Get source table schema
            schema_info = self.connection_service.get_table_schema(
                source_connection_id,
                table_name,
                database=source_database,
                schema=source_schema
            )
            
            if not schema_info.get("success"):
                return {
                    "success": False,
                    "error": f"Failed to get source table schema: {schema_info.get('error')}"
                }
            
            source_columns = schema_info.get("columns", [])
            if not source_columns:
                return {
                    "success": False,
                    "error": f"No columns found for table {table_name}"
                }
            
            # Get connections
            from ingestion.database.models_db import ConnectionModel
            source_conn = self.connection_service.session.query(ConnectionModel).filter_by(
                id=source_connection_id
            ).first()
            
            target_conn = self.connection_service.session.query(ConnectionModel).filter_by(
                id=target_connection_id
            ).first()
            
            if not source_conn or not target_conn:
                return {
                    "success": False,
                    "error": "Source or target connection not found"
                }
            
            # Get connectors
            source_config = {
                "host": source_conn.host,
                "port": source_conn.port,
                "database": source_conn.database,
                "user": source_conn.username,
                "password": source_conn.password
            }
            if source_conn.schema:
                source_config["schema"] = source_conn.schema
            
            target_config = {
                "host": target_conn.host,
                "port": target_conn.port,
                "database": target_conn.database,
                "user": target_conn.username,
                "password": target_conn.password
            }
            if target_conn.schema:
                target_config["schema"] = target_conn.schema
            
            if source_conn.database_type == "postgresql":
                source_connector = PostgreSQLConnector(source_config)
            elif source_conn.database_type == "sqlserver":
                source_connector = SQLServerConnector(source_config)
            elif source_conn.database_type == "oracle":
                from ingestion.connectors.oracle import OracleConnector
                # Oracle connector expects 'database' (for SID) or 'service_name'
                # Keep 'database' key - Oracle connector will use it as SID
                # If service_name is in additional_config, use that instead
                if hasattr(source_conn, 'additional_config') and source_conn.additional_config:
                    if 'service_name' in source_conn.additional_config:
                        # If service_name is provided, use it and remove database
                        if 'database' in source_config:
                            source_config.pop('database')
                        source_config["service_name"] = source_conn.additional_config['service_name']
                    # If database is empty/None, try to get from additional_config
                    elif not source_config.get('database'):
                        if 'database' in source_conn.additional_config:
                            source_config["database"] = source_conn.additional_config['database']
                        elif 'sid' in source_conn.additional_config:
                            source_config["database"] = source_conn.additional_config['sid']
                # Ensure database is set - use connection.database if not already set
                if not source_config.get('database') and not source_config.get('service_name'):
                    if source_conn.database:
                        source_config["database"] = source_conn.database
                source_connector = OracleConnector(source_config)
            else:
                return {"success": False, "error": f"Unsupported source type: {source_conn.database_type}"}
            
            # Handle Snowflake separately since DataTransfer doesn't support it
            if target_conn.database_type == "snowflake":
                return self._create_snowflake_table(
                    source_connection_id=source_connection_id,
                    target_connection_id=target_connection_id,
                    table_name=table_name,
                    source_columns=source_columns,
                    source_database=source_database,
                    source_schema=source_schema,
                    target_database=target_database,
                    target_schema=target_schema,
                    target_table_name=target_table_name
                )
            
            if target_conn.database_type == "postgresql":
                target_connector = PostgreSQLConnector(target_config)
            elif target_conn.database_type == "sqlserver":
                target_connector = SQLServerConnector(target_config)
            else:
                return {"success": False, "error": f"Unsupported target type: {target_conn.database_type}"}
            
            # Use DataTransfer to create table
            from ingestion.transfer import DataTransfer
            transfer = DataTransfer(source_connector, target_connector)
            
            target_table = target_table_name or table_name
            target_db = target_database or target_conn.database
            target_sch = target_schema or target_conn.schema or ("public" if target_conn.database_type == "postgresql" else "dbo")
            
            # Create table with schema only
            transfer.transfer_tables(
                tables=[table_name],
                source_database=source_database or source_conn.database,
                source_schema=source_schema or source_conn.schema or "public",
                target_database=target_db,
                target_schema=target_sch,
                transfer_schema=True,
                transfer_data=False,  # Only create schema, no data
                batch_size=1000
            )
            
            return {
                "success": True,
                "message": f"Table '{target_table}' created successfully in {target_db}.{target_sch}",
                "table_name": target_table,
                "database": target_db,
                "schema": target_sch,
                "column_count": len(source_columns)
            }
            
        except Exception as e:
            logger.error(f"Error creating target table: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def _create_snowflake_table(
        self,
        source_connection_id: str,
        target_connection_id: str,
        table_name: str,
        source_columns: List[Dict[str, Any]],
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        target_table_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create table in Snowflake with schema from source.
        
        Args:
            source_connection_id: Source connection ID
            target_connection_id: Target connection ID
            table_name: Source table name
            source_columns: List of column definitions from source
            source_database: Source database (optional)
            source_schema: Source schema (optional)
            target_database: Target database (optional)
            target_schema: Target schema (optional)
            target_table_name: Target table name (optional)
            
        Returns:
            Creation result
        """
        conn = None
        cursor = None
        try:
            # Get target connection and create connector
            from ingestion.database.models_db import ConnectionModel
            target_conn = self.connection_service.session.query(ConnectionModel).filter_by(
                id=target_connection_id
            ).first()
            
            if not target_conn:
                return {
                    "success": False,
                    "error": "Target connection not found"
                }
            
            # Get Snowflake connector
            connector = self.connection_service._get_connector(target_conn)
            
            # Get target database and schema
            target_db = (target_database or target_conn.database or "").strip('"\'')
            if not target_db and hasattr(connector, 'config'):
                target_db = connector.config.get('database', '').strip('"\'')
            
            target_sch = (target_schema or target_conn.schema or "").strip('"\'')
            target_table = (target_table_name or table_name).strip('"\'')
            
            if not target_db:
                return {
                    "success": False,
                    "error": "Target database is required for Snowflake table creation"
                }
            
            # Connect to Snowflake
            conn = connector.connect()
            cursor = conn.cursor()
            
            # Ensure warehouse is set (required for some operations, though DDL usually works without it)
            # But it's good practice to have it set
            try:
                cursor.execute("SELECT CURRENT_WAREHOUSE()")
                current_warehouse = cursor.fetchone()[0]
                if not current_warehouse:
                    # Try to set warehouse from connector config
                    if hasattr(connector, 'config') and connector.config.get('warehouse'):
                        cursor.execute(f"USE WAREHOUSE {connector.config['warehouse']}")
                        logger.info(f"Set warehouse to: {connector.config['warehouse']}")
                    else:
                        logger.warning("No warehouse set - this might cause issues with some operations")
                else:
                    logger.info(f"Current warehouse: {current_warehouse}")
            except Exception as warehouse_error:
                logger.warning(f"Could not check/set warehouse: {warehouse_error}")
            
            # First, verify database exists by listing databases
            existing_databases = []
            try:
                cursor.execute("SHOW DATABASES")
                existing_databases = [row[1] for row in cursor.fetchall()]  # Second column is database name
                logger.info(f"Available databases: {existing_databases}")
            except Exception as list_db_error:
                logger.warning(f"Could not list databases: {list_db_error}")
            
            # Try to use database - Snowflake converts unquoted identifiers to uppercase
            # But if database was created with quotes, it's case-sensitive
            target_db_upper = target_db.upper()
            db_name_to_use = target_db_upper  # Default to uppercase
            
            # Check if database exists (case-insensitive check)
            if existing_databases:
                # Check for exact match (case-sensitive)
                if target_db in existing_databases:
                    db_name_to_use = target_db  # Use original case
                elif target_db_upper in existing_databases:
                    db_name_to_use = target_db_upper  # Use uppercase
                else:
                    # Try case-insensitive match
                    db_match = next((db for db in existing_databases if db.upper() == target_db_upper), None)
                    if db_match:
                        db_name_to_use = db_match
                    else:
                        return {
                            "success": False,
                            "error": f"Database '{target_db}' (or '{target_db_upper}') does not exist in Snowflake. Available databases: {', '.join(existing_databases[:10])} (showing first 10). Please verify the database name in your Snowflake connection configuration."
                        }
            
            # Set database context - try uppercase first, then original case if that fails
            db_switched = False
            for db_name_attempt in [db_name_to_use, target_db_upper, target_db]:
                try:
                    cursor.execute(f"USE DATABASE {db_name_attempt}")
                    logger.info(f"Switched to database: {db_name_attempt}")
                    db_name_to_use = db_name_attempt
                    db_switched = True
                    break
                except Exception as use_db_error:
                    error_msg = str(use_db_error)
                    if "does not exist" in error_msg.lower():
                        continue  # Try next attempt
                    elif "not authorized" in error_msg.lower():
                        return {
                            "success": False,
                            "error": f"You don't have permission to access database '{target_db}'. Please check your Snowflake role and permissions."
                        }
                    else:
                        # Other error, try next attempt
                        continue
            
            if not db_switched:
                error_details = f"Available databases: {', '.join(existing_databases[:10])}" if existing_databases else "Could not list databases"
                return {
                    "success": False,
                    "error": f"Database '{target_db}' does not exist or you don't have permission to access it. {error_details}. Please verify the database name in your Snowflake connection configuration."
                }
            
            # Use uppercase for schema and table (Snowflake standard)
            target_sch_upper = target_sch.upper() if target_sch else "PUBLIC"
            target_table_upper = target_table.upper()
            
            # Ensure schema exists before using it
            if target_sch_upper:
                # Check if schema exists and create if needed
                try:
                    cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                    existing_schemas = [row[1] for row in cursor.fetchall()]  # Second column is schema name
                    logger.info(f"Existing schemas in {db_name_to_use}: {existing_schemas}")
                    
                    if target_sch_upper not in existing_schemas:
                        # Create schema if it doesn't exist
                        logger.info(f"Schema {target_sch_upper} does not exist, creating it...")
                        try:
                            # Since we're already in the database context, use simple schema name
                            # Snowflake will create it in the current database
                            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_sch_upper}")
                            logger.info(f"✓ Created schema {target_sch_upper} in database {db_name_to_use}")
                            
                            # Wait a moment to ensure schema is visible (Snowflake DDL is eventually consistent)
                            import time
                            time.sleep(0.5)
                            
                            # Verify it was created - refresh the schema list
                            cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                            existing_schemas = [row[1] for row in cursor.fetchall()]
                            logger.info(f"Schemas after creation attempt: {existing_schemas}")
                            
                            if target_sch_upper not in existing_schemas:
                                # Try one more time with fully qualified name as fallback
                                logger.warning(f"Schema not found after creation, trying fully qualified name...")
                                try:
                                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name_to_use}.{target_sch_upper}")
                                    time.sleep(0.5)  # Wait again
                                    cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                                    existing_schemas = [row[1] for row in cursor.fetchall()]
                                    if target_sch_upper not in existing_schemas:
                                        return {
                                            "success": False,
                                            "error": f"Schema '{target_sch_upper}' was not created successfully in database '{db_name_to_use}'. Available schemas: {', '.join(existing_schemas[:10])}. This might be a permission issue. Please verify you have CREATE SCHEMA privileges in database '{db_name_to_use}'."
                                        }
                                except Exception as fallback_error:
                                    error_msg = str(fallback_error)
                                    logger.error(f"Fallback schema creation failed: {error_msg}")
                                    return {
                                        "success": False,
                                        "error": f"Schema '{target_sch_upper}' was not created successfully. Error: {error_msg}. Available schemas: {', '.join(existing_schemas[:10])}. Please verify you have CREATE SCHEMA privileges."
                                    }
                            else:
                                logger.info(f"✓ Schema {target_sch_upper} verified to exist")
                        except Exception as create_schema_error:
                            error_msg = str(create_schema_error)
                            logger.error(f"Failed to create schema {target_sch_upper}: {error_msg}")
                            # Check if it's a permission error
                            if "not authorized" in error_msg.lower() or "permission" in error_msg.lower():
                                return {
                                    "success": False,
                                    "error": f"Permission denied. You may not have permission to create schemas in database '{db_name_to_use}'. Please check your Snowflake role and permissions. Error: {error_msg}"
                                }
                            return {
                                "success": False,
                                "error": f"Failed to create schema '{target_sch_upper}': {error_msg}"
                            }
                    else:
                        logger.info(f"Schema {target_sch_upper} already exists")
                    
                    # Now use the schema - try simple name first
                    try:
                        cursor.execute(f"USE SCHEMA {target_sch_upper}")
                        logger.info(f"Switched to schema: {target_sch_upper}")
                    except Exception as use_schema_error:
                        # If USE SCHEMA fails, try with fully qualified name
                        logger.warning(f"USE SCHEMA failed with simple name, trying fully qualified: {use_schema_error}")
                        try:
                            cursor.execute(f"USE SCHEMA {db_name_to_use}.{target_sch_upper}")
                            logger.info(f"Switched to schema using fully qualified name: {db_name_to_use}.{target_sch_upper}")
                        except Exception as use_schema_error2:
                            error_msg = str(use_schema_error2)
                            logger.error(f"Failed to use schema {target_sch_upper}: {error_msg}")
                            return {
                                "success": False,
                                "error": f"Failed to use schema '{target_sch_upper}': {error_msg}"
                            }
                except Exception as schema_check_error:
                    error_msg = str(schema_check_error)
                    logger.error(f"Error checking/creating schema: {error_msg}")
                    return {
                        "success": False,
                        "error": f"Error with schema '{target_sch_upper}': {error_msg}"
                    }
            
            # Check if table exists
            try:
                cursor.execute(f"SHOW TABLES LIKE '{target_table_upper}' IN SCHEMA {db_name_to_use}.{target_sch_upper}")
                exists = len(cursor.fetchall()) > 0
            except:
                # Fallback to INFORMATION_SCHEMA
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{target_sch_upper}' AND TABLE_NAME = '{target_table_upper}'
                """)
                exists = cursor.fetchone()[0] > 0
            
            if exists:
                logger.info(f"Table {db_name_to_use}.{target_sch_upper}.{target_table_upper} already exists")
                return {
                    "success": True,
                    "message": f"Table '{target_table_upper}' already exists in {db_name_to_use}.{target_sch_upper}",
                    "table_name": target_table_upper,
                    "database": db_name_to_use,
                    "schema": target_sch_upper,
                    "column_count": len(source_columns)
                }
            
            # Validate we have source columns
            if not source_columns:
                return {
                    "success": False,
                    "error": f"No columns found for source table '{table_name}'. Cannot create target table without column definitions."
                }
            
            logger.info(f"Building CREATE TABLE statement for Snowflake with RECORD_CONTENT/RECORD_METADATA format")
            
            # For Snowflake targets, create tables with RECORD_CONTENT (VARIANT) and RECORD_METADATA (VARIANT)
            # This matches the format that Snowflake Kafka connector uses, ensuring compatibility with CDC
            # CRITICAL: Both columns must be VARIANT (not OBJECT) for Snowflake Kafka connector compatibility
            # Build column definitions for Snowflake Kafka connector format
            column_defs = [
                '"RECORD_CONTENT" VARIANT',  # Stores the actual record data as JSON
                '"RECORD_METADATA" VARIANT'  # Stores metadata like source, timestamp, partition, offset, operation
            ]
            
            # Also preserve source column names as computed columns or metadata for reference
            # But the primary storage is in RECORD_CONTENT VARIANT column
            # Store original column information in a comment or as computed columns for easy access
            # For now, we'll just use RECORD_CONTENT and RECORD_METADATA as per Snowflake connector format
            
            # Log source columns for reference (not used in table schema, but stored in RECORD_CONTENT)
            logger.info(f"Source columns (will be stored in RECORD_CONTENT as JSON): {[col.get('name') if isinstance(col, dict) else getattr(col, 'name', '') for col in source_columns[:5]]}")
            
            # Original column building code commented out - not needed for Snowflake Kafka connector format
            """
            # Build column definitions with type mapping
            column_defs = []
            for col in source_columns:
                # Handle both dict format and object format
                if isinstance(col, dict):
                    col_name = col.get("name", "").upper()
                    data_type = col.get("data_type") or "VARCHAR"
                    is_nullable = col.get("is_nullable", True)
                    json_schema = col.get("json_schema", {})
                    # Also check for direct fields that might be in the dict
                    max_length = col.get("character_maximum_length") or col.get("max_length")
                    precision = col.get("numeric_precision") or col.get("precision")
                    scale = col.get("numeric_scale") or col.get("scale")
                else:
                    # Handle object format (ConnectorColumn)
                    col_name = (getattr(col, "name", "") or "").upper()
                    data_type = (getattr(col, "data_type", None) or "VARCHAR")
                    is_nullable = getattr(col, "is_nullable", True)
                    json_schema = getattr(col, "json_schema", {}) or {}
                    max_length = getattr(col, "max_length", None)
                    precision = getattr(col, "precision", None)
                    scale = getattr(col, "scale", None)
                
                # Ensure data_type is a string
                if not isinstance(data_type, str):
                    data_type = str(data_type) if data_type else "VARCHAR"
                
                if not col_name:
                    logger.warning(f"Skipping column with no name: {col}")
                    continue
                
                # Validate column name is a valid Snowflake identifier
                # Remove any invalid characters (keep only alphanumeric and underscore)
                import re
                # Snowflake identifiers can contain letters, numbers, underscore, and dollar sign
                # But we'll sanitize to be safe
                if not re.match(r'^[A-Za-z_][A-Za-z0-9_$]*$', col_name):
                    # Replace invalid characters with underscore
                    sanitized_name = re.sub(r'[^A-Za-z0-9_$]', '_', col_name)
                    # Ensure it starts with letter or underscore
                    if not re.match(r'^[A-Za-z_]', sanitized_name):
                        sanitized_name = '_' + sanitized_name
                    logger.warning(f"Column name '{col_name}' contains invalid characters, sanitizing to '{sanitized_name}'")
                    col_name = sanitized_name
                
                # Extract length/precision from json_schema or data_type string (if not already extracted)
                if max_length is None:
                    max_length = json_schema.get("max_length") if json_schema else None
                if precision is None:
                    precision = json_schema.get("precision") if json_schema else None
                if scale is None:
                    scale = json_schema.get("scale") if json_schema else None
                
                # Parse from data_type string if needed (e.g., "varchar(100)")
                if max_length is None and "(" in data_type:
                    try:
                        length_part = data_type.split("(")[1].split(")")[0]
                        if "," in length_part:
                            parts = length_part.split(",")
                            precision = int(parts[0].strip())
                            scale = int(parts[1].strip()) if len(parts) > 1 else None
                        else:
                            max_length = int(length_part.strip())
                    except (ValueError, IndexError):
                        pass
                
                # Map data types to Snowflake types
                base_type = data_type.lower().split("(")[0].strip()
                
                # Type mapping from common database types to Snowflake
                type_mapping = {
                    # Integer types
                    "int": "INTEGER",
                    "integer": "INTEGER",
                    "bigint": "BIGINT",
                    "smallint": "SMALLINT",
                    "tinyint": "TINYINT",
                    # Decimal types
                    "decimal": "NUMBER",
                    "numeric": "NUMBER",
                    "money": "NUMBER(19,4)",
                    "smallmoney": "NUMBER(10,4)",
                    # Float types
                    "float": "FLOAT",
                    "real": "REAL",
                    "double precision": "DOUBLE PRECISION",
                    "double": "DOUBLE",
                    # String types
                    "varchar": "VARCHAR",
                    "char": "CHAR",
                    "character varying": "VARCHAR",
                    "character": "CHAR",
                    "text": "TEXT",
                    "string": "STRING",
                    "nvarchar": "VARCHAR",
                    "nchar": "CHAR",
                    "ntext": "TEXT",
                    # Boolean
                    "bit": "BOOLEAN",
                    "boolean": "BOOLEAN",
                    # Date/Time types
                    "date": "DATE",
                    "time": "TIME",
                    "datetime": "TIMESTAMP_NTZ",
                    "datetime2": "TIMESTAMP_NTZ",
                    "timestamp": "TIMESTAMP_NTZ",
                    "timestamp without time zone": "TIMESTAMP_NTZ",
                    "timestamp with time zone": "TIMESTAMP_TZ",
                    "datetimeoffset": "TIMESTAMP_TZ",
                    "smalldatetime": "TIMESTAMP_NTZ",
                    # Binary types
                    "binary": "BINARY",
                    "varbinary": "VARBINARY",
                    "image": "VARBINARY",
                    # Other types
                    "uniqueidentifier": "VARCHAR(36)",  # UUID as string
                    "uuid": "VARCHAR(36)",
                    "xml": "VARIANT",
                    "json": "VARIANT",
                }
                
                mapped_type = type_mapping.get(base_type, "VARCHAR")  # Default to VARCHAR if unknown
                
                # Add length/precision to mapped type
                if max_length is not None and mapped_type in ("VARCHAR", "CHAR", "STRING"):
                    mapped_type = f"{mapped_type}({max_length})"
                elif max_length is None and mapped_type in ("VARCHAR", "CHAR", "STRING"):
                    mapped_type = f"{mapped_type}(16777216)"  # Snowflake VARCHAR default max
                elif precision is not None and mapped_type == "NUMBER":
                    if scale is not None:
                        mapped_type = f"NUMBER({precision},{scale})"
                    else:
                        mapped_type = f"NUMBER({precision})"
                
                nullable = "" if is_nullable else " NOT NULL"
                
                # Quote column name to handle reserved words and special characters
                # Snowflake uses double quotes for identifiers
                quoted_col_name = f'"{col_name}"'
                
                column_defs.append(f"{quoted_col_name} {mapped_type}{nullable}")
            """
            
            # Validate column definitions are not empty (should always have RECORD_CONTENT and RECORD_METADATA)
            if not column_defs:
                return {
                    "success": False,
                    "error": f"No valid column definitions generated from source columns. Source columns: {len(source_columns)}"
                }
            
            # Log first few column definitions for debugging
            logger.info(f"Generated {len(column_defs)} column definitions. First 3: {column_defs[:3]}")
            
            # CRITICAL: Verify schema exists one more time before creating table
            # This is important because schema creation might have happened in a different connection
            import time
            max_retries = 3
            schema_verified = False
            
            for retry in range(max_retries):
                try:
                    cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                    existing_schemas = [row[1] for row in cursor.fetchall()]
                    logger.info(f"Schemas in {db_name_to_use} (attempt {retry + 1}/{max_retries}): {existing_schemas}")
                    
                    if target_sch_upper in existing_schemas:
                        logger.info(f"✓ Schema {target_sch_upper} verified to exist")
                        schema_verified = True
                        break
                    
                    # Schema doesn't exist - create it
                    if retry < max_retries - 1:  # Don't log error on last retry
                        logger.warning(f"Schema {target_sch_upper} not found (attempt {retry + 1}), creating it...")
                    
                    try:
                        # Try simple name first (we're in database context)
                        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_sch_upper}")
                        logger.info(f"✓ Executed CREATE SCHEMA IF NOT EXISTS {target_sch_upper}")
                        
                        # Wait a bit for Snowflake to make it visible
                        time.sleep(1)
                        
                        # Verify it was created
                        cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                        existing_schemas = [row[1] for row in cursor.fetchall()]
                        
                        if target_sch_upper in existing_schemas:
                            logger.info(f"✓ Schema {target_sch_upper} created and verified")
                            schema_verified = True
                            break
                        else:
                            # Try with fully qualified name
                            logger.warning(f"Schema not found after simple creation, trying fully qualified...")
                            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name_to_use}.{target_sch_upper}")
                            time.sleep(1)
                            cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                            existing_schemas = [row[1] for row in cursor.fetchall()]
                            
                            if target_sch_upper in existing_schemas:
                                logger.info(f"✓ Schema {target_sch_upper} created with fully qualified name")
                                schema_verified = True
                                break
                            
                    except Exception as create_error:
                        error_msg = str(create_error)
                        logger.error(f"Failed to create schema {target_sch_upper} (attempt {retry + 1}): {error_msg}")
                        if "not authorized" in error_msg.lower() or "permission" in error_msg.lower():
                            return {
                                "success": False,
                                "error": f"Permission denied creating schema '{target_sch_upper}': {error_msg}. Please verify you have CREATE SCHEMA privileges in database '{db_name_to_use}'."
                            }
                        # Continue to next retry
                        time.sleep(1)
                        
                except Exception as schema_check_error:
                    logger.warning(f"Error checking schemas (attempt {retry + 1}): {schema_check_error}")
                    if retry < max_retries - 1:
                        time.sleep(1)
            
            if not schema_verified:
                # Final check - get current schemas for error message
                try:
                    cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                    existing_schemas = [row[1] for row in cursor.fetchall()]
                except:
                    existing_schemas = []
                
                return {
                    "success": False,
                    "error": f"Schema '{target_sch_upper}' does not exist in database '{db_name_to_use}' and could not be created after {max_retries} attempts. Available schemas: {', '.join(existing_schemas[:10]) if existing_schemas else 'None'}. Please create the schema manually in Snowflake using: CREATE SCHEMA IF NOT EXISTS {db_name_to_use}.{target_sch_upper};"
                }
            
            # Ensure we're in the correct schema context
            try:
                cursor.execute(f"USE SCHEMA {target_sch_upper}")
                logger.info(f"Switched to schema context: {target_sch_upper}")
            except Exception as use_schema_error:
                # Try with fully qualified name
                try:
                    cursor.execute(f"USE SCHEMA {db_name_to_use}.{target_sch_upper}")
                    logger.info(f"Switched to schema using fully qualified name: {db_name_to_use}.{target_sch_upper}")
                except Exception as use_schema_error2:
                    error_msg = str(use_schema_error2)
                    logger.error(f"Failed to use schema: {error_msg}")
                    return {
                        "success": False,
                        "error": f"Failed to use schema '{target_sch_upper}': {error_msg}. Please verify the schema exists in database '{db_name_to_use}'."
                    }
            
            # CRITICAL: Verify schema exists one final time in this connection before creating table
            # This ensures the schema is visible in the current connection context
            try:
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                final_schemas = [row[1] for row in cursor.fetchall()]
                logger.info(f"Final schema check before table creation - Schemas in {db_name_to_use}: {final_schemas}")
                
                if target_sch_upper not in final_schemas:
                    logger.warning(f"Schema {target_sch_upper} not found in final check, creating it now...")
                    try:
                        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_sch_upper}")
                        import time
                        time.sleep(0.5)
                        # Verify again
                        cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name_to_use}")
                        final_schemas = [row[1] for row in cursor.fetchall()]
                        if target_sch_upper not in final_schemas:
                            return {
                                "success": False,
                                "error": f"Schema '{target_sch_upper}' does not exist in database '{db_name_to_use}' and could not be created. Available schemas: {', '.join(final_schemas[:10])}. Please create it manually: CREATE SCHEMA IF NOT EXISTS {db_name_to_use}.{target_sch_upper};"
                            }
                        logger.info(f"✓ Schema {target_sch_upper} created in final check")
                    except Exception as final_create_error:
                        error_msg = str(final_create_error)
                        logger.error(f"Failed to create schema in final check: {error_msg}")
                        return {
                            "success": False,
                            "error": f"Schema '{target_sch_upper}' does not exist and could not be created: {error_msg}. Available schemas: {', '.join(final_schemas[:10]) if final_schemas else 'None'}"
                        }
                else:
                    logger.info(f"✓ Schema {target_sch_upper} verified to exist in final check")
            except Exception as final_check_error:
                logger.error(f"Error in final schema check: {final_check_error}")
                # Continue anyway - we'll try to create the table
            
            # Ensure we're in the correct schema context before creating table
            # This is critical - we must be in the schema context
            # Verify current context first
            try:
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                current_db, current_schema = cursor.fetchone()
                logger.info(f"Current context before USE SCHEMA: database={current_db}, schema={current_schema}")
            except Exception as context_check_error:
                logger.warning(f"Could not check current context: {context_check_error}")
            
            try:
                cursor.execute(f"USE SCHEMA {target_sch_upper}")
                # Verify it worked
                cursor.execute("SELECT CURRENT_SCHEMA()")
                actual_schema = cursor.fetchone()[0]
                logger.info(f"✓ Set schema context to: {target_sch_upper} (verified: {actual_schema})")
                if actual_schema.upper() != target_sch_upper:
                    logger.warning(f"Schema context mismatch: expected {target_sch_upper}, got {actual_schema}")
            except Exception as use_schema_final_error:
                # Try with fully qualified name
                try:
                    cursor.execute(f"USE SCHEMA {db_name_to_use}.{target_sch_upper}")
                    cursor.execute("SELECT CURRENT_SCHEMA()")
                    actual_schema = cursor.fetchone()[0]
                    logger.info(f"✓ Set schema context using fully qualified name: {db_name_to_use}.{target_sch_upper} (verified: {actual_schema})")
                except Exception as use_schema_final_error2:
                    error_msg = str(use_schema_final_error2)
                    logger.error(f"Failed to set schema context: {error_msg}")
                    return {
                        "success": False,
                        "error": f"Failed to set schema context to '{target_sch_upper}': {error_msg}. Please verify the schema exists in database '{db_name_to_use}'. You can create it manually with: CREATE SCHEMA IF NOT EXISTS {db_name_to_use}.{target_sch_upper};"
                    }
            
            # Verify we're in the right context one final time and fix if needed
            try:
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                current_db, current_schema = cursor.fetchone()
                logger.info(f"Final context check: database={current_db}, schema={current_schema}")
                
                context_ok = True
                
                if current_db.upper() != db_name_to_use.upper():
                    logger.warning(f"Database context mismatch: expected {db_name_to_use}, got {current_db}")
                    try:
                        cursor.execute(f"USE DATABASE {db_name_to_use}")
                        cursor.execute("SELECT CURRENT_DATABASE()")
                        verified_db = cursor.fetchone()[0]
                        logger.info(f"Re-set database context to {db_name_to_use} (verified: {verified_db})")
                        if verified_db.upper() != db_name_to_use.upper():
                            context_ok = False
                            logger.error(f"Failed to set database context - still showing {verified_db}")
                    except Exception as db_error:
                        logger.error(f"Failed to set database context: {db_error}")
                        context_ok = False
                
                if current_schema.upper() != target_sch_upper:
                    logger.warning(f"Schema context mismatch: expected {target_sch_upper}, got {current_schema}")
                    try:
                        cursor.execute(f"USE SCHEMA {target_sch_upper}")
                        cursor.execute("SELECT CURRENT_SCHEMA()")
                        verified_schema = cursor.fetchone()[0]
                        logger.info(f"Re-set schema context to {target_sch_upper} (verified: {verified_schema})")
                        if verified_schema.upper() != target_sch_upper:
                            context_ok = False
                            logger.error(f"Failed to set schema context - still showing {verified_schema}")
                    except Exception as schema_error:
                        logger.error(f"Failed to set schema context: {schema_error}")
                        context_ok = False
                
                if not context_ok:
                    return {
                        "success": False,
                        "error": f"Failed to set correct database/schema context. Expected: database={db_name_to_use}, schema={target_sch_upper}. Please verify the schema exists and you have access to it."
                    }
                
                # Final verification - check context one more time
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                final_db, final_schema = cursor.fetchone()
                logger.info(f"✓ Final verified context: database={final_db}, schema={final_schema}")
                
            except Exception as context_fix_error:
                logger.error(f"Error verifying/fixing context: {context_fix_error}")
                return {
                    "success": False,
                    "error": f"Failed to verify database/schema context: {context_fix_error}"
                }
            
            # Log column definitions for debugging
            logger.info(f"Column definitions ({len(column_defs)} columns): {column_defs[:3]}..." if len(column_defs) > 3 else f"Column definitions: {column_defs}")
            
            # Create table - use simple table name since we're in schema context
            # Snowflake will create it in the current schema (PUBLIC)
            # Quote table name to handle reserved words
            quoted_table_name = f'"{target_table_upper}"'
            create_query = f"CREATE TABLE {quoted_table_name} (\n    {', '.join(column_defs)}\n)"
            
            logger.info(f"Creating Snowflake table with SQL:\n{create_query}")
            
            try:
                cursor.execute(create_query)
                logger.info(f"✓ Successfully created table {target_table_upper}")
                table_created = True
            except Exception as create_error:
                error_msg = str(create_error)
                logger.error(f"Table creation failed: {error_msg}")
                logger.error(f"Failed SQL was:\n{create_query}")
                if hasattr(create_error, 'errno'):
                    logger.error(f"Error number: {create_error.errno}")
                if hasattr(create_error, 'sqlstate'):
                    logger.error(f"SQL state: {create_error.sqlstate}")
                
                # If simple name fails, try fully qualified name
                logger.info(f"Trying with fully qualified name...")
                quoted_db = f'"{db_name_to_use}"'
                quoted_schema = f'"{target_sch_upper}"'
                quoted_table = f'"{target_table_upper}"'
                create_query2 = f"CREATE TABLE {quoted_db}.{quoted_schema}.{quoted_table} (\n    {', '.join(column_defs)}\n)"
                try:
                    logger.info(f"Creating with fully qualified name:\n{create_query2}")
                    cursor.execute(create_query2)
                    logger.info(f"✓ Successfully created table with fully qualified name")
                    table_created = True
                except Exception as create_error2:
                    error_msg2 = str(create_error2)
                    logger.error(f"Fully qualified name also failed: {error_msg2}")
                    last_error = error_msg2
                    table_created = False
            
            if not table_created:
                # Get final context for error message
                try:
                    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                    final_db, final_schema = cursor.fetchone()
                    logger.error(f"Final context when error occurred: database={final_db}, schema={final_schema}")
                except:
                    final_db = "unknown"
                    final_schema = "unknown"
                
                # Provide more helpful error message
                if "does not exist" in last_error.lower():
                    return {
                        "success": False,
                        "error": f"Schema or database does not exist. Schema: {target_sch_upper}, Database: {db_name_to_use}. Current context: database={final_db}, schema={final_schema}. Available databases: {', '.join(existing_databases[:10]) if existing_databases else 'Could not list'}. Error: {last_error}. Please verify the schema exists and you have CREATE TABLE privileges."
                    }
                elif "not authorized" in last_error.lower() or "permission" in last_error.lower():
                    return {
                        "success": False,
                        "error": f"Permission denied. You may not have permission to create tables in schema '{target_sch_upper}'. Current context: database={final_db}, schema={final_schema}. Error: {last_error}"
                    }
                else:
                    return {
                        "success": False,
                        "error": f"Failed to create table '{target_table_upper}': {last_error}. Current context: database={final_db}, schema={final_schema}"
                    }
            
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            
            return {
                "success": True,
                "message": f"Table '{target_table_upper}' created successfully in {db_name_to_use}.{target_sch_upper}",
                "table_name": target_table_upper,
                "database": db_name_to_use,
                "schema": target_sch_upper,
                "column_count": len(source_columns)
            }
            
        except Exception as e:
            logger.error(f"Error creating Snowflake table: {e}", exc_info=True)
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
            return {
                "success": False,
                "error": str(e)
            }
    
    def sync_schema(
        self,
        source_connection_id: str,
        target_connection_id: str,
        table_name: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        target_table_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update target schema to match source.
        
        Args:
            source_connection_id: Source connection ID
            target_connection_id: Target connection ID
            table_name: Source table name
            source_database: Source database (optional)
            source_schema: Source schema (optional)
            target_database: Target database (optional)
            target_schema: Target schema (optional)
            target_table_name: Target table name (optional)
            
        Returns:
            Sync result with changes
        """
        try:
            # Get source and target schemas
            source_schema_info = self.connection_service.get_table_schema(
                source_connection_id,
                table_name,
                database=source_database,
                schema=source_schema
            )
            
            if not source_schema_info.get("success"):
                return {
                    "success": False,
                    "error": f"Failed to get source schema: {source_schema_info.get('error')}"
                }
            
            target_table = target_table_name or table_name
            target_schema_info = self.connection_service.get_table_schema(
                target_connection_id,
                target_table,
                database=target_database,
                schema=target_schema
            )
            
            if not target_schema_info.get("success"):
                # Table doesn't exist, create it
                return self.create_target_table(
                    source_connection_id,
                    target_connection_id,
                    table_name,
                    source_database=source_database,
                    source_schema=source_schema,
                    target_database=target_database,
                    target_schema=target_schema,
                    target_table_name=target_table_name
                )
            
            # Compare schemas and generate ALTER statements
            source_columns = {col["name"]: col for col in source_schema_info.get("columns", [])}
            target_columns = {col["name"]: col for col in target_schema_info.get("columns", [])}
            
            changes = {
                "added_columns": [],
                "removed_columns": [],
                "modified_columns": []
            }
            
            # Find added columns
            for col_name, col_info in source_columns.items():
                if col_name not in target_columns:
                    changes["added_columns"].append(col_info)
            
            # Find removed columns
            for col_name in target_columns:
                if col_name not in source_columns:
                    changes["removed_columns"].append(target_columns[col_name])
            
            # Find modified columns
            for col_name in source_columns:
                if col_name in target_columns:
                    source_col = source_columns[col_name]
                    target_col = target_columns[col_name]
                    if (source_col.get("data_type") != target_col.get("data_type") or
                        source_col.get("is_nullable") != target_col.get("is_nullable")):
                        changes["modified_columns"].append({
                            "column": col_name,
                            "source": source_col,
                            "target": target_col
                        })
            
            return {
                "success": True,
                "message": "Schema comparison completed",
                "changes": changes,
                "has_changes": len(changes["added_columns"]) > 0 or len(changes["removed_columns"]) > 0 or len(changes["modified_columns"]) > 0
            }
            
        except Exception as e:
            logger.error(f"Error syncing schema: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def validate_schema_compatibility(
        self,
        source_connection_id: str,
        target_connection_id: str,
        table_name: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Check if source and target schemas are compatible.
        
        Args:
            source_connection_id: Source connection ID
            target_connection_id: Target connection ID
            table_name: Table name
            source_database: Source database (optional)
            source_schema: Source schema (optional)
            target_database: Target database (optional)
            target_schema: Target schema (optional)
            
        Returns:
            Compatibility validation result
        """
        try:
            # Get source schema
            source_schema_info = self.connection_service.get_table_schema(
                source_connection_id,
                table_name,
                database=source_database,
                schema=source_schema
            )
            
            if not source_schema_info.get("success"):
                return {
                    "compatible": False,
                    "error": f"Failed to get source schema: {source_schema_info.get('error')}"
                }
            
            source_columns = source_schema_info.get("columns", [])
            
            # Check if target table exists
            target_schema_info = self.connection_service.get_table_schema(
                target_connection_id,
                table_name,
                database=target_database,
                schema=target_schema
            )
            
            if not target_schema_info.get("success"):
                # Table doesn't exist - compatible (can be created)
                return {
                    "compatible": True,
                    "message": "Target table does not exist - can be created",
                    "source_columns": len(source_columns),
                    "target_columns": 0
                }
            
            target_columns = target_schema_info.get("columns", [])
            
            # Basic compatibility check
            source_col_names = {col["name"] for col in source_columns}
            target_col_names = {col["name"] for col in target_columns}
            
            missing_in_target = source_col_names - target_col_names
            extra_in_target = target_col_names - source_col_names
            
            compatible = len(missing_in_target) == 0
            
            return {
                "compatible": compatible,
                "message": "Compatible" if compatible else "Incompatible - missing columns in target",
                "source_columns": len(source_columns),
                "target_columns": len(target_columns),
                "missing_columns": list(missing_in_target),
                "extra_columns": list(extra_in_target)
            }
            
        except Exception as e:
            logger.error(f"Error validating schema compatibility: {e}", exc_info=True)
            return {
                "compatible": False,
                "error": str(e)
            }

