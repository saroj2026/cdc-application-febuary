"""Snowflake connector for metadata extraction and data operations."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import snowflake.connector as snowflake_connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake_connector = None  # type: ignore

try:
    from ingestion.models import ConnectorColumn, ConnectorTable
except ImportError:
    # Fallback classes for when models are not available
    class ConnectorColumn:
        def __init__(self, name, data_type, ordinal_position=1, is_nullable=True, 
                     default_value=None, description=None, json_schema=None):
            self.name = name
            self.data_type = data_type
            self.ordinal_position = ordinal_position
            self.is_nullable = is_nullable
            self.default_value = default_value
            self.description = description
            self.json_schema = json_schema or {}

    class ConnectorTable:
        def __init__(self, fully_qualified_name, name, service_fully_qualified_name,
                     database_schema=None, table_type="table", description=None,
                     columns=None, properties=None):
            self.fully_qualified_name = fully_qualified_name
            self.name = name
            self.service_fully_qualified_name = service_fully_qualified_name
            self.database_schema = database_schema
            self.table_type = table_type
            self.description = description
            self.columns = columns or []
            self.properties = properties or {}

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class SnowflakeConnector(BaseConnector):
    """Connector for extracting metadata and data from Snowflake."""

    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize Snowflake connector with connection configuration.

        Args:
            connection_config: Dictionary containing:
                - account: Snowflake account identifier (required, e.g., "xy12345.us-east-1")
                - user: Snowflake username (required)
                - password: Snowflake password (required, unless private_key is provided)
                - database: Database name (required)
                - schema: Schema name (optional, default: "PUBLIC")
                - warehouse: Warehouse name (optional, but recommended)
                - role: Role name (optional)
                - private_key: Private key for key pair authentication (optional, alternative to password)
                - private_key_passphrase: Passphrase for private key (optional, if private_key is encrypted)
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is not installed. "
                "Install it with: pip install snowflake-connector-python"
            )

        super().__init__(connection_config)
        self.connection = None

    def _validate_config(self) -> None:
        """Validate that required connection parameters are present."""
        required = ["account", "user", "database"]
        missing = [key for key in required if not self.config.get(key)]
        if missing:
            raise ValueError(f"Missing required connection parameters: {', '.join(missing)}")
        
        # Either password or private_key must be provided
        if not self.config.get("password") and not self.config.get("private_key"):
            raise ValueError("Either 'password' or 'private_key' must be provided for Snowflake authentication")

    def _get_connection(self):
        """Get or create Snowflake connection."""
        if self.connection is None:
            # Build connection parameters
            conn_params = {
                "account": self.config["account"],
                "user": self.config["user"],
                "database": self.config["database"],
                "schema": self.config.get("schema", "PUBLIC"),
            }
            
            # Add password or private key authentication
            if self.config.get("password"):
                conn_params["password"] = self.config["password"]
            elif self.config.get("private_key"):
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.backends import default_backend
                
                private_key_str = self.config["private_key"]
                private_key_passphrase = self.config.get("private_key_passphrase")
                
                # Load private key
                if private_key_passphrase:
                    p_key = serialization.load_pem_private_key(
                        private_key_str.encode('utf-8'),
                        password=private_key_passphrase.encode('utf-8'),
                        backend=default_backend()
                    )
                else:
                    p_key = serialization.load_pem_private_key(
                        private_key_str.encode('utf-8'),
                        password=None,
                        backend=default_backend()
                    )
                
                conn_params["private_key"] = p_key
            
            # Add optional parameters
            if self.config.get("warehouse"):
                conn_params["warehouse"] = self.config["warehouse"]
            if self.config.get("role"):
                conn_params["role"] = self.config["role"]
            
            self.connection = snowflake_connector.connect(**conn_params)
        
        return self.connection

    def connect(self):
        """Establish connection to Snowflake.

        Returns:
            Snowflake connection object
        """
        try:
            conn = self._get_connection()
            # Test connection with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()
            cursor.close()
            logger.info(f"Connected to Snowflake account: {self.config['account']}, database: {self.config['database']}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def test_connection(self) -> bool:
        """Test the Snowflake connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return False

    def get_version(self) -> str:
        """Get Snowflake version information.
        
        Returns:
            String with Snowflake version information
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()
            cursor.close()
            return f"Snowflake {version[0] if version else 'Unknown'}"
        except Exception as e:
            logger.warning(f"Could not get Snowflake version info: {e}")
            return f"Snowflake - Account: {self.config.get('account', 'unknown')}"

    def disconnect(self, connection=None):
        """Close Snowflake connection.
        
        Args:
            connection: Optional connection object to close. If not provided, uses self.connection.
        """
        conn_to_close = connection or self.connection
        if conn_to_close:
            try:
                conn_to_close.close()
                if conn_to_close == self.connection:
                    self.connection = None
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {e}")

    def extract_lsn_offset(
        self,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract LSN (Log Sequence Number) or offset metadata from Snowflake.
        
        For Snowflake, we use the current timestamp as the LSN equivalent.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            
        Returns:
            Dictionary containing LSN/offset information
        """
        db = database or self.config.get("database")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get current timestamp from Snowflake
            cursor.execute("SELECT CURRENT_TIMESTAMP()")
            timestamp_result = cursor.fetchone()
            current_timestamp = timestamp_result[0] if timestamp_result else datetime.utcnow()
            
            cursor.close()
            
            lsn = f"TIMESTAMP:{current_timestamp.isoformat()}"
            offset = int(current_timestamp.timestamp()) if hasattr(current_timestamp, 'timestamp') else None
            
            lsn_result = {
                "lsn": lsn,
                "offset": offset,
                "timestamp": current_timestamp.isoformat() if hasattr(current_timestamp, 'isoformat') else str(current_timestamp),
                "database": db,
                "metadata": {
                    "lsn_source": "snowflake_current_timestamp"
                }
            }
            
            logger.info(f"Extracted LSN/offset for Snowflake database {db}: LSN={lsn}")
            return lsn_result
            
        except Exception as e:
            logger.error(f"Error extracting LSN/offset from Snowflake: {e}")
            return {
                "lsn": None,
                "offset": None,
                "timestamp": datetime.utcnow().isoformat(),
                "database": db,
                "metadata": {"error": str(e)}
            }

    def list_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[str]:
        """List all tables in a schema.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            schema: Schema name (optional, uses config default if not provided)
            
        Returns:
            List of table names
        """
        db = database or self.config.get("database")
        schema_name = schema or self.config.get("schema", "PUBLIC")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # For Snowflake, we need to use the correct database context
            # First, ensure we're using the correct database
            if db:
                cursor.execute(f"USE DATABASE {db}")
            
            # Use the correct schema
            if schema_name:
                cursor.execute(f"USE SCHEMA {schema_name}")
            
            # Query tables - Snowflake INFORMATION_SCHEMA uses uppercase schema names
            query = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = UPPER(%s)
                    AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """
            cursor.execute(query, (schema_name.upper() if schema_name else "PUBLIC",))
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Found {len(tables)} tables in {db}.{schema_name}")
            return tables
            
        except Exception as e:
            logger.error(f"Error listing tables from Snowflake: {e}", exc_info=True)
            raise

    def list_schemas(self, database: Optional[str] = None) -> List[str]:
        """List all schemas in a database.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            
        Returns:
            List of schema names
        """
        db = database or self.config.get("database")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE CATALOG_NAME = %s
                ORDER BY SCHEMA_NAME
            """
            cursor.execute(query, (db,))
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Found {len(schemas)} schemas in database {db}")
            return schemas
            
        except Exception as e:
            logger.error(f"Error listing schemas from Snowflake: {e}", exc_info=True)
            raise

    def list_databases(self) -> List[str]:
        """List all databases accessible to the user.
        
        Returns:
            List of database names
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            query = """
                SHOW DATABASES
            """
            cursor.execute(query)
            databases = [row[1] for row in cursor.fetchall()]  # Second column is database name
            cursor.close()
            
            logger.info(f"Found {len(databases)} databases")
            return databases
            
        except Exception as e:
            logger.error(f"Error listing databases from Snowflake: {e}", exc_info=True)
            raise

    def extract_schema(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract schema information from Snowflake.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            schema: Schema name (optional, uses config default if not provided)
            table: Table name (optional, if None extracts all tables in schema)
            
        Returns:
            Dictionary containing schema information
        """
        db = database or self.config.get("database")
        schema_name = schema or self.config.get("schema", "PUBLIC")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Build query to get tables
            if table:
                query = """
                    SELECT TABLE_NAME, TABLE_TYPE, COMMENT
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY TABLE_NAME
                """
                cursor.execute(query, (schema_name, table))
            else:
                query = """
                    SELECT TABLE_NAME, TABLE_TYPE, COMMENT
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s
                    ORDER BY TABLE_NAME
                """
                cursor.execute(query, (schema_name,))
            
            tables_data = cursor.fetchall()
            tables_dict = []
            
            for table_row in tables_data:
                table_name = table_row[0]
                table_type = table_row[1]
                comment = table_row[2]
                
                # Get columns for this table
                columns_query = """
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        ORDINAL_POSITION,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        COMMENT
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                """
                cursor.execute(columns_query, (schema_name, table_name))
                columns_data = cursor.fetchall()
                
                columns = []
                for col_row in columns_data:
                    columns.append({
                        "name": col_row[0],
                        "data_type": col_row[1],
                        "ordinal_position": col_row[2],
                        "is_nullable": col_row[3] == "YES",
                        "default_value": col_row[4],
                        "description": col_row[5],
                        "json_schema": {}
                    })
                
                tables_dict.append({
                    "name": table_name,
                    "fully_qualified_name": f"{db}.{schema_name}.{table_name}",
                    "columns": columns,
                    "properties": {
                        "table_type": table_type,
                        "comment": comment
                    },
                    "table_type": table_type.lower(),
                    "description": comment or f"Snowflake table: {table_name}"
                })
            
            cursor.close()
            
            return {
                "database": db,
                "schema": schema_name,
                "tables": tables_dict
            }
            
        except Exception as e:
            logger.error(f"Error extracting schema from Snowflake: {e}")
            raise

    def extract_data(
        self,
        database: str,
        schema: str,
        table_name: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Extract actual data from a Snowflake table.
        
        Args:
            database: Database name
            schema: Schema name
            table_name: Table name
            limit: Maximum number of rows to extract (None for all rows)
            offset: Number of rows to skip (for pagination)
            
        Returns:
            Dictionary containing extracted data
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Build query with limit and offset
            query = f'SELECT * FROM "{database}"."{schema}"."{table_name}"'
            
            if limit:
                query += f" LIMIT {limit}"
            if offset > 0:
                query += f" OFFSET {offset}"
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Convert rows to list of lists
            rows_list = [list(row) for row in rows]
            
            cursor.close()
            
            return {
                "rows": rows_list,
                "row_count": len(rows_list),
                "total_rows": len(rows_list),  # Snowflake doesn't provide total count easily
                "has_more": False,  # Can't determine without another query
                "column_names": column_names
            }
            
        except Exception as e:
            logger.error(f"Error extracting data from Snowflake: {e}")
            raise

    def full_load(
        self,
        tables: List[str],
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_schema: bool = True,
        include_data: bool = True,
        data_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Perform full load for specified Snowflake tables.
        
        Args:
            tables: List of table names to extract
            database: Database name (optional, uses config default if not provided)
            schema: Schema name (optional, uses config default if not provided)
            include_schema: Whether to extract schema information
            include_data: Whether to extract data rows
            data_limit: Maximum rows per table (None for all rows)
            
        Returns:
            Dictionary containing full load results
        """
        db = database or self.config.get("database")
        schema_name = schema or self.config.get("schema", "PUBLIC")
        
        # Extract LSN/offset metadata
        lsn_offset = self.extract_lsn_offset(database=db)
        
        result = {
            "database": db,
            "schema": schema_name,
            "lsn_offset": lsn_offset,
            "tables": [],
            "metadata": {
                "extraction_timestamp": datetime.utcnow().isoformat(),
                "tables_processed": len(tables),
                "tables_successful": 0,
                "tables_failed": 0
            }
        }
        
        for table_name in tables:
            try:
                table_result = {
                    "table_name": table_name,
                    "metadata": {
                        "extraction_timestamp": datetime.utcnow().isoformat()
                    }
                }
                
                # Extract schema if requested
                if include_schema:
                    try:
                        schema_info = self.extract_schema(
                            database=db,
                            schema=schema_name,
                            table=table_name
                        )
                        if schema_info.get("tables"):
                            table_result["schema"] = schema_info["tables"][0]
                    except Exception as e:
                        logger.warning(f"Failed to extract schema for {table_name}: {e}")
                        table_result["schema"] = None
                
                # Extract data if requested
                if include_data:
                    try:
                        data_info = self.extract_data(
                            database=db,
                            schema=schema_name,
                            table_name=table_name,
                            limit=data_limit
                        )
                        table_result["data"] = data_info
                    except Exception as e:
                        logger.warning(f"Failed to extract data for {table_name}: {e}")
                        table_result["data"] = None
                
                result["tables"].append(table_result)
                result["metadata"]["tables_successful"] += 1
                
            except Exception as e:
                logger.error(f"Error processing table {table_name}: {e}")
                result["tables"].append({
                    "table_name": table_name,
                    "error": str(e),
                    "metadata": {
                        "extraction_timestamp": datetime.utcnow().isoformat(),
                        "status": "failed"
                    }
                })
                result["metadata"]["tables_failed"] += 1
        
        logger.info(
            f"Full load completed for {result['metadata']['tables_successful']} tables, "
            f"{result['metadata']['tables_failed']} failed"
        )
        return result

    def get_table_columns(
        self,
        table: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get column information for a table.

        Args:
            table: Table name
            database: Database name (optional, uses config default if not provided)
            schema: Schema name (optional, uses config default if not provided)

        Returns:
            List of column dictionaries
        """
        db = database or self.config.get("database")
        schema_name = schema or self.config.get("schema", "PUBLIC")
        
        if not db:
            raise ValueError("Database name is required")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Snowflake INFORMATION_SCHEMA uses uppercase schema names
            # Use pyformat (%s) for Snowflake connector
            schema_upper = schema_name.upper() if schema_name else "PUBLIC"
            table_upper = table.upper() if table else table
            
            # Build query with proper parameter binding
            # Use %s placeholders for Snowflake pyformat style
            query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    ORDINAL_POSITION,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s
                ORDER BY ORDINAL_POSITION
            """
            # Use named parameters to avoid formatting issues
            cursor.execute(query, {"schema": schema_upper, "table": table_upper})
            
            columns = []
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1]
                max_length = row[2]
                precision = row[3]
                scale = row[4]
                ordinal = row[5]
                nullable = row[6]
                default = row[7]
                comment = row[8]
                
                # Build full data type string
                full_data_type = data_type
                if max_length and data_type in ("VARCHAR", "CHAR", "STRING", "TEXT"):
                    full_data_type = f"{data_type}({max_length})"
                elif precision and data_type in ("NUMBER", "NUMERIC", "DECIMAL"):
                    if scale is not None:
                        full_data_type = f"{data_type}({precision},{scale})"
                    else:
                        full_data_type = f"{data_type}({precision})"
                
                columns.append({
                    "name": col_name,
                    "data_type": full_data_type,
                    "ordinal_position": int(ordinal) if ordinal else 1,
                    "is_nullable": nullable == "YES",
                    "default_value": str(default) if default else None,
                    "description": comment,
                    "json_schema": {
                        "data_type": data_type,
                        "max_length": max_length,
                        "precision": precision,
                        "scale": scale
                    }
                })
            
            cursor.close()
            return columns
            
        except Exception as e:
            logger.error(f"Failed to get table columns: {e}")
            raise

    def get_primary_keys(
        self,
        table: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[str]:
        """Get primary key columns for a table.

        Args:
            table: Table name
            database: Database name (optional, uses config default if not provided)
            schema: Schema name (optional, uses config default if not provided)

        Returns:
            List of primary key column names (empty list if no PK or error)
        """
        db = database or self.config.get("database")
        schema_name = schema or self.config.get("schema", "PUBLIC")
        
        if not db:
            logger.warning("Database name is required for get_primary_keys, returning empty list")
            return []
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Snowflake INFORMATION_SCHEMA is at database level, not schema level
            # Use database qualification to avoid schema prefix issues (SEG.INFORMATION_SCHEMA)
            # Also ensure schema and table names are uppercase as Snowflake requires
            schema_upper = schema_name.upper() if schema_name else "PUBLIC"
            table_upper = table.upper() if table else table
            
            # Set database context to ensure INFORMATION_SCHEMA is accessed correctly
            try:
                cursor.execute(f'USE DATABASE "{db}"')
            except Exception as db_use_error:
                logger.debug(f"Could not USE DATABASE {db}: {db_use_error}, continuing with query")
            
            # Use database-qualified INFORMATION_SCHEMA to avoid schema prefix issues
            # In Snowflake, INFORMATION_SCHEMA is at DATABASE level, not schema level
            query = """
                SELECT kcu.COLUMN_NAME
                FROM {}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN {}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                    AND tc.TABLE_CATALOG = kcu.TABLE_CATALOG
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_SCHEMA = %(schema)s
                    AND tc.TABLE_NAME = %(table)s
                ORDER BY kcu.ORDINAL_POSITION
            """.format(db, db)
            
            cursor.execute(query, {"schema": schema_upper, "table": table_upper})
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            
            # Log warning if no primary keys found (not an error, some tables don't have PKs)
            if not primary_keys:
                logger.debug(f"No primary keys found for table {schema_upper}.{table_upper} in database {db}")
            
            return primary_keys
            
        except Exception as e:
            # Don't fail the entire operation if we can't get primary keys
            # Some tables may not have PKs, or user may not have permission
            # Log the error but return empty list instead of raising
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not authorized" in error_msg or "permission" in error_msg:
                logger.warning(f"Cannot access primary key information for table {schema_name}.{table} in database {db}: {e}. "
                             f"This may be due to insufficient permissions or the table may not have a primary key. "
                             f"Returning empty primary key list.")
            else:
                logger.warning(f"Failed to get primary keys for table {schema_name}.{table} in database {db}: {e}. "
                             f"Returning empty primary key list.")
            return []  # Return empty list instead of raising - allows table data to still be fetched


