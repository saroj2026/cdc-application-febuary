"""Oracle connector for metadata extraction and data operations."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import oracledb
    ORACLEDB_AVAILABLE = True
except ImportError:
    ORACLEDB_AVAILABLE = False
    oracledb = None  # type: ignore

try:
    import cx_Oracle
    CX_ORACLE_AVAILABLE = True
except ImportError:
    CX_ORACLE_AVAILABLE = False
    cx_Oracle = None  # type: ignore

try:
    from app.ingestion.models import ConnectorColumn, ConnectorTable
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


class OracleConnector(BaseConnector):
    """Connector for extracting metadata and data from Oracle."""

    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize Oracle connector with connection configuration.

        Args:
            connection_config: Dictionary containing:
                - host: Server hostname or IP (required)
                - port: Port number (optional, default: 1521)
                - database: Database name or SID (required)
                - user or username: Username (required)
                - password: Password (required)
                - schema: Schema name (optional, default: username)
                - service_name: Service name (optional, alternative to SID)
                - mode: Connection mode (optional, default: 'normal')
        """
        if not (ORACLEDB_AVAILABLE or CX_ORACLE_AVAILABLE):
            raise ImportError(
                "Neither 'oracledb' nor 'cx_Oracle' is installed. "
                "Install one with: pip install oracledb (recommended) or pip install cx_Oracle"
            )

        # Normalize field names: accept both 'username' and 'user'
        if "username" in connection_config and "user" not in connection_config:
            connection_config["user"] = connection_config["username"]
        
        super().__init__(connection_config)
        self.connection = None  # Initialize connection attribute

    def _validate_config(self) -> None:
        """Validate that required connection parameters are present."""
        required = ["host", "user", "password"]
        missing = [key for key in required if not self.config.get(key)]
        if missing:
            raise ValueError(f"Missing required connection parameters: {', '.join(missing)}")
        
        # Either database (SID) or service_name must be provided
        if not self.config.get("database") and not self.config.get("service_name"):
            raise ValueError("Either 'database' (SID) or 'service_name' must be provided")

    def _build_dsn(self) -> str:
        """Build Oracle DSN (Data Source Name) string.
        
        Returns:
            DSN string for Oracle connection
        """
        host = self.config["host"]
        port = self.config.get("port", 1521)
        
        # Use service_name if provided, otherwise use database (SID)
        if self.config.get("service_name"):
            if ORACLEDB_AVAILABLE:
                dsn = oracledb.makedsn(host, port, service_name=self.config["service_name"])
            else:
                dsn = cx_Oracle.makedsn(host, port, service_name=self.config["service_name"])
        else:
            if ORACLEDB_AVAILABLE:
                dsn = oracledb.makedsn(host, port, sid=self.config["database"])
            else:
                dsn = cx_Oracle.makedsn(host, port, sid=self.config["database"])
        
        return dsn

    def connect(self):
        """Establish connection to Oracle.

        Returns:
            Oracle connection object
        """
        try:
            user = self.config["user"]
            password = self.config["password"]
            dsn = self._build_dsn()
            
            # Try to connect - use oracledb (newer) if available, otherwise cx_Oracle
            if ORACLEDB_AVAILABLE:
                # oracledb doesn't use encoding parameter, it defaults to UTF-8
                conn = oracledb.connect(user=user, password=password, dsn=dsn)
            elif CX_ORACLE_AVAILABLE:
                # cx_Oracle supports encoding parameter
                conn = cx_Oracle.connect(
                    user=user,
                    password=password,
                    dsn=dsn,
                    encoding="UTF-8"
                )
            else:
                raise ImportError("No Oracle driver available")
            
            logger.info(f"Connected to Oracle: {self.config['host']}:{self.config.get('port', 1521)}")
            self.connection = conn  # Store connection for disconnect
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            raise

    def test_connection(self) -> bool:
        """Test the Oracle connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Oracle connection test failed: {e}")
            return False

    def get_version(self) -> str:
        """Get Oracle version information.
        
        Returns:
            String with Oracle version information
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM V$VERSION WHERE BANNER LIKE 'Oracle%'")
            version = cursor.fetchone()
            cursor.close()
            conn.close()
            return f"Oracle {version[0] if version else 'Unknown'}"
        except Exception as e:
            logger.warning(f"Could not get Oracle version info: {e}")
            return f"Oracle - Host: {self.config.get('host', 'unknown')}"

    def disconnect(self, connection=None):
        """Close Oracle connection.
        
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
                logger.warning(f"Error closing Oracle connection: {e}")

    def extract_lsn_offset(
        self,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract SCN (System Change Number) or offset metadata from Oracle.
        
        Oracle uses SCN (System Change Number) instead of LSN.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            
        Returns:
            Dictionary containing SCN/offset information
        """
        db = database or self.config.get("database")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Get current SCN from Oracle
            cursor.execute("SELECT CURRENT_SCN FROM V$DATABASE")
            scn_result = cursor.fetchone()
            current_scn = scn_result[0] if scn_result else None
            
            # Get current timestamp
            cursor.execute("SELECT SYSTIMESTAMP FROM DUAL")
            timestamp_result = cursor.fetchone()
            current_timestamp = timestamp_result[0] if timestamp_result else datetime.utcnow()
            
            cursor.close()
            conn.close()
            
            lsn = f"SCN:{current_scn}" if current_scn else None
            offset = current_scn
            
            lsn_result = {
                "lsn": lsn,
                "offset": offset,
                "scn": current_scn,
                "timestamp": str(current_timestamp),
                "database": db,
                "metadata": {
                    "lsn_source": "oracle_scn",
                    "scn": current_scn
                }
            }
            
            logger.info(f"Extracted SCN/offset for Oracle database {db}: SCN={current_scn}")
            return lsn_result
            
        except Exception as e:
            logger.error(f"Error extracting SCN/offset from Oracle: {e}")
            return {
                "lsn": None,
                "offset": None,
                "scn": None,
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
            schema: Schema name (optional, uses config default or username if not provided)
            
        Returns:
            List of table names
        """
        schema_name = schema or self.config.get("schema") or self.config.get("user")
        
        if not schema_name:
            raise ValueError("Schema name is required for listing tables")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # If schema matches current user, use USER_TABLES (no OWNER column)
            # Otherwise use ALL_TABLES with OWNER filter
            current_user = self.config.get("user", "").upper()
            if schema_name.upper() == current_user:
                query = """
                    SELECT TABLE_NAME
                    FROM USER_TABLES
                    ORDER BY TABLE_NAME
                """
                cursor.execute(query)
            else:
                query = """
                    SELECT TABLE_NAME
                    FROM ALL_TABLES
                    WHERE OWNER = :schema_name
                    ORDER BY TABLE_NAME
                """
                cursor.execute(query, schema_name=schema_name.upper())
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(tables)} tables in schema {schema_name}")
            return tables
            
        except Exception as e:
            logger.error(f"Error listing tables from Oracle: {e}", exc_info=True)
            raise

    def list_schemas(self, database: Optional[str] = None) -> List[str]:
        """List all schemas (users) in the database.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            
        Returns:
            List of schema names
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            query = """
                SELECT USERNAME
                FROM ALL_USERS
                WHERE USERNAME NOT IN ('SYS', 'SYSTEM', 'SYSAUX')
                ORDER BY USERNAME
            """
            cursor.execute(query)
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(schemas)} schemas")
            return schemas
            
        except Exception as e:
            logger.error(f"Error listing schemas from Oracle: {e}", exc_info=True)
            raise

    def list_databases(self) -> List[str]:
        """List all databases (instances) accessible.
        
        In Oracle, this typically returns the current database/SID.
        
        Returns:
            List of database names
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Get database name
            cursor.execute("SELECT NAME FROM V$DATABASE")
            db_name = cursor.fetchone()
            cursor.close()
            conn.close()
            
            databases = [db_name[0]] if db_name else [self.config.get("database", "UNKNOWN")]
            logger.info(f"Found {len(databases)} database(s)")
            return databases
            
        except Exception as e:
            logger.error(f"Error listing databases from Oracle: {e}", exc_info=True)
            raise

    def extract_schema(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract schema information from Oracle.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            schema: Schema name (optional, uses config default or username if not provided)
            table: Table name (optional, if None extracts all tables in schema)
            
        Returns:
            Dictionary containing schema information
        """
        schema_name = schema or self.config.get("schema") or self.config.get("user")
        
        if not schema_name:
            raise ValueError("Schema name is required")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Build query to get tables
            schema_upper = schema_name.upper()
            if table:
                table_upper = table.upper()
                # Use parameterized query to prevent string formatting issues
                query = """
                    SELECT TABLE_NAME, 'TABLE' as TABLE_TYPE
                    FROM ALL_TABLES
                    WHERE OWNER = :1 AND TABLE_NAME = :2
                    ORDER BY TABLE_NAME
                """
                logger.info("[Oracle list_tables] Executing query with: schema=%r, table=%r", schema_upper, table_upper)
                cursor.execute(query, (schema_upper, table_upper))
            else:
                # Use parameterized query to prevent string formatting issues
                query = """
                    SELECT TABLE_NAME, 'TABLE' as TABLE_TYPE
                    FROM ALL_TABLES
                    WHERE OWNER = :1
                    ORDER BY TABLE_NAME
                """
                logger.info("[Oracle list_tables] Executing query with: schema=%r", schema_upper)
                cursor.execute(query, (schema_upper,))
            
            tables_data = cursor.fetchall()
            tables_dict = []
            
            for table_row in tables_data:
                table_name = table_row[0]
                table_type = table_row[1]
                
                fqn = f"{schema_name}.{table_name}"
                
                # Get columns for this table
                try:
                    columns = self._extract_columns_internal(
                        cursor, schema_name, table_name
                    )
                except Exception as e:
                    logger.warning("Error extracting columns for %s.%s: %r", schema_name, table_name, e, exc_info=True)
                    columns = []
                
                # Extract table properties
                try:
                    properties = self._extract_table_properties(
                        cursor, schema_name, table_name
                    )
                except Exception as e:
                    logger.warning("Error extracting properties for %s.%s: %r", schema_name, table_name, e, exc_info=True)
                    properties = {}
                
                table_dict = {
                    "name": table_name,
                    "fully_qualified_name": fqn,
                    "columns": columns,
                    "properties": properties,
                    "table_type": "table"
                }
                tables_dict.append(table_dict)
            
            cursor.close()
            conn.close()
            
            return {
                "database": database or self.config.get("database"),
                "schema": schema_name,
                "tables": tables_dict
            }
            
        except Exception as e:
            logger.error(f"Error extracting schema from Oracle: {e}")
            raise

    def _extract_columns_internal(
        self,
        cursor,
        schema: str,
        table: str
    ) -> List[Dict[str, Any]]:
        """Extract column metadata for a specific table (internal method).

        Args:
            cursor: Database cursor
            schema: Schema name
            table: Table name

        Returns:
            List of column dictionaries
        """
        # Use parameterized queries to prevent SQL injection and formatting issues
        # Oracle uses :1, :2 for bind variables
        # First try uppercase (unquoted identifiers are uppercase in Oracle)
        schema_upper = schema.upper().strip() if schema else None
        table_upper = table.upper().strip() if table else None
        
        if not schema_upper or not table_upper:
            raise ValueError("Schema and table names cannot be empty")
        
        # Log before executing query
        logger.info("[Oracle get_table_columns] Executing query with: schema=%r, table=%r", schema_upper, table_upper)
        
        query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                DATA_LENGTH,
                DATA_PRECISION,
                DATA_SCALE,
                COLUMN_ID,
                NULLABLE,
                DATA_DEFAULT
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :1 AND TABLE_NAME = :2
            ORDER BY COLUMN_ID
        """
        
        try:
            cursor.execute(query, (schema_upper, table_upper))
        except Exception as e:
            # If uppercase fails with ORA-00942, try with quoted (case-preserving) names
            error_msg = str(e) if hasattr(e, '__str__') else repr(e)
            if "ORA-00942" in error_msg or "does not exist" in error_msg.lower():
                logger.info("[Oracle get_table_columns] Uppercase query failed, trying with quoted (case-preserving) names: schema=%r, table=%r", schema, table)
                # Try with original case (quoted identifiers)
                query_quoted = """
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        DATA_LENGTH,
                        DATA_PRECISION,
                        DATA_SCALE,
                        COLUMN_ID,
                        NULLABLE,
                        DATA_DEFAULT
                    FROM ALL_TAB_COLUMNS
                    WHERE OWNER = :1 AND TABLE_NAME = :2
                    ORDER BY COLUMN_ID
                """
                try:
                    cursor.execute(query_quoted, (schema.strip(), table.strip()))
                except Exception as e2:
                    logger.error("[Oracle get_table_columns] Both uppercase and quoted queries failed: %r", e2, exc_info=True)
                    raise
            else:
                logger.error("[Oracle get_table_columns] Query execution failed: %r", e, exc_info=True)
                raise
        
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
            
            # Build full data type string
            full_data_type = data_type
            if max_length and data_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"):
                full_data_type = f"{data_type}({max_length})"
            elif precision and data_type in ("NUMBER", "NUMERIC"):
                if scale is not None:
                    full_data_type = f"{data_type}({precision},{scale})"
                else:
                    full_data_type = f"{data_type}({precision})"
            
            column_dict = {
                "name": col_name,
                "data_type": full_data_type,
                "ordinal_position": int(ordinal) if ordinal else 1,
                "is_nullable": nullable == "Y",
                "default_value": str(default) if default else None,
                "description": None,
                "json_schema": {
                    "data_type": data_type,
                    "max_length": max_length,
                    "precision": precision,
                    "scale": scale
                }
            }
            columns.append(column_dict)
        
        return columns

    def _extract_table_properties(
        self,
        cursor,
        schema: str,
        table: str
    ) -> Dict[str, Any]:
        """Extract comprehensive table properties.

        Args:
            cursor: Database cursor
            schema: Schema name
            table: Table name

        Returns:
            Dictionary with table properties
        """
        properties = {}
        
        try:
            # Get row count
            count_query = """
                SELECT COUNT(*) 
                FROM {}.{}
            """.format(schema, table)
            cursor.execute(count_query)
            row_count = cursor.fetchone()[0]
            properties["row_count"] = row_count or 0
            
            # Get primary keys - use string formatting
            schema_upper = schema.upper()
            table_upper = table.upper()
            pk_query = f"""
                SELECT cols.COLUMN_NAME
                FROM ALL_CONSTRAINTS cons
                INNER JOIN ALL_CONS_COLUMNS cols 
                    ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
                    AND cons.OWNER = cols.OWNER
                    AND cons.TABLE_NAME = cols.TABLE_NAME
                WHERE cons.OWNER = '{schema_upper}'
                  AND cons.TABLE_NAME = '{table_upper}'
                  AND cons.CONSTRAINT_TYPE = 'P'
                ORDER BY cols.POSITION
            """
            cursor.execute(pk_query)
            pk_columns = [row[0] for row in cursor.fetchall()]
            if pk_columns:
                properties["primary_keys"] = pk_columns
            
        except Exception as e:
            logger.debug(f"Could not extract all table properties for {schema}.{table}: {e}")
        
        return properties

    def extract_data(
        self,
        database: str,
        schema: str,
        table_name: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Extract actual data from an Oracle table.
        
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
            conn = self.connect()
            cursor = conn.cursor()
            
            # For Oracle, schema is typically the username
            # Use uppercase for schema/table in queries unless they were created with quotes
            # Try without quotes first (Oracle converts unquoted identifiers to uppercase)
            schema_upper = schema.upper()
            table_upper = table_name.upper()
            
            # Get total row count - try without quotes first (uppercase)
            # Note: For COUNT queries, we need to use table names directly, not parameterized
            # But we'll escape them properly to prevent SQL injection
            try:
                # Escape schema and table names for safety (they should already be validated)
                schema_escaped = schema_upper.replace('"', '""')
                table_escaped = table_upper.replace('"', '""')
                count_query = f'SELECT COUNT(*) FROM "{schema_escaped}"."{table_escaped}"'
                logger.info("[Oracle get_table_data] Executing count query (uppercase): %r", count_query)
                cursor.execute(count_query)
                total_rows = cursor.fetchone()[0]
                use_quotes = False
            except Exception as e1:
                # If that fails, try with quotes (for case-sensitive identifiers)
                try:
                    schema_escaped_orig = schema.replace('"', '""')
                    table_escaped_orig = table_name.replace('"', '""')
                    count_query = f'SELECT COUNT(*) FROM "{schema_escaped_orig}"."{table_escaped_orig}"'
                    logger.info("[Oracle get_table_data] Executing count query (original case): %r", count_query)
                    cursor.execute(count_query)
                    total_rows = cursor.fetchone()[0]
                    use_quotes = True
                except Exception as e2:
                    logger.error("[Oracle get_table_data] Could not access table %s.%s: %r (uppercase error: %r)", schema, table_name, e2, e1, exc_info=True)
                    raise
            
            # Get column names - use parameterized query to prevent string formatting issues
            columns_query = """
                SELECT COLUMN_NAME
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = :1 AND TABLE_NAME = :2
                ORDER BY COLUMN_ID
            """
            logger.info("[Oracle get_table_data] Getting column names with: schema=%r, table=%r", schema_upper, table_upper)
            cursor.execute(columns_query, (schema_upper, table_upper))
            column_names = [row[0] for row in cursor.fetchall()]
            
            if not column_names:
                logger.warning("[Oracle get_table_data] No columns found for table %s.%s", schema, table_name)
                cursor.close()
                conn.close()
                return {
                    "rows": [],
                    "row_count": 0,
                    "total_rows": 0,
                    "has_more": False,
                    "column_names": []
                }
            
            # Build SELECT query with pagination
            if use_quotes:
                # Use quoted identifiers for case-sensitive names
                column_list = ", ".join([f'"{col}"' for col in column_names])
                base_query = f'SELECT {column_list} FROM "{schema}"."{table_name}"'
            else:
                # Use unquoted identifiers (Oracle converts to uppercase)
                column_list = ", ".join([col for col in column_names])
                base_query = f'SELECT {column_list} FROM {schema_upper}.{table_upper}'
            
            # Add pagination using OFFSET/FETCH (Oracle 12c+) or ROWNUM (Oracle 11g and earlier)
            if limit is not None:
                # Use modern OFFSET/FETCH syntax (Oracle 12c+)
                data_query = f"{base_query} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
            else:
                data_query = base_query
            
            cursor.execute(data_query)
            rows = cursor.fetchall()
            
            # Convert rows to list of lists
            rows_list = [list(row) for row in rows]
            
            cursor.close()
            conn.close()
            
            return {
                "rows": rows_list,
                "row_count": len(rows_list),
                "total_rows": total_rows,
                "has_more": (offset + len(rows_list)) < total_rows if limit else False,
                "column_names": column_names
            }
            
        except Exception as e:
            logger.error("[Oracle get_table_data] Error extracting data from Oracle: %r", e, exc_info=True)
            raise

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
            schema: Schema name (optional, uses config default or username if not provided)

        Returns:
            List of column dictionaries
        """
        schema_name = schema or self.config.get("schema") or self.config.get("user")
        
        if not schema_name:
            raise ValueError("Schema name is required")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            columns = self._extract_columns_internal(cursor, schema_name, table)
            
            cursor.close()
            conn.close()
            return columns
            
        except Exception as e:
            logger.error("[Oracle get_table_columns] Failed to get table columns: %r", e, exc_info=True)
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
            schema: Schema name (optional, uses config default or username if not provided)

        Returns:
            List of primary key column names
        """
        schema_name = schema or self.config.get("schema") or self.config.get("user")
        
        if not schema_name:
            raise ValueError("Schema name is required")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Use string formatting for schema and table to avoid bind variable issues
            schema_upper = schema_name.upper()
            table_upper = table.upper()
            query = f"""
                SELECT cols.COLUMN_NAME
                FROM ALL_CONSTRAINTS cons
                INNER JOIN ALL_CONS_COLUMNS cols 
                    ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
                    AND cons.OWNER = cols.OWNER
                    AND cons.TABLE_NAME = cols.TABLE_NAME
                WHERE cons.OWNER = '{schema_upper}'
                  AND cons.TABLE_NAME = '{table_upper}'
                  AND cons.CONSTRAINT_TYPE = 'P'
                ORDER BY cols.POSITION
            """
            cursor.execute(query)
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            return primary_keys
            
        except Exception as e:
            logger.error("[Oracle get_primary_keys] Failed to get primary keys: %r", e, exc_info=True)
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
        """Perform full load for specified Oracle tables.
        
        Args:
            tables: List of table names to extract
            database: Database name (optional, uses config default if not provided)
            schema: Schema name (optional, uses config default or username if not provided)
            include_schema: Whether to extract schema information
            include_data: Whether to extract data rows
            data_limit: Maximum rows per table (None for all rows)
            
        Returns:
            Dictionary containing full load results
        """
        schema_name = schema or self.config.get("schema") or self.config.get("user")
        db = database or self.config.get("database")
        
        # Extract SCN/offset metadata
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

