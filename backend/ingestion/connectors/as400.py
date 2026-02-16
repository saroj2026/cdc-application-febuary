"""AS400/IBM i connector for metadata extraction."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import pyodbc

    AS400_AVAILABLE = True
except ImportError:
    AS400_AVAILABLE = False
    pyodbc = None  # type: ignore

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


class AS400Connector(BaseConnector):
    """Connector for extracting metadata from AS400/IBM i."""

    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize AS400 connector with connection configuration.

        Args:
            connection_config: Dictionary containing:
                - host or server: Server hostname or IP (required)
                - port: Port number (optional, default: 446)
                - database or library: Database/library name (required)
                - username or user: Username (required)
                - password: Password (required)
                - driver: ODBC driver name (optional, default: IBM i Access ODBC Driver)
                - schema: Schema/library name (optional)
        """
        if not AS400_AVAILABLE:
            raise ImportError(
                "pyodbc is not installed. "
                "Install it with: pip install pyodbc"
            )

        # Normalize field names
        if "username" in connection_config and "user" not in connection_config:
            connection_config["user"] = connection_config["username"]
        if "host" in connection_config and "server" not in connection_config:
            connection_config["server"] = connection_config["host"]
        if "library" in connection_config and "database" not in connection_config:
            connection_config["database"] = connection_config["library"]
        
        super().__init__(connection_config)

    def _validate_config(self) -> None:
        """Validate that required connection parameters are present."""
        required = ["server", "user", "password"]
        # Database/library is optional - can use library from additional_config
        missing = [key for key in required if not self.config.get(key)]
        if missing:
            raise ValueError(f"Missing required connection parameters: {', '.join(missing)}")
        
        # For AS400, we need either database or library in additional_config
        if not self.config.get("database") and not self.config.get("additional_config", {}).get("library"):
            raise ValueError("Either 'database' or 'library' in additional_config must be provided for AS400 connection")

    def _build_connection_string(self) -> str:
        """Build AS400/IBM i connection string.
        
        Returns:
            Connection string for pyodbc
        """
        server = self.config.get("server")
        if not server:
            raise ValueError("Server is required for AS400 connection")
        
        port = self.config.get("port", 446)  # Default AS400 port
        # Use library from additional_config if database is not provided
        database = self.config.get("database") or self.config.get("additional_config", {}).get("library", "")
        user = self.config.get("user")
        password = self.config.get("password")
        
        if not user:
            raise ValueError("User is required for AS400 connection")
        if not password:
            raise ValueError("Password is required for AS400 connection")
        
        driver = self.config.get("driver")
        
        # Try to detect available ODBC driver if not specified
        if not driver:
            driver = self._detect_odbc_driver()
        
        # If still no driver, try to find any IBM i driver
        if not driver:
            try:
                if pyodbc:
                    available_drivers = pyodbc.drivers()
                    # More specific filter to avoid SQL Server drivers
                    ibm_drivers = [
                        d for d in available_drivers 
                        if any(keyword in d.upper() for keyword in ["IBM", "AS400", "ISERIES"]) 
                        and ("ODBC" in d.upper() or "DRIVER" in d.upper())
                        and "SQL SERVER" not in d.upper()  # Explicitly exclude SQL Server
                    ]
                    if ibm_drivers:
                        driver = ibm_drivers[0]
                        logger.warning(f"Using detected IBM driver: {driver}")
                    else:
                        # List available drivers for debugging
                        logger.error(f"No IBM i Access ODBC Driver found. Available drivers: {available_drivers}")
                        available_list = ', '.join(available_drivers[:10]) if len(available_drivers) > 10 else ', '.join(available_drivers)
                        error_msg = (
                            "No IBM i Access ODBC Driver found on this system. "
                            "To connect to AS400/IBM i, you need to install IBM i Access Client Solutions.\n\n"
                            "Installation Steps:\n"
                            "1. Download IBM i Access Client Solutions from IBM's website\n"
                            "2. Install the software (includes ODBC driver)\n"
                            "3. Restart the application after installation\n\n"
                            f"Currently available ODBC drivers: {available_list}"
                        )
                        raise ValueError(error_msg)
                else:
                    raise ValueError("pyodbc not available. Cannot create connection.")
            except ValueError:
                raise  # Re-raise ValueError as-is
            except Exception as e:
                error_msg = (
                    f"Error detecting ODBC driver for AS400/IBM i: {str(e)}. "
                    f"Please install IBM i Access Client Solutions."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Validate driver name is not empty
        if not driver or not driver.strip():
            raise ValueError("ODBC driver name cannot be empty. Please specify 'driver' in connection config or install IBM i Access Client Solutions.")
        
        # Build connection string for AS400/IBM i
        # Format: DRIVER={IBM i Access ODBC Driver};SYSTEM=server;UID=user;PWD=password;DBQ=database
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SYSTEM={server};"
            f"UID={user};"
            f"PWD={password};"
        )
        
        # Add database/library if provided
        if database:
            conn_str += f"DBQ={database};"
        
        # Add port if not default
        if port != 446:
            conn_str += f"PORT={port};"
        
        logger.info(f"AS400 connection string: DRIVER={{...}};SYSTEM={server};UID={user};DBQ={database if database else '(none)'};PORT={port}")
        return conn_str

    def _detect_odbc_driver(self) -> Optional[str]:
        """Detect available AS400/IBM i ODBC driver.
        
        Returns:
            Driver name if found, None otherwise
        """
        if not pyodbc:
            return None
        
        try:
            available_drivers = pyodbc.drivers()
            logger.debug(f"Available ODBC drivers: {available_drivers}")
            
            # Common AS400/IBM i driver names (exact matches first)
            driver_patterns = [
                "IBM i Access ODBC Driver",
                "IBM i Access ODBC Driver 64-bit",
                "IBM i Access ODBC Driver 32-bit",
                "IBM i Access",
                "iSeries Access ODBC Driver",
                "IBM DB2 ODBC Driver",
            ]
            
            # Try exact matches first
            for pattern in driver_patterns:
                if pattern in available_drivers:
                    logger.info(f"Found AS400 driver (exact match): {pattern}")
                    return pattern
            
            # Try partial matches - be very specific to avoid SQL Server
            for driver in available_drivers:
                driver_upper = driver.upper()
                # Explicitly exclude SQL Server drivers
                if "SQL SERVER" in driver_upper or "SQLSERVER" in driver_upper:
                    continue
                # Look for IBM/AS400/iSeries keywords
                if any(keyword in driver_upper for keyword in ["IBM", "AS400", "ISERIES"]):
                    # Must have ODBC or DRIVER in name
                    if "ODBC" in driver_upper or "DRIVER" in driver_upper:
                        # Additional check: should not be generic DB2 (could be SQL Server DB2)
                        if "DB2" in driver_upper and "IBM" not in driver_upper:
                            continue
                        logger.info(f"Found potential AS400 driver: {driver}")
                        return driver
            
            logger.warning(f"No AS400/IBM i ODBC driver detected. Available drivers: {available_drivers}")
            return None
            
        except Exception as e:
            logger.error(f"Error detecting ODBC driver: {e}")
            return None

    def connect(self):
        """Establish connection to AS400/IBM i.

        Returns:
            AS400 connection object (pyodbc.Connection)
        """
        try:
            conn_str = self._build_connection_string()
            conn = pyodbc.connect(conn_str, timeout=30)
            logger.info(f"Connected to AS400/IBM i: {self.config['server']}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to AS400/IBM i: {e}")
            raise

    def disconnect(self, connection) -> None:
        """Close connection to AS400/IBM i.

        Args:
            connection: pyodbc connection object
        """
        try:
            if connection:
                connection.close()
                logger.info("Disconnected from AS400/IBM i")
        except Exception as e:
            logger.warning(f"Error disconnecting from AS400/IBM i: {e}")

    def test_connection(self) -> bool:
        """Test connection to AS400/IBM i.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            cursor.fetchone()
            cursor.close()
            self.disconnect(conn)
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_version(self) -> str:
        """Get AS400/IBM i version information.

        Returns:
            Version string
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            # Query system information
            cursor.execute("SELECT OS_VERSION, OS_RELEASE FROM SYSIBMADM.ENV_SYS_INFO FETCH FIRST 1 ROW ONLY")
            row = cursor.fetchone()
            cursor.close()
            self.disconnect(conn)
            
            if row:
                return f"IBM i {row[0]}.{row[1]}"
            else:
                return "IBM i (version unknown)"
        except Exception as e:
            logger.warning(f"Could not get version: {e}")
            return "IBM i"

    def list_databases(self) -> List[str]:
        """List databases/libraries on AS400/IBM i.

        Returns:
            List of database/library names
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Query system catalog for libraries
            cursor.execute("""
                SELECT SCHEMA_NAME 
                FROM QSYS2.SYSSCHEMAS 
                WHERE SCHEMA_TYPE = 'L' 
                ORDER BY SCHEMA_NAME
            """)
            
            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            self.disconnect(conn)
            
            return databases
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
            raise

    def list_schemas(self, database: Optional[str] = None) -> List[str]:
        """List schemas/libraries on AS400/IBM i.

        Args:
            database: Database name (optional, uses connection database if not provided)

        Returns:
            List of schema/library names
        """
        return self.list_databases()

    def list_tables(self, schema: Optional[str] = None, database: Optional[str] = None) -> List[ConnectorTable]:
        """List tables in a schema/library.

        Args:
            schema: Schema/library name (optional, uses connection schema if not provided)
            database: Database name (optional, uses connection database if not provided)

        Returns:
            List of ConnectorTable objects
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Use provided schema or connection schema
            target_schema = schema or self.config.get("schema") or self.config.get("database")
            
            if not target_schema:
                raise ValueError("Schema/library name is required")
            
            # Query system catalog for tables
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                FROM QSYS2.SYSTABLES
                WHERE TABLE_SCHEMA = ?
                AND TABLE_TYPE IN ('T', 'P')  -- T = Table, P = Physical file
                ORDER BY TABLE_NAME
            """, (target_schema,))
            
            tables = []
            for row in cursor.fetchall():
                table_schema, table_name, table_type = row
                fully_qualified = f"{table_schema}.{table_name}"
                
                table = ConnectorTable(
                    fully_qualified_name=fully_qualified,
                    name=table_name,
                    service_fully_qualified_name=fully_qualified,
                    database_schema=table_schema,
                    table_type="table" if table_type == "T" else "physical_file"
                )
                tables.append(table)
            
            cursor.close()
            self.disconnect(conn)
            
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            raise

    def get_table_columns(self, table_name: str, schema: Optional[str] = None) -> List[ConnectorColumn]:
        """Get columns for a table.

        Args:
            table_name: Table name
            schema: Schema/library name (optional)

        Returns:
            List of ConnectorColumn objects
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            target_schema = schema or self.config.get("schema") or self.config.get("database")
            
            if not target_schema:
                raise ValueError("Schema/library name is required")
            
            # Query system catalog for columns
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    ORDINAL_POSITION,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM QSYS2.SYSCOLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, (target_schema, table_name))
            
            columns = []
            for row in cursor.fetchall():
                col_name, data_type, ordinal_pos, is_nullable, default_val, char_max_len, num_precision, num_scale = row
                
                # Build full data type string
                full_type = data_type
                if char_max_len:
                    full_type += f"({char_max_len})"
                elif num_precision:
                    if num_scale:
                        full_type += f"({num_precision},{num_scale})"
                    else:
                        full_type += f"({num_precision})"
                
                column = ConnectorColumn(
                    name=col_name,
                    data_type=full_type,
                    ordinal_position=ordinal_pos,
                    is_nullable=is_nullable == "YES",
                    default_value=default_val
                )
                columns.append(column)
            
            cursor.close()
            self.disconnect(conn)
            
            return columns
        except Exception as e:
            logger.error(f"Failed to get table columns: {e}")
            raise

    def extract_schema(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract table and column metadata (schema information).

        Args:
            database: Database/library name (optional, uses config default if not provided)
            schema: Schema/library name (optional, uses config default if not provided)
            table: Table name (optional, if None extracts all tables in schema)

        Returns:
            Dictionary containing schema information
        """
        database = database or self.config.get("database") or self.config.get("additional_config", {}).get("library")
        schema = schema or self.config.get("schema") or database
        
        if not database:
            raise ValueError("Database/library name is required")

        conn = self.connect()
        cursor = conn.cursor()
        tables_list = []

        try:
            # Query for tables
            if table:
                query = """
                    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                    FROM QSYS2.SYSTABLES
                    WHERE TABLE_SCHEMA = ?
                        AND TABLE_NAME = ?
                        AND TABLE_TYPE IN ('T', 'P')
                    ORDER BY TABLE_NAME
                """
                cursor.execute(query, (schema, table))
            else:
                query = """
                    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                    FROM QSYS2.SYSTABLES
                    WHERE TABLE_SCHEMA = ?
                        AND TABLE_TYPE IN ('T', 'P')
                    ORDER BY TABLE_NAME
                """
                cursor.execute(query, (schema,))

            for row in cursor.fetchall():
                table_schema, table_name, table_type = row
                fqn = f"{table_schema}.{table_name}"

                # Extract columns for this table
                try:
                    columns = self._extract_columns_internal(cursor, table_schema, table_name)
                except Exception as e:
                    logger.warning(f"Error extracting columns for {table_schema}.{table_name}: {e}")
                    columns = []

                # Extract table properties
                try:
                    properties = self._extract_table_properties(cursor, table_schema, table_name)
                except Exception as e:
                    logger.warning(f"Error extracting properties for {table_schema}.{table_name}: {e}")
                    properties = {}

                table_dict = {
                    "name": table_name,
                    "fully_qualified_name": fqn,
                    "columns": columns,
                    "properties": properties,
                    "table_type": "table" if table_type == "T" else "physical_file"
                }
                tables_list.append(table_dict)

            logger.info(f"Extracted {len(tables_list)} tables from AS400/IBM i")
            return {
                "database": database,
                "schema": schema,
                "tables": tables_list
            }

        except Exception as e:
            logger.error(f"Error extracting schema from AS400/IBM i: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def _extract_columns_internal(
        self,
        cursor,
        schema: str,
        table: str
    ) -> List[Dict[str, Any]]:
        """Extract column metadata for a specific table (internal method).

        Args:
            cursor: Database cursor
            schema: Schema/library name
            table: Table name

        Returns:
            List of column dictionaries
        """
        query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                ORDINAL_POSITION,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM QSYS2.SYSCOLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, (schema, table))
        columns = []

        for row in cursor.fetchall():
            col_name, data_type, ordinal_pos, is_nullable, default_val, char_max_len, num_precision, num_scale = row
            
            # Build full data type string
            full_data_type = data_type
            if char_max_len:
                full_data_type = f"{data_type}({char_max_len})"
            elif num_precision:
                if num_scale:
                    full_data_type = f"{data_type}({num_precision},{num_scale})"
                else:
                    full_data_type = f"{data_type}({num_precision})"

            column_dict = {
                "name": col_name,
                "data_type": full_data_type,
                "ordinal_position": int(ordinal_pos) if ordinal_pos else 1,
                "is_nullable": is_nullable == "YES",
                "default_value": str(default_val) if default_val else None,
                "json_schema": {
                    "data_type": data_type,
                    "max_length": char_max_len,
                    "precision": num_precision,
                    "scale": num_scale
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
            schema: Schema/library name
            table: Table name

        Returns:
            Dictionary with table properties
        """
        properties = {}

        try:
            # Get row count
            count_query = "SELECT COUNT(*) FROM {}.{}".format(schema, table)
            cursor.execute(count_query)
            row_count = cursor.fetchone()[0]
            properties["row_count"] = row_count if row_count else 0

            # Get primary keys
            pk_query = """
                SELECT COLUMN_NAME
                FROM QSYS2.SYSCSTCOL
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    AND CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(pk_query, (schema, table))
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
        """Extract actual data rows from a table.

        Args:
            database: Database/library name
            schema: Schema/library name
            table_name: Table name
            limit: Maximum number of rows to extract (None for all rows)
            offset: Number of rows to skip (for pagination)

        Returns:
            Dictionary containing data extraction results
        """
        conn = self.connect()
        cursor = conn.cursor()

        try:
            # Get total row count
            count_query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]

            # Get column names
            columns_query = """
                SELECT COLUMN_NAME
                FROM QSYS2.SYSCOLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(columns_query, (schema, table_name))
            column_names = [row[0] for row in cursor.fetchall()]

            if not column_names:
                logger.warning(f"No columns found for table {schema}.{table_name}")
                return {
                    "rows": [],
                    "row_count": 0,
                    "total_rows": 0,
                    "has_more": False,
                    "column_names": []
                }

            # Build SELECT query
            column_list = ", ".join(column_names)
            data_query = f"SELECT {column_list} FROM {schema}.{table_name}"

            # Add pagination (AS400/IBM i uses FETCH FIRST ... OFFSET ...)
            if limit is not None:
                data_query = f"{data_query} OFFSET {offset} ROWS FETCH FIRST {limit} ROWS ONLY"
                cursor.execute(data_query)
            else:
                cursor.execute(data_query)

            # Fetch all rows
            rows = cursor.fetchall()
            row_data = [list(row) for row in rows]

            # Check if there are more rows
            has_more = False
            if limit is not None:
                rows_extracted = len(row_data)
                has_more = (offset + rows_extracted) < total_rows

            logger.info(
                f"Extracted {len(row_data)} rows from {schema}.{table_name} "
                f"(total: {total_rows}, has_more: {has_more})"
            )

            return {
                "rows": row_data,
                "row_count": len(row_data),
                "total_rows": total_rows,
                "has_more": has_more,
                "column_names": column_names
            }

        except Exception as e:
            logger.error(f"Error extracting data from {schema}.{table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def extract_lsn_offset(
        self,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract LSN/offset information from AS400/IBM i.
        
        Note: AS400/IBM i uses journaling, not LSN like PostgreSQL.
        This method returns journal information.

        Args:
            database: Database/library name (optional, uses config default if not provided)

        Returns:
            Dictionary containing journal/LSN information
        """
        database = database or self.config.get("database") or self.config.get("additional_config", {}).get("library")
        if not database:
            raise ValueError("Database/library name is required")

        conn = self.connect()
        cursor = conn.cursor()

        try:
            metadata = {}

            # Get journal information from additional_config
            journal_library = self.config.get("additional_config", {}).get("journal_library", "QSYS")
            logger.info(f"Extracting journal/LSN for AS400 database {database}, journal library: {journal_library}")

            # Query journal information
            try:
                cursor.execute("""
                    SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_STATUS
                    FROM QSYS2.JOURNAL_INFO
                    WHERE JOURNAL_LIBRARY = ?
                    FETCH FIRST 1 ROW ONLY
                """, (journal_library,))
                row = cursor.fetchone()
                
                if row:
                    metadata["journal_library"] = row[0]
                    metadata["journal_name"] = row[1]
                    metadata["journal_status"] = row[2]
                    logger.info(f"Found journal: {row[0]}/{row[1]}, status: {row[2]}")
                else:
                    logger.warning(f"Journal not found in QSYS2.JOURNAL_INFO for library {journal_library}")
                    metadata["journal_library"] = journal_library
                    metadata["journal_name"] = None
                    metadata["journal_status"] = "unknown"
            except Exception as e:
                logger.warning(f"Could not query journal information from QSYS2.JOURNAL_INFO: {e}")
                logger.info(f"Using journal library from config: {journal_library}")
                metadata["journal_library"] = journal_library
                metadata["journal_name"] = None
                metadata["journal_status"] = "unknown"

            # Get IBM i version
            try:
                cursor.execute("SELECT OS_VERSION, OS_RELEASE FROM SYSIBMADM.ENV_SYS_INFO FETCH FIRST 1 ROW ONLY")
                version_row = cursor.fetchone()
                if version_row:
                    metadata["ibm_i_version"] = f"{version_row[0]}.{version_row[1]}"
            except Exception:
                pass

            # Use timestamp as LSN equivalent for AS400
            # Format: JOURNAL:{library}:{timestamp} - This will be used by Debezium to start CDC
            current_timestamp = datetime.utcnow().isoformat()
            lsn_value = f"JOURNAL:{journal_library}:{current_timestamp}"
            
            lsn_result = {
                "lsn": lsn_value,
                "offset": None,
                "timestamp": current_timestamp,
                "database": database,
                "metadata": metadata
            }

            logger.info(f"✅ Extracted journal/LSN info for database {database}: {lsn_value}")
            return lsn_result

        except Exception as e:
            logger.error(f"Error extracting journal/LSN info from AS400/IBM i: {e}")
            # Even on error, create a fallback LSN so CDC can start from a known point
            journal_library = self.config.get("additional_config", {}).get("journal_library", "JRNRCV")
            current_timestamp = datetime.utcnow().isoformat()
            fallback_lsn = f"JOURNAL:{journal_library}:{current_timestamp}"
            logger.warning(f"Using fallback LSN due to extraction error: {fallback_lsn}")
            return {
                "lsn": fallback_lsn,  # Always return a valid LSN, even on error
                "offset": None,
                "timestamp": current_timestamp,
                "database": database,
                "metadata": {"error": str(e), "fallback": True}
            }
        finally:
            cursor.close()
            conn.close()

    def full_load(
        self,
        tables: List[str],
        database: Optional[str] = None,
        schema: Optional[str] = None,
        include_schema: bool = True,
        include_data: bool = True,
        data_limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Perform full load for specified tables.

        Extracts schema and/or data for the specified tables, including
        journal/LSN metadata for tracking purposes.

        Args:
            tables: List of table names to extract
            database: Database/library name (optional, uses config default if not provided)
            schema: Schema/library name (optional, uses config default if not provided)
            include_schema: Whether to extract schema information
            include_data: Whether to extract data rows
            data_limit: Maximum rows per table (None for all rows)

        Returns:
            Dictionary containing full load results
        """
        database = database or self.config.get("database") or self.config.get("additional_config", {}).get("library")
        schema = schema or self.config.get("schema") or database

        if not database:
            raise ValueError("Database/library name is required")

        # Extract journal/LSN metadata
        lsn_offset = self.extract_lsn_offset(database=database)

        result = {
            "database": database,
            "schema": schema,
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
            table_result = {
                "table_name": table_name,
                "schema": None,
                "data": None,
                "metadata": {}
            }

            try:
                # Extract schema if requested
                if include_schema:
                    schema_info = self.extract_schema(
                        database=database,
                        schema=schema,
                        table=table_name
                    )
                    # Find the specific table in the results
                    table_info = next(
                        (t for t in schema_info.get("tables", []) if t["name"] == table_name),
                        None
                    )
                    if table_info:
                        table_result["schema"] = table_info

                # Extract data if requested
                if include_data:
                    data_info = self.extract_data(
                        database=database,
                        schema=schema,
                        table_name=table_name,
                        limit=data_limit,
                        offset=0
                    )
                    table_result["data"] = data_info

                result["metadata"]["tables_successful"] += 1
                logger.info(f"Successfully loaded table {schema}.{table_name}")

            except Exception as e:
                logger.error(f"Error loading table {schema}.{table_name}: {e}")
                table_result["metadata"]["error"] = str(e)
                result["metadata"]["tables_failed"] += 1

            result["tables"].append(table_result)

        logger.info(
            f"Full load completed: {result['metadata']['tables_successful']} successful, "
            f"{result['metadata']['tables_failed']} failed"
        )
        return result

