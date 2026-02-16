"""SQL Server connector for metadata extraction."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import pyodbc

    SQLSERVER_AVAILABLE = True
except ImportError:
    SQLSERVER_AVAILABLE = False
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


class SQLServerConnector(BaseConnector):
    """Connector for extracting metadata from SQL Server."""

    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize SQL Server connector with connection configuration.

        Args:
            connection_config: Dictionary containing:
                - server: Server hostname or IP (required)
                - port: Port number (optional, default: 1433)
                - database: Database name (optional)
                - username or user: Username (required)
                - password: Password (required)
                - driver: ODBC driver name (optional, default: ODBC Driver 17 for SQL Server)
                - trust_server_certificate: Trust server certificate (optional, default: False)
                - encrypt: Encrypt connection (optional, default: True)
                - schema: Schema name (optional, default: dbo)
        """
        if not SQLSERVER_AVAILABLE:
            raise ImportError(
                "pyodbc is not installed. "
                "Install it with: pip install pyodbc"
            )

        # Normalize field names: accept both 'username' and 'user'
        if "username" in connection_config and "user" not in connection_config:
            connection_config["user"] = connection_config["username"]
        
        super().__init__(connection_config)

    def _validate_config(self) -> None:
        """Validate that required connection parameters are present."""
        required = ["server", "user", "password"]
        missing = [key for key in required if not self.config.get(key)]
        if missing:
            raise ValueError(f"Missing required connection parameters: {', '.join(missing)}")
    
    def _escape_odbc_password(self, password: str) -> str:
        """Escape special characters in password for ODBC connection strings.
        
        ODBC connection strings require special characters to be escaped:
        - { must be escaped as {{ (double brace) - this is critical
        - } must be escaped as }} (double brace) - this is critical
        - ; (semicolon) is a delimiter - ODBC Driver 18 may not support it in passwords
        - = (equals) separates key=value pairs - may cause issues
        
        For ODBC Driver 18, we need to properly escape braces and handle other special chars.
        Note: Some ODBC drivers don't support certain special characters in passwords at all.
        
        Args:
            password: Raw password string
            
        Returns:
            Escaped password string safe for ODBC connection strings
        """
        if not password:
            return ""
        
        # Convert to string and handle None
        password_str = str(password) if password is not None else ""
        if not password_str:
            return ""
        
        # Critical: Escape braces first (these MUST be escaped for ODBC)
        # { must become {{ and } must become }}
        escaped = password_str.replace("{", "{{")
        escaped = escaped.replace("}", "}}")
        
        # For semicolons and equals, ODBC Driver 18 may not support URL encoding
        # Instead, we'll try to escape them or handle them differently
        # However, if the password contains these characters, it might not work with ODBC
        # The best solution is to URL-encode them and hope the driver supports it
        # If not, the user may need to change their password
        
        # For semicolons and equals, ODBC Driver 18 has issues
        # Try multiple approaches:
        # 1. First try URL encoding (some drivers support this)
        # 2. If that doesn't work, we'll need to handle it differently
        
        # URL-encode problematic characters
        # Note: ODBC may not decode URL-encoded passwords, but it's worth trying
        escaped = escaped.replace(";", "%3B")  # Semicolon - connection string delimiter
        escaped = escaped.replace("=", "%3D")   # Equals - key=value separator
        
        # Also handle backslashes which can cause issues
        escaped = escaped.replace("\\", "\\\\")
        
        # Log a warning if password contains problematic characters
        problematic_chars = set(password_str) & set(';={}\\')
        if problematic_chars:
            logger.warning(
                f"Password contains special characters that may cause ODBC connection issues: {problematic_chars}. "
                f"Attempting to escape/encode them. If connection still fails, consider changing the password."
            )
        
        logger.debug(f"Password escaped: original length={len(password_str)}, escaped length={len(escaped)}, "
                    f"has_special_chars={bool(problematic_chars)}")
        
        return escaped

    def _build_connection_string(self) -> str:
        """Build SQL Server connection string.
        
        Returns:
            Connection string for pyodbc
        """
        server = self.config["server"]
        port = self.config.get("port", 1433)
        database = self.config.get("database", "master")
        user = self.config["user"]
        password = self.config.get("password")
        driver = self.config.get("driver")
        trust_cert = self.config.get("trust_server_certificate", False)
        encrypt = self.config.get("encrypt", True)
        
        # Validate password is not None or empty
        if password is None:
            raise ValueError("Password cannot be None for SQL Server connection")
        if not isinstance(password, str) and password is not None:
            password = str(password)
        if password == "":
            raise ValueError("Password cannot be empty for SQL Server connection")
        
        # Escape password for ODBC connection string
        escaped_password = self._escape_odbc_password(password)
        
        # Try to detect available ODBC driver if not specified
        if not driver:
            driver = self._detect_odbc_driver()
        
        # Build connection string
        # Always try to use a driver if available, as DSN-less connections without driver often fail
        if driver:
            # Ensure driver name is properly formatted (with braces for pyodbc)
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={escaped_password};"
                f"TrustServerCertificate={'yes' if trust_cert else 'no'};"
                f"Encrypt={'yes' if encrypt else 'no'};"
            )
            logger.info(f"Using ODBC driver: {driver}")
            logger.debug(f"Connection string (password hidden): DRIVER={{{driver}}};SERVER={server},{port};DATABASE={database};UID={user};...")
        else:
            # If no driver detected, try to get any available driver from pyodbc
            try:
                if pyodbc:
                    available_drivers = pyodbc.drivers()
                    if available_drivers:
                        # Use the first available driver as last resort
                        fallback_driver = available_drivers[0]
                        logger.warning(f"No SQL Server driver detected, using fallback driver: {fallback_driver}")
                        conn_str = (
                            f"DRIVER={{{fallback_driver}}};"
                            f"SERVER={server},{port};"
                            f"DATABASE={database};"
                            f"UID={user};"
                            f"PWD={escaped_password};"
                            f"TrustServerCertificate={'yes' if trust_cert else 'no'};"
                            f"Encrypt={'yes' if encrypt else 'no'};"
                        )
                    else:
                        raise ValueError("No ODBC drivers available. Please install Microsoft ODBC Driver for SQL Server.")
                else:
                    raise ValueError("pyodbc not available. Cannot create connection.")
            except Exception as e:
                error_msg = (
                    f"No ODBC driver found for SQL Server. "
                    f"Please install Microsoft ODBC Driver for SQL Server. "
                    f"Error: {str(e)}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        return conn_str
    
    def _detect_odbc_driver(self) -> Optional[str]:
        """Detect available ODBC driver for SQL Server.
        
        Returns:
            Driver name if found, None otherwise
        """
        try:
            # Try to get drivers from pyodbc directly (works on Windows, macOS, Linux)
            if pyodbc:
                available_drivers = pyodbc.drivers()
                logger.debug(f"Available ODBC drivers: {available_drivers}")
                
                # Check for common SQL Server drivers in order of preference
                # Include variations that might exist on different platforms
                driver_candidates = [
                    "ODBC Driver 18 for SQL Server",
                    "ODBC Driver 17 for SQL Server",
                    "ODBC Driver 13 for SQL Server",
                    "ODBC Driver 11 for SQL Server",
                    "SQL Server Native Client 11.0",
                    "SQL Server Native Client 10.0",
                    "SQL Server",
                    "FreeTDS",  # Alternative open-source driver
                ]
                
                for driver_name in driver_candidates:
                    if driver_name in available_drivers:
                        logger.info(f"Detected ODBC driver: {driver_name}")
                        return driver_name
                
                # If no exact match, try partial matching (for macOS variations)
                for driver_name in available_drivers:
                    driver_lower = driver_name.lower()
                    if "odbc driver" in driver_lower and "sql server" in driver_lower:
                        # Check if it's version 18, 17, or 13
                        if "18" in driver_lower or "17" in driver_lower or "13" in driver_lower:
                            logger.info(f"Detected ODBC driver (partial match): {driver_name}")
                            return driver_name
                
                # Last resort: any driver containing "SQL Server"
                for driver_name in available_drivers:
                    if "sql server" in driver_name.lower():
                        logger.info(f"Detected ODBC driver (fallback): {driver_name}")
                        return driver_name
                
                logger.warning(f"No SQL Server ODBC driver found. Available drivers: {available_drivers}")
        except Exception as e:
            logger.warning(f"Could not detect ODBC driver via pyodbc: {e}")
        
        # Fallback: try subprocess method (works on Linux/macOS with odbcinst)
        try:
            import subprocess
            import shutil
            # Check if odbcinst is available
            if shutil.which("odbcinst"):
                result = subprocess.run(
                    ["odbcinst", "-q", "-d"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    drivers = result.stdout
                    logger.debug(f"ODBC drivers from odbcinst: {drivers}")
                    # Check for common SQL Server drivers
                    for driver_name in [
                        "ODBC Driver 18 for SQL Server",
                        "ODBC Driver 17 for SQL Server",
                        "ODBC Driver 13 for SQL Server",
                        "SQL Server Native Client 11.0",
                        "SQL Server",
                        "FreeTDS",
                    ]:
                        if driver_name in drivers:
                            logger.info(f"Detected ODBC driver via odbcinst: {driver_name}")
                            return driver_name
        except Exception as e:
            logger.debug(f"Could not detect ODBC driver via odbcinst: {e}")
        
        return None
    
    def _build_connection_string_with_freetds(self) -> str:
        """Build SQL Server connection string using FreeTDS driver.
        
        Returns:
            Connection string for pyodbc with FreeTDS
        """
        server = self.config["server"]
        port = self.config.get("port", 1433)
        database = self.config.get("database", "master")
        user = self.config["user"]
        password = self.config.get("password")
        trust_cert = self.config.get("trust_server_certificate", False)
        
        # Validate password is not None or empty
        if password is None:
            raise ValueError("Password cannot be None for SQL Server connection")
        if not isinstance(password, str) and password is not None:
            password = str(password)
        if password == "":
            raise ValueError("Password cannot be empty for SQL Server connection")
        
        # Escape password for ODBC connection string
        escaped_password = self._escape_odbc_password(password)
        
        # On Windows Docker, use host.docker.internal to access host machine
        # If server is a Windows hostname, try host.docker.internal
        if server and not server.startswith("host.docker.internal") and not "." in server.split(":")[0]:
            # Might be a Windows hostname, try host.docker.internal
            server = "host.docker.internal"
            logger.info(f"Using host.docker.internal to access Windows host SQL Server")
        
        conn_str = (
            f"DRIVER={{FreeTDS}};"
            f"SERVER={server};"
            f"PORT={port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={escaped_password};"
            f"TDS_Version=7.4;"
        )
        
        return conn_str

    def connect(self):
        """Establish connection to SQL Server.

        Returns:
            SQL Server connection object (pyodbc.Connection)
        """
        try:
            conn_str = self._build_connection_string()
            logger.debug(f"Connection string: {conn_str.replace(self.config.get('password', ''), '***')}")
            
            # Verify driver is in connection string
            if "DRIVER=" not in conn_str:
                logger.warning("No DRIVER specified in connection string, attempting to detect and retry...")
                # Force driver detection
                driver = self._detect_odbc_driver()
                if driver:
                    logger.info(f"Detected driver: {driver}, rebuilding connection string...")
                    self.config["driver"] = driver
                    conn_str = self._build_connection_string()
                    logger.debug(f"New connection string: {conn_str.replace(self.config.get('password', ''), '***')}")
                else:
                    # Last resort: get any available driver
                    if pyodbc:
                        available_drivers = pyodbc.drivers()
                        if available_drivers:
                            fallback_driver = available_drivers[0]
                            logger.warning(f"No SQL Server driver found, using fallback: {fallback_driver}")
                            self.config["driver"] = fallback_driver
                            conn_str = self._build_connection_string()
            
            # Use pyodbc.connect with connection string
            # The password is already escaped in conn_str
            try:
                conn = pyodbc.connect(conn_str, timeout=30)
            except (pyodbc.Error, Exception) as e:
                # If connection fails with password error, try alternative methods
                error_msg = str(e)
                is_password_error = ("PWD" in error_msg or "password" in error_msg.lower() or 
                                   "08001" in error_msg or "Invalid value" in error_msg)
                
                if is_password_error:
                    logger.warning(f"ODBC password error detected. Trying alternative password handling...")
                    logger.debug(f"Error: {error_msg}")
                    
                    # Get the original password for alternative escaping
                    original_password = self.config.get("password", "")
                    # Get driver and connection parameters from config
                    driver = self.config.get("driver")
                    if not driver:
                        driver = self._detect_odbc_driver()
                    server = self.config["server"]
                    port = self.config.get("port", 1433)
                    database = self.config.get("database", "master")
                    user = self.config["user"]
                    trust_cert = self.config.get("trust_server_certificate", False)
                    encrypt = self.config.get("encrypt", True)
                    
                    if original_password and driver:
                        # Try alternative: Use password without URL encoding (just brace escaping)
                        # Some ODBC drivers don't support URL-encoded passwords
                        simple_escaped = str(original_password).replace("{", "{{").replace("}", "}}")
                        # Get the escaped password from the original connection string for comparison
                        escaped_password = self._escape_odbc_password(original_password)
                        if simple_escaped != escaped_password:
                            try:
                                alt_conn_str = (
                                    f"DRIVER={{{driver}}};"
                                    f"SERVER={server},{port};"
                                    f"DATABASE={database};"
                                    f"UID={user};"
                                    f"PWD={simple_escaped};"
                                    f"TrustServerCertificate={'yes' if trust_cert else 'no'};"
                                    f"Encrypt={'yes' if encrypt else 'no'};"
                                )
                                logger.info("Trying connection with simpler password escaping (no URL encoding)...")
                                conn = pyodbc.connect(alt_conn_str, timeout=30)
                                logger.info("Connected successfully with alternative password escaping")
                                return conn
                            except Exception as e2:
                                logger.warning(f"Alternative escaping also failed: {e2}")
                    
                    # If all else fails, provide helpful error message
                    raise ValueError(
                        f"SQL Server connection failed due to password format issue. "
                        f"ODBC Driver 18 may not support certain special characters in passwords "
                        f"(semicolons, equals signs, or braces). Please check your password. "
                        f"Original error: {error_msg}"
                    ) from e
                raise
            logger.info(f"Connected to SQL Server: {self.config['server']}")
            return conn
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to connect to SQL Server: {error_msg}")
            
            # If driver-related error, try to fix it
            if "Data source name not found" in error_msg or "no default driver specified" in error_msg:
                logger.warning("Driver-related error detected, attempting to fix...")
                # Force re-detection
                driver = self._detect_odbc_driver()
                if driver:
                    logger.info(f"Re-detected driver: {driver}, retrying connection...")
                    self.config["driver"] = driver
                    try:
                        conn_str = self._build_connection_string()
                        # Re-escape password for the new connection string
                        password = self.config.get("password")
                        if password:
                            escaped_password = self._escape_odbc_password(password)
                            # Update connection string with escaped password
                            # Extract driver and other params
                            driver = self.config.get("driver")
                            server = self.config["server"]
                            port = self.config.get("port", 1433)
                            database = self.config.get("database", "master")
                            user = self.config["user"]
                            trust_cert = self.config.get("trust_server_certificate", False)
                            encrypt = self.config.get("encrypt", True)
                            
                            conn_str = (
                                f"DRIVER={{{driver}}};"
                                f"SERVER={server},{port};"
                                f"DATABASE={database};"
                                f"UID={user};"
                                f"PWD={escaped_password};"
                                f"TrustServerCertificate={'yes' if trust_cert else 'no'};"
                                f"Encrypt={'yes' if encrypt else 'no'};"
                            )
                        
                        conn = pyodbc.connect(conn_str, timeout=30)
                        logger.info(f"Connected to SQL Server after driver fix: {self.config['server']}")
                        return conn
                    except Exception as e2:
                        logger.error(f"Retry with detected driver also failed: {e2}")
            
            # Try with FreeTDS if other drivers fail
            if "Data source name not found" in error_msg or "file not found" in error_msg:
                logger.info("Attempting connection with FreeTDS driver...")
                try:
                    server = self.config["server"]
                    # On Windows Docker, use host.docker.internal to access host machine
                    if server and not server.startswith("host.docker.internal"):
                        # Check if server is a Windows hostname (might need host.docker.internal)
                        logger.debug(f"Original server: {server}, trying host.docker.internal")
                    
                    freeTdsConnStr = self._build_connection_string_with_freetds()
                    conn = pyodbc.connect(freeTdsConnStr, timeout=30)
                    logger.info(f"Connected to SQL Server using FreeTDS: {self.config['server']}")
                    return conn
                except Exception as e2:
                    logger.error(f"FreeTDS connection also failed: {e2}")
            raise

    def extract_tables(
        self, 
        database: Optional[str] = None, 
        schema: Optional[str] = None
    ) -> List[ConnectorTable]:
        """Extract table metadata from SQL Server.

        Args:
            database: Database name (uses config default if not provided)
            schema: Schema name (uses config default or dbo if not provided)

        Returns:
            List of ConnectorTable objects with metadata
        """
        database = database or self.config.get("database", "master")
        schema = schema or self.config.get("schema", "dbo")

        conn = self.connect()
        cursor = conn.cursor()
        tables = []

        try:
            # Query SQL Server information schema for tables
            # Use brackets for SQL Server identifiers
            query = """
            SELECT 
                TABLE_CATALOG,
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = ?
                AND TABLE_SCHEMA = ?
                AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            ORDER BY TABLE_NAME
            """

            logger.info(f"Querying tables from {database}.{schema}")
            cursor.execute(query, (database, schema))

            for row in cursor.fetchall():
                catalog, schema_name, table_name, table_type = row
                fqn = f"{catalog}.{schema_name}.{table_name}"

                logger.debug(f"Extracting columns for table: {fqn}")

                # Extract columns for this table
                columns = self.extract_columns(catalog, schema_name, table_name)

                # Map SQL Server table types to our table types
                table_type_mapped = "view" if table_type == "VIEW" else "table"

                # Build service FQN
                service_fqn = f"service.{catalog.lower()}"

                # Try to get table description from extended properties
                description = self._get_table_description(catalog, schema_name, table_name)

                # Extract comprehensive table metadata
                table_properties = self._extract_table_properties(
                    catalog, schema_name, table_name, table_type
                )

                table = ConnectorTable(
                    fully_qualified_name=fqn,
                    name=table_name,
                    service_fully_qualified_name=service_fqn,
                    database_schema=schema_name,
                    table_type=table_type_mapped,
                    description=description,
                    columns=columns,
                    properties=table_properties,
                )
                tables.append(table)

            logger.info(f"Extracted {len(tables)} tables from SQL Server")
            return tables

        except Exception as e:
            logger.error(f"Error extracting tables from SQL Server: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def extract_columns(
        self, 
        database: str, 
        schema: str, 
        table: str
    ) -> List[ConnectorColumn]:
        """Extract column metadata for a specific table.

        Args:
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            List of ConnectorColumn objects with metadata
        """
        conn = self.connect()
        cursor = conn.cursor()
        columns = []

        try:
            query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                ISNULL(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) AS MAX_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                ORDINAL_POSITION,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                CHARACTER_SET_NAME,
                COLLATION_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = ?
                AND TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """

            cursor.execute(query, (database, schema, table))

            for row in cursor.fetchall():
                (
                    col_name,
                    data_type,
                    max_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal,
                    nullable,
                    default,
                    character_set,
                    collation,
                ) = row

                # Build full data type string
                full_data_type = data_type
                if max_length and data_type in ("varchar", "nvarchar", "char", "nchar", "binary", "varbinary"):
                    full_data_type = f"{data_type}({max_length})"
                elif numeric_precision and data_type in ("decimal", "numeric"):
                    if numeric_scale:
                        full_data_type = f"{data_type}({numeric_precision},{numeric_scale})"
                    else:
                        full_data_type = f"{data_type}({numeric_precision})"

                # Get column description from extended properties
                description = self._get_column_description(database, schema, table, col_name)

                # Extract comprehensive column metadata
                column_metadata = self._extract_column_metadata(
                    database, schema, table, col_name, data_type
                )
                
                # Add character set and collation from INFORMATION_SCHEMA
                if character_set:
                    column_metadata["character_set_name"] = character_set
                if collation:
                    column_metadata["collation_name"] = collation

                column = ConnectorColumn(
                    name=col_name,
                    data_type=full_data_type,
                    ordinal_position=int(ordinal) if ordinal else 1,
                    is_nullable=nullable == "YES",
                    default_value=str(default) if default else None,
                    description=description,
                    json_schema=column_metadata,
                )
                columns.append(column)

            return columns

        except Exception as e:
            logger.error(f"Error extracting columns for {database}.{schema}.{table}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def _get_table_description(
        self, 
        database: str, 
        schema: str, 
        table: str
    ) -> Optional[str]:
        """Get table description from extended properties.

        Args:
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            Table description or None
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            query = """
            SELECT value
            FROM sys.extended_properties
            WHERE major_id = OBJECT_ID(?)
                AND minor_id = 0
                AND name = 'MS_Description'
            """
            
            # Build full table name
            full_table_name = f"[{database}].[{schema}].[{table}]"
            cursor.execute(query, full_table_name)
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result[0] if result else None
        except Exception as e:
            logger.debug(f"Could not get table description for {database}.{schema}.{table}: {e}")
            return None

    def _get_column_description(
        self, 
        database: str, 
        schema: str, 
        table: str, 
        column: str
    ) -> Optional[str]:
        """Get column description from extended properties.

        Args:
            database: Database name
            schema: Schema name
            table: Table name
            column: Column name

        Returns:
            Column description or None
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            query = """
            SELECT ep.value
            FROM sys.extended_properties ep
            INNER JOIN sys.columns c ON ep.major_id = c.object_id 
                AND ep.minor_id = c.column_id
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = ?
                AND t.name = ?
                AND c.name = ?
                AND ep.name = 'MS_Description'
            """
            
            cursor.execute(query, (schema, table, column))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return result[0] if result else None
        except Exception as e:
            logger.debug(f"Could not get column description for {database}.{schema}.{table}.{column}: {e}")
            return None

    def extract_query_statistics(
        self, 
        database: Optional[str] = None,
        days_back: int = 7
    ) -> List[Dict[str, Any]]:
        """Extract query statistics and usage information from SQL Server.

        This method queries SQL Server's Dynamic Management Views (DMVs) to get:
        - Query execution statistics
        - Most frequently executed queries
        - Query performance metrics
        - Table access patterns

        Args:
            database: Database name (optional, if not provided queries all databases)
            days_back: Number of days to look back for query statistics (default: 7)

        Returns:
            List of dictionaries containing query statistics with keys:
            - query_text: The SQL query text
            - execution_count: Number of times executed
            - total_elapsed_time_ms: Total elapsed time in milliseconds
            - avg_elapsed_time_ms: Average elapsed time in milliseconds
            - total_logical_reads: Total logical reads
            - total_physical_reads: Total physical reads
            - last_execution_time: Last execution timestamp
            - database_name: Database where query was executed
            - referenced_tables: List of tables referenced in the query
        """
        conn = self.connect()
        cursor = conn.cursor()
        query_stats = []

        try:
            # Query to get query statistics from DMVs
            # This requires VIEW SERVER STATE permission
            query = """
            SELECT 
                DB_NAME(qt.dbid) AS database_name,
                qt.text AS query_text,
                qs.execution_count,
                qs.total_elapsed_time / 1000 AS total_elapsed_time_ms,
                (qs.total_elapsed_time / qs.execution_count) / 1000 AS avg_elapsed_time_ms,
                qs.total_logical_reads,
                qs.total_physical_reads,
                qs.last_execution_time,
                qs.creation_time,
                qs.plan_handle
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
            WHERE qs.last_execution_time >= DATEADD(day, ?, GETDATE())
            """
            
            if database:
                query += " AND DB_NAME(qt.dbid) = ?"
            
            query += """
            ORDER BY qs.execution_count DESC
            """

            logger.info(f"Extracting query statistics (last {days_back} days)")
            
            if database:
                cursor.execute(query, (-days_back, database))
            else:
                cursor.execute(query, -days_back)

            for row in cursor.fetchall():
                (
                    db_name,
                    query_text,
                    exec_count,
                    total_elapsed_ms,
                    avg_elapsed_ms,
                    total_logical_reads,
                    total_physical_reads,
                    last_exec_time,
                    creation_time,
                    plan_handle,
                ) = row

                # Extract referenced tables from query text (simple parsing)
                # Note: This is a basic implementation - could be enhanced with SQL parsing
                referenced_tables = self._extract_table_references(query_text, db_name)

                stat = {
                    "query_text": query_text,
                    "execution_count": exec_count,
                    "total_elapsed_time_ms": total_elapsed_ms,
                    "avg_elapsed_time_ms": avg_elapsed_ms,
                    "total_logical_reads": total_logical_reads,
                    "total_physical_reads": total_physical_reads,
                    "last_execution_time": last_exec_time.isoformat() if last_exec_time else None,
                    "creation_time": creation_time.isoformat() if creation_time else None,
                    "database_name": db_name or "unknown",
                    "referenced_tables": referenced_tables,
                }
                query_stats.append(stat)

            logger.info(f"Extracted {len(query_stats)} query statistics from SQL Server")
            return query_stats

        except Exception as e:
            # If we don't have permissions, log warning but don't fail
            if "permission" in str(e).lower() or "denied" in str(e).lower():
                logger.warning(
                    f"Could not extract query statistics (insufficient permissions): {e}. "
                    "User needs VIEW SERVER STATE permission."
                )
                return []
            logger.error(f"Error extracting query statistics from SQL Server: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def _extract_table_references(
        self, 
        query_text: str, 
        database: Optional[str] = None
    ) -> List[str]:
        """Extract table references from SQL query text.

        This is a simple parser that looks for FROM and JOIN clauses.
        For production use, consider using a proper SQL parser.

        Args:
            query_text: SQL query text
            database: Database name for building FQNs

        Returns:
            List of table fully qualified names
        """
        tables = []
        if not query_text:
            return tables

        # Simple regex to find table references
        # Look for FROM and JOIN clauses
        patterns = [
            r'(?:FROM|JOIN)\s+(?:\[?(\w+)\]?\.)?(?:\[?(\w+)\]?\.)?\[?(\w+)\]?',
            r'INTO\s+(?:\[?(\w+)\]?\.)?(?:\[?(\w+)\]?\.)?\[?(\w+)\]?',
        ]

        for pattern in patterns:
            matches = re.finditer(pattern, query_text, re.IGNORECASE)
            for match in matches:
                groups = match.groups()
                # Filter out None values
                parts = [g for g in groups if g]
                if len(parts) >= 1:
                    # Build FQN
                    if len(parts) == 3:
                        # schema.table format
                        fqn = f"{parts[0]}.{parts[1]}.{parts[2]}"
                    elif len(parts) == 2:
                        # schema.table or database.table
                        fqn = f"{parts[0]}.{parts[1]}"
                    else:
                        # Just table name
                        table_name = parts[0]
                        if database:
                            fqn = f"{database}.dbo.{table_name}"
                        else:
                            fqn = table_name
                    
                    if fqn not in tables:
                        tables.append(fqn)

        return tables

    def list_databases(self) -> List[str]:
        """List all databases accessible to the user.

        Returns:
            List of database names
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sys.databases WHERE state = 0")  # state 0 = online
            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            logger.info(f"Found {len(databases)} databases: {databases}")
            return databases
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
            raise

    def list_schemas(self, database: Optional[str] = None) -> List[str]:
        """List all schemas in a database.

        Args:
            database: Database name (uses config default if not provided)

        Returns:
            List of schema names
        """
        database = database or self.config.get("database", "master")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(f"USE [{database}]")
            cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME")
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            logger.info(f"Found {len(schemas)} schemas in {database}: {schemas}")
            return schemas
        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
            raise

    def list_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[str]:
        """List all tables in a schema.

        Args:
            database: Database name (optional)
            schema: Schema name (optional)

        Returns:
            List of table names
        """
        database = database or self.config.get("database", "master")
        schema = schema or self.config.get("schema", "dbo")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(f"USE [{database}]")
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """, schema)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            logger.info(f"Found {len(tables)} tables in {database}.{schema}: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
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
            database: Database name (optional)
            schema: Schema name (optional)

        Returns:
            List of column dictionaries
        """
        database = database or self.config.get("database", "master")
        schema = schema or self.config.get("schema", "dbo")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            # Ensure database is a string
            db_str = str(database) if database else "master"
            cursor.execute(f"USE [{db_str}]")
            
            query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    ORDINAL_POSITION,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """
            # Ensure schema and table are strings and not None
            schema_str = str(schema) if schema else "dbo"
            table_str = str(table) if table else ""
            if not table_str:
                raise ValueError("Table name cannot be empty")
            
            # Validate that schema and table are strings before passing to cursor.execute
            if not isinstance(schema_str, str):
                schema_str = str(schema_str)
            if not isinstance(table_str, str):
                table_str = str(table_str)
            
            # Clean schema and table strings - remove any special characters that might cause issues
            schema_str = schema_str.strip()
            table_str = table_str.strip()
            
            # Log before executing query - use %r to avoid formatting issues
            logger.info("[SQL Server get_table_columns] Executing query with: schema=%r, table=%r", schema_str, table_str)
            logger.info("[SQL Server get_table_columns] Query: %r", query)
            
            # Execute query with parameters as a tuple
            # Wrap in try-except to catch any pyodbc-specific errors
            try:
                cursor.execute(query, (schema_str, table_str))
                logger.info("[SQL Server get_table_columns] Query executed successfully, fetching results...")
            except Exception as query_error:
                # Import safe_error_message to handle the error safely
                try:
                    from ingestion.connection_service import safe_error_message
                    safe_query_error = safe_error_message(query_error)
                except Exception:
                    # Use string concatenation instead of f-string to avoid formatting issues
                    safe_query_error = "Query execution failed: " + type(query_error).__name__
                
                # Log the error safely - use %r (repr) to avoid any formatting issues
                try:
                    logger.error("Query execution failed. Schema: %r, Table: %r, Error: %r", 
                               schema_str, table_str, safe_query_error)
                except Exception as log_err:
                    # If logging fails, just print to avoid cascading errors
                    try:
                        print(f"Query execution failed for schema={repr(schema_str)}, table={repr(table_str)}")
                    except Exception:
                        print("Query execution failed")
                raise
            
            columns = []
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1]
                max_length = row[2]
                precision = row[3]
                scale = row[4]
                
                # Build full data type string
                full_data_type = data_type
                if max_length and data_type in ("varchar", "nvarchar", "char", "nchar"):
                    full_data_type = f"{data_type}({max_length})"
                elif precision and data_type in ("numeric", "decimal"):
                    if scale:
                        full_data_type = f"{data_type}({precision},{scale})"
                    else:
                        full_data_type = f"{data_type}({precision})"
                
                columns.append({
                    "name": col_name,
                    "data_type": full_data_type,
                    "ordinal_position": int(row[5]),
                    "is_nullable": row[6] == "YES",
                    "default_value": str(row[7]) if row[7] else None
                })
            
            cursor.close()
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass  # Ignore errors when closing connection
            return columns
            
        except Exception as e:
            # Safely get error message using a helper function
            # Import safe_error_message from connection_service
            safe_error_msg = None
            try:
                from ingestion.connection_service import safe_error_message
                safe_error_msg = safe_error_message(e)
            except Exception as import_error:
                # If import or safe_error_message fails, use ultra-safe fallback
                try:
                    # Try to get error from args without calling __str__
                    if hasattr(e, 'args') and e.args and len(e.args) > 0:
                        first_arg = e.args[0]
                        if isinstance(first_arg, str):
                            # Escape % characters immediately
                            safe_error_msg = first_arg.replace('%', '%%')
                        else:
                            # Use repr() to avoid __str__ formatting issues
                            safe_error_msg = repr(first_arg).replace('%', '%%')
                    else:
                        # Use exception type name as fallback
                        safe_error_msg = "Error type: " + type(e).__name__
                except Exception:
                    safe_error_msg = "Unknown error occurred"
            
            # Ensure we have a safe error message
            if not safe_error_msg:
                safe_error_msg = "Unknown error occurred"
            
            # Use logger with explicit formatting to avoid any issues
            # Use %r (repr) instead of %s to avoid any formatting issues
            try:
                logger.error("Failed to get table columns: %r", safe_error_msg, exc_info=True)
            except Exception as log_error:
                # If logging fails, just print to avoid cascading errors
                try:
                    # Use repr() to avoid any formatting issues
                    print("Failed to get table columns:", repr(safe_error_msg))
                except Exception:
                    print("Failed to get table columns: [Error message could not be formatted]")
            
            # Re-raise with a safe error message that won't cause formatting issues
            # Use string concatenation to build the exception message (avoid f-strings and % formatting)
            error_prefix = "Failed to get table columns: "
            try:
                # Ensure safe_error_msg is a string and escape any % characters
                if isinstance(safe_error_msg, str):
                    # Replace % with %% to prevent formatting issues
                    escaped_msg = safe_error_msg.replace('%', '%%')
                    final_error_msg = error_prefix + escaped_msg
                else:
                    # If not a string, convert safely
                    try:
                        escaped_msg = str(safe_error_msg).replace('%', '%%')
                        final_error_msg = error_prefix + escaped_msg
                    except Exception:
                        final_error_msg = error_prefix + "Unknown error"
            except Exception:
                final_error_msg = error_prefix + "Unknown error"
            
            raise Exception(final_error_msg) from e

    def get_primary_keys(
        self,
        table: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[str]:
        """Get primary key columns for a table.

        Args:
            table: Table name
            database: Database name (optional)
            schema: Schema name (optional)

        Returns:
            List of primary key column names
        """
        database = database or self.config.get("database", "master")
        schema = schema or self.config.get("schema", "dbo")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(f"USE [{database}]")
            
            query = """
                SELECT 
                    c.name AS column_name,
                    ic.key_ordinal AS key_order
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.is_primary_key = 1
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE s.name = ? AND t.name = ?
                ORDER BY ic.key_ordinal
            """
            cursor.execute(query, (schema, table))
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            return primary_keys
            
        except Exception as e:
            logger.error(f"Failed to get primary keys: {e}")
            raise

    def get_table_row_count(
        self,
        table: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> int:
        """Get row count for a table.

        Args:
            table: Table name
            database: Database name (optional)
            schema: Schema name (optional)

        Returns:
            Row count
        """
        target_database = database or self.config.get("database", "master")
        target_schema = schema or self.config.get("schema", "dbo")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(f"USE [{target_database}]")
            cursor.execute(f"SELECT COUNT(*) FROM [{target_schema}].[{table}]")
            row_count = cursor.fetchone()[0]
            result = int(row_count) if row_count else 0
            logger.debug(f"Row count for {target_database}.{target_schema}.{table}: {result}")
            cursor.close()
            conn.close()
            return result
            
        except Exception as e:
            logger.error(f"Failed to get row count for {target_database}.{target_schema}.{table}: {e}", exc_info=True)
            raise

    def get_version(self) -> str:
        """Get database version.

        Returns:
            Database version string
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return version
        except Exception as e:
            logger.error(f"Failed to get version: {e}")
            raise

    def disconnect(self, connection=None):
        """Close database connection if open.
        
        Args:
            connection: Optional connection object to close. If provided, closes it.
        
        Note: SQL Server connections are typically closed after each operation.
        This method is provided for consistency with other connectors.
        """
        # Close provided connection if given
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass  # Ignore errors when closing
        # SQL Server connections are closed after each operation
        # This is a no-op for consistency when no connection provided
        pass

    def _extract_table_properties(
        self,
        database: str,
        schema: str,
        table: str,
        table_type: str,
    ) -> Dict[str, Any]:
        """Extract comprehensive table properties including statistics, indexes, keys, etc.
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            table_type: Table type (BASE TABLE, VIEW)
            
        Returns:
            Dictionary with table properties
        """
        properties = {}
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Get table statistics (row count, size)
            try:
                stats_query = """
                SELECT 
                    p.rows AS row_count,
                    (SUM(a.total_pages) * 8) / 1024.0 AS size_mb,
                    (SUM(a.used_pages) * 8) / 1024.0 AS used_mb,
                    (SUM(a.data_pages) * 8) / 1024.0 AS data_mb
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ?
                    AND t.name = ?
                    AND t.type = 'U'
                GROUP BY p.rows
                """
                cursor.execute(stats_query, (schema, table))
                stats = cursor.fetchone()
                if stats:
                    properties["row_count"] = stats[0] if stats[0] else 0
                    properties["size_mb"] = round(float(stats[1]) if stats[1] else 0, 2)
                    properties["used_mb"] = round(float(stats[2]) if stats[2] else 0, 2)
                    properties["data_mb"] = round(float(stats[3]) if stats[3] else 0, 2)
            except Exception as e:
                logger.debug(f"Could not get table statistics: {e}")
            
            # Get primary keys
            try:
                pk_query = """
                SELECT 
                    c.name AS column_name,
                    ic.key_ordinal AS key_order
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.is_primary_key = 1
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE s.name = ?
                    AND t.name = ?
                ORDER BY ic.key_ordinal
                """
                cursor.execute(pk_query, (schema, table))
                pk_columns = [row[0] for row in cursor.fetchall()]
                if pk_columns:
                    properties["primary_keys"] = pk_columns
            except Exception as e:
                logger.debug(f"Could not get primary keys: {e}")
            
            # Get foreign keys
            try:
                fk_query = """
                SELECT 
                    fk.name AS foreign_key_name,
                    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS parent_schema,
                    OBJECT_NAME(fk.parent_object_id) AS parent_table,
                    cp.name AS parent_column,
                    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
                    OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
                    cr.name AS referenced_column
                FROM sys.foreign_keys fk
                INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id 
                    AND fkc.parent_column_id = cp.column_id
                INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id 
                    AND fkc.referenced_column_id = cr.column_id
                INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ?
                    AND t.name = ?
                """
                cursor.execute(fk_query, (schema, table))
                foreign_keys = []
                for row in cursor.fetchall():
                    foreign_keys.append({
                        "name": row[0],
                        "parent_table": f"{row[1]}.{row[2]}",
                        "parent_column": row[3],
                        "referenced_table": f"{row[4]}.{row[5]}",
                        "referenced_column": row[6],
                    })
                if foreign_keys:
                    properties["foreign_keys"] = foreign_keys
            except Exception as e:
                logger.debug(f"Could not get foreign keys: {e}")
            
            # Get indexes
            try:
                index_query = """
                SELECT 
                    i.name AS index_name,
                    i.type_desc AS index_type,
                    i.is_unique AS is_unique,
                    i.is_primary_key AS is_pk,
                    STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE s.name = ?
                    AND t.name = ?
                    AND i.type > 0  -- Exclude heap
                GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
                ORDER BY i.is_primary_key DESC, i.name
                """
                cursor.execute(index_query, (schema, table))
                indexes = []
                for row in cursor.fetchall():
                    indexes.append({
                        "name": row[0],
                        "type": row[1],
                        "is_unique": bool(row[2]),
                        "is_primary_key": bool(row[3]),
                        "columns": row[4].split(", ") if row[4] else [],
                    })
                if indexes:
                    properties["indexes"] = indexes
            except Exception as e:
                logger.debug(f"Could not get indexes: {e}")
            
            # Get constraints (unique, check)
            try:
                constraint_query = """
                SELECT 
                    cc.name AS constraint_name,
                    cc.type_desc AS constraint_type,
                    cc.definition AS constraint_definition
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.check_constraints cc ON t.object_id = cc.parent_object_id
                WHERE s.name = ?
                    AND t.name = ?
                UNION ALL
                SELECT 
                    uc.name AS constraint_name,
                    'UNIQUE_CONSTRAINT' AS constraint_type,
                    NULL AS constraint_definition
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.key_constraints uc ON t.object_id = uc.parent_object_id
                WHERE s.name = ?
                    AND t.name = ?
                    AND uc.type = 'UQ'
                """
                cursor.execute(constraint_query, (schema, table, schema, table))
                constraints = []
                for row in cursor.fetchall():
                    constraints.append({
                        "name": row[0],
                        "type": row[1],
                        "definition": row[2],
                    })
                if constraints:
                    properties["constraints"] = constraints
            except Exception as e:
                logger.debug(f"Could not get constraints: {e}")
            
            # Get view definition if it's a view
            if table_type == "VIEW":
                try:
                    view_query = """
                    SELECT definition
                    FROM sys.sql_modules m
                    INNER JOIN sys.views v ON m.object_id = v.object_id
                    INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
                    WHERE s.name = ?
                        AND v.name = ?
                    """
                    cursor.execute(view_query, (schema, table))
                    view_def = cursor.fetchone()
                    if view_def and view_def[0]:
                        properties["view_definition"] = view_def[0]
                except Exception as e:
                    logger.debug(f"Could not get view definition: {e}")
            
            # Get table creation and modification dates
            try:
                date_query = """
                SELECT 
                    create_date,
                    modify_date
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ?
                    AND t.name = ?
                """
                cursor.execute(date_query, (schema, table))
                dates = cursor.fetchone()
                if dates:
                    properties["create_date"] = dates[0].isoformat() if dates[0] else None
                    properties["modify_date"] = dates[1].isoformat() if dates[1] else None
            except Exception as e:
                logger.debug(f"Could not get table dates: {e}")
            
            return properties
            
        except Exception as e:
            logger.warning(f"Error extracting table properties for {database}.{schema}.{table}: {e}")
            return {}
        finally:
            cursor.close()
            conn.close()
    
    def _extract_column_metadata(
        self,
        database: str,
        schema: str,
        table: str,
        column: str,
        data_type: str,
    ) -> Dict[str, Any]:
        """Extract comprehensive column metadata including character set, collation, etc.
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            column: Column name
            data_type: Base data type
            
        Returns:
            Dictionary with column metadata
        """
        metadata = {}
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Get detailed column information from sys.columns
            col_query = """
            SELECT 
                c.column_id,
                c.max_length,
                c.precision,
                c.scale,
                c.collation_name,
                c.is_identity,
                c.is_computed,
                c.is_rowguidcol,
                c.is_filestream,
                ty.name AS system_type_name,
                ty.is_user_defined
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            WHERE s.name = ?
                AND t.name = ?
                AND c.name = ?
            """
            cursor.execute(col_query, (schema, table, column))
            col_info = cursor.fetchone()
            
            if col_info:
                metadata["column_id"] = col_info[0]
                metadata["max_length"] = col_info[1] if col_info[1] else None
                metadata["precision"] = col_info[2] if col_info[2] else None
                metadata["scale"] = col_info[3] if col_info[3] else None
                metadata["collation_name"] = col_info[4] if col_info[4] else None
                metadata["is_identity"] = bool(col_info[5])
                metadata["is_computed"] = bool(col_info[6])
                metadata["is_rowguidcol"] = bool(col_info[7])
                metadata["is_filestream"] = bool(col_info[8])
                metadata["system_type_name"] = col_info[9] if col_info[9] else None
                metadata["is_user_defined"] = bool(col_info[10])
                
                # Get identity seed and increment if it's an identity column
                if col_info[5]:  # is_identity
                    try:
                        identity_query = """
                        SELECT 
                            seed_value,
                            increment_value,
                            last_value
                        FROM sys.identity_columns
                        WHERE object_id = OBJECT_ID(?, 'U')
                            AND name = ?
                        """
                        full_name = f"[{database}].[{schema}].[{table}]"
                        cursor.execute(identity_query, (full_name, column))
                        identity_info = cursor.fetchone()
                        if identity_info:
                            metadata["identity_seed"] = str(identity_info[0]) if identity_info[0] else None
                            metadata["identity_increment"] = str(identity_info[1]) if identity_info[1] else None
                            metadata["identity_last_value"] = str(identity_info[2]) if identity_info[2] else None
                    except Exception as e:
                        logger.debug(f"Could not get identity column info: {e}")
                
                # Get computed column definition if it's computed
                if col_info[6]:  # is_computed
                    try:
                        computed_query = """
                        SELECT definition
                        FROM sys.computed_columns
                        WHERE object_id = OBJECT_ID(?, 'U')
                            AND name = ?
                        """
                        full_name = f"[{database}].[{schema}].[{table}]"
                        cursor.execute(computed_query, (full_name, column))
                        computed_def = cursor.fetchone()
                        if computed_def and computed_def[0]:
                            metadata["computed_definition"] = computed_def[0]
                    except Exception as e:
                        logger.debug(f"Could not get computed column definition: {e}")
            
            return metadata
            
        except Exception as e:
            logger.debug(f"Could not extract column metadata for {database}.{schema}.{table}.{column}: {e}")
            return {}
        finally:
            cursor.close()
            conn.close()

    def test_connection(self) -> bool:
        """Test the SQL Server connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()
            cursor.close()
            conn.close()
            logger.info(f"SQL Server connection test successful. Version: {version[0][:50] if version else 'unknown'}...")
            return True
        except Exception as e:
            logger.error(f"SQL Server connection test failed: {e}")
            return False

    def validate_cdc_setup(self, database: Optional[str] = None) -> Dict[str, Any]:
        """Validate CDC setup for SQL Server.
        
        SQL Server CDC requires:
        - CDC enabled on database
        - CDC enabled on tables
        - Appropriate permissions
        
        Args:
            database: Database name (optional, uses config default if not provided)
            
        Returns:
            Validation result dictionary
        """
        database = database or self.config.get("database", "master")
        conn = self.connect()
        cursor = conn.cursor()
        
        result = {
            "database": database,
            "cdc_enabled": False,
            "cdc_tables": [],
            "errors": [],
            "warnings": []
        }
        
        try:
            cursor.execute(f"USE [{database}]")
            
            # Check if CDC is enabled on database
            cdc_check_query = """
                SELECT is_cdc_enabled
                FROM sys.databases
                WHERE name = ?
            """
            cursor.execute(cdc_check_query, database)
            cdc_enabled = cursor.fetchone()
            
            if cdc_enabled and cdc_enabled[0]:
                result["cdc_enabled"] = True
            else:
                result["errors"].append("CDC is not enabled on database. Run: EXEC sys.sp_cdc_enable_db")
            
            # Get CDC enabled tables
            if result["cdc_enabled"]:
                cdc_tables_query = """
                    SELECT s.name AS schema_name, t.name AS table_name
                    FROM sys.tables t
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    INNER JOIN sys.change_tracking_tables ct ON t.object_id = ct.object_id
                """
                cursor.execute(cdc_tables_query)
                for row in cursor.fetchall():
                    result["cdc_tables"].append(f"{row[0]}.{row[1]}")
            
        except Exception as e:
            result["errors"].append(f"Error validating CDC setup: {str(e)}")
            logger.error(f"CDC validation failed: {e}")
        finally:
            cursor.close()
            conn.close()
        
        return result

    def get_cdc_tables(self, database: Optional[str] = None, schema: Optional[str] = None) -> List[str]:
        """Get list of tables with CDC enabled.
        
        Args:
            database: Database name (optional)
            schema: Schema name (optional)
            
        Returns:
            List of table names with CDC enabled
        """
        validation = self.validate_cdc_setup(database)
        return validation.get("cdc_tables", [])

    def extract_profiling_data(
        self,
        database: str,
        schema: str,
        table_name: str,
    ) -> Dict[str, Any]:
        """Extract profiling data for a specific table.
        
        This method extracts data quality metrics including:
        - Table-level: row count, column count, size
        - Column-level: null counts, distinct values, statistics
        
        Args:
            database: Database name
            schema: Schema name
            table_name: Table name
            
        Returns:
            Dictionary containing profiling data with structure:
            {
                "row_count": int,
                "column_count": int,
                "size_bytes": int,
                "columns": [
                    {
                        "column_name": str,
                        "null_count": int,
                        "null_percentage": float,
                        "distinct_count": int,
                        "unique_count": int,
                        "min_value": float (for numeric),
                        "max_value": float (for numeric),
                        "mean_value": float (for numeric),
                        "min_length": int (for string),
                        "max_length": int (for string),
                        "avg_length": float (for string),
                        "sample_values": dict (top N values)
                    }
                ],
                "metadata": dict
            }
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Use the specified database
            cursor.execute(f"USE [{database}]")
            
            # Get table row count
            row_count_query = f"""
                SELECT COUNT(*) 
                FROM [{schema}].[{table_name}]
            """
            cursor.execute(row_count_query)
            row_count = cursor.fetchone()[0]
            
            # Get table size
            size_query = """
                SELECT 
                    SUM(a.total_pages) * 8 AS size_kb,
                    SUM(a.used_pages) * 8 AS used_kb
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                WHERE t.name = ? AND SCHEMA_NAME(t.schema_id) = ?
            """
            cursor.execute(size_query, (table_name, schema))
            size_row = cursor.fetchone()
            size_bytes = int(size_row[0] * 1024) if size_row and size_row[0] else 0
            
            # Get column count
            column_count_query = """
                SELECT COUNT(*) 
                FROM sys.columns c
                INNER JOIN sys.tables t ON c.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name = ? AND s.name = ?
            """
            cursor.execute(column_count_query, (table_name, schema))
            column_count = cursor.fetchone()[0]
            
            # Get column information
            columns_query = """
                SELECT 
                    c.name AS column_name,
                    t.name AS data_type,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tab ON c.object_id = tab.object_id
                INNER JOIN sys.schemas s ON tab.schema_id = s.schema_id
                WHERE tab.name = ? AND s.name = ?
                ORDER BY c.column_id
            """
            cursor.execute(columns_query, (table_name, schema))
            columns_info = cursor.fetchall()
            
            column_profiles = []
            
            for col_info in columns_info:
                col_name, data_type, max_length, precision, scale, is_nullable = col_info
                col_profile = {
                    "column_name": col_name,
                    "null_count": None,
                    "null_percentage": None,
                    "distinct_count": None,
                    "unique_count": None,
                    "min_value": None,
                    "max_value": None,
                    "mean_value": None,
                    "median_value": None,
                    "std_dev": None,
                    "min_length": None,
                    "max_length": None,
                    "avg_length": None,
                    "sample_values": None,
                    "metadata": {
                        "data_type": data_type,
                        "max_length": max_length,
                        "precision": precision,
                        "scale": scale,
                        "is_nullable": is_nullable
                    }
                }
                
                try:
                    # Get null count
                    null_query = f"""
                        SELECT 
                            SUM(CASE WHEN [{col_name}] IS NULL THEN 1 ELSE 0 END) AS null_count,
                            COUNT(*) AS total_count
                        FROM [{schema}].[{table_name}]
                    """
                    cursor.execute(null_query)
                    null_row = cursor.fetchone()
                    if null_row:
                        null_count = null_row[0] or 0
                        total_count = null_row[1] or row_count
                        col_profile["null_count"] = null_count
                        col_profile["null_percentage"] = (null_count / total_count * 100) if total_count > 0 else 0.0
                    
                    # Get distinct count
                    distinct_query = f"""
                        SELECT COUNT(DISTINCT [{col_name}]) AS distinct_count
                        FROM [{schema}].[{table_name}]
                        WHERE [{col_name}] IS NOT NULL
                    """
                    cursor.execute(distinct_query)
                    distinct_row = cursor.fetchone()
                    if distinct_row:
                        col_profile["distinct_count"] = distinct_row[0] or 0
                        # Unique count is same as distinct count if distinct count equals non-null count
                        non_null_count = total_count - null_count
                        if col_profile["distinct_count"] == non_null_count:
                            col_profile["unique_count"] = col_profile["distinct_count"]
                    
                    # For numeric types, get statistics
                    if data_type in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney']:
                        stats_query = f"""
                            SELECT 
                                MIN(CAST([{col_name}] AS FLOAT)) AS min_val,
                                MAX(CAST([{col_name}] AS FLOAT)) AS max_val,
                                AVG(CAST([{col_name}] AS FLOAT)) AS mean_val
                            FROM [{schema}].[{table_name}]
                            WHERE [{col_name}] IS NOT NULL
                        """
                        cursor.execute(stats_query)
                        stats_row = cursor.fetchone()
                        if stats_row:
                            col_profile["min_value"] = float(stats_row[0]) if stats_row[0] is not None else None
                            col_profile["max_value"] = float(stats_row[1]) if stats_row[1] is not None else None
                            col_profile["mean_value"] = float(stats_row[2]) if stats_row[2] is not None else None
                            
                            # Get standard deviation
                            stddev_query = f"""
                                SELECT STDEV(CAST([{col_name}] AS FLOAT)) AS std_dev
                                FROM [{schema}].[{table_name}]
                                WHERE [{col_name}] IS NOT NULL
                            """
                            cursor.execute(stddev_query)
                            stddev_row = cursor.fetchone()
                            if stddev_row and stddev_row[0] is not None:
                                col_profile["std_dev"] = float(stddev_row[0])
                    
                    # For string types, get length statistics
                    elif data_type in ['varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext']:
                        length_query = f"""
                            SELECT 
                                MIN(LEN([{col_name}])) AS min_len,
                                MAX(LEN([{col_name}])) AS max_len,
                                AVG(CAST(LEN([{col_name}]) AS FLOAT)) AS avg_len
                            FROM [{schema}].[{table_name}]
                            WHERE [{col_name}] IS NOT NULL
                        """
                        cursor.execute(length_query)
                        length_row = cursor.fetchone()
                        if length_row:
                            col_profile["min_length"] = int(length_row[0]) if length_row[0] is not None else None
                            col_profile["max_length"] = int(length_row[1]) if length_row[1] is not None else None
                            col_profile["avg_length"] = float(length_row[2]) if length_row[2] is not None else None
                    
                    # Get sample values (top 10 distinct values)
                    sample_query = f"""
                        SELECT TOP 10 
                            [{col_name}] AS value,
                            COUNT(*) AS count
                        FROM [{schema}].[{table_name}]
                        WHERE [{col_name}] IS NOT NULL
                        GROUP BY [{col_name}]
                        ORDER BY COUNT(*) DESC
                    """
                    cursor.execute(sample_query)
                    sample_rows = cursor.fetchall()
                    if sample_rows:
                        col_profile["sample_values"] = {
                            "values": [{"value": str(row[0]), "count": row[1]} for row in sample_rows]
                        }
                
                except Exception as e:
                    logger.warning(f"Error profiling column {col_name}: {e}")
                    # Continue with other columns
                
                column_profiles.append(col_profile)
            
            profiling_data = {
                "row_count": row_count,
                "column_count": column_count,
                "size_bytes": size_bytes,
                "columns": column_profiles,
                "metadata": {
                    "database": database,
                    "schema": schema,
                    "table": table_name
                }
            }
            
            logger.info(f"Extracted profiling data for {database}.{schema}.{table_name}: {row_count} rows, {column_count} columns")
            return profiling_data
            
        except Exception as e:
            logger.error(f"Error extracting profiling data for {database}.{schema}.{table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def extract_lineage(
        self,
        database: Optional[str] = None,
        days_back: int = 7
    ) -> Dict[str, Any]:
        """Extract data lineage relationships from SQL Server.
        
        This method extracts lineage from:
        - Foreign key relationships
        - Query logs (if available)
        - View definitions
        
        Args:
            database: Database name (optional)
            days_back: Number of days to look back for query-based lineage
            
        Returns:
            Dictionary containing lineage data with structure:
            {
                "edges": [
                    {
                        "from_table": str (FQN),
                        "to_table": str (FQN),
                        "from_column": str (optional),
                        "to_column": str (optional),
                        "type": str (foreign_key, view, query, etc.),
                        "metadata": dict
                    }
                ],
                "metadata": dict
            }
        """
        conn = self.connect()
        cursor = conn.cursor()
        edges = []
        
        try:
            # Use specified database or current database
            if database:
                cursor.execute(f"USE [{database}]")
                current_db = database
            else:
                cursor.execute("SELECT DB_NAME()")
                current_db = cursor.fetchone()[0]
            
            # Extract lineage from foreign keys
            fk_query = """
                SELECT 
                    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS source_schema,
                    OBJECT_NAME(fk.parent_object_id) AS source_table,
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS source_column,
                    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS target_schema,
                    OBJECT_NAME(fk.referenced_object_id) AS target_table,
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS target_column,
                    fk.name AS foreign_key_name
                FROM sys.foreign_keys fk
                INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) IS NOT NULL
                    AND OBJECT_SCHEMA_NAME(fk.referenced_object_id) IS NOT NULL
            """
            
            if database:
                fk_query += " AND (OBJECT_SCHEMA_NAME(fk.parent_object_id) = ? OR OBJECT_SCHEMA_NAME(fk.referenced_object_id) = ?)"
                cursor.execute(fk_query, (database, database))
            else:
                cursor.execute(fk_query)
            
            for row in cursor.fetchall():
                source_schema, source_table, source_col, target_schema, target_table, target_col, fk_name = row
                
                from_table = f"{current_db}.{source_schema}.{source_table}"
                to_table = f"{current_db}.{target_schema}.{target_table}"
                
                edge = {
                    "from_table": from_table,
                    "to_table": to_table,
                    "from_column": source_col,
                    "to_column": target_col,
                    "type": "foreign_key",
                    "metadata": {
                        "foreign_key_name": fk_name,
                        "database": current_db
                    }
                }
                edges.append(edge)
            
            # Extract lineage from views (views depend on tables)
            view_query = """
                SELECT 
                    s.name AS view_schema,
                    v.name AS view_name,
                    OBJECT_DEFINITION(v.object_id) AS view_definition
                FROM sys.views v
                INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            """
            
            if database:
                view_query += " WHERE DB_NAME() = ?"
                cursor.execute(view_query, database)
            else:
                cursor.execute(view_query)
            
            for row in cursor.fetchall():
                view_schema, view_name, view_def = row
                if not view_def:
                    continue
                
                # Simple parsing to find table references in view definition
                # This is a basic implementation - could be enhanced with SQL parsing
                view_def_lower = view_def.lower()
                
                # Look for FROM and JOIN clauses
                table_pattern = r'from\s+\[?(\w+)\]?\.\[?(\w+)\]?|join\s+\[?(\w+)\]?\.\[?(\w+)\]?'
                matches = re.finditer(table_pattern, view_def_lower, re.IGNORECASE)
                
                view_fqn = f"{current_db}.{view_schema}.{view_name}"
                
                for match in matches:
                    # Extract schema and table from match
                    groups = match.groups()
                    if groups[0] and groups[1]:  # FROM clause
                        ref_schema = groups[0]
                        ref_table = groups[1]
                    elif groups[2] and groups[3]:  # JOIN clause
                        ref_schema = groups[2]
                        ref_table = groups[3]
                    else:
                        continue
                    
                    ref_table_fqn = f"{current_db}.{ref_schema}.{ref_table}"
                    
                    edge = {
                        "from_table": ref_table_fqn,
                        "to_table": view_fqn,
                        "from_column": None,
                        "to_column": None,
                        "type": "view",
                        "metadata": {
                            "database": current_db,
                            "view_definition": view_def[:500]  # Truncate for storage
                        }
                    }
                    edges.append(edge)
            
            # Extract lineage from query logs (if available and permissions allow)
            try:
                query_log_query = """
                    SELECT 
                        DB_NAME(qt.dbid) AS database_name,
                        qt.text AS query_text,
                        qs.last_execution_time
                    FROM sys.dm_exec_query_stats qs
                    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
                    WHERE qs.last_execution_time >= DATEADD(day, ?, GETDATE())
                        AND qt.text LIKE '%INSERT%INTO%'
                        AND qt.text LIKE '%SELECT%FROM%'
                """
                
                if database:
                    query_log_query += " AND DB_NAME(qt.dbid) = ?"
                    cursor.execute(query_log_query, (-days_back, database))
                else:
                    cursor.execute(query_log_query, -days_back)
                
                for row in cursor.fetchall():
                    db_name, query_text, last_exec = row
                    if not query_text:
                        continue
                    
                    # Simple parsing to extract INSERT INTO ... SELECT FROM patterns
                    # This is basic - could be enhanced with proper SQL parsing
                    query_lower = query_text.lower()
                    
                    # Look for INSERT INTO ... SELECT FROM pattern
                    insert_match = re.search(r'insert\s+into\s+\[?(\w+)\]?\.\[?(\w+)\]?', query_lower, re.IGNORECASE)
                    select_match = re.search(r'select\s+.*\s+from\s+\[?(\w+)\]?\.\[?(\w+)\]?', query_lower, re.IGNORECASE)
                    
                    if insert_match and select_match:
                        target_schema = insert_match.group(1)
                        target_table = insert_match.group(2)
                        source_schema = select_match.group(1)
                        source_table = select_match.group(2)
                        
                        from_table = f"{db_name or current_db}.{source_schema}.{source_table}"
                        to_table = f"{db_name or current_db}.{target_schema}.{target_table}"
                        
                        edge = {
                            "from_table": from_table,
                            "to_table": to_table,
                            "from_column": None,
                            "to_column": None,
                            "type": "query",
                            "metadata": {
                                "database": db_name or current_db,
                                "last_execution_time": last_exec.isoformat() if last_exec else None,
                                "query_text": query_text[:500]  # Truncate
                            }
                        }
                        edges.append(edge)
            
            except Exception as e:
                # If we don't have permissions for query logs, just log and continue
                if "permission" in str(e).lower() or "denied" in str(e).lower():
                    logger.debug(f"Could not extract query-based lineage (insufficient permissions): {e}")
                else:
                    logger.warning(f"Error extracting query-based lineage: {e}")
            
            lineage_data = {
                "edges": edges,
                "metadata": {
                    "database": current_db,
                    "days_back": days_back,
                    "edge_count": len(edges)
                }
            }
            
            logger.info(f"Extracted {len(edges)} lineage edges from SQL Server")
            return lineage_data
            
        except Exception as e:
            logger.error(f"Error extracting lineage from SQL Server: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def extract_table_data(
        self,
        database: str,
        schema: str,
        table_name: str,
        limit: Optional[int] = None,
        offset: int = 0,
        include_schema: bool = True,
    ) -> Dict[str, Any]:
        """Extract actual data rows from a table along with schema information.
        
        This method extracts both the schema metadata and the actual data rows
        from a SQL Server table, similar to OpenMetadata's data extraction pattern.
        
        Args:
            database: Database name
            schema: Schema name
            table_name: Table name
            limit: Maximum number of rows to extract (None for all rows, use with caution)
            offset: Number of rows to skip (for pagination)
            include_schema: Whether to include schema metadata in the response
            
        Returns:
            Dictionary containing:
            {
                "schema": {
                    "database": str,
                    "schema": str,
                    "table": str,
                    "columns": List[Dict] (column metadata),
                    "table_properties": Dict (table metadata)
                },
                "data": {
                    "rows": List[List] (actual row data),
                    "row_count": int (total rows extracted),
                    "has_more": bool (whether more rows exist)
                },
                "metadata": {
                    "extraction_timestamp": str (ISO format),
                    "limit": int or None,
                    "offset": int
                }
            }
        """
        from datetime import datetime
        
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Use the specified database
            cursor.execute(f"USE [{database}]")
            
            result = {
                "schema": None,
                "data": {
                    "rows": [],
                    "row_count": 0,
                    "has_more": False
                },
                "metadata": {
                    "extraction_timestamp": datetime.utcnow().isoformat(),
                    "limit": limit,
                    "offset": offset
                }
            }
            
            # Extract schema information if requested
            if include_schema:
                columns = self.extract_columns(database, schema, table_name)
                table_properties = self._extract_table_properties(
                    database, schema, table_name, "BASE TABLE"
                )
                
                # Get table description
                description = self._get_table_description(database, schema, table_name)
                
                result["schema"] = {
                    "database": database,
                    "schema": schema,
                    "table": table_name,
                    "description": description,
                    "columns": [
                        {
                            "name": col.name,
                            "data_type": col.data_type,
                            "ordinal_position": col.ordinal_position,
                            "is_nullable": col.is_nullable,
                            "default_value": col.default_value,
                            "description": col.description,
                            "json_schema": col.json_schema
                        }
                        for col in columns
                    ],
                    "table_properties": table_properties
                }
            
            # Get total row count first
            count_query = f"""
                SELECT COUNT(*) 
                FROM [{schema}].[{table_name}]
            """
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]
            
            # Build data extraction query
            # Get column names for SELECT statement
            columns_query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(columns_query, (database, schema, table_name))
            column_names = [row[0] for row in cursor.fetchall()]
            
            if not column_names:
                logger.warning(f"No columns found for table {database}.{schema}.{table_name}")
                return result
            
            # Build SELECT query with column names
            column_list = ", ".join([f"[{col}]" for col in column_names])
            data_query = f"""
                SELECT {column_list}
                FROM [{schema}].[{table_name}]
                ORDER BY (SELECT NULL)  -- No specific ordering, faster
            """
            
            # Add pagination if limit is specified
            if limit is not None:
                data_query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
            
            # Execute data extraction query
            logger.info(f"Extracting data from {database}.{schema}.{table_name} (limit={limit}, offset={offset})")
            cursor.execute(data_query)
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Convert rows to lists (pyodbc returns tuples)
            row_data = [list(row) for row in rows]
            
            # Check if there are more rows
            has_more = False
            if limit is not None:
                rows_extracted = len(row_data)
                has_more = (offset + rows_extracted) < total_rows
            else:
                has_more = False
            
            result["data"] = {
                "rows": row_data,
                "row_count": len(row_data),
                "total_rows": total_rows,
                "has_more": has_more,
                "column_names": column_names  # Include column names for row mapping
            }
            
            logger.info(
                f"Extracted {len(row_data)} rows from {database}.{schema}.{table_name} "
                f"(total: {total_rows}, has_more: {has_more})"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting data from {database}.{schema}.{table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def extract_all_tables_data(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        limit_per_table: Optional[int] = 1000,
        include_schema: bool = True,
    ) -> Dict[str, Any]:
        """Extract data from all tables in a database/schema.
        
        This method extracts both schema and data from all tables,
        similar to OpenMetadata's bulk data extraction pattern.
        
        Args:
            database: Database name (uses config default if not provided)
            schema: Schema name (uses config default or dbo if not provided)
            limit_per_table: Maximum rows to extract per table (None for all, use with caution)
            include_schema: Whether to include schema metadata for each table
            
        Returns:
            Dictionary containing:
            {
                "database": str,
                "schema": str,
                "tables": [
                    {
                        "table_name": str,
                        "schema": {...},  # If include_schema=True
                        "data": {...},   # Table data
                        "metadata": {...}
                    },
                    ...
                ],
                "metadata": {
                    "extraction_timestamp": str,
                    "total_tables": int,
                    "tables_with_data": int
                }
            }
        """
        from datetime import datetime
        
        database = database or self.config.get("database", "master")
        schema = schema or self.config.get("schema", "dbo")
        
        # First, get all tables
        tables = self.extract_tables(database=database, schema=schema)
        
        result = {
            "database": database,
            "schema": schema,
            "tables": [],
            "metadata": {
                "extraction_timestamp": datetime.utcnow().isoformat(),
                "total_tables": len(tables),
                "tables_with_data": 0
            }
        }
        
        for table in tables:
            try:
                # Extract table name from FQN (format: database.schema.table)
                table_name = table.name
                
                # Extract data for this table
                table_data = self.extract_table_data(
                    database=database,
                    schema=schema,
                    table_name=table_name,
                    limit=limit_per_table,
                    offset=0,
                    include_schema=include_schema
                )
                
                table_result = {
                    "table_name": table_name,
                    "fully_qualified_name": table.fully_qualified_name,
                    "data": table_data["data"],
                    "metadata": table_data["metadata"]
                }
                
                if include_schema:
                    table_result["schema"] = table_data["schema"]
                
                result["tables"].append(table_result)
                
                if table_data["data"]["row_count"] > 0:
                    result["metadata"]["tables_with_data"] += 1
                
                logger.info(
                    f"Extracted data from {table.fully_qualified_name}: "
                    f"{table_data['data']['row_count']} rows"
                )
                
            except Exception as e:
                logger.warning(f"Failed to extract data from table {table.name}: {e}")
                # Continue with other tables
                continue
        
        logger.info(
            f"Extracted data from {result['metadata']['tables_with_data']} out of "
            f"{result['metadata']['total_tables']} tables"
        )
        
        return result

    def extract_lsn_offset(
        self,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract LSN (Log Sequence Number) or offset metadata from SQL Server.
        
        This method extracts the current position in the transaction log,
        which can be used for tracking replication or change data capture positions.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            
        Returns:
            Dictionary containing LSN/offset information:
            {
                "lsn": str (LSN value in SQL Server format),
                "offset": Optional[int] (offset value if applicable),
                "timestamp": str (ISO format timestamp),
                "database": str,
                "metadata": {...} (additional SQL Server-specific metadata)
            }
        """
        database = database or self.config.get("database", "master")
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Use the specified database
            cursor.execute(f"USE [{database}]")
            
            # Get current LSN from transaction log
            # Try multiple methods to get LSN
            lsn = None
            lsn_hex = None
            offset = None
            metadata = {}
            
            # Method 1: Get current LSN from fn_dblog (if available)
            try:
                lsn_query = """
                    SELECT 
                        MAX([Current LSN]) AS current_lsn
                    FROM fn_dblog(NULL, NULL)
                    WHERE [Operation] IN ('LOP_BEGIN_XACT', 'LOP_COMMIT_XACT')
                """
                cursor.execute(lsn_query)
                result = cursor.fetchone()
                if result and result[0]:
                    lsn_hex = result[0]
                    # Convert hex LSN to decimal for offset calculation
                    # LSN format: 00000000:00000000:0000
                    if lsn_hex and ':' in str(lsn_hex):
                        parts = str(lsn_hex).split(':')
                        if len(parts) >= 2:
                            # Calculate offset from LSN parts
                            try:
                                file_id = int(parts[0], 16) if parts[0] else 0
                                slot = int(parts[1], 16) if parts[1] else 0
                                offset = (file_id << 32) | slot
                            except ValueError:
                                pass
            except Exception as e:
                logger.debug(f"Could not get LSN from fn_dblog: {e}")
            
            # Method 2: Get LSN from sys.dm_tran_database_transactions
            try:
                tran_query = """
                    SELECT 
                        MAX(database_transaction_current_lsn) AS current_lsn
                    FROM sys.dm_tran_database_transactions
                    WHERE database_id = DB_ID(?)
                """
                cursor.execute(tran_query, database)
                result = cursor.fetchone()
                if result and result[0]:
                    lsn_hex = result[0]
            except Exception as e:
                logger.debug(f"Could not get LSN from dm_tran_database_transactions: {e}")
            
            # Method 3: Get replication LSN if replication is enabled
            try:
                repl_query = """
                    SELECT 
                        MAX(current_lsn) AS current_lsn
                    FROM sys.dm_repl_traninfo
                """
                cursor.execute(repl_query)
                result = cursor.fetchone()
                if result and result[0]:
                    repl_lsn = result[0]
                    if not lsn_hex:
                        lsn_hex = repl_lsn
                    metadata["replication_lsn"] = repl_lsn
            except Exception as e:
                logger.debug(f"Could not get replication LSN: {e}")
            
            # Method 4: Get database checkpoint LSN
            try:
                checkpoint_query = """
                    SELECT 
                        checkpoint_lsn
                    FROM sys.database_recovery_status
                    WHERE database_id = DB_ID(?)
                """
                cursor.execute(checkpoint_query, database)
                result = cursor.fetchone()
                if result and result[0]:
                    checkpoint_lsn = result[0]
                    metadata["checkpoint_lsn"] = checkpoint_lsn
                    if not lsn_hex:
                        lsn_hex = checkpoint_lsn
            except Exception as e:
                logger.debug(f"Could not get checkpoint LSN: {e}")
            
            # If we still don't have an LSN, use a timestamp-based approach
            if not lsn_hex:
                # Get current transaction timestamp as fallback
                cursor.execute("SELECT GETDATE()")
                timestamp = cursor.fetchone()[0]
                lsn_hex = f"TIMESTAMP:{timestamp.isoformat()}"
                metadata["lsn_source"] = "timestamp_fallback"
            else:
                metadata["lsn_source"] = "transaction_log"
            
            # Get database version and additional metadata
            try:
                version_query = "SELECT @@VERSION"
                cursor.execute(version_query)
                version = cursor.fetchone()[0]
                metadata["sql_server_version"] = version[:100] if version else None
            except Exception:
                pass
            
            lsn_result = {
                "lsn": str(lsn_hex),
                "offset": offset,
                "timestamp": datetime.utcnow().isoformat(),
                "database": database,
                "metadata": metadata
            }
            
            logger.info(f"Extracted LSN/offset for database {database}: LSN={lsn_hex}")
            return lsn_result
            
        except Exception as e:
            logger.error(f"Error extracting LSN/offset from SQL Server: {e}")
            # Return a minimal result with timestamp
            return {
                "lsn": None,
                "offset": None,
                "timestamp": datetime.utcnow().isoformat(),
                "database": database,
                "metadata": {"error": str(e)}
            }
        finally:
            cursor.close()
            conn.close()

    def extract_schema(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract table and column metadata (schema information).
        
        Args:
            database: Database name (optional, uses config default if not provided)
            schema: Schema name (optional, uses config default if not provided)
            table: Table name (optional, if None extracts all tables in schema)
            
        Returns:
            Dictionary containing schema information
        """
        database = database or self.config.get("database", "master")
        schema = schema or self.config.get("schema", "dbo")
        
        if table:
            # Extract single table
            tables_list = self.extract_tables(database=database, schema=schema)
            # Filter to specific table
            tables_list = [t for t in tables_list if t.name == table]
        else:
            # Extract all tables
            tables_list = self.extract_tables(database=database, schema=schema)
        
        # Convert ConnectorTable objects to dictionaries
        tables_dict = []
        for tbl in tables_list:
            table_dict = {
                "name": tbl.name,
                "fully_qualified_name": tbl.fully_qualified_name,
                "columns": [
                    {
                        "name": col.name,
                        "data_type": col.data_type,
                        "ordinal_position": col.ordinal_position,
                        "is_nullable": col.is_nullable,
                        "default_value": col.default_value,
                        "description": col.description,
                        "json_schema": col.json_schema
                    }
                    for col in tbl.columns
                ],
                "properties": tbl.properties,
                "table_type": tbl.table_type,
                "description": tbl.description
            }
            tables_dict.append(table_dict)
        
        return {
            "database": database,
            "schema": schema,
            "tables": tables_dict
        }

    def extract_data(
        self,
        database: str,
        schema: str,
        table_name: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Extract actual data rows from a table.
        
        This is a wrapper around extract_table_data that matches the base interface.
        
        Args:
            database: Database name
            schema: Schema name
            table_name: Table name
            limit: Maximum number of rows to extract (None for all rows)
            offset: Number of rows to skip (for pagination)
            
        Returns:
            Dictionary containing data extraction results
        """
        result = self.extract_table_data(
            database=database,
            schema=schema,
            table_name=table_name,
            limit=limit,
            offset=offset,
            include_schema=False
        )
        
        # Return only the data portion to match base interface
        return result["data"]

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
        LSN/offset metadata for tracking purposes.
        
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
        database = database or self.config.get("database", "master")
        schema = schema or self.config.get("schema", "dbo")
        
        # Extract LSN/offset metadata
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
                        columns = self.extract_columns(database, schema, table_name)
                        table_properties = self._extract_table_properties(
                            database, schema, table_name, "BASE TABLE"
                        )
                        description = self._get_table_description(database, schema, table_name)
                        
                        table_result["schema"] = {
                            "database": database,
                            "schema": schema,
                            "table": table_name,
                            "description": description,
                            "columns": [
                                {
                                    "name": col.name,
                                    "data_type": col.data_type,
                                    "ordinal_position": col.ordinal_position,
                                    "is_nullable": col.is_nullable,
                                    "default_value": col.default_value,
                                    "description": col.description,
                                    "json_schema": col.json_schema
                                }
                                for col in columns
                            ],
                            "table_properties": table_properties
                        }
                    except Exception as e:
                        logger.warning(f"Failed to extract schema for {table_name}: {e}")
                        table_result["schema"] = None
                        table_result["metadata"]["schema_error"] = str(e)
                
                # Extract data if requested
                if include_data:
                    try:
                        data_result = self.extract_data(
                            database=database,
                            schema=schema,
                            table_name=table_name,
                            limit=data_limit,
                            offset=0
                        )
                        table_result["data"] = data_result
                    except Exception as e:
                        logger.warning(f"Failed to extract data for {table_name}: {e}")
                        table_result["data"] = {
                            "rows": [],
                            "row_count": 0,
                            "total_rows": 0,
                            "has_more": False,
                            "column_names": []
                        }
                        table_result["metadata"]["data_error"] = str(e)
                
                result["tables"].append(table_result)
                result["metadata"]["tables_successful"] += 1
                
                logger.info(
                    f"Full load completed for {database}.{schema}.{table_name}"
                )
                
            except Exception as e:
                logger.error(f"Failed to process table {table_name}: {e}")
                result["metadata"]["tables_failed"] += 1
                result["tables"].append({
                    "table_name": table_name,
                    "error": str(e),
                    "metadata": {
                        "extraction_timestamp": datetime.utcnow().isoformat()
                    }
                })
                continue
        
        logger.info(
            f"Full load completed: {result['metadata']['tables_successful']} successful, "
            f"{result['metadata']['tables_failed']} failed out of "
            f"{result['metadata']['tables_processed']} tables"
        )
        
        return result

