"""PostgreSQL connector for metadata extraction."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import RealDictCursor

    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
    psycopg2 = None  # type: ignore
    sql = None  # type: ignore
    RealDictCursor = None  # type: ignore

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class PostgreSQLConnector(BaseConnector):
    """Connector for extracting metadata from PostgreSQL."""

    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize PostgreSQL connector with connection configuration.

        Args:
            connection_config: Dictionary containing:
                - host: Server hostname or IP (required)
                - port: Port number (optional, default: 5432)
                - database: Database name (required)
                - user or username: Username (required)
                - password: Password (required)
                - schema: Schema name (optional, default: public)
        """
        if not POSTGRESQL_AVAILABLE:
            raise ImportError(
                "psycopg2 is not installed. "
                "Install it with: pip install psycopg2-binary"
            )

        # Normalize field names: accept both 'username' and 'user'
        if "username" in connection_config and "user" not in connection_config:
            connection_config["user"] = connection_config["username"]
        
        super().__init__(connection_config)

    def _validate_config(self) -> None:
        """Validate that required connection parameters are present."""
        required = ["host", "database", "user", "password"]
        missing = [key for key in required if not self.config.get(key)]
        if missing:
            raise ValueError(f"Missing required connection parameters: {', '.join(missing)}")

    def connect(self):
        """Establish connection to PostgreSQL.

        Returns:
            PostgreSQL connection object (psycopg2.Connection)
        """
        try:
            host = self.config["host"]
            port = self.config.get("port", 5432)
            database = self.config["database"]
            user = self.config["user"]
            password = self.config["password"]
            
            # Try connection with SSL disabled (common for local connections)
            # Some PostgreSQL configs require this for localhost
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password,
                    connect_timeout=30,
                    sslmode='disable'  # Disable SSL for local connections
                )
            except psycopg2.OperationalError:
                # If SSL disabled fails, try without sslmode parameter
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password,
                    connect_timeout=30
                )
            # Set autocommit to avoid transaction issues
            conn.autocommit = True
            logger.info("Connected to PostgreSQL: %s:%s/%s", host, port, database)
            return conn
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL: %r", e, exc_info=True)
            raise

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
        database = database or self.config.get("database")
        schema = schema or self.config.get("schema", "public")
        
        if not database:
            raise ValueError("Database name is required")

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        tables_list = []

        try:
            # Query for tables
            if table:
                query = """
                    SELECT 
                        table_catalog,
                        table_schema,
                        table_name,
                        table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                        AND table_name = %s
                        AND table_type IN ('BASE TABLE', 'VIEW')
                    ORDER BY table_name
                """
                cursor.execute(query, (schema, table))
            else:
                query = """
                    SELECT 
                        table_catalog,
                        table_schema,
                        table_name,
                        table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                        AND table_type IN ('BASE TABLE', 'VIEW')
                    ORDER BY table_name
                """
                cursor.execute(query, (schema,))

            for row in cursor.fetchall():
                table_catalog = row["table_catalog"]
                table_schema = row["table_schema"]
                table_name = row["table_name"]
                table_type = row["table_type"]
                
                fqn = f"{table_catalog}.{table_schema}.{table_name}"

                # Extract columns for this table
                try:
                    columns = self._extract_columns_internal(
                        cursor, table_catalog, table_schema, table_name
                    )
                except Exception as e:
                    logger.warning("Error extracting columns for %s.%s: %r", table_schema, table_name, e, exc_info=True)
                    columns = []

                # Extract table properties
                try:
                    properties = self._extract_table_properties(
                        cursor, table_schema, table_name, table_type
                    )
                except Exception as e:
                    logger.warning(f"Error extracting properties for {table_schema}.{table_name}: {e}")
                    properties = {}

                table_dict = {
                    "name": table_name,
                    "fully_qualified_name": fqn,
                    "columns": columns,
                    "properties": properties,
                    "table_type": "view" if table_type == "VIEW" else "table"
                }
                tables_list.append(table_dict)

            logger.info(f"Extracted {len(tables_list)} tables from PostgreSQL")
            return {
                "database": database,
                "schema": schema,
                "tables": tables_list
            }

        except Exception as e:
            logger.error(f"Error extracting schema from PostgreSQL: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            cursor.close()
            conn.close()

    def _extract_columns_internal(
        self,
        cursor,
        database: str,
        schema: str,
        table: str
    ) -> List[Dict[str, Any]]:
        """Extract column metadata for a specific table (internal method).

        Args:
            cursor: Database cursor
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            List of column dictionaries
        """
        # Use a new connection/cursor to avoid transaction issues
        # or ensure we're in autocommit mode
        query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position,
                is_nullable,
                column_default,
                udt_name
            FROM information_schema.columns
            WHERE table_catalog = %s
                AND table_schema = %s
                AND table_name = %s
            ORDER BY ordinal_position
        """
        try:
            cursor.execute(query, (database, schema, table))
        except Exception as e:
            # If transaction is aborted, rollback and retry
            if "transaction is aborted" in str(e).lower():
                cursor.connection.rollback()
                cursor.execute(query, (database, schema, table))
            else:
                raise
        columns = []

        for row in cursor.fetchall():
            col_name = row["column_name"]
            data_type = row["data_type"]
            max_length = row["character_maximum_length"]
            precision = row["numeric_precision"]
            scale = row["numeric_scale"]
            ordinal = row["ordinal_position"]
            nullable = row["is_nullable"]
            default = row["column_default"]
            udt_name = row["udt_name"]

            # Build full data type string
            full_data_type = data_type
            if max_length and data_type in ("character varying", "varchar", "character", "char"):
                full_data_type = f"{data_type}({max_length})"
            elif precision and data_type in ("numeric", "decimal"):
                if scale:
                    full_data_type = f"{data_type}({precision},{scale})"
                else:
                    full_data_type = f"{data_type}({precision})"

            # Get column description from pg_description
            description = self._get_column_description(cursor, schema, table, col_name)

            column_dict = {
                "name": col_name,
                "data_type": full_data_type,
                "udt_name": udt_name,
                "ordinal_position": int(ordinal) if ordinal else 1,
                "is_nullable": nullable == "YES",
                "default_value": str(default) if default else None,
                "description": description,
                "json_schema": {
                    "data_type": data_type,
                    "max_length": max_length,
                    "precision": precision,
                    "scale": scale,
                    "udt_name": udt_name
                }
            }
            columns.append(column_dict)

        return columns

    def _get_column_description(
        self,
        cursor,
        schema: str,
        table: str,
        column: str
    ) -> Optional[str]:
        """Get column description from pg_description.

        Args:
            cursor: Database cursor
            schema: Schema name
            table: Table name
            column: Column name

        Returns:
            Column description or None
        """
        try:
            query = """
                SELECT d.description
                FROM pg_description d
                JOIN pg_class c ON d.objoid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_attribute a ON d.objoid = a.attrelid AND d.objsubid = a.attnum
                WHERE n.nspname = %s
                    AND c.relname = %s
                    AND a.attname = %s
            """
            cursor.execute(query, (schema, table, column))
            result = cursor.fetchone()
            return result["description"] if result and result.get("description") else None
        except Exception as e:
            logger.debug(f"Could not get column description for {schema}.{table}.{column}: {e}")
            return None

    def _extract_table_properties(
        self,
        cursor,
        schema: str,
        table: str,
        table_type: str
    ) -> Dict[str, Any]:
        """Extract comprehensive table properties.

        Args:
            cursor: Database cursor
            schema: Schema name
            table: Table name
            table_type: Table type (BASE TABLE, VIEW)

        Returns:
            Dictionary with table properties
        """
        properties = {}

        try:
            # Get row count and size
            if table_type == "BASE TABLE":
                stats_query = """
                    SELECT 
                        n_live_tup AS row_count,
                        pg_total_relation_size(c.oid) AS total_size,
                        pg_relation_size(c.oid) AS table_size
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = %s AND c.relname = %s
                """
                cursor.execute(stats_query, (schema, table))
                stats = cursor.fetchone()
                if stats:
                    properties["row_count"] = stats.get("row_count", 0) or 0
                    properties["total_size_bytes"] = stats.get("total_size", 0) or 0
                    properties["table_size_bytes"] = stats.get("table_size", 0) or 0

            # Get primary keys
            pk_query = """
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON i.indrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE i.indisprimary = true
                    AND n.nspname = %s
                    AND c.relname = %s
                ORDER BY a.attnum
            """
            cursor.execute(pk_query, (schema, table))
            pk_columns = [row["column_name"] for row in cursor.fetchall()]
            if pk_columns:
                properties["primary_keys"] = pk_columns

            # Get foreign keys
            fk_query = """
                SELECT
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
            """
            cursor.execute(fk_query, (schema, table))
            foreign_keys = []
            for row in cursor.fetchall():
                foreign_keys.append({
                    "name": row["constraint_name"],
                    "column": row["column_name"],
                    "referenced_table": f"{row['foreign_table_schema']}.{row['foreign_table_name']}",
                    "referenced_column": row["foreign_column_name"]
                })
            if foreign_keys:
                properties["foreign_keys"] = foreign_keys

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
            database: Database name
            schema: Schema name
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
            count_query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table_name)
            )
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]

            # Get column names
            columns_query = sql.SQL("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                    AND table_name = %s
                ORDER BY ordinal_position
            """)
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
            column_list = sql.SQL(", ").join([
                sql.Identifier(col) for col in column_names
            ])
            data_query = sql.SQL("SELECT {} FROM {}.{}").format(
                column_list,
                sql.Identifier(schema),
                sql.Identifier(table_name)
            )

            # Add pagination
            if limit is not None:
                data_query = sql.SQL("{} LIMIT %s OFFSET %s").format(data_query)
                cursor.execute(data_query, (limit, offset))
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
        """Extract LSN (Log Sequence Number) or offset metadata from PostgreSQL.

        This method extracts the current position in the Write-Ahead Log (WAL),
        which can be used for tracking replication or change data capture positions.

        Args:
            database: Database name (optional, uses config default if not provided)

        Returns:
            Dictionary containing LSN/offset information
        """
        database = database or self.config.get("database")
        if not database:
            raise ValueError("Database name is required")

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            metadata = {}

            # Get current WAL LSN
            try:
                cursor.execute("SELECT pg_current_wal_lsn() AS current_lsn")
                result = cursor.fetchone()
                current_lsn = result["current_lsn"] if result else None
            except Exception as e:
                logger.debug(f"Could not get current WAL LSN: {e}")
                # Fallback for older PostgreSQL versions
                try:
                    cursor.execute("SELECT pg_current_xlog_location() AS current_lsn")
                    result = cursor.fetchone()
                    current_lsn = result["current_lsn"] if result else None
                except Exception as e2:
                    logger.debug(f"Could not get current xlog location: {e2}")
                    current_lsn = None

            # Get WAL insert LSN
            try:
                cursor.execute("SELECT pg_current_wal_insert_lsn() AS insert_lsn")
                result = cursor.fetchone()
                insert_lsn = result["insert_lsn"] if result else None
                if insert_lsn:
                    metadata["insert_lsn"] = insert_lsn
            except Exception:
                pass

            # Get replication slots information
            try:
                cursor.execute("""
                    SELECT 
                        slot_name,
                        slot_type,
                        active,
                        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
                    FROM pg_replication_slots
                """)
                replication_slots = cursor.fetchall()
                if replication_slots:
                    metadata["replication_slots"] = [
                        {
                            "slot_name": row["slot_name"],
                            "slot_type": row["slot_type"],
                            "active": row["active"],
                            "lag_bytes": row["lag_bytes"]
                        }
                        for row in replication_slots
                    ]
            except Exception as e:
                logger.debug(f"Could not get replication slots: {e}")
            
            # Get replication stats (active connections)
            try:
                cursor.execute("""
                    SELECT 
                        pid, usename, application_name, client_addr, state,
                        write_lag, flush_lag, replay_lag
                    FROM pg_stat_replication
                """)
                replication_stats = cursor.fetchall()
                if replication_stats:
                    metadata["replication_stats"] = []
                    for row in replication_stats:
                        # Convert intervals to seconds if present
                        write_lag = row.get("write_lag")
                        flush_lag = row.get("flush_lag")
                        replay_lag = row.get("replay_lag")
                        
                        stat = {
                            "pid": row.get("pid"),
                            "application_name": row.get("application_name"),
                            "state": row.get("state"),
                            "client_addr": row.get("client_addr")
                        }
                        
                        if write_lag:
                            stat["write_lag_seconds"] = write_lag.total_seconds()
                        if flush_lag:
                            stat["flush_lag_seconds"] = flush_lag.total_seconds()
                        if replay_lag:
                            stat["replay_lag_seconds"] = replay_lag.total_seconds()
                            
                        metadata["replication_stats"].append(stat)
            except Exception as e:
                logger.debug(f"Could not get replication stats: {e}")

            # Get PostgreSQL version
            try:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()["version"]
                metadata["postgresql_version"] = version[:100] if version else None
            except Exception:
                pass

            # If we don't have an LSN, use a timestamp-based approach
            if not current_lsn:
                current_lsn = f"TIMESTAMP:{datetime.utcnow().isoformat()}"
                metadata["lsn_source"] = "timestamp_fallback"
            else:
                metadata["lsn_source"] = "wal"

            # Calculate numeric offset from LSN if possible
            # PostgreSQL LSN format: XXXX/XXXXXXXX (hex)
            offset = None
            if current_lsn and "/" in str(current_lsn):
                try:
                    parts = str(current_lsn).split("/")
                    if len(parts) == 2:
                        # Convert hex to decimal
                        high = int(parts[0], 16)
                        low = int(parts[1], 16)
                        offset = (high << 32) | low
                except (ValueError, IndexError):
                    pass

            lsn_result = {
                "lsn": str(current_lsn) if current_lsn else None,
                "offset": offset,
                "timestamp": datetime.utcnow().isoformat(),
                "database": database,
                "metadata": metadata
            }

            logger.info(f"Extracted LSN/offset for database {database}: LSN={current_lsn}")
            return lsn_result

        except Exception as e:
            logger.error(f"Error extracting LSN/offset from PostgreSQL: {e}")
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
        database = database or self.config.get("database")
        schema = schema or self.config.get("schema", "public")

        if not database:
            raise ValueError("Database name is required")

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

        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
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
                            columns = self._extract_columns_internal(
                                cursor, database, schema, table_name
                            )
                            # Get table type
                            type_query = """
                                SELECT table_type
                                FROM information_schema.tables
                                WHERE table_schema = %s AND table_name = %s
                            """
                            cursor.execute(type_query, (schema, table_name))
                            type_row = cursor.fetchone()
                            table_type = type_row["table_type"] if type_row else "BASE TABLE"

                            properties = self._extract_table_properties(
                                cursor, schema, table_name, table_type
                            )

                            table_result["schema"] = {
                                "database": database,
                                "schema": schema,
                                "table": table_name,
                                "columns": columns,
                                "table_properties": properties,
                                "table_type": "view" if table_type == "VIEW" else "table"
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

        finally:
            cursor.close()
            conn.close()

        logger.info(
            f"Full load completed: {result['metadata']['tables_successful']} successful, "
            f"{result['metadata']['tables_failed']} failed out of "
            f"{result['metadata']['tables_processed']} tables"
        )

        return result

    def test_connection(self) -> bool:
        """Test the PostgreSQL connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            cursor.close()
            conn.close()
            logger.info(f"PostgreSQL connection test successful. Version: {version[0][:50] if version else 'unknown'}...")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {e}")
            return False

    def validate_cdc_setup(self, database: Optional[str] = None) -> Dict[str, Any]:
        """Validate CDC setup for PostgreSQL.
        
        PostgreSQL CDC requires:
        - Logical replication enabled (wal_level = logical)
        - Replication slot permissions
        - Publication permissions
        
        Args:
            database: Database name (optional, uses config default if not provided)
            
        Returns:
            Validation result dictionary
        """
        database = database or self.config.get("database")
        if not database:
            raise ValueError("Database name is required")
        
        conn = self.connect()
        cursor = conn.cursor()
        
        result = {
            "database": database,
            "wal_level": None,
            "replication_slots_available": False,
            "publication_permissions": False,
            "errors": [],
            "warnings": []
        }
        
        try:
            # Check WAL level
            wal_level_query = "SHOW wal_level"
            cursor.execute(wal_level_query)
            wal_level = cursor.fetchone()
            if wal_level:
                result["wal_level"] = wal_level[0]
                if wal_level[0] != "logical":
                    result["errors"].append(
                        f"WAL level is '{wal_level[0]}', but 'logical' is required for CDC. "
                        "Update postgresql.conf: wal_level = logical"
                    )
            
            # Check replication slot permissions
            try:
                cursor.execute("SELECT COUNT(*) FROM pg_replication_slots")
                result["replication_slots_available"] = True
            except Exception as e:
                result["errors"].append(f"No permission to access replication slots: {e}")
            
            # Check publication permissions
            try:
                cursor.execute("SELECT COUNT(*) FROM pg_publication")
                result["publication_permissions"] = True
            except Exception as e:
                result["errors"].append(f"No permission to access publications: {e}")
            
        except Exception as e:
            result["errors"].append(f"Error validating CDC setup: {str(e)}")
            logger.error(f"CDC validation failed: {e}")
        finally:
            cursor.close()
            conn.close()
        
        return result

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
        database = database or self.config.get("database")
        schema = schema or self.config.get("schema", "public")
        
        if not database:
            raise ValueError("Database name is required")
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                    AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_name
            """
            cursor.execute(query, (schema,))
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            return tables
        except Exception as e:
            logger.error(f"Error listing tables: {e}", exc_info=True)
            raise

    def get_cdc_tables(self, database: Optional[str] = None, schema: Optional[str] = None) -> List[str]:
        """Get list of tables available for CDC.
        
        For PostgreSQL, all tables are available for CDC if replication is set up.
        This method returns all tables in the specified schema.
        
        Args:
            database: Database name (optional)
            schema: Schema name (optional)
            
        Returns:
            List of table names
        """
        return self.list_tables(database=database, schema=schema)

    def list_databases(self) -> List[str]:
        """List all databases accessible to the user.

        Returns:
            List of database names
        """
        try:
            # Connect to default 'postgres' database to list all databases
            original_db = self.config.get("database")
            self.config["database"] = "postgres"
            conn = self.connect()
            cursor = conn.cursor()
            
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
            databases = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            # Restore original database
            if original_db:
                self.config["database"] = original_db
            else:
                self.config.pop("database", None)
            
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
        database = database or self.config.get("database")
        if not database:
            raise ValueError("Database name is required")
        
        try:
            original_db = self.config.get("database")
            self.config["database"] = database
            conn = self.connect()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY schema_name
            """)
            schemas = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            # Restore original database
            if original_db:
                self.config["database"] = original_db
            else:
                self.config.pop("database", None)
            
            logger.info(f"Found {len(schemas)} schemas in {database}: {schemas}")
            return schemas
        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
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
        database = database or self.config.get("database")
        schema = schema or self.config.get("schema", "public")
        
        if not database:
            raise ValueError("Database name is required")
        
        # Clean table name - remove schema prefix if present (e.g., "public.projects_simple" -> "projects_simple")
        clean_table_name = table
        # Use %r (repr) for logging to avoid formatting issues
        logger.info("[PostgreSQL get_table_columns] Input: table=%r, schema=%r, database=%r", table, schema, database)
        if '.' in clean_table_name and schema:
            parts = clean_table_name.split('.', 1)
            logger.info("[PostgreSQL get_table_columns] Table name has dot, parts: %r, comparing %r == %r", parts, parts[0] if len(parts) > 0 else None, schema)
            if len(parts) == 2 and parts[0] == schema:
                clean_table_name = parts[1]
                logger.info("[PostgreSQL get_table_columns] Removed duplicate schema from table name: %r -> %r", table, clean_table_name)
            else:
                logger.info("[PostgreSQL get_table_columns] Schema prefix %r does not match schema %r, keeping original name", parts[0] if len(parts) > 0 else None, schema)
        else:
            logger.info("[PostgreSQL get_table_columns] No schema prefix to clean, using table as-is: %r", clean_table_name)
        
        # Ensure schema and table are strings
        schema_str = str(schema) if schema else "public"
        table_str = str(clean_table_name) if clean_table_name else ""
        if not table_str:
            raise ValueError("Table name cannot be empty")
        
        # Validate that schema and table are strings before passing to cursor.execute
        if not isinstance(schema_str, str):
            schema_str = str(schema_str)
        if not isinstance(table_str, str):
            table_str = str(table_str)
        
        conn = self.connect()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            query = """
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position,
                    is_nullable,
                    column_default,
                    udt_name
                FROM information_schema.columns
                WHERE table_catalog = %s
                    AND table_schema = %s
                    AND table_name = %s
                ORDER BY ordinal_position
            """
            # Execute query with parameters as a tuple - ensure all are strings
            logger.info("[PostgreSQL get_table_columns] Executing query with: database=%r, schema=%r, table=%r", str(database), schema_str, table_str)
            cursor.execute(query, (str(database), schema_str, table_str))
            logger.info("[PostgreSQL get_table_columns] Query executed successfully, fetching results...")
            
            columns = []
            for row in cursor.fetchall():
                col_name = row["column_name"]
                data_type = row["data_type"]
                max_length = row["character_maximum_length"]
                precision = row["numeric_precision"]
                scale = row["numeric_scale"]
                
                # Build full data type string
                full_data_type = data_type
                if max_length and data_type in ("character varying", "varchar", "character", "char"):
                    full_data_type = f"{data_type}({max_length})"
                elif precision and data_type in ("numeric", "decimal"):
                    if scale:
                        full_data_type = f"{data_type}({precision},{scale})"
                    else:
                        full_data_type = f"{data_type}({precision})"
                
                columns.append({
                    "name": col_name,
                    "data_type": full_data_type,
                    "udt_name": row["udt_name"],
                    "ordinal_position": int(row["ordinal_position"]),
                    "is_nullable": row["is_nullable"] == "YES",
                    "default_value": str(row["column_default"]) if row["column_default"] else None
                })
            
            return columns
            
        except Exception as e:
            # Safely get error message to avoid formatting issues
            try:
                from ingestion.connection_service import safe_error_message
                safe_error_msg = safe_error_message(e)
            except Exception:
                # Fallback if import fails
                try:
                    if hasattr(e, 'args') and e.args and len(e.args) > 0:
                        first_arg = e.args[0]
                        if isinstance(first_arg, str):
                            safe_error_msg = first_arg.replace('%', '%%')
                        else:
                            safe_error_msg = repr(first_arg).replace('%', '%%')
                    else:
                        safe_error_msg = "Error type: " + type(e).__name__
                except Exception:
                    safe_error_msg = "Unknown error occurred"
            
            # Use logger with %r (repr) to avoid any formatting issues
            try:
                logger.error("Failed to get table columns: %r", safe_error_msg, exc_info=True)
            except Exception as log_error:
                # If logging fails, just print to avoid cascading errors
                try:
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
        finally:
            cursor.close()
            conn.close()

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
        database = database or self.config.get("database")
        schema = schema or self.config.get("schema", "public")
        
        if not database:
            raise ValueError("Database name is required")
        
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary = true
                    AND n.nspname = %s
                    AND c.relname = %s
                ORDER BY a.attnum
            """
            cursor.execute(query, (schema, table))
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            return primary_keys
            
        except Exception as e:
            logger.error(f"Failed to get primary keys: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

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
        target_database = database or self.config.get("database")
        target_schema = schema or self.config.get("schema", "public")
        
        if not target_database:
            raise ValueError("Database name is required")
        
        # Create connection to the target database
        # If database is different from config, we need to connect to that database
        conn = None
        try:
            host = self.config["host"]
            port = self.config.get("port", 5432)
            user = self.config["user"]
            password = self.config["password"]
            
            # Connect to the target database
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=target_database,  # Use target database
                user=user,
                password=password,
                connect_timeout=30,
                sslmode='disable'
            )
            conn.autocommit = True
            
            cursor = conn.cursor()
            
            try:
                query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(target_schema),
                    sql.Identifier(table)
                )
                cursor.execute(query)
                row_count = cursor.fetchone()[0]
                result = int(row_count) if row_count else 0
                logger.debug(f"Row count for {target_database}.{target_schema}.{table}: {result}")
                return result
                
            except Exception as e:
                logger.error(f"Failed to get row count for {target_database}.{target_schema}.{table}: {e}", exc_info=True)
                raise
            finally:
                cursor.close()
                
        except Exception as e:
            logger.error(f"Failed to connect or get row count: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()

    def get_version(self) -> str:
        """Get database version.

        Returns:
            Database version string
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
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
                       If None, this is a no-op (connections are typically closed after each operation).
        
        Note: PostgreSQL connections are typically closed after each operation.
        This method is provided for consistency with other connectors.
        """
        if connection and hasattr(connection, 'close'):
            try:
                connection.close()
                logger.debug("PostgreSQL connection closed")
            except Exception as e:
                logger.warning(f"Error closing PostgreSQL connection: {e}")


