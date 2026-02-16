"""Data transfer utility for copying data between databases."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from ingestion.connectors.base_connector import BaseConnector
from ingestion.connectors.sqlserver import SQLServerConnector
from ingestion.connectors.postgresql import PostgreSQLConnector
from ingestion.connectors.oracle import OracleConnector
from ingestion.connectors.as400 import AS400Connector

logger = logging.getLogger(__name__)


class DataTransfer:
    """Utility class for transferring data between databases.
    
    Supports:
    - SQL Server → SQL Server
    - PostgreSQL → PostgreSQL
    - SQL Server → PostgreSQL (with type mapping)
    - PostgreSQL → SQL Server (with type mapping)
    - PostgreSQL → Oracle (with type mapping)
    - AS400/DB2 → SQL Server (with type mapping)
    - AS400/DB2 → PostgreSQL (with type mapping)
    """

    # Type mapping from SQL Server to PostgreSQL
    # Note: datetime2 maps to bigint because Debezium SQL Server sends it as epoch nanoseconds (int64).
    # For a readable timestamp, add a generated column: col_ts timestamp GENERATED ALWAYS AS (to_timestamp(col/1e9)) STORED
    SQLSERVER_TO_POSTGRESQL_TYPE_MAP = {
        "int": "integer",
        "bigint": "bigint",
        "smallint": "smallint",
        "tinyint": "smallint",  # PostgreSQL doesn't have tinyint
        "bit": "boolean",
        "decimal": "numeric",
        "numeric": "numeric",
        "float": "double precision",
        "real": "real",
        "money": "numeric(19,4)",
        "smallmoney": "numeric(10,4)",
        "char": "char",
        "varchar": "varchar",
        "nchar": "char",
        "nvarchar": "varchar",
        "text": "text",
        "ntext": "text",
        "date": "date",
        "time": "time",
        "datetime": "timestamp",
        "datetime2": "bigint",  # Debezium emits epoch nanoseconds; use generated column for timestamp display
        "smalldatetime": "timestamp",
        "datetimeoffset": "timestamp with time zone",
        "timestamp": "bytea",  # SQL Server timestamp is binary
        "binary": "bytea",
        "varbinary": "bytea",
        "image": "bytea",
        "uniqueidentifier": "uuid",
        "xml": "xml",
    }

    # Type mapping from PostgreSQL to SQL Server
    POSTGRESQL_TO_SQLSERVER_TYPE_MAP = {
        "integer": "int",
        "bigint": "bigint",
        "smallint": "smallint",
        "boolean": "bit",
        "numeric": "decimal",
        "decimal": "decimal",
        "double precision": "float",
        "real": "real",
        "char": "char",
        "character": "char",
        "varchar": "varchar",
        "character varying": "varchar",
        "text": "text",
        "date": "date",
        "time": "time",
        "timestamp": "datetime2",
        "timestamp without time zone": "datetime2",
        "timestamp with time zone": "datetimeoffset",
        "bytea": "varbinary",
        "uuid": "uniqueidentifier",
        "xml": "xml",
    }

    # Type mapping from PostgreSQL to Oracle (for PostgreSQL → Oracle pipelines)
    POSTGRESQL_TO_ORACLE_TYPE_MAP = {
        "integer": "NUMBER(10)",
        "int": "NUMBER(10)",
        "bigint": "NUMBER(19)",
        "smallint": "NUMBER(5)",
        "boolean": "NUMBER(1)",
        "numeric": "NUMBER",
        "decimal": "NUMBER",
        "double precision": "BINARY_DOUBLE",
        "real": "BINARY_FLOAT",
        "char": "CHAR",
        "character": "CHAR",
        "varchar": "VARCHAR2",
        "character varying": "VARCHAR2",
        "text": "CLOB",
        "date": "DATE",
        "time": "TIMESTAMP",
        "timestamp": "TIMESTAMP",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMP WITH TIME ZONE",
        "bytea": "BLOB",
        "uuid": "VARCHAR2(36)",
        "xml": "XMLTYPE",
    }

    # Type mapping from AS400/DB2 (IBM i) to SQL Server
    AS400_TO_SQLSERVER_TYPE_MAP = {
        "char": "char",
        "varchar": "varchar",
        "graphic": "nchar",
        "vargraphic": "nvarchar",
        "clob": "nvarchar(max)",
        "dbclob": "nvarchar(max)",
        "blob": "varbinary(max)",
        "decimal": "decimal",
        "numeric": "decimal",
        "integer": "int",
        "int": "int",
        "smallint": "smallint",
        "bigint": "bigint",
        "real": "real",
        "double": "float",
        "float": "float",
        "date": "date",
        "time": "time",
        "timestamp": "datetime2",
        "timezone": "datetimeoffset",
    }

    def __init__(
        self,
        source_connector: BaseConnector,
        target_connector: BaseConnector
    ):
        """Initialize data transfer utility.

        Args:
            source_connector: Source database connector instance
            target_connector: Target database connector instance
        """
        self.source = source_connector
        self.target = target_connector
        self._validate_connectors()

    def _validate_connectors(self) -> None:
        """Validate that connectors are properly initialized."""
        if not isinstance(self.source, BaseConnector):
            raise ValueError("Source connector must be a BaseConnector instance")
        if not isinstance(self.target, BaseConnector):
            raise ValueError("Target connector must be a BaseConnector instance")

    def _get_type_mapping(
        self,
        source_type: str,
        target_type: str
    ) -> Optional[str]:
        """Get mapped data type for cross-database transfers.

        Args:
            source_type: Source database type
            target_type: Target database type ('sqlserver' or 'postgresql')

        Returns:
            Mapped type name or None if no mapping exists
        """
        source_type_lower = source_type.lower().split("(")[0].strip()

        if isinstance(self.source, SQLServerConnector) and isinstance(self.target, PostgreSQLConnector):
            return self.SQLSERVER_TO_POSTGRESQL_TYPE_MAP.get(source_type_lower)
        elif isinstance(self.source, PostgreSQLConnector) and isinstance(self.target, SQLServerConnector):
            return self.POSTGRESQL_TO_SQLSERVER_TYPE_MAP.get(source_type_lower)
        elif isinstance(self.source, PostgreSQLConnector) and isinstance(self.target, OracleConnector):
            return self.POSTGRESQL_TO_ORACLE_TYPE_MAP.get(source_type_lower)
        elif isinstance(self.source, AS400Connector) and isinstance(self.target, SQLServerConnector):
            return self.AS400_TO_SQLSERVER_TYPE_MAP.get(source_type_lower)
        elif isinstance(self.source, AS400Connector) and isinstance(self.target, PostgreSQLConnector):
            return self.AS400_TO_POSTGRESQL_TYPE_MAP.get(source_type_lower)

        # Same database type, no mapping needed
        return None

    def transfer_table(
        self,
        table_name: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        transfer_schema: bool = True,
        transfer_data: bool = True,
        batch_size: int = 1000,
        create_if_not_exists: bool = True
    ) -> Dict[str, Any]:
        """Transfer a single table from source to target database.

        Args:
            table_name: Name of the table to transfer
            source_database: Source database name (optional)
            source_schema: Source schema name (optional)
            target_database: Target database name (optional)
            target_schema: Target schema name (optional)
            transfer_schema: Whether to transfer/create table schema
            transfer_data: Whether to transfer table data
            batch_size: Number of rows to insert per batch
            create_if_not_exists: Whether to create table if it doesn't exist

        Returns:
            Dictionary with transfer results
        """
        logger.info(
            f"Starting table transfer: {table_name} from "
            f"{source_database}.{source_schema} to {target_database}.{target_schema}"
        )

        result = {
            "table_name": table_name,
            "schema_transferred": False,
            "data_transferred": False,
            "rows_transferred": 0,
            "errors": []
        }

        try:
            # Transfer schema if requested
            if transfer_schema:
                try:
                    self.transfer_schema(
                        table_name=table_name,
                        source_database=source_database,
                        source_schema=source_schema,
                        target_database=target_database,
                        target_schema=target_schema,
                        create_if_not_exists=create_if_not_exists
                    )
                    result["schema_transferred"] = True
                except Exception as e:
                    error_msg = f"Schema transfer failed: {str(e)}"
                    logger.error(error_msg)
                    result["errors"].append(error_msg)
                    if not create_if_not_exists:
                        raise

            # Transfer data if requested
            if transfer_data:
                try:
                    rows_transferred = self.transfer_data(
                        table_name=table_name,
                        source_database=source_database,
                        source_schema=source_schema,
                        target_database=target_database,
                        target_schema=target_schema,
                        batch_size=batch_size
                    )
                    result["data_transferred"] = True
                    result["rows_transferred"] = rows_transferred
                    
                    # Validate that rows were actually transferred
                    # Note: transfer_data now raises an exception if 0 rows when source has data
                    # So if we get here with 0 rows, it means source is empty (which is OK)
                    if rows_transferred == 0:
                        logger.info(f"Data transfer completed with 0 rows for {table_name} (source table may be empty)")
                        
                except Exception as e:
                    error_msg = f"Data transfer failed: {str(e)}"
                    logger.error(error_msg)
                    result["errors"].append(error_msg)
                    # Re-raise to propagate the error
                    raise

            logger.info(
                f"Table transfer completed: {table_name} "
                f"({result['rows_transferred']} rows transferred)"
            )

        except Exception as e:
            # Only catch exceptions that weren't already handled
            logger.error(f"Table transfer failed for {table_name}: {e}")
            if str(e) not in result["errors"]:
                result["errors"].append(str(e))
            # Re-raise critical exceptions
            if "Data transfer failed" in str(e) or "Schema transfer failed" in str(e):
                raise

        return result

    def transfer_tables(
        self,
        tables: List[str],
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        transfer_schema: bool = True,
        transfer_data: bool = True,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """Transfer multiple tables from source to target database.

        Args:
            tables: List of table names to transfer
            source_database: Source database name (optional)
            source_schema: Source schema name (optional)
            target_database: Target database name (optional)
            target_schema: Target schema name (optional)
            transfer_schema: Whether to transfer/create table schemas
            transfer_data: Whether to transfer table data
            batch_size: Number of rows to insert per batch

        Returns:
            Dictionary with transfer results for all tables
        """
        results = {
            "tables_processed": len(tables),
            "tables_successful": 0,
            "tables_failed": 0,
            "total_rows_transferred": 0,
            "tables": []
        }

        for table_name in tables:
            try:
                table_result = self.transfer_table(
                    table_name=table_name,
                    source_database=source_database,
                    source_schema=source_schema,
                    target_database=target_database,
                    target_schema=target_schema,
                    transfer_schema=transfer_schema,
                    transfer_data=transfer_data,
                    batch_size=batch_size
                )

                results["tables"].append(table_result)

                if table_result["errors"]:
                    results["tables_failed"] += 1
                elif table_result.get("data_transferred") and table_result.get("rows_transferred", 0) == 0:
                    # Data transfer was attempted but 0 rows transferred - this indicates failure
                    # (unless source table is actually empty, but we can't know that here)
                    # Add a warning/error to indicate this suspicious state
                    if not table_result.get("errors"):
                        table_result["errors"] = ["Data transfer reported success but transferred 0 rows. This may indicate a schema mismatch or insertion failure."]
                    results["tables_failed"] += 1
                else:
                    results["tables_successful"] += 1
                    results["total_rows_transferred"] += table_result["rows_transferred"]

            except Exception as e:
                logger.error(f"Failed to transfer table {table_name}: {e}")
                results["tables_failed"] += 1
                results["tables"].append({
                    "table_name": table_name,
                    "error": str(e),
                    "schema_transferred": False,
                    "data_transferred": False,
                    "rows_transferred": 0
                })

        logger.info(
            f"Bulk transfer completed: {results['tables_successful']} successful, "
            f"{results['tables_failed']} failed, "
            f"{results['total_rows_transferred']} total rows transferred"
        )

        return results

    def transfer_schema(
        self,
        table_name: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        create_if_not_exists: bool = True
    ) -> None:
        """Transfer table schema (DDL) from source to target.

        Args:
            table_name: Name of the table
            source_database: Source database name (optional)
            source_schema: Source schema name (optional)
            target_database: Target database name (optional)
            target_schema: Target schema name (optional)
            create_if_not_exists: Whether to create table if it doesn't exist

        Raises:
            NotImplementedError: If cross-database schema transfer is not supported
        """
        # Extract schema from source
        source_schema_info = self.source.extract_schema(
            database=source_database,
            schema=source_schema,
            table=table_name
        )

        if not source_schema_info.get("tables"):
            raise ValueError(f"Table {table_name} not found in source database")

        source_table = source_schema_info["tables"][0]
        source_columns = source_table["columns"]

        # Generate CREATE TABLE statement for target
        if isinstance(self.target, SQLServerConnector):
            self._create_sqlserver_table(
                table_name, source_columns, target_database, target_schema, create_if_not_exists
            )
        elif isinstance(self.target, PostgreSQLConnector):
            self._create_postgresql_table(
                table_name, source_columns, target_database, target_schema, create_if_not_exists
            )
        elif isinstance(self.target, OracleConnector):
            self._create_oracle_table(
                table_name, source_columns, target_database, target_schema, create_if_not_exists
            )
        else:
            raise NotImplementedError(
                f"Schema transfer to {type(self.target).__name__} is not yet supported"
            )

    def _create_sqlserver_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        database: Optional[str],
        schema: Optional[str],
        create_if_not_exists: bool
    ) -> None:
        """Create SQL Server table from column definitions."""
        schema_name = schema or "dbo"
        conn = self.target.connect()
        cursor = conn.cursor()

        try:
            if database:
                cursor.execute(f"USE [{database}]")

            # Check if table exists
            check_query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """
            cursor.execute(check_query, (schema_name, table_name))
            exists = cursor.fetchone()[0] > 0

            if exists and not create_if_not_exists:
                logger.info(f"Table {schema_name}.{table_name} already exists, skipping creation")
                return

            if exists:
                # Drop existing table
                drop_query = f"DROP TABLE [{schema_name}].[{table_name}]"
                cursor.execute(drop_query)
                logger.info(f"Dropped existing table {schema_name}.{table_name}")

            # Build column definitions
            column_defs = []
            for col in columns:
                col_name = col["name"]
                data_type = col["data_type"]
                is_nullable = col.get("is_nullable", True)
                default_value = col.get("default_value")
                json_schema = col.get("json_schema", {})

                # Extract length/precision from json_schema first (most reliable)
                max_length = json_schema.get("max_length") if json_schema else None
                precision = json_schema.get("precision") if json_schema else None
                scale = json_schema.get("scale") if json_schema else None
                
                # If not in json_schema, try to parse from data_type string (e.g., "varchar(100)" or "character varying(100)")
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

                # Map type if needed (get base type without length)
                # Handle PostgreSQL types like "character varying", "character", etc.
                base_type = data_type.lower().split("(")[0].strip()
                
                # Map PostgreSQL types to SQL Server
                type_mapping = {
                    "character varying": "varchar",
                    "varchar": "varchar",
                    "character": "char",
                    "char": "char",
                    "text": "text",
                    "integer": "int",
                    "bigint": "bigint",
                    "smallint": "smallint",
                    "boolean": "bit",
                    "numeric": "decimal",
                    "decimal": "decimal",
                    "real": "real",
                    "double precision": "float",
                    "date": "date",
                    "time": "time",
                    "timestamp": "datetime2",
                    "timestamp without time zone": "datetime2",
                    "timestamp with time zone": "datetimeoffset"
                }
                
                mapped_type = type_mapping.get(base_type)
                if mapped_type:
                    data_type = mapped_type
                else:
                    # Try the existing type mapping method
                    mapped_type = self._get_type_mapping(base_type, "sqlserver")
                    if mapped_type:
                        data_type = mapped_type
                    else:
                        # Use base type if no mapping
                        data_type = base_type

                # Add length/precision to mapped type
                # CRITICAL: Always preserve max_length for VARCHAR/CHAR types
                if max_length is not None and data_type.lower() in ("varchar", "char", "nvarchar", "nchar"):
                    data_type = f"{data_type}({max_length})"
                elif max_length is None and data_type.lower() in ("varchar", "char"):
                    # If no max_length specified but it's a varchar/char, use MAX or a reasonable default
                    # For PostgreSQL character varying without length, use MAX
                    if base_type in ("character varying", "varchar"):
                        data_type = "varchar(max)"
                    else:
                        data_type = f"{data_type}(255)"  # Default fallback
                elif precision is not None and data_type.lower() in ("decimal", "numeric"):
                    if scale is not None:
                        data_type = f"{data_type}({precision},{scale})"
                    else:
                        data_type = f"{data_type}({precision})"

                nullable = "NULL" if is_nullable else "NOT NULL"
                
                # Clean and convert PostgreSQL default values to SQL Server compatible syntax
                default = ""
                if default_value:
                    default_value_str = str(default_value).strip()
                    
                    # Remove PostgreSQL casting syntax (::type)
                    # e.g., "nextval('seq'::regclass)" -> "nextval('seq')"
                    import re
                    default_value_str = re.sub(r'::\w+', '', default_value_str)
                    
                    # Convert PostgreSQL-specific functions to SQL Server equivalents
                    default_upper = default_value_str.upper()
                    
                    # PostgreSQL sequences -> SQL Server IDENTITY (handled separately if needed)
                    if "nextval" in default_upper:
                        # For sequences, we'll skip default and let SQL Server handle IDENTITY
                        # Or convert to a SQL Server compatible expression
                        default = ""  # Skip default for sequences
                    elif "gen_random_uuid()" in default_upper or "uuid_generate" in default_upper:
                        default = " DEFAULT NEWID()"
                    elif "current_timestamp" in default_upper or "now()" in default_upper:
                        default = " DEFAULT GETDATE()"
                    elif "current_date" in default_upper:
                        default = " DEFAULT CAST(GETDATE() AS DATE)"
                    elif "current_time" in default_upper:
                        default = " DEFAULT CAST(GETDATE() AS TIME)"
                    elif default_value_str.startswith("'") and default_value_str.endswith("'"):
                        # String literal - use as is
                        default = f" DEFAULT {default_value_str}"
                    elif default_value_str.replace('.', '').replace('-', '').isdigit():
                        # Numeric literal - use as is
                        default = f" DEFAULT {default_value_str}"
                    elif "(" in default_value_str or "::" in default_value_str:
                        # Complex expression with PostgreSQL syntax - skip it
                        logger.warning(
                            f"Skipping default value for column {col_name}: {default_value_str} "
                            f"(contains PostgreSQL-specific syntax that cannot be converted to SQL Server)"
                        )
                        default = ""
                    else:
                        # Try to use as is, but wrap in parentheses if it looks like a function call
                        if "(" in default_value_str:
                            default = f" DEFAULT {default_value_str}"
                        else:
                            default = f" DEFAULT {default_value_str}"

                column_defs.append(f"[{col_name}] {data_type} {nullable}{default}")
                
                # Log for debugging
                logger.debug(f"Column {col_name}: {data_type} (max_length={max_length}, precision={precision}, scale={scale})")

            # Add surrogate key and CDC metadata columns for SCD2-style history (matches JDBC Sink + Debezium add.fields)
            # row_id: surrogate PK so multiple rows per business key are allowed
            # __op, __source_ts_ms, __deleted: produced by ExtractNewRecordState add.fields=op,source.ts_ms and delete.handling.mode=rewrite
            column_defs.insert(0, "[row_id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY")
            column_defs.append("[__op] NVARCHAR(10) NULL")
            column_defs.append("[__source_ts_ms] BIGINT NULL")
            column_defs.append("[__deleted] NVARCHAR(10) NULL")

            # Create table
            create_query = f"""
                CREATE TABLE [{schema_name}].[{table_name}] (
                    {', '.join(column_defs)}
                )
            """
            cursor.execute(create_query)
            conn.commit()
            logger.info(f"Created table {schema_name}.{table_name}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create SQL Server table: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def _create_postgresql_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        database: Optional[str],
        schema: Optional[str],
        create_if_not_exists: bool
    ) -> None:
        """Create PostgreSQL table from column definitions."""
        from psycopg2 import sql

        schema_name = schema or "public"
        conn = self.target.connect()
        cursor = conn.cursor()

        try:
            # Check if table exists
            check_query = """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """
            cursor.execute(check_query, (schema_name, table_name))
            exists = cursor.fetchone()[0] > 0

            if exists and not create_if_not_exists:
                logger.info(f"Table {schema_name}.{table_name} already exists, skipping creation")
                return

            if exists:
                # Drop existing table
                drop_query = sql.SQL("DROP TABLE {}.{}").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name)
                )
                cursor.execute(drop_query)
                logger.info(f"Dropped existing table {schema_name}.{table_name}")

            # Build column definitions
            column_defs = []
            for col in columns:
                col_name = col["name"]
                data_type = col["data_type"]
                is_nullable = col.get("is_nullable", True)
                default_value = col.get("default_value")

                # Map type if needed
                mapped_type = self._get_type_mapping(data_type, "postgresql")
                if mapped_type:
                    data_type = mapped_type

                nullable = "" if is_nullable else " NOT NULL"
                
                # Handle default values - convert SQL Server functions to PostgreSQL equivalents
                default = ""
                if default_value:
                    default_str = str(default_value).upper().strip()
                    # SQL Server bit -> PostgreSQL boolean: 0/((0)) -> false, 1/((1)) -> true
                    if data_type.lower() == "boolean" and isinstance(self.source, SQLServerConnector):
                        if default_str in ("0", "((0))", "(0)"):
                            default = " DEFAULT false"
                        elif default_str in ("1", "((1))", "(1)"):
                            default = " DEFAULT true"
                        else:
                            default = ""
                    elif "GETDATE()" in default_str:
                        default = " DEFAULT CURRENT_TIMESTAMP"
                    elif "GETUTCDATE()" in default_str:
                        default = " DEFAULT (NOW() AT TIME ZONE 'UTC')"
                    elif "NEWID()" in default_str or "NEWSEQUENTIALID()" in default_str:
                        default = " DEFAULT gen_random_uuid()"
                    elif default_str.startswith("((") and default_str.endswith("))"):
                        # Remove extra parentheses from SQL Server defaults
                        default_val = default_str[2:-2].strip()
                        if "GETDATE()" in default_val:
                            default = " DEFAULT CURRENT_TIMESTAMP"
                        else:
                            default = f" DEFAULT {default_val}"
                    else:
                        default = f" DEFAULT {default_value}"

                column_defs.append(
                    sql.SQL("{} {}{}{}").format(
                        sql.Identifier(col_name),
                        sql.SQL(data_type),
                        sql.SQL(nullable),
                        sql.SQL(default)
                    )
                )

            # SCD2-style: surrogate key + CDC metadata (same as SQL Server target)
            column_defs.insert(0, sql.SQL("row_id BIGSERIAL PRIMARY KEY"))
            column_defs.append(sql.SQL("{} VARCHAR(10)").format(sql.Identifier("__op")))
            column_defs.append(sql.SQL("{} BIGINT").format(sql.Identifier("__source_ts_ms")))
            column_defs.append(sql.SQL("{} VARCHAR(10)").format(sql.Identifier("__deleted")))

            # Create table
            create_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.SQL(", ").join(column_defs)
            )
            cursor.execute(create_query)
            conn.commit()
            logger.info(f"Created table {schema_name}.{table_name}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create PostgreSQL table: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def _create_oracle_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        database: Optional[str],
        schema: Optional[str],
        create_if_not_exists: bool
    ) -> None:
        """Create Oracle table from column definitions (e.g. from PostgreSQL source). SCD2-style: row_id + __op, __source_ts_ms, __deleted."""
        schema_name = (schema or self.target.config.get("schema") or self.target.config.get("user") or "PUBLIC").upper()
        table_name_upper = table_name.upper()
        conn = self.target.connect()
        cursor = conn.cursor()

        try:
            cursor.execute(
                "SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
                (schema_name, table_name_upper),
            )
            exists = cursor.fetchone()[0] > 0

            if exists and not create_if_not_exists:
                logger.info(f"Table {schema_name}.{table_name_upper} already exists, skipping creation")
                return

            if exists:
                cursor.execute(f'DROP TABLE "{schema_name}"."{table_name_upper}"')
                conn.commit()
                logger.info(f"Dropped existing table {schema_name}.{table_name_upper}")

            column_defs = []
            for col in columns:
                col_name = col["name"]
                data_type = col["data_type"]
                is_nullable = col.get("is_nullable", True)
                default_value = col.get("default_value")
                max_length = col.get("max_length")
                precision = col.get("precision")
                scale = col.get("scale")

                base_type = data_type.lower().split("(")[0].strip()
                mapped = self._get_type_mapping(data_type, "oracle") if isinstance(self.source, PostgreSQLConnector) else None
                if mapped:
                    data_type = mapped
                else:
                    data_type = base_type

                if "VARCHAR2" in data_type.upper():
                    data_type = f"VARCHAR2({min(max_length or 4000, 4000)})"
                elif max_length is not None and "CHAR" in data_type.upper() and "VARCHAR" not in data_type.upper():
                    data_type = f"CHAR({max_length})"
                elif "NUMBER" in data_type.upper() and "(" not in data_type and (precision is not None or scale is not None):
                    if precision is not None and scale is not None:
                        data_type = f"NUMBER({precision},{scale})"
                    elif precision is not None:
                        data_type = f"NUMBER({precision})"

                nullable = "" if is_nullable else " NOT NULL"
                default = ""
                if default_value:
                    default_str = str(default_value).upper().strip()
                    if "CURRENT_TIMESTAMP" in default_str or "NOW()" in default_str:
                        default = " DEFAULT CURRENT_TIMESTAMP"
                    elif "CURRENT_DATE" in default_str:
                        default = " DEFAULT CURRENT_DATE"
                    elif default_str.startswith("'") and default_str.endswith("'"):
                        default = f" DEFAULT {default_value}"
                    else:
                        default = ""
                column_defs.append(f'"{col_name}" {data_type}{nullable}{default}')

            column_defs.insert(0, '"row_id" NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY')
            column_defs.append('"__op" VARCHAR2(10)')
            column_defs.append('"__source_ts_ms" NUMBER(19)')
            column_defs.append('"__deleted" VARCHAR2(10)')

            create_sql = f'CREATE TABLE "{schema_name}"."{table_name_upper}" (\n  ' + ",\n  ".join(column_defs) + "\n)"
            cursor.execute(create_sql)
            conn.commit()
            logger.info(f"Created Oracle table {schema_name}.{table_name_upper}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create Oracle table: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def transfer_data(
        self,
        table_name: str,
        source_database: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_database: Optional[str] = None,
        target_schema: Optional[str] = None,
        batch_size: int = 1000
    ) -> int:
        """Transfer table data (DML) from source to target.

        Args:
            table_name: Name of the table
            source_database: Source database name (optional)
            source_schema: Source schema name (optional)
            target_database: Target database name (optional)
            target_schema: Target schema name (optional)
            batch_size: Number of rows to insert per batch

        Returns:
            Number of rows transferred
        """
        # Extract data from source in batches
        offset = 0
        total_rows = 0

        while True:
            # Get batch of data from source
            source_data = self.source.extract_data(
                database=source_database or self.source.config.get("database"),
                schema=source_schema or self.source.config.get("schema", "dbo" if isinstance(self.source, SQLServerConnector) else "public"),
                table_name=table_name,
                limit=batch_size,
                offset=offset
            )

            if not source_data.get("rows"):
                break

            rows = source_data["rows"]
            column_names = source_data["column_names"]

            # Insert batch into target
            try:
                self._insert_batch(
                    table_name=table_name,
                    rows=rows,
                    column_names=column_names,
                    target_database=target_database,
                    target_schema=target_schema
                )
                # Verify rows were actually inserted
                batch_rows_inserted = len(rows)
                total_rows += batch_rows_inserted
                offset += batch_rows_inserted
                
                logger.debug(f"Successfully inserted {batch_rows_inserted} rows into target")
            except Exception as e:
                logger.error(f"Failed to insert batch of {len(rows)} rows: {e}")
                # Re-raise to fail the transfer
                raise Exception(f"Data insertion failed for {table_name}: {str(e)}")

            logger.info(
                f"Transferred {total_rows} rows for table {table_name} "
                f"(batch of {len(rows)} rows)"
            )

            if not source_data.get("has_more", False):
                break

        logger.info(f"Data transfer completed: {total_rows} rows transferred for {table_name}")
        
        # If we attempted to transfer data but got 0 rows, check if source has data
        if total_rows == 0:
            # Check if source actually has data
            try:
                check_data = self.source.extract_data(
                    database=source_database or self.source.config.get("database"),
                    schema=source_schema or self.source.config.get("schema", "dbo" if isinstance(self.source, SQLServerConnector) else "public"),
                    table_name=table_name,
                    limit=1,
                    offset=0
                )
                if check_data.get("rows") or check_data.get("total_rows", 0) > 0:
                    # Source has data but we transferred 0 rows - this is a failure
                    raise Exception(
                        f"Data transfer failed for {table_name}: Source has data ({check_data.get('total_rows', 'unknown')} rows) "
                        f"but 0 rows were transferred. This indicates an insertion failure."
                    )
                else:
                    logger.info(f"Source table {table_name} is empty, 0 rows transferred is expected")
            except Exception as check_error:
                # If we can't check, assume failure if 0 rows
                if "Data transfer failed" in str(check_error):
                    raise  # Re-raise our own exception
                logger.warning(f"Could not verify source data for {table_name}: {check_error}")
                # Still raise an error - 0 rows when we expect data is suspicious
                raise Exception(
                    f"Data transfer returned 0 rows for {table_name} and could not verify source data. "
                    f"This may indicate a problem with the transfer."
                )
        
        return total_rows

    def _insert_batch(
        self,
        table_name: str,
        rows: List[List[Any]],
        column_names: List[str],
        target_database: Optional[str],
        target_schema: Optional[str]
    ) -> None:
        """Insert a batch of rows into target database."""
        if isinstance(self.target, SQLServerConnector):
            self._insert_batch_sqlserver(
                table_name, rows, column_names, target_database, target_schema
            )
        elif isinstance(self.target, PostgreSQLConnector):
            self._insert_batch_postgresql(
                table_name, rows, column_names, target_database, target_schema
            )
        elif isinstance(self.target, OracleConnector):
            self._insert_batch_oracle(
                table_name, rows, column_names, target_database, target_schema
            )
        else:
            raise NotImplementedError(
                f"Data insertion to {type(self.target).__name__} is not yet supported"
            )

    def _insert_batch_sqlserver(
        self,
        table_name: str,
        rows: List[List[Any]],
        column_names: List[str],
        database: Optional[str],
        schema: Optional[str]
    ) -> None:
        """Insert batch into SQL Server with transaction management."""
        schema_name = schema or "dbo"
        conn = None
        cursor = None
        
        try:
            conn = self.target.connect()
            # pyodbc connections have autocommit as a property
            # Set to False to use transactions (default is True)
            if hasattr(conn, 'autocommit'):
                original_autocommit = conn.autocommit
                conn.autocommit = False
            cursor = conn.cursor()

            if database:
                cursor.execute(f"USE [{database}]")

            # Build INSERT statement; target table has row_id IDENTITY + __op, __source_ts_ms, __deleted (CDC metadata)
            # So we insert only source columns + __op='r', __source_ts_ms, __deleted for full load
            ts_ms = int(time.time() * 1000)
            insert_columns = column_names + ["__op", "__source_ts_ms", "__deleted"]
            column_list = ", ".join([f"[{col}]" for col in insert_columns])
            placeholders = ", ".join(["?" for _ in insert_columns])
            insert_query = f"""
                INSERT INTO [{schema_name}].[{table_name}] ({column_list})
                VALUES ({placeholders})
            """

            # Append full-load metadata to each row: __op='r', __source_ts_ms, __deleted=NULL
            rows_with_metadata = [list(row) + ["r", ts_ms, None] for row in rows]

            # Execute batch insert within transaction
            try:
                cursor.executemany(insert_query, rows_with_metadata)
                conn.commit()
                logger.debug(f"Successfully inserted {len(rows)} rows into {schema_name}.{table_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to insert batch into SQL Server {schema_name}.{table_name}: {e}")
                raise

        except Exception as e:
            # Ensure rollback on any error
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"Transaction failed for SQL Server batch insert: {e}")
            raise
        finally:
            # Clean up resources
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _insert_batch_postgresql(
        self,
        table_name: str,
        rows: List[List[Any]],
        column_names: List[str],
        database: Optional[str],
        schema: Optional[str]
    ) -> None:
        """Insert batch into PostgreSQL with transaction management.
        Target table has row_id (serial) + __op, __source_ts_ms, __deleted for SCD2.
        Full load inserts source columns + __op='r', __source_ts_ms, __deleted=NULL.
        """
        from psycopg2 import sql
        from psycopg2.extras import execute_values

        schema_name = schema or "public"
        conn = None
        cursor = None
        
        try:
            conn = self.target.connect()
            conn.autocommit = False
            cursor = conn.cursor()

            # Insert columns: source columns + __op, __source_ts_ms, __deleted
            insert_columns = column_names + ["__op", "__source_ts_ms", "__deleted"]
            ts_ms = int(time.time() * 1000)
            rows_with_metadata = [list(row) + ["r", ts_ms, None] for row in rows]

            column_list = sql.SQL(", ").join([
                sql.Identifier(col) for col in insert_columns
            ])
            insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                column_list
            )

            try:
                execute_values(cursor, insert_query, rows_with_metadata)
                conn.commit()
                logger.debug(f"Successfully inserted {len(rows)} rows into {schema_name}.{table_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to insert batch into PostgreSQL {schema_name}.{table_name}: {e}")
                raise

        except Exception as e:
            # Ensure rollback on any error
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"Transaction failed for PostgreSQL batch insert: {e}")
            raise
        finally:
            # Clean up resources
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _insert_batch_oracle(
        self,
        table_name: str,
        rows: List[List[Any]],
        column_names: List[str],
        database: Optional[str],
        schema: Optional[str]
    ) -> None:
        """Insert batch into Oracle. Target table has row_id IDENTITY + __op, __source_ts_ms, __deleted. Full load appends __op='r', __source_ts_ms, __deleted=NULL."""
        schema_name = (schema or self.target.config.get("schema") or self.target.config.get("user") or "PUBLIC").upper()
        table_name_upper = table_name.upper()
        conn = None
        cursor = None

        try:
            conn = self.target.connect()
            cursor = conn.cursor()

            insert_columns = column_names + ["__op", "__source_ts_ms", "__deleted"]
            ts_ms = int(time.time() * 1000)
            rows_with_metadata = [list(row) + ["r", ts_ms, None] for row in rows]

            quoted_cols = ", ".join(f'"{c}"' for c in insert_columns)
            placeholders = ", ".join(f":{i+1}" for i in range(len(insert_columns)))
            insert_sql = f'INSERT INTO "{schema_name}"."{table_name_upper}" ({quoted_cols}) VALUES ({placeholders})'

            cursor.executemany(insert_sql, rows_with_metadata)
            conn.commit()
            logger.debug(f"Successfully inserted {len(rows)} rows into Oracle {schema_name}.{table_name_upper}")

        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"Failed to insert batch into Oracle {schema_name}.{table_name_upper}: {e}")
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

