"""Table discovery service for filtering, mapping, and validating table selections."""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from ingestion.connection_service import ConnectionService
from ingestion.database.models_db import ConnectionModel

logger = logging.getLogger(__name__)


class DiscoveryService:
    """Service for discovering, filtering, and mapping tables."""
    
    def __init__(self):
        """Initialize discovery service."""
        self.connection_service = ConnectionService()
    
    def discover_all(
        self,
        connection_id: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Complete discovery workflow (databases, schemas, tables).
        
        Args:
            connection_id: Connection ID
            database: Database name (optional)
            schema: Schema name (optional)
            
        Returns:
            Dictionary with complete discovery results
        """
        result = {
            "connection_id": connection_id,
            "databases": [],
            "schemas": [],
            "tables": []
        }
        
        # Discover databases
        db_result = self.connection_service.discover_databases(connection_id)
        if db_result.get("success"):
            result["databases"] = db_result.get("databases", [])
        
        # Discover schemas if database specified
        if database:
            schema_result = self.connection_service.discover_schemas(connection_id, database=database)
            if schema_result.get("success"):
                result["schemas"] = schema_result.get("schemas", [])
            
            # Discover tables if schema specified
            if schema:
                table_result = self.connection_service.discover_tables(
                    connection_id,
                    database=database,
                    schema=schema
                )
                if table_result.get("success"):
                    result["tables"] = table_result.get("tables", [])
        
        return result
    
    def filter_tables(
        self,
        tables: List[Dict[str, Any]],
        pattern: str
    ) -> List[Dict[str, Any]]:
        """Filter tables by pattern (supports wildcards).
        
        Args:
            tables: List of table dictionaries with 'name' field
            pattern: Filter pattern (supports * and ? wildcards)
            
        Returns:
            Filtered list of tables
        """
        if not pattern:
            return tables
        
        # Convert wildcard pattern to regex
        # * matches any sequence, ? matches single character
        regex_pattern = pattern.replace("*", ".*").replace("?", ".")
        regex = re.compile(regex_pattern, re.IGNORECASE)
        
        filtered = []
        for table in tables:
            table_name = table.get("name", "")
            if regex.match(table_name):
                filtered.append(table)
        
        logger.info(f"Filtered {len(tables)} tables to {len(filtered)} using pattern '{pattern}'")
        return filtered
    
    def map_tables(
        self,
        source_tables: List[str],
        target_tables: Optional[List[str]] = None,
        custom_mapping: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """Generate source->target table mappings.
        
        Args:
            source_tables: List of source table names
            target_tables: List of target table names (optional, 1:1 mapping if not provided)
            custom_mapping: Custom mapping dictionary (source -> target)
            
        Returns:
            Dictionary mapping source table names to target table names
        """
        mapping = {}
        
        # Use custom mapping if provided
        if custom_mapping:
            for source_table in source_tables:
                target_table = custom_mapping.get(source_table, source_table)
                mapping[source_table] = target_table
        elif target_tables:
            # Use provided target tables (1:1 mapping by index)
            for i, source_table in enumerate(source_tables):
                if i < len(target_tables):
                    mapping[source_table] = target_tables[i]
                else:
                    mapping[source_table] = source_table  # Default to same name
        else:
            # Default: 1:1 mapping (source name = target name)
            for source_table in source_tables:
                mapping[source_table] = source_table
        
        logger.info(f"Generated table mapping for {len(mapping)} tables")
        return mapping
    
    def validate_table_selection(
        self,
        connection_id: str,
        tables: List[str],
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Validate that selected tables exist and have required properties.
        
        Args:
            connection_id: Connection ID
            tables: List of table names to validate
            database: Database name (optional)
            schema: Schema name (optional)
            
        Returns:
            Validation result with details
        """
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "table_details": []
        }
        
        # Discover tables in the connection
        table_result = self.connection_service.discover_tables(
            connection_id,
            database=database,
            schema=schema
        )
        
        if not table_result.get("success"):
            validation_result["valid"] = False
            validation_result["errors"].append(f"Failed to discover tables: {table_result.get('error')}")
            return validation_result
        
        available_tables = {table["name"]: table for table in table_result.get("tables", [])}
        
        # Validate each selected table
        for table_name in tables:
            table_info = {
                "name": table_name,
                "exists": False,
                "has_primary_key": False,
                "row_count": None,
                "errors": [],
                "warnings": []
            }
            
            if table_name not in available_tables:
                validation_result["valid"] = False
                table_info["errors"].append(f"Table '{table_name}' does not exist")
                validation_result["errors"].append(f"Table '{table_name}' not found in {schema or 'default schema'}")
            else:
                table_info["exists"] = True
                available_table = available_tables[table_name]
                table_info["row_count"] = available_table.get("row_count")
                
                # Check for primary key
                try:
                    schema_info = self.connection_service.get_table_schema(
                        connection_id,
                        table_name,
                        database=database,
                        schema=schema
                    )
                    if schema_info.get("success"):
                        primary_keys = schema_info.get("primary_keys", [])
                        if primary_keys:
                            table_info["has_primary_key"] = True
                        else:
                            table_info["warnings"].append("Table has no primary key - may cause issues with CDC")
                            validation_result["warnings"].append(f"Table '{table_name}' has no primary key")
                except Exception as e:
                    logger.warning(f"Could not check primary key for {table_name}: {e}")
                    table_info["warnings"].append(f"Could not verify primary key: {str(e)}")
            
            validation_result["table_details"].append(table_info)
        
        return validation_result
    
    def get_table_dependencies(
        self,
        connection_id: str,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Detect foreign key relationships for a table.
        
        Args:
            connection_id: Connection ID
            table_name: Table name
            database: Database name (optional)
            schema: Schema name (optional)
            
        Returns:
            Dictionary with dependency information
        """
        try:
            connection = self.connection_service.session.query(ConnectionModel).filter_by(
                id=connection_id
            ).first()
            
            if not connection:
                return {
                    "success": False,
                    "error": f"Connection not found: {connection_id}"
                }
            
            connector = self.connection_service._get_connector(connection)
            
            # Get foreign keys (implementation depends on database type)
            dependencies = []
            conn = None
            
            if connection.database_type == "postgresql":
                # PostgreSQL foreign key query
                conn = connector.connect()
                from psycopg2.extras import RealDictCursor
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                try:
                    query = """
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
                    cursor.execute(query, (schema or "public", table_name))
                    for row in cursor.fetchall():
                        dependencies.append({
                            "constraint_name": row[0],
                            "column": row[1],
                            "referenced_schema": row[2],
                            "referenced_table": row[3],
                            "referenced_column": row[4]
                        })
                finally:
                    cursor.close()
                    conn.close()
            
            elif connection.database_type == "sqlserver":
                # SQL Server foreign key query
                conn = connector.connect()
                cursor = conn.cursor()
                try:
                    cursor.execute(f"USE [{database or connection.database}]")
                    query = """
                        SELECT
                            fk.name AS constraint_name,
                            cp.name AS column_name,
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
                        WHERE s.name = ? AND t.name = ?
                    """
                    cursor.execute(query, schema or "dbo", table_name)
                    for row in cursor.fetchall():
                        dependencies.append({
                            "constraint_name": row[0],
                            "column": row[1],
                            "referenced_schema": row[2],
                            "referenced_table": row[3],
                            "referenced_column": row[4]
                        })
                finally:
                    cursor.close()
                    conn.close()
            
            # Disconnect if method exists and connection was created
            # Note: Connection may already be closed above, but disconnect() handles that
            if hasattr(connector, 'disconnect') and conn:
                try:
                    connector.disconnect(conn)
                except:
                    pass  # Ignore if connection already closed
            
            return {
                "success": True,
                "table": table_name,
                "dependencies": dependencies,
                "dependency_count": len(dependencies)
            }
            
        except Exception as e:
            logger.error(f"Error getting table dependencies: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def estimate_data_size(
        self,
        connection_id: str,
        tables: List[str],
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Estimate data size for tables.
        
        Args:
            connection_id: Connection ID
            tables: List of table names
            database: Database name (optional)
            schema: Schema name (optional)
            
        Returns:
            Dictionary with size estimates
        """
        estimates = {
            "tables": [],
            "total_rows": 0,
            "total_size_mb": 0.0
        }
        
        for table_name in tables:
            try:
                table_info = self.connection_service.get_table_schema(
                    connection_id,
                    table_name,
                    database=database,
                    schema=schema
                )
                
                if table_info.get("success"):
                    # Get row count
                    table_result = self.connection_service.discover_tables(
                        connection_id,
                        database=database,
                        schema=schema
                    )
                    
                    table_details = None
                    if table_result.get("success"):
                        for t in table_result.get("tables", []):
                            if t["name"] == table_name:
                                table_details = t
                                break
                    
                    row_count = table_details.get("row_count") if table_details else None
                    
                    # Estimate size (rough calculation)
                    column_count = len(table_info.get("columns", []))
                    estimated_size_mb = 0.0
                    if row_count:
                        # Rough estimate: assume average row size based on column count
                        avg_row_size_bytes = column_count * 100  # Conservative estimate
                        estimated_size_mb = (row_count * avg_row_size_bytes) / (1024 * 1024)
                    
                    estimates["tables"].append({
                        "name": table_name,
                        "row_count": row_count,
                        "column_count": column_count,
                        "estimated_size_mb": round(estimated_size_mb, 2)
                    })
                    
                    if row_count:
                        estimates["total_rows"] += row_count
                    estimates["total_size_mb"] += estimated_size_mb
                    
            except Exception as e:
                logger.warning(f"Could not estimate size for {table_name}: {e}")
                estimates["tables"].append({
                    "name": table_name,
                    "row_count": None,
                    "column_count": None,
                    "estimated_size_mb": None,
                    "error": str(e)
                })
        
        estimates["total_size_mb"] = round(estimates["total_size_mb"], 2)
        return estimates

