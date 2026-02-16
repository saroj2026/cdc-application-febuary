"""Data quality monitoring for CDC pipelines."""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from ingestion.connection_service import ConnectionService

logger = logging.getLogger(__name__)


class DataQualityMonitor:
    """Monitor data quality and consistency."""
    
    def __init__(self, connection_service: ConnectionService):
        """Initialize data quality monitor.
        
        Args:
            connection_service: Connection service instance
        """
        self.connection_service = connection_service
    
    def validate_row_counts(
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
        """Validate row counts between source and target.
        
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
            Validation result
        """
        try:
            # Get source row count
            source_schema = self.connection_service.get_table_schema(
                source_connection_id,
                table_name,
                database=source_database,
                schema=source_schema
            )
            
            source_tables = self.connection_service.discover_tables(
                source_connection_id,
                database=source_database,
                schema=source_schema
            )
            
            source_row_count = None
            if source_tables.get("success"):
                for table in source_tables.get("tables", []):
                    if table["name"] == table_name:
                        source_row_count = table.get("row_count")
                        break
            
            # Get target row count
            target_table = target_table_name or table_name
            target_schema_info = self.connection_service.get_table_schema(
                target_connection_id,
                target_table,
                database=target_database,
                schema=target_schema
            )
            
            target_tables = self.connection_service.discover_tables(
                target_connection_id,
                database=target_database,
                schema=target_schema
            )
            
            target_row_count = None
            if target_tables.get("success"):
                for table in target_tables.get("tables", []):
                    if table["name"] == target_table:
                        target_row_count = table.get("row_count")
                        break
            
            # Compare
            match = source_row_count == target_row_count if (source_row_count is not None and target_row_count is not None) else None
            difference = None
            if source_row_count is not None and target_row_count is not None:
                difference = source_row_count - target_row_count
            
            return {
                "table_name": table_name,
                "target_table_name": target_table,
                "source_row_count": source_row_count,
                "target_row_count": target_row_count,
                "match": match,
                "difference": difference,
                "status": "valid" if match else ("warning" if difference else "unknown"),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error validating row counts: {e}", exc_info=True)
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def detect_schema_drift(
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
        """Detect schema drift between source and target.
        
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
            Schema drift detection result
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
                    "error": f"Failed to get source schema: {source_schema_info.get('error')}",
                    "timestamp": datetime.utcnow().isoformat()
                }
            
            # Get target schema
            target_table = target_table_name or table_name
            target_schema_info = self.connection_service.get_table_schema(
                target_connection_id,
                target_table,
                database=target_database,
                schema=target_schema
            )
            
            if not target_schema_info.get("success"):
                return {
                    "drift_detected": True,
                    "message": "Target table does not exist",
                    "timestamp": datetime.utcnow().isoformat()
                }
            
            source_columns = {col["name"]: col for col in source_schema_info.get("columns", [])}
            target_columns = {col["name"]: col for col in target_schema_info.get("columns", [])}
            
            # Detect differences
            added_columns = [col for col in source_columns if col not in target_columns]
            removed_columns = [col for col in target_columns if col not in source_columns]
            modified_columns = []
            
            for col_name in source_columns:
                if col_name in target_columns:
                    source_col = source_columns[col_name]
                    target_col = target_columns[col_name]
                    if (source_col.get("data_type") != target_col.get("data_type") or
                        source_col.get("is_nullable") != target_col.get("is_nullable")):
                        modified_columns.append({
                            "column": col_name,
                            "source": source_col,
                            "target": target_col
                        })
            
            drift_detected = len(added_columns) > 0 or len(removed_columns) > 0 or len(modified_columns) > 0
            
            return {
                "table_name": table_name,
                "target_table_name": target_table,
                "drift_detected": drift_detected,
                "added_columns": added_columns,
                "removed_columns": removed_columns,
                "modified_columns": modified_columns,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error detecting schema drift: {e}", exc_info=True)
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }


