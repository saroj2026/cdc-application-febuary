"""Base connector class for database connectors."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """Abstract base class for all database connectors.
    
    This class defines the common interface that all database connectors
    must implement. It ensures consistency across different database systems
    while allowing database-specific implementations.
    """

    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize connector with connection configuration.
        
        Args:
            connection_config: Dictionary containing database connection parameters.
                Credentials and connection details are provided dynamically.
        """
        self.config = connection_config.copy()
        self._validate_config()

    @abstractmethod
    def _validate_config(self) -> None:
        """Validate that required connection parameters are present.
        
        Raises:
            ValueError: If required parameters are missing.
        """
        pass

    @abstractmethod
    def connect(self):
        """Establish connection to the database.
        
        Returns:
            Database connection object (type depends on database driver).
        """
        pass

    @abstractmethod
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
            Dictionary containing schema information:
            {
                "database": str,
                "schema": str,
                "tables": [
                    {
                        "name": str,
                        "fully_qualified_name": str,
                        "columns": [...],
                        "properties": {...}
                    },
                    ...
                ]
            }
        """
        pass

    @abstractmethod
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
            Dictionary containing:
            {
                "rows": List[List] (actual row data),
                "row_count": int (number of rows extracted),
                "total_rows": int (total rows in table),
                "has_more": bool (whether more rows exist),
                "column_names": List[str] (column names in order)
            }
        """
        pass

    @abstractmethod
    def extract_lsn_offset(
        self,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract LSN (Log Sequence Number) or offset metadata.
        
        This method extracts the current position in the transaction log
        or write-ahead log, which can be used for tracking replication
        or change data capture positions.
        
        Args:
            database: Database name (optional, uses config default if not provided)
            
        Returns:
            Dictionary containing LSN/offset information:
            {
                "lsn": str (LSN value in database-specific format),
                "offset": Optional[int] (offset value if applicable),
                "timestamp": str (ISO format timestamp),
                "database": str,
                "metadata": {...} (additional database-specific metadata)
            }
        """
        pass

    @abstractmethod
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
            Dictionary containing full load results:
            {
                "database": str,
                "schema": str,
                "lsn_offset": {...} (from extract_lsn_offset),
                "tables": [
                    {
                        "table_name": str,
                        "schema": {...} (if include_schema=True),
                        "data": {...} (if include_data=True),
                        "metadata": {...}
                    },
                    ...
                ],
                "metadata": {
                    "extraction_timestamp": str (ISO format),
                    "tables_processed": int,
                    "tables_successful": int,
                    "tables_failed": int
                }
            }
        """
        pass

    def test_connection(self) -> bool:
        """Test the database connection.
        
        This is a default implementation that can be overridden by subclasses
        for database-specific connection testing.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            conn = self.connect()
            if conn:
                # Try to close if it has a close method
                if hasattr(conn, 'close'):
                    conn.close()
                return True
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False




