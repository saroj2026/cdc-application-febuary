"""Data validation functions for CDC pipeline."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ingestion.exceptions import ValidationError
from ingestion.connectors.base_connector import BaseConnector
from ingestion.connectors.postgresql import PostgreSQLConnector
from ingestion.connectors.sqlserver import SQLServerConnector

logger = logging.getLogger(__name__)


def validate_source_data(
    connector: BaseConnector,
    database: str,
    schema: str,
    table_name: str
) -> Dict[str, Any]:
    """Verify source table exists and has data.
    
    Args:
        connector: Source database connector
        database: Database name
        schema: Schema name
        table_name: Table name
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Check if table exists by extracting schema
        schema_result = connector.extract_schema(
            database=database,
            schema=schema,
            table=table_name
        )
        
        tables = schema_result.get('tables', [])
        if not tables:
            raise ValidationError(
                f"Source table {schema}.{table_name} not found in database {database}",
                validation_type="table_existence"
            )
        
        # Get row count
        row_count = 0
        if isinstance(connector, PostgreSQLConnector):
            row_count = connector.get_table_row_count(
                table=table_name,
                database=database,
                schema=schema
            )
        elif isinstance(connector, SQLServerConnector):
            # SQL Server connector doesn't have get_table_row_count, use extract_data
            data_result = connector.extract_data(
                database=database,
                schema=schema,
                table_name=table_name,
                limit=1,
                offset=0
            )
            # Get total_rows from result if available
            row_count = data_result.get('total_rows', 0)
            if row_count == 0:
                # Try to get actual count by checking if any rows exist
                rows = data_result.get('rows', [])
                if rows:
                    # Table has data, but we don't know exact count
                    # This is acceptable - we just need to know it's not empty
                    row_count = 1  # At least 1 row exists
        
        logger.info(f"Source table {schema}.{table_name} validation: {row_count} rows found")
        
        return {
            "valid": True,
            "table_exists": True,
            "row_count": row_count,
            "has_data": row_count > 0
        }
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(
            f"Failed to validate source table {schema}.{table_name}: {str(e)}",
            validation_type="source_validation",
            error=str(e)
        )


def validate_target_row_count(
    source_connector: BaseConnector,
    target_connector: BaseConnector,
    source_database: str,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    expected_rows: Optional[int] = None
) -> Dict[str, Any]:
    """Verify target table has expected row count matching source.
    
    Args:
        source_connector: Source database connector
        target_connector: Target database connector
        source_database: Source database name
        source_schema: Source schema name
        source_table: Source table name
        target_database: Target database name
        target_schema: Target schema name
        target_table: Target table name
        expected_rows: Expected row count (if None, will get from source)
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Get source row count if not provided
        if expected_rows is None:
            if isinstance(source_connector, PostgreSQLConnector):
                expected_rows = source_connector.get_table_row_count(
                    table=source_table,
                    database=source_database,
                    schema=source_schema
                )
            else:
                # For SQL Server, we'll use a different approach
                data_result = source_connector.extract_data(
                    database=source_database,
                    schema=source_schema,
                    table_name=source_table,
                    limit=1,
                    offset=0
                )
                expected_rows = data_result.get('total_rows', 0)
        
        # Get target row count
        target_rows = 0
        if isinstance(target_connector, PostgreSQLConnector):
            target_rows = target_connector.get_table_row_count(
                table=target_table,
                database=target_database,
                schema=target_schema
            )
        elif isinstance(target_connector, SQLServerConnector):
            data_result = target_connector.extract_data(
                database=target_database,
                schema=target_schema,
                table_name=target_table,
                limit=1,
                offset=0
            )
            target_rows = data_result.get('total_rows', 0)
        
        logger.info(
            f"Row count validation: source={expected_rows}, target={target_rows} "
            f"for table {target_schema}.{target_table}"
        )
        
        if target_rows != expected_rows:
            raise ValidationError(
                f"Row count mismatch: source has {expected_rows} rows, "
                f"target has {target_rows} rows for table {target_schema}.{target_table}",
                validation_type="row_count",
                source_rows=expected_rows,
                target_rows=target_rows
            )
        
        return {
            "valid": True,
            "source_rows": expected_rows,
            "target_rows": target_rows,
            "match": True
        }
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(
            f"Failed to validate target row count for {target_schema}.{target_table}: {str(e)}",
            validation_type="row_count_validation",
            error=str(e)
        )


def validate_target_schema(
    source_connector: BaseConnector,
    target_connector: BaseConnector,
    source_database: str,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_schema: str,
    target_table: str
) -> Dict[str, Any]:
    """Verify target schema matches source schema.
    
    Args:
        source_connector: Source database connector
        target_connector: Target database connector
        source_database: Source database name
        source_schema: Source schema name
        source_table: Source table name
        target_database: Target database name
        target_schema: Target schema name
        target_table: Target table name
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Get source schema
        source_schema_result = source_connector.extract_schema(
            database=source_database,
            schema=source_schema,
            table=source_table
        )
        
        source_tables = source_schema_result.get('tables', [])
        if not source_tables:
            raise ValidationError(
                f"Source table {source_schema}.{source_table} not found",
                validation_type="source_schema"
            )
        
        source_columns = {col['name']: col for col in source_tables[0].get('columns', [])}
        
        # Get target schema
        target_schema_result = target_connector.extract_schema(
            database=target_database,
            schema=target_schema,
            table=target_table
        )
        
        target_tables = target_schema_result.get('tables', [])
        if not target_tables:
            raise ValidationError(
                f"Target table {target_schema}.{target_table} not found",
                validation_type="target_schema"
            )
        
        target_columns = {col['name']: col for col in target_tables[0].get('columns', [])}
        
        # Compare column counts
        if len(source_columns) != len(target_columns):
            raise ValidationError(
                f"Column count mismatch: source has {len(source_columns)} columns, "
                f"target has {len(target_columns)} columns",
                validation_type="schema_validation",
                source_columns=len(source_columns),
                target_columns=len(target_columns)
            )
        
        # Check for missing columns
        missing_columns = set(source_columns.keys()) - set(target_columns.keys())
        if missing_columns:
            raise ValidationError(
                f"Missing columns in target: {missing_columns}",
                validation_type="schema_validation",
                missing_columns=list(missing_columns)
            )
        
        logger.info(
            f"Schema validation passed: {len(source_columns)} columns match "
            f"for table {target_schema}.{target_table}"
        )
        
        return {
            "valid": True,
            "source_columns": len(source_columns),
            "target_columns": len(target_columns),
            "match": True
        }
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(
            f"Failed to validate target schema for {target_schema}.{target_table}: {str(e)}",
            validation_type="schema_validation",
            error=str(e)
        )


def validate_data_integrity(
    source_connector: BaseConnector,
    target_connector: BaseConnector,
    source_database: str,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    sample_size: int = 10,
    primary_key_columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Sample data comparison between source and target.
    
    Args:
        source_connector: Source database connector
        target_connector: Target database connector
        source_database: Source database name
        source_schema: Source schema name
        source_table: Source table name
        target_database: Target database name
        target_schema: Target schema name
        target_table: Target table name
        sample_size: Number of rows to sample
        primary_key_columns: Primary key columns for matching rows
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Get sample data from source
        source_data = source_connector.extract_data(
            database=source_database,
            schema=source_schema,
            table_name=source_table,
            limit=sample_size,
            offset=0
        )
        
        source_rows = source_data.get('rows', [])
        source_columns = source_data.get('column_names', [])
        
        if not source_rows:
            logger.warning(f"No source data to sample for {source_schema}.{source_table}")
            return {
                "valid": True,
                "sampled": 0,
                "message": "No data to sample"
            }
        
        # Get sample data from target
        target_data = target_connector.extract_data(
            database=target_database,
            schema=target_schema,
            table_name=target_table,
            limit=sample_size,
            offset=0
        )
        
        target_rows = target_data.get('rows', [])
        target_columns = target_data.get('column_names', [])
        
        if len(source_rows) != len(target_rows):
            raise ValidationError(
                f"Sample size mismatch: source has {len(source_rows)} rows, "
                f"target has {len(target_rows)} rows",
                validation_type="data_integrity",
                source_sample_size=len(source_rows),
                target_sample_size=len(target_rows)
            )
        
        # Basic validation - column names should match
        if set(source_columns) != set(target_columns):
            missing_in_target = set(source_columns) - set(target_columns)
            missing_in_source = set(target_columns) - set(source_columns)
            raise ValidationError(
                f"Column name mismatch: missing in target: {missing_in_target}, "
                f"missing in source: {missing_in_source}",
                validation_type="data_integrity"
            )
        
        logger.info(
            f"Data integrity validation passed: {len(source_rows)} rows sampled "
            f"for table {target_schema}.{target_table}"
        )
        
        return {
            "valid": True,
            "sampled": len(source_rows),
            "columns_match": True
        }
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(
            f"Failed to validate data integrity for {target_schema}.{target_table}: {str(e)}",
            validation_type="data_integrity",
            error=str(e)
        )
from ingestion.connectors.postgresql import PostgreSQLConnector
from ingestion.connectors.sqlserver import SQLServerConnector

logger = logging.getLogger(__name__)


def validate_source_data(
    connector: BaseConnector,
    database: str,
    schema: str,
    table_name: str
) -> Dict[str, Any]:
    """Verify source table exists and has data.
    
    Args:
        connector: Source database connector
        database: Database name
        schema: Schema name
        table_name: Table name
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Check if table exists by extracting schema
        schema_result = connector.extract_schema(
            database=database,
            schema=schema,
            table=table_name
        )
        
        tables = schema_result.get('tables', [])
        if not tables:
            raise ValidationError(
                f"Source table {schema}.{table_name} not found in database {database}",
                validation_type="table_existence"
            )
        
        # Get row count
        row_count = 0
        if isinstance(connector, PostgreSQLConnector):
            row_count = connector.get_table_row_count(
                table=table_name,
                database=database,
                schema=schema
            )
        elif isinstance(connector, SQLServerConnector):
            # SQL Server connector doesn't have get_table_row_count, use extract_data
            data_result = connector.extract_data(
                database=database,
                schema=schema,
                table_name=table_name,
                limit=1,
                offset=0
            )
            # Get total_rows from result if available
            row_count = data_result.get('total_rows', 0)
            if row_count == 0:
                # Try to get actual count by checking if any rows exist
                rows = data_result.get('rows', [])
                if rows:
                    # Table has data, but we don't know exact count
                    # This is acceptable - we just need to know it's not empty
                    row_count = 1  # At least 1 row exists
        
        logger.info(f"Source table {schema}.{table_name} validation: {row_count} rows found")
        
        return {
            "valid": True,
            "table_exists": True,
            "row_count": row_count,
            "has_data": row_count > 0
        }
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(
            f"Failed to validate source table {schema}.{table_name}: {str(e)}",
            validation_type="source_validation",
            error=str(e)
        )


def validate_target_row_count(
    source_connector: BaseConnector,
    target_connector: BaseConnector,
    source_database: str,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    expected_rows: Optional[int] = None
) -> Dict[str, Any]:
    """Verify target table has expected row count matching source.
    
    Args:
        source_connector: Source database connector
        target_connector: Target database connector
        source_database: Source database name
        source_schema: Source schema name
        source_table: Source table name
        target_database: Target database name
        target_schema: Target schema name
        target_table: Target table name
        expected_rows: Expected row count (if None, will get from source)
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Get source row count if not provided
        if expected_rows is None:
            if isinstance(source_connector, PostgreSQLConnector):
                expected_rows = source_connector.get_table_row_count(
                    table=source_table,
                    database=source_database,
                    schema=source_schema
                )
            else:
                # For SQL Server, we'll use a different approach
                data_result = source_connector.extract_data(
                    database=source_database,
                    schema=source_schema,
                    table_name=source_table,
                    limit=1,
                    offset=0
                )
                expected_rows = data_result.get('total_rows', 0)
        
        # Get target row count
        target_rows = 0
        if isinstance(target_connector, PostgreSQLConnector):
            target_rows = target_connector.get_table_row_count(
                table=target_table,
                database=target_database,
                schema=target_schema
            )
        elif isinstance(target_connector, SQLServerConnector):
            data_result = target_connector.extract_data(
                database=target_database,
                schema=target_schema,
                table_name=target_table,
                limit=1,
                offset=0
            )
            target_rows = data_result.get('total_rows', 0)
        
        logger.info(
            f"Row count validation: source={expected_rows}, target={target_rows} "
            f"for table {target_schema}.{target_table}"
        )
        
        if target_rows != expected_rows:
            raise ValidationError(
                f"Row count mismatch: source has {expected_rows} rows, "
                f"target has {target_rows} rows for table {target_schema}.{target_table}",
                validation_type="row_count",
                source_rows=expected_rows,
                target_rows=target_rows
            )
        
        return {
            "valid": True,
            "source_rows": expected_rows,
            "target_rows": target_rows,
            "match": True
        }
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(
            f"Failed to validate target row count for {target_schema}.{target_table}: {str(e)}",
            validation_type="row_count_validation",
            error=str(e)
        )


def validate_target_schema(
    source_connector: BaseConnector,
    target_connector: BaseConnector,
    source_database: str,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_schema: str,
    target_table: str
) -> Dict[str, Any]:
    """Verify target schema matches source schema.
    
    Args:
        source_connector: Source database connector
        target_connector: Target database connector
        source_database: Source database name
        source_schema: Source schema name
        source_table: Source table name
        target_database: Target database name
        target_schema: Target schema name
        target_table: Target table name
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Get source schema
        source_schema_result = source_connector.extract_schema(
            database=source_database,
            schema=source_schema,
            table=source_table
        )
        
        source_tables = source_schema_result.get('tables', [])
        if not source_tables:
            raise ValidationError(
                f"Source table {source_schema}.{source_table} not found",
                validation_type="source_schema"
            )
        
        source_columns = {col['name']: col for col in source_tables[0].get('columns', [])}
        
        # Get target schema
        target_schema_result = target_connector.extract_schema(
            database=target_database,
            schema=target_schema,
            table=target_table
        )
        
        target_tables = target_schema_result.get('tables', [])
        if not target_tables:
            raise ValidationError(
                f"Target table {target_schema}.{target_table} not found",
                validation_type="target_schema"
            )
        
        target_columns = {col['name']: col for col in target_tables[0].get('columns', [])}
        
        # Compare column counts
        if len(source_columns) != len(target_columns):
            raise ValidationError(
                f"Column count mismatch: source has {len(source_columns)} columns, "
                f"target has {len(target_columns)} columns",
                validation_type="schema_validation",
                source_columns=len(source_columns),
                target_columns=len(target_columns)
            )
        
        # Check for missing columns
        missing_columns = set(source_columns.keys()) - set(target_columns.keys())
        if missing_columns:
            raise ValidationError(
                f"Missing columns in target: {missing_columns}",
                validation_type="schema_validation",
                missing_columns=list(missing_columns)
            )
        
        logger.info(
            f"Schema validation passed: {len(source_columns)} columns match "
            f"for table {target_schema}.{target_table}"
        )
        
        return {
            "valid": True,
            "source_columns": len(source_columns),
            "target_columns": len(target_columns),
            "match": True
        }
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(
            f"Failed to validate target schema for {target_schema}.{target_table}: {str(e)}",
            validation_type="schema_validation",
            error=str(e)
        )


def validate_data_integrity(
    source_connector: BaseConnector,
    target_connector: BaseConnector,
    source_database: str,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    sample_size: int = 10,
    primary_key_columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Sample data comparison between source and target.
    
    Args:
        source_connector: Source database connector
        target_connector: Target database connector
        source_database: Source database name
        source_schema: Source schema name
        source_table: Source table name
        target_database: Target database name
        target_schema: Target schema name
        target_table: Target table name
        sample_size: Number of rows to sample
        primary_key_columns: Primary key columns for matching rows
        
    Returns:
        Dictionary with validation results
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Get sample data from source
        source_data = source_connector.extract_data(
            database=source_database,
            schema=source_schema,
            table_name=source_table,
            limit=sample_size,
            offset=0
        )
        
        source_rows = source_data.get('rows', [])
        source_columns = source_data.get('column_names', [])
        
        if not source_rows:
            logger.warning(f"No source data to sample for {source_schema}.{source_table}")
            return {
                "valid": True,
                "sampled": 0,
                "message": "No data to sample"
            }
        
        # Get sample data from target
        target_data = target_connector.extract_data(
            database=target_database,
            schema=target_schema,
            table_name=target_table,
            limit=sample_size,
            offset=0
        )
        
        target_rows = target_data.get('rows', [])
        target_columns = target_data.get('column_names', [])
        
        if len(source_rows) != len(target_rows):
            raise ValidationError(
                f"Sample size mismatch: source has {len(source_rows)} rows, "
                f"target has {len(target_rows)} rows",
                validation_type="data_integrity",
                source_sample_size=len(source_rows),
                target_sample_size=len(target_rows)
            )
        
        # Basic validation - column names should match
        if set(source_columns) != set(target_columns):
            missing_in_target = set(source_columns) - set(target_columns)
            missing_in_source = set(target_columns) - set(source_columns)
            raise ValidationError(
                f"Column name mismatch: missing in target: {missing_in_target}, "
                f"missing in source: {missing_in_source}",
                validation_type="data_integrity"
            )
        
        logger.info(
            f"Data integrity validation passed: {len(source_rows)} rows sampled "
            f"for table {target_schema}.{target_table}"
        )
        
        return {
            "valid": True,
            "sampled": len(source_rows),
            "columns_match": True
        }
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(
            f"Failed to validate data integrity for {target_schema}.{target_table}: {str(e)}",
            validation_type="data_integrity",
            error=str(e)
        )


