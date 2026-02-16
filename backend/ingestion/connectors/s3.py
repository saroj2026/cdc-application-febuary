"""AWS S3 connector for metadata extraction and data operations."""

from __future__ import annotations

import logging
import json
import csv
import io
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    boto3 = None  # type: ignore

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


class S3Connector(BaseConnector):
    """Connector for extracting metadata and data from AWS S3."""

    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize S3 connector with connection configuration.

        Args:
            connection_config: Dictionary containing:
                - bucket: S3 bucket name (required)
                - aws_access_key_id: AWS access key ID (required)
                - aws_secret_access_key: AWS secret access key (required)
                - region_name: AWS region (optional, default: us-east-1)
                - endpoint_url: Custom S3 endpoint URL (optional, for S3-compatible services)
                - prefix: Object key prefix/folder (optional, default: empty)
                - schema: Schema/prefix name (optional, maps to S3 prefix)
        """
        if not S3_AVAILABLE:
            raise ImportError(
                "boto3 is not installed. "
                "Install it with: pip install boto3"
            )

        super().__init__(connection_config)
        self.s3_client = None
        self.s3_resource = None

    def _validate_config(self) -> None:
        """Validate that required connection parameters are present."""
        required = ["bucket", "aws_access_key_id", "aws_secret_access_key"]
        missing = [key for key in required if not self.config.get(key)]
        if missing:
            raise ValueError(f"Missing required connection parameters: {', '.join(missing)}")

    def _get_s3_client(self):
        """Get or create S3 client."""
        if self.s3_client is None:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config["aws_access_key_id"],
                aws_secret_access_key=self.config["aws_secret_access_key"],
                region_name=self.config.get("region_name", "us-east-1"),
                endpoint_url=self.config.get("endpoint_url")
            )
        return self.s3_client

    def _get_s3_resource(self):
        """Get or create S3 resource."""
        if self.s3_resource is None:
            self.s3_resource = boto3.resource(
                's3',
                aws_access_key_id=self.config["aws_access_key_id"],
                aws_secret_access_key=self.config["aws_secret_access_key"],
                region_name=self.config.get("region_name", "us-east-1"),
                endpoint_url=self.config.get("endpoint_url")
            )
        return self.s3_resource

    def connect(self):
        """Establish connection to S3.

        Returns:
            S3 client object (boto3.client)
        """
        try:
            client = self._get_s3_client()
            # Test connection by listing buckets or head bucket
            bucket = self.config["bucket"]
            client.head_bucket(Bucket=bucket)
            logger.info(f"Connected to S3 bucket: {bucket}")
            return client
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                raise ValueError(f"S3 bucket '{bucket}' not found")
            elif error_code == '403':
                raise ValueError(f"Access denied to S3 bucket '{bucket}'. Check credentials and permissions.")
            else:
                logger.error(f"Failed to connect to S3: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            raise

    def test_connection(self) -> bool:
        """Test the S3 connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            client = self.connect()
            bucket = self.config["bucket"]
            client.head_bucket(Bucket=bucket)
            return True
        except Exception as e:
            logger.error(f"S3 connection test failed: {e}")
            return False

    def get_version(self) -> str:
        """Get S3 service version information.
        
        Returns:
            String with S3 service information
        """
        try:
            client = self._get_s3_client()
            bucket = self.config["bucket"]
            region = self.config.get("region_name", "us-east-1")
            
            # Get bucket location to verify region
            try:
                location = client.get_bucket_location(Bucket=bucket)
                region = location.get('LocationConstraint') or region
            except Exception:
                pass
            
            # Try to get bucket versioning status
            versioning_status = "Unknown"
            try:
                versioning = client.get_bucket_versioning(Bucket=bucket)
                versioning_status = versioning.get('Status', 'NotEnabled')
            except Exception:
                pass
            
            version_info = f"AWS S3 - Bucket: {bucket}, Region: {region}, Versioning: {versioning_status}"
            return version_info
        except Exception as e:
            logger.warning(f"Could not get S3 version info: {e}")
            return f"AWS S3 - Bucket: {self.config.get('bucket', 'unknown')}"

    def disconnect(self, connection=None):
        """Close S3 connection (no-op for S3, but required by interface).
        
        Args:
            connection: Optional connection object (ignored for S3).
        """
        # S3 connections are stateless, nothing to close
        pass

    def extract_lsn_offset(
        self,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract LSN (Log Sequence Number) or offset metadata from S3.
        
        For S3, we use the latest object modification timestamp as the LSN equivalent.
        
        Args:
            database: Bucket name (optional, uses config default if not provided)
            
        Returns:
            Dictionary containing LSN/offset information:
            {
                "lsn": str (timestamp or version ID),
                "offset": Optional[int] (timestamp as offset),
                "timestamp": str (ISO format timestamp),
                "database": str (bucket name),
                "metadata": {...} (additional S3-specific metadata)
            }
        """
        bucket = database or self.config.get("bucket")
        prefix = self.config.get("prefix", "")
        
        try:
            client = self._get_s3_client()
            
            # Get the latest object modification time in the bucket/prefix
            latest_timestamp = None
            latest_key = None
            object_count = 0
            
            paginator = client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        object_count += 1
                        obj_timestamp = obj['LastModified']
                        if latest_timestamp is None or obj_timestamp > latest_timestamp:
                            latest_timestamp = obj_timestamp
                            latest_key = obj['Key']
            
            # If no objects found, use current timestamp
            if latest_timestamp is None:
                latest_timestamp = datetime.utcnow()
                lsn = f"TIMESTAMP:{latest_timestamp.isoformat()}"
            else:
                lsn = f"TIMESTAMP:{latest_timestamp.isoformat()}"
            
            # Convert timestamp to offset (Unix timestamp)
            offset = int(latest_timestamp.timestamp()) if latest_timestamp else None
            
            metadata = {
                "latest_object_key": latest_key,
                "object_count": object_count,
                "bucket": bucket,
                "prefix": prefix,
                "lsn_source": "object_last_modified"
            }
            
            # Get bucket versioning status if available
            try:
                versioning = client.get_bucket_versioning(Bucket=bucket)
                metadata["versioning_status"] = versioning.get('Status', 'NotEnabled')
            except Exception:
                metadata["versioning_status"] = "Unknown"
            
            lsn_result = {
                "lsn": lsn,
                "offset": offset,
                "timestamp": datetime.utcnow().isoformat(),
                "database": bucket,
                "metadata": metadata
            }
            
            logger.info(f"Extracted LSN/offset for S3 bucket {bucket}: LSN={lsn}")
            return lsn_result
            
        except Exception as e:
            logger.error(f"Error extracting LSN/offset from S3: {e}")
            # Return a minimal result with timestamp
            return {
                "lsn": None,
                "offset": None,
                "timestamp": datetime.utcnow().isoformat(),
                "database": bucket,
                "metadata": {"error": str(e)}
            }

    def extract_schema(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract object metadata (schema information) from S3.
        
        For S3, objects are treated as tables. The schema is inferred from:
        - Object metadata (ContentType, Metadata, etc.)
        - File structure (if JSON, CSV, Parquet, etc.)
        
        Args:
            database: Bucket name (optional, uses config default if not provided)
            schema: Prefix/folder path (optional, uses config default if not provided)
            table: Object key/name (optional, if None extracts all objects in prefix)
            
        Returns:
            Dictionary containing schema information:
            {
                "database": str (bucket name),
                "schema": str (prefix),
                "tables": [
                    {
                        "name": str (object key),
                        "fully_qualified_name": str,
                        "columns": [...],
                        "properties": {...}
                    },
                    ...
                ]
            }
        """
        bucket = database or self.config.get("bucket")
        prefix = schema or self.config.get("prefix", "")
        
        if table:
            # Extract single object
            objects_list = self._list_objects(bucket, prefix, table_filter=table)
        else:
            # Extract all objects
            objects_list = self._list_objects(bucket, prefix)
        
        # Convert to table format
        tables_dict = []
        for obj_info in objects_list:
            obj_key = obj_info["key"]
            obj_name = obj_key.split("/")[-1]  # Get filename without path
            
            # Infer schema from object
            columns = self._infer_object_schema(bucket, obj_key)
            
            table_dict = {
                "name": obj_name,
                "fully_qualified_name": f"{bucket}/{obj_key}",
                "columns": columns,
                "properties": {
                    "content_type": obj_info.get("content_type", "unknown"),
                    "size": obj_info.get("size", 0),
                    "last_modified": obj_info.get("last_modified", "").isoformat() if obj_info.get("last_modified") else None,
                    "etag": obj_info.get("etag", ""),
                    "storage_class": obj_info.get("storage_class", "STANDARD")
                },
                "table_type": "object",
                "description": f"S3 object: {obj_key}"
            }
            tables_dict.append(table_dict)
        
        return {
            "database": bucket,
            "schema": prefix or "",
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
        """Extract actual data from an S3 object.
        
        Args:
            database: Bucket name
            schema: Prefix/folder path
            table_name: Object key/name
            limit: Maximum number of rows to extract (None for all rows)
            offset: Number of rows to skip (for pagination)
            
        Returns:
            Dictionary containing:
            {
                "rows": List[List] (actual row data),
                "row_count": int (number of rows extracted),
                "total_rows": int (total rows in object),
                "has_more": bool (whether more rows exist),
                "column_names": List[str] (column names in order)
            }
        """
        bucket = database
        # Construct full object key
        if schema:
            object_key = f"{schema}/{table_name}" if not schema.endswith("/") else f"{schema}{table_name}"
        else:
            object_key = table_name
        
        try:
            client = self._get_s3_client()
            
            # Get object
            response = client.get_object(Bucket=bucket, Key=object_key)
            content_type = response.get('ContentType', '')
            body = response['Body'].read()
            
            # Parse based on content type
            rows = []
            column_names = []
            
            if 'json' in content_type.lower() or object_key.endswith('.json'):
                # Parse JSON
                data = json.loads(body.decode('utf-8'))
                if isinstance(data, list):
                    rows = data
                    if rows and isinstance(rows[0], dict):
                        column_names = list(rows[0].keys())
                elif isinstance(data, dict):
                    # Single object or nested structure
                    rows = [data]
                    column_names = list(data.keys())
            elif 'csv' in content_type.lower() or object_key.endswith('.csv'):
                # Parse CSV
                csv_content = body.decode('utf-8')
                csv_reader = csv.DictReader(io.StringIO(csv_content))
                column_names = csv_reader.fieldnames or []
                rows = [list(row.values()) for row in csv_reader]
            else:
                # Treat as text, one line per row
                text_content = body.decode('utf-8')
                lines = text_content.split('\n')
                rows = [[line] for line in lines if line.strip()]
                column_names = ['content']
            
            # Apply pagination
            total_rows = len(rows)
            if offset > 0:
                rows = rows[offset:]
            if limit:
                rows = rows[:limit]
            
            row_count = len(rows)
            has_more = (offset + row_count) < total_rows
            
            return {
                "rows": rows,
                "row_count": row_count,
                "total_rows": total_rows,
                "has_more": has_more,
                "column_names": column_names
            }
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchKey':
                raise ValueError(f"S3 object '{object_key}' not found in bucket '{bucket}'")
            else:
                logger.error(f"Error extracting data from S3 object {object_key}: {e}")
                raise
        except Exception as e:
            logger.error(f"Error extracting data from S3: {e}")
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
        """Perform full load for specified S3 objects.
        
        Extracts schema and/or data for the specified objects, including
        LSN/offset metadata for tracking purposes.
        
        Args:
            tables: List of object keys/names to extract
            database: Bucket name (optional, uses config default if not provided)
            schema: Prefix/folder path (optional, uses config default if not provided)
            include_schema: Whether to extract schema information
            include_data: Whether to extract data rows
            data_limit: Maximum rows per object (None for all rows)
            
        Returns:
            Dictionary containing full load results
        """
        bucket = database or self.config.get("bucket")
        prefix = schema or self.config.get("prefix", "")
        
        # Extract LSN/offset metadata
        lsn_offset = self.extract_lsn_offset(database=bucket)
        
        result = {
            "database": bucket,
            "schema": prefix or "",
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
                            database=bucket,
                            schema=prefix,
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
                            database=bucket,
                            schema=prefix,
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
                logger.error(f"Error processing object {table_name}: {e}")
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
            f"Full load completed for {result['metadata']['tables_successful']} objects, "
            f"{result['metadata']['tables_failed']} failed"
        )
        return result

    def _list_objects(
        self,
        bucket: str,
        prefix: str = "",
        table_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List objects in S3 bucket with optional prefix filter.
        
        Args:
            bucket: Bucket name
            prefix: Object key prefix
            table_filter: Specific object key to filter (optional)
            
        Returns:
            List of object information dictionaries
        """
        client = self._get_s3_client()
        objects = []
        
        try:
            paginator = client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        obj_key = obj['Key']
                        
                        # Apply table filter if specified
                        if table_filter:
                            if obj_key != prefix + table_filter and not obj_key.endswith(table_filter):
                                continue
                        
                        # Skip directory markers
                        if obj_key.endswith('/'):
                            continue
                        
                        # Get object metadata
                        try:
                            head_response = client.head_object(Bucket=bucket, Key=obj_key)
                            objects.append({
                                "key": obj_key,
                                "size": obj['Size'],
                                "last_modified": obj['LastModified'],
                                "etag": obj['ETag'].strip('"'),
                                "content_type": head_response.get('ContentType', 'application/octet-stream'),
                                "storage_class": obj.get('StorageClass', 'STANDARD'),
                                "metadata": head_response.get('Metadata', {})
                            })
                        except Exception as e:
                            logger.warning(f"Could not get metadata for {obj_key}: {e}")
                            objects.append({
                                "key": obj_key,
                                "size": obj['Size'],
                                "last_modified": obj['LastModified'],
                                "etag": obj['ETag'].strip('"'),
                                "content_type": "application/octet-stream",
                                "storage_class": obj.get('StorageClass', 'STANDARD'),
                                "metadata": {}
                            })
            
            logger.info(f"Listed {len(objects)} objects from S3 bucket {bucket} with prefix {prefix}")
            return objects
            
        except Exception as e:
            logger.error(f"Error listing objects from S3: {e}")
            raise

    def _infer_object_schema(
        self,
        bucket: str,
        object_key: str
    ) -> List[Dict[str, Any]]:
        """Infer schema from S3 object structure.
        
        Args:
            bucket: Bucket name
            object_key: Object key
            
        Returns:
            List of column dictionaries
        """
        columns = []
        
        try:
            client = self._get_s3_client()
            
            # Get object to inspect
            response = client.get_object(Bucket=bucket, Key=object_key)
            content_type = response.get('ContentType', '')
            
            # Try to infer from file extension or content type
            if object_key.endswith('.json') or 'json' in content_type.lower():
                # Try to read first few bytes to infer JSON structure
                body = response['Body'].read(1024)  # Read first 1KB
                try:
                    data = json.loads(body.decode('utf-8'))
                    if isinstance(data, dict):
                        for key, value in data.items():
                            columns.append({
                                "name": key,
                                "data_type": self._infer_type(value),
                                "ordinal_position": len(columns) + 1,
                                "is_nullable": True,
                                "default_value": None,
                                "description": None,
                                "json_schema": {}
                            })
                    elif isinstance(data, list) and data and isinstance(data[0], dict):
                        for key, value in data[0].items():
                            columns.append({
                                "name": key,
                                "data_type": self._infer_type(value),
                                "ordinal_position": len(columns) + 1,
                                "is_nullable": True,
                                "default_value": None,
                                "description": None,
                                "json_schema": {}
                            })
                except Exception:
                    # If can't parse, use generic schema
                    columns.append({
                        "name": "content",
                        "data_type": "json",
                        "ordinal_position": 1,
                        "is_nullable": True,
                        "default_value": None,
                        "description": None,
                        "json_schema": {}
                    })
            elif object_key.endswith('.csv') or 'csv' in content_type.lower():
                # Try to read CSV header
                body = response['Body'].read(2048)  # Read first 2KB
                csv_content = body.decode('utf-8')
                csv_reader = csv.DictReader(io.StringIO(csv_content))
                fieldnames = csv_reader.fieldnames or []
                for i, fieldname in enumerate(fieldnames):
                    columns.append({
                        "name": fieldname,
                        "data_type": "string",  # CSV columns are typically strings
                        "ordinal_position": i + 1,
                        "is_nullable": True,
                        "default_value": None,
                        "description": None,
                        "json_schema": {}
                    })
            else:
                # Generic text/binary file
                columns.append({
                    "name": "content",
                    "data_type": "binary",
                    "ordinal_position": 1,
                    "is_nullable": True,
                    "default_value": None,
                    "description": None,
                    "json_schema": {}
                })
            
            # If no columns inferred, add a default
            if not columns:
                columns.append({
                    "name": "content",
                    "data_type": "string",
                    "ordinal_position": 1,
                    "is_nullable": True,
                    "default_value": None,
                    "description": None,
                    "json_schema": {}
                })
            
            return columns
            
        except Exception as e:
            logger.warning(f"Could not infer schema for {object_key}: {e}")
            # Return default column
            return [{
                "name": "content",
                "data_type": "string",
                "ordinal_position": 1,
                "is_nullable": True,
                "default_value": None,
                "description": None,
                "json_schema": {}
            }]

    def _infer_type(self, value: Any) -> str:
        """Infer data type from Python value.
        
        Args:
            value: Python value
            
        Returns:
            Data type string
        """
        if value is None:
            return "string"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, dict):
            return "json"
        elif isinstance(value, list):
            return "array"
        else:
            return "string"

