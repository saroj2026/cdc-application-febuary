"""Kafka Sink connector configuration generator."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ingestion.models import Connection

logger = logging.getLogger(__name__)


class SinkConfigGenerator:
    """Generate Kafka Sink connector configurations."""
    
    @staticmethod
    def generate_sink_config(
        connector_name: str,
        target_connection: Connection,
        target_database: str,
        target_schema: str,
        kafka_topics: List[str],
        table_mapping: Optional[Dict[str, str]] = None,
        batch_size: int = 3000,
        insert_mode: str = "insert",
        pk_mode: str = "record_key"
    ) -> Dict[str, Any]:
        """Generate Kafka Sink connector configuration.
        
        Args:
            connector_name: Name of the sink connector
            target_connection: Target database connection
            target_database: Target database name
            target_schema: Target schema name
            kafka_topics: List of Kafka topics to consume
            table_mapping: Optional mapping from topic to table name
            batch_size: Batch size for inserts
            insert_mode: Insert mode (insert, upsert, update)
            pk_mode: Primary key mode (record_key, record_value, etc.)
            
        Returns:
            Sink connector configuration dictionary
        """
        database_type = target_connection.database_type.lower()
        
        if database_type == "postgresql":
            return SinkConfigGenerator._generate_postgresql_sink_config(
                connector_name=connector_name,
                connection=target_connection,
                database=target_database,
                schema=target_schema,
                topics=kafka_topics,
                table_mapping=table_mapping,
                batch_size=batch_size,
                insert_mode=insert_mode,
                pk_mode=pk_mode
            )
        elif database_type in ["sqlserver", "mssql"]:
            return SinkConfigGenerator._generate_sqlserver_sink_config(
                connector_name=connector_name,
                connection=target_connection,
                database=target_database,
                schema=target_schema,
                topics=kafka_topics,
                table_mapping=table_mapping,
                batch_size=batch_size,
                insert_mode=insert_mode,
                pk_mode=pk_mode
            )
        elif database_type == "s3":
            # Ensure batch_size has a valid default for S3
            # Use smaller default (10) for immediate data visibility
            # Can be increased for production to optimize performance
            s3_batch_size = batch_size if batch_size and batch_size > 0 else 10
            return SinkConfigGenerator._generate_s3_sink_config(
                connector_name=connector_name,
                connection=target_connection,
                bucket=target_database,
                prefix=target_schema or "",
                topics=kafka_topics,
                table_mapping=table_mapping,
                batch_size=s3_batch_size
            )
        elif database_type == "snowflake":
            return SinkConfigGenerator._generate_snowflake_sink_config(
                connector_name=connector_name,
                connection=target_connection,
                database=target_database,
                schema=target_schema,
                topics=kafka_topics,
                table_mapping=table_mapping,
                batch_size=batch_size
            )
        elif database_type == "oracle":
            return SinkConfigGenerator._generate_oracle_sink_config(
                connector_name=connector_name,
                connection=target_connection,
                database=target_database,
                schema=target_schema,
                topics=kafka_topics,
                table_mapping=table_mapping,
                batch_size=batch_size,
                insert_mode=insert_mode,
                pk_mode=pk_mode
            )
        else:
            raise ValueError(f"Unsupported database type for Sink: {database_type}")
    
    @staticmethod
    def _generate_postgresql_sink_config(
        connector_name: str,
        connection: Connection,
        database: str,
        schema: str,
        topics: List[str],
        table_mapping: Optional[Dict[str, str]],
        batch_size: int,
        insert_mode: str,
        pk_mode: str
    ) -> Dict[str, Any]:
        """Generate PostgreSQL JDBC Sink connector configuration.
        
        Args:
            connector_name: Connector name
            connection: PostgreSQL connection
            database: Database name
            schema: Schema name
            topics: Kafka topics
            table_mapping: Topic to table mapping
            batch_size: Batch size
            insert_mode: Insert mode
            pk_mode: Primary key mode
            
        Returns:
            PostgreSQL Sink connector configuration
        """
        # Build connection URL
        connection_url = (
            f"jdbc:postgresql://{connection.host}:{connection.port}/{database}"
        )
        
        # Build topics list
        topics_list = ",".join(topics)
        
        # Extract table name from topic (format: {server}.{schema}.{table})
        # For each topic, extract the last part (table name)
        # We'll use a regex pattern in table.name.format to extract just the table name
        # Format: {schema}.${topic} where we extract table name from topic
        # Since JDBC Sink doesn't support regex directly, we'll use a Single Message Transform
        
        # SCD2-style (same as SQL Server target): insert every change as new row with __op, __source_ts_ms, __deleted
        config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": topics_list,
            "connection.url": connection_url,
            "connection.user": connection.username,
            "connection.password": connection.password,
            "insert.mode": "insert",
            "pk.mode": "none",  # Allow multiple rows per key for SCD2 history
            "batch.size": str(batch_size),
            "auto.create": "true",
            "auto.evolve": "false",  # Target tables created with row_id + CDC metadata
            "delete.enabled": "false",
            # Debezium ExtractNewRecordState: unwrap envelope, add __op, __source_ts_ms, keep deletes as __deleted=true
            # Note: Debezium SQL Server sends datetime2 as epoch nanoseconds (int64). Store as bigint in Postgres
            # and use a generated column (registration_ts) for timestamp display - see fix_customer2_registration_date_postgres.sql
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "true",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            # Omit delete.tombstone.handling.mode for Kafka Connect 2.6 (Debezium 3.4+ only)
            "transforms.unwrap.add.fields": "op,source.ts_ms",
            "consumer.override.auto.offset.reset": "earliest",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
        }
        # Table name from first topic
        if topics:
            first_topic = topics[0]
            topic_parts = first_topic.split(".")
            if len(topic_parts) >= 3:
                table_name = topic_parts[-1]
                config["table.name.format"] = f"{schema}.{table_name}"
        
        # If table mapping is provided, use it
        if table_mapping:
            # Build table name format based on mapping
            logger.info(f"Table mapping provided: {table_mapping}")
            # Override with mapped table name if available
            if topics and topics[0] in table_mapping:
                mapped_table = table_mapping[topics[0]]
                config["table.name.format"] = f"{schema}.{mapped_table}"
        
        return config
    
    @staticmethod
    def _generate_sqlserver_sink_config(
        connector_name: str,
        connection: Connection,
        database: str,
        schema: str,
        topics: List[str],
        table_mapping: Optional[Dict[str, str]],
        batch_size: int,
        insert_mode: str,
        pk_mode: str
    ) -> Dict[str, Any]:
        """Generate SQL Server JDBC Sink connector configuration.
        
        Args:
            connector_name: Connector name
            connection: SQL Server connection
            database: Database name
            schema: Schema name
            topics: Kafka topics
            table_mapping: Topic to table mapping
            batch_size: Batch size
            insert_mode: Insert mode
            pk_mode: Primary key mode
            
        Returns:
            SQL Server Sink connector configuration
        """
        # Build connection URL for SQL Server
        # Format: jdbc:sqlserver://host:port;databaseName=database
        connection_url = (
            f"jdbc:sqlserver://{connection.host}:{connection.port};"
            f"databaseName={database}"
        )
        
        # Build topics list
        topics_list = ",".join(topics)
        
        # Extract table name from topic (format: {server}.{schema}.{table})
        # For SQL Server JDBC Sink, we should use just the table name, not schema.table
        # The JDBC connector interprets schema.table as database.table, which causes errors
        # The schema is determined by the connection's default schema (dbo for SQL Server)
        table_name = None
        if topics:
            first_topic = topics[0]
            topic_parts = first_topic.split(".")
            if len(topic_parts) >= 3:
                # Extract just the table name (last part of topic)
                table_name_only = topic_parts[-1]
                # For SQL Server, use just the table name - schema is from connection default
                table_name = table_name_only
        
        config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": topics_list,
            "connection.url": connection_url,
            "connection.user": connection.username,
            "connection.password": connection.password,
            # insert + pk.mode=none: append every change as new row (history/SCD2-style: a,b,c; update a→o adds row with __op=u; delete b adds row with __op=d)
            "insert.mode": insert_mode,
            "pk.mode": "none",  # Allow multiple rows per id so updates/deletes append new rows
            "batch.size": str(batch_size),
            # For SQL Server, use just table name (schema defaults to dbo when databaseName is in connection URL)
            # The JDBC connector interprets schema.table as database.table, so we use just the table name
            "table.name.format": table_name if table_name else "${topic}",
            "auto.create": "true",
            "auto.evolve": "false",  # Avoid schema drift; target tables created with __op, __source_ts_ms, __deleted
            "delete.enabled": "false",  # Disabled - we only do inserts
            # Use Debezium's ExtractNewRecordState transform to unwrap the envelope
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "true",
            # rewrite: keep DELETE events as records with __deleted=true (for SCD2-style tracking)
            "transforms.unwrap.delete.handling.mode": "rewrite",
            # Omit delete.tombstone.handling.mode for Kafka Connect 2.6 (Debezium 3.4+ only)
            # Add envelope metadata; use default names so Debezium produces __op, __source_ts_ms (double underscore)
            "transforms.unwrap.add.fields": "op,source.ts_ms",
            "consumer.override.auto.offset.reset": "earliest",  # New consumer groups start from beginning
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true"  # Debezium envelope has schemas
        }
        
        # SQL Server specific settings
        if connection.additional_config.get("encrypt", False):
            config["connection.url"] += ";encrypt=true"
        else:
            config["connection.url"] += ";encrypt=false"
        
        if connection.additional_config.get("trust_server_certificate", False):
            config["connection.url"] += ";trustServerCertificate=true"
        
        return config
    
    @staticmethod
    def _generate_oracle_sink_config(
        connector_name: str,
        connection: Connection,
        database: str,
        schema: str,
        topics: List[str],
        table_mapping: Optional[Dict[str, str]],
        batch_size: int,
        insert_mode: str,
        pk_mode: str
    ) -> Dict[str, Any]:
        """Generate Oracle JDBC Sink connector configuration (SCD2-style).
        
        Args:
            connector_name: Connector name
            connection: Oracle connection
            database: Database/service name
            schema: Schema name (Oracle schema/user)
            topics: Kafka topics
            table_mapping: Topic to table mapping
            batch_size: Batch size
            insert_mode: Insert mode
            pk_mode: Primary key mode
            
        Returns:
            Oracle Sink connector configuration
        """
        # Oracle JDBC URL: jdbc:oracle:thin:@//host:port/service_name
        service_name = database or connection.additional_config.get("service_name") or "ORCL"
        connection_url = (
            f"jdbc:oracle:thin:@//{connection.host}:{connection.port}/{service_name}"
        )
        topics_list = ",".join(topics)
        # Oracle uses schema.table; schema is typically uppercase
        schema_upper = (schema or connection.additional_config.get("schema") or connection.username or "").upper()
        config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": topics_list,
            "connection.url": connection_url,
            "connection.user": connection.username,
            "connection.password": connection.password,
            "insert.mode": insert_mode,
            "pk.mode": "none",
            "batch.size": str(batch_size),
            "auto.create": "true",
            "auto.evolve": "false",
            "delete.enabled": "false",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "true",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            # Omit delete.tombstone.handling.mode for Kafka Connect 2.6 (Debezium 3.4+ only)
            "transforms.unwrap.add.fields": "op,source.ts_ms",
            "consumer.override.auto.offset.reset": "earliest",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
        }
        if topics:
            first_topic = topics[0]
            topic_parts = first_topic.split(".")
            table_name = topic_parts[-1] if len(topic_parts) >= 2 else first_topic
            config["table.name.format"] = f"{schema_upper}.{table_name}"
        if table_mapping and topics and topics[0] in table_mapping:
            config["table.name.format"] = f"{schema_upper}.{table_mapping[topics[0]]}"
        return config
    
    @staticmethod
    def generate_connector_name(
        pipeline_name: str,
        database_type: str,
        schema: str
    ) -> str:
        """Generate Sink connector name.
        
        Args:
            pipeline_name: Pipeline name
            database_type: Database type
            schema: Schema name
            
        Returns:
            Connector name
        """
        # Format: sink-{pipeline_name}-{db_type}-{schema}
        if database_type == "postgresql":
            db_short = "pg"
        elif database_type in ["sqlserver", "mssql"]:
            db_short = "mssql"
        elif database_type == "oracle":
            db_short = "oracle"
        elif database_type == "s3":
            db_short = "s3"
        else:
            db_short = database_type.lower()[:4]  # Use first 4 chars as fallback
        return f"sink-{pipeline_name.lower().replace(' ', '_')}-{db_short}-{schema.lower()}"
    
    @staticmethod
    def _generate_s3_sink_config(
        connector_name: str,
        connection: Connection,
        bucket: str,
        prefix: str,
        topics: List[str],
        table_mapping: Optional[Dict[str, str]],
        batch_size: int
    ) -> Dict[str, Any]:
        """Generate S3 Sink connector configuration.
        
        Args:
            connector_name: Connector name
            connection: S3 connection
            bucket: S3 bucket name
            prefix: S3 prefix/path
            topics: Kafka topics to consume
            table_mapping: Topic to table mapping (optional)
            batch_size: Batch size for writes
            
        Returns:
            S3 Sink connector configuration
        """
        # Get AWS credentials from connection
        aws_access_key_id = connection.username  # Access key stored in username
        aws_secret_access_key = connection.password  # Secret key stored in password
        
        # Get region from additional_config
        region = connection.additional_config.get("region_name", "us-east-1")
        
        # Build topics list (use table mapping if provided)
        topics_list = topics if topics else []
        if table_mapping:
            # Map topics based on table mapping
            topics_list = [table_mapping.get(topic, topic) for topic in topics_list]
        
        # Validate topics list
        if not topics_list:
            raise ValueError("Cannot create S3 sink connector: No Kafka topics provided")
        
        # Build S3 path prefix
        s3_prefix = prefix if prefix else ""
        if s3_prefix and not s3_prefix.endswith("/"):
            s3_prefix += "/"
        
        # Validate AWS credentials
        if not aws_access_key_id:
            raise ValueError("Cannot create S3 sink connector: AWS access key ID is missing")
        if not aws_secret_access_key:
            raise ValueError("Cannot create S3 sink connector: AWS secret access key is missing")
        if not bucket:
            raise ValueError("Cannot create S3 sink connector: S3 bucket name is missing")
        
        # Generate connector configuration
        config = {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",
            "topics": ",".join(topics_list),
            
            # S3 Configuration
            "s3.region": region,
            "s3.bucket.name": bucket,
            "s3.part.size": "5242880",  # 5MB default
            "flush.size": str(batch_size) if batch_size and batch_size > 0 else "10",  # Required field, default to 10 for immediate writes
            
            # Storage and Format
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.json.JsonFormat",  # Use JSON format
            "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
            
            # Schema compatibility
            "schema.compatibility": "NONE",
            
            # AWS Credentials
            "aws.access.key.id": aws_access_key_id,
            "aws.secret.access.key": aws_secret_access_key,
        }
        
        # Add prefix if specified
        if s3_prefix:
            config["s3.prefix"] = s3_prefix
        
        # Optional: Add endpoint URL if provided
        endpoint_url = connection.additional_config.get("endpoint_url")
        if endpoint_url:
            config["s3.endpoint.url"] = endpoint_url
        
        logger.info(f"Generated S3 sink config for bucket: {bucket}, prefix: {s3_prefix}, topics: {topics_list}")
        
        return config
    
    @staticmethod
    def _generate_snowflake_sink_config(
        connector_name: str,
        connection: Connection,
        database: str,
        schema: str,
        topics: List[str],
        table_mapping: Optional[Dict[str, str]],
        batch_size: int
    ) -> Dict[str, Any]:
        """Generate Snowflake Sink connector configuration.
        
        Args:
            connector_name: Connector name
            connection: Snowflake connection
            database: Database name
            schema: Schema name
            topics: Kafka topics to consume
            table_mapping: Topic to table mapping (optional)
            batch_size: Batch size for writes
            
        Returns:
            Snowflake Sink connector configuration
        """
        # Get Snowflake account from connection
        # Account can be in format: "xy12345" or "xy12345.us-east-1" or full URL
        account = connection.host or connection.additional_config.get("account")
        if not account:
            raise ValueError("Cannot create Snowflake sink connector: Account is missing. Provide it in 'host' field or 'additional_config.account'")
        
        # Format Snowflake account URL properly for Kafka connector
        # Remove protocol if present to get base account
        account_clean = account.replace("https://", "").replace("http://", "")
        account_clean = account_clean.replace(".snowflakecomputing.com", "")
        account_clean = account_clean.rstrip('/')
        
        # Convert to lowercase (Snowflake URLs are case-insensitive but lowercase is standard)
        account_clean = account_clean.lower()
        
        # Build full URL: https://account.snowflakecomputing.com
        account = f"https://{account_clean}.snowflakecomputing.com"
        
        # Get credentials
        username = connection.username
        password = connection.password
        
        # Get optional parameters
        warehouse = connection.additional_config.get("warehouse")
        role = connection.additional_config.get("role")
        private_key = connection.additional_config.get("private_key")
        private_key_passphrase = connection.additional_config.get("private_key_passphrase")
        
        # Build topics list (use table mapping if provided)
        topics_list = topics if topics else []
        if table_mapping:
            topics_list = [table_mapping.get(topic, topic) for topic in topics_list]
        
        # Validate topics list
        if not topics_list:
            raise ValueError("Cannot create Snowflake sink connector: No Kafka topics provided")
        
        # Validate required fields
        if not username:
            raise ValueError("Cannot create Snowflake sink connector: Username is missing")
        if not password and not private_key:
            raise ValueError("Cannot create Snowflake sink connector: Either password or private_key must be provided")
        if not database:
            raise ValueError("Cannot create Snowflake sink connector: Database name is missing")
        if not schema:
            raise ValueError("Cannot create Snowflake sink connector: Schema name is missing")
        
        # Generate connector configuration
        config = {
            "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
            "tasks.max": "1",
            "topics": ",".join(topics_list),
            
            # Snowflake Connection Configuration
            "snowflake.url.name": account,
            "snowflake.user.name": username,
            "snowflake.database.name": database,
            "snowflake.schema.name": schema,
            
            # Optional Configuration
            "buffer.count.records": str(batch_size) if batch_size and batch_size > 0 else "10000",
            "buffer.flush.time": "60",  # Flush every 60 seconds
            "buffer.size.bytes": "5000000",  # 5MB buffer
            
            # Table and Topic Configuration
            # Extract table name from topic (format: {server}.{schema}.{table})
            # Use regex to extract just the table name (last part after last dot)
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            
            # Use JsonConverter with ExtractNewRecordState transform to handle Debezium format
            # This extracts the 'after' field from Debezium envelope
            # Snowflake connector will store in RECORD_CONTENT (VARIANT) and RECORD_METADATA (OBJECT) format
            # when no schema-based mapping is specified (default behavior)
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
            
            # Do NOT use ExtractNewRecordState transform - preserve full Debezium envelope
            # This keeps 'op', 'after', 'before' fields in RECORD_CONTENT
            # This is essential for CDC operations:
            # - 'op' field shows operation type (c=INSERT, u=UPDATE, d=DELETE)
            # - 'after' field shows new state (null for DELETE)
            # - 'before' field shows old state (null for INSERT)
            # Without transform, Snowflake connector stores full envelope in RECORD_CONTENT (VARIANT) column
            
            # Ensure Snowflake connector uses RECORD_CONTENT/RECORD_METADATA format
            # By not specifying snowflake.metadata.columns.mapping, connector defaults to variant format
            # This matches the format we use during full load
            
            # Error handling
            "errors.tolerance": "all",  # Continue on errors
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
        }
        
        # Authentication (only include the method being used, not empty strings)
        if private_key:
            config["snowflake.private.key"] = private_key
            if private_key_passphrase:
                config["snowflake.private.key.passphrase"] = private_key_passphrase
        elif password:
            config["snowflake.password"] = password
        else:
            raise ValueError("Either password or private_key must be provided for Snowflake authentication")
        
        # Add warehouse if provided
        if warehouse:
            config["snowflake.warehouse.name"] = warehouse
        
        # Add role if provided
        if role:
            config["snowflake.role.name"] = role
        
        # Table name format: Extract table name from topic
        # Topic format: {server}.{schema}.{table}
        # IMPORTANT: Use the actual topic names as-is (they may be UPPERCASE for Oracle)
        # The topic2table map must match the exact topic name case
        # Extract table name (last part) and convert to lowercase for Snowflake table name
        config["snowflake.topic2table.map"] = ",".join([
            f"{topic}:{topic.split('.')[-1].lower()}" for topic in topics_list
        ])
        
        logger.info(f"Generated Snowflake sink config for account: {account}, database: {database}, schema: {schema}, topics: {topics_list}")
        
        return config
    
    @staticmethod
    def extract_table_name_from_topic(topic_name: str) -> str:
        """Extract table name from Kafka topic name.
        
        Args:
            topic_name: Kafka topic name (format: {server}.{schema}.{table})
            
        Returns:
            Table name
        """
        # Topic format: {pipeline_name}.{schema}.{table}
        parts = topic_name.split(".")
        if len(parts) >= 3:
            return parts[-1]  # Last part is table name
        return topic_name

