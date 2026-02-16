"""Debezium connector configuration generator."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from ingestion.models import Connection

logger = logging.getLogger(__name__)


class DebeziumConfigGenerator:
    """Generate Debezium source connector configurations."""
    
    @staticmethod
    def generate_source_config(
        pipeline_name: str,
        source_connection: Connection,
        source_database: str,
        source_schema: str,
        source_tables: List[str],
        full_load_lsn: Optional[str] = None,
        snapshot_mode: str = "never"
    ) -> Dict[str, Any]:
        """Generate Debezium source connector configuration.
        
        Args:
            pipeline_name: Name of the pipeline (used for server name)
            source_connection: Source database connection
            source_database: Source database name
            source_schema: Source schema name
            source_tables: List of tables to capture
            full_load_lsn: LSN/offset from full load (if available)
            snapshot_mode: Snapshot mode (never, initial, etc.)
            
        Returns:
            Debezium connector configuration dictionary
        """
        dt = source_connection.database_type
        database_type = (getattr(dt, "value", None) or str(dt)).lower()
        
        if database_type == "postgresql":
            return DebeziumConfigGenerator._generate_postgresql_config(
                pipeline_name=pipeline_name,
                connection=source_connection,
                database=source_database,
                schema=source_schema,
                tables=source_tables,
                full_load_lsn=full_load_lsn,
                snapshot_mode=snapshot_mode
            )
        elif database_type in ["sqlserver", "mssql"]:
            return DebeziumConfigGenerator._generate_sqlserver_config(
                pipeline_name=pipeline_name,
                connection=source_connection,
                database=source_database,
                schema=source_schema,
                tables=source_tables,
                full_load_lsn=full_load_lsn,
                snapshot_mode=snapshot_mode
            )
        elif database_type in ["as400", "ibm_i"]:
            # Use AS400 RPC connector (io.debezium.connector.db2as400.As400RpcConnector)
            # REQUIRES: debezium-connector-ibmi plugin installed in Kafka Connect
            return DebeziumConfigGenerator._generate_as400_config(
                pipeline_name=pipeline_name,
                connection=source_connection,
                database=source_database,
                schema=source_schema,
                tables=source_tables,
                full_load_lsn=full_load_lsn,
                snapshot_mode=snapshot_mode
            )
        elif database_type == "db2":
            return DebeziumConfigGenerator._generate_db2_config(
                pipeline_name=pipeline_name,
                connection=source_connection,
                database=source_database,
                schema=source_schema,
                tables=source_tables,
                full_load_lsn=full_load_lsn,
                snapshot_mode=snapshot_mode
            )
        elif database_type == "oracle":
            return DebeziumConfigGenerator._generate_oracle_config(
                pipeline_name=pipeline_name,
                connection=source_connection,
                database=source_database,
                schema=source_schema,
                tables=source_tables,
                full_load_lsn=full_load_lsn,
                snapshot_mode=snapshot_mode
            )
        else:
            raise ValueError(f"Unsupported database type for Debezium: {database_type}")
    
    @staticmethod
    def _generate_postgresql_config(
        pipeline_name: str,
        connection: Connection,
        database: str,
        schema: str,
        tables: List[str],
        full_load_lsn: Optional[str],
        snapshot_mode: str
    ) -> Dict[str, Any]:
        """Generate PostgreSQL Debezium connector configuration.
        
        Args:
            pipeline_name: Pipeline name
            connection: PostgreSQL connection
            database: Database name
            schema: Schema name
            tables: List of tables
            full_load_lsn: LSN from full load
            snapshot_mode: Snapshot mode
            
        Returns:
            Debezium PostgreSQL connector configuration
        """
        # Validate and default schema to "public" if empty or None
        if not schema or schema.strip() == "":
            logger.warning(f"Empty schema provided for pipeline {pipeline_name}, defaulting to 'public'")
            schema = "public"
        
        # Validate tables list
        if not tables:
            raise ValueError(f"No tables provided for pipeline {pipeline_name}")
        
        # Build table include list with schema validation
        table_include_list = ",".join([f"{schema}.{table}" for table in tables])
        
        # Log the final table include list for debugging
        logger.info(f"Generated table.include.list for {pipeline_name}: {table_include_list}")
        
        # Generate replication slot name
        # Sanitize slot name: Postgres slots allow only [a-z0-9_]
        sanitized_pipeline = pipeline_name.lower().replace('-', '_')
        import re
        sanitized_pipeline = re.sub(r'[^a-z0-9_]', '_', sanitized_pipeline)
        slot_name = f"{sanitized_pipeline}_slot"
        
        # Check if we should use existing publication (e.g., cdc_publication)
        # If publication name is provided in additional_config, use it
        publication_name = connection.additional_config.get("publication_name")
        if not publication_name:
            # Sanitize publication name same as slot
            publication_name = f"{sanitized_pipeline}_pub"
        
        # Check if we should auto-create publication or use existing
        publication_autocreate = connection.additional_config.get("publication_autocreate", "filtered")
        
        # Fix snapshot mode - PostgreSQL valid values: always, never, initial_only, initial, custom
        # Use "initial_only" if full load done (captures schema only, no data)
        # Also handle "schema_only" which is used for SQL Server but not valid for PostgreSQL
        # For "never" mode (CDC_ONLY), keep it as "never" to start streaming immediately
        if snapshot_mode == "never":
            # CDC_ONLY mode - keep as "never" to start streaming without snapshot
            snapshot_mode = "never"
        elif snapshot_mode == "schema_only" and full_load_lsn:
            snapshot_mode = "initial_only"  # Only capture schema, skip data (already loaded)
        elif snapshot_mode == "schema_only":
            snapshot_mode = "initial"  # Default to initial if no full load
        elif full_load_lsn:
            snapshot_mode = "initial_only"  # Full load done - only schema, skip data
        
        # Use Docker hostname if specified (for Kafka Connect running in Docker)
        # Otherwise use the connection host (for local connections)
        db_hostname = connection.additional_config.get("docker_hostname", connection.host)
        
        # Generate connector name for the config
        connector_name = DebeziumConfigGenerator.generate_connector_name(
            pipeline_name=pipeline_name,
            database_type="postgresql",
            schema=schema
        )
        
        config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": db_hostname,
            "database.port": str(connection.port),
            "database.user": connection.username,
            "database.password": connection.password,
            "database.dbname": database,
            "database.server.name": pipeline_name,
            "topic.prefix": pipeline_name,  # Required for PostgreSQL connector
            "table.include.list": table_include_list,
            "plugin.name": "pgoutput",
            "slot.name": slot_name,
            "publication.name": publication_name,
            "publication.autocreate.mode": publication_autocreate,
            "snapshot.mode": snapshot_mode,
            "snapshot.locking.mode": "none",
            # Remove unwrap transform - keep Debezium envelope format with schemas
            # Sink connector will extract 'after' field from envelope
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",  # Keep schemas for JDBC sink
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true"
        }
        
        # If we have LSN from full load, configure starting position
        if full_load_lsn:
            logger.info(f"Using full load LSN for PostgreSQL: {full_load_lsn}")
            # Note: Debezium will automatically resume from the replication slot position
            # The snapshot mode is already set above (initial_only if full load done)
        
        return config
    
    @staticmethod
    def _generate_sqlserver_config(
        pipeline_name: str,
        connection: Connection,
        database: str,
        schema: str,
        tables: List[str],
        full_load_lsn: Optional[str],
        snapshot_mode: str
    ) -> Dict[str, Any]:
        """Generate SQL Server Debezium connector configuration.
        
        Args:
            pipeline_name: Pipeline name
            connection: SQL Server connection
            database: Database name
            schema: Schema name
            tables: List of tables
            full_load_lsn: LSN from full load
            snapshot_mode: Snapshot mode
            
        Returns:
            Debezium SQL Server connector configuration
        """
        # Validate and default schema to "dbo" if empty or None
        if not schema or schema.strip() == "":
            logger.warning(f"Empty schema provided for pipeline {pipeline_name}, defaulting to 'dbo'")
            schema = "dbo"
        
        # Validate tables list
        if not tables:
            raise ValueError(f"No tables provided for pipeline {pipeline_name}")
        
        # Build table include list with schema validation
        table_include_list = ",".join([f"{schema}.{table}" for table in tables])
        
        # Log the final table include list for debugging
        logger.info(f"Generated table.include.list for {pipeline_name}: {table_include_list}")
        
        # Build connection URL
        # SQL Server connection format: host:port;databaseName=database
        connection_url = f"{connection.host}:{connection.port};databaseName={database}"
        
        # Fix snapshot mode - SQL Server doesn't support "never", use "schema_only" if full load done
        if snapshot_mode == "never" and full_load_lsn:
            snapshot_mode = "schema_only"  # Only capture schema, skip data (already loaded)
        elif snapshot_mode == "never":
            snapshot_mode = "initial"  # Default to initial if no full load
        
        # Get SSL settings from connection config
        encrypt = connection.additional_config.get("encrypt", False)
        trust_server_cert = connection.additional_config.get("trust_server_certificate", True)
        
        # Generate connector name for the config
        connector_name = DebeziumConfigGenerator.generate_connector_name(
            pipeline_name=pipeline_name,
            database_type="sqlserver",
            schema=schema
        )
        
        config = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "tasks.max": "1",
            "database.hostname": connection.host,
            "database.port": str(connection.port),
            "database.user": connection.username,
            "database.password": connection.password,
            "database.names": database,
            "database.server.name": pipeline_name,
            "topic.prefix": pipeline_name,  # Required for SQL Server connector
            "table.include.list": table_include_list,
            "snapshot.mode": snapshot_mode,
            "snapshot.isolation.mode": "snapshot",
            "database.history.skip.unparseable.ddl": "true",
            # Schema history configuration (required) - use same Kafka as Connect worker
            "schema.history.internal": "io.debezium.storage.kafka.history.KafkaSchemaHistory",
            "schema.history.internal.kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "72.61.233.209:9092"),
            "schema.history.internal.kafka.topic": f"{pipeline_name}.schema.history.internal",
            # SSL/TLS configuration
            "database.encrypt": str(encrypt).lower(),
            "database.trustServerCertificate": str(trust_server_cert).lower(),
            # ExtractNewRecordState: unwrap envelope and add __op, __source_ts_ms from envelope (not static)
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",  # Debezium 3.4+ uses this; ensures deletes become rows with __deleted=true
            "transforms.unwrap.add.fields": "op,source.ts_ms",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",  # Enable schemas to match sink connector requirement
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true"
        }
        
        # SQL Server CDC requires CDC to be enabled on the database
        # Add CDC-specific configuration
        config["database.cdc.enabled"] = "true"
        
        # If we have LSN from full load, we can't directly set it in SQL Server connector
        # The connector will use the offset storage to track position
        # We'll need to ensure the offset is set correctly
        if full_load_lsn:
            logger.info(f"Full load LSN captured: {full_load_lsn} (will be used via offset storage)")
        
        return config
    
    @staticmethod
    def _generate_as400_config(
        pipeline_name: str,
        connection: Connection,
        database: str,
        schema: str,
        tables: List[str],
        full_load_lsn: Optional[str],
        snapshot_mode: str
    ) -> Dict[str, Any]:
        """Generate AS400/IBM i Debezium connector configuration using As400RpcConnector.
        
        Args:
            pipeline_name: Pipeline name
            connection: AS400 connection
            database: Database/library name
            schema: Schema/library name
            tables: List of tables
            full_load_lsn: LSN from full load (not used for AS400, uses journal instead)
            snapshot_mode: Snapshot mode
            
        Returns:
            Debezium AS400 RPC connector configuration for IBM i
        """
        # Validate and default schema/library
        # For AS400, library can come from database, schema, or additional_config
        if not schema:
            library = connection.additional_config.get("library") if connection.additional_config else None
            if library:
                schema = library
                logger.info(f"Using library from additional_config: {library}")
            elif database:
                schema = database
                logger.info(f"Using database as library: {database}")
            else:
                logger.warning(f"Schema/library not provided for AS400 pipeline {pipeline_name}")
                schema = "QSYS"  # Default to QSYS
        
        if not tables:
            logger.warning(f"No tables provided for AS400 pipeline {pipeline_name}")
        
        # Build table include list
        table_include_list = ",".join([f"{schema}.{table}" for table in tables])
        
        # AS400/IBM i uses journaling for CDC
        # Debezium Db2 connector can work with AS400 if configured correctly
        # Note: Requires journaling to be enabled on the tables
        
        # Build connection URL for Db2
        # Format: jdbc:db2://host:port/database
        db_hostname = connection.additional_config.get("docker_hostname", connection.host)
        port = connection.port or 446  # Default AS400 port
        
        # Adjust snapshot mode for AS400
        # If full_load_lsn exists, we can use "never" to start from offset
        # If no full_load_lsn, we must use "initial" to establish baseline
        if snapshot_mode == "never":
            if not full_load_lsn:
                # No offset available - must take snapshot first
                logger.warning(f"AS400 connector: snapshot.mode='never' but no full_load_lsn. Changing to 'initial' to establish baseline.")
                snapshot_mode = "initial"
            else:
                # Offset exists - can start from offset
                snapshot_mode = "never"
        elif snapshot_mode == "schema_only" and full_load_lsn:
            snapshot_mode = "schema_only"
        elif snapshot_mode == "schema_only":
            snapshot_mode = "initial"
        elif full_load_lsn:
            # Full load completed with offset - use never to start from offset
            snapshot_mode = "never"
        else:
            # No full load or no offset - must take initial snapshot
            snapshot_mode = "initial"
        
        # Generate connector name for the config
        connector_name = DebeziumConfigGenerator.generate_connector_name(
            pipeline_name=pipeline_name,
            database_type="as400",
            schema=schema
        )
        
        # On AS400/IBM i: database.dbname = default library (e.g. QGPL); database.schema = library where table lives (e.g. SEGMETRIQ1).
        # User credentials: database=QGPL, username=segmetriq, password=..., host=pub400.com, port=9471.
        dbname = database or (connection.database if connection.database else None) or "QGPL"
        if connection.additional_config and connection.additional_config.get("default_library"):
            dbname = connection.additional_config["default_library"]
        config = {
            "connector.class": "io.debezium.connector.db2as400.As400RpcConnector",
            "tasks.max": "1",
            "database.hostname": db_hostname,
            "database.port": str(port),
            "database.user": connection.username,
            "database.password": connection.password,
            "database.dbname": dbname,  # Default library (e.g. QGPL) - connector uses this to obtain real DB name
            "database.schema": schema,  # Library where table lives (e.g. SEGMETRIQ1)
            "database.server.name": pipeline_name,
            "topic.prefix": pipeline_name,
            "table.include.list": table_include_list,
            "snapshot.mode": snapshot_mode,
            "snapshot.locking.mode": "none",
            # AS400/IBM i specific settings
            "database.history.skip.unparseable.ddl": "true",
            # Schema history configuration (use same bootstrap as SQL Server - reachable from Connect)
            "schema.history.internal": "io.debezium.storage.kafka.history.KafkaSchemaHistory",
            "schema.history.internal.kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "72.61.233.209:9092"),
            "schema.history.internal.kafka.topic": f"{pipeline_name}.schema.history.internal",
            # Transforms to flatten messages; add __op, __source_ts_ms for SCD2 sink (no 3.4+ only props for 2.6)
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "true",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.add.fields": "op,source.ts_ms",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true"
        }
        
        # Add journal library if specified
        journal_library = connection.additional_config.get("journal_library")
        if journal_library:
            config["database.journal.library"] = journal_library
            logger.info(f"Using journal library: {journal_library}")
        else:
            # Default to QSYS if not specified
            config["database.journal.library"] = "QSYS"
            logger.info("Using default journal library: QSYS")
        
        logger.info(f"Generated AS400/IBM i Debezium config for {pipeline_name}")
        
        return config
    
    @staticmethod
    def _generate_db2_config(
        pipeline_name: str,
        connection: Connection,
        database: str,
        schema: str,
        tables: List[str],
        full_load_lsn: Optional[str],
        snapshot_mode: str
    ) -> Dict[str, Any]:
        """Generate Debezium Db2 connector configuration (io.debezium.connector.db2.Db2Connector).
        
        Use this when Kafka Connect has the standard Db2 connector (not As400RpcConnector).
        Topic format: {topic.prefix}.{schema}.{table}.
        """
        if not schema:
            schema = connection.schema or connection.additional_config.get("schema") if connection.additional_config else "DB2INST1"
            logger.warning(f"Schema not provided for Db2 pipeline {pipeline_name}, using: {schema}")
        if not tables:
            logger.warning(f"No tables provided for Db2 pipeline {pipeline_name}")
        
        table_include_list = ",".join([f"{schema}.{t}" for t in tables])
        db_hostname = connection.additional_config.get("docker_hostname", connection.host) if connection.additional_config else connection.host
        port = connection.port or 50000  # Db2 LUW default; use connection.port for AS400/IBM i (e.g. 9471)
        
        if snapshot_mode == "never" and not full_load_lsn:
            logger.warning(f"Db2 connector: snapshot.mode='never' but no full_load_lsn. Using 'initial'.")
            snapshot_mode = "initial"
        elif snapshot_mode == "never":
            snapshot_mode = "never"
        elif snapshot_mode == "schema_only" and full_load_lsn:
            snapshot_mode = "schema_only"
        elif snapshot_mode == "schema_only":
            snapshot_mode = "no_data"  # Db2 uses no_data for schema-only
        elif full_load_lsn:
            snapshot_mode = "never"
        else:
            snapshot_mode = snapshot_mode or "initial"
        
        config = {
            "connector.class": "io.debezium.connector.db2.Db2Connector",
            "tasks.max": "1",
            "database.hostname": db_hostname,
            "database.port": str(port),
            "database.user": connection.username,
            "database.password": connection.password,
            "database.dbname": database or schema,
            "database.server.name": pipeline_name,
            "topic.prefix": pipeline_name,
            "table.include.list": table_include_list,
            "snapshot.mode": snapshot_mode,
            "schema.history.internal": "io.debezium.storage.kafka.history.KafkaSchemaHistory",
            "schema.history.internal.kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "72.61.233.209:9092"),
            "schema.history.internal.kafka.topic": f"{pipeline_name}.schema.history.internal",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "true",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",  # Debezium 3.4+; deletes become rows with __deleted=true
            "transforms.unwrap.add.fields": "op,source.ts_ms",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
        }
        
        logger.info(f"Generated Db2 Debezium config for {pipeline_name}")
        return config
    
    @staticmethod
    def _generate_oracle_config(
        pipeline_name: str,
        connection: Connection,
        database: str,
        schema: str,
        tables: List[str],
        full_load_lsn: Optional[str],
        snapshot_mode: str
    ) -> Dict[str, Any]:
        """Generate Oracle Debezium connector configuration.
        
        Args:
            pipeline_name: Pipeline name
            connection: Oracle connection
            database: Database name (SID) or service name
            schema: Schema name (user)
            tables: List of tables
            full_load_lsn: SCN from full load (if available)
            snapshot_mode: Snapshot mode
            
        Returns:
            Debezium Oracle connector configuration
        """
        # Validate and default schema
        if not schema:
            schema = connection.username  # Default to username
            logger.info(f"Using username as schema: {schema}")
        
        if not tables:
            logger.warning(f"No tables provided for Oracle pipeline {pipeline_name}")
        
        # Build table include list
        # Format: schema.table
        table_include_list = ",".join([f"{schema}.{table}" for table in tables])
        
        # Sanitize schema name for topic naming (Kafka topics can't have # or other special chars)
        # We'll use a topic name transformation to replace invalid characters
        import re
        sanitized_schema_for_topic = re.sub(r'[#@$%^&*()+=\[\]{}|\\:;"\'<>?,/`~!]', '_', schema)
        sanitized_schema_for_topic = re.sub(r'_+', '_', sanitized_schema_for_topic).strip('_')
        
        # Adjust snapshot mode
        # Oracle uses SCN (System Change Number) instead of LSN
        # IMPORTANT: Oracle Debezium connector does NOT support "never" snapshot mode
        # Valid Oracle snapshot modes: always, initial_only, initial, schema_only, schema_only_recovery
        # For CDC-only (after full load), use "initial_only" (schema only, no data) or "schema_only"
        if snapshot_mode == "never":
            # Oracle doesn't support "never" - use "initial_only" for CDC-only mode
            if full_load_lsn and "SCN:" in str(full_load_lsn):
                # Full load completed - use initial_only (schema only, no data snapshot)
                snapshot_mode = "initial_only"
                logger.info(f"Oracle: Changed 'never' to 'initial_only' (full load completed, CDC-only mode)")
            else:
                # No full load - use initial to capture existing data
                snapshot_mode = "initial"
                logger.warning(f"Oracle connector: snapshot.mode='never' not supported. Changed to 'initial' to establish baseline.")
        elif full_load_lsn and "SCN:" in str(full_load_lsn):
            # Extract SCN from full_load_lsn (format: "SCN:123456")
            scn = str(full_load_lsn).split(":")[1] if ":" in str(full_load_lsn) else None
            if scn:
                # Full load completed - use initial_only (schema only, CDC will start from SCN offset)
                snapshot_mode = "initial_only"
                logger.info(f"Using SCN from full load: {scn}, snapshot mode: initial_only")
        
        # Get connection details
        db_hostname = connection.additional_config.get("docker_hostname", connection.host)
        port = connection.port or 1521  # Default Oracle port
        
        # Determine if using SID or service name
        service_name = connection.additional_config.get("service_name")
        database_name = database or connection.database
        
        # Generate connector name for the config
        connector_name = DebeziumConfigGenerator.generate_connector_name(
            pipeline_name=pipeline_name,
            database_type="oracle",
            schema=schema
        )
        
        config = {
            "connector.class": "io.debezium.connector.oracle.OracleConnector",
            "tasks.max": "1",
            "database.hostname": db_hostname,
            "database.port": str(port),
            "database.user": connection.username,
            "database.password": connection.password,
            "database.server.name": pipeline_name,
            "topic.prefix": pipeline_name,
            "table.include.list": table_include_list,
            "snapshot.mode": snapshot_mode,
            "snapshot.locking.mode": "none",
            # Oracle-specific settings
            "database.history.skip.unparseable.ddl": "true",
            # Schema history configuration
            "schema.history.internal": "io.debezium.storage.kafka.history.KafkaSchemaHistory",
            # LogMiner configuration (Oracle CDC method)
            "log.mining.strategy": "online_catalog",
            "log.mining.continuous.mine": "true",
            # IMPORTANT: Topic name sanitization
            # Kafka topic names can only contain: alphanumeric, period (.), underscore (_), hyphen (-)
            # Debezium generates topic names as: {database.server.name}.{schema}.{table}
            # If schema contains invalid chars (like # in "c##cdc_user"), Debezium should auto-sanitize
            # However, the connector may fail before sanitization happens
            # 
            # NOTE: RegexRouter transform doesn't work for topic name transformation - it's for message routing
            # Debezium handles topic name sanitization internally, but may fail if topic creation is attempted
            # with invalid name before sanitization
            #
            # SOLUTION: Pre-create topics with sanitized names OR ensure Debezium sanitizes before topic creation
            # For now, we'll rely on Debezium's internal sanitization (replaces invalid chars with _)
            # Transforms - keep Debezium envelope format
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true"
        }
        
        # IMPORTANT: Do NOT set bootstrap.servers for the connector itself
        # The connector should inherit bootstrap.servers from the Kafka Connect worker configuration
        # (set via CONNECT_BOOTSTRAP_SERVERS environment variable in the Docker container)
        # Explicitly setting bootstrap.servers causes connectivity issues (hostname may not resolve)
        #
        # However, schema.history.internal.kafka.bootstrap.servers MUST be set explicitly
        # Schema history storage does NOT inherit from worker config and requires explicit bootstrap servers
        # Default: "kafka-cdc:9092" (matches CONNECT_BOOTSTRAP_SERVERS on VPS server - port 9092, not 29092)
        # To override, set KAFKA_BOOTSTRAP_SERVERS_INTERNAL env var
        kafka_bootstrap_internal = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "kafka-cdc:9092")
        kafka_host_port = kafka_bootstrap_internal.split(",")[0].strip()
        
        # Set schema history configuration (required for Oracle connector)
        config["schema.history.internal.kafka.bootstrap.servers"] = kafka_host_port
        config["schema.history.internal.kafka.topic"] = f"{pipeline_name}.schema.history.internal"
        logger.info(f"Schema history Kafka bootstrap servers: {kafka_host_port}")
        
        # Configure database connection (SID or service name)
        # IMPORTANT: database.connection.adapter must be one of: olr, xstream, logminer
        # We use "logminer" for Oracle LogMiner-based CDC (default and most common)
        # The database.dbname can be either SID or service name, but adapter must be logminer
        config["database.connection.adapter"] = "logminer"  # Use LogMiner for CDC
        
        if service_name:
            # When using service name, set it in database.dbname
            config["database.dbname"] = service_name
            logger.info(f"Using service name: {service_name} with LogMiner adapter")
        else:
            # Use SID (database name)
            config["database.dbname"] = database_name
            logger.info(f"Using SID: {database_name} with LogMiner adapter")
        
        # If we have SCN from full load, ensure snapshot mode is correct
        # Note: We already set snapshot_mode above, but if full_load_lsn exists,
        # we should use "initial_only" (not "never" - Oracle doesn't support "never")
        if full_load_lsn and "SCN:" in str(full_load_lsn):
            scn = str(full_load_lsn).split(":")[1] if ":" in str(full_load_lsn) else None
            if scn:
                # Ensure snapshot mode is valid for Oracle (use initial_only, not never)
                if config.get("snapshot.mode") == "never":
                    config["snapshot.mode"] = "initial_only"
                    logger.info(f"Changed snapshot.mode from 'never' to 'initial_only' (Oracle doesn't support 'never')")
                config["database.history.skip.unparseable.ddl"] = "true"
                # Note: Debezium Oracle connector will resume from the last committed offset
                # The SCN is stored in the offset, so it will automatically resume from there
                logger.info(f"Will resume from SCN: {scn}, snapshot mode: {config.get('snapshot.mode')}")
        
        logger.info(f"Generated Oracle Debezium config for {pipeline_name}")
        
        return config
    
    @staticmethod
    def generate_connector_name(
        pipeline_name: str,
        database_type: str,
        schema: str
    ) -> str:
        """Generate Debezium connector name.
        
        Args:
            pipeline_name: Pipeline name
            database_type: Database type
            schema: Schema name
            
        Returns:
            Connector name
        """
        # Format: cdc-{pipeline_name}-{db_type}-{schema}
        if database_type == "postgresql":
            db_short = "pg"
        elif database_type in ["sqlserver", "mssql"]:
            db_short = "mssql"
        elif database_type in ["as400", "ibm_i"]:
            db_short = "as400"
        elif database_type == "db2":
            db_short = "db2"
        elif database_type == "oracle":
            db_short = "ora"
        else:
            db_short = database_type[:6]  # Use first 6 chars as fallback
        
        # Sanitize schema name: replace special characters that cause URL issues
        # Replace # with _ (common in Oracle container database schemas like c##cdc_user)
        # Replace other special characters with underscore
        import re
        sanitized_schema = re.sub(r'[#@$%^&*()+=\[\]{}|\\:;"\'<>?,/`~!]', '_', schema.lower())
        # Remove multiple consecutive underscores
        sanitized_schema = re.sub(r'_+', '_', sanitized_schema)
        # Remove leading/trailing underscores
        sanitized_schema = sanitized_schema.strip('_')
        
        return f"cdc-{pipeline_name.lower()}-{db_short}-{sanitized_schema}"
    
    @staticmethod
    def get_topic_name(
        pipeline_name: str,
        schema: str,
        table: str,
        database: Optional[str] = None
    ) -> str:
        """Get Kafka topic name for a table.
        
        Args:
            pipeline_name: Pipeline name (database.server.name / topic.prefix)
            schema: Schema name
            table: Table name
            database: Database name (optional). Required for SQL Server - Debezium uses
                      {topic.prefix}.{database}.{schema}.{table}; without it we get wrong topic.
            
        Returns:
            Kafka topic name (sanitized to remove invalid characters)
        """
        import re
        # Replace each invalid char with underscore
        sanitized_schema = re.sub(r'[^a-zA-Z0-9._-]', '_', schema).strip('_')
        sanitized_table = re.sub(r'[^a-zA-Z0-9._-]', '_', table).strip('_')
        # SQL Server Debezium topic format: {topic.prefix}.{database}.{schema}.{table}
        if database:
            sanitized_db = re.sub(r'[^a-zA-Z0-9._-]', '_', database).strip('_')
            return f"{pipeline_name}.{sanitized_db}.{sanitized_schema}.{sanitized_table}"
        # PostgreSQL and others: {database.server.name}.{schema}.{table}
        return f"{pipeline_name}.{sanitized_schema}.{sanitized_table}"

