# AS400–S3 pipeline flow (AS400-S3_P)

This doc describes the connector setup used for the AS400 → S3 pipeline.

## Connectors

### 1. Source connector (AS400 → Kafka)

| Item | Value |
|------|--------|
| **Connector class** | `io.debezium.connector.db2as400.As400RpcConnector` |
| **Plugin** | debezium-connector-ibmi (IBM i / AS400) |
| **Role** | Connects to AS400/IBM i, reads from journal, publishes change events to Kafka topics |

**Code:** `ingestion/debezium_config.py` → `_generate_as400_config()` builds the config for this class.

**Scripts:** Check for `As400RpcConnector` or `db2as400` in connector-plugins (e.g. `connection_service.py` when validating AS400 connections).

### 2. Sink connector (Kafka → S3)

| Item | Value |
|------|--------|
| **Connector class** | `io.confluent.connect.s3.S3SinkConnector` |
| **Version (from docs)** | v11.0.8 (Confluent S3 Sink) |
| **Connector name (instance)** | `sink-as400-s3_p-s3-dbo` (example: pipeline `as400-s3_p`, target S3, schema `dbo`) |
| **Role** | Consumes from Kafka topics produced by the AS400 source and writes records to the S3 bucket |

**Code:** `ingestion/sink_config.py` → `_generate_s3_sink_config()`; connector name from `generate_connector_name()` (format: `sink-{pipeline_name}-{db_type}-{schema}`).

**Scripts:** `check_connector_status.py`, `restart_connector.py`, `fix_flush_size.py` use the sink connector instance name (e.g. `sink-as400-s3_p-s3-dbo`).

## End-to-end flow

```
AS400 (source)  →  Debezium As400RpcConnector  →  Kafka topics
                                                          ↓
S3 (target)  ←  Confluent S3 Sink Connector  ←  (same topics)
```

So AS400–S3 uses:

- **Source:** Debezium `As400RpcConnector` (`io.debezium.connector.db2as400.As400RpcConnector`)
- **Sink:** Confluent `S3SinkConnector` (`io.confluent.connect.s3.S3SinkConnector`)
- **Sink connector instance name:** `sink-as400-s3_p-s3-dbo` (for pipeline name `as400-s3_p`, target S3, schema `dbo`)

## References

- `KAFKA_VPS_CONNECTION.md`: S3 Sink Connector class and connector naming
- `debezium_config.py`: `_generate_as400_config()` for As400RpcConnector
- `sink_config.py`: `_generate_s3_sink_config()` for S3 Sink
