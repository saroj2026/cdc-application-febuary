# Installing AS400/IBM i Connector for Kafka Connect

## Problem

Standard Debezium Db2 connector (`io.debezium.connector.db2.Db2Connector`) does not fully support AS400/IBM i systems. You need the specialized AS400 connector.

## Solution: Install debezium-connector-ibmi

This plugin provides `io.debezium.connector.db2as400.As400RpcConnector` which is designed specifically for AS400/IBM i.

### Installation Steps (on Kafka Connect server: 72.61.233.209)

#### 1. Create plugin directory
```bash
ssh user@72.61.233.209
cd /path/to/kafka/connect/plugins  # Or wherever your plugins are
mkdir -p debezium-connector-ibmi
cd debezium-connector-ibmi
```

#### 2. Download the connector

**Option A: Maven Central**
```bash
# Find latest version at: https://mvnrepository.com/artifact/io.debezium/debezium-connector-db2as400
VERSION="2.5.0.Final"  # Use latest version
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-db2as400/${VERSION}/debezium-connector-db2as400-${VERSION}-plugin.tar.gz
tar -xzf debezium-connector-db2as400-${VERSION}-plugin.tar.gz
```

**Option B: Debezium releases**
```bash
wget https://github.com/debezium/debezium-connector-db2as400/releases/download/v2.5.0/debezium-connector-db2as400-2.5.0-plugin.tar.gz
tar -xzf debezium-connector-db2as400-2.5.0-plugin.tar.gz
```

#### 3. Add jt400.jar (IBM Toolbox for Java)

The AS400 connector requires jt400.jar:

```bash
cd debezium-connector-ibmi  # or the extracted directory
wget https://repo1.maven.org/maven2/net/sf/jt400/jt400/20.0.5/jt400-20.0.5.jar
# Or download from: https://mvnrepository.com/artifact/net.sf/jt400
```

#### 4. Verify plugin structure

Your plugin directory should look like:
```
/path/to/kafka/connect/plugins/debezium-connector-ibmi/
├── debezium-connector-db2as400-2.5.0.Final.jar
├── debezium-core-2.5.0.Final.jar
├── jt400-20.0.5.jar
└── (other dependency JARs)
```

#### 5. Update Kafka Connect configuration

Edit `/etc/kafka/connect-distributed.properties` (or wherever your config is):

```properties
plugin.path=/path/to/kafka/connect/plugins
```

#### 6. Restart Kafka Connect

```bash
sudo systemctl restart confluent-kafka-connect
# OR if using Docker:
docker restart kafka-connect
# OR if running manually:
./bin/connect-distributed.sh config/connect-distributed.properties
```

#### 7. Verify installation

```bash
curl http://72.61.233.209:8083/connector-plugins | python -m json.tool | grep -i as400
```

You should see:
```json
{
  "class": "io.debezium.connector.db2as400.As400RpcConnector",
  "type": "source",
  "version": "2.5.0.Final"
}
```

## After Installation

1. The backend code is already configured to use As400RpcConnector for AS400/IBM i sources
2. Simply restart pipeline-6:
   ```bash
   cd backend
   python scripts/create_pipeline6_as400_to_sql.py
   ```

## Alternative: Use Db2 Connector with AS400-specific config

If you cannot install the AS400 connector, you may be able to use the standard Db2 connector with additional configuration:

- Add `database.cdcschema` property pointing to your journal library
- Configure journal-based CDC settings
- May require additional AS400-specific Db2 driver properties

However, this is not recommended - the As400RpcConnector is purpose-built for AS400/IBM i and will be much more reliable.

## References

- Debezium DB2 AS400 connector: https://github.com/debezium/debezium-connector-db2as400
- JT400 (IBM Toolbox): https://mvnrepository.com/artifact/net.sf/jt400
- AS400 CDC configuration: See `backend/ingestion/debezium_config.py` → `_generate_as400_config()`
