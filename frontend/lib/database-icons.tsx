/**
 * Database Icons and Logos
 * Real database logos for connection cards
 * Supports both image files from /public/logos/ and SVG components
 */

import {
  MySQLLogo,
  PostgreSQLLogo,
  SQLServerLogo,
  OracleLogo,
  MongoLogo,
  SnowflakeLogo,
  RedshiftLogo,
  BigQueryLogo,
  DatabricksLogo,
  MariaDBLogo,
  CassandraLogo,
  CouchbaseLogo,
  DynamoDBLogo,
  ClickHouseLogo,
  AthenaLogo,
  AzureSQLLogo,
  BigTableLogo,
  CockroachLogo,
  S3Logo,
  DefaultDatabaseLogo,
} from "./database-logos"
import { DatabaseLogo } from "./database-logo-loader"

export interface DatabaseInfo {
  id: string
  name: string
  displayName: string
  icon: React.ComponentType<{ className?: string }>
  defaultPort: number
  connectionType: string
  category: 'relational' | 'nosql' | 'warehouse' | 'cloud' | 'other'
  beta?: boolean
}

export const DATABASE_SERVICES: DatabaseInfo[] = [
  // Relational Databases
  {
    id: 'mysql',
    name: 'MySQL',
    displayName: 'MySQL',
    icon: MySQLLogo,
    defaultPort: 3306,
    connectionType: 'mysql',
    category: 'relational'
  },
  {
    id: 'postgresql',
    name: 'PostgreSQL',
    displayName: 'PostgreSQL',
    icon: PostgreSQLLogo,
    defaultPort: 5432,
    connectionType: 'postgresql',
    category: 'relational'
  },
  {
    id: 'mssql',
    name: 'SQL Server',
    displayName: 'SQL Server',
    icon: SQLServerLogo,
    defaultPort: 1433,
    connectionType: 'sqlserver',
    category: 'relational'
  },
  {
    id: 'oracle',
    name: 'Oracle',
    displayName: 'Oracle',
    icon: OracleLogo,
    defaultPort: 1521,
    connectionType: 'oracle',
    category: 'relational'
  },
  {
    id: 'mariadb',
    name: 'MariaDB',
    displayName: 'MariaDB',
    icon: MariaDBLogo,
    defaultPort: 3306,
    connectionType: 'mysql',
    category: 'relational'
  },
  {
    id: 'sqlite',
    name: 'SQLite',
    displayName: 'SQLite',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'sqlite',
    category: 'relational'
  },
  {
    id: 'db2',
    name: 'DB2',
    displayName: 'IBM DB2',
    icon: DefaultDatabaseLogo,
    defaultPort: 50000,
    connectionType: 'db2',
    category: 'relational'
  },
  {
    id: 'cockroach',
    name: 'CockroachDB',
    displayName: 'CockroachDB',
    icon: CockroachLogo,
    defaultPort: 26257,
    connectionType: 'cockroach',
    category: 'relational',
    beta: true
  },
  {
    id: 'singlestore',
    name: 'SingleStore',
    displayName: 'SingleStore',
    icon: DefaultDatabaseLogo,
    defaultPort: 3306,
    connectionType: 'singlestore',
    category: 'relational'
  },
  {
    id: 'greenplum',
    name: 'Greenplum',
    displayName: 'Greenplum',
    icon: DefaultDatabaseLogo,
    defaultPort: 5432,
    connectionType: 'greenplum',
    category: 'relational'
  },
  {
    id: 'vertica',
    name: 'Vertica',
    displayName: 'Vertica',
    icon: DefaultDatabaseLogo,
    defaultPort: 5433,
    connectionType: 'vertica',
    category: 'relational'
  },
  {
    id: 'teradata',
    name: 'Teradata',
    displayName: 'Teradata',
    icon: DefaultDatabaseLogo,
    defaultPort: 1025,
    connectionType: 'teradata',
    category: 'relational'
  },
  {
    id: 'exasol',
    name: 'Exasol',
    displayName: 'Exasol',
    icon: DefaultDatabaseLogo,
    defaultPort: 8563,
    connectionType: 'exasol',
    category: 'relational'
  },
  {
    id: 'as400',
    name: 'AS-400',
    displayName: 'IBM AS-400',
    icon: DefaultDatabaseLogo,
    defaultPort: 9471,
    connectionType: 'as400',
    category: 'relational'
  },
  {
    id: 'sap_hana',
    name: 'SAP HANA',
    displayName: 'SAP HANA',
    icon: DefaultDatabaseLogo,
    defaultPort: 30015,
    connectionType: 'sap_hana',
    category: 'relational'
  },
  {
    id: 'sap_erp',
    name: 'SAP ERP',
    displayName: 'SAP ERP',
    icon: DefaultDatabaseLogo,
    defaultPort: 3300,
    connectionType: 'sap_erp',
    category: 'relational'
  },
  {
    id: 'timescale',
    name: 'TimescaleDB',
    displayName: 'TimescaleDB',
    icon: DefaultDatabaseLogo,
    defaultPort: 5432,
    connectionType: 'timescale',
    category: 'relational',
    beta: true
  },
  
  // NoSQL Databases
  {
    id: 'mongodb',
    name: 'MongoDB',
    displayName: 'MongoDB',
    icon: MongoLogo,
    defaultPort: 27017,
    connectionType: 'mongodb',
    category: 'nosql'
  },
  {
    id: 'cassandra',
    name: 'Cassandra',
    displayName: 'Cassandra',
    icon: CassandraLogo,
    defaultPort: 9042,
    connectionType: 'cassandra',
    category: 'nosql',
    beta: true
  },
  {
    id: 'couchbase',
    name: 'Couchbase',
    displayName: 'Couchbase',
    icon: CouchbaseLogo,
    defaultPort: 8091,
    connectionType: 'couchbase',
    category: 'nosql'
  },
  {
    id: 'dynamodb',
    name: 'DynamoDB',
    displayName: 'DynamoDB',
    icon: DynamoDBLogo,
    defaultPort: 0,
    connectionType: 'dynamodb',
    category: 'nosql'
  },
  {
    id: 'bigtable',
    name: 'BigTable',
    displayName: 'BigTable',
    icon: BigTableLogo,
    defaultPort: 0,
    connectionType: 'bigtable',
    category: 'nosql'
  },
  
  // Data Warehouses
  {
    id: 'snowflake',
    name: 'Snowflake',
    displayName: 'Snowflake',
    icon: SnowflakeLogo,
    defaultPort: 443,
    connectionType: 'snowflake',
    category: 'warehouse'
  },
  {
    id: 'redshift',
    name: 'Redshift',
    displayName: 'Amazon Redshift',
    icon: RedshiftLogo,
    defaultPort: 5439,
    connectionType: 'redshift',
    category: 'warehouse'
  },
  {
    id: 'bigquery',
    name: 'BigQuery',
    displayName: 'BigQuery',
    icon: BigQueryLogo,
    defaultPort: 0,
    connectionType: 'bigquery',
    category: 'warehouse'
  },
  {
    id: 'databricks',
    name: 'Databricks',
    displayName: 'Databricks',
    icon: DatabricksLogo,
    defaultPort: 443,
    connectionType: 'databricks',
    category: 'warehouse'
  },
  {
    id: 'athena',
    name: 'Athena',
    displayName: 'Amazon Athena',
    icon: AthenaLogo,
    defaultPort: 0,
    connectionType: 'athena',
    category: 'warehouse'
  },
  {
    id: 'presto',
    name: 'Presto',
    displayName: 'Presto',
    icon: DefaultDatabaseLogo,
    defaultPort: 8080,
    connectionType: 'presto',
    category: 'warehouse'
  },
  {
    id: 'trino',
    name: 'Trino',
    displayName: 'Trino',
    icon: DefaultDatabaseLogo,
    defaultPort: 8080,
    connectionType: 'trino',
    category: 'warehouse'
  },
  {
    id: 'clickhouse',
    name: 'ClickHouse',
    displayName: 'ClickHouse',
    icon: ClickHouseLogo,
    defaultPort: 8123,
    connectionType: 'clickhouse',
    category: 'warehouse'
  },
  {
    id: 'druid',
    name: 'Druid',
    displayName: 'Apache Druid',
    icon: DefaultDatabaseLogo,
    defaultPort: 8082,
    connectionType: 'druid',
    category: 'warehouse'
  },
  {
    id: 'pinot',
    name: 'Pinot',
    displayName: 'Apache Pinot',
    icon: DefaultDatabaseLogo,
    defaultPort: 9000,
    connectionType: 'pinot',
    category: 'warehouse'
  },
  {
    id: 'doris',
    name: 'Doris',
    displayName: 'Apache Doris',
    icon: DefaultDatabaseLogo,
    defaultPort: 9030,
    connectionType: 'doris',
    category: 'warehouse'
  },
  {
    id: 'impala',
    name: 'Impala',
    displayName: 'Apache Impala',
    icon: DefaultDatabaseLogo,
    defaultPort: 21000,
    connectionType: 'impala',
    category: 'warehouse'
  },
  
  // Data Lakes
  {
    id: 'datalake',
    name: 'Data Lake',
    displayName: 'Data Lake',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'datalake',
    category: 'warehouse'
  },
  {
    id: 'deltalake',
    name: 'Delta Lake',
    displayName: 'Delta Lake',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'deltalake',
    category: 'warehouse'
  },
  {
    id: 'iceberg',
    name: 'Iceberg',
    displayName: 'Apache Iceberg',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'iceberg',
    category: 'warehouse'
  },
  {
    id: 'hive',
    name: 'Hive',
    displayName: 'Apache Hive',
    icon: DefaultDatabaseLogo,
    defaultPort: 10000,
    connectionType: 'hive',
    category: 'warehouse'
  },
  {
    id: 'glue',
    name: 'Glue',
    displayName: 'AWS Glue',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'glue',
    category: 'warehouse'
  },
  {
    id: 'unity_catalog',
    name: 'Unity Catalog',
    displayName: 'Unity Catalog',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'unity_catalog',
    category: 'warehouse'
  },
  
  // Cloud Services
  {
    id: 's3',
    name: 'Amazon S3',
    displayName: 'Amazon S3',
    icon: S3Logo,
    defaultPort: 443,
    connectionType: 's3',
    category: 'cloud'
  },
  {
    id: 'aws_s3',
    name: 'AWS S3',
    displayName: 'AWS S3',
    icon: S3Logo,
    defaultPort: 443,
    connectionType: 'aws_s3',
    category: 'cloud'
  },
  {
    id: 'azuresql',
    name: 'Azure SQL',
    displayName: 'Azure SQL',
    icon: AzureSQLLogo,
    defaultPort: 1433,
    connectionType: 'sqlserver',
    category: 'cloud'
  },
  {
    id: 'salesforce',
    name: 'Salesforce',
    displayName: 'Salesforce',
    icon: DefaultDatabaseLogo,
    defaultPort: 443,
    connectionType: 'salesforce',
    category: 'cloud'
  },
  {
    id: 'domo',
    name: 'Domo Database',
    displayName: 'Domo',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'domo',
    category: 'cloud'
  },
  {
    id: 'sas',
    name: 'SAS',
    displayName: 'SAS',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'sas',
    category: 'cloud'
  },
  
  // Custom
  {
    id: 'custom',
    name: 'Custom Database',
    displayName: 'Custom Database',
    icon: DefaultDatabaseLogo,
    defaultPort: 0,
    connectionType: 'custom',
    category: 'other'
  }
]

export const getDatabaseInfo = (id: string): DatabaseInfo | undefined => {
  return DATABASE_SERVICES.find(db => db.id === id || db.connectionType === id)
}

export const getDatabaseByConnectionType = (connectionType: string): DatabaseInfo | undefined => {
  return DATABASE_SERVICES.find(db => db.connectionType === connectionType.toLowerCase())
}

