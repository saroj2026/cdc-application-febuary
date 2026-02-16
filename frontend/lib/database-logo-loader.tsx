/**
 * Database Logo Loader
 * Automatically loads logo images from /public/assets/images/ directory
 * Falls back to SVG components if image not found
 */

"use client"

import React from "react"
import Image from "next/image"
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
  DefaultDatabaseLogo,
} from "./database-logos"

interface LogoProps {
  className?: string
  size?: number
  connectionType: string
  databaseName?: string
  displayName?: string
  databaseId?: string
}

// Map connection types to actual logo file names in /public/assets/images/
const LOGO_FILE_MAP: Record<string, { file: string; ext: string }> = {
  mysql: { file: "service-icon-sql", ext: "png" }, // MySQL - using SQL icon (no specific MySQL icon found)
  postgresql: { file: "service-icon-post", ext: "png" },
  sqlserver: { file: "service-icon-mssql", ext: "png" },
  mssql: { file: "service-icon-mssql", ext: "png" },
  oracle: { file: "service-icon-oracle", ext: "png" },
  mariadb: { file: "service-icon-mariadb", ext: "png" }, // MariaDB - using specific MariaDB icon
  mongodb: { file: "service-icon-mongodb", ext: "png" },
  snowflake: { file: "service-icon-snowflakes", ext: "png" },
  redshift: { file: "service-icon-redshift", ext: "png" },
  bigquery: { file: "service-icon-query", ext: "png" },
  databricks: { file: "service-icon-databrick", ext: "png" },
  cassandra: { file: "service-icon-cassandra", ext: "png" },
  couchbase: { file: "service-icon-couchbase", ext: "svg" },
  dynamodb: { file: "service-icon-dynamodb", ext: "png" },
  clickhouse: { file: "service-icon-clickhouse", ext: "png" },
  athena: { file: "service-icon-athena", ext: "png" },
  azuresql: { file: "service-icon-azuresql", ext: "png" },
  bigtable: { file: "service-icon-bigtable", ext: "png" },
  cockroach: { file: "service-icon-cockroach", ext: "png" },
  cockroachdb: { file: "service-icon-cockroach", ext: "png" },
  sqlite: { file: "service-icon-sqlite", ext: "png" },
  db2: { file: "service-icon-ibmdb2", ext: "png" },
  singlestore: { file: "service-icon-singlestore", ext: "png" },
  greenplum: { file: "service-icon-greenplum", ext: "png" },
  vertica: { file: "service-icon-vertica", ext: "png" },
  teradata: { file: "service-icon-generic", ext: "png" }, // Not found, using generic
  exasol: { file: "service-icon-exasol", ext: "png" },
  as400: { file: "service-icon-ibmdb2", ext: "png" }, // Using DB2 icon as IBM icon
  sap_hana: { file: "service-icon-sap-hana", ext: "png" },
  sap_erp: { file: "service-icon-sap-erp", ext: "png" },
  timescale: { file: "service-icon-timescale", ext: "png" },
  presto: { file: "service-icon-presto", ext: "png" },
  trino: { file: "service-icon-trino", ext: "png" },
  druid: { file: "service-icon-druid", ext: "png" },
  pinot: { file: "service-icon-pinot", ext: "png" },
  doris: { file: "service-icon-doris", ext: "png" },
  impala: { file: "service-icon-impala", ext: "png" },
  datalake: { file: "service-icon-datalake", ext: "png" },
  deltalake: { file: "service-icon-delta-lake", ext: "png" },
  iceberg: { file: "service-icon-iceberg", ext: "png" },
  hive: { file: "service-icon-hive", ext: "png" },
  glue: { file: "service-icon-glue", ext: "png" },
  unity_catalog: { file: "service-icon-unitycatalog", ext: "svg" },
  salesforce: { file: "service-icon-salesforce", ext: "png" },
  domo: { file: "service-icon-domo", ext: "png" },
  sas: { file: "service-icon-sas", ext: "svg" },
  custom: { file: "service-icon-generic", ext: "png" },
}

// Map connection types to SVG components (fallback)
const LOGO_COMPONENT_MAP: Record<string, React.ComponentType<{ className?: string; size?: number }>> = {
  mysql: MySQLLogo,
  postgresql: PostgreSQLLogo,
  sqlserver: SQLServerLogo,
  mssql: SQLServerLogo,
  oracle: OracleLogo,
  mariadb: MariaDBLogo,
  mongodb: MongoLogo,
  snowflake: SnowflakeLogo,
  redshift: RedshiftLogo,
  bigquery: BigQueryLogo,
  databricks: DatabricksLogo,
  cassandra: CassandraLogo,
  couchbase: CouchbaseLogo,
  dynamodb: DynamoDBLogo,
  clickhouse: ClickHouseLogo,
  athena: AthenaLogo,
  azuresql: AzureSQLLogo,
  bigtable: BigTableLogo,
  cockroach: CockroachLogo,
  cockroachdb: CockroachLogo,
}

/**
 * Database Logo Component
 * Tries to load image from /public/assets/images/ first
 * Falls back to SVG component if image not found
 */
export const DatabaseLogo: React.FC<LogoProps> = ({ 
  className = "", 
  size = 24, 
  connectionType,
  databaseName,
  displayName,
  databaseId
}) => {
  const normalizedType = (connectionType || '').toLowerCase().trim()
  
  // Special handling: MariaDB connections use connection_type='mysql'
  // Check databaseId first (most reliable), then displayName, then databaseName
  let logoKey = normalizedType
  if (normalizedType === 'mysql') {
    // Priority 1: Check databaseId (e.g., 'mariadb' from database selector)
    if (databaseId === 'mariadb') {
      logoKey = 'mariadb'
    }
    // Priority 2: Check displayName (e.g., 'MariaDB' from getDatabaseByConnectionType)
    else if (displayName?.toLowerCase().includes('maria')) {
      logoKey = 'mariadb'
    }
    // Priority 3: Check databaseName/connection name
    else {
      const dbNameLower = (databaseName || '').toLowerCase()
      if (dbNameLower.includes('maria') || dbNameLower.includes('mariadb')) {
        logoKey = 'mariadb'
      }
    }
  }
  
  const logoInfo = LOGO_FILE_MAP[logoKey]
  const FallbackIcon = LOGO_COMPONENT_MAP[logoKey] || DefaultDatabaseLogo

  const [imageError, setImageError] = React.useState(false)

  // If we have logo info, try to show image using Next.js Image component
  if (logoInfo && !imageError) {
    const imagePath = `/assets/images/${logoInfo.file}.${logoInfo.ext}`
    
    return (
      <div 
        className={`${className} relative flex items-center justify-center`} 
        style={{ width: size, height: size }}
      >
        <Image
          src={imagePath}
          alt={`${connectionType || 'database'} logo`}
          width={size}
          height={size}
          className="object-contain w-full h-full"
          style={{ objectFit: "contain" }}
          onError={() => setImageError(true)}
          unoptimized // Use unoptimized for external/public images
        />
      </div>
    )
  }

  // Fallback to SVG component
  return (
    <div className={className} style={{ width: size, height: size, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <FallbackIcon className={className} size={size} />
    </div>
  )
}

/**
 * Check if logo image exists
 */
export const hasLogoImage = (connectionType: string): boolean => {
  // This will be checked at runtime by the component
  // For now, we assume images might exist
  return true
}

