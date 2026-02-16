/**
 * Original Database Brand Logos as SVG Components
 * Official brand logos matching Open Metadata style
 */

import React from "react"

interface LogoProps {
  className?: string
  size?: number
}

// MySQL Logo - Dolphin icon
export const MySQLLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M16.405 5.501c-.194-.16-.405-.296-.405-.296s-.5.5-1 1c-1.5 1.5-3 3-3 3s-1.5-1.5-3-3c-.5-.5-1-1-1-1s-.21.136-.405.296C2.647 6.854 1 9.274 1 12.065c0 2.791 1.647 5.211 3.595 6.564.194.16.405.296.405.296s.5-.5 1-1c1.5-1.5 3-3 3-3s1.5 1.5 3 3c.5.5 1 1 1 1s.21-.136.405-.296C16.353 17.276 18 14.856 18 12.065c0-2.791-1.647-5.211-3.595-6.564z" fill="#00758F"/>
    <path d="M16.405 5.501c-.194-.16-.405-.296-.405-.296s-.5.5-1 1c-1.5 1.5-3 3-3 3s-1.5-1.5-3-3c-.5-.5-1-1-1-1s-.21.136-.405.296C2.647 6.854 1 9.274 1 12.065c0 2.791 1.647 5.211 3.595 6.564.194.16.405.296.405.296s.5-.5 1-1c1.5-1.5 3-3 3-3s1.5 1.5 3 3c.5.5 1 1 1 1s.21-.136.405-.296C16.353 17.276 18 14.856 18 12.065c0-2.791-1.647-5.211-3.595-6.564z" fill="#F29111" opacity="0.8"/>
    <path d="M12 8c-2.21 0-4 1.79-4 4s1.79 4 4 4 4-1.79 4-4-1.79-4-4-4z" fill="#FFFFFF"/>
  </svg>
)

// PostgreSQL Logo - Elephant icon
export const PostgreSQLLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M23.559 4.684c-.377-1.772-1.431-3.205-2.817-4.133-.138-.093-.276-.185-.414-.276-.138-.092-.276-.185-.414-.276C18.639-.185 17.585-.738 16.531-.738c-.276 0-.552.046-.828.138-.276.093-.552.185-.828.277-.552.185-1.105.37-1.657.555-.552.185-1.105.37-1.657.555-.552.185-1.105.37-1.657.555-.276.093-.552.185-.828.277-.276.093-.552.138-.828.138-1.054 0-2.108.553-3.163 1.291-.138.092-.276.185-.414.276-.138.092-.276.185-.414.276C1.431 1.479.377 2.912 0 4.684c-.138.647-.138 1.295-.138 1.942 0 .647 0 1.295.138 1.942.377 1.772 1.431 3.205 2.817 4.133.138.093.276.185.414.277.138.092.276.185.414.276 1.054.739 2.108 1.291 3.163 1.291.276 0 .552-.046.828-.138.276-.092.552-.184.828-.277.552-.185 1.105-.37 1.657-.555.552-.185 1.105-.37 1.657-.555.552-.185 1.105-.37 1.657-.555.276-.093.552-.185.828-.277.276-.093.552-.138.828-.138 1.054 0 2.108-.552 3.163-1.291.138-.091.276-.184.414-.276.138-.092.276-.184.414-.277 1.386-.928 2.44-2.361 2.817-4.133.138-.647.138-1.295.138-1.942 0-.647 0-1.295-.138-1.942z" fill="#336791"/>
    <path d="M12 6c-3.314 0-6 2.686-6 6s2.686 6 6 6 6-2.686 6-6-2.686-6-6-6zm0 10c-2.209 0-4-1.791-4-4s1.791-4 4-4 4 1.791 4 4-1.791 4-4 4z" fill="#FFFFFF"/>
  </svg>
)

// SQL Server Logo - Red flag
export const SQLServerLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M2 2h20v20H2V2z" fill="#CC2927"/>
    <path d="M4 4h16v2H4V4zm0 4h16v2H4V8zm0 4h12v2H4v-2zm0 4h16v2H4v-2z" fill="#FFFFFF"/>
  </svg>
)

// Oracle Logo - Red O
export const OracleLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <circle cx="12" cy="12" r="10" fill="#F80000"/>
    <circle cx="12" cy="12" r="6" fill="#FFFFFF"/>
    <path d="M12 8c-2.21 0-4 1.79-4 4s1.79 4 4 4 4-1.79 4-4-1.79-4-4-4z" fill="#F80000"/>
  </svg>
)

// MongoDB Logo - Green leaf
export const MongoLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M17.193 9.555c-1.264-5.58-4.252-7.414-4.573-8.115-.28-.394-.53-.954-.735-1.44-.036.495-.055.685-.523 1.184-.723.782-4.533 3.682-4.533 8.238 0 4.288 3.703 7.11 4.533 7.78.366.293.54.375.54.63 0 .37-.226.59-.54.63-.36.05-.9.05-1.36.05-.72 0-1.35-.05-1.72-.05-.36-.04-.54-.26-.54-.63 0-.255.174-.337.54-.63.83-.67 4.533-3.492 4.533-7.78 0-4.556-3.81-7.456-4.533-8.238-.468-.499-.487-.689-.523-1.184-.205.486-.455 1.046-.735 1.44-.321.701-3.309 2.535-4.573 8.115C3.373 14.587 6.8 18.4 12 18.4s8.627-3.813 9.193-8.845z" fill="#47A248"/>
  </svg>
)

// Snowflake Logo - Snowflake icon
export const SnowflakeLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M12 2L13.5 6.5L18 5L16.5 9.5L21 8L19.5 12.5L24 11L21 15L19.5 12.5L15 14L16.5 9.5L12 11L13.5 6.5L9 8L10.5 3.5L6 5L7.5 9.5L3 8L4.5 12.5L0 11L3 15L4.5 12.5L9 14L7.5 9.5L12 11L10.5 6.5L15 8L13.5 3.5L18 5L16.5 9.5L21 8L19.5 12.5L24 11L21 15L19.5 12.5L15 14L16.5 9.5L12 11z" fill="#29B5E8"/>
    <path d="M12 6L10.5 10.5L6 9L7.5 13.5L3 12L4.5 16.5L0 15L3 19L4.5 16.5L9 18L7.5 13.5L12 15L10.5 10.5L15 12L13.5 7.5L18 9L16.5 13.5L21 12L19.5 16.5L24 15L21 19L19.5 16.5L15 18L16.5 13.5L12 15L13.5 10.5L18 12L16.5 7.5L21 9L19.5 13.5L24 12L21 16L19.5 13.5L15 15L16.5 10.5L12 12z" fill="#FFFFFF" opacity="0.3"/>
  </svg>
)

// Redshift Logo - Orange bars
export const RedshiftLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <rect x="2" y="4" width="4" height="16" fill="#FF9900"/>
    <rect x="8" y="6" width="4" height="14" fill="#FF9900"/>
    <rect x="14" y="8" width="4" height="12" fill="#FF9900"/>
    <rect x="20" y="10" width="2" height="10" fill="#FF9900"/>
  </svg>
)

// BigQuery Logo - Blue square with Q
export const BigQueryLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <rect x="2" y="2" width="20" height="20" rx="2" fill="#4285F4"/>
    <path d="M12 6c-3.314 0-6 2.686-6 6s2.686 6 6 6 6-2.686 6-6-2.686-6-6-6zm0 10c-2.209 0-4-1.791-4-4s1.791-4 4-4 4 1.791 4 4-1.791 4-4 4z" fill="#FFFFFF"/>
    <path d="M14 10l-2 2 2 2 2-2-2-2z" fill="#4285F4"/>
  </svg>
)

// Databricks Logo - Red bricks
export const DatabricksLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <rect x="2" y="4" width="6" height="4" fill="#FF3621"/>
    <rect x="10" y="4" width="6" height="4" fill="#FF3621"/>
    <rect x="18" y="4" width="4" height="4" fill="#FF3621"/>
    <rect x="4" y="10" width="6" height="4" fill="#FF3621"/>
    <rect x="12" y="10" width="6" height="4" fill="#FF3621"/>
    <rect x="2" y="16" width="6" height="4" fill="#FF3621"/>
    <rect x="10" y="16" width="6" height="4" fill="#FF3621"/>
    <rect x="18" y="16" width="4" height="4" fill="#FF3621"/>
  </svg>
)

// MariaDB Logo - Dolphin
export const MariaDBLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z" fill="#C49A3C"/>
    <path d="M8 10c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm8 0c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z" fill="#C49A3C"/>
  </svg>
)

// Cassandra Logo - Eye
export const CassandraLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <circle cx="12" cy="12" r="10" fill="#1287B1"/>
    <circle cx="12" cy="12" r="4" fill="#FFFFFF"/>
    <circle cx="12" cy="12" r="2" fill="#1287B1"/>
  </svg>
)

// Couchbase Logo - Red C
export const CouchbaseLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z" fill="#EA2328"/>
    <path d="M12 6c-3.31 0-6 2.69-6 6s2.69 6 6 6 6-2.69 6-6-2.69-6-6-6zm0 10c-2.21 0-4-1.79-4-4s1.79-4 4-4 4 1.79 4 4-1.79 4-4 4z" fill="#FFFFFF"/>
  </svg>
)

// DynamoDB Logo - Blue cylinder
export const DynamoDBLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <ellipse cx="12" cy="6" rx="8" ry="2" fill="#4053D6"/>
    <rect x="4" y="6" width="16" height="12" fill="#4053D6"/>
    <ellipse cx="12" cy="18" rx="8" ry="2" fill="#4053D6"/>
    <path d="M12 8v8M8 10v4M16 10v4" stroke="#FFFFFF" strokeWidth="1.5" fill="none"/>
  </svg>
)

// ClickHouse Logo - Yellow bars
export const ClickHouseLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <rect x="2" y="4" width="3" height="16" fill="#FFCC02"/>
    <rect x="7" y="6" width="3" height="14" fill="#FFCC02"/>
    <rect x="12" y="8" width="3" height="12" fill="#FFCC02"/>
    <rect x="17" y="10" width="3" height="10" fill="#FFCC02"/>
    <rect x="21" y="12" width="2" height="8" fill="#FFCC02"/>
  </svg>
)

// Athena Logo - Golden temple
export const AthenaLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M12 2L2 7v10l10 5 10-5V7l-10-5z" fill="#FF9900"/>
    <path d="M12 4L4 8v8l8 4 8-4V8l-8-4z" fill="#FFB84D"/>
    <rect x="10" y="10" width="4" height="6" fill="#FF9900"/>
  </svg>
)

// Azure SQL Logo
export const AzureSQLLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M12 2L2 7v10l10 5 10-5V7l-10-5z" fill="#0078D4"/>
    <path d="M12 4L4 8v8l8 4 8-4V8l-8-4z" fill="#40A5E6"/>
    <path d="M8 10h8v4H8v-4z" fill="#FFFFFF"/>
  </svg>
)

// BigTable Logo - Hexagonal table
export const BigTableLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M12 2l8 4.5v9l-8 4.5-8-4.5v-9L12 2z" fill="#4285F4"/>
    <path d="M12 4l6 3.5v7L12 18l-6-3.5v-7L12 4z" fill="#5C9EF8"/>
  </svg>
)

// CockroachDB Logo
export const CockroachLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <circle cx="12" cy="12" r="10" fill="#6933FF"/>
    <path d="M12 6c-3.314 0-6 2.686-6 6s2.686 6 6 6 6-2.686 6-6-2.686-6-6-6z" fill="#FFFFFF"/>
  </svg>
)

// Default Database Icon
// AWS S3 Logo - Official AWS S3 bucket design
export const S3Logo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    {/* AWS S3 Bucket - Official Design */}
    <rect x="4" y="6" width="16" height="12" rx="1" fill="#FF9900"/>
    <rect x="4" y="6" width="16" height="3" rx="1" fill="#FFB347"/>
    <rect x="4" y="15" width="16" height="3" rx="1" fill="#E68900"/>
    <path d="M6 9h12M6 12h12M6 15h12" stroke="#FFFFFF" strokeWidth="0.8" opacity="0.3"/>
    <circle cx="8" cy="10.5" r="1" fill="#FFFFFF" opacity="0.6"/>
    <circle cx="10.5" cy="10.5" r="1" fill="#FFFFFF" opacity="0.6"/>
    <circle cx="13" cy="10.5" r="1" fill="#FFFFFF" opacity="0.6"/>
  </svg>
)

export const DefaultDatabaseLogo: React.FC<LogoProps> = ({ className = "", size = 24 }) => (
  <svg viewBox="0 0 24 24" className={className} width={size} height={size} fill="none" xmlns="http://www.w3.org/2000/svg">
    <ellipse cx="12" cy="5" rx="8" ry="2" fill="#6B7280"/>
    <rect x="4" y="5" width="16" height="14" fill="#6B7280"/>
    <ellipse cx="12" cy="19" rx="8" ry="2" fill="#6B7280"/>
    <path d="M12 7v10M8 9v6M16 9v6" stroke="#FFFFFF" strokeWidth="1.5" fill="none"/>
  </svg>
)
