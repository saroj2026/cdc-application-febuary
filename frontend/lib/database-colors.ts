/**
 * Database Brand Colors
 * Official brand colors for each database
 */

export const DATABASE_COLORS: Record<string, {
  primary: string
  secondary: string
  gradient: string
  border: string
  text: string
}> = {
  mysql: {
    primary: '#00758F',
    secondary: '#F29111',
    gradient: 'from-[#00758F] to-[#F29111]',
    border: 'border-[#00758F]/30',
    text: 'text-[#00758F]'
  },
  postgresql: {
    primary: '#336791',
    secondary: '#4A90A4',
    gradient: 'from-[#336791] to-[#4A90A4]',
    border: 'border-[#336791]/30',
    text: 'text-[#336791]'
  },
  sqlserver: {
    primary: '#CC2927',
    secondary: '#E63946',
    gradient: 'from-[#CC2927] to-[#E63946]',
    border: 'border-[#CC2927]/30',
    text: 'text-[#CC2927]'
  },
  oracle: {
    primary: '#F80000',
    secondary: '#FF3333',
    gradient: 'from-[#F80000] to-[#FF3333]',
    border: 'border-[#F80000]/30',
    text: 'text-[#F80000]'
  },
  mongodb: {
    primary: '#47A248',
    secondary: '#68B741',
    gradient: 'from-[#47A248] to-[#68B741]',
    border: 'border-[#47A248]/30',
    text: 'text-[#47A248]'
  },
  snowflake: {
    primary: '#29B5E8',
    secondary: '#4FC3F7',
    gradient: 'from-[#29B5E8] to-[#4FC3F7]',
    border: 'border-[#29B5E8]/30',
    text: 'text-[#29B5E8]'
  },
  redshift: {
    primary: '#FF9900',
    secondary: '#FFB84D',
    gradient: 'from-[#FF9900] to-[#FFB84D]',
    border: 'border-[#FF9900]/30',
    text: 'text-[#FF9900]'
  },
  bigquery: {
    primary: '#4285F4',
    secondary: '#5C9EF8',
    gradient: 'from-[#4285F4] to-[#5C9EF8]',
    border: 'border-[#4285F4]/30',
    text: 'text-[#4285F4]'
  },
  databricks: {
    primary: '#FF3621',
    secondary: '#FF6B5A',
    gradient: 'from-[#FF3621] to-[#FF6B5A]',
    border: 'border-[#FF3621]/30',
    text: 'text-[#FF3621]'
  },
  mariadb: {
    primary: '#C49A3C',
    secondary: '#D4B15F',
    gradient: 'from-[#C49A3C] to-[#D4B15F]',
    border: 'border-[#C49A3C]/30',
    text: 'text-[#C49A3C]'
  },
  cassandra: {
    primary: '#1287B1',
    secondary: '#3BA3C7',
    gradient: 'from-[#1287B1] to-[#3BA3C7]',
    border: 'border-[#1287B1]/30',
    text: 'text-[#1287B1]'
  },
  couchbase: {
    primary: '#EA2328',
    secondary: '#F04C51',
    gradient: 'from-[#EA2328] to-[#F04C51]',
    border: 'border-[#EA2328]/30',
    text: 'text-[#EA2328]'
  },
  dynamodb: {
    primary: '#4053D6',
    secondary: '#5D6FE8',
    gradient: 'from-[#4053D6] to-[#5D6FE8]',
    border: 'border-[#4053D6]/30',
    text: 'text-[#4053D6]'
  },
  clickhouse: {
    primary: '#FFCC02',
    secondary: '#FFD633',
    gradient: 'from-[#FFCC02] to-[#FFD633]',
    border: 'border-[#FFCC02]/30',
    text: 'text-[#FFCC02]'
  },
  // Default colors for other databases
  default: {
    primary: '#6B7280',
    secondary: '#9CA3AF',
    gradient: 'from-[#6B7280] to-[#9CA3AF]',
    border: 'border-[#6B7280]/30',
    text: 'text-[#6B7280]'
  }
}

export const getDatabaseColor = (connectionType: string) => {
  const type = connectionType.toLowerCase()
  return DATABASE_COLORS[type] || DATABASE_COLORS.default
}

