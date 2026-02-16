"""Run user management migration script."""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Get database URL from environment
database_url = os.getenv("DATABASE_URL", "postgresql://cdc_user:cdc_pass@72.61.233.209:5432/cdctest")

# Parse database URL
# Format: postgresql://user:password@host:port/database
url_parts = database_url.replace("postgresql://", "").split("/")
auth_part = url_parts[0]
database = url_parts[1] if len(url_parts) > 1 else "cdctest"

auth_parts = auth_part.split("@")
credentials = auth_parts[0].split(":")
host_port = auth_parts[1].split(":") if len(auth_parts) > 1 else ["72.61.233.209", "5432"]

user = credentials[0]
password = credentials[1] if len(credentials) > 1 else "cdc_pass"
host = host_port[0]
port = int(host_port[1]) if len(host_port) > 1 else 5432

print("=" * 70)
print("Running User Management Migration")
print("=" * 70)
print(f"Host: {host}:{port}")
print(f"Database: {database}")
print(f"User: {user}")
print()

try:
    # Connect to database
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("✅ Connected to database")
    
    # Read migration script (migrations folder is in backend directory)
    migration_path = os.path.join(os.path.dirname(__file__), "migrations", "add_user_management_features.sql")
    with open(migration_path, "r") as f:
        migration_sql = f.read()
    
    print("📄 Reading migration script...")
    
    # Execute migration
    print("🚀 Running migration...")
    cursor.execute(migration_sql)
    
    print("✅ Migration completed successfully!")
    print()
    
    # Verify changes
    print("Verifying changes...")
    cursor.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'users'
        ORDER BY ordinal_position;
    """)
    
    print("\nUsers table columns:")
    for row in cursor.fetchall():
        print(f"  - {row[0]} ({row[1]}, nullable: {row[2]})")
    
    # Check if user_sessions table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'user_sessions'
        );
    """)
    if cursor.fetchone()[0]:
        print("\n✅ user_sessions table created")
    else:
        print("\n⚠️  user_sessions table not found")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("Migration completed successfully!")
    print("=" * 70)
    
except Exception as e:
    print(f"\n❌ Error running migration: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

