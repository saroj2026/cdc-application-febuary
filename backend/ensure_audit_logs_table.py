"""Script to ensure audit_logs table exists with all required columns."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text, inspect
from ingestion.database import SessionLocal, engine, Base
from ingestion.database.models_db import AuditLogModel

def ensure_audit_logs_table():
    """Ensure audit_logs table exists with all required columns."""
    db = SessionLocal()
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        if 'audit_logs' not in tables:
            print("Creating audit_logs table...")
            Base.metadata.create_all(bind=engine, tables=[AuditLogModel.__table__])
            print("✓ audit_logs table created successfully!")
        else:
            print("✓ audit_logs table already exists")
            # Check if all required columns exist
            columns = {col['name']: col for col in inspector.get_columns('audit_logs')}
            required_columns = {
                'id', 'tenant_id', 'user_id', 'action', 'resource_type', 'resource_id',
                'old_value', 'new_value', 'ip_address', 'user_agent', 'created_at',
                'entity_type', 'entity_id', 'old_values', 'new_values', 'timestamp'
            }
            
            missing_columns = required_columns - set(columns.keys())
            if missing_columns:
                print(f"⚠ Missing columns: {missing_columns}")
                print("Adding missing columns...")
                
                # Add missing columns
                for col_name in missing_columns:
                    try:
                        if col_name == 'tenant_id':
                            db.execute(text("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(36)"))
                        elif col_name == 'resource_type':
                            db.execute(text("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS resource_type VARCHAR(50)"))
                        elif col_name == 'resource_id':
                            db.execute(text("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS resource_id VARCHAR(36)"))
                        elif col_name == 'old_value':
                            db.execute(text("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS old_value JSONB"))
                        elif col_name == 'new_value':
                            db.execute(text("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS new_value JSONB"))
                        elif col_name == 'ip_address':
                            db.execute(text("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS ip_address VARCHAR(45)"))
                        elif col_name == 'user_agent':
                            db.execute(text("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS user_agent TEXT"))
                        elif col_name == 'created_at':
                            db.execute(text("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
                        
                        print(f"  ✓ Added column: {col_name}")
                    except Exception as e:
                        print(f"  ✗ Failed to add column {col_name}: {e}")
                
                db.commit()
                print("✓ Missing columns added successfully!")
            else:
                print("✓ All required columns exist")
            
            # Create indexes if they don't exist
            indexes = [idx['name'] for idx in inspector.get_indexes('audit_logs')]
            if 'idx_audit_logs_tenant_id' not in indexes:
                try:
                    db.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_id ON audit_logs(tenant_id)"))
                    db.commit()
                    print("✓ Created index: idx_audit_logs_tenant_id")
                except Exception as e:
                    print(f"  ⚠ Could not create index: {e}")
            
            if 'idx_audit_logs_user_id' not in indexes:
                try:
                    db.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id)"))
                    db.commit()
                    print("✓ Created index: idx_audit_logs_user_id")
                except Exception as e:
                    print(f"  ⚠ Could not create index: {e}")
            
            if 'idx_audit_logs_action' not in indexes:
                try:
                    db.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action)"))
                    db.commit()
                    print("✓ Created index: idx_audit_logs_action")
                except Exception as e:
                    print(f"  ⚠ Could not create index: {e}")
            
            if 'idx_audit_logs_resource_type' not in indexes:
                try:
                    db.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_logs_resource_type ON audit_logs(resource_type)"))
                    db.commit()
                    print("✓ Created index: idx_audit_logs_resource_type")
                except Exception as e:
                    print(f"  ⚠ Could not create index: {e}")
            
            if 'idx_audit_logs_created_at' not in indexes:
                try:
                    db.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at)"))
                    db.commit()
                    print("✓ Created index: idx_audit_logs_created_at")
                except Exception as e:
                    print(f"  ⚠ Could not create index: {e}")
        
        print("\n✓ Audit logs table is ready!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        db.rollback()
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    ensure_audit_logs_table()

