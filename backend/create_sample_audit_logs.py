"""Script to create sample audit logs for testing."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
import uuid
from ingestion.database import SessionLocal
from ingestion.database.models_db import AuditLogModel, UserModel

def create_sample_audit_logs():
    """Create sample audit logs for testing."""
    db = SessionLocal()
    try:
        # Get the first user (boss@gmail.com) if exists
        user = db.query(UserModel).filter(UserModel.email == "boss@gmail.com").first()
        user_id = str(user.id) if user else None
        
        sample_logs = [
            {
                "id": str(uuid.uuid4()),
                "tenant_id": None,
                "user_id": user_id,
                "action": "create_user",
                "resource_type": "user",
                "resource_id": str(uuid.uuid4()),
                "old_value": None,
                "new_value": {"email": "test@example.com", "role_name": "viewer"},
                "ip_address": "127.0.0.1",
                "user_agent": "Mozilla/5.0",
                "created_at": datetime.utcnow(),
                "entity_type": "user",
                "entity_id": str(uuid.uuid4()),
                "old_values": None,
                "new_values": {"email": "test@example.com", "role_name": "viewer"},
                "timestamp": datetime.utcnow()
            },
            {
                "id": str(uuid.uuid4()),
                "tenant_id": None,
                "user_id": user_id,
                "action": "update_user",
                "resource_type": "user",
                "resource_id": str(uuid.uuid4()),
                "old_value": {"role_name": "viewer"},
                "new_value": {"role_name": "data_engineer"},
                "ip_address": "127.0.0.1",
                "user_agent": "Mozilla/5.0",
                "created_at": datetime.utcnow() - timedelta(hours=1),
                "entity_type": "user",
                "entity_id": str(uuid.uuid4()),
                "old_values": {"role_name": "viewer"},
                "new_values": {"role_name": "data_engineer"},
                "timestamp": datetime.utcnow() - timedelta(hours=1)
            },
            {
                "id": str(uuid.uuid4()),
                "tenant_id": None,
                "user_id": user_id,
                "action": "create_pipeline",
                "resource_type": "pipeline",
                "resource_id": str(uuid.uuid4()),
                "old_value": None,
                "new_value": {"name": "Test Pipeline", "status": "inactive"},
                "ip_address": "127.0.0.1",
                "user_agent": "Mozilla/5.0",
                "created_at": datetime.utcnow() - timedelta(hours=2),
                "entity_type": "pipeline",
                "entity_id": str(uuid.uuid4()),
                "old_values": None,
                "new_values": {"name": "Test Pipeline", "status": "inactive"},
                "timestamp": datetime.utcnow() - timedelta(hours=2)
            },
            {
                "id": str(uuid.uuid4()),
                "tenant_id": None,
                "user_id": user_id,
                "action": "start_pipeline",
                "resource_type": "pipeline",
                "resource_id": str(uuid.uuid4()),
                "old_value": {"status": "inactive"},
                "new_value": {"status": "active"},
                "ip_address": "127.0.0.1",
                "user_agent": "Mozilla/5.0",
                "created_at": datetime.utcnow() - timedelta(hours=3),
                "entity_type": "pipeline",
                "entity_id": str(uuid.uuid4()),
                "old_values": {"status": "inactive"},
                "new_values": {"status": "active"},
                "timestamp": datetime.utcnow() - timedelta(hours=3)
            },
            {
                "id": str(uuid.uuid4()),
                "tenant_id": None,
                "user_id": user_id,
                "action": "delete_user",
                "resource_type": "user",
                "resource_id": str(uuid.uuid4()),
                "old_value": {"email": "deleted@example.com"},
                "new_value": None,
                "ip_address": "127.0.0.1",
                "user_agent": "Mozilla/5.0",
                "created_at": datetime.utcnow() - timedelta(hours=4),
                "entity_type": "user",
                "entity_id": str(uuid.uuid4()),
                "old_values": {"email": "deleted@example.com"},
                "new_values": None,
                "timestamp": datetime.utcnow() - timedelta(hours=4)
            }
        ]
        
        for log_data in sample_logs:
            log = AuditLogModel(**log_data)
            db.add(log)
        
        db.commit()
        print(f"✓ Created {len(sample_logs)} sample audit logs!")
        print("\nSample audit logs created:")
        print("  - create_user")
        print("  - update_user")
        print("  - create_pipeline")
        print("  - start_pipeline")
        print("  - delete_user")
        print("\nRefresh the audit logs page to see them!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        db.rollback()
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    from datetime import timedelta
    create_sample_audit_logs()

