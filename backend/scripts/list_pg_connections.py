from __future__ import annotations

import json
from datetime import datetime

from ingestion.database.session import SessionLocal
from ingestion.database.models_db import ConnectionModel, DatabaseType


def main() -> None:
    db = SessionLocal()
    try:
        rows = (
            db.query(ConnectionModel)
            .filter(ConnectionModel.deleted_at.is_(None))
            .all()
        )
        result = []
        for r in rows:
            try:
                db_type = r.database_type.value if hasattr(r.database_type, "value") else str(r.database_type)
            except Exception:
                db_type = str(r.database_type)
            if db_type.lower() in ("postgresql", "postgres"):
                result.append(
                    {
                        "id": r.id,
                        "name": r.name,
                        "database_type": db_type,
                        "host": r.host,
                        "port": r.port,
                        "database": r.database,
                        "username": r.username,
                        "last_test_status": r.last_test_status,
                        "last_tested_at": r.last_tested_at.isoformat() if r.last_tested_at else None,
                        "created_at": r.created_at.isoformat() if isinstance(r.created_at, datetime) else None,
                    }
                )
        print(json.dumps(result, indent=2))
    finally:
        db.close()


if __name__ == "__main__":
    main()



