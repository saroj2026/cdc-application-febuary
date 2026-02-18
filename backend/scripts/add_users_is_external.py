"""
Add users.is_external column if missing. Run from project root:
  python -m backend.scripts.add_users_is_external
  or: cd backend && python scripts/add_users_is_external.py
"""
import os
import sys

# Allow running as script or as module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cdc_user:cdc_pass@72.61.233.209:5432/cdctest")

def main():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # Add is_external if not exists (PostgreSQL 9.5+)
        try:
            conn.execute(text("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS is_external BOOLEAN NOT NULL DEFAULT false
            """))
            conn.commit()
            print("Column users.is_external added (or already existed).")
        except ProgrammingError as e:
            if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                print("Column users.is_external already exists.")
            else:
                # Try without IF NOT EXISTS for older PostgreSQL
                try:
                    conn.execute(text("""
                        ALTER TABLE users
                        ADD COLUMN is_external BOOLEAN NOT NULL DEFAULT false
                    """))
                    conn.commit()
                    print("Column users.is_external added.")
                except ProgrammingError as e2:
                    if "already exists" in str(e2).lower():
                        print("Column users.is_external already exists.")
                    else:
                        print("Error:", e2)
                        sys.exit(1)
        except Exception as e:
            print("Error:", e)
            sys.exit(1)

if __name__ == "__main__":
    main()
