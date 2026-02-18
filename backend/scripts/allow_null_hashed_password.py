"""
Make users.hashed_password nullable (for PENDING/invited users from CSV import).
Run from project root: py backend/scripts/allow_null_hashed_password.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cdc_user:cdc_pass@72.61.233.209:5432/cdctest")

def main():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        try:
            conn.execute(text("""
                ALTER TABLE users ALTER COLUMN hashed_password DROP NOT NULL
            """))
            conn.commit()
            print("Column users.hashed_password is now nullable.")
        except ProgrammingError as e:
            conn.rollback()
            print("Error:", e)
            sys.exit(1)

if __name__ == "__main__":
    main()
