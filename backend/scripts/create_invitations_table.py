"""
Create invitations table (and roles, user_roles if missing) for user import/invite.
Run from project root:
  py backend/scripts/create_invitations_table.py
  or: python backend/scripts/create_invitations_table.py
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
        # 1) roles (needed for user_roles FK; create first)
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS roles (
                    id VARCHAR(36) PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roles_name ON roles(name)"))
            conn.commit()
            print("Table roles OK.")
        except Exception as e:
            conn.rollback()
            print("Roles:", e)

        # 2) user_roles (optional; references roles and users)
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_roles (
                    id VARCHAR(36) PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    role_id VARCHAR(36) NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
                    workspace_id VARCHAR(36),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_user_roles_user_id ON user_roles(user_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_user_roles_role_id ON user_roles(role_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_user_roles_user_workspace ON user_roles(user_id, workspace_id)"))
            conn.commit()
            print("Table user_roles OK.")
        except Exception as e:
            conn.rollback()
            print("User_roles:", e)

        # 3) invitations (required for CSV import)
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS invitations (
                    id VARCHAR(36) PRIMARY KEY,
                    email VARCHAR(255) NOT NULL,
                    invited_by VARCHAR(36) REFERENCES users(id) ON DELETE SET NULL,
                    token VARCHAR(255) UNIQUE NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                    role_name VARCHAR(50),
                    workspace_id VARCHAR(36),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    accepted_at TIMESTAMP
                )
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_invitations_email ON invitations(email)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_invitations_token ON invitations(token)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_invitations_status ON invitations(status)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_invitations_expires_at ON invitations(expires_at)"))
            conn.commit()
            print("Table invitations created (or already existed).")
        except ProgrammingError as e:
            conn.rollback()
            if "already exists" in str(e).lower():
                print("Table invitations already exists.")
            else:
                print("Error creating invitations:", e)
                sys.exit(1)
        except Exception as e:
            conn.rollback()
            print("Error:", e)
            sys.exit(1)

if __name__ == "__main__":
    main()
