"""Add Oracle to database_type enum.

Revision ID: add_oracle_enum
Revises: add_as400_to_database_type_enum
Create Date: 2026-01-08

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'add_oracle_enum'
down_revision: Union[str, None] = 'add_as400_enum'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add 'oracle' to the database_type enum
    # PostgreSQL enum modification requires creating a new enum, migrating data, and dropping old enum
    op.execute("""
        ALTER TYPE databasetype ADD VALUE IF NOT EXISTS 'oracle';
    """)


def downgrade() -> None:
    # Note: PostgreSQL doesn't support removing enum values directly
    # This would require recreating the enum without 'oracle' and migrating data
    # For safety, we'll leave it as a no-op
    # If you need to remove it, you'll need to:
    # 1. Create new enum without 'oracle'
    # 2. Update all rows to use new enum
    # 3. Drop old enum
    # 4. Rename new enum
    pass

