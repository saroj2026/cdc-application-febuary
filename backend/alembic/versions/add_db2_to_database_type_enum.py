"""Add db2 to DatabaseType enum.

Revision ID: add_db2_enum
Revises: add_oracle_enum
Create Date: 2026-02-09

"""
from typing import Sequence, Union
from alembic import op

revision: str = "add_db2_enum"
down_revision: Union[str, None] = "add_oracle_enum"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE databasetype ADD VALUE IF NOT EXISTS 'db2'")


def downgrade() -> None:
    pass
