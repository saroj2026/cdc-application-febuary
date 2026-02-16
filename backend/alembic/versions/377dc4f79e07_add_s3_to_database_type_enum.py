"""add_s3_to_database_type_enum

Revision ID: 377dc4f79e07
Revises: 709b073db8d0
Create Date: 2025-12-31 14:52:37.397010

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '377dc4f79e07'
down_revision: Union[str, None] = '709b073db8d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add 's3' to the databasetype enum
    # Note: In PostgreSQL, you cannot remove enum values, so downgrade is not possible
    op.execute("ALTER TYPE databasetype ADD VALUE IF NOT EXISTS 's3'")


def downgrade() -> None:
    # PostgreSQL does not support removing enum values
    # If needed, you would have to recreate the enum and update all columns
    # This is a destructive operation, so we leave it as a no-op
    pass
