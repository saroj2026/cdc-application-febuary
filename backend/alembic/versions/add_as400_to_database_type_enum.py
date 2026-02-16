"""Add AS400 and IBM_I to DatabaseType enum

Revision ID: add_as400_enum
Revises: 
Create Date: 2026-01-05

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_as400_enum'
down_revision = '377dc4f79e07'  # Continue from S3 migration
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add 'as400' and 'ibm_i' to the databasetype enum
    # Note: In PostgreSQL, you cannot remove enum values, so downgrade is not possible
    op.execute("ALTER TYPE databasetype ADD VALUE IF NOT EXISTS 'as400'")
    op.execute("ALTER TYPE databasetype ADD VALUE IF NOT EXISTS 'ibm_i'")


def downgrade() -> None:
    # PostgreSQL does not support removing enum values
    # If needed, you would have to recreate the enum and update all columns
    # This is a destructive operation, so we leave it as a no-op
    pass


