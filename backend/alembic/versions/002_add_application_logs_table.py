"""Add application_logs table.

Revision ID: 002
Revises: 001
Create Date: 2025-01-XX

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'application_logs',
        sa.Column('id', sa.String(36), nullable=False),
        sa.Column('level', sa.String(20), nullable=False),
        sa.Column('logger', sa.String(255), nullable=True),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('module', sa.String(255), nullable=True),
        sa.Column('function', sa.String(255), nullable=True),
        sa.Column('line', sa.Integer(), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('extra', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for better query performance
    op.create_index('idx_log_level', 'application_logs', ['level'])
    op.create_index('idx_log_logger', 'application_logs', ['logger'])
    op.create_index('idx_log_timestamp', 'application_logs', ['timestamp'])
    op.create_index('idx_log_level_timestamp', 'application_logs', ['level', 'timestamp'])


def downgrade() -> None:
    op.drop_index('idx_log_level_timestamp', table_name='application_logs')
    op.drop_index('idx_log_timestamp', table_name='application_logs')
    op.drop_index('idx_log_logger', table_name='application_logs')
    op.drop_index('idx_log_level', table_name='application_logs')
    op.drop_table('application_logs')

