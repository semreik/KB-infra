"""add supplier risk profiles

Revision ID: f11c1bc3379d
Revises: 20250715_initial
Create Date: 2025-07-15 06:03:15.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = 'f11c1bc3379d'
down_revision = '20250715_initial'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'supplier_risk_profiles',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('supplier_id', sa.String(length=50), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('risk_json', JSONB, nullable=False),
        sa.ForeignKeyConstraint(['supplier_id'], ['suppliers.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(
        'idx_risk_profiles_supplier_latest',
        'supplier_risk_profiles',
        ['supplier_id', 'timestamp'],
        unique=False
    )


def downgrade() -> None:
    op.drop_index('idx_risk_profiles_supplier_latest')
    op.drop_table('supplier_risk_profiles')
