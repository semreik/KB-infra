"""Initial database schema.

Revision ID: 20250715_initial
Create Date: 2025-07-15 05:29:32.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '20250715_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create suppliers table
    op.create_table(
        'suppliers',
        sa.Column('id', sa.String, primary_key=True),
        sa.Column('name', sa.String, nullable=False),
        sa.Column('annual_revenue', sa.Float),
        sa.Column('employee_count', sa.Integer),
        sa.Column('founded_year', sa.Integer),
        sa.Column('hq_location', sa.String),
        sa.Column('industry', sa.String),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'))
    )
    
    # Create risk_scores table
    op.create_table(
        'risk_scores',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('supplier_id', sa.String, sa.ForeignKey('suppliers.id'), nullable=False),
        sa.Column('overall_score', sa.Float, nullable=False),
        sa.Column('financial_score', sa.Float),
        sa.Column('supply_score', sa.Float),
        sa.Column('reputation_score', sa.Float),
        sa.Column('quality_score', sa.Float),
        sa.Column('geo_score', sa.Float),
        sa.Column('evidence_hash', sa.String),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'))
    )
    
    # Create indices
    op.create_index('ix_suppliers_name', 'suppliers', ['name'])
    op.create_index('ix_risk_scores_supplier_id', 'risk_scores', ['supplier_id'])
    op.create_index('ix_risk_scores_created_at', 'risk_scores', ['created_at'])


def downgrade() -> None:
    op.drop_index('ix_risk_scores_created_at')
    op.drop_index('ix_risk_scores_supplier_id')
    op.drop_index('ix_suppliers_name')
    op.drop_table('risk_scores')
    op.drop_table('suppliers')
