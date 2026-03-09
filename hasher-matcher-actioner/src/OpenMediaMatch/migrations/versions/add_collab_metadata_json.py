"""Add collab_metadata_json column to bank_content

Revision ID: a1b2c3d4e5f6
Revises: 21cb8a3df884
Create Date: 2025-02-08 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

revision = "a1b2c3d4e5f6"
down_revision = "21cb8a3df884"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "bank_content",
        sa.Column("collab_metadata_json", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("bank_content", "collab_metadata_json")
