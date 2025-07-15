from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB

from app.database import Base


class SupplierRiskProfile(Base):
    __tablename__ = "supplier_risk_profiles"

    id = Column(Integer, primary_key=True)
    supplier_id = Column(String(50), ForeignKey("suppliers.id"), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    risk_json = Column(JSONB, nullable=False)
