from sqlalchemy import Column, String

from app.database import Base


class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(String(50), primary_key=True)
    name = Column(String(255), nullable=False)
