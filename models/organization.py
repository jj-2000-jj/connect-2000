"""
Organization model for the Contact Discovery System.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Enum
from sqlalchemy.orm import relationship

from app.models.base import Base

class Organization(Base):
    """Organization model."""
    __tablename__ = 'organizations'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    org_type = Column(Enum('manufacturer', 'distributor', 'retailer', name='org_type'), nullable=False)
    state = Column(String(2), nullable=False)
    relevance_score = Column(Float, nullable=False, default=1.0)
    discovered_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Relationships
    contacts = relationship("Contact", back_populates="organization")
    emails = relationship("Email", back_populates="organization")

    def __repr__(self):
        return f"<Organization(name='{self.name}', type='{self.org_type}', state='{self.state}')>" 