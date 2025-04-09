"""
Discovery model for the Contact Discovery System.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Text
from sqlalchemy.orm import relationship

from app.models.base import Base

class Discovery(Base):
    """Discovery model."""
    __tablename__ = 'discoveries'

    id = Column(Integer, primary_key=True)
    source_url = Column(String(1024), nullable=False)
    source_type = Column(String(50), nullable=False)  # e.g., 'google', 'linkedin', 'website'
    relevance_score = Column(Float, nullable=False, default=1.0)
    raw_data = Column(Text)
    discovered_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Foreign keys
    organization_id = Column(Integer, ForeignKey('organizations.id'), nullable=False)
    
    # Relationships
    organization = relationship("Organization")

    def __repr__(self):
        return f"<Discovery(source='{self.source_type}', url='{self.source_url}', score={self.relevance_score})>" 