"""
Contact model for the Contact Discovery System.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum
from sqlalchemy.orm import relationship

from app.models.base import Base

class Contact(Base):
    """Contact model."""
    __tablename__ = 'contacts'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    title = Column(String(255))
    email = Column(String(255), nullable=False, unique=True)
    contact_type = Column(Enum('actual', 'generated', 'inferred', name='contact_type'), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Foreign keys
    organization_id = Column(Integer, ForeignKey('organizations.id'), nullable=False)
    
    # Relationships
    organization = relationship("Organization", back_populates="contacts")
    emails = relationship("Email", back_populates="contact")

    def __repr__(self):
        return f"<Contact(name='{self.name}', email='{self.email}', type='{self.contact_type}')>" 