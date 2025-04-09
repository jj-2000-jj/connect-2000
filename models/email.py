"""
Email model for the Contact Discovery System.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Text
from sqlalchemy.orm import relationship

from app.models.base import Base

class Email(Base):
    """Email model."""
    __tablename__ = 'emails'

    id = Column(Integer, primary_key=True)
    subject = Column(String(255), nullable=False)
    body = Column(Text, nullable=False)
    to_email = Column(String(255), nullable=False)
    status = Column(Enum('email_draft', 'emailed', 'failed', name='email_status'), nullable=False)
    sent_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Foreign keys
    organization_id = Column(Integer, ForeignKey('organizations.id'), nullable=False)
    contact_id = Column(Integer, ForeignKey('contacts.id'), nullable=False)
    
    # Relationships
    organization = relationship("Organization", back_populates="emails")
    contact = relationship("Contact", back_populates="emails")

    def __repr__(self):
        return f"<Email(to='{self.to_email}', status='{self.status}', sent_at='{self.sent_at}')>" 