"""
Database models for the Dashboard to match the actual database schema.
These models are used only for the dashboard and queries.
"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Float, Table, create_engine, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

# Organization model that matches the actual database schema
class Organization(Base):
    __tablename__ = "organizations"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    org_type = Column(String(50), nullable=False)  # Not an enum, just a string
    subtype = Column(String(100))
    website = Column(String(255))
    address = Column(String(255))
    city = Column(String(100))
    state = Column(String(50))
    zip_code = Column(String(20))
    county = Column(String(100))
    phone = Column(String(50))
    description = Column(Text)
    source_url = Column(String(255))
    date_added = Column(DateTime)  # Use this instead of discovered_at
    last_updated = Column(DateTime)
    relevance_score = Column(Float)
    discovery_date = Column(DateTime)  # Use this instead of discovered_at
    
    # Relationships
    contacts = relationship("Contact", back_populates="organization")
    
    def __repr__(self):
        return f"<Organization(id={self.id}, name='{self.name}', type='{self.org_type}', state='{self.state}')>"

# Contact model that matches the actual database schema
class Contact(Base):
    __tablename__ = "contacts"
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Integer, ForeignKey("organizations.id"))
    first_name = Column(String(100))
    last_name = Column(String(100))
    job_title = Column(String(200))
    email = Column(String(255))
    date_added = Column(DateTime)  # Use this instead of created_at
    status = Column(String(50))  # Not an enum, just a string
    email_draft_created = Column(Boolean)
    email_draft_date = Column(DateTime)
    email_draft_id = Column(String(255))
    assigned_to = Column(String(255))
    contact_confidence_score = Column(Float)
    contact_relevance_score = Column(Float)
    
    # Relationships
    organization = relationship("Organization", back_populates="contacts")
    engagements = relationship("EmailEngagement", back_populates="contact")
    validations = relationship("ValidationReport", back_populates="contact")
    
    def __repr__(self):
        return f"<Contact(id={self.id}, name='{self.first_name} {self.last_name}', title='{self.job_title}')>"

# EmailEngagement model to work as a proxy for the missing Emails table
class EmailEngagement(Base):
    __tablename__ = "email_engagements"
    
    id = Column(Integer, primary_key=True)
    contact_id = Column(Integer, ForeignKey("contacts.id"), nullable=False)
    email_id = Column(String(255), nullable=False)  # Microsoft Graph email ID
    email_sent_date = Column(DateTime)
    email_opened = Column(Boolean, default=False)
    email_replied = Column(Boolean, default=False)
    
    # Relationships
    contact = relationship("Contact", back_populates="engagements")
    
    def __repr__(self):
        return f"<EmailEngagement(id={self.id}, contact_id={self.contact_id})>"

# Create a view/alias for the Emails table that maps to EmailEngagement
# This solves the "no such table: emails" error
class Email(EmailEngagement):
    __tablename__ = None  # No separate table, reuse EmailEngagement
    
    def __repr__(self):
        return f"<Email(id={self.id}, contact_id={self.contact_id})>"

# Any queries trying to reach 'emails' should use EmailEngagement instead
# SQLAlchemy will still look for a table called 'emails', but we're mapping the model
# This class allows us to handle queries looking for the Email model
# But query the email_engagements table under the hood
Email.__table__ = EmailEngagement.__table__

# ValidationReport model to store Gemini validation results
class ValidationReport(Base):
    __tablename__ = "validation_reports"
    
    id = Column(Integer, primary_key=True)
    contact_id = Column(Integer, ForeignKey("contacts.id"), nullable=False)
    validation_date = Column(DateTime, default=datetime.utcnow)
    auto_send_approved = Column(Boolean, default=False)
    contact_score = Column(Float)
    org_score = Column(Float)
    combined_score = Column(Float)  # Keep for backward compatibility but may be null
    threshold = Column(Float)       # Keep for backward compatibility but may be null
    org_threshold = Column(Float)   # New individual threshold for organizations
    contact_threshold = Column(Float) # New individual threshold for contacts
    reasons = Column(Text)  # JSON-serialized array of reasons
    model_used = Column(String(100))  # e.g. "gemini-2.0-flash"
    validation_type = Column(String(50))  # e.g. "email", "contact"
    
    # Relationships
    contact = relationship("Contact", back_populates="validations")
    
    def __repr__(self):
        return f"<ValidationReport(id={self.id}, contact_id={self.contact_id}, approved={self.auto_send_approved})>"

class ProcessSummary(Base):
    """Model for storing process run summaries"""
    __tablename__ = "process_summaries"

    id = Column(Integer, primary_key=True, index=True)
    process_type = Column(String(50), nullable=False)  # org_building, contact_building, email_sending
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String(20), default="running")  # running, completed, failed
    items_processed = Column(Integer, default=0)
    items_added = Column(Integer, default=0)
    details = Column(JSON, nullable=True)  # Store additional details like organization names, contacts, emails
    
    def __repr__(self):
        return f"<ProcessSummary(id={self.id}, process_type='{self.process_type}', status='{self.status}', items_added={self.items_added})>" 