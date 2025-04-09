"""
Database models for the GBL Data Contact Management System.
"""
import datetime
import json
import threading
import contextlib
from enum import Enum
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Float, Table, create_engine, JSON, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
import time
import sqlalchemy.exc
import os
import sqlite3
import logging

# Set up logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variables for database connections
DB_ENGINE = None
DB_SESSION = None

# Determine the database path
DATABASE_PATH = os.path.join(os.getcwd(), "data", "contacts.db")

Base = declarative_base()

# Thread-local storage for database sessions
thread_local = threading.local()

class OrganizationType(str, Enum):
    ENGINEERING = "engineering"
    GOVERNMENT = "government"
    MUNICIPAL = "municipal"
    WATER = "water"
    UTILITY = "utility"
    TRANSPORTATION = "transportation"
    OIL_GAS = "oil_gas"
    AGRICULTURE = "agriculture"


class ContactStatus(str, Enum):
    NEW = "new"
    EMAIL_DRAFT = "email_draft"
    EMAILED = "emailed"
    RESPONDED = "responded"
    MEETING_SCHEDULED = "meeting_scheduled"
    NOT_INTERESTED = "not_interested"
    INVALID = "invalid"


# Many-to-many relationship between Organizations and Keywords
# This is defined in relationship_models.py - keeping reference here for backwards compatibility
org_keywords = Table(
    'org_keywords',
    Base.metadata,
    Column('organization_id', Integer, ForeignKey('organizations.id'), primary_key=True),
    Column('keyword_id', Integer, ForeignKey('keywords.id'), primary_key=True),
    extend_existing=True
)


class Keyword(Base):
    """Keyword model for organization classification."""
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True)
    word = Column(String(100), nullable=False, unique=True)
    category = Column(String(50))

    def __repr__(self):
        return f"<Keyword(id={self.id}, word='{self.word}', category='{self.category}')>"


class Organization(Base):
    """Organization model for storing company/agency information."""
    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    org_type = Column(String(50), nullable=False)
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
    date_added = Column(DateTime, default=datetime.datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    last_crawled = Column(DateTime)
    
    # Data quality and relevance metrics
    confidence_score = Column(Float, default=0.0)  # 0.0-1.0 confidence in classification
    relevance_score = Column(Float, default=0.0)   # 0.0-1.0 relevance to SCADA integration
    data_quality_score = Column(Float, default=0.0)  # 0.0-1.0 quality of organization data
    
    # Infrastructure and process indicators
    infrastructure_score = Column(Float, default=0.0)  # 0.0-1.0 score for infrastructure complexity
    process_complexity_score = Column(Float, default=0.0)  # 0.0-1.0 score for process system complexity
    automation_level = Column(Float, default=0.0)  # 0.0-1.0 current level of automation (lower is better for sales)
    is_competitor = Column(Boolean, default=False)  # Whether organization is a SCADA provider/competitor
    integration_opportunity_score = Column(Float, default=0.0)  # Overall score for integration potential
    extended_data = Column(JSON)  # Flexible JSON data for storing detailed analysis
    
    # Discovery data
    discovery_method = Column(String(50))  # search, directory, crawler, linkedin, etc.
    discovery_query = Column(String(255))
    discovery_date = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Contact discovery tracking
    contact_discovery_status = Column(String(50))  # 'completed', 'partial', 'attempted', or NULL
    last_contact_discovery = Column(DateTime)  # When contacts were last discovered
    
    # Do not contact flag
    do_not_contact = Column(Boolean, default=False)  # Whether this organization should not be contacted

    # Relationships
    contacts = relationship("Contact", back_populates="organization")
    keywords = relationship("Keyword", secondary=org_keywords)
    urls = relationship("DiscoveredURL", back_populates="organization")

    def __repr__(self):
        return f"<Organization(id={self.id}, name='{self.name}', type='{self.org_type}', state='{self.state}')>"


class Contact(Base):
    __tablename__ = "contacts"
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Integer, ForeignKey("organizations.id"))
    first_name = Column(String)
    last_name = Column(String)
    job_title = Column(String)
    email = Column(String)
    phone = Column(String)
    assigned_to = Column(String)
    email_draft_id = Column(String)
    email_sent_date = Column(DateTime)
    email_status = Column(String)
    contact_status = Column(String)
    discovery_method = Column(String)
    discovery_url = Column(String)
    date_added = Column(DateTime, default=datetime.datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    contact_confidence_score = Column(Float, default=0.0)  # 0.0-1.0
    contact_relevance_score = Column(Float, default=0.0)   # 0.0-10.0
    email_valid = Column(Boolean, default=False)
    notes = Column(String)
    
    # Add this new field to distinguish between real and generic contacts
    contact_type = Column(String, default="actual")  # Values: "actual", "generic", "inferred"
    
    # Status tracking
    status = Column(String(50), default=ContactStatus.NEW.value)
    status_date = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Email tracking
    email_sent = Column(Boolean, default=False)
    email_sent_date = Column(DateTime)
    email_draft_created = Column(Boolean, default=False)
    email_draft_date = Column(DateTime)
    email_draft_id = Column(String(255))  # Microsoft 365 draft email ID
    assigned_to = Column(String(255))  # Email of the sales person this contact is assigned to
    
    # Data quality and relevance metrics
    contact_confidence_score = Column(Float, default=0.0)  # 0.0-1.0 confidence in contact information
    contact_relevance_score = Column(Float, default=5.0)   # 1-10 scale for SCADA integration relevance
    email_valid = Column(Boolean, default=False)  # Whether email has been validated
    
    # Discovery data
    discovery_method = Column(String(50))  # website, linkedin, apollo, google_search, directory, inference, etc.
    discovery_url = Column(String(255))  # URL where contact was found
    
    # Relationships
    organization = relationship("Organization", back_populates="contacts")
    interactions = relationship("ContactInteraction", back_populates="contact")
    engagements = relationship("EmailEngagement", back_populates="contact", cascade="all, delete-orphan")
    engagement_score = relationship("ContactEngagementScore", back_populates="contact", uselist=False, cascade="all, delete-orphan")
    shortened_urls = relationship("ShortenedURL", back_populates="contact", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Contact(id={self.id}, name='{self.first_name} {self.last_name}', title='{self.job_title}')>"
        
    @property
    def org_type(self):
        """Get the organization type for this contact."""
        if hasattr(self, "organization") and self.organization:
            return self.organization.org_type
        return None
        
    @property
    def org_name(self):
        """Get the organization name for this contact."""
        if hasattr(self, "organization") and self.organization:
            return self.organization.name
        return None
        
    @property
    def org_state(self):
        """Get the organization state for this contact."""
        if hasattr(self, "organization") and self.organization:
            return self.organization.state
        return None


class ContactInteraction(Base):
    """Model to track interactions with contacts."""
    __tablename__ = "contact_interactions"

    id = Column(Integer, primary_key=True)
    contact_id = Column(Integer, ForeignKey("contacts.id"), nullable=False)
    interaction_type = Column(String(50), nullable=False)  # email_sent, response_received, meeting, etc.
    interaction_date = Column(DateTime, default=datetime.datetime.utcnow)
    notes = Column(Text)
    
    # If email interaction
    email_subject = Column(String(255))
    email_body = Column(Text)
    email_id = Column(String(255))  # Microsoft 365 email ID
    
    # Relationships
    contact = relationship("Contact", back_populates="interactions")

    def __repr__(self):
        return f"<ContactInteraction(id={self.id}, type='{self.interaction_type}', date='{self.interaction_date}')>"


class DiscoveredURL(Base):
    """Model to track URLs discovered during crawling."""
    __tablename__ = "discovered_urls"

    id = Column(Integer, primary_key=True, autoincrement=True)
    organization_id = Column(Integer, ForeignKey("organizations.id"), nullable=True)
    url = Column(String(255), nullable=False)
    page_type = Column(String(50))  # homepage, about, contact, team, etc.
    title = Column(String(255))
    description = Column(Text)
    last_crawled = Column(DateTime)
    crawl_depth = Column(Integer, default=0)
    contains_contact_info = Column(Boolean, default=False)
    contains_infrastructure = Column(Boolean, default=False)  # Contains infrastructure indicators
    industry_indicators = Column(JSON)  # Industry specific indicators 
    project_data = Column(JSON)  # Project information extracted from page
    priority_score = Column(Float, default=0.0)  # Priority for crawling (0.0-1.0)
    html_content = Column(Text)  # Stored HTML content of the page
    extracted_links = Column(Text)  # JSON array of extracted links
    
    # Relationships
    organization = relationship("Organization", back_populates="urls")

    def __repr__(self):
        return f"<DiscoveredURL(id={self.id}, url='{self.url}', organization_id={self.organization_id})>"


class SearchQuery(Base):
    """Model to track search queries and results."""
    __tablename__ = "search_queries"

    id = Column(Integer, primary_key=True)
    query = Column(String(255), nullable=False)
    category = Column(String(50))
    state = Column(String(50))
    search_engine = Column(String(50))  # google, bing, etc.
    execution_date = Column(DateTime, default=datetime.datetime.utcnow)
    results_count = Column(Integer, default=0)
    organizations_found = Column(Integer, default=0)  # Number of organizations extracted
    
    def __repr__(self):
        return f"<SearchQuery(id={self.id}, query='{self.query}', engine='{self.search_engine}')>"


class EmailTemplate(Base):
    """Model to store email templates."""
    __tablename__ = "email_templates"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    category = Column(String(50), nullable=False)
    subject = Column(String(255), nullable=False)
    body = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    
    def __repr__(self):
        return f"<EmailTemplate(id={self.id}, name='{self.name}', category='{self.category}')>"


class SystemMetric(Base):
    """Model to track system performance metrics."""
    __tablename__ = "system_metrics"

    id = Column(Integer, primary_key=True)
    metric_date = Column(DateTime, default=datetime.datetime.utcnow)
    urls_discovered = Column(Integer, default=0)
    urls_crawled = Column(Integer, default=0)
    organizations_discovered = Column(Integer, default=0)
    contacts_discovered = Column(Integer, default=0)
    emails_drafted = Column(Integer, default=0)
    search_queries_executed = Column(Integer, default=0)
    runtime_seconds = Column(Integer, default=0)
    errors_count = Column(Integer, default=0)
    
    def __repr__(self):
        return f"<SystemMetric(id={self.id}, date='{self.metric_date}')>"


class DiscoverySession(Base):
    """Model for tracking discovery sessions."""
    __tablename__ = "discovery_sessions"
    
    id = Column(Integer, primary_key=True)
    start_time = Column(DateTime, default=datetime.datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    status = Column(String(50), default="running")  # running, completed, error
    checkpoints = relationship("DiscoveryCheckpoint", back_populates="session")
    
    def __repr__(self):
        return f"<DiscoverySession(id={self.id}, status='{self.status}')>"


class DiscoveryCheckpoint(Base):
    """Model for storing discovery process checkpoints."""
    __tablename__ = "discovery_checkpoints"
    
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("discovery_sessions.id"))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    stage = Column(String(50), nullable=False)
    progress_data = Column(Text)  # JSON data
    
    session = relationship("DiscoverySession", back_populates="checkpoints")
    
    def __repr__(self):
        return f"<DiscoveryCheckpoint(id={self.id}, stage='{self.stage}')>"


class OrganizationTaxonomy(Base):
    """Model for storing organization taxonomy data."""
    __tablename__ = "organization_taxonomy"
    
    id = Column(Integer, primary_key=True)
    org_type = Column(String(50), nullable=False)
    field_type = Column(String(50), nullable=False)  # subtype, search_query, keyword, industry_association
    value = Column(String(255), nullable=False)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    
    def __repr__(self):
        return f"<OrganizationTaxonomy(id={self.id}, org_type='{self.org_type}', field_type='{self.field_type}')>"


class RoleProfile(Base):
    """Model for storing role/position profiles for organization types."""
    __tablename__ = "role_profiles"
    
    id = Column(Integer, primary_key=True)
    org_type = Column(String(50), nullable=False)
    role_title = Column(String(100), nullable=False)
    role_synonyms = Column(Text, nullable=False)  # JSON array of synonym titles
    relevance_score = Column(Integer, default=5)  # 1-10 scale
    decision_making_level = Column(Integer, default=5)  # 1-10 scale
    technical_knowledge_level = Column(Integer, default=5)  # 1-10 scale
    description = Column(Text)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    
    def __repr__(self):
        return f"<RoleProfile(id={self.id}, org_type='{self.org_type}', role='{self.role_title}')>"


# Old checkpoint model - keeping for compatibility but not using
# class DiscoveryCheckpoint(Base):
#     """Model for storing discovery process checkpoints."""
#     __tablename__ = "discovery_checkpoints"
#     
#     id = Column(Integer, primary_key=True)
#     checkpoint_id = Column(String(100), nullable=False)
#     timestamp = Column(DateTime, default=datetime.datetime.utcnow)
#     stage = Column(String(50), nullable=False)
#     data = Column(Text)  # JSON data
#
#     def __repr__(self):
#         return f"<DiscoveryCheckpoint(id={self.id}, checkpoint='{self.checkpoint_id}', stage='{self.stage}')>"


class EmailEngagement(Base):
    """Model for tracking email engagement metrics."""
    
    __tablename__ = "email_engagements"
    
    id = Column(Integer, primary_key=True)
    contact_id = Column(Integer, ForeignKey("contacts.id"), nullable=False)
    email_id = Column(String(255), nullable=False)  # Microsoft Graph email ID
    email_sent_date = Column(DateTime)
    email_opened = Column(Boolean, default=False)
    email_opened_date = Column(DateTime)
    email_opened_count = Column(Integer, default=0)
    email_replied = Column(Boolean, default=False)
    email_replied_date = Column(DateTime)
    clicked_link = Column(Boolean, default=False)
    clicked_link_date = Column(DateTime)
    clicked_link_count = Column(Integer, default=0)
    converted = Column(Boolean, default=False)  # Indicates a successful conversion
    conversion_date = Column(DateTime)
    conversion_type = Column(String(50))  # Type of conversion (meeting, call, etc.)
    last_tracked = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Relationships
    contact = relationship("Contact", back_populates="engagements")
    
    def __repr__(self):
        return f"<EmailEngagement(id={self.id}, contact_id={self.contact_id}, email_id={self.email_id})>"


class ContactEngagementScore(Base):
    """Model for storing contact engagement scores."""
    
    __tablename__ = "contact_engagement_scores"
    
    id = Column(Integer, primary_key=True)
    contact_id = Column(Integer, ForeignKey("contacts.id"), nullable=False, unique=True)
    engagement_score = Column(Float, default=0.0)  # 0-100 numeric score
    recency_score = Column(Float, default=0.0)  # How recently they engaged
    frequency_score = Column(Float, default=0.0)  # How frequently they engage
    depth_score = Column(Float, default=0.0)  # How deeply they engage (clicks vs just opens)
    conversion_score = Column(Float, default=0.0)  # How likely to convert to a meeting
    last_calculated = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Relationships
    contact = relationship("Contact", back_populates="engagement_score")
    
    def __repr__(self):
        return f"<ContactEngagementScore(id={self.id}, contact_id={self.contact_id}, score={self.engagement_score})>"


class ShortenedURL(Base):
    """Model to track shortened URLs for link tracking."""
    __tablename__ = "shortened_urls"
    
    id = Column(Integer, primary_key=True)
    contact_id = Column(Integer, ForeignKey("contacts.id"), nullable=False)
    email_id = Column(String(255))  # Microsoft 365 email ID for association
    
    # URL data
    original_url = Column(String(2048), nullable=False)
    short_id = Column(String(16), unique=True, nullable=False, index=True)
    short_code = Column(String(16), unique=True, nullable=False, index=True)  # Added for compatibility
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Tracking data
    clicks = Column(Integer, default=0)
    last_clicked = Column(DateTime)
    tracking_data = Column(JSON)  # Additional tracking data (devices, referrers, etc.)
    
    # Link metadata
    link_text = Column(String(255))  # Text of the link as it appeared in the email
    link_position = Column(String(50))  # Position in the email (header, body, footer)
    link_type = Column(String(50))  # Type of link (CTA, resource, website, etc.)
    
    # Relationships
    contact = relationship("Contact", back_populates="shortened_urls")
    
    def __repr__(self):
        return f"<ShortenedURL(id={self.id}, short_id='{self.short_id}', clicks={self.clicks})>"
    
    @property
    def tracking_url(self):
        """Get the tracking URL for this shortened URL."""
        base_url = os.environ.get("TRACKING_BASE_URL", "https://trk.connect-tron.com")
        return f"{base_url}/track/click/{self.short_id}"


class ProcessSummary(Base):
    """Model for storing process run summaries"""
    __tablename__ = "process_summaries"

    id = Column(Integer, primary_key=True, index=True)
    process_type = Column(String(50), nullable=False)  # org_building, contact_building, email_sending
    started_at = Column(DateTime, default=datetime.datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String(20), default="running")  # running, completed, failed
    items_processed = Column(Integer, default=0)
    items_added = Column(Integer, default=0)
    details = Column(Text, nullable=True)  # Store JSON details as text for SQLite compatibility
    
    def __repr__(self):
        return f"<ProcessSummary(id={self.id}, process_type='{self.process_type}', status='{self.status}', items_added={self.items_added})>"


def init_db():
    """Initialize the database connection and return session."""
    global DB_ENGINE, DB_SESSION
    
    # If we already have an engine, return it
    if DB_ENGINE is not None and DB_SESSION is not None:
        return DB_ENGINE, DB_SESSION
    
    # Get the database path from the base directory
    DB_PATH = os.path.join(os.getcwd(), "data", "contacts.db")
    if not os.path.exists(os.path.dirname(DB_PATH)):
        os.makedirs(os.path.dirname(DB_PATH))
    
    # Create a custom SQLite connection function that sets check_same_thread=False
    def _connect():
        return sqlite3.connect(
            DB_PATH,
            check_same_thread=False,  # Allow threads to share the connection
            timeout=60  # Long timeout for busy database
        )
    
    # Create the database engine with thread-safe settings
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        echo=False,  # Set to True for SQL query logging
        connect_args={
            "check_same_thread": False,  # Critical for thread safety
        },
        # Use more generous connection pooling settings
        poolclass=QueuePool,
        pool_size=5,  # Increased from 1 to 5
        max_overflow=10,  # Increased from 0 to 10
        pool_timeout=120,  # Increased from 60 to 120
        pool_pre_ping=True,  # Enable connection pooling with ping
        pool_recycle=3600,  # Recycle connections after an hour
        creator=_connect  # Use our custom connect function
    )
    
    # Set up event listeners for connection cleanup
    @event.listens_for(engine, 'connect')
    def receive_connect(dbapi_connection, connection_record):
        # Set isolation level for the connection
        dbapi_connection.isolation_level = "IMMEDIATE"
        # Set busy timeout
        dbapi_connection.execute("PRAGMA busy_timeout = 60000")
        # Set journal mode
        dbapi_connection.execute("PRAGMA journal_mode = WAL")
        # Set synchronous mode
        dbapi_connection.execute("PRAGMA synchronous = NORMAL")
        # Set cache size
        dbapi_connection.execute("PRAGMA cache_size = -2000")
        # Set temp store
        dbapi_connection.execute("PRAGMA temp_store = MEMORY")
    
    # Create all tables if they don't exist
    Base.metadata.create_all(engine)
    
    # Create a thread-safe session factory
    session_factory = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False  # Prevent objects from expiring after commit
    )
    
    # Create a scoped session that handles thread-local storage
    Session = scoped_session(session_factory)
    
    # Store the engine and session globally
    DB_ENGINE = engine
    DB_SESSION = Session
    
    return engine, Session


def get_db_session():
    """Get a SQLAlchemy session for the database."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    # Always use the actual application database (not test db)
    db_path = "./data/contacts.db"
    print(f"Connecting to database: {db_path}")
    
    # Create engine and session
    engine = create_engine(f'sqlite:///{db_path}')
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)
    
    return SessionLocal()


def close_connections():
    """Close all database connections and dispose of the engine."""
    global DB_ENGINE, DB_SESSION
    
    try:
        if DB_SESSION is not None:
            DB_SESSION.remove()
            logger.info("Database session cleared")
        
        if DB_ENGINE is not None:
            DB_ENGINE.dispose()
            logger.info("Database engine disposed")
        
        logger.info("All database connections closed")
        return True
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")
        return False


def get_db_engine():
    """Get the database engine, creating it if necessary."""
    global DB_ENGINE
    
    if DB_ENGINE is None:
        DB_PATH = str(DATABASE_PATH)
        if not os.path.exists(os.path.dirname(DB_PATH)):
            os.makedirs(os.path.dirname(DB_PATH))
        
        # Create the database engine with thread-safe settings
        engine = create_engine(
            f"sqlite:///{DB_PATH}",
            echo=False,  # Set to True for SQL query logging
            connect_args={
                "check_same_thread": False,  # Critical for thread safety
            },
            # Use more generous connection pooling settings
            poolclass=QueuePool,
            pool_size=5,  # Increased from 1 to 5
            max_overflow=10,  # Increased from 0 to 10
            pool_timeout=120,  # Increased from 60 to 120
            pool_pre_ping=True,  # Enable connection pooling with ping
            pool_recycle=3600,  # Recycle connections after an hour
            creator=_connect  # Use our custom connect function
        )
        
        DB_ENGINE = engine
    
    return DB_ENGINE
