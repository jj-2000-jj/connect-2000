"""
Relationship models for the Connect-Tron-2000 database.

This module defines all relationship tables that connect primary entities.
It serves as the single source of truth for all many-to-many relationships
and association tables throughout the application.
"""
from sqlalchemy import Column, Integer, ForeignKey, DateTime, Table
from app.database.models import Base

# Many-to-many relationship between Organizations and Keywords
org_keywords = Table(
    'org_keywords',
    Base.metadata,
    Column('organization_id', Integer, ForeignKey('organizations.id'), primary_key=True),
    Column('keyword_id', Integer, ForeignKey('keywords.id'), primary_key=True),
    extend_existing=True
)

# Relationship between DiscoveredURLs and Organizations
discovered_url_organizations = Table(
    'discovered_url_organizations',
    Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('url_id', Integer, ForeignKey('discovered_urls.id'), nullable=False),
    Column('organization_id', Integer, ForeignKey('organizations.id'), nullable=False),
    Column('date_added', DateTime, default=None),
    extend_existing=True
)

# Export all tables for easy importing
__all__ = [
    'org_keywords',
    'discovered_url_organizations'
] 