"""
Validated Discovery Manager for the GBL Data Contact Management System.

This module extends the DiscoveryManager class to add Gemini AI validation
for organizations and contacts before they are added to the database.
"""
import time
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session
from bs4 import BeautifulSoup

from app.discovery.discovery_manager import DiscoveryManager
from app.database.validated_crud import ValidatedCrud
from app.database.models import Organization, Contact, DiscoveredURL
from app.utils.logger import get_logger

logger = get_logger(__name__)

class ValidatedDiscoveryManager(DiscoveryManager):
    """
    Enhanced discovery manager with Gemini AI validation to filter out problematic data.
    """
    
    def __init__(self, db_session: Session):
        """
        Initialize the validated discovery manager.
        
        Args:
            db_session: Database session
        """
        super().__init__(db_session)
        
        # Create validated CRUD operations
        self.validated_crud = ValidatedCrud(db_session)
        
        # Add validation metrics
        self.metrics.update({
            "validation": {
                "orgs_validated": 0,
                "orgs_rejected": 0,
                "orgs_improved": 0,
                "contacts_validated": 0,
                "contacts_rejected": 0
            }
        })
        
        logger.info("Initialized ValidatedDiscoveryManager with Gemini validation")
    
    def run_scheduled_discovery(self, max_orgs_per_run: int = 50) -> Dict[str, Any]:
        """
        Run a scheduled discovery process with validation.
        
        Args:
            max_orgs_per_run: Maximum number of organizations to discover
            
        Returns:
            Dictionary with discovery metrics and validation stats
        """
        logger.info(f"Starting validated discovery run (max_orgs={max_orgs_per_run})")
        
        # Run the standard discovery process using the parent method
        result = super().run_scheduled_discovery(max_orgs_per_run)
        
        # Add validation stats to the result
        validation_stats = self.validated_crud.get_validation_stats()
        result["validation"] = validation_stats
        
        # Log validation results
        logger.info(f"Organizations: validated={validation_stats['orgs_validated']}, "
                  f"rejected={validation_stats['orgs_rejected']}, "
                  f"improved={validation_stats['orgs_improved']}")
        logger.info(f"Contacts: validated={validation_stats['contacts_validated']}, "
                  f"rejected={validation_stats['contacts_rejected']}")
        
        # Calculate rejection rates
        if validation_stats['orgs_validated'] > 0:
            org_rejection_rate = validation_stats['orgs_rejected'] / validation_stats['orgs_validated'] * 100
            logger.info(f"Organization rejection rate: {org_rejection_rate:.1f}%")
            
        if validation_stats['contacts_validated'] > 0:
            contact_rejection_rate = validation_stats['contacts_rejected'] / validation_stats['contacts_validated'] * 100
            logger.info(f"Contact rejection rate: {contact_rejection_rate:.1f}%")
        
        return result
    
    def _extract_contacts_from_content(self, content: str, organization: Organization, profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract and validate contacts from content.
        
        This overrides the parent class method to add validation.
        
        Args:
            content: HTML content
            organization: Organization record
            profiles: List of role profiles
            
        Returns:
            List of validated contact dictionaries
        """
        # Call parent method to extract contacts
        contacts_data = super()._extract_contacts_from_content(content, organization, profiles)
        
        # Validate contacts in a batch for better performance
        logger.info(f"Validating {len(contacts_data)} contacts extracted from {organization.name}")
        valid_contacts = self.validated_crud.batch_validate_contacts(contacts_data)
        
        # Log validation results
        rejected_count = len(contacts_data) - len(valid_contacts)
        if rejected_count > 0:
            logger.info(f"Rejected {rejected_count} contacts from {organization.name} that failed validation")
        
        return valid_contacts
    
    def _discover_contacts_for_organization(self, organization: Organization, profiles: List[Dict[str, Any]]) -> List[Contact]:
        """
        Discover and validate contacts for an organization.
        
        This overrides the parent class method to add validation.
        
        Args:
            organization: Organization record
            profiles: List of role profiles
            
        Returns:
            List of discovered and validated contacts
        """
        # Use parent method to discover contacts
        discovered_contacts = super()._discover_contacts_for_organization(organization, profiles)
        
        # Update validation metrics for reporting
        validation_stats = self.validated_crud.get_validation_stats()
        self.metrics["validation"] = validation_stats
        
        return discovered_contacts
    
    def _save_discovered_url(self, url: str, title: str, description: str, source: str, category: str) -> DiscoveredURL:
        """
        Save a discovered URL to the database after validating its association with the organization.
        
        This overrides the parent class method to add validation.
        
        Args:
            url: URL string
            title: Page title
            description: Page description
            source: Source of discovery (search, crawler, etc.)
            category: Category/industry
            
        Returns:
            DiscoveredURL record
        """
        # Use parent method to save the URL
        url_record = super()._save_discovered_url(url, title, description, source, category)
        
        # If the URL is associated with an organization, validate that it's the right organization
        if url_record.organization_id and url_record.organization:
            org_data = {
                "name": url_record.organization.name,
                "website": url,
                "org_type": url_record.organization.org_type
            }
            
            # Validate that the URL matches the organization
            is_valid, _ = self.validated_crud.validator.validate_organization(org_data)
            
            if not is_valid:
                # Log the mismatch but don't delete the URL - just unlink from organization
                logger.warning(f"URL {url} may not be the official website for {url_record.organization.name}")
                
                # Unlink URL from organization
                url_record.organization_id = None
                self.db_session.commit()
        
        return url_record