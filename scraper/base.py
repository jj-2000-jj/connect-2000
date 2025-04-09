"""
Base scraper module for the GBL Data Contact Management System.
"""
import logging
import requests
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from app.database import crud
from app.utils.logger import get_logger

logger = get_logger(__name__)


class BaseScraper(ABC):
    """Base scraper class that all specific scrapers should inherit from."""

    def __init__(self, db_session: Session):
        """
        Initialize the base scraper.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch a web page and return a BeautifulSoup object.
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if the request fails
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, "html.parser")
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def save_organization(self, org_data: Dict[str, Any]) -> Optional[int]:
        """
        Save an organization to the database if it doesn't already exist.
        
        Args:
            org_data: Dictionary with organization data
            
        Returns:
            Organization ID if saved or found, None otherwise
        """
        try:
            existing_org = crud.get_organization_by_name_and_state(
                self.db_session, org_data["name"], org_data["state"]
            )
            
            if existing_org:
                logger.info(f"Organization already exists: {org_data['name']} in {org_data['state']}")
                return existing_org.id
            
            new_org = crud.create_organization(self.db_session, org_data)
            logger.info(f"Created new organization: {new_org.name} in {new_org.state}")
            return new_org.id
        except Exception as e:
            logger.error(f"Error saving organization {org_data.get('name', 'Unknown')}: {e}")
            return None
    
    def save_contact(self, contact_data: Dict[str, Any]) -> Optional[int]:
        """
        Save a contact to the database if it doesn't already exist.
        
        Args:
            contact_data: Dictionary with contact data
            
        Returns:
            Contact ID if saved or found, None otherwise
        """
        try:
            if contact_data.get("email"):
                existing_contact = crud.get_contact_by_email(self.db_session, contact_data["email"])
                if existing_contact:
                    logger.info(f"Contact already exists: {contact_data['email']}")
                    return existing_contact.id
            
            # Determine which sales person this contact should be assigned to
            from app.config import EMAIL_USERS
            org_type = self.db_session.query(crud.Organization).filter(
                crud.Organization.id == contact_data["organization_id"]
            ).first().org_type
            
            for email, org_types in EMAIL_USERS.items():
                if org_type in org_types:
                    contact_data["assigned_to"] = email
                    break
            
            new_contact = crud.create_contact(self.db_session, contact_data)
            logger.info(f"Created new contact: {new_contact.first_name} {new_contact.last_name} ({new_contact.job_title})")
            return new_contact.id
        except Exception as e:
            logger.error(f"Error saving contact: {e}")
            return None
    
    @abstractmethod
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape data from sources specific to this scraper.
        
        Returns:
            List of dictionaries with contact data
        """
        pass