"""
URL shortener for Connect-Tron-2000.

This module provides functionality to create and manage shortened URLs for tracking
email engagement through link clicks.
"""
import os
import uuid
import string
import random
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from urllib.parse import urlparse, urlencode, quote

from sqlalchemy.exc import IntegrityError

from app.database.models import get_db_session, ShortenedURL, EmailEngagement, Contact
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Constants
DEFAULT_SHORT_ID_LENGTH = 12
TRACKING_BASE_URL = os.environ.get("TRACKING_BASE_URL", "https://trk.connect-tron.com")

class URLShortener:
    """Service for creating and managing shortened URLs."""
    
    def __init__(self, db_session=None):
        """
        Initialize the URL shortener.
        
        Args:
            db_session: Database session (optional, will create if not provided)
        """
        self.db_session = db_session
        self.own_session = False
        
        if not self.db_session:
            self.db_session = get_db_session()
            self.own_session = True
        
    def __del__(self):
        """Clean up resources."""
        if self.own_session and self.db_session:
            self.db_session.close()
    
    def generate_short_id(self, length: int = DEFAULT_SHORT_ID_LENGTH) -> str:
        """
        Generate a random short ID.
        
        Args:
            length: Length of the short ID
            
        Returns:
            Random short ID
        """
        # Use a mix of letters and numbers, avoiding confusing characters
        chars = string.ascii_letters + string.digits
        chars = chars.replace('l', '').replace('1', '').replace('I', '').replace('O', '').replace('0', '')
        
        return ''.join(random.choice(chars) for _ in range(length))
    
    def create_shortened_url(self, original_url: str, contact_id: int, 
                          email_id: Optional[str] = None, 
                          link_text: Optional[str] = None,
                          link_position: Optional[str] = None,
                          link_type: Optional[str] = None) -> Optional[ShortenedURL]:
        """
        Create a shortened URL for tracking.
        
        Args:
            original_url: Original URL to shorten
            contact_id: Contact ID associated with this URL
            email_id: Email ID associated with this URL (optional)
            link_text: Text of the link in the email (optional)
            link_position: Position of the link in the email (optional)
            link_type: Type of link (optional)
            
        Returns:
            ShortenedURL object or None if creation failed
        """
        try:
            # Validate URL (basic check)
            parsed = urlparse(original_url)
            if not parsed.netloc:
                # Try adding https:// if missing
                original_url = f"https://{original_url}"
                parsed = urlparse(original_url)
                if not parsed.netloc:
                    logger.error(f"Invalid URL: {original_url}")
                    return None
            
            # Check if the contact exists
            contact = self.db_session.query(Contact).get(contact_id)
            if not contact:
                logger.error(f"Contact ID {contact_id} not found")
                return None
            
            # Generate a unique short ID
            max_attempts = 15  # Increased from 5 to 15 to reduce collision likelihood
            for attempt in range(max_attempts):
                try:
                    short_id = self.generate_short_id()
                    
                    # Create shortened URL - set both short_id and short_code to the same value
                    shortened_url = ShortenedURL(
                        original_url=original_url,
                        short_id=short_id,
                        short_code=short_id,  # Added this for compatibility
                        contact_id=contact_id,
                        email_id=email_id,
                        link_text=link_text,
                        link_position=link_position,
                        link_type=link_type,
                        created_date=datetime.utcnow(),
                        clicks=0
                    )
                    
                    self.db_session.add(shortened_url)
                    self.db_session.commit()
                    
                    logger.info(f"Created shortened URL: {shortened_url.tracking_url} -> {original_url}")
                    return shortened_url
                
                except IntegrityError as e:
                    # Short ID already exists, try again
                    logger.warning(f"Short ID collision on attempt {attempt+1}")
                    logger.debug(f"IntegrityError details: {str(e)}")
                    self.db_session.rollback()
                    continue
                
                except Exception as e:
                    logger.error(f"Error creating shortened URL: {e}", exc_info=True)
                    self.db_session.rollback()
                    return None
            
            logger.error(f"Failed to create shortened URL after {max_attempts} attempts")
            return None
            
        except Exception as e:
            logger.error(f"Error in create_shortened_url: {e}", exc_info=True)
            return None
    
    def get_tracking_url(self, short_id: str) -> str:
        """
        Get the tracking URL for a short ID.
        
        Args:
            short_id: Short ID
            
        Returns:
            Tracking URL
        """
        return f"{TRACKING_BASE_URL}/track/click/{short_id}"
    
    def get_by_short_id(self, short_id: str) -> Optional[ShortenedURL]:
        """
        Get a shortened URL by its short ID.
        
        Args:
            short_id: Short ID
            
        Returns:
            ShortenedURL object or None if not found
        """
        try:
            return self.db_session.query(ShortenedURL).filter_by(short_id=short_id).first()
        except Exception as e:
            logger.error(f"Error retrieving shortened URL: {e}")
            return None
    
    def get_by_email_id(self, email_id: str) -> List[ShortenedURL]:
        """
        Get all shortened URLs for an email.
        
        Args:
            email_id: Email ID
            
        Returns:
            List of ShortenedURL objects
        """
        try:
            return self.db_session.query(ShortenedURL).filter_by(email_id=email_id).all()
        except Exception as e:
            logger.error(f"Error retrieving shortened URLs for email: {e}")
            return []
    
    def get_by_contact_id(self, contact_id: int) -> List[ShortenedURL]:
        """
        Get all shortened URLs for a contact.
        
        Args:
            contact_id: Contact ID
            
        Returns:
            List of ShortenedURL objects
        """
        try:
            return self.db_session.query(ShortenedURL).filter_by(contact_id=contact_id).all()
        except Exception as e:
            logger.error(f"Error retrieving shortened URLs for contact: {e}")
            return []
    
    def process_email_links(self, email_body: str, contact_id: int, 
                           email_id: Optional[str] = None) -> str:
        """
        Process an email body to replace links with tracking links.
        
        Args:
            email_body: HTML email body
            contact_id: Contact ID associated with this email
            email_id: Email ID associated with this email (optional)
            
        Returns:
            Processed email body with tracking links
        """
        import re
        from bs4 import BeautifulSoup
        
        try:
            soup = BeautifulSoup(email_body, 'html.parser')
            
            # Find all links
            links = soup.find_all('a', href=True)
            
            for i, link in enumerate(links):
                original_url = link['href']
                link_text = link.get_text().strip()
                
                # Skip mailto links, anchors, etc.
                if original_url.startswith(('mailto:', 'tel:', '#', 'javascript:')):
                    continue
                
                # Determine link position (rough approximation)
                parent = link.parent
                if parent and parent.name == 'header' or i == 0:
                    link_position = 'header'
                elif parent and parent.name == 'footer' or i == len(links) - 1:
                    link_position = 'footer'
                else:
                    link_position = 'body'
                
                # Determine link type (rough approximation)
                link_type = 'general'
                if 'contact' in original_url.lower() or 'contact' in link_text.lower():
                    link_type = 'contact'
                elif 'download' in original_url.lower() or 'download' in link_text.lower():
                    link_type = 'download'
                elif 'demo' in original_url.lower() or 'demo' in link_text.lower():
                    link_type = 'demo'
                elif 'get started' in link_text.lower() or 'sign up' in link_text.lower():
                    link_type = 'cta'
                
                # Create shortened URL
                shortened_url = self.create_shortened_url(
                    original_url=original_url, 
                    contact_id=contact_id,
                    email_id=email_id,
                    link_text=link_text,
                    link_position=link_position,
                    link_type=link_type
                )
                
                if shortened_url:
                    # Replace the link with the tracking URL
                    link['href'] = shortened_url.tracking_url
            
            # Add tracking pixel for open tracking
            if email_id:
                pixel_url = f"{TRACKING_BASE_URL}/track/open/{email_id}"
                pixel_tag = soup.new_tag("img", src=pixel_url, 
                                      width="1", height="1", alt="", 
                                      style="display:none;")
                
                # Add the pixel to the end of the email body
                body_tag = soup.body
                if body_tag:
                    body_tag.append(pixel_tag)
                else:
                    # If no body tag, just append to the soup
                    soup.append(pixel_tag)
            
            # Return the processed HTML
            return str(soup)
            
        except Exception as e:
            logger.error(f"Error processing email links: {e}", exc_info=True)
            # Return original email body on error
            return email_body
    
    def get_click_stats(self, contact_id: Optional[int] = None, 
                      email_id: Optional[str] = None,
                      days_back: Optional[int] = None) -> Dict[str, Any]:
        """
        Get click statistics.
        
        Args:
            contact_id: Filter by contact ID (optional)
            email_id: Filter by email ID (optional)
            days_back: Filter by days back (optional)
            
        Returns:
            Dictionary with click statistics
        """
        try:
            query = self.db_session.query(ShortenedURL)
            
            # Apply filters
            if contact_id:
                query = query.filter(ShortenedURL.contact_id == contact_id)
            
            if email_id:
                query = query.filter(ShortenedURL.email_id == email_id)
            
            if days_back:
                cutoff_date = datetime.utcnow() - timedelta(days=days_back)
                query = query.filter(ShortenedURL.created_date >= cutoff_date)
            
            # Execute query
            urls = query.all()
            
            # Calculate statistics
            total_links = len(urls)
            total_clicks = sum(url.clicks for url in urls)
            
            # URL types breakdown
            url_types = {}
            for url in urls:
                url_type = url.link_type or 'general'
                if url_type not in url_types:
                    url_types[url_type] = {'count': 0, 'clicks': 0}
                
                url_types[url_type]['count'] += 1
                url_types[url_type]['clicks'] += url.clicks
            
            # Calculate average clicks per URL
            avg_clicks = total_clicks / total_links if total_links > 0 else 0
            
            # Return statistics
            return {
                'total_links': total_links,
                'total_clicks': total_clicks,
                'avg_clicks': avg_clicks,
                'url_types': url_types
            }
            
        except Exception as e:
            logger.error(f"Error getting click statistics: {e}", exc_info=True)
            return {
                'total_links': 0,
                'total_clicks': 0,
                'avg_clicks': 0,
                'url_types': {}
            } 