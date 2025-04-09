"""
Mock crawler for the GBL Data Contact Management System.

This module provides a simple mock crawler that always returns mock content instead
of actually crawling websites. This is useful for testing and when real crawling
is restricted or failing due to network issues.
"""

import datetime
import json
import random
import re
from urllib.parse import urlparse
from typing import Dict, List, Any, Tuple, Optional

from sqlalchemy.orm import Session
from app.database.models import DiscoveredURL
from app.utils.logger import get_logger

logger = get_logger(__name__)

class MockCrawler:
    """
    Mock crawler that always returns synthetic content.
    """
    
    def __init__(self, db_session: Session):
        """
        Initialize the mock crawler.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        self.content_cache = {}
    
    def crawl_url(self, url: str) -> Dict[str, Any]:
        """
        Create synthetic content and links for a URL.
        
        Args:
            url: URL to "crawl"
            
        Returns:
            Dictionary with html_content and links
        """
        logger.info(f"Mock crawling URL: {url}")
        
        # Check if we have content in database
        db_content = self._check_database(url)
        if db_content:
            logger.info(f"Using stored content for {url}")
            return db_content
        
        # Generate mock content and links
        html_content, links = self._generate_mock_content(url)
        
        # Store in database
        self._update_url_in_database(url, html_content, links)
        
        return {
            "html_content": html_content,
            "links": links
        }
    
    def _check_database(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Check if URL exists in database with content.
        
        Args:
            url: URL to check
            
        Returns:
            Dict with content and links or None
        """
        try:
            discovered_url = self.db_session.query(DiscoveredURL).filter(
                DiscoveredURL.url == url,
                DiscoveredURL.html_content.isnot(None)
            ).first()
            
            if discovered_url and discovered_url.html_content:
                links = []
                if discovered_url.extracted_links:
                    try:
                        links = json.loads(discovered_url.extracted_links)
                    except:
                        links = []
                
                return {
                    "html_content": discovered_url.html_content,
                    "links": links
                }
            
            return None
        except Exception as e:
            logger.error(f"Error checking database for URL {url}: {e}")
            return None
    
    def _update_url_in_database(self, url: str, html_content: str, links: List[str]) -> None:
        """
        Update or create URL in database.
        
        Args:
            url: URL to update
            html_content: HTML content
            links: List of links
        """
        try:
            discovered_url = self.db_session.query(DiscoveredURL).filter(
                DiscoveredURL.url == url
            ).first()
            
            if discovered_url:
                discovered_url.html_content = html_content
                discovered_url.extracted_links = json.dumps(links)
                discovered_url.last_crawled = datetime.datetime.utcnow()
                self.db_session.commit()
                logger.info(f"Updated database record for {url}")
            else:
                # Create new URL record
                new_url = DiscoveredURL(
                    url=url,
                    html_content=html_content,
                    extracted_links=json.dumps(links),
                    last_crawled=datetime.datetime.utcnow(),
                    page_type="synthetic",
                    priority_score=0.5
                )
                self.db_session.add(new_url)
                self.db_session.commit()
                logger.info(f"Created new database record for {url}")
        except Exception as e:
            logger.error(f"Error updating URL in database: {e}")
            self.db_session.rollback()
    
    def _generate_mock_content(self, url: str) -> Tuple[str, List[str]]:
        """
        Generate mock content and links based on URL.
        
        Args:
            url: URL to generate mock content for
            
        Returns:
            Tuple of (mock content, mock links)
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        # Generate organization name from domain
        org_name = domain.split('.')[0].replace('-', ' ').title()
        
        # Determine organization type and state from URL
        org_type = None
        state = None
        
        # Check domain for common patterns
        domain_lower = domain.lower()
        for keyword in ["water", "wastewater"]:
            if keyword in domain_lower:
                org_type = "water"
                break
        
        if not org_type:
            for keyword in ["engineering", "engineer", "design"]:
                if keyword in domain_lower:
                    org_type = "engineering"
                    break
                    
        if not org_type:
            for keyword in ["government", "agency", "dept"]:
                if keyword in domain_lower:
                    org_type = "government"
                    break
                    
        if not org_type:
            for keyword in ["utility", "power", "electric"]:
                if keyword in domain_lower:
                    org_type = "utility"
                    break
                    
        if not org_type:
            org_type = "municipal"  # Default
        
        # Extract state from domain
        for target_state in ["utah", "illinois", "arizona", "missouri", "newmexico", "nevada"]:
            if target_state in domain_lower or target_state in url.lower():
                state = target_state.title()
                # Fix New Mexico
                if state == "Newmexico":
                    state = "New Mexico"
                break
        
        if not state:
            state = "Utah"  # Default
        
        # Generate basic HTML content based on organization type
        mock_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{org_name} - {state} {org_type.title()} Services</title>
            <meta name="description" content="{org_name} provides {org_type} services in {state}.">
        </head>
        <body>
            <header>
                <h1>{org_name}</h1>
                <nav>
                    <ul>
                        <li><a href="{url}">Home</a></li>
                        <li><a href="{url}/about">About Us</a></li>
                        <li><a href="{url}/services">Services</a></li>
                        <li><a href="{url}/projects">Projects</a></li>
                        <li><a href="{url}/contact">Contact</a></li>
                    </ul>
                </nav>
            </header>
            
            <main>
                <section>
                    <h2>Welcome to {org_name}</h2>
                    <p>Serving the {state} area since 1985, {org_name} is a leading provider of {org_type} services.</p>
                    <p>Our mission is to deliver reliable and efficient solutions for our community.</p>
                </section>
                
                <section>
                    <h2>Our Services</h2>
                    <ul>
        """
        
        # Add service items based on organization type
        if org_type == "water":
            mock_content += """
                        <li>Water Treatment and Distribution</li>
                        <li>Wastewater Collection and Processing</li>
                        <li>Water Quality Monitoring and Testing</li>
                        <li>Regulatory Compliance Management</li>
                        <li>SCADA System Operation and Maintenance</li>
            """
        elif org_type == "engineering":
            mock_content += """
                        <li>Civil Engineering Design</li>
                        <li>Infrastructure Planning</li>
                        <li>Water System Engineering</li>
                        <li>Construction Management</li>
                        <li>Technical Consulting</li>
            """
        elif org_type == "government":
            mock_content += """
                        <li>Public Infrastructure Management</li>
                        <li>Regulatory Oversight</li>
                        <li>Environmental Protection</li>
                        <li>Water Resource Management</li>
                        <li>Public Works Administration</li>
            """
        elif org_type == "municipal":
            mock_content += """
                        <li>City Water Services</li>
                        <li>Public Works Management</li>
                        <li>Utilities Administration</li>
                        <li>Community Development</li>
                        <li>Infrastructure Maintenance</li>
            """
        elif org_type == "utility":
            mock_content += """
                        <li>Power Generation and Distribution</li>
                        <li>Utility Management</li>
                        <li>Infrastructure Maintenance</li>
                        <li>System Monitoring and Control</li>
                        <li>Customer Service</li>
            """
        
        # Continue with common content
        mock_content += f"""
                    </ul>
                </section>
                
                <section>
                    <h2>About {org_name}</h2>
                    <p>We are a leading provider of {org_type} services in {state}. Our team of experienced professionals is dedicated to delivering high-quality solutions to our clients.</p>
                    <p>Our organization was founded in 1985 and has grown to become a trusted partner for many communities and businesses across {state}.</p>
                </section>
                
                <section>
                    <h2>Contact Information</h2>
                    <p>Main Office: 123 Main Street, Capital City, {state}</p>
                    <p>Phone: (555) 123-4567</p>
                    <p>Email: info@{domain}</p>
                </section>
                
                <section>
                    <h2>Our Team</h2>
                    <div class="team-member">
                        <h3>John Smith</h3>
                        <p class="title">Director of Operations</p>
                        <p>Email: jsmith@{domain}</p>
                        <p>Phone: (555) 123-4568</p>
                    </div>
                    
                    <div class="team-member">
                        <h3>Sarah Johnson</h3>
                        <p class="title">Systems Manager</p>
                        <p>Email: sjohnson@{domain}</p>
                        <p>Phone: (555) 123-4569</p>
                    </div>
                    
                    <div class="team-member">
                        <h3>Michael Davis</h3>
                        <p class="title">Technical Supervisor</p>
                        <p>Email: mdavis@{domain}</p>
                        <p>Phone: (555) 123-4570</p>
                    </div>
                </section>
            </main>
            
            <footer>
                <p>&copy; 2025 {org_name}. All rights reserved.</p>
            </footer>
        </body>
        </html>
        """
        
        # Generate mock links for the domain
        mock_links = [
            f"{url}/about",
            f"{url}/services",
            f"{url}/projects",
            f"{url}/contact",
            f"{url}/team",
            f"{url}/facilities",
            f"{url}/locations",
            f"{url}/resources"
        ]
        
        return mock_content, mock_links