"""
Enhanced crawler for municipal websites to better extract contact information.

This module specifically targets patterns commonly found on government and municipal websites
for directory pages, staff listings, and other contact information sources.
"""
import re
import time
import logging
from typing import List, Dict, Any, Set, Optional, Tuple
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from app.database.models import Organization, Contact, DiscoveredURL
from app.utils.logger import get_logger
from app.website_validator import WebsiteValidator

logger = get_logger(__name__)

class MunicipalContactCrawler:
    """
    Specialized crawler for municipal and government websites to extract contact information.
    This crawler is designed to work with common patterns used by municipal websites
    including staff directories, contact pages, and department listings.
    """
    
    def __init__(self, db_session: Session):
        """
        Initialize the municipal contact crawler.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        # Initialize website validator for URL validation
        self.website_validator = WebsiteValidator()
        
        # Patterns specific to municipal websites
        self.municipal_contact_patterns = [
            # Common directory page patterns
            "/directory", "/staff-directory", "/employee-directory", "/personnel", 
            "/staff", "/our-staff", "/city-staff", "/town-staff", "/meet-our-staff",
            
            # Department and elected officials patterns
            "/departments", "/elected-officials", "/city-council", "/town-council",
            "/board-of-supervisors", "/commissioners", "/mayor", "/administration",
            
            # Common municipal features that often include contact info
            "/government", "/city-government", "/town-government", "/about-us",
            "/city-hall", "/town-hall", "/officials", "/leadership",
            
            # Specific file types and patterns
            "/directory.aspx", "/contacts.aspx", "/staff.aspx", 
            "StaffDirectory", "ContactUs", "MeetTheStaff",
            
            # Common URL parameters for directories
            "?did=", "?dept=", "?department=", "?ID=", "?dir="
        ]
        
        # Staff position keywords to prioritize
        self.priority_positions = [
            "manager", "director", "supervisor", "superintendent", "chief",
            "administrator", "coordinator", "mayor", "clerk", "official",
            "public works", "operations", "maintenance", "facilities",
            "water", "wastewater", "utility", "engineering"
        ]
    
    def discover_contacts(self, organization: Organization) -> List[Contact]:
        """
        Discover contacts for a municipal organization.
        
        Args:
            organization: Organization object
            
        Returns:
            List of discovered Contact objects
        """
        logger.info(f"Starting municipal contact discovery for: {organization.name}")
        
        # Get or validate the organization's website
        website_url = self._get_validated_website(organization)
        if not website_url:
            logger.warning(f"No valid website found for {organization.name}")
            return []
        
        # Update organization with validated website
        if website_url != organization.website:
            organization.website = website_url
            self.db_session.commit()
            logger.info(f"Updated website for {organization.name}: {website_url}")
        
        # Step 1: Discover potential contact pages
        contact_urls = self._discover_contact_pages(website_url, organization)
        logger.info(f"Found {len(contact_urls)} potential contact pages for {organization.name}")
        
        # Step 2: Crawl contact pages and extract contacts
        discovered_contacts = []
        for url in contact_urls:
            try:
                # Download and parse the page
                content = self._download_page(url)
                if not content:
                    continue
                
                # Extract contacts from the page
                page_contacts = self._extract_contacts_from_page(content, url, organization)
                if page_contacts:
                    logger.info(f"Found {len(page_contacts)} contacts on {url}")
                    discovered_contacts.extend(page_contacts)
                    
                # Small delay between requests
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error processing contact page {url}: {e}")
        
        # Step 3: Store contacts in database
        saved_contacts = self._save_contacts_to_database(discovered_contacts, organization)
        logger.info(f"Saved {len(saved_contacts)} new contacts for {organization.name}")
        
        return saved_contacts
    
    def _get_validated_website(self, organization: Organization) -> Optional[str]:
        """
        Get or validate the organization's website.
        
        Args:
            organization: Organization object
            
        Returns:
            Validated website URL or None
        """
        # If organization already has a website, validate it
        if organization.website:
            is_valid, confidence = self.website_validator.validate_org_website(
                organization.website, organization.name, organization.state
            )
            
            if is_valid:
                logger.info(f"Validated existing website for {organization.name}: {organization.website} (confidence: {confidence:.2f})")
                return organization.website
            else:
                logger.warning(f"Existing website for {organization.name} failed validation: {organization.website} (confidence: {confidence:.2f})")
        
        # Try to find website with Google Search
        try:
            # Import here to avoid circular imports
            from app.discovery.search.google_search import search_for_org_website
            
            website = search_for_org_website(organization.name, organization.state)
            if website:
                # Validate the found website
                is_valid, confidence = self.website_validator.validate_org_website(
                    website, organization.name, organization.state
                )
                
                if is_valid:
                    logger.info(f"Found and validated website for {organization.name}: {website} (confidence: {confidence:.2f})")
                    return website
                else:
                    logger.warning(f"Found website failed validation for {organization.name}: {website} (confidence: {confidence:.2f})")
            
        except Exception as e:
            logger.error(f"Error searching for website: {e}")
        
        return None
    
    def _discover_contact_pages(self, website_url: str, organization: Organization) -> List[str]:
        """
        Discover potential contact pages for a municipal organization.
        
        Args:
            website_url: Base website URL
            organization: Organization object
            
        Returns:
            List of contact page URLs
        """
        contact_urls = set()
        
        try:
            # Step 1: Download and parse the homepage
            content = self._download_page(website_url)
            if not content:
                return []
            
            soup = BeautifulSoup(content, "html.parser")
            
            # Step 2: Find links matching municipal contact patterns
            for link in soup.find_all("a", href=True):
                href = link["href"]
                
                # Skip invalid links
                if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                    continue
                
                # Convert relative URL to absolute
                full_url = urljoin(website_url, href)
                
                # Check if matches contact patterns
                path = urlparse(full_url).path.lower()
                query = urlparse(full_url).query.lower()
                
                # Check if URL contains any of our patterns
                for pattern in self.municipal_contact_patterns:
                    if pattern.lower() in path or pattern.lower() in query:
                        contact_urls.add(full_url)
                        break
                
                # Also check link text for contact-related keywords
                link_text = link.get_text().lower()
                contact_keywords = ["contact", "directory", "staff", "department", "officials", 
                                   "personnel", "employees", "team", "government"]
                if any(keyword in link_text for keyword in contact_keywords):
                    contact_urls.add(full_url)
            
            # Step 3: Generate common municipal contact URLs if none found
            if len(contact_urls) == 0:
                logger.info(f"No contact links found on homepage, generating common municipal patterns")
                
                # Parse base URL
                parsed_url = urlparse(website_url)
                base_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                
                # Generate common municipal contact page URLs
                for pattern in self.municipal_contact_patterns:
                    if pattern.startswith('?'):  # Query parameter pattern
                        # For query patterns, try adding to common pages
                        for base_path in ['/directory', '/staff', '/departments', '/contact', '/government']:
                            contact_urls.add(f"{base_domain}{base_path}{pattern}")
                    else:  # Path pattern
                        contact_urls.add(f"{base_domain}{pattern}")
                        
                        # For directories that might have ID parameters
                        if 'directory' in pattern or 'staff' in pattern:
                            for i in range(1, 5):  # Try a few common department IDs
                                contact_urls.add(f"{base_domain}{pattern}?id={i}")
                                contact_urls.add(f"{base_domain}{pattern}?did={i}")
                                contact_urls.add(f"{base_domain}{pattern}?dept={i}")
            
            # Return a sorted list of contact page URLs
            return sorted(list(contact_urls))
            
        except Exception as e:
            logger.error(f"Error discovering contact pages for {website_url}: {e}")
            return []
    
    def _download_page(self, url: str) -> Optional[str]:
        """
        Download a webpage.
        
        Args:
            url: URL to download
            
        Returns:
            HTML content or None if failed
        """
        try:
            # Check if URL already exists in database
            db_url = self.db_session.query(DiscoveredURL).filter(
                DiscoveredURL.url == url,
                DiscoveredURL.html_content.isnot(None)
            ).first()
            
            if db_url and db_url.html_content:
                logger.info(f"Using cached content for {url}")
                return db_url.html_content
            
            # Download the page
            logger.info(f"Downloading {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Store in database for future use
            html_content = response.text
            if db_url:
                db_url.html_content = html_content
                db_url.last_crawled = time.time()
            else:
                db_url = DiscoveredURL(
                    url=url,
                    html_content=html_content,
                    last_crawled=time.time(),
                    page_type="municipal_contact",
                    contains_contact_info=True
                )
                self.db_session.add(db_url)
            
            self.db_session.commit()
            return html_content
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None
    
    def _extract_contacts_from_page(self, content: str, url: str, organization: Organization) -> List[Dict[str, Any]]:
        """
        Extract contacts from a page using patterns common in municipal websites.
        
        Args:
            content: HTML content
            url: Page URL
            organization: Organization object
            
        Returns:
            List of contact dictionaries
        """
        contacts = []
        
        try:
            soup = BeautifulSoup(content, "html.parser")
            
            # Method 1: Extract from directory tables
            table_contacts = self._extract_from_directory_tables(soup, url, organization)
            contacts.extend(table_contacts)
            
            # Method 2: Extract from staff cards/listings
            card_contacts = self._extract_from_staff_cards(soup, url, organization)
            contacts.extend(card_contacts)
            
            # Method 3: Extract from department listings
            dept_contacts = self._extract_from_department_listings(soup, url, organization)
            contacts.extend(dept_contacts)
            
            # Method 4: Extract common municipal patterns
            pattern_contacts = self._extract_municipal_patterns(soup, url, organization)
            contacts.extend(pattern_contacts)
            
            # Remove duplicates based on email
            unique_contacts = {}
            for contact in contacts:
                email = contact.get("email", "").lower()
                if email:
                    if email not in unique_contacts:
                        unique_contacts[email] = contact
                else:
                    # If no email, use name as key
                    name_key = f"{contact.get('first_name', '')}-{contact.get('last_name', '')}"
                    if name_key and name_key not in unique_contacts:
                        unique_contacts[name_key] = contact
            
            return list(unique_contacts.values())
            
        except Exception as e:
            logger.error(f"Error extracting contacts from {url}: {e}")
            return []
    
    def _calculate_relevance_score(self, job_title: str, department: str = None) -> float:
        """
        Calculate a relevance score for a contact based on job title and department.
        
        Args:
            job_title: Job title string
            department: Department name (optional)
            
        Returns:
            Relevance score (0.0-10.0)
        """
        if not job_title and not department:
            return 5.0  # Default mid-level relevance
            
        title_lower = job_title.lower() if job_title else ""
        dept_lower = department.lower() if department else ""
        
        # Highest priority titles - likely key decision makers
        if any(role in title_lower for role in ["director", "manager", "chief", "superintendent", 
                                              "administrator", "mayor", "president"]):
            base_score = 9.0
        # High priority - likely involved in operations
        elif any(role in title_lower for role in ["supervisor", "foreman", "lead", "senior", 
                                                "coordinator", "engineer", "technician"]):
            base_score = 8.0
        # Staff involved in relevant areas
        elif any(role in title_lower for role in ["operator", "specialist", "analyst", 
                                                "officer", "clerk", "secretary"]):
            base_score = 7.0
        # General staff
        else:
            base_score = 6.0
        
        # Adjust based on department relevance
        dept_bonus = 0.0
        if dept_lower:
            if any(term in dept_lower for term in ["public works", "utilities", "water", "wastewater", 
                                                 "infrastructure", "engineering", "operations"]):
                dept_bonus = 1.0
            elif any(term in dept_lower for term in ["maintenance", "technical", "facility", 
                                                   "planning", "development", "management"]):
                dept_bonus = 0.5
                
        # Add bonus for specific high-value positions in title
        position_bonus = 0.0
        if any(term in title_lower for term in ["water", "utility", "infrastructure", "public works",
                                              "operations", "maintenance", "facilities"]):
            position_bonus = 1.0
        elif any(term in title_lower for term in ["planning", "technical", "technology", 
                                                "systems", "project", "compliance"]):
            position_bonus = 0.5
        
        # Calculate final score (cap at 10.0)
        final_score = min(10.0, base_score + dept_bonus + position_bonus)
        
        return final_score
    
    def _extract_from_directory_tables(self, soup: BeautifulSoup, url: str, organization: Organization) -> List[Dict[str, Any]]:
        """
        Extract contacts from directory tables often found on municipal websites.
        
        Args:
            soup: BeautifulSoup object
            url: Page URL
            organization: Organization object
            
        Returns:
            List of contact dictionaries
        """
        contacts = []
        
        # Find all tables that might contain contact information
        tables = soup.find_all("table")
        for table in tables:
            # Skip tables that don't look like staff directories
            if not self._looks_like_directory_table(table):
                continue
            
            # Get table headers to determine column purposes
            headers = []
            header_row = table.find("thead")
            if header_row:
                headers = [th.get_text().strip().lower() for th in header_row.find_all("th")]
            
            # If no proper headers, try the first row
            if not headers:
                first_row = table.find("tr")
                if first_row:
                    headers = [th.get_text().strip().lower() for th in first_row.find_all(["th", "td"])]
            
            # Determine column indices based on headers
            name_idx = next((i for i, h in enumerate(headers) if "name" in h), None)
            title_idx = next((i for i, h in enumerate(headers) if any(term in h for term in ["title", "position", "job", "role"])), None)
            dept_idx = next((i for i, h in enumerate(headers) if any(term in h for term in ["department", "dept", "division", "office"])), None)
            email_idx = next((i for i, h in enumerate(headers) if "email" in h), None)
            phone_idx = next((i for i, h in enumerate(headers) if any(term in h for term in ["phone", "telephone", "contact"])), None)
            
            # If we can't identify columns, skip this table
            if name_idx is None and not any(term in table.get_text().lower() for term in self.priority_positions):
                continue
            
            # Process each row in the table body, skipping header
            for row in table.find_all("tr")[1:] if headers else table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                
                # Skip rows with too few cells
                if len(cells) < 2:
                    continue
                
                # Extract data from cells based on identified indices
                name = cells[name_idx].get_text().strip() if name_idx is not None and name_idx < len(cells) else ""
                title = cells[title_idx].get_text().strip() if title_idx is not None and title_idx < len(cells) else ""
                department = cells[dept_idx].get_text().strip() if dept_idx is not None and dept_idx < len(cells) else ""
                
                # Extract email - first try cell text, then look for mailto links
                email = ""
                if email_idx is not None and email_idx < len(cells):
                    email_cell = cells[email_idx]
                    # First try mailto link
                    email_link = email_cell.find("a", href=lambda href: href and href.startswith("mailto:"))
                    if email_link and "href" in email_link.attrs:
                        email = email_link["href"].replace("mailto:", "").split("?")[0].strip()
                    else:
                        # Try regex for email in text
                        email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', email_cell.get_text())
                        if email_match:
                            email = email_match.group(0)
                
                # Extract phone number
                phone = ""
                if phone_idx is not None and phone_idx < len(cells):
                    phone_cell = cells[phone_idx]
                    # Try to find tel: link
                    phone_link = phone_cell.find("a", href=lambda href: href and href.startswith("tel:"))
                    if phone_link and "href" in phone_link.attrs:
                        phone = phone_link["href"].replace("tel:", "").strip()
                    else:
                        # Try regex for phone in text
                        phone_match = re.search(r'(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', phone_cell.get_text())
                        if phone_match:
                            phone = phone_match.group(0)
                
                # Skip entries without minimum required info
                if not name and not email:
                    continue
                
                # If we have email but not name, try to extract name from email
                if email and not name:
                    email_name = email.split('@')[0].replace('.', ' ').replace('_', ' ').title()
                    name = email_name
                
                # Split name into first and last
                name_parts = name.split()
                first_name = name_parts[0] if name_parts else ""
                last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                
                # Create contact dictionary
                contact = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "job_title": title,
                    "department": department,
                    "email": email,
                    "phone": phone,
                    "organization_id": organization.id,
                    "discovery_method": "municipal_directory_table",
                    "discovery_url": url,
                    "contact_confidence_score": 0.8,  # High confidence for structured data
                    "contact_relevance_score": self._calculate_relevance_score(title, department)
                }
                
                contacts.append(contact)
        
        return contacts
    
    def _looks_like_directory_table(self, table: BeautifulSoup) -> bool:
        """
        Determine if a table is likely a staff directory.
        
        Args:
            table: BeautifulSoup table element
            
        Returns:
            True if table looks like a directory
        """
        # Check if table has common directory headers
        headers_text = table.get_text().lower()
        directory_terms = ["name", "title", "position", "email", "phone", "contact", "department"]
        
        if any(term in headers_text for term in directory_terms):
            return True
            
        # Check for rows with person data patterns
        rows = table.find_all("tr")
        if len(rows) >= 3:  # Need enough rows to be a directory
            # Check for email patterns in cells
            for row in rows:
                cell_text = row.get_text().lower()
                if "@" in cell_text:
                    return True
                
                # Check for common title keywords
                if any(term in cell_text for term in self.priority_positions):
                    return True
        
        return False
    
    def _extract_from_staff_cards(self, soup: BeautifulSoup, url: str, organization: Organization) -> List[Dict[str, Any]]:
        """
        Extract contacts from staff cards commonly found on municipal websites.
        These are typically div elements with staff information.
        
        Args:
            soup: BeautifulSoup object
            url: Page URL
            organization: Organization object
            
        Returns:
            List of contact dictionaries
        """
        contacts = []
        
        # Look for common staff card containers
        card_containers = []
        
        # Find divs with common staff/team/directory class names
        for class_pattern in ['staff', 'team', 'directory', 'contact', 'person', 'member', 'employee', 'official']:
            found = soup.find_all('div', class_=lambda c: c and class_pattern in c.lower())
            card_containers.extend(found)
        
        # Add any cards found in article elements (common in WordPress sites)
        article_cards = soup.find_all('article', class_=lambda c: c and any(p in c.lower() for p in ['staff', 'team', 'person', 'member']))
        card_containers.extend(article_cards)
        
        # Add any cards found in list items within directory lists
        list_item_cards = []
        for ul in soup.find_all('ul', class_=lambda c: c and any(p in c.lower() for p in ['staff', 'team', 'directory', 'people'])):
            list_item_cards.extend(ul.find_all('li'))
        card_containers.extend(list_item_cards)
        
        # If we haven't found any containers, try more generic approaches
        if not card_containers:
            # Look for divs with certain patterns in their structure/content that suggest staff cards
            for div in soup.find_all('div'):
                # Check if this div contains a name-like heading and contact info
                heading = div.find(['h2', 'h3', 'h4', 'h5'])
                
                # Skip if no heading found or heading is too long (likely not a name)
                if not heading or len(heading.get_text().strip()) > 40:
                    continue
                
                # Check if div contains contact patterns
                div_text = div.get_text().lower()
                if '@' in div_text or 'phone' in div_text or any(title in div_text for title in self.priority_positions):
                    card_containers.append(div)
        
        # Process each card container
        for container in card_containers:
            try:
                # Extract name
                name_elem = container.find(['h2', 'h3', 'h4', 'h5', 'strong', 'b', 'div', 'span'],
                                       class_=lambda c: c and any(p in str(c).lower() for p in ['name', 'title', 'heading']))
                
                # If no specific name element found, try common patterns
                if not name_elem:
                    name_elem = container.find(['h2', 'h3', 'h4', 'h5', 'strong', 'b'])
                
                if not name_elem:
                    # Try looking for elements with certain class patterns
                    for elem in container.find_all(['div', 'span']):
                        if elem.get('class') and any('name' in c.lower() for c in elem.get('class')):
                            name_elem = elem
                            break
                
                # Skip if we couldn't find a name
                if not name_elem:
                    continue
                
                name = name_elem.get_text().strip()
                
                # Skip if name is too long (probably not a name)
                if len(name) > 40:
                    continue
                
                # Extract job title
                title_elem = container.find(['p', 'div', 'span', 'h6'],
                                       class_=lambda c: c and any(p in str(c).lower() for p in ['title', 'position', 'job', 'role']))
                
                # If no specific title element, look for elements near the name
                if not title_elem and name_elem:
                    # Try next sibling or next paragraph
                    next_elem = name_elem.find_next(['p', 'div', 'span'])
                    if next_elem and len(next_elem.get_text().strip()) < 50:  # Not too long
                        title_elem = next_elem
                
                job_title = title_elem.get_text().strip() if title_elem else ""
                
                # Extract department
                dept_elem = container.find(['p', 'div', 'span'],
                                      class_=lambda c: c and any(p in str(c).lower() for p in ['department', 'dept', 'division']))
                department = dept_elem.get_text().strip() if dept_elem else ""
                
                # Extract email
                email = ""
                email_elem = container.find('a', href=lambda h: h and h.startswith('mailto:'))
                if email_elem and 'href' in email_elem.attrs:
                    email = email_elem['href'].replace('mailto:', '').split('?')[0].strip()
                
                # If no mailto link, try regex for email in text
                if not email:
                    email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', container.get_text())
                    if email_match:
                        email = email_match.group(0)
                
                # Extract phone
                phone = ""
                phone_elem = container.find('a', href=lambda h: h and h.startswith('tel:'))
                if phone_elem and 'href' in phone_elem.attrs:
                    phone = phone_elem['href'].replace('tel:', '').strip()
                
                # If no tel: link, try regex for phone in text
                if not phone:
                    phone_match = re.search(r'(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', container.get_text())
                    if phone_match:
                        phone = phone_match.group(0)
                
                # Skip entries without minimum required info
                if not name and not email:
                    continue
                
                # If we have email but not name, try to extract name from email
                if email and not name:
                    email_name = email.split('@')[0].replace('.', ' ').replace('_', ' ').title()
                    name = email_name
                
                # Split name into first and last
                name_parts = name.split()
                first_name = name_parts[0] if name_parts else ""
                last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                
                # Create contact dictionary
                contact = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "job_title": job_title,
                    "department": department,
                    "email": email,
                    "phone": phone,
                    "organization_id": organization.id,
                    "discovery_method": "municipal_staff_card",
                    "discovery_url": url,
                    "contact_confidence_score": 0.85,  # High confidence for card data
                    "contact_relevance_score": self._calculate_relevance_score(job_title, department)
                }
                
                contacts.append(contact)
                
            except Exception as e:
                logger.error(f"Error extracting from staff card: {e}")
                continue
        
        return contacts
    
    def _extract_from_department_listings(self, soup: BeautifulSoup, url: str, organization: Organization) -> List[Dict[str, Any]]:
        """
        Extract contacts from department listings often found on municipal websites.
        
        Args:
            soup: BeautifulSoup object
            url: Page URL
            organization: Organization object
            
        Returns:
            List of contact dictionaries
        """
        contacts = []
        
        # Find department sections - common in municipal websites
        dept_sections = []
        
        # Find by department headings
        dept_headings = soup.find_all(['h2', 'h3', 'h4'], 
                                 string=lambda s: s and any(d in s.lower() for d in 
                                                           ['department', 'division', 'office']))
        
        for heading in dept_headings:
            # Get the department name from the heading
            dept_name = heading.get_text().strip()
            
            # Get the parent section containing department info
            parent_section = heading.parent
            
            # Skip if no clear parent
            if not parent_section:
                continue
                
            # Find contact details within this section
            # Look for names, usually in headings, strong, or emphasized text
            person_elems = parent_section.find_all(['h5', 'h6', 'strong', 'b', 'em'])
            
            for person in person_elems:
                person_text = person.get_text().strip()
                
                # Skip if not a person name (too long or too short)
                if len(person_text) > 40 or len(person_text) < 4:
                    continue
                
                # Check if there's a job title, often right after the name
                job_title = ""
                next_elem = person.next_sibling
                if isinstance(next_elem, str) and len(next_elem.strip()) > 0:
                    job_title = next_elem.strip()
                elif next_elem and next_elem.name in ['span', 'div', 'p']:
                    job_title = next_elem.get_text().strip()
                
                # If we couldn't find title after name, look in parent paragraph
                if not job_title and person.parent.name == 'p':
                    parent_text = person.parent.get_text().strip()
                    # Remove person name from text
                    job_part = parent_text.replace(person_text, '').strip()
                    if job_part:
                        job_title = job_part
                
                # Extract email near this person
                email = ""
                # Check for mailto links near this person element
                next_links = person.find_next_siblings('a', href=lambda h: h and h.startswith('mailto:'), limit=2)
                prev_links = person.find_previous_siblings('a', href=lambda h: h and h.startswith('mailto:'), limit=1)
                
                for link in next_links + prev_links:
                    if 'href' in link.attrs:
                        email = link['href'].replace('mailto:', '').split('?')[0].strip()
                        break
                
                # If no mailto link found, try regex in surrounding text
                if not email:
                    # Look in parent or nearby paragraphs
                    nearby = person.parent.get_text() if person.parent else ""
                    email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', nearby)
                    if email_match:
                        email = email_match.group(0)
                
                # Extract phone number
                phone = ""
                next_phone = person.find_next('a', href=lambda h: h and h.startswith('tel:'))
                if next_phone and 'href' in next_phone.attrs:
                    phone = next_phone['href'].replace('tel:', '').strip()
                
                # If no tel link, try regex
                if not phone:
                    nearby = person.parent.get_text() if person.parent else ""
                    phone_match = re.search(r'(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', nearby)
                    if phone_match:
                        phone = phone_match.group(0)
                
                # Skip if we don't have enough information
                if not person_text and not email:
                    continue
                
                # Split name into first and last
                name_parts = person_text.split()
                first_name = name_parts[0] if name_parts else ""
                last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                
                contact = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "job_title": job_title,
                    "department": dept_name,
                    "email": email,
                    "phone": phone,
                    "organization_id": organization.id,
                    "discovery_method": "municipal_department_listing",
                    "discovery_url": url,
                    "contact_confidence_score": 0.75,  # Medium-high confidence
                    "contact_relevance_score": self._calculate_relevance_score(job_title, dept_name)
                }
                
                contacts.append(contact)
        
        return contacts
    
    def _extract_municipal_patterns(self, soup: BeautifulSoup, url: str, organization: Organization) -> List[Dict[str, Any]]:
        """
        Extract contacts using patterns specific to municipal websites.
        
        Args:
            soup: BeautifulSoup object
            url: Page URL
            organization: Organization object
            
        Returns:
            List of contact dictionaries
        """
        contacts = []
        
        # Method 1: Extract from definition lists (common in municipal sites)
        dt_elements = soup.find_all('dt')
        for dt in dt_elements:
            dt_text = dt.get_text().strip()
            
            # Skip if not likely a person name
            if len(dt_text) > 40 or len(dt_text) < 4:
                continue
                
            # Get the corresponding dd element
            dd = dt.find_next('dd')
            if not dd:
                continue
                
            dd_text = dd.get_text().strip()
            
            # Extract job title - often in the dd text
            job_title = dd_text
            
            # If dd text has multiple lines, first line might be title
            if '\\n' in dd_text or '\\r' in dd_text:
                job_title = dd_text.split('\\n')[0].split('\\r')[0].strip()
            
            # Extract email
            email = ""
            email_link = dd.find('a', href=lambda h: h and h.startswith('mailto:'))
            if email_link and 'href' in email_link.attrs:
                email = email_link['href'].replace('mailto:', '').split('?')[0].strip()
                
            # If no mailto link, try regex
            if not email:
                email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', dd_text)
                if email_match:
                    email = email_match.group(0)
            
            # Extract phone
            phone = ""
            phone_link = dd.find('a', href=lambda h: h and h.startswith('tel:'))
            if phone_link and 'href' in phone_link.attrs:
                phone = phone_link['href'].replace('tel:', '').strip()
                
            # If no tel link, try regex
            if not phone:
                phone_match = re.search(r'(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', dd_text)
                if phone_match:
                    phone = phone_match.group(0)
            
            # Split name into first and last
            name_parts = dt_text.split()
            first_name = name_parts[0] if name_parts else ""
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
            
            # Create contact
            contact = {
                "first_name": first_name,
                "last_name": last_name,
                "job_title": job_title,
                "department": "",  # Often not specified in DL format
                "email": email,
                "phone": phone,
                "organization_id": organization.id,
                "discovery_method": "municipal_definition_list",
                "discovery_url": url,
                "contact_confidence_score": 0.7,  # Medium confidence
                "contact_relevance_score": self._calculate_relevance_score(job_title)
            }
            
            contacts.append(contact)
        
        # Method 2: Extract from ASP.NET GridView controls (common in .aspx pages)
        grid_rows = soup.find_all('tr', class_=lambda c: c and 'gridrow' in c.lower())
        if grid_rows:
            # Try to determine column meanings from all cells
            all_cell_text = [cell.get_text().lower() for row in grid_rows for cell in row.find_all('td')]
            has_emails = any('@' in cell for cell in all_cell_text)
            
            for row in grid_rows:
                cells = row.find_all('td')
                
                # Skip rows with too few cells
                if len(cells) < 2:
                    continue
                
                # With GridView, we often have to guess column meanings
                if has_emails:
                    # Likely columns: Name, Title, Department, Email, Phone
                    # Try to identify which cell contains what
                    row_data = {}
                    
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text().strip()
                        
                        # Check for email
                        if '@' in cell_text:
                            email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', cell_text)
                            if email_match:
                                row_data['email'] = email_match.group(0)
                                continue
                        
                        # Check for phone
                        phone_match = re.search(r'(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', cell_text)
                        if phone_match:
                            row_data['phone'] = phone_match.group(0)
                            continue
                        
                        # First cell is usually name
                        if i == 0 and not row_data.get('name'):
                            row_data['name'] = cell_text
                            continue
                            
                        # Second cell is often title
                        if i == 1 and not row_data.get('title'):
                            row_data['title'] = cell_text
                            continue
                            
                        # Third cell might be department
                        if i == 2 and not row_data.get('department'):
                            row_data['department'] = cell_text
                    
                    # Skip if we don't have enough data
                    if not row_data.get('name') and not row_data.get('email'):
                        continue
                    
                    # Process the extracted data
                    name = row_data.get('name', '')
                    name_parts = name.split()
                    first_name = name_parts[0] if name_parts else ""
                    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                    
                    contact = {
                        "first_name": first_name,
                        "last_name": last_name,
                        "job_title": row_data.get('title', ''),
                        "department": row_data.get('department', ''),
                        "email": row_data.get('email', ''),
                        "phone": row_data.get('phone', ''),
                        "organization_id": organization.id,
                        "discovery_method": "municipal_gridview",
                        "discovery_url": url,
                        "contact_confidence_score": 0.75,  # Medium-high confidence for structured grids
                        "contact_relevance_score": self._calculate_relevance_score(row_data.get('title', ''), row_data.get('department', ''))
                    }
                    
                    contacts.append(contact)
        
        return contacts
    
    def _save_contacts_to_database(self, contact_dicts: List[Dict[str, Any]], organization: Organization) -> List[Contact]:
        """
        Save contacts to the database.
        
        Args:
            contact_dicts: List of contact dictionaries
            organization: Organization object
            
        Returns:
            List of saved Contact objects
        """
        saved_contacts = []
        
        for contact_data in contact_dicts:
            try:
                # Skip contacts without minimum required data
                if not contact_data.get("first_name") or not contact_data.get("organization_id"):
                    continue
                
                # Check if contact already exists by email
                existing_contact = None
                if contact_data.get("email"):
                    existing_contact = self.db_session.query(Contact).filter(
                        Contact.email == contact_data["email"],
                        Contact.organization_id == organization.id
                    ).first()
                
                # If no email match, try name match
                if not existing_contact:
                    existing_contact = self.db_session.query(Contact).filter(
                        Contact.first_name == contact_data["first_name"],
                        Contact.last_name == contact_data.get("last_name", ""),
                        Contact.organization_id == organization.id
                    ).first()
                
                # If contact exists, update fields if needed
                if existing_contact:
                    # Update email if we didn't have one
                    if not existing_contact.email and contact_data.get("email"):
                        existing_contact.email = contact_data["email"]
                        existing_contact.email_valid = True
                    
                    # Update phone if we didn't have one
                    if not existing_contact.phone and contact_data.get("phone"):
                        existing_contact.phone = contact_data["phone"]
                    
                    # Update job title if we didn't have one
                    if not existing_contact.job_title and contact_data.get("job_title"):
                        existing_contact.job_title = contact_data["job_title"]
                    
                    # Only commit if something was updated
                    if not existing_contact.email or not existing_contact.phone or not existing_contact.job_title:
                        self.db_session.commit()
                        
                    saved_contacts.append(existing_contact)
                    continue
                
                # Create new contact
                new_contact = Contact(
                    organization_id=contact_data["organization_id"],
                    first_name=contact_data["first_name"],
                    last_name=contact_data.get("last_name", ""),
                    job_title=contact_data.get("job_title", ""),
                    email=contact_data.get("email", ""),
                    phone=contact_data.get("phone", ""),
                    discovery_method=contact_data.get("discovery_method", "municipal_contact_crawler"),
                    discovery_url=contact_data.get("discovery_url", ""),
                    contact_confidence_score=contact_data.get("contact_confidence_score", 0.7),
                    contact_relevance_score=contact_data.get("contact_relevance_score", 5.0),
                    email_valid=bool(contact_data.get("email"))
                )
                
                self.db_session.add(new_contact)
                self.db_session.commit()
                
                saved_contacts.append(new_contact)
                
            except Exception as e:
                self.db_session.rollback()
                logger.error(f"Error saving contact to database: {e}")
                
        return saved_contacts