"""
Industry directory scraper for organization discovery.
"""
import time
import re
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from sqlalchemy.orm import Session
from app.config import INDUSTRY_DIRECTORIES, TARGET_STATES, ILLINOIS_SOUTH_OF_I80
from app.database.models import DiscoveredURL
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DirectoryScraper:
    """Scraper for industry directories."""
    
    def __init__(self, db_session: Session):
        """
        Initialize the directory scraper.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.delay_between_requests = 2  # seconds between requests
    
    def scrape_industry_association(self, url: str, category: str) -> List[Dict[str, Any]]:
        """
        Scrape an industry association directory for organization information.
        
        Args:
            url: URL of the industry directory
            category: Organization category to search for
            
        Returns:
            List of discovered organizations
        """
        logger.info(f"Scraping industry association: {url} for category {category}")
        results = []
        
        try:
            # Get the directory page
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Parse the HTML
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract organization links based on directory structure
            # This is a generic approach - each directory might need specific parsing
            org_links = self._extract_organization_links(soup, url, category)
            
            # Filter by target states and add metadata
            for link in org_links:
                # Add metadata
                link["discovery_method"] = "industry_directory"
                link["source_url"] = url
                link["category"] = category
                
                results.append(link)
            
            logger.info(f"Discovered {len(results)} organizations from {url}")
            
        except Exception as e:
            logger.error(f"Error scraping industry association {url}: {e}")
        
        return results
    
    def scrape_directories(self, category: str = None) -> List[Dict[str, Any]]:
        """
        Scrape industry directories for the specified category.
        
        Args:
            category: Organization category to search for (if None, all categories)
            
        Returns:
            List of discovered URLs
        """
        all_results = []
        
        # Determine categories to scrape
        categories = [category] if category else INDUSTRY_DIRECTORIES.keys()
        
        for cat in categories:
            directory_urls = INDUSTRY_DIRECTORIES.get(cat, [])
            
            for url in directory_urls:
                logger.info(f"Scraping directory: {url} for category {cat}")
                
                try:
                    # Get the directory page
                    response = requests.get(url, headers=self.headers, timeout=30)
                    response.raise_for_status()
                    
                    # Parse the HTML
                    soup = BeautifulSoup(response.content, "html.parser")
                    
                    # Extract organization links based on directory structure
                    # This is a generic approach - each directory might need specific parsing
                    org_links = self._extract_organization_links(soup, url, cat)
                    
                    if org_links:
                        # Filter by target states
                        filtered_links = self._filter_by_states(org_links)
                        
                        # Store discovered URLs
                        for link in filtered_links:
                            try:
                                # Create DiscoveredURL record
                                discovered_url = DiscoveredURL(
                                    url=link["url"],
                                    title=link.get("title", ""),
                                    description=link.get("description", ""),
                                    page_type="directory_listing",
                                    priority_score=0.8  # High priority for directory listings
                                )
                                self.db_session.add(discovered_url)
                                self.db_session.commit()
                            except Exception as e:
                                self.db_session.rollback()
                                logger.error(f"Error adding directory URL {link['url']}: {e}")
                                # Continue with next URL
                        
                        all_results.extend(filtered_links)
                        logger.info(f"Discovered {len(filtered_links)} organizations from {url}")
                    
                    # Add delay to avoid overloading the server
                    time.sleep(self.delay_between_requests)
                    
                except Exception as e:
                    logger.error(f"Error scraping directory {url}: {e}")
        
        return all_results
    
    def _extract_organization_links(self, soup: BeautifulSoup, base_url: str, 
                                  category: str) -> List[Dict[str, Any]]:
        """
        Extract organization links from a directory page.
        
        Args:
            soup: BeautifulSoup object for the directory page
            base_url: Base URL of the directory
            category: Organization category
            
        Returns:
            List of extracted organization links
        """
        links = []
        
        # Look for common directory listing patterns
        # 1. Look for tables with organization listings
        tables = soup.find_all("table")
        for table in tables:
            # Check if table contains organization listings
            if self._is_org_listing_table(table):
                links.extend(self._extract_from_table(table, base_url, category))
        
        # 2. Look for div-based listings (e.g., cards, list items)
        org_divs = soup.find_all(["div", "li"], class_=lambda c: c and any(term in c.lower() 
                                                                         for term in ["member", "listing", "directory", "item", "card"]))
        for div in org_divs:
            link_data = self._extract_from_div(div, base_url, category)
            if link_data:
                links.append(link_data)
        
        # 3. Look for standalone links that might be organization listings
        if not links:
            links.extend(self._extract_standalone_links(soup, base_url, category))
        
        return links
    
    def _is_org_listing_table(self, table) -> bool:
        """
        Check if a table contains organization listings.
        
        Args:
            table: BeautifulSoup table element
            
        Returns:
            True if the table likely contains organization listings
        """
        # Check header row for organization-related columns
        headers = table.find_all("th") or table.find_all("td", attrs={"class": lambda c: c and "header" in c.lower()})
        header_text = " ".join([h.text.lower() for h in headers])
        
        # Check for organization-related terms in headers
        org_terms = ["name", "organization", "company", "member", "location", "address", "state", "city"]
        return any(term in header_text for term in org_terms)
    
    def _extract_from_table(self, table, base_url: str, category: str) -> List[Dict[str, Any]]:
        """
        Extract organization links from a table.
        
        Args:
            table: BeautifulSoup table element
            base_url: Base URL of the directory
            category: Organization category
            
        Returns:
            List of extracted organization links
        """
        links = []
        
        rows = table.find_all("tr")
        for row in rows:
            # Skip header row
            if row.find("th"):
                continue
                
            # Extract link
            link_element = row.find("a")
            if not link_element:
                continue
                
            # Get URL, title and description
            url = urljoin(base_url, link_element.get("href", ""))
            title = link_element.text.strip()
            
            # Extract state information
            state = None
            state_cell = row.find(string=lambda s: s and any(state_name in s for state_name in TARGET_STATES))
            if state_cell:
                for state_name in TARGET_STATES:
                    if state_name in state_cell:
                        state = state_name
                        break
            
            # Extract description
            description = ""
            for cell in row.find_all("td"):
                if cell.text.strip() and cell != link_element.parent:
                    description += cell.text.strip() + " "
            
            links.append({
                "url": url,
                "title": title,
                "description": description.strip(),
                "state": state,
                "category": category,
                "discovery_method": "directory_scraper"
            })
        
        return links
    
    def _extract_from_div(self, div, base_url: str, category: str) -> Optional[Dict[str, Any]]:
        """
        Extract organization link from a div.
        
        Args:
            div: BeautifulSoup div element
            base_url: Base URL of the directory
            category: Organization category
            
        Returns:
            Extracted organization link or None if no link found
        """
        # Extract link
        link_element = div.find("a")
        if not link_element:
            return None
            
        # Get URL and title
        url = urljoin(base_url, link_element.get("href", ""))
        title = link_element.text.strip()
        
        # Extract state information
        state = None
        for state_name in TARGET_STATES:
            if state_name in div.text:
                state = state_name
                break
        
        # Extract description
        description = div.text.strip()
        if link_element.text.strip() in description:
            description = description.replace(link_element.text.strip(), "").strip()
        
        return {
            "url": url,
            "title": title,
            "description": description,
            "state": state,
            "category": category,
            "discovery_method": "directory_scraper"
        }
    
    def _extract_standalone_links(self, soup: BeautifulSoup, base_url: str, 
                                category: str) -> List[Dict[str, Any]]:
        """
        Extract standalone organization links.
        
        Args:
            soup: BeautifulSoup object for the directory page
            base_url: Base URL of the directory
            category: Organization category
            
        Returns:
            List of extracted organization links
        """
        links = []
        
        # Look for links that might be organization listings
        for link in soup.find_all("a"):
            href = link.get("href", "")
            title = link.text.strip()
            
            # Skip if href is empty or title is too short
            if not href or len(title) < 3:
                continue
                
            # Skip if href is not a proper URL
            if not href.startswith(("http", "https", "/")):
                continue
                
            # Skip if link text contains navigation terms
            nav_terms = ["next", "previous", "back", "home", "login", "register", "search"]
            if any(term in title.lower() for term in nav_terms):
                continue
            
            # Check if the link contains state information
            state = None
            for state_name in TARGET_STATES:
                if state_name in link.text or state_name in href:
                    state = state_name
                    break
            
            # If no state found in link, check parent elements
            if not state:
                parent = link.parent
                for _ in range(3):  # Check up to 3 levels up
                    if parent and any(state_name in parent.text for state_name in TARGET_STATES):
                        for state_name in TARGET_STATES:
                            if state_name in parent.text:
                                state = state_name
                                break
                        break
                    parent = parent.parent if parent else None
            
            # Only include if state is in our target states or no state is mentioned
            if state or not any(state_name in soup.text for state_name in TARGET_STATES):
                # Create link data
                url = urljoin(base_url, href)
                
                # Get description from surrounding text
                description = ""
                parent = link.parent
                if parent:
                    description = parent.text.strip()
                    if title in description:
                        description = description.replace(title, "").strip()
                
                links.append({
                    "url": url,
                    "title": title,
                    "description": description,
                    "state": state,
                    "category": category,
                    "discovery_method": "directory_scraper"
                })
        
        return links
    
    def scrape_staff_directory(self, url: str, org_name: str) -> List[Dict[str, Any]]:
        """
        Scrape a staff directory page to extract contacts.
        
        Args:
            url: URL of the staff directory page
            org_name: Name of the organization
            
        Returns:
            List of extracted contacts
        """
        contacts = []
        
        try:
            # Get the page
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Parse the HTML
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract contact information using multiple strategies
            
            # 1. Look for structured data (Schema.org, vCard, JSON-LD)
            contacts.extend(self._extract_structured_contact_data(soup))
            
            # 2. Look for contact tables
            table_contacts = self._extract_contacts_from_tables(soup)
            contacts.extend(table_contacts)
            
            # 3. Look for contact cards/divs
            div_contacts = self._extract_contacts_from_divs(soup)
            contacts.extend(div_contacts)
            
            # 4. Extract contact information from text using regex patterns
            text_contacts = self._extract_contacts_from_text(soup)
            contacts.extend(text_contacts)
            
            # If still no contacts found, try to extract from general page content
            if not contacts:
                contacts.extend(self._extract_general_contacts(soup, org_name))
            
            # Process contacts to ensure they have organization name
            for contact in contacts:
                if 'organization_name' not in contact:
                    contact['organization_name'] = org_name
                    
                # Convert name to first/last if only full name is available
                if 'name' in contact and 'first_name' not in contact and 'last_name' not in contact:
                    name_parts = contact['name'].split()
                    if len(name_parts) >= 2:
                        contact['first_name'] = name_parts[0]
                        contact['last_name'] = ' '.join(name_parts[1:])
            
            logger.info(f"Extracted {len(contacts)} contacts from {url}")
            
        except Exception as e:
            logger.error(f"Error scraping staff directory {url}: {e}")
        
        return contacts
    
    def _extract_structured_contact_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract contact information from structured data.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of contacts
        """
        contacts = []
        
        # JSON-LD
        for script in soup.find_all('script', {'type': 'application/ld+json'}):
            try:
                if not script.string:
                    continue
                
                import json
                data = json.loads(script.string)
                
                # Handle both single items and lists
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    items = [data]
                else:
                    continue
                
                for item in items:
                    if '@type' in item and item['@type'] == 'Person':
                        contact = {
                            'name': item.get('name', ''),
                            'job_title': item.get('jobTitle', ''),
                            'email': item.get('email', ''),
                            'phone': item.get('telephone', ''),
                            'url': item.get('url', '')
                        }
                        
                        # Only add if we have a name
                        if contact['name']:
                            contacts.append(contact)
            except Exception as e:
                logger.error(f"Error parsing JSON-LD: {e}")
        
        # Microdata (schema.org Person)
        for element in soup.find_all(itemtype=re.compile('schema.org/Person')):
            try:
                name = element.find(itemprop='name')
                job_title = element.find(itemprop='jobTitle')
                email = element.find(itemprop='email')
                telephone = element.find(itemprop='telephone')
                
                contact = {
                    'name': name.get_text().strip() if name else '',
                    'job_title': job_title.get_text().strip() if job_title else '',
                    'email': email.get('content', email.get_text().strip()) if email else '',
                    'phone': telephone.get('content', telephone.get_text().strip()) if telephone else ''
                }
                
                if contact['name']:  # Only add if we have at least a name
                    contacts.append(contact)
            except Exception as e:
                logger.error(f"Error parsing microdata Person: {e}")
        
        # vCard
        for vcard in soup.find_all(class_=re.compile('vcard')):
            try:
                name = vcard.find(class_='fn')
                title = vcard.find(class_='title')
                email = vcard.find(class_='email')
                tel = vcard.find(class_='tel')
                
                contact = {
                    'name': name.get_text().strip() if name else '',
                    'job_title': title.get_text().strip() if title else '',
                    'email': email.get_text().strip() if email else '',
                    'phone': tel.get_text().strip() if tel else ''
                }
                
                if contact['name']:  # Only add if we have at least a name
                    contacts.append(contact)
            except Exception as e:
                logger.error(f"Error parsing vCard: {e}")
        
        return contacts
    
    def _extract_contacts_from_tables(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract contact information from tables.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of contacts
        """
        contacts = []
        
        # Look for tables that might contain contact information
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if not rows:
                continue
                
            # Check if the table has headers that suggest contact information
            headers = rows[0].find_all(['th', 'td'])
            header_text = ' '.join([h.get_text().lower() for h in headers])
            
            contact_headers = ['name', 'contact', 'email', 'phone', 'title', 'position', 'role', 'department']
            
            if any(term in header_text for term in contact_headers):
                # Get header indices
                column_indices = {}
                for i, header in enumerate(headers):
                    text = header.get_text().lower()
                    if 'name' in text:
                        column_indices['name'] = i
                    elif any(term in text for term in ['title', 'position', 'role']):
                        column_indices['job_title'] = i
                    elif 'email' in text:
                        column_indices['email'] = i
                    elif any(term in text for term in ['phone', 'tel', 'contact']):
                        column_indices['phone'] = i
                
                # Extract data rows
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < len(headers):
                        continue
                        
                    contact = {}
                    
                    # Extract data based on column indices
                    for field, idx in column_indices.items():
                        if idx < len(cells):
                            # Look for links that might contain email addresses
                            if field == 'email':
                                email_link = cells[idx].find('a', href=lambda h: h and h.startswith('mailto:'))
                                if email_link and 'href' in email_link.attrs:
                                    contact[field] = email_link['href'].replace('mailto:', '')
                                else:
                                    # Try to extract email from text using regex
                                    text = cells[idx].get_text()
                                    email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                                    if email_match:
                                        contact[field] = email_match.group(0)
                                    else:
                                        contact[field] = text.strip()
                            else:
                                contact[field] = cells[idx].get_text().strip()
                    
                    # Only add contact if it has at least a name
                    if 'name' in contact and contact['name']:
                        contacts.append(contact)
        
        return contacts
    
    def _extract_contacts_from_divs(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract contact information from divs (cards, list items, etc.)
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of contacts
        """
        contacts = []
        
        # Look for common contact card patterns
        contact_divs = soup.find_all(['div', 'li', 'article'], class_=lambda c: c and any(term in str(c).lower() 
                                                              for term in ['card', 'person', 'staff', 'team', 'member', 'profile', 'contact']))
        
        for div in contact_divs:
            contact = {}
            
            # Extract name - usually in a heading element
            name_elem = div.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'p', 'span', 'div'], 
                              class_=lambda c: c and 'name' in str(c).lower())
            
            if not name_elem:
                # Try to find name by position
                name_elem = div.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong'])
            
            if name_elem:
                contact['name'] = name_elem.get_text().strip()
            
            # Extract job title
            title_elem = div.find(['p', 'span', 'div'], 
                               class_=lambda c: c and any(term in str(c).lower() 
                                                for term in ['title', 'position', 'role', 'job']))
            
            if title_elem:
                contact['job_title'] = title_elem.get_text().strip()
            elif contact.get('name'):
                # Look for text after name that might be a title
                name_siblings = list(name_elem.next_siblings)
                for sibling in name_siblings:
                    if hasattr(sibling, 'text') and sibling.text.strip():
                        contact['job_title'] = sibling.text.strip()
                        break
            
            # Extract email
            email_elem = div.find('a', href=lambda h: h and h.startswith('mailto:'))
            if email_elem and 'href' in email_elem.attrs:
                contact['email'] = email_elem['href'].replace('mailto:', '')
            else:
                # Try to find email using regex
                text = div.get_text()
                email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                if email_match:
                    contact['email'] = email_match.group(0)
            
            # Extract phone
            phone_elem = div.find('a', href=lambda h: h and h.startswith('tel:'))
            if phone_elem and 'href' in phone_elem.attrs:
                contact['phone'] = phone_elem['href'].replace('tel:', '')
            else:
                # Try to find phone using regex
                text = div.get_text()
                phone_match = re.search(r'(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', text)
                if phone_match:
                    contact['phone'] = phone_match.group(0)
            
            # Only add contact if it has at least a name
            if 'name' in contact and contact['name']:
                contacts.append(contact)
        
        return contacts
    
    def _extract_contacts_from_text(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract contact information from text using regex patterns.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of contacts
        """
        contacts = []
        
        # Get all text from the page
        text = soup.get_text()
        
        # Pattern for name with title: "John Smith, CEO" or "John Smith - Director"
        name_title_pattern = r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)+)[\s,:-]+([A-Za-z\s&]+?(?:Director|Manager|Engineer|President|CEO|CTO|CFO|Officer|Supervisor|Lead|Head|Chief|Coordinator)(?:[A-Za-z\s&]*))(?:\s|,|\.|\n|$)'
        
        for match in re.finditer(name_title_pattern, text):
            name = match.group(1).strip()
            job_title = match.group(2).strip()
            
            # Look for email and phone near this match
            context = text[max(0, match.start() - 100):min(len(text), match.end() + 100)]
            
            email = None
            email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', context)
            if email_match:
                email = email_match.group(0)
                
            phone = None
            phone_match = re.search(r'(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', context)
            if phone_match:
                phone = phone_match.group(0)
                
            # Only add if not already found (avoid duplicates)
            if not any(c.get('name') == name for c in contacts):
                contact = {
                    'name': name,
                    'job_title': job_title
                }
                
                if email:
                    contact['email'] = email
                    
                if phone:
                    contact['phone'] = phone
                    
                contacts.append(contact)
        
        return contacts
    
    def _extract_general_contacts(self, soup: BeautifulSoup, org_name: str) -> List[Dict[str, Any]]:
        """
        Extract general contact information when specific contacts can't be found.
        
        Args:
            soup: BeautifulSoup object
            org_name: Organization name
            
        Returns:
            List of contacts
        """
        contacts = []
        
        # Look for general contact email
        email_elements = soup.find_all('a', href=lambda h: h and h.startswith('mailto:'))
        for email_elem in email_elements:
            email = email_elem['href'].replace('mailto:', '')
            
            # Skip common non-personal email addresses
            skip_patterns = ['info@', 'contact@', 'hello@', 'support@', 'sales@', 'service@']
            if any(pattern in email.lower() for pattern in skip_patterns):
                continue
                
            # Create contact
            job_title = 'Unknown Position'
            
            # Try to infer position from the email address
            if 'director' in email.lower():
                job_title = 'Director'
            elif 'manager' in email.lower():
                job_title = 'Manager'
            elif 'admin' in email.lower():
                job_title = 'Administrator'
            elif 'eng' in email.lower():
                job_title = 'Engineer'
            
            # Get name from email if possible
            name_parts = email.split('@')[0].split('.')
            if len(name_parts) >= 2:
                first_name = name_parts[0].capitalize()
                last_name = name_parts[1].capitalize()
                name = f"{first_name} {last_name}"
                
                contact = {
                    'first_name': first_name,
                    'last_name': last_name,
                    'name': name,
                    'job_title': job_title,
                    'email': email,
                    'organization_name': org_name
                }
                
                contacts.append(contact)
        
        return contacts
        
    def _filter_by_states(self, links: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter links by target states.
        
        Args:
            links: List of links
            
        Returns:
            Filtered list of links
        """
        filtered_links = []
        
        for link in links:
            # Include if state is explicitly in target states
            if link.get("state") in TARGET_STATES:
                filtered_links.append(link)
                continue
            
            # Check if state is in description
            for state_name in TARGET_STATES:
                if state_name in link.get("description", ""):
                    link["state"] = state_name
                    filtered_links.append(link)
                    break
            
            # For Illinois, check if it's south of I-80
            if "Illinois" in link.get("description", ""):
                # Check if any county/city south of I-80 is mentioned
                for county in ILLINOIS_SOUTH_OF_I80:
                    if county in link.get("description", ""):
                        link["state"] = "Illinois"
                        filtered_links.append(link)
                        break
        
        return filtered_links