"""
Government agencies scraper for the GBL Data Contact Management System.
"""
import time
import re
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import requests
from app.scraper.base import BaseScraper
from app.config import TARGET_STATES, ORG_TYPES
from app.utils.logger import get_logger

logger = get_logger(__name__)


class GovernmentScraper(BaseScraper):
    """Scraper for government agencies."""
    
    def __init__(self, db_session):
        """Initialize the government scraper."""
        super().__init__(db_session)
        self.org_type = "government"
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape government agencies from various sources.
        
        Returns:
            List of dictionaries with contact data
        """
        logger.info("Starting government agencies scraping")
        contacts = []
        
        # Scrape from multiple sources
        contacts.extend(self._scrape_state_agencies())
        contacts.extend(self._scrape_federal_agencies())
        contacts.extend(self._scrape_search_based_agencies())
        
        logger.info(f"Found {len(contacts)} government agency contacts")
        return contacts

    def _scrape_state_agencies(self) -> List[Dict[str, Any]]:
        """
        Scrape state government agencies in target states.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # State government portal URLs
        state_portals = {
            "Utah": [
                "https://utah.gov/government/agencies.html",
                "https://jobs.utah.gov/jsp/utjobs/seeker/employer/list.do?emptype=2"
            ],
            "Illinois": [
                "https://www2.illinois.gov/pages/agencies.aspx",
                "https://illinois.gov/agencies/"
            ],
            "Arizona": [
                "https://az.gov/agency-directory",
                "https://www.azleg.gov/agencies-departments/"
            ],
            "Missouri": [
                "https://www.mo.gov/government/state-government-agencies/",
                "https://www.sos.mo.gov/business/outreach/rescources"
            ],
            "New Mexico": [
                "https://www.newmexico.gov/government/",
                "https://www.nm.gov/state-government/state-agencies/"
            ],
            "Nevada": [
                "https://nv.gov/agencies/",
                "https://budget.nv.gov/StateAgencies/"
            ]
        }
        
        # Target departments related to infrastructure
        target_departments = ["transportation", "environmental", "highway", "resource", "water", 
                             "utility", "public works", "infrastructure", "agriculture", "energy",
                             "development", "conservation", "natural resource", "technology",
                             "facilities", "emergency", "management"]
        
        # For each target state, find agencies
        for state, urls in state_portals.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Finding government agencies in {state}")
            
            for url in urls:
                try:
                    logger.info(f"Accessing state portal at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Extract agencies
                    agencies = self._extract_agencies_from_portal(soup, url, state)
                    
                    # Process each agency
                    for agency in agencies:
                        try:
                            # Skip if it doesn't have a name or website
                            if not agency.get("name") or not agency.get("website"):
                                continue
                            
                            # Check if the agency is related to our target areas
                            is_target_agency = False
                            for target in target_departments:
                                if target in agency["name"].lower():
                                    is_target_agency = True
                                    break
                            
                            # Skip non-target agencies unless we have very few
                            if not is_target_agency and len(agencies) > 15:
                                continue
                            
                            # Set default state if not in agency data
                            if not agency.get("state"):
                                agency["state"] = state
                                
                            # Make sure we have the org_type set
                            agency["org_type"] = self.org_type
                            
                            # Save organization and get ID
                            org_id = self.save_organization(agency)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the agency website
                            agency_contacts = self._scrape_agency_website(agency.get("website"), org_id)
                            contacts.extend(agency_contacts)
                            
                            # Track success for logging
                            if agency_contacts:
                                logger.info(f"Found {len(agency_contacts)} contacts at {agency['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing agency: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing state portal {url}: {e}")
        
        return contacts
    
    def _scrape_federal_agencies(self) -> List[Dict[str, Any]]:
        """
        Scrape federal government agencies with offices in target states.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Federal agencies that often have regional/state offices
        federal_agencies = [
            {
                "name": "U.S. Environmental Protection Agency",
                "website": "https://www.epa.gov/aboutepa/regional-and-geographic-offices"
            },
            {
                "name": "U.S. Army Corps of Engineers",
                "website": "https://www.usace.army.mil/Locations/"
            },
            {
                "name": "Bureau of Land Management",
                "website": "https://www.blm.gov/about/contact"
            },
            {
                "name": "U.S. Department of Agriculture",
                "website": "https://www.usda.gov/contact-us"
            },
            {
                "name": "U.S. Geological Survey",
                "website": "https://www.usgs.gov/connect/locations"
            },
            {
                "name": "Federal Emergency Management Agency",
                "website": "https://www.fema.gov/about/contact"
            },
            {
                "name": "Bureau of Reclamation",
                "website": "https://www.usbr.gov/main/regions.html"
            }
        ]
        
        for agency in federal_agencies:
            try:
                logger.info(f"Finding regional offices for {agency['name']}")
                
                soup = self.get_page(agency['website'])
                if not soup:
                    continue
                
                # Extract regional offices - match our target states
                for state in TARGET_STATES:
                    # Look for links or content with the state name
                    state_elements = soup.find_all(
                        lambda tag: tag.name in ["a", "div", "section", "li", "tr"] and 
                                  state in tag.text
                    )
                    
                    for element in state_elements:
                        try:
                            # Extract office link
                            office_link = None
                            if element.name == "a" and element.has_attr("href"):
                                office_link = element["href"]
                            else:
                                link = element.find("a")
                                if link and link.has_attr("href"):
                                    office_link = link["href"]
                            
                            # Skip if no link found
                            if not office_link:
                                continue
                            
                            # Make absolute URL if needed
                            if office_link.startswith("/"):
                                # Get base URL
                                from urllib.parse import urlparse
                                parsed_url = urlparse(agency['website'])
                                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                                office_link = base_url + office_link
                            
                            # Skip if the link isn't to a different page
                            if office_link == agency['website']:
                                continue
                                
                            # Extract office name
                            office_name = element.get_text().strip()
                            if len(office_name) > 100:
                                # Too long - try to find a better name
                                heading = element.find(["h1", "h2", "h3", "h4", "strong"])
                                if heading:
                                    office_name = heading.get_text().strip()
                                else:
                                    # Use a generic name
                                    office_name = f"{agency['name']} - {state} Office"
                            
                            # Create organization data
                            org_data = {
                                "name": office_name,
                                "org_type": self.org_type,
                                "website": office_link,
                                "state": state,
                                "source_url": agency['website']
                            }
                            
                            # Save organization and get ID
                            org_id = self.save_organization(org_data)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the office website
                            office_contacts = self._scrape_agency_website(office_link, org_id)
                            contacts.extend(office_contacts)
                            
                            # Track success for logging
                            if office_contacts:
                                logger.info(f"Found {len(office_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading servers
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing regional office: {e}")
                
            except Exception as e:
                logger.error(f"Error accessing federal agency {agency['name']}: {e}")
        
        return contacts
    
    def _scrape_search_based_agencies(self) -> List[Dict[str, Any]]:
        """
        Find government agencies using search engine.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Initialize search engine
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, search for government agencies
        for state in TARGET_STATES:
            logger.info(f"Searching for government agencies in {state}")
            
            # Advanced search queries for government agencies
            specialized_queries = [
                f"state government agencies {state} environmental",
                f"state government agencies {state} transportation",
                f"state government agencies {state} water resources",
                f"state government agencies {state} infrastructure",
                f"{state} department of environmental quality",
                f"{state} department of transportation",
                f"{state} department of natural resources",
                f"{state} facilities management",
                f"{state} public utilities commission"
            ]
            
            for query in specialized_queries:
                try:
                    logger.info(f"Executing search: {query}")
                    search_results = search_engine.execute_search(query, "government", state)
                    
                    # Process each search result
                    for result in search_results:
                        try:
                            # Extract URL, title and snippet
                            url = result.get("link", "") or result.get("url", "")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                            
                            if not url or not title:
                                continue
                                
                            # Check if this is a government website (.gov TLD)
                            if not (".gov" in url.lower() or ".state." in url.lower() or ".us" in url.lower()):
                                continue
                            
                            # Skip social media, job sites, etc.
                            skip_domains = ["linkedin.com", "facebook.com", "twitter.com", "indeed.com", 
                                          "glassdoor.com", "wikipedia.org"]
                            if any(domain in url.lower() for domain in skip_domains):
                                continue
                                
                            # Create organization data
                            org_data = {
                                "name": title.split(" - ")[0].split(" | ")[0],  # Take first part of title as name
                                "org_type": self.org_type,
                                "website": url,
                                "state": state,
                                "source_url": url
                            }
                            
                            # Try to get website content
                            soup = self.get_page(url)
                            if soup:
                                # Try to extract a better agency name from the website
                                better_name = self._extract_agency_name(soup)
                                if better_name:
                                    org_data["name"] = better_name
                                    
                                # Try to extract address/location
                                city = self._extract_city(soup, state)
                                if city:
                                    org_data["city"] = city
                            
                            # Save organization and get ID
                            org_id = self.save_organization(org_data)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the agency website
                            agency_contacts = self._scrape_agency_website(url, org_id)
                            if agency_contacts:
                                contacts.extend(agency_contacts)
                                logger.info(f"Found {len(agency_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading servers
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing search result: {e}")
                    
                except Exception as e:
                    logger.error(f"Error executing search query '{query}': {e}")
        
        return contacts
    
    def _extract_agencies_from_portal(self, soup: BeautifulSoup, source_url: str, state: str) -> List[Dict[str, Any]]:
        """Extract government agencies from state portal pages."""
        agencies = []
        
        try:
            # Try to find agency listings by common class/id patterns
            agency_sections = soup.find_all(["div", "section", "ul"], id=lambda i: i and 
                                         any(term in i.lower() for term in ["agency", "department", "government", "directory"]))
            
            if not agency_sections:
                agency_sections = soup.find_all(["div", "section", "ul"], class_=lambda c: c and 
                                            any(term in c.lower() for term in ["agency", "department", "government", "directory"]))
            
            # If no specific sections found, look at the whole page
            if not agency_sections:
                agency_sections = [soup]
            
            # Process each section
            for section in agency_sections:
                # Look for links to agencies
                links = section.find_all("a", href=lambda h: h and 
                                      not any(term in h.lower() for term in ["mailto:", "javascript:", "#"]))
                
                for link in links:
                    try:
                        agency_name = link.get_text().strip()
                        href = link["href"]
                        
                        # Skip if empty name or it doesn't look like an agency name
                        if not agency_name or len(agency_name) < 3:
                            continue
                            
                        # Skip obvious non-agency links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word == agency_name.lower() for word in skip_words):
                            continue
                        
                        # Make absolute URL if needed
                        if href.startswith("/"):
                            # Get base URL
                            from urllib.parse import urlparse
                            parsed_url = urlparse(source_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = base_url + href
                        
                        # Create agency data
                        agency_data = {
                            "name": agency_name,
                            "website": href,
                            "state": state,
                            "source_url": source_url
                        }
                        
                        agencies.append(agency_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting agency from link: {e}")
            
            # If still not enough agencies found, try more generic patterns
            if len(agencies) < 5:
                # Look for tables or lists that might contain agency info
                tables = soup.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    for row in rows[1:]:  # Skip header row
                        try:
                            cells = row.find_all(["td", "th"])
                            if len(cells) < 2:
                                continue
                            
                            # First column usually has the name
                            name_cell = cells[0]
                            
                            # Check for a link
                            link = name_cell.find("a")
                            if link:
                                agency_name = link.get_text().strip()
                                href = link["href"]
                                
                                # Make absolute URL if needed
                                if href.startswith("/"):
                                    from urllib.parse import urlparse
                                    parsed_url = urlparse(source_url)
                                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                                    href = base_url + href
                                
                                agencies.append({
                                    "name": agency_name,
                                    "website": href,
                                    "state": state,
                                    "source_url": source_url
                                })
                                
                        except Exception as e:
                            logger.error(f"Error extracting agency from table row: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting agencies from portal: {e}")
        
        logger.info(f"Found {len(agencies)} agencies on portal {source_url}")
        return agencies
    
    def _extract_agency_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract agency name from website content."""
        try:
            # Check for schema.org Organization markup
            org_schema = soup.find("script", {"type": "application/ld+json"})
            if org_schema:
                import json
                try:
                    data = json.loads(org_schema.string)
                    if isinstance(data, dict) and data.get("@type") == "Organization" and data.get("name"):
                        return data.get("name")
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and item.get("@type") == "Organization" and item.get("name"):
                                return item.get("name")
                except:
                    pass
            
            # Check meta tags
            meta_title = soup.find("meta", {"property": "og:site_name"})
            if meta_title and meta_title.get("content"):
                return meta_title["content"].strip()
            
            # Look for header content
            header = soup.find("header")
            if header:
                h1 = header.find("h1")
                if h1:
                    return h1.get_text().strip()
                
                # Try other heading levels
                for heading in ["h2", "h3"]:
                    h = header.find(heading)
                    if h:
                        return h.get_text().strip()
            
            # Look for agency name in typical government page structures
            agency_header = soup.find(["div", "span"], class_=lambda c: c and 
                                   any(term in c.lower() for term in ["agency-name", "site-title", "site-header"]))
            if agency_header:
                return agency_header.get_text().strip()
            
            # Check the title tag
            if soup.title:
                title = soup.title.text.strip()
                # Remove common suffixes
                suffixes = [" - Home", " | Home", " - Official Website", " | Official Website", " | Official Site"]
                for suffix in suffixes:
                    if title.endswith(suffix):
                        return title[:-len(suffix)].strip()
                
                return title
        
        except Exception as e:
            logger.error(f"Error extracting agency name: {e}")
            
        return None
    
    def _extract_city(self, soup: BeautifulSoup, state: str) -> Optional[str]:
        """Extract city from website content using the state as context."""
        try:
            # Look for address patterns
            address_containers = soup.find_all(["p", "div", "span"], string=re.compile(f"{state}", re.IGNORECASE))
            
            for container in address_containers:
                text = container.get_text()
                # Look for "City, State" pattern
                city_match = re.search(r'([A-Za-z\s\.]+),\s*' + state, text, re.IGNORECASE)
                if city_match:
                    city = city_match.group(1).strip()
                    # Sanity check - cities shouldn't be longer than 30 chars
                    if city and len(city) < 30:
                        return city
                        
            # Look specifically in contact or footer sections
            contact_section = soup.find(["div", "section"], id=lambda x: x and "contact" in x.lower())
            if not contact_section:
                contact_section = soup.find(["div", "section"], class_=lambda x: x and "contact" in x.lower())
                
            if contact_section:
                address_text = contact_section.get_text()
                city_match = re.search(r'([A-Za-z\s\.]+),\s*' + state, address_text, re.IGNORECASE)
                if city_match:
                    city = city_match.group(1).strip()
                    if city and len(city) < 30:
                        return city
                        
            # Try to find in footer
            footer = soup.find("footer")
            if footer:
                address_text = footer.get_text()
                city_match = re.search(r'([A-Za-z\s\.]+),\s*' + state, address_text, re.IGNORECASE)
                if city_match:
                    city = city_match.group(1).strip()
                    if city and len(city) < 30:
                        return city
        
        except Exception as e:
            logger.error(f"Error extracting city: {e}")
            
        return None
    
    def _scrape_agency_website(self, website: str, org_id: int) -> List[Dict[str, Any]]:
        """
        Scrape contacts from a government agency website.
        
        Args:
            website: Agency website URL
            org_id: Organization ID in the database
            
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Common paths where staff/leadership information might be found
        staff_paths = ["about/leadership", "about-us/leadership", "leadership", "management", 
                      "about/staff", "about-us/staff", "staff", "contact-us", "contact",
                      "directory", "about/directory", "about/contact", "about-us/contact",
                      "our-staff", "team", "about/team", "about-us/team",
                      "about/management", "about-us/management"]
        
        # Try each path
        for path in staff_paths:
            url = f"{website.rstrip('/')}/{path}"
            
            try:
                soup = self.get_page(url)
                if not soup:
                    continue
                
                # Look for staff listings
                staff_elements = soup.find_all(["div", "article", "section"], 
                                            class_=lambda c: c and any(term in c.lower() 
                                                                   for term in ["team", "staff", "member", "people", 
                                                                              "leadership", "board", "director", 
                                                                              "management", "employee"]))
                
                for element in staff_elements:
                    # Look for job titles that match our target
                    target_titles = ORG_TYPES["government"]["job_titles"]
                    
                    # Extract job title, name, and contact info
                    title_element = element.find(["h3", "h4", "div", "span"], 
                                             class_=lambda c: c and ("title" in c.lower() or "position" in c.lower()))
                    
                    if not title_element:
                        # Try other elements that might contain job titles
                        title_elements = element.find_all(["p", "div", "span"])
                        for elem in title_elements:
                            if any(title.lower() in elem.get_text().lower() for title in ["director", "manager", "chief", "head", "administrator"]):
                                title_element = elem
                                break
                    
                    if not title_element:
                        continue
                        
                    job_title = title_element.text.strip()
                    
                    # Check if job title is relevant (management, leadership, or technical roles)
                    relevant_title = any(target.lower() in job_title.lower() for target in target_titles)
                    
                    # Also check for operations, facilities, or technical roles which are important for government agencies
                    operations_role = "operations" in job_title.lower() or "operator" in job_title.lower()
                    facilities_role = "facilities" in job_title.lower() or "infrastructure" in job_title.lower()
                    technical_role = "technical" in job_title.lower() or "engineer" in job_title.lower() or "director" in job_title.lower()
                    
                    if not (relevant_title or operations_role or facilities_role or technical_role):
                        continue
                    
                    # Extract name
                    name_element = element.find(["h2", "h3", "div", "span"], 
                                            class_=lambda c: c and "name" in c.lower())
                    
                    if not name_element:
                        # Try other common elements that might contain names
                        name_element = element.find(["h2", "h3", "strong"])
                    
                    if name_element:
                        full_name = name_element.text.strip()
                        # Split into first and last name
                        name_parts = full_name.split(" ", 1)
                        first_name = name_parts[0]
                        last_name = name_parts[1] if len(name_parts) > 1 else ""
                    else:
                        continue
                    
                    # Extract email
                    email_element = element.find("a", href=lambda h: h and "mailto:" in h)
                    email = email_element["href"].replace("mailto:", "") if email_element else None
                    
                    # Extract phone
                    phone_element = element.find("a", href=lambda h: h and "tel:" in h)
                    phone = phone_element["href"].replace("tel:", "") if phone_element else None
                    
                    # Create contact data
                    contact_data = {
                        "organization_id": org_id,
                        "first_name": first_name,
                        "last_name": last_name,
                        "job_title": job_title,
                        "email": email,
                        "phone": phone,
                        "discovery_method": "website",
                        "discovery_url": url,
                        "contact_confidence_score": 0.9,  # High confidence for direct website extraction
                        "contact_relevance_score": 9.0 if technical_role or facilities_role else 7.0,
                        "notes": f"Found on {url}"
                    }
                    
                    # Save contact
                    contact_id = self.save_contact(contact_data)
                    if contact_id:
                        contacts.append(contact_data)
            
            except Exception as e:
                logger.error(f"Error scraping agency website {url}: {e}")
                
        return contacts