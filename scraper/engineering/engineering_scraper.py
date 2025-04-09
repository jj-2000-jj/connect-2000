"""
Engineering firms scraper for the GBL Data Contact Management System.
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


class EngineeringScraper(BaseScraper):
    """Scraper for engineering firms that work with SCADA systems."""
    
    def __init__(self, db_session):
        """Initialize the engineering scraper."""
        super().__init__(db_session)
        self.org_type = "engineering"
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape engineering firms from various sources.
        
        Returns:
            List of dictionaries with contact data
        """
        logger.info("Starting engineering firms scraping")
        contacts = []
        
        # Scrape from multiple sources
        contacts.extend(self._scrape_engineering_directory())
        contacts.extend(self._scrape_acec_chapters())
        contacts.extend(self._scrape_linkedin_engineering_firms())
        
        logger.info(f"Found {len(contacts)} engineering contacts")
        return contacts

    def _scrape_engineering_directory(self) -> List[Dict[str, Any]]:
        """
        Scrape engineering firms from engineering directories.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Real engineering directories to scrape
        eng_directories = {
            "Utah": [
                "https://www.acecofutah.org/page/MemberDirectory",
                "https://www.professionalengineersutah.org/page/MemberDirectory"
            ],
            "Illinois": [
                "https://www.acecil.org/Directories/FindAFirm",
                "https://www.ispe.org/membership/directory/"
            ],
            "Arizona": [
                "https://www.acec-az.org/page/MemberDirectory", 
                "https://www.azaspe.org/content.php?page=Member_Firms"
            ],
            "Missouri": [
                "https://www.acecmo.org/member-directory/", 
                "https://mspe.org/membership-directory/"
            ],
            "New Mexico": [
                "https://www.acecnm.org/page/MemberDirectory", 
                "https://www.nmspe.org/page/MemberDirectory"
            ],
            "Nevada": [
                "https://www.acecnv.org/membership-directory/", 
                "https://www.nvengineers.org/membership/membership-directory"
            ]
        }
        
        # Use search engine to find engineering firms that may not be in directories
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, use both directories and search
        for state in TARGET_STATES:
            logger.info(f"Finding engineering firms in {state}")
            
            # 1. First use search queries to find engineering firms
            search_queries = [
                f"engineering firms in {state}",
                f"civil engineering companies {state}",
                f"engineering consultants {state}",
                f"structural engineering firms {state}",
                f"mechanical engineering companies {state}"
            ]
            
            for query in search_queries:
                try:
                    logger.info(f"Searching for: {query}")
                    search_results = search_engine.execute_search(query, "engineering", state)
                    
                    # Process each search result
                    for result in search_results:
                        try:
                            # Extract URL, title and snippet
                            url = result.get("link", "") or result.get("url", "")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                            
                            if not url:
                                continue
                                
                            # Skip if this clearly isn't a firm website (like directories, job sites, or social media)
                            skip_domains = ["linkedin.com", "indeed.com", "ziprecruiter.com", "monster.com", 
                                          "glassdoor.com", "bbb.org", "yelp.com", "yellowpages.com",
                                          "instagram.com", "facebook.com", "twitter.com", "youtube.com"]
                            if any(domain in url.lower() for domain in skip_domains):
                                continue
                                
                            # Attempt to determine if this is an engineering firm
                            is_firm = False
                            engineering_terms = ["engineering", "engineers", "consultants", "consulting"]
                            if any(term.lower() in title.lower() for term in engineering_terms):
                                is_firm = True
                            elif any(term.lower() in snippet.lower() for term in engineering_terms):
                                is_firm = True
                            
                            if not is_firm:
                                continue
                                
                            # Create organization data - we'll get city later from website if possible
                            org_data = {
                                "name": title.split(" - ")[0].split(" | ")[0],  # Take first part of title as name
                                "org_type": self.org_type,
                                "subtype": self._determine_engineering_type(title, url),
                                "website": url,
                                "state": state,
                                "source_url": url
                            }
                            
                            # Try to get company website content
                            soup = self.get_page(url)
                            if soup:
                                # Try to extract a better company name from the website
                                better_name = self._extract_company_name(soup)
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
                            
                            # Scrape contacts from the company website
                            company_contacts = self._scrape_company_website(url, org_id)
                            if company_contacts:
                                contacts.extend(company_contacts)
                                logger.info(f"Found {len(company_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading servers
                            time.sleep(2)
                        
                        except Exception as e:
                            logger.error(f"Error processing search result: {e}")
                
                except Exception as e:
                    logger.error(f"Error executing search query '{query}': {e}")
            
            # 2. Then try to scrape engineering directories
            state_directories = eng_directories.get(state, [])
            
            for url in state_directories:
                try:
                    logger.info(f"Scraping engineering directory for {state} at {url}")
                    
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Different parsing strategies based on the URL
                    if "acec" in url.lower():
                        # ACEC sites usually have a membership directory with links
                        firms = self._extract_acec_firms(soup, url)
                    elif "aspe" in url.lower() or "spe.org" in url.lower():
                        # Society of Professional Engineers sites
                        firms = self._extract_spe_firms(soup, url)
                    else:
                        # Generic approach
                        firms = self._extract_generic_directory_firms(soup, url)
                    
                    # Process each firm found
                    for firm in firms:
                        try:
                            # Validate firm data
                            if not firm.get("name") or not firm.get("website"):
                                continue
                            
                            # Set default state if not in firm data
                            if not firm.get("state"):
                                firm["state"] = state
                                
                            # Make sure we have the org_type set
                            firm["org_type"] = self.org_type
                            
                            # Determine engineering subtype
                            if not firm.get("subtype"):
                                firm["subtype"] = self._determine_engineering_type(firm["name"], firm["website"])
                            
                            # Save organization and get ID
                            org_id = self.save_organization(firm)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the company website
                            company_contacts = self._scrape_company_website(firm["website"], org_id)
                            contacts.extend(company_contacts)
                            
                            # Track success for logging
                            if company_contacts:
                                logger.info(f"Found {len(company_contacts)} contacts at {firm['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                        
                        except Exception as e:
                            logger.error(f"Error processing firm listing: {e}")
                    
                except Exception as e:
                    logger.error(f"Error scraping directory {url}: {e}")
        
        return contacts
        
    def _extract_company_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract company name from website content."""
        # Try common places for company name
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
                
            # Check main logo alt text
            logo = soup.find("img", {"id": "logo"}) or soup.find("img", {"class": "logo"})
            if logo and logo.get("alt") and "logo" in logo["alt"].lower():
                name = logo["alt"].replace("logo", "", flags=re.IGNORECASE).strip()
                if name:
                    return name
                    
            # Check title without suffixes
            if soup.title:
                title = soup.title.text.strip()
                # Remove common suffixes
                for suffix in [" - Home", " | Home", " - Engineering", " | Engineering"]:
                    if title.endswith(suffix):
                        return title[:-len(suffix)].strip()
        except:
            pass
            
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
        except:
            pass
            
        return None
        
    def _extract_acec_firms(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, Any]]:
        """Extract firms from ACEC member directory pages."""
        firms = []
        
        try:
            # ACEC sites have different structures, try multiple approaches
            
            # Try to find member listings (common ACEC pattern)
            member_elements = soup.find_all(["div", "article", "li"], class_=lambda c: c and ("member" in c.lower() or "directory-item" in c.lower()))
            
            # If that didn't work, try table rows
            if not member_elements:
                tables = soup.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    # Skip the header row
                    member_elements.extend(rows[1:])
            
            # If still nothing, try cards or list items with links
            if not member_elements:
                member_elements = soup.find_all(["div", "li"], class_=lambda c: c and ("card" in c.lower() or "list-item" in c.lower()))
            
            # Process each member element
            for element in member_elements:
                try:
                    # Try to extract name - usually in a heading, link or strong tag
                    name_elem = element.find(["h2", "h3", "h4", "a", "strong"])
                    if not name_elem:
                        continue
                        
                    name = name_elem.get_text().strip()
                    
                    # Try to find website link
                    website_elem = element.find("a", href=lambda h: h and ("http" in h and "mailto:" not in h))
                    website = website_elem["href"] if website_elem else None
                    
                    # If the name element is itself a link, use that
                    if not website and name_elem.name == "a" and "href" in name_elem.attrs:
                        website = name_elem["href"]
                    
                    if not website or not name:
                        continue
                    
                    # Try to find city/location if available
                    location_text = None
                    for tag in element.find_all(["p", "div", "span"]):
                        text = tag.get_text().strip()
                        # Look for city, state pattern
                        if re.search(r'[A-Za-z\s]+,\s*[A-Z]{2}', text):
                            location_text = text
                            break
                    
                    city = None
                    state_code = None
                    
                    if location_text:
                        # Parse city and state
                        city_state_match = re.search(r'([^,]+),\s*([A-Z]{2})', location_text)
                        if city_state_match:
                            city = city_state_match.group(1).strip()
                            state_code = city_state_match.group(2)
                    
                    # Create firm data
                    firm_data = {
                        "name": name,
                        "website": website,
                        "source_url": source_url
                    }
                    
                    if city:
                        firm_data["city"] = city
                    
                    if state_code:
                        firm_data["state"] = state_code
                    
                    firms.append(firm_data)
                
                except Exception as e:
                    logger.error(f"Error extracting ACEC firm: {e}")
            
            # If we still found nothing, general fallback to link extraction
            if not firms:
                # Look for links that might be firm websites
                firm_links = soup.find_all("a", href=lambda h: h and "http" in h and "acec" not in h.lower())
                
                for link in firm_links:
                    try:
                        firm_name = link.get_text().strip()
                        if not firm_name or len(firm_name) < 3:
                            continue
                            
                        # Skip obvious non-firm links like "Click Here", "More Info", etc.
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word in firm_name.lower() for word in skip_words):
                            continue
                        
                        # Skip links that appear to be breadcrumbs or navigation
                        if not link.get("href"):
                            continue
                            
                        firms.append({
                            "name": firm_name,
                            "website": link["href"],
                            "source_url": source_url
                        })
                    except Exception as e:
                        logger.error(f"Error extracting firm from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting ACEC firms: {e}")
        
        logger.info(f"Found {len(firms)} firms in ACEC directory")
        return firms
        
    def _extract_spe_firms(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, Any]]:
        """Extract firms from Society of Professional Engineers directory pages."""
        firms = []
        
        try:
            # SPE sites often have lists or cards for members
            member_elements = soup.find_all(["div", "li"], class_=lambda c: c and ("member" in c.lower() or "directory-item" in c.lower() or "card" in c.lower()))
            
            # If that didn't work, try table rows
            if not member_elements:
                tables = soup.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    # Skip the header row
                    member_elements.extend(rows[1:])
            
            # Process each member element
            for element in member_elements:
                try:
                    # Try to extract name
                    name_elem = element.find(["h2", "h3", "h4", "a", "strong", "td"])
                    if not name_elem:
                        continue
                        
                    name = name_elem.get_text().strip()
                    
                    # Try to find website link
                    website_elem = element.find("a", href=lambda h: h and ("http" in h and "mailto:" not in h))
                    website = website_elem["href"] if website_elem else None
                    
                    # If the name element is itself a link, use that
                    if not website and name_elem.name == "a" and "href" in name_elem.attrs:
                        website = name_elem["href"]
                    
                    if not website or not name:
                        continue
                    
                    # Try to find location if available
                    location_text = None
                    for tag in element.find_all(["p", "div", "span", "td"]):
                        text = tag.get_text().strip()
                        # Look for city, state pattern
                        if re.search(r'[A-Za-z\s]+,\s*[A-Z]{2}', text):
                            location_text = text
                            break
                    
                    city = None
                    state_code = None
                    
                    if location_text:
                        # Parse city and state
                        city_state_match = re.search(r'([^,]+),\s*([A-Z]{2})', location_text)
                        if city_state_match:
                            city = city_state_match.group(1).strip()
                            state_code = city_state_match.group(2)
                    
                    # Create firm data
                    firm_data = {
                        "name": name,
                        "website": website,
                        "source_url": source_url
                    }
                    
                    if city:
                        firm_data["city"] = city
                    
                    if state_code:
                        firm_data["state"] = state_code
                    
                    firms.append(firm_data)
                
                except Exception as e:
                    logger.error(f"Error extracting SPE firm: {e}")
            
            # If we still found nothing, try a more general approach
            if not firms:
                # Look for links that might be firm websites
                firm_links = soup.find_all("a", href=lambda h: h and "http" in h and "spe.org" not in h.lower())
                
                for link in firm_links:
                    try:
                        firm_name = link.get_text().strip()
                        if not firm_name or len(firm_name) < 3:
                            continue
                            
                        # Skip obvious non-firm links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word in firm_name.lower() for word in skip_words):
                            continue
                        
                        firms.append({
                            "name": firm_name,
                            "website": link["href"],
                            "source_url": source_url
                        })
                    except Exception as e:
                        logger.error(f"Error extracting firm from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting SPE firms: {e}")
        
        logger.info(f"Found {len(firms)} firms in SPE directory")
        return firms
        
    def _extract_generic_directory_firms(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, Any]]:
        """Extract firms from generic directory pages."""
        firms = []
        
        try:
            # Try to find member listings by common class names
            member_elements = soup.find_all(["div", "li", "article"], class_=lambda c: c and (
                "member" in c.lower() or "listing" in c.lower() or "card" in c.lower() or 
                "item" in c.lower() or "entry" in c.lower() or "result" in c.lower()
            ))
            
            # If that didn't work, try common HTML patterns
            if not member_elements:
                # Try unordered lists
                uls = soup.find_all("ul", class_=lambda c: c and (
                    "member" in c.lower() or "directory" in c.lower() or "list" in c.lower()
                ))
                for ul in uls:
                    member_elements.extend(ul.find_all("li"))
                
                # Try tables
                tables = soup.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    # Skip the first row (likely headers)
                    if len(rows) > 1:
                        member_elements.extend(rows[1:])
            
            # Process each member element
            for element in member_elements:
                try:
                    # Try to extract name
                    name_elem = element.find(["h2", "h3", "h4", "a", "strong", "td"])
                    if not name_elem:
                        continue
                        
                    name = name_elem.get_text().strip()
                    
                    # Try to find website link
                    website_elem = element.find("a", href=lambda h: h and ("http" in h and "mailto:" not in h))
                    website = website_elem["href"] if website_elem else None
                    
                    # If the name element is itself a link, use that
                    if not website and name_elem.name == "a" and "href" in name_elem.attrs:
                        website = name_elem["href"]
                    
                    if not website or not name:
                        continue
                    
                    # Try to find location if available
                    location_text = None
                    for tag in element.find_all(["p", "div", "span", "td"]):
                        text = tag.get_text().strip()
                        # Look for city, state pattern
                        if re.search(r'[A-Za-z\s]+,\s*[A-Z]{2}', text):
                            location_text = text
                            break
                    
                    city = None
                    state_code = None
                    
                    if location_text:
                        # Parse city and state
                        city_state_match = re.search(r'([^,]+),\s*([A-Z]{2})', location_text)
                        if city_state_match:
                            city = city_state_match.group(1).strip()
                            state_code = city_state_match.group(2)
                    
                    # Create firm data
                    firm_data = {
                        "name": name,
                        "website": website,
                        "source_url": source_url
                    }
                    
                    if city:
                        firm_data["city"] = city
                    
                    if state_code:
                        firm_data["state"] = state_code
                    
                    firms.append(firm_data)
                
                except Exception as e:
                    logger.error(f"Error extracting generic directory firm: {e}")
            
            # If we still found nothing, use the most general approach
            if not firms:
                # Look for links that might be firm websites
                firm_links = soup.find_all("a", href=lambda h: h and "http" in h)
                
                for link in firm_links:
                    try:
                        firm_name = link.get_text().strip()
                        if not firm_name or len(firm_name) < 3:
                            continue
                            
                        # Skip obvious non-firm links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "home", "login"]
                        if any(word in firm_name.lower() for word in skip_words):
                            continue
                        
                        # Skip links that point to the same site
                        if "about" in link["href"].lower() or "contact" in link["href"].lower():
                            continue
                        
                        firms.append({
                            "name": firm_name,
                            "website": link["href"],
                            "source_url": source_url
                        })
                    except Exception as e:
                        logger.error(f"Error extracting firm from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting generic directory firms: {e}")
        
        logger.info(f"Found {len(firms)} firms in generic directory")
        return firms
    
    def _scrape_acec_chapters(self) -> List[Dict[str, Any]]:
        """
        Scrape engineering firms from ACEC (American Council of Engineering Companies) state chapters.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Real ACEC chapter URLs - updated with correct URLs 
        state_chapters = {
            "Utah": "https://www.acecofutah.org/page/MemberDirectory",
            "Illinois": "https://www.acecil.org/find-an-engineer/",
            "Arizona": "https://www.acec-az.org/page/MemberDirectory", 
            "Missouri": "https://www.acecmo.org/member-directory/",
            "New Mexico": "https://www.acecnm.org/page/MemberDirectory",
            "Nevada": "https://www.acecnv.org/membership-directory/"
        }
        
        for state, url in state_chapters.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Scraping ACEC chapter for {state} at {url}")
            
            soup = self.get_page(url)
            if not soup:
                continue
            
            # Use our specialized ACEC firm extraction method
            firms = self._extract_acec_firms(soup, url)
            
            for firm in firms:
                try:
                    # Make sure we have a name and website
                    if not firm.get("name") or not firm.get("website"):
                        continue
                    
                    # Add state if not already in the firm data
                    if not firm.get("state"):
                        firm["state"] = state
                        
                    # Add organization type and subtype
                    firm["org_type"] = self.org_type
                    if not firm.get("subtype"):
                        firm["subtype"] = self._determine_engineering_type(firm["name"], firm["website"])
                    
                    # Save organization and get ID
                    org_id = self.save_organization(firm)
                    if not org_id:
                        continue
                    
                    # Scrape contacts from the company website
                    company_contacts = self._scrape_company_website(firm["website"], org_id)
                    contacts.extend(company_contacts)
                    
                    # Track successful contact discovery
                    if company_contacts:
                        logger.info(f"Found {len(company_contacts)} contacts at {firm['name']}")
                    
                    # Avoid overloading the server
                    time.sleep(1.5)
                    
                except Exception as e:
                    logger.error(f"Error processing ACEC firm: {e}")
        
        return contacts
    
    def _scrape_linkedin_engineering_firms(self) -> List[Dict[str, Any]]:
        """
        Find engineering firms via search engine queries that mention LinkedIn.
        Instead of directly scraping LinkedIn (which requires authentication),
        we use search engine results to find engineering firms mentioned on LinkedIn.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Initialize search engine
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, search for engineering firms on LinkedIn
        for state in TARGET_STATES:
            logger.info(f"Finding engineering firms in {state} via LinkedIn mentions")
            
            # Use search queries that will find LinkedIn profiles of engineering firms
            search_queries = [
                f"site:linkedin.com/company engineering firms {state}",
                f"site:linkedin.com/company civil engineering {state}",
                f"site:linkedin.com/company structural engineering {state}",
                f"site:linkedin.com/company mechanical engineering {state}"
            ]
            
            firms_found = {}  # Use dict to avoid duplicates
            
            for query in search_queries:
                try:
                    logger.info(f"Searching for: {query}")
                    search_results = search_engine.execute_search(query, "engineering", state)
                    
                    # Process each search result
                    for result in search_results:
                        try:
                            # Extract URL, title and snippet
                            url = result.get("link", "") or result.get("url", "")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                            
                            if not url or not title:
                                continue
                            
                            # Skip non-company pages
                            if "/company/" not in url.lower():
                                continue
                                
                            # Extract firm name from title
                            # LinkedIn titles typically follow pattern: "Company Name | LinkedIn"
                            firm_name = title.split(" | ")[0].strip()
                            if not firm_name or firm_name.lower() == "linkedin":
                                continue
                            
                            # Skip if this doesn't seem like an engineering firm
                            is_engineering = False
                            engineering_terms = ["engineering", "engineers", "consultants", "consulting"]
                            if any(term.lower() in firm_name.lower() for term in engineering_terms):
                                is_engineering = True
                            elif any(term.lower() in snippet.lower() for term in engineering_terms):
                                is_engineering = True
                                
                            if not is_engineering:
                                continue
                            
                            # Create a unique key to avoid duplicates
                            firm_key = firm_name.lower().strip()
                            
                            # Check if we've already found this firm
                            if firm_key in firms_found:
                                continue
                                
                            # Extract website URL from snippet or search for it
                            website = self._extract_website_from_snippet(snippet)
                            
                            # If we couldn't get a website, try to search for it
                            if not website:
                                website = self._find_website_for_company(firm_name, state)
                                
                            # Skip if we still don't have a website
                            if not website:
                                continue
                                
                            # Create organization data
                            org_data = {
                                "name": firm_name,
                                "org_type": self.org_type,
                                "subtype": self._determine_engineering_type(firm_name, website),
                                "website": website,
                                "state": state,
                                "source_url": url
                            }
                            
                            # Try to get city from the snippet (look for "City, State" pattern)
                            city_match = re.search(r'([A-Za-z\s\.]+),\s*' + state, snippet)
                            if city_match:
                                org_data["city"] = city_match.group(1).strip()
                            
                            # Save in our firms_found dict
                            firms_found[firm_key] = org_data
                            
                            # Rate limiting
                            time.sleep(0.5)
                            
                        except Exception as e:
                            logger.error(f"Error processing LinkedIn search result: {e}")
                            
                except Exception as e:
                    logger.error(f"Error executing LinkedIn search query '{query}': {e}")
            
            # Now process all the firms we found
            logger.info(f"Found {len(firms_found)} unique engineering firms in {state} via LinkedIn mentions")
            
            for firm_data in firms_found.values():
                try:
                    # Save organization and get ID
                    org_id = self.save_organization(firm_data)
                    if not org_id:
                        continue
                    
                    # Get website content if possible
                    website = firm_data.get("website")
                    if not website:
                        continue
                        
                    # Try to update city if we don't have it
                    if not firm_data.get("city"):
                        soup = self.get_page(website)
                        if soup:
                            city = self._extract_city(soup, state)
                            if city:
                                # Update in database
                                from sqlalchemy import update
                                from app.database.models import Organization
                                update_stmt = update(Organization).where(Organization.id == org_id).values(city=city)
                                self.db_session.execute(update_stmt)
                                self.db_session.commit()
                    
                    # Scrape contacts from the company website
                    company_contacts = self._scrape_company_website(website, org_id)
                    if company_contacts:
                        contacts.extend(company_contacts)
                        logger.info(f"Found {len(company_contacts)} contacts at {firm_data['name']}")
                    
                    # Avoid overloading servers
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing LinkedIn-derived firm: {e}")
        
        return contacts
    
    def _extract_website_from_snippet(self, snippet: str) -> Optional[str]:
        """Extract website URL from search result snippet."""
        # Look for URLs in the snippet
        url_pattern = r'https?://(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
        urls = re.findall(url_pattern, snippet)
        
        for url in urls:
            # Skip LinkedIn URLs
            if "linkedin.com" in url:
                continue
            
            # Skip common search and job sites
            skip_domains = ["indeed.com", "glassdoor.com", "monster.com", "ziprecruiter.com", 
                           "google.com", "bing.com", "yahoo.com"]
            if any(domain in url for domain in skip_domains):
                continue
                
            return f"https://{url}"
            
        return None
        
    def _find_website_for_company(self, company_name: str, state: str) -> Optional[str]:
        """Search for a company website using search engine."""
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        query = f"{company_name} {state} official website"
        
        try:
            search_results = search_engine.execute_search(query, "engineering", state)
            
            # Look at top 3 results
            for result in search_results[:3]:
                url = result.get("link", "") or result.get("url", "")
                if not url:
                    continue
                    
                # Skip LinkedIn and job sites
                skip_domains = ["linkedin.com", "indeed.com", "glassdoor.com", "monster.com", 
                              "ziprecruiter.com", "facebook.com", "twitter.com", "instagram.com"]
                if any(domain in url.lower() for domain in skip_domains):
                    continue
                    
                # Skip if URL contains search keywords
                if "search" in url.lower():
                    continue
                    
                return url
                
        except Exception as e:
            logger.error(f"Error searching for company website: {e}")
            
        return None
    
    def _determine_engineering_type(self, name: str, website: str) -> str:
        """
        Determine the type of engineering firm based on name and website.
        
        Args:
            name: Company name
            website: Company website
            
        Returns:
            Engineering subtype
        """
        name = name.lower()
        
        # Simple keyword matching
        if "civil" in name:
            return "civil"
        elif "electrical" in name:
            return "electrical"
        elif "environmental" in name:
            return "environmental"
        elif "mechanical" in name:
            return "mechanical"
        
        # If no match in name, try to fetch and analyze the website
        try:
            soup = self.get_page(website)
            if soup:
                text = soup.text.lower()
                
                if "civil engineering" in text:
                    return "civil"
                elif "electrical engineering" in text:
                    return "electrical" 
                elif "environmental engineering" in text:
                    return "environmental"
                elif "mechanical engineering" in text:
                    return "mechanical"
        except Exception:
            pass
            
        # Default to multidisciplinary if can't determine
        return "multidisciplinary"
    
    def _scrape_company_website(self, website: str, org_id: int) -> List[Dict[str, Any]]:
        """
        Scrape contacts from a company website.
        
        Args:
            website: Company website URL
            org_id: Organization ID in the database
            
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Common paths where staff/team information might be found
        staff_paths = ["team", "about/team", "about-us/team", "our-team", 
                      "staff", "about/staff", "people", "about/people",
                      "about-us", "about", "contact", "leadership"]
        
        # Try each path
        for path in staff_paths:
            url = f"{website.rstrip('/')}/{path}"
            
            try:
                soup = self.get_page(url)
                if not soup:
                    continue
                
                # Look for staff listings
                # This is simplified; actual implementation would need to handle
                # various website structures
                staff_elements = soup.find_all(["div", "article", "section"], 
                                              class_=lambda c: c and any(term in c.lower() 
                                                                         for term in ["team", "staff", "member", "people", "leadership"]))
                
                for element in staff_elements:
                    # Look for job titles that match our target
                    target_titles = ORG_TYPES["engineering"]["job_titles"]
                    
                    # Extract job title, name, and contact info
                    # This is a simplified example
                    title_element = element.find(["h3", "h4", "div", "span"], 
                                               class_=lambda c: c and "title" in c.lower())
                    
                    if not title_element:
                        continue
                        
                    job_title = title_element.text.strip()
                    
                    # Check if job title is relevant
                    if not any(target.lower() in job_title.lower() for target in target_titles):
                        continue
                    
                    # Extract name
                    name_element = element.find(["h2", "h3", "div", "span"], 
                                              class_=lambda c: c and "name" in c.lower())
                    
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
                        "notes": f"Found on {url}"
                    }
                    
                    # Save contact
                    contact_id = self.save_contact(contact_data)
                    if contact_id:
                        contacts.append(contact_data)
            
            except Exception as e:
                logger.error(f"Error scraping company website {url}: {e}")
                
        return contacts