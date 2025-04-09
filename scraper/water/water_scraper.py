"""
Water and wastewater utilities scraper for the GBL Data Contact Management System.
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


class WaterScraper(BaseScraper):
    """Scraper for water and wastewater utilities and districts."""
    
    def __init__(self, db_session):
        """Initialize the water scraper."""
        super().__init__(db_session)
        self.org_type = "water"
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape water utilities and districts from various sources.
        
        Returns:
            List of dictionaries with contact data
        """
        logger.info("Starting water utilities scraping")
        contacts = []
        
        # Scrape from multiple sources
        contacts.extend(self._scrape_water_directories())
        contacts.extend(self._scrape_state_water_agencies())
        contacts.extend(self._scrape_search_based_water_utilities())
        
        logger.info(f"Found {len(contacts)} water/wastewater contacts")
        return contacts

    def _scrape_water_directories(self) -> List[Dict[str, Any]]:
        """
        Scrape water utilities from water association directories.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Real water association directories to scrape
        water_directories = {
            "Utah": [
                "https://www.rwau.net/members-directory",
                "https://www.uasd.org/members.php"
            ],
            "Illinois": [
                "https://www.isawwa.org/utility-directory/",
                "https://www.wateroperator.org/illinois"
            ],
            "Arizona": [
                "https://www.azwater.gov/drinking-water-facilities",
                "https://www.azwwa.org/page/MemberDirectory"
            ],
            "Missouri": [
                "https://www.dnr.mo.gov/water/business-industry/water-utilities",
                "https://moawwa.org/membership/membership-directory/"
            ],
            "New Mexico": [
                "https://www.env.nm.gov/drinking_water/utility-operator-certification-program/",
                "https://nmrwa.org/membership/"
            ],
            "Nevada": [
                "https://ndep.nv.gov/water/drinking-water/utility-link",
                "https://www.nvawwa.org/page/MemberDirectory"
            ]
        }
        
        # Use search engine to find water utilities that may not be in directories
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, use both directories and search
        for state in TARGET_STATES:
            logger.info(f"Finding water utilities in {state}")
            
            # 1. First scrape water directories
            state_directories = water_directories.get(state, [])
            
            for url in state_directories:
                try:
                    logger.info(f"Scraping water directory for {state} at {url}")
                    
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Different parsing strategies based on the URL
                    if "rwau" in url.lower() or "awwa.org" in url.lower() or "isawwa" in url.lower():
                        # Water association sites often use similar formats
                        utilities = self._extract_water_association_utilities(soup, url)
                    elif "wateroperator.org" in url.lower():
                        # WaterOperator.org has a specific format
                        utilities = self._extract_wateroperator_utilities(soup, url, state)
                    elif "azwater.gov" in url.lower() or "dnr.mo.gov" in url.lower() or "env.nm.gov" in url.lower() or "ndep.nv.gov" in url.lower():
                        # Government sites with lists of regulated utilities
                        utilities = self._extract_government_water_utilities(soup, url, state)
                    else:
                        # Generic approach
                        utilities = self._extract_generic_directory_utilities(soup, url, state)
                    
                    # Process each utility found
                    for utility in utilities:
                        try:
                            # Validate utility data
                            if not utility.get("name"):
                                continue
                            
                            # Set default state if not in utility data
                            if not utility.get("state"):
                                utility["state"] = state
                                
                            # Make sure we have the org_type set
                            utility["org_type"] = self.org_type
                            
                            # Add website if missing but we have enough info to search for it
                            if not utility.get("website") and utility.get("name"):
                                utility["website"] = self._find_website_for_utility(utility["name"], state)
                            
                            # If still no website, skip since we need it for contact discovery
                            if not utility.get("website"):
                                continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(utility)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the website
                            utility_contacts = self._scrape_utility_website(utility.get("website"), org_id)
                            contacts.extend(utility_contacts)
                            
                            # Track success for logging
                            if utility_contacts:
                                logger.info(f"Found {len(utility_contacts)} contacts at {utility['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing water utility: {e}")
                    
                except Exception as e:
                    logger.error(f"Error scraping water directory {url}: {e}")
            
            # 2. Then use search queries to find additional water utilities
            search_queries = [
                f"water district {state}",
                f"water utility {state}",
                f"water treatment plant {state}",
                f"municipal water {state}",
                f"wastewater treatment {state}"
            ]
            
            for query in search_queries:
                try:
                    logger.info(f"Searching for: {query}")
                    search_results = search_engine.execute_search(query, "water", state)
                    
                    # Process each search result
                    for result in search_results:
                        try:
                            # Extract URL, title and snippet
                            url = result.get("link", "") or result.get("url", "")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                            
                            if not url:
                                continue
                                
                            # Skip if this clearly isn't a water utility website
                            skip_domains = ["linkedin.com", "indeed.com", "ziprecruiter.com", "monster.com", 
                                          "glassdoor.com", "wikipedia.org", "yelp.com", "yellowpages.com"]
                            if any(domain in url.lower() for domain in skip_domains):
                                continue
                                
                            # Attempt to determine if this is a water utility or district
                            is_water_utility = False
                            water_terms = ["water", "wastewater", "utility", "district", "treatment", "public works"]
                            if any(term.lower() in title.lower() for term in water_terms):
                                is_water_utility = True
                            elif any(term.lower() in snippet.lower() for term in water_terms):
                                is_water_utility = True
                            
                            if not is_water_utility:
                                continue
                                
                            # Check for competitor indicators (companies selling water treatment products)
                            competitor_indicators = ["water treatment products", "equipment supplier", "chemical supplier", 
                                                   "equipment manufacturer", "consulting services", "distributor"]
                            if any(indicator.lower() in title.lower() for indicator in competitor_indicators) or \
                               any(indicator.lower() in snippet.lower() for indicator in competitor_indicators):
                                continue
                                
                            # Create organization data - we'll get city later from website if possible
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
                                # Try to extract a better company name from the website
                                better_name = self._extract_utility_name(soup)
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
                            
                            # Scrape contacts from the utility website
                            utility_contacts = self._scrape_utility_website(url, org_id)
                            if utility_contacts:
                                contacts.extend(utility_contacts)
                                logger.info(f"Found {len(utility_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading servers
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing search result: {e}")
                    
                except Exception as e:
                    logger.error(f"Error executing search query '{query}': {e}")
        
        return contacts
    
    def _scrape_state_water_agencies(self) -> List[Dict[str, Any]]:
        """
        Scrape water information from state environmental and water resource agencies.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # State water and environmental agency URLs
        state_agencies = {
            "Utah": [
                "https://deq.utah.gov/division-drinking-water",
                "https://naturalresources.utah.gov/water-resources"
            ],
            "Illinois": [
                "https://www2.illinois.gov/epa/topics/water-quality/Pages/default.aspx",
                "https://www.isws.illinois.edu/"
            ],
            "Arizona": [
                "https://new.azwater.gov/",
                "https://azdeq.gov/programs/water-programs"
            ],
            "Missouri": [
                "https://dnr.mo.gov/water/",
                "https://mdc.mo.gov/conservation/watershed"
            ],
            "New Mexico": [
                "https://www.ose.state.nm.us/",
                "https://www.env.nm.gov/water/"
            ],
            "Nevada": [
                "https://ndep.nv.gov/water",
                "https://dcnr.nv.gov/divisions-programs/water"
            ]
        }
        
        # For each target state, check state agencies
        for state, urls in state_agencies.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Checking state water agencies for {state}")
            
            for url in urls:
                try:
                    logger.info(f"Accessing state agency at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Look for lists or directories of water utilities
                    # Most state agencies have pages with links to regulated utilities or districts
                    utility_links = self._extract_utility_links_from_agency(soup, url, state)
                    
                    # Process utility links
                    for utility_link in utility_links:
                        try:
                            # Get the utility page content
                            utility_soup = self.get_page(utility_link.get("url"))
                            if not utility_soup:
                                continue
                            
                            # Extract organization data
                            org_data = {
                                "name": utility_link.get("name"),
                                "org_type": self.org_type,
                                "website": utility_link.get("url"),
                                "state": state,
                                "source_url": url
                            }
                            
                            # Try to extract city/location
                            city = self._extract_city(utility_soup, state)
                            if city:
                                org_data["city"] = city
                            
                            # Save organization and get ID
                            org_id = self.save_organization(org_data)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the utility website
                            utility_contacts = self._scrape_utility_website(utility_link.get("url"), org_id)
                            contacts.extend(utility_contacts)
                            
                            # Track success
                            if utility_contacts:
                                logger.info(f"Found {len(utility_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing utility link: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing state agency {url}: {e}")
        
        return contacts
    
    def _scrape_search_based_water_utilities(self) -> List[Dict[str, Any]]:
        """
        Find water utilities using search engine with advanced queries.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Initialize search engine
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # Advanced search queries targeting specific types of water utilities
        for state in TARGET_STATES:
            logger.info(f"Searching for specific water utility types in {state}")
            
            # More specific search queries for different types of water utilities
            specialized_queries = [
                f"water reclamation district {state}",
                f"regional water authority {state}",
                f"rural water district {state}",
                f"metropolitan water district {state}",
                f"municipal wastewater treatment {state}",
                f"water conservation district {state}",
                f"county water department {state}",
                f"irrigation district {state} water",
                f"water improvement district {state}"
            ]
            
            # Counties with population centers - add specific county searches
            major_counties = {
                "Utah": ["Salt Lake", "Utah", "Davis", "Weber", "Washington"],
                "Illinois": ["Cook", "DuPage", "Will", "Lake", "Kane"],
                "Arizona": ["Maricopa", "Pima", "Pinal", "Yavapai", "Mohave"],
                "Missouri": ["Jackson", "St. Louis", "St. Charles", "Greene", "Clay"],
                "New Mexico": ["Bernalillo", "Doña Ana", "Santa Fe", "Sandoval", "San Juan"],
                "Nevada": ["Clark", "Washoe", "Elko", "Carson City", "Douglas"]
            }
            
            for county in major_counties.get(state, []):
                specialized_queries.append(f"{county} county water utility {state}")
                specialized_queries.append(f"{county} county water district {state}")
            
            # Process each search query
            for query in specialized_queries:
                try:
                    logger.info(f"Executing specialized search: {query}")
                    search_results = search_engine.execute_search(query, "water", state)
                    
                    # Process each search result
                    for result in search_results:
                        try:
                            # Extract URL, title and snippet
                            url = result.get("link", "") or result.get("url", "")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                            
                            if not url:
                                continue
                                
                            # Skip if this clearly isn't a water utility website
                            skip_domains = ["linkedin.com", "indeed.com", "ziprecruiter.com", "monster.com", 
                                          "glassdoor.com", "wikipedia.org", "yelp.com", "yellowpages.com"]
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
                                # Try to extract a better company name from the website
                                better_name = self._extract_utility_name(soup)
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
                            
                            # Scrape contacts from the utility website
                            utility_contacts = self._scrape_utility_website(url, org_id)
                            if utility_contacts:
                                contacts.extend(utility_contacts)
                                logger.info(f"Found {len(utility_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading servers
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing specialized search result: {e}")
                
                except Exception as e:
                    logger.error(f"Error executing specialized search query '{query}': {e}")
        
        return contacts
    
    def _extract_water_association_utilities(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, Any]]:
        """Extract utilities from water association directory pages."""
        utilities = []
        
        try:
            # Association sites often have member listings or directories
            member_elements = soup.find_all(["div", "article", "li"], class_=lambda c: c and 
                                        ("member" in c.lower() or "directory-item" in c.lower() or 
                                         "listing" in c.lower() or "utility" in c.lower()))
            
            # If that didn't work, try table rows
            if not member_elements:
                tables = soup.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    # Skip the header row
                    member_elements.extend(rows[1:])
            
            # If still nothing, try cards or list items with links
            if not member_elements:
                member_elements = soup.find_all(["div", "li"], class_=lambda c: c and 
                                             ("card" in c.lower() or "list-item" in c.lower()))
            
            # Process each member element
            for element in member_elements:
                try:
                    # Try to extract name - usually in a heading, link or strong tag
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
                    
                    if not name:
                        continue
                    
                    # Try to find city/location if available
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
                    
                    # Create utility data
                    utility_data = {
                        "name": name,
                        "source_url": source_url
                    }
                    
                    if website:
                        utility_data["website"] = website
                        
                    if city:
                        utility_data["city"] = city
                    
                    if state_code:
                        utility_data["state"] = state_code
                    
                    utilities.append(utility_data)
                
                except Exception as e:
                    logger.error(f"Error extracting water association utility: {e}")
            
            # If we still found nothing, general fallback to link extraction
            if not utilities:
                # Look for links that might be utility websites
                utility_links = soup.find_all("a", href=lambda h: h and "http" in h)
                
                for link in utility_links:
                    try:
                        utility_name = link.get_text().strip()
                        if not utility_name or len(utility_name) < 3:
                            continue
                            
                        # Skip obvious non-utility links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word in utility_name.lower() for word in skip_words):
                            continue
                            
                        # Check if this looks like a water utility name
                        water_terms = ["water", "district", "utility", "county", "city of", "municipal"]
                        has_water_term = any(term in utility_name.lower() for term in water_terms)
                        
                        if not has_water_term:
                            continue
                        
                        utilities.append({
                            "name": utility_name,
                            "website": link["href"],
                            "source_url": source_url
                        })
                    except Exception as e:
                        logger.error(f"Error extracting utility from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting water association utilities: {e}")
        
        logger.info(f"Found {len(utilities)} utilities in water association directory")
        return utilities
    
    def _extract_wateroperator_utilities(self, soup: BeautifulSoup, source_url: str, state: str) -> List[Dict[str, Any]]:
        """Extract utilities from WaterOperator.org directory."""
        utilities = []
        
        try:
            # WaterOperator.org uses specific HTML structures
            utility_sections = soup.find_all("div", class_=lambda c: c and "utility-item" in c.lower())
            
            if not utility_sections:
                utility_sections = soup.find_all("div", class_=lambda c: c and "result-item" in c.lower())
            
            if not utility_sections:
                # Try a more generic approach
                utility_sections = soup.find_all(["div", "section"], id=lambda i: i and "results" in i.lower())
                if utility_sections:
                    for section in utility_sections:
                        utility_sections = section.find_all(["div", "article", "li"], class_=lambda c: c and 
                                                        ("item" in c.lower() or "result" in c.lower()))
            
            # Process utility sections
            for section in utility_sections:
                try:
                    # Extract name
                    name_elem = section.find(["h2", "h3", "h4", "strong", "a"])
                    if not name_elem:
                        continue
                        
                    name = name_elem.get_text().strip()
                    
                    # Try to find website
                    website_elem = section.find("a", href=lambda h: h and "http" in h and "mailto:" not in h)
                    website = website_elem["href"] if website_elem else None
                    
                    # Extract location information
                    location = None
                    location_elem = section.find(["p", "div", "span"], text=lambda t: t and ("," in t or state in t))
                    if location_elem:
                        location = location_elem.get_text().strip()
                    
                    city = None
                    if location:
                        # Try to extract city from location text
                        city_match = re.search(r'([^,]+),\s*' + state, location)
                        if city_match:
                            city = city_match.group(1).strip()
                    
                    # Create utility data
                    utility_data = {
                        "name": name,
                        "state": state,
                        "source_url": source_url
                    }
                    
                    if website:
                        utility_data["website"] = website
                        
                    if city:
                        utility_data["city"] = city
                    
                    utilities.append(utility_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting WaterOperator.org utility: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting WaterOperator.org utilities: {e}")
        
        logger.info(f"Found {len(utilities)} utilities in WaterOperator.org directory")
        return utilities
    
    def _extract_government_water_utilities(self, soup: BeautifulSoup, source_url: str, state: str) -> List[Dict[str, Any]]:
        """Extract utilities from government agency pages."""
        utilities = []
        
        try:
            # Government sites often have tables of regulated utilities
            tables = soup.find_all("table")
            
            for table in tables:
                rows = table.find_all("tr")
                
                # Skip header row
                for row in rows[1:]:
                    try:
                        cells = row.find_all(["td", "th"])
                        if len(cells) < 2:
                            continue
                        
                        # Try to determine which cell has the utility name
                        name_cell = cells[0]  # Usually the first column
                        
                        # Check if there's a link in the cell
                        link = name_cell.find("a")
                        if link:
                            name = link.get_text().strip()
                            href = link.get("href")
                            
                            if href and href.startswith("http"):
                                website = href
                            else:
                                website = None
                        else:
                            name = name_cell.get_text().strip()
                            website = None
                        
                        # Try to find location information
                        location = None
                        for cell in cells[1:]:  # Check other columns for location info
                            text = cell.get_text().strip()
                            if re.search(r'[A-Za-z\s]+,\s*[A-Z]{2}', text) or state in text:
                                location = text
                                break
                        
                        city = None
                        if location:
                            city_match = re.search(r'([^,]+),\s*' + state, location)
                            if city_match:
                                city = city_match.group(1).strip()
                        
                        # Create utility data
                        utility_data = {
                            "name": name,
                            "state": state,
                            "source_url": source_url
                        }
                        
                        if website:
                            utility_data["website"] = website
                            
                        if city:
                            utility_data["city"] = city
                        
                        utilities.append(utility_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting government utility from table row: {e}")
            
            # If no tables found, try lists
            if not utilities:
                # Try to find lists of utilities
                lists = soup.find_all(["ul", "ol"])
                
                for lst in lists:
                    list_items = lst.find_all("li")
                    
                    for item in list_items:
                        try:
                            # Check if the list item has a link
                            link = item.find("a")
                            if link:
                                name = link.get_text().strip()
                                href = link.get("href")
                                
                                if href and href.startswith("http"):
                                    website = href
                                else:
                                    website = None
                            else:
                                name = item.get_text().strip()
                                website = None
                            
                            # Check if this looks like a water utility name
                            water_terms = ["water", "district", "utility", "county", "city of", "municipal"]
                            has_water_term = any(term in name.lower() for term in water_terms)
                            
                            if not has_water_term:
                                continue
                            
                            # Create utility data
                            utility_data = {
                                "name": name,
                                "state": state,
                                "source_url": source_url
                            }
                            
                            if website:
                                utility_data["website"] = website
                            
                            utilities.append(utility_data)
                            
                        except Exception as e:
                            logger.error(f"Error extracting government utility from list item: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting government water utilities: {e}")
        
        logger.info(f"Found {len(utilities)} utilities in government agency page")
        return utilities
    
    def _extract_generic_directory_utilities(self, soup: BeautifulSoup, source_url: str, state: str) -> List[Dict[str, Any]]:
        """Extract utilities from generic directory pages."""
        utilities = []
        
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
                    
                    # Check if this looks like a water utility name
                    water_terms = ["water", "district", "utility", "county", "city of", "municipal", "treatment"]
                    has_water_term = any(term in name.lower() for term in water_terms)
                    
                    if not has_water_term:
                        continue
                    
                    # Try to find website link
                    website_elem = element.find("a", href=lambda h: h and ("http" in h and "mailto:" not in h))
                    website = website_elem["href"] if website_elem else None
                    
                    # If the name element is itself a link, use that
                    if not website and name_elem.name == "a" and "href" in name_elem.attrs:
                        website = name_elem["href"]
                    
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
                    
                    # Create utility data
                    utility_data = {
                        "name": name,
                        "source_url": source_url
                    }
                    
                    if website:
                        utility_data["website"] = website
                        
                    if city:
                        utility_data["city"] = city
                    
                    if state_code:
                        utility_data["state"] = state_code
                    else:
                        utility_data["state"] = state
                    
                    utilities.append(utility_data)
                
                except Exception as e:
                    logger.error(f"Error extracting generic directory utility: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting generic directory utilities: {e}")
        
        logger.info(f"Found {len(utilities)} utilities in generic directory")
        return utilities
    
    def _extract_utility_links_from_agency(self, soup: BeautifulSoup, source_url: str, state: str) -> List[Dict[str, Any]]:
        """Extract utility links from state agency pages."""
        utility_links = []
        
        try:
            # Look for links to water utilities
            links = soup.find_all("a", href=lambda h: h and "http" in h)
            
            for link in links:
                try:
                    text = link.get_text().strip()
                    href = link.get("href")
                    
                    if not text or not href:
                        continue
                    
                    # Skip if the link text seems like navigation
                    skip_words = ["home", "about", "contact", "search", "login", "back", "next", 
                                 "previous", "services", "programs"]
                    if any(word == text.lower() for word in skip_words):
                        continue
                    
                    # Check if this looks like a water utility
                    water_terms = ["water", "district", "utility", "department", "division", "authority"]
                    waste_terms = ["wastewater", "sewer", "treatment"]
                    location_terms = ["city of", "town of", "county", "municipal"]
                    
                    # Check if the link text contains water or location terms
                    has_water_term = any(term in text.lower() for term in water_terms)
                    has_waste_term = any(term in text.lower() for term in waste_terms)
                    has_location_term = any(term in text.lower() for term in location_terms)
                    
                    # If it's clearly a water utility, add it
                    if (has_water_term or has_waste_term) and (has_location_term or len(text) > 10):
                        utility_links.append({
                            "name": text,
                            "url": href,
                            "source_url": source_url
                        })
                        
                except Exception as e:
                    logger.error(f"Error processing potential utility link: {e}")
            
            # Try to find specialized sections that might contain utility information
            utility_sections = soup.find_all(["div", "section"], 
                                          id=lambda i: i and any(term in i.lower() for term in 
                                                              ["utility", "water", "directory", "providers"]))
            
            if not utility_sections:
                utility_sections = soup.find_all(["div", "section"], 
                                             class_=lambda c: c and any(term in c.lower() for term in 
                                                                     ["utility", "water", "directory", "providers"]))
            
            # Process each utility section
            for section in utility_sections:
                section_links = section.find_all("a", href=lambda h: h and "http" in h)
                
                for link in section_links:
                    try:
                        text = link.get_text().strip()
                        href = link.get("href")
                        
                        if not text or not href:
                            continue
                        
                        # Since these are from utility-specific sections, we can be less strict
                        utility_links.append({
                            "name": text,
                            "url": href,
                            "source_url": source_url
                        })
                        
                    except Exception as e:
                        logger.error(f"Error processing utility section link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting utility links from agency page: {e}")
        
        logger.info(f"Found {len(utility_links)} potential utility links from agency page")
        return utility_links
    
    def _extract_utility_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract utility name from website content."""
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
            
            # Look for logo alt text
            logo = soup.find("img", {"id": "logo"}) or soup.find("img", {"class": "logo"})
            if logo and logo.get("alt") and len(logo["alt"]) > 3:
                return logo["alt"].strip()
            
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
            
            # Check the title tag
            if soup.title:
                title = soup.title.text.strip()
                # Remove common suffixes
                suffixes = [" - Home", " | Home", " - Official Website", " | Official Website"]
                for suffix in suffixes:
                    if title.endswith(suffix):
                        return title[:-len(suffix)].strip()
                
                return title
        
        except Exception as e:
            logger.error(f"Error extracting utility name: {e}")
            
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
    
    def _find_website_for_utility(self, utility_name: str, state: str) -> Optional[str]:
        """Search for a utility website using search engine."""
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # Create a search query
        query = f"{utility_name} {state} official website"
        
        try:
            search_results = search_engine.execute_search(query, "water", state)
            
            # Look at top 3 results
            for result in search_results[:3]:
                url = result.get("link", "") or result.get("url", "")
                if not url:
                    continue
                    
                # Skip social media, job sites, etc.
                skip_domains = ["linkedin.com", "facebook.com", "twitter.com", "indeed.com", 
                              "glassdoor.com", "ziprecruiter.com", "monster.com"]
                if any(domain in url.lower() for domain in skip_domains):
                    continue
                    
                # Skip if URL contains search keywords
                if "search" in url.lower():
                    continue
                    
                return url
                
        except Exception as e:
            logger.error(f"Error searching for utility website: {e}")
            
        return None
    
    def _scrape_utility_website(self, website: str, org_id: int) -> List[Dict[str, Any]]:
        """
        Scrape contacts from a utility website.
        
        Args:
            website: Utility website URL
            org_id: Organization ID in the database
            
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Common paths where staff/team information might be found
        staff_paths = ["staff", "about/staff", "about-us/staff", "team", "about/team", "about-us/team",
                      "about/leadership", "about-us/leadership", "leadership", "management", "board", 
                      "contact-us", "contact", "directory", "department", "departments", "about/departments"]
        
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
                                                                              "management", "department"]))
                
                for element in staff_elements:
                    # Look for job titles that match our target
                    target_titles = ORG_TYPES["water"]["job_titles"]
                    
                    # Extract job title, name, and contact info
                    title_element = element.find(["h3", "h4", "div", "span"], 
                                             class_=lambda c: c and ("title" in c.lower() or "position" in c.lower()))
                    
                    if not title_element:
                        continue
                        
                    job_title = title_element.text.strip()
                    
                    # Check if job title is relevant
                    relevant_title = any(target.lower() in job_title.lower() for target in target_titles)
                    
                    # Also check for operations and manager/director roles which are important for water utilities
                    operations_role = "operations" in job_title.lower() or "operator" in job_title.lower()
                    management_role = "manager" in job_title.lower() or "director" in job_title.lower()
                    engineering_role = "engineer" in job_title.lower()
                    
                    if not (relevant_title or operations_role or management_role or engineering_role):
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
                        "contact_relevance_score": 9.0 if management_role or operations_role else 7.0,
                        "notes": f"Found on {url}"
                    }
                    
                    # Save contact
                    contact_id = self.save_contact(contact_data)
                    if contact_id:
                        contacts.append(contact_data)
            
            except Exception as e:
                logger.error(f"Error scraping utility website {url}: {e}")
                
        return contacts