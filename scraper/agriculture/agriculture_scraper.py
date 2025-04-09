"""
Agriculture and irrigation districts scraper for the GBL Data Contact Management System.
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


class AgricultureScraper(BaseScraper):
    """Scraper for agriculture operations and irrigation districts."""
    
    def __init__(self, db_session):
        """Initialize the agriculture scraper."""
        super().__init__(db_session)
        self.org_type = "agriculture"
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape agriculture organizations and irrigation districts from various sources.
        
        Returns:
            List of dictionaries with contact data
        """
        logger.info("Starting agriculture and irrigation districts scraping")
        contacts = []
        
        # Scrape from multiple sources
        contacts.extend(self._scrape_irrigation_districts())
        contacts.extend(self._scrape_agriculture_associations())
        contacts.extend(self._scrape_state_agriculture_agencies())
        contacts.extend(self._scrape_search_based_agriculture())
        
        logger.info(f"Found {len(contacts)} agriculture and irrigation district contacts")
        return contacts

    def _scrape_irrigation_districts(self) -> List[Dict[str, Any]]:
        """
        Scrape irrigation districts and water conservation districts.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Irrigation district directory URLs
        irrigation_directories = {
            "National": [
                "https://www.irrigation.org/IA/Resources/Directories/Find-an-IA-Irrigation-Contractor/",
                "https://watereducation.org/resources/water-districts-water-agencies"
            ],
            "Utah": [
                "https://water.utah.gov/irrigation-districts/",
                "https://conservewater.utah.gov/partners-and-links/"
            ],
            "Illinois": [
                "https://www2.illinois.gov/dnr/WaterResources/Pages/SWConservationDistricts.aspx",
                "https://www2.illinois.gov/sites/agr/Resources/FarmPrograms/soil-and-water/Pages/default.aspx"
            ],
            "Arizona": [
                "https://new.azwater.gov/irrigation-districts",
                "https://new.azwater.gov/conservation/agricultural"
            ],
            "Missouri": [
                "https://dnr.mo.gov/land-geology/water-resources/irrigation-information",
                "https://mosoilandwater.land/soil-and-water-conservation-districts"
            ],
            "New Mexico": [
                "https://www.ose.state.nm.us/IWRS/regions.php",
                "https://www.nmda.nmsu.edu/natural-resources/soil-and-water-conservation-districts/"
            ],
            "Nevada": [
                "https://water.nv.gov/water-organizations",
                "https://dcnr.nv.gov/divisions-programs/conservation-districts-program"
            ]
        }
        
        # Process each directory
        for region, urls in irrigation_directories.items():
            for url in urls:
                try:
                    logger.info(f"Accessing irrigation district directory at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Extract irrigation districts
                    districts = self._extract_irrigation_districts_from_directory(soup, url, region)
                    
                    # Process each district
                    for district in districts:
                        try:
                            # Skip if it doesn't have a name
                            if not district.get("name"):
                                continue
                            
                            # Filter to our target states
                            if district.get("state") and district["state"] not in TARGET_STATES:
                                continue
                                
                            # If no state specified but region is a specific state, use that
                            if not district.get("state") and region in TARGET_STATES:
                                district["state"] = region
                            
                            # Skip if still no state or not in our target states
                            if not district.get("state") or district["state"] not in TARGET_STATES:
                                continue
                                
                            # Make sure we have the org_type set
                            district["org_type"] = self.org_type
                            
                            # If we don't have a website, try to find one
                            if not district.get("website") and district.get("name"):
                                district["website"] = self._find_website_for_district(district["name"], district["state"])
                            
                            # Skip if still no website since we need it for contact discovery
                            if not district.get("website"):
                                continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(district)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the district website
                            district_contacts = self._scrape_agriculture_website(district.get("website"), org_id)
                            contacts.extend(district_contacts)
                            
                            # Track success for logging
                            if district_contacts:
                                logger.info(f"Found {len(district_contacts)} contacts at {district['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing irrigation district: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing irrigation directory {url}: {e}")
        
        return contacts
    
    def _scrape_agriculture_associations(self) -> List[Dict[str, Any]]:
        """
        Scrape agriculture associations and farm bureaus.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Agriculture association URLs
        agriculture_associations = {
            "National": [
                "https://www.fb.org/about/join/state-farm-bureaus",
                "https://irrigationshow.org/explore-exhibitors"
            ],
            "Utah": [
                "https://utahfarmbureau.org/",
                "https://ag.utah.gov/related-links/"
            ],
            "Illinois": [
                "https://www.ilfb.org/resources/",
                "https://www2.illinois.gov/sites/agr/Pages/default.aspx"
            ],
            "Arizona": [
                "https://www.azfb.org/Member-Benefits/",
                "https://agriculture.az.gov/resources"
            ],
            "Missouri": [
                "https://mofb.org/about/county-farm-bureaus/",
                "https://agriculture.mo.gov/connect/associations/"
            ],
            "New Mexico": [
                "https://www.nmfb.org/About/County-Farm-Bureaus",
                "https://www.nmda.nmsu.edu/home/agriculture-industry-resources/"
            ],
            "Nevada": [
                "https://nvfb.org/",
                "https://agri.nv.gov/Agriculture/Ag_Resources_Overview/"
            ]
        }
        
        # Process each association
        for region, urls in agriculture_associations.items():
            for url in urls:
                try:
                    logger.info(f"Accessing agriculture association at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Extract agriculture organizations
                    organizations = self._extract_agriculture_orgs_from_directory(soup, url, region)
                    
                    # Process each organization
                    for organization in organizations:
                        try:
                            # Skip if it doesn't have a name
                            if not organization.get("name"):
                                continue
                            
                            # Filter to our target states
                            if organization.get("state") and organization["state"] not in TARGET_STATES:
                                continue
                                
                            # If no state specified but region is a specific state, use that
                            if not organization.get("state") and region in TARGET_STATES:
                                organization["state"] = region
                            
                            # Skip if still no state or not in our target states
                            if not organization.get("state") or organization["state"] not in TARGET_STATES:
                                continue
                                
                            # Make sure we have the org_type set
                            organization["org_type"] = self.org_type
                            
                            # If we don't have a website, try to find one
                            if not organization.get("website") and organization.get("name"):
                                organization["website"] = self._find_website_for_district(organization["name"], organization["state"])
                            
                            # Skip if still no website since we need it for contact discovery
                            if not organization.get("website"):
                                continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(organization)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the organization website
                            org_contacts = self._scrape_agriculture_website(organization.get("website"), org_id)
                            contacts.extend(org_contacts)
                            
                            # Track success for logging
                            if org_contacts:
                                logger.info(f"Found {len(org_contacts)} contacts at {organization['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing agriculture organization: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing agriculture association {url}: {e}")
        
        return contacts
    
    def _scrape_state_agriculture_agencies(self) -> List[Dict[str, Any]]:
        """
        Scrape state agriculture departments and agencies.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # State agriculture department URLs
        state_agencies = {
            "Utah": [
                {"name": "Utah Department of Agriculture and Food", "website": "https://ag.utah.gov/"}
            ],
            "Illinois": [
                {"name": "Illinois Department of Agriculture", "website": "https://www2.illinois.gov/sites/agr/"}
            ],
            "Arizona": [
                {"name": "Arizona Department of Agriculture", "website": "https://agriculture.az.gov/"}
            ],
            "Missouri": [
                {"name": "Missouri Department of Agriculture", "website": "https://agriculture.mo.gov/"}
            ],
            "New Mexico": [
                {"name": "New Mexico Department of Agriculture", "website": "https://www.nmda.nmsu.edu/"}
            ],
            "Nevada": [
                {"name": "Nevada Department of Agriculture", "website": "https://agri.nv.gov/"}
            ]
        }
        
        # Process each state agency
        for state, agencies in state_agencies.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Processing agriculture agencies in {state}")
            
            for agency in agencies:
                try:
                    # Create organization data
                    org_data = {
                        "name": agency["name"],
                        "org_type": self.org_type,
                        "website": agency["website"],
                        "state": state
                    }
                    
                    # Save organization and get ID
                    org_id = self.save_organization(org_data)
                    if not org_id:
                        continue
                    
                    # Scrape contacts from the agency website
                    agency_contacts = self._scrape_agriculture_website(agency["website"], org_id)
                    
                    # If no contacts found on main page, try common department paths
                    if not agency_contacts:
                        department_paths = [
                            "about/staff", "staff", "about/leadership", "leadership", 
                            "about/directory", "directory", "contact-us", "contact",
                            "divisions/agriculture-water-quality", "water-quality",
                            "water-conservation", "irrigation", "divisions/water"
                        ]
                        
                        for path in department_paths:
                            url = f"{agency['website'].rstrip('/')}/{path}"
                            path_contacts = self._scrape_agriculture_website(url, org_id)
                            if path_contacts:
                                agency_contacts.extend(path_contacts)
                                logger.info(f"Found {len(path_contacts)} contacts at {url}")
                                # Give the server a break between requests
                                time.sleep(1)
                    
                    contacts.extend(agency_contacts)
                    
                    # Track success for logging
                    if agency_contacts:
                        logger.info(f"Found {len(agency_contacts)} contacts at {agency['name']}")
                    
                    # Avoid overloading the server
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing agriculture agency {agency['name']}: {e}")
        
        return contacts
    
    def _scrape_search_based_agriculture(self) -> List[Dict[str, Any]]:
        """
        Find agriculture organizations using search engine.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Initialize search engine
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, search for agriculture organizations
        for state in TARGET_STATES:
            logger.info(f"Searching for agriculture and irrigation organizations in {state}")
            
            # Define search queries
            search_queries = [
                f"irrigation district {state}",
                f"water conservation district {state}",
                f"large farms {state}",
                f"agricultural water management {state}",
                f"farm water systems {state}",
                f"precision agriculture {state}",
                f"irrigation system companies {state}",
                f"center pivot irrigation {state}",
                f"agricultural irrigation {state}"
            ]
            
            for query in search_queries:
                try:
                    logger.info(f"Executing search: {query}")
                    search_results = search_engine.execute_search(query, "agriculture", state)
                    
                    # Process each search result
                    for result in search_results:
                        try:
                            # Extract URL, title and snippet
                            url = result.get("link", "") or result.get("url", "")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                            
                            if not url or not title:
                                continue
                                
                            # Skip social media, job sites, etc.
                            skip_domains = ["linkedin.com", "facebook.com", "twitter.com", "indeed.com", 
                                          "glassdoor.com", "wikipedia.org", "yelp.com", "yellowpages.com"]
                            if any(domain in url.lower() for domain in skip_domains):
                                continue
                                
                            # Check if this looks like an agriculture or irrigation organization
                            agriculture_indicators = ["farm", "agriculture", "irrigation", "water", "conservation", 
                                                    "district", "crop", "agricultural", "farming"]
                            is_agriculture = False
                            
                            # Check title
                            if any(indicator in title.lower() for indicator in agriculture_indicators):
                                is_agriculture = True
                                
                            # Check snippet if title doesn't have indicators
                            if not is_agriculture and any(indicator in snippet.lower() for indicator in agriculture_indicators):
                                is_agriculture = True
                                
                            if not is_agriculture:
                                continue
                                
                            # Skip if this appears to be a news article or blog post
                            news_indicators = ["news", "article", "blog", "press release"]
                            if any(indicator in title.lower() for indicator in news_indicators):
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
                                # Try to extract a better organization name from the website
                                better_name = self._extract_agriculture_name(soup)
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
                            
                            # Scrape contacts from the organization website
                            org_contacts = self._scrape_agriculture_website(url, org_id)
                            if org_contacts:
                                contacts.extend(org_contacts)
                                logger.info(f"Found {len(org_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading servers
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing search result: {e}")
                    
                except Exception as e:
                    logger.error(f"Error executing search query '{query}': {e}")
        
        return contacts
    
    def _extract_irrigation_districts_from_directory(self, soup: BeautifulSoup, source_url: str, region: str) -> List[Dict[str, Any]]:
        """Extract irrigation districts from directory pages."""
        districts = []
        
        try:
            # Try to find district listings
            district_elements = []
            
            # Try table rows first
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                # Skip header row
                district_elements.extend(rows[1:])
            
            # If no tables, try list items
            if not district_elements:
                list_elements = soup.find_all(["ul", "ol"])
                for list_elem in list_elements:
                    items = list_elem.find_all("li")
                    # Add list items with content
                    for item in items:
                        if item.get_text().strip() and (item.find("a") or len(item.get_text().strip()) > 10):
                            district_elements.append(item)
            
            # If still no results, try sections or divs with potential district listings
            if not district_elements:
                district_elements = soup.find_all(["div", "section", "article"], 
                                              class_=lambda c: c and any(term in (c.lower() if c else "") 
                                                                     for term in ["district", "irrigation", "water", 
                                                                                "member", "listing", "directory"]))
            
            # Process each element
            for element in district_elements:
                try:
                    # Extract district name
                    district_name = None
                    name_element = element.find(["a", "h2", "h3", "h4", "strong", "td"])
                    if name_element:
                        district_name = name_element.get_text().strip()
                    else:
                        # Try to just get the text of the element if it's not too long
                        element_text = element.get_text().strip()
                        if 3 < len(element_text) < 100:  # Avoid getting long paragraphs or very short text
                            district_name = element_text
                    
                    if not district_name:
                        continue
                        
                    # Skip if it clearly isn't a district name
                    skip_words = ["click", "here", "more", "info", "search", "back", "next", "previous"]
                    if any(word == district_name.lower() for word in skip_words):
                        continue
                    
                    # Check if this looks like an irrigation or water district
                    water_terms = ["irrigation", "water", "district", "conservation", "agricultural"]
                    has_water_term = False
                    for term in water_terms:
                        if term in district_name.lower():
                            has_water_term = True
                            break
                    
                    if not has_water_term:
                        # Skip if it doesn't look like a water/irrigation district
                        continue
                    
                    # Extract website link
                    website = None
                    link_element = element.find("a")
                    if link_element and "href" in link_element.attrs:
                        href = link_element["href"]
                        # Make absolute URL if needed
                        if href.startswith("/"):
                            from urllib.parse import urlparse
                            parsed_url = urlparse(source_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = base_url + href
                        website = href
                    
                    # Try to extract state information
                    state = None
                    for target_state in TARGET_STATES:
                        if target_state in element.get_text():
                            state = target_state
                            break
                    
                    # Create district data
                    district_data = {
                        "name": district_name,
                        "source_url": source_url
                    }
                    
                    if website:
                        district_data["website"] = website
                        
                    if state:
                        district_data["state"] = state
                    
                    districts.append(district_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting irrigation district from element: {e}")
            
            # If we couldn't find districts in structured elements, try extracting all links
            if not districts:
                links = soup.find_all("a", href=lambda h: h and 
                                   not any(term in str(h).lower() for term in ["mailto:", "javascript:", "#"]))
                
                for link in links:
                    try:
                        district_name = link.get_text().strip()
                        href = link["href"]
                        
                        # Skip if it doesn't look like a district name
                        if not district_name or len(district_name) < 3:
                            continue
                            
                        # Skip obvious non-district links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word == district_name.lower() for word in skip_words):
                            continue
                            
                        # Check if this looks like an irrigation or water district
                        water_terms = ["irrigation", "water", "district", "conservation"]
                        has_water_term = False
                        for term in water_terms:
                            if term in district_name.lower():
                                has_water_term = True
                                break
                        
                        if not has_water_term:
                            # Skip if it doesn't look like a water/irrigation district
                            continue
                        
                        # Make absolute URL if needed
                        if href.startswith("/"):
                            from urllib.parse import urlparse
                            parsed_url = urlparse(source_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = base_url + href
                        
                        # Try to extract state information
                        state = None
                        for target_state in TARGET_STATES:
                            if target_state in link.get_text() or (link.parent and target_state in link.parent.get_text()):
                                state = target_state
                                break
                        
                        district_data = {
                            "name": district_name,
                            "website": href,
                            "source_url": source_url
                        }
                        
                        if state:
                            district_data["state"] = state
                        
                        districts.append(district_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting irrigation district from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting irrigation districts from directory: {e}")
        
        logger.info(f"Found {len(districts)} irrigation districts on directory page {source_url}")
        return districts
    
    def _extract_agriculture_orgs_from_directory(self, soup: BeautifulSoup, source_url: str, region: str) -> List[Dict[str, Any]]:
        """Extract agriculture organizations from directory pages."""
        organizations = []
        
        try:
            # Try to find organization listings
            org_elements = []
            
            # Try table rows first
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                # Skip header row
                org_elements.extend(rows[1:])
            
            # If no tables, try list items
            if not org_elements:
                list_elements = soup.find_all(["ul", "ol"])
                for list_elem in list_elements:
                    items = list_elem.find_all("li")
                    # Add list items with content
                    for item in items:
                        if item.get_text().strip() and (item.find("a") or len(item.get_text().strip()) > 10):
                            org_elements.append(item)
            
            # If still no results, try sections or divs with potential organization listings
            if not org_elements:
                org_elements = soup.find_all(["div", "section", "article"], 
                                          class_=lambda c: c and any(term in (c.lower() if c else "") 
                                                                 for term in ["member", "organization", "farm", 
                                                                            "agriculture", "listing", "directory"]))
            
            # Process each element
            for element in org_elements:
                try:
                    # Extract organization name
                    org_name = None
                    name_element = element.find(["a", "h2", "h3", "h4", "strong", "td"])
                    if name_element:
                        org_name = name_element.get_text().strip()
                    else:
                        # Try to just get the text of the element if it's not too long
                        element_text = element.get_text().strip()
                        if 3 < len(element_text) < 100:  # Avoid getting long paragraphs or very short text
                            org_name = element_text
                    
                    if not org_name:
                        continue
                        
                    # Skip if it clearly isn't an organization name
                    skip_words = ["click", "here", "more", "info", "search", "back", "next", "previous"]
                    if any(word == org_name.lower() for word in skip_words):
                        continue
                    
                    # Extract website link
                    website = None
                    link_element = element.find("a")
                    if link_element and "href" in link_element.attrs:
                        href = link_element["href"]
                        # Make absolute URL if needed
                        if href.startswith("/"):
                            from urllib.parse import urlparse
                            parsed_url = urlparse(source_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = base_url + href
                        website = href
                    
                    # Try to extract state information
                    state = None
                    for target_state in TARGET_STATES:
                        if target_state in element.get_text():
                            state = target_state
                            break
                    
                    # Create organization data
                    org_data = {
                        "name": org_name,
                        "source_url": source_url
                    }
                    
                    if website:
                        org_data["website"] = website
                        
                    if state:
                        org_data["state"] = state
                    
                    organizations.append(org_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting agriculture organization from element: {e}")
            
            # If we couldn't find organizations in structured elements, try extracting all links
            if not organizations:
                links = soup.find_all("a", href=lambda h: h and 
                                   not any(term in str(h).lower() for term in ["mailto:", "javascript:", "#"]))
                
                for link in links:
                    try:
                        org_name = link.get_text().strip()
                        href = link["href"]
                        
                        # Skip if it doesn't look like an organization name
                        if not org_name or len(org_name) < 3:
                            continue
                            
                        # Skip obvious non-organization links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word == org_name.lower() for word in skip_words):
                            continue
                        
                        # Make absolute URL if needed
                        if href.startswith("/"):
                            from urllib.parse import urlparse
                            parsed_url = urlparse(source_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = base_url + href
                        
                        # Try to extract state information
                        state = None
                        for target_state in TARGET_STATES:
                            if target_state in link.get_text() or (link.parent and target_state in link.parent.get_text()):
                                state = target_state
                                break
                        
                        # Create organization data
                        org_data = {
                            "name": org_name,
                            "website": href,
                            "source_url": source_url
                        }
                        
                        if state:
                            org_data["state"] = state
                        
                        organizations.append(org_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting agriculture organization from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting agriculture organizations from directory: {e}")
        
        logger.info(f"Found {len(organizations)} agriculture organizations on directory page {source_url}")
        return organizations
    
    def _extract_agriculture_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract organization name from website content."""
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
                suffixes = [" - Home", " | Home", " - Official Website", " | Official Website", " | Official Site"]
                for suffix in suffixes:
                    if title.endswith(suffix):
                        return title[:-len(suffix)].strip()
                
                return title
        
        except Exception as e:
            logger.error(f"Error extracting agriculture organization name: {e}")
            
        return None
    
    def _extract_city(self, soup: BeautifulSoup, state: str) -> Optional[str]:
        """Extract city from website content using the state as context."""
        try:
            # Look for schema.org data first
            org_schema = soup.find("script", {"type": "application/ld+json"})
            if org_schema:
                import json
                try:
                    data = json.loads(org_schema.string)
                    # Organization markup might have address
                    if isinstance(data, dict):
                        if data.get("address", {}).get("addressLocality"):
                            return data["address"]["addressLocality"]
                        elif data.get("location", {}).get("address", {}).get("addressLocality"):
                            return data["location"]["address"]["addressLocality"]
                except:
                    pass
            
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
    
    def _find_website_for_district(self, district_name: str, state: str) -> Optional[str]:
        """Search for a district website using search engine."""
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # Create a search query
        query = f"{district_name} {state} official website"
        
        try:
            search_results = search_engine.execute_search(query, "agriculture", state)
            
            # Look at top 3 results
            for result in search_results[:3]:
                url = result.get("link", "") or result.get("url", "")
                if not url:
                    continue
                    
                # Skip social media, job sites, etc.
                skip_domains = ["linkedin.com", "facebook.com", "twitter.com", "indeed.com", 
                              "glassdoor.com", "yelp.com", "yellowpages.com", "wikipedia.org"]
                if any(domain in url.lower() for domain in skip_domains):
                    continue
                    
                # Skip obvious news sites
                news_domains = ["news", "article", "press", "blog"]
                if any(domain in url.lower() for domain in news_domains):
                    continue
                    
                return url
                    
        except Exception as e:
            logger.error(f"Error searching for district website: {e}")
            
        return None
    
    def _scrape_agriculture_website(self, website: str, org_id: int) -> List[Dict[str, Any]]:
        """
        Scrape contacts from an agriculture or irrigation district website.
        
        Args:
            website: District website URL
            org_id: Organization ID in the database
            
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Common paths where staff/leadership information might be found
        staff_paths = ["about/leadership", "about-us/leadership", "leadership", "management", 
                      "about/management", "about-us/management", "board", "about/board",
                      "about-us/board", "directors", "board-of-directors", "about/team", 
                      "about-us/team", "team", "about/staff", "about-us/staff", "staff", 
                      "contact-us", "contact", "about/contact", "about-us/contact",
                      "about/directory", "about-us/directory", "directory"]
        
        # Additional paths for agriculture and irrigation specific departments
        agriculture_paths = ["operations", "about/operations", "about-us/operations",
                           "water", "water-management", "irrigation", "irrigation-management",
                           "departments/operations", "departments/water", "departments/irrigation",
                           "services/irrigation", "services/water", "water-delivery", "water-distribution"]
        
        # Combine all paths
        all_paths = staff_paths + agriculture_paths
        
        # Try each path
        for path in all_paths:
            url = f"{website.rstrip('/')}/{path}"
            
            try:
                soup = self.get_page(url)
                if not soup:
                    continue
                
                # Look for staff listings
                staff_elements = soup.find_all(["div", "article", "section"], 
                                            class_=lambda c: c and any(term in (c.lower() if c else "") 
                                                                   for term in ["team", "staff", "member", "people", 
                                                                              "leadership", "board", "director", 
                                                                              "management"]))
                
                if not staff_elements:
                    # Try finding elements by ID
                    staff_elements = soup.find_all(["div", "article", "section"], 
                                                id=lambda i: i and any(term in (i.lower() if i else "")
                                                                     for term in ["team", "staff", "leadership", 
                                                                                "board", "director", "management", 
                                                                                "directory"]))
                
                # If still nothing found, look for department sections
                if not staff_elements:
                    staff_elements = soup.find_all(["div", "article", "section"], 
                                                id=lambda i: i and any(term in (i.lower() if i else "")
                                                                     for term in ["operations", "water", 
                                                                                "irrigation", "delivery", 
                                                                                "distribution"]))
                    if not staff_elements:
                        staff_elements = soup.find_all(["div", "article", "section"], 
                                                    class_=lambda c: c and any(term in (c.lower() if c else "")
                                                                           for term in ["operations", "water", 
                                                                                      "irrigation", "delivery", 
                                                                                      "distribution"]))
                
                # If still nothing, try a more general approach with the whole page
                if not staff_elements:
                    staff_elements = [soup]
                
                for element in staff_elements:
                    # Look for job titles that match our target
                    target_titles = ORG_TYPES["agriculture"]["job_titles"]
                    
                    # Try to find staff listings within this element
                    staff_listings = element.find_all(["div", "article", "li"], 
                                                   class_=lambda c: c and any(term in (c.lower() if c else "")
                                                                          for term in ["staff", "person", "member", 
                                                                                     "employee", "leader", "director", 
                                                                                     "board", "manager"]))
                    
                    # If no specific staff listings found, look for biographical sections
                    if not staff_listings:
                        staff_listings = element.find_all(["div", "article", "section"], 
                                                       class_=lambda c: c and any(term in (c.lower() if c else "")
                                                                              for term in ["bio", "profile", "card", 
                                                                                         "personnel", "vcard"]))
                    
                    # If still nothing, use the whole element
                    if not staff_listings:
                        staff_listings = [element]
                    
                    for staff in staff_listings:
                        try:
                            # Extract job title
                            title_element = staff.find(["h3", "h4", "div", "span", "p"], 
                                                   class_=lambda c: c and ("title" in (c.lower() if c else "") or 
                                                                        "position" in (c.lower() if c else "") or
                                                                        "role" in (c.lower() if c else "")))
                            
                            if not title_element:
                                # Try finding title by text pattern
                                title_patterns = ["Director", "Manager", "Supervisor", "Superintendent", 
                                               "President", "Vice President", "VP", 
                                               "Water", "Irrigation", "Operations", "District"]
                                for pattern in title_patterns:
                                    title_elements = staff.find_all(text=lambda t: pattern in t if t else False)
                                    if title_elements:
                                        for t in title_elements:
                                            parent = t.parent
                                            if parent.name in ["p", "div", "span", "h3", "h4", "h5"]:
                                                title_element = parent
                                                break
                                        if title_element:
                                            break
                            
                            if not title_element:
                                continue
                                
                            job_title = title_element.text.strip()
                            
                            # Check if job title is relevant (water management, operations, or board roles)
                            relevant_title = any(target.lower() in job_title.lower() for target in target_titles)
                            
                            # Also check for specific roles which are important for agriculture and irrigation districts
                            water_role = "water" in job_title.lower()
                            irrigation_role = "irrigation" in job_title.lower()
                            operations_role = "operations" in job_title.lower() or "operator" in job_title.lower()
                            management_role = "manager" in job_title.lower() or "director" in job_title.lower()
                            board_role = "board" in job_title.lower() or "president" in job_title.lower()
                            
                            if not (relevant_title or water_role or irrigation_role or operations_role or management_role or board_role):
                                continue
                            
                            # Extract name
                            name_element = staff.find(["h2", "h3", "div", "span"], 
                                                   class_=lambda c: c and "name" in (c.lower() if c else ""))
                            
                            if not name_element:
                                # Try other common elements that might contain names
                                name_element = staff.find(["h2", "h3", "h4", "strong"])
                            
                            if name_element:
                                full_name = name_element.text.strip()
                                # Split into first and last name
                                name_parts = full_name.split(" ", 1)
                                first_name = name_parts[0]
                                last_name = name_parts[1] if len(name_parts) > 1 else ""
                            else:
                                continue
                            
                            # Extract email
                            email_element = staff.find("a", href=lambda h: h and "mailto:" in h)
                            email = email_element["href"].replace("mailto:", "") if email_element else None
                            
                            # Extract phone
                            phone_element = staff.find("a", href=lambda h: h and "tel:" in h)
                            phone = phone_element["href"].replace("tel:", "") if phone_element else None
                            
                            # If no phone found via tel: link, try text pattern
                            if not phone:
                                phone_patterns = staff.find_all(text=lambda t: t and re.search(r'\(\d{3}\)\s*\d{3}-\d{4}', t))
                                if phone_patterns:
                                    for pattern in phone_patterns:
                                        phone_match = re.search(r'\(\d{3}\)\s*\d{3}-\d{4}', pattern)
                                        if phone_match:
                                            phone = phone_match.group(0)
                                            break
                            
                            # Calculate relevance score - higher for water and operations roles
                            relevance_score = 7.0  # Base score
                            if water_role or irrigation_role:
                                relevance_score = 9.0
                            elif operations_role:
                                relevance_score = 8.5
                            elif management_role:
                                relevance_score = 8.0
                            
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
                                "contact_relevance_score": relevance_score,
                                "notes": f"Found on {url}"
                            }
                            
                            # Save contact
                            contact_id = self.save_contact(contact_data)
                            if contact_id:
                                contacts.append(contact_data)
                                
                        except Exception as e:
                            logger.error(f"Error processing staff listing: {e}")
            
            except Exception as e:
                logger.error(f"Error scraping agriculture website {url}: {e}")
                
        return contacts