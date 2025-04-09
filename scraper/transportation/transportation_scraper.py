"""
Transportation authorities scraper for the GBL Data Contact Management System.
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


class TransportationScraper(BaseScraper):
    """Scraper for transportation authorities and agencies."""
    
    def __init__(self, db_session):
        """Initialize the transportation scraper."""
        super().__init__(db_session)
        self.org_type = "transportation"
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape transportation authorities from various sources.
        
        Returns:
            List of dictionaries with contact data
        """
        logger.info("Starting transportation authorities scraping")
        contacts = []
        
        # Scrape from multiple sources
        contacts.extend(self._scrape_state_dots())
        contacts.extend(self._scrape_transit_agencies())
        contacts.extend(self._scrape_transportation_directories())
        contacts.extend(self._scrape_search_based_transportation())
        
        logger.info(f"Found {len(contacts)} transportation authority contacts")
        return contacts

    def _scrape_state_dots(self) -> List[Dict[str, Any]]:
        """
        Scrape state Departments of Transportation (DOTs).
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # State DOT websites
        state_dots = {
            "Utah": {
                "name": "Utah Department of Transportation (UDOT)",
                "website": "https://udot.utah.gov/"
            },
            "Illinois": {
                "name": "Illinois Department of Transportation (IDOT)",
                "website": "https://idot.illinois.gov/"
            },
            "Arizona": {
                "name": "Arizona Department of Transportation (ADOT)",
                "website": "https://azdot.gov/"
            },
            "Missouri": {
                "name": "Missouri Department of Transportation (MoDOT)",
                "website": "https://www.modot.org/"
            },
            "New Mexico": {
                "name": "New Mexico Department of Transportation (NMDOT)",
                "website": "https://dot.state.nm.us/"
            },
            "Nevada": {
                "name": "Nevada Department of Transportation (NDOT)",
                "website": "https://www.dot.nv.gov/"
            }
        }
        
        # For each target state, find DOT contacts
        for state, dot_info in state_dots.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Processing {state} DOT")
            
            try:
                # Create organization data
                org_data = {
                    "name": dot_info["name"],
                    "org_type": self.org_type,
                    "website": dot_info["website"],
                    "state": state
                }
                
                # Save organization and get ID
                org_id = self.save_organization(org_data)
                if not org_id:
                    continue
                
                # Scrape contacts from the DOT website
                # Try different department paths relevant to SCADA systems
                department_paths = [
                    "about/leadership", "about-us/leadership", "leadership",
                    "divisions/operations", "operations", "traffic-operations",
                    "maintenance", "its", "intelligent-transportation-systems",
                    "traffic-management", "traffic-management-center",
                    "engineering", "technology", "systems", "technical-services",
                    "infrastructure", "facilities", "facilities-management"
                ]
                
                all_dot_contacts = []
                
                for path in department_paths:
                    url = f"{dot_info['website'].rstrip('/')}/{path}"
                    
                    try:
                        logger.info(f"Checking {dot_info['name']} at {url}")
                        
                        # Scrape contacts from the DOT department
                        department_contacts = self._scrape_transportation_website(url, org_id)
                        if department_contacts:
                            all_dot_contacts.extend(department_contacts)
                            logger.info(f"Found {len(department_contacts)} contacts in {path} department")
                            
                        # Avoid overloading the server
                        time.sleep(2)
                        
                    except Exception as e:
                        logger.error(f"Error accessing DOT department {url}: {e}")
                
                # Also try the main contact page if we didn't find many contacts in departments
                if len(all_dot_contacts) < 5:
                    try:
                        contact_url = f"{dot_info['website'].rstrip('/')}/contact"
                        contact_contacts = self._scrape_transportation_website(contact_url, org_id)
                        if contact_contacts:
                            all_dot_contacts.extend(contact_contacts)
                            logger.info(f"Found {len(contact_contacts)} contacts on contact page")
                    except Exception as e:
                        logger.error(f"Error accessing DOT contact page: {e}")
                
                contacts.extend(all_dot_contacts)
                logger.info(f"Found {len(all_dot_contacts)} contacts total at {dot_info['name']}")
                
            except Exception as e:
                logger.error(f"Error processing {state} DOT: {e}")
        
        return contacts
    
    def _scrape_transit_agencies(self) -> List[Dict[str, Any]]:
        """
        Scrape major transit agencies in target states.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Major transit agencies in target states
        transit_agencies = {
            "Utah": [
                {"name": "Utah Transit Authority (UTA)", "website": "https://www.rideuta.com/"},
                {"name": "Park City Transit", "website": "https://www.parkcity.org/departments/transit-bus"},
                {"name": "Cache Valley Transit District", "website": "https://cvtdbus.org/"}
            ],
            "Illinois": [
                {"name": "Champaign-Urbana Mass Transit District", "website": "https://mtd.org/"},
                {"name": "Springfield Mass Transit District", "website": "https://www.smtd.org/"},
                {"name": "Madison County Transit", "website": "https://www.mct.org/"}
            ],
            "Arizona": [
                {"name": "Valley Metro", "website": "https://www.valleymetro.org/"},
                {"name": "Sun Tran (Tucson)", "website": "https://www.suntran.com/"},
                {"name": "Mountain Line (Flagstaff)", "website": "https://mountainline.az.gov/"}
            ],
            "Missouri": [
                {"name": "Bi-State Development/Metro Transit", "website": "https://www.metrostlouis.org/"},
                {"name": "Kansas City Area Transportation Authority", "website": "https://www.kcata.org/"},
                {"name": "City Utilities Transit (Springfield)", "website": "https://www.cutransit.net/"}
            ],
            "New Mexico": [
                {"name": "ABQ RIDE", "website": "https://www.cabq.gov/transit"},
                {"name": "Santa Fe Trails", "website": "https://www.santafenm.gov/transit"},
                {"name": "Rio Metro Regional Transit District", "website": "https://www.riometro.org/"}
            ],
            "Nevada": [
                {"name": "Regional Transportation Commission of Southern Nevada", "website": "https://www.rtcsnv.com/"},
                {"name": "Regional Transportation Commission of Washoe County", "website": "https://www.rtcwashoe.com/"},
                {"name": "Carson City JAC Transit", "website": "https://carson.org/government/departments-a-f/community-development/operations/public-transit"}
            ]
        }
        
        # Process each transit agency
        for state, agencies in transit_agencies.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Processing transit agencies in {state}")
            
            for agency in agencies:
                try:
                    # Create organization data
                    org_data = {
                        "name": agency["name"],
                        "org_type": self.org_type,
                        "website": agency["website"],
                        "state": state
                    }
                    
                    # Try to get the city from website
                    soup = self.get_page(agency["website"])
                    if soup:
                        city = self._extract_city(soup, state)
                        if city:
                            org_data["city"] = city
                    
                    # Save organization and get ID
                    org_id = self.save_organization(org_data)
                    if not org_id:
                        continue
                    
                    # Scrape contacts from the transit agency website
                    agency_contacts = self._scrape_transportation_website(agency["website"], org_id)
                    
                    # If we didn't find contacts on the main page, try common paths
                    if not agency_contacts:
                        common_paths = ["about/leadership", "leadership", "staff", "contact", "about/staff", "directory"]
                        for path in common_paths:
                            url = f"{agency['website'].rstrip('/')}/{path}"
                            path_contacts = self._scrape_transportation_website(url, org_id)
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
                    logger.error(f"Error processing transit agency {agency['name']}: {e}")
        
        return contacts
    
    def _scrape_transportation_directories(self) -> List[Dict[str, Any]]:
        """
        Scrape transportation authorities from directories and associations.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Transportation directory and association URLs
        transportation_directories = {
            "National": [
                "https://www.apta.com/research-technical-resources/public-transportation-links/links/",
                "https://www.transportation.gov/administrations",
                "https://www.ntdprogram.gov/ntdprogram/links.htm",
                "https://www.aashto.org/state-transportation-websites/"
            ],
            "Utah": [
                "https://www.udot.utah.gov/connect/public/local-government-transportation-resources/"
            ],
            "Illinois": [
                "https://idot.illinois.gov/transportation-system/local-transportation-partners/index"
            ],
            "Arizona": [
                "https://azdot.gov/planning/transportation-programs/transit-planning/transit-providers"
            ],
            "Missouri": [
                "https://www.modot.org/transit-providers"
            ],
            "New Mexico": [
                "https://dot.state.nm.us/content/nmdot/en/Transit_Rail.html"
            ],
            "Nevada": [
                "https://www.dot.nv.gov/mobility/transit"
            ]
        }
        
        # For each directory/association, extract transportation authorities
        for region, urls in transportation_directories.items():
            for url in urls:
                try:
                    logger.info(f"Accessing transportation directory at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Extract transportation authorities
                    authorities = self._extract_transportation_from_directory(soup, url, region)
                    
                    # Process each authority
                    for authority in authorities:
                        try:
                            # Skip if it doesn't have a name
                            if not authority.get("name"):
                                continue
                            
                            # Filter to our target states
                            if authority.get("state") and authority["state"] not in TARGET_STATES:
                                continue
                                
                            # Skip if we can't determine which target state it belongs to
                            if not authority.get("state"):
                                # Try to determine state from the name or website
                                state_found = False
                                for target_state in TARGET_STATES:
                                    if target_state in authority.get("name", "") or (authority.get("website") and target_state in authority["website"]):
                                        authority["state"] = target_state
                                        state_found = True
                                        break
                                        
                                if not state_found:
                                    continue  # Skip if we can't associate with a target state
                            
                            # Make sure we have the org_type set
                            authority["org_type"] = self.org_type
                            
                            # If we don't have a website, try to find one
                            if not authority.get("website") and authority.get("name"):
                                authority["website"] = self._find_website_for_authority(authority["name"], authority["state"])
                            
                            # Skip if still no website since we need it for contact discovery
                            if not authority.get("website"):
                                continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(authority)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the authority website
                            authority_contacts = self._scrape_transportation_website(authority.get("website"), org_id)
                            contacts.extend(authority_contacts)
                            
                            # Track success for logging
                            if authority_contacts:
                                logger.info(f"Found {len(authority_contacts)} contacts at {authority['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing transportation authority: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing transportation directory {url}: {e}")
        
        return contacts
    
    def _scrape_search_based_transportation(self) -> List[Dict[str, Any]]:
        """
        Find transportation authorities using search engine.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Initialize search engine
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, search for transportation authorities
        for state in TARGET_STATES:
            logger.info(f"Searching for transportation authorities in {state}")
            
            # Define search queries
            search_queries = [
                f"transportation authority {state}",
                f"transit agency {state}",
                f"traffic management {state}",
                f"regional transportation {state}",
                f"airport authority {state}",
                f"toll road authority {state}",
                f"bridge authority {state}",
                f"highway department {state}"
            ]
            
            for query in search_queries:
                try:
                    logger.info(f"Executing search: {query}")
                    search_results = search_engine.execute_search(query, "transportation", state)
                    
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
                                
                            # Check if this looks like a transportation authority
                            transport_indicators = ["transportation", "transit", "traffic", "highway", "airport", 
                                                  "toll", "bridge", "road", "rail", "bus", "metro"]
                            is_transport = False
                            
                            # Check title
                            if any(indicator in title.lower() for indicator in transport_indicators):
                                is_transport = True
                                
                            # Check snippet if title doesn't have indicators
                            if not is_transport and any(indicator in snippet.lower() for indicator in transport_indicators):
                                is_transport = True
                                
                            if not is_transport:
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
                                better_name = self._extract_transportation_name(soup)
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
                            
                            # Scrape contacts from the transportation authority website
                            authority_contacts = self._scrape_transportation_website(url, org_id)
                            if authority_contacts:
                                contacts.extend(authority_contacts)
                                logger.info(f"Found {len(authority_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading servers
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing search result: {e}")
                    
                except Exception as e:
                    logger.error(f"Error executing search query '{query}': {e}")
        
        return contacts
    
    def _extract_transportation_from_directory(self, soup: BeautifulSoup, source_url: str, region: str) -> List[Dict[str, Any]]:
        """Extract transportation authorities from directory pages."""
        authorities = []
        
        try:
            # Try to find authority listings
            authority_elements = []
            
            # Try table rows first
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                # Skip header row
                utility_elements = rows[1:]
                
                # Process each row - if it has links, it might be an authority listing
                for row in utility_elements:
                    if row.find("a"):
                        authority_elements.append(row)
            
            # If no tables with links, try list items
            if not authority_elements:
                list_elements = soup.find_all(["ul", "ol"])
                for list_elem in list_elements:
                    items = list_elem.find_all("li")
                    # Add only items with links
                    for item in items:
                        if item.find("a"):
                            authority_elements.append(item)
            
            # If still no results, try sections or divs with potential authority listings
            if not authority_elements:
                authority_elements = soup.find_all(["div", "section", "article"], 
                                              class_=lambda c: c and any(term in (c.lower() if c else "") 
                                                                     for term in ["transit", "transportation", "agency", 
                                                                                "authority", "provider", "member", "listing"]))
            
            # Process each element
            for element in authority_elements:
                try:
                    # Extract authority name
                    authority_name = None
                    name_element = element.find(["a", "h2", "h3", "h4", "strong", "td"])
                    if name_element:
                        authority_name = name_element.get_text().strip()
                    else:
                        # Try to just get the text of the element if it's not too long
                        element_text = element.get_text().strip()
                        if len(element_text) < 100:  # Avoid getting long paragraphs
                            authority_name = element_text
                    
                    if not authority_name:
                        continue
                        
                    # Skip if it clearly isn't an authority name
                    skip_words = ["click", "here", "more", "info", "search", "back", "next", "previous"]
                    if any(word == authority_name.lower() for word in skip_words):
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
                    
                    # If region is one of our target states, use that if no state found
                    if not state and region in TARGET_STATES:
                        state = region
                    
                    # Create authority data
                    authority_data = {
                        "name": authority_name,
                        "source_url": source_url
                    }
                    
                    if website:
                        authority_data["website"] = website
                        
                    if state:
                        authority_data["state"] = state
                    
                    authorities.append(authority_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting transportation authority from element: {e}")
            
            # If we couldn't find authorities in structured elements, try extracting all links
            if not authorities:
                links = soup.find_all("a", href=lambda h: h and 
                                   not any(term in str(h).lower() for term in ["mailto:", "javascript:", "#"]))
                
                for link in links:
                    try:
                        authority_name = link.get_text().strip()
                        href = link["href"]
                        
                        # Skip if it doesn't look like an authority name
                        if not authority_name or len(authority_name) < 3:
                            continue
                            
                        # Skip obvious non-authority links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word == authority_name.lower() for word in skip_words):
                            continue
                            
                        # Check if it looks like a transportation authority name
                        transport_terms = ["transit", "transportation", "traffic", "highway", "airport", "toll", "bridge"]
                        if not any(term in authority_name.lower() for term in transport_terms):
                            continue
                        
                        # Make absolute URL if needed
                        if href.startswith("/"):
                            from urllib.parse import urlparse
                            parsed_url = urlparse(source_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = base_url + href
                        
                        # Try to extract state information
                        state = None
                        parent_text = ""
                        if link.parent:
                            parent_text = link.parent.get_text()
                            
                        for target_state in TARGET_STATES:
                            if target_state in parent_text or target_state in authority_name:
                                state = target_state
                                break
                        
                        # If region is one of our target states, use that if no state found
                        if not state and region in TARGET_STATES:
                            state = region
                        
                        authority_data = {
                            "name": authority_name,
                            "website": href,
                            "source_url": source_url
                        }
                        
                        if state:
                            authority_data["state"] = state
                        
                        authorities.append(authority_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting transportation authority from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting transportation authorities from directory: {e}")
        
        logger.info(f"Found {len(authorities)} transportation authorities on directory page {source_url}")
        return authorities
    
    def _extract_transportation_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract transportation authority name from website content."""
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
            logger.error(f"Error extracting transportation authority name: {e}")
            
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
    
    def _find_website_for_authority(self, authority_name: str, state: str) -> Optional[str]:
        """Search for a transportation authority website using search engine."""
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # Create a search query
        query = f"{authority_name} {state} official website"
        
        try:
            search_results = search_engine.execute_search(query, "transportation", state)
            
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
            logger.error(f"Error searching for transportation authority website: {e}")
            
        return None
    
    def _scrape_transportation_website(self, website: str, org_id: int) -> List[Dict[str, Any]]:
        """
        Scrape contacts from a transportation authority website.
        
        Args:
            website: Transportation authority website URL
            org_id: Organization ID in the database
            
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Common paths where staff/leadership information might be found
        staff_paths = ["about/leadership", "about-us/leadership", "leadership", "management", 
                      "about/management", "about-us/management", "executives", "about/executives",
                      "about-us/executives", "about/team", "about-us/team", "team", "about/staff",
                      "about-us/staff", "staff", "contact-us", "contact", "about/contact",
                      "about-us/contact", "about/directory", "about-us/directory", "directory"]
        
        # Additional paths for transportation-specific departments
        transportation_paths = ["operations", "about/operations", "about-us/operations",
                              "engineering", "about/engineering", "about-us/engineering",
                              "traffic-management", "traffic-operations", "its",
                              "technical-services", "technology", "systems",
                              "departments/operations", "departments/engineering",
                              "departments/technical-services"]
        
        # Combine all paths
        all_paths = staff_paths + transportation_paths
        
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
                                                                              "leadership", "executive", "management",
                                                                              "director", "officer"]))
                
                if not staff_elements:
                    # Try finding elements by ID
                    staff_elements = soup.find_all(["div", "article", "section"], 
                                                id=lambda i: i and any(term in (i.lower() if i else "")
                                                                     for term in ["team", "staff", "leadership", 
                                                                                "executive", "management", "directory"]))
                
                # If still nothing found, look for department sections
                if not staff_elements:
                    staff_elements = soup.find_all(["div", "article", "section"], 
                                                id=lambda i: i and any(term in (i.lower() if i else "")
                                                                     for term in ["operations", "engineering", 
                                                                                "technical", "system", "control", 
                                                                                "traffic", "its"]))
                    if not staff_elements:
                        staff_elements = soup.find_all(["div", "article", "section"], 
                                                    class_=lambda c: c and any(term in (c.lower() if c else "")
                                                                           for term in ["operations", "engineering", 
                                                                                      "technical", "system", "control", 
                                                                                      "traffic", "its"]))
                
                # If still nothing, try a more general approach with the whole page
                if not staff_elements:
                    staff_elements = [soup]
                
                for element in staff_elements:
                    # Look for job titles that match our target
                    target_titles = ORG_TYPES["transportation"]["job_titles"]
                    
                    # Try to find staff listings within this element
                    staff_listings = element.find_all(["div", "article", "li"], 
                                                   class_=lambda c: c and any(term in (c.lower() if c else "")
                                                                          for term in ["staff", "person", "member", 
                                                                                     "employee", "leader", "director", 
                                                                                     "executive", "manager"]))
                    
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
                                               "Engineer", "Operator", "President", "Vice President", "VP", 
                                               "Chief", "Officer", "Traffic", "Operations", "Systems"]
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
                            
                            # Check if job title is relevant (operations, traffic management, or systems roles)
                            relevant_title = any(target.lower() in job_title.lower() for target in target_titles)
                            
                            # Also check for specific roles which are important for transportation authorities
                            operations_role = "operations" in job_title.lower() or "operator" in job_title.lower()
                            traffic_role = "traffic" in job_title.lower()
                            systems_role = "system" in job_title.lower() or "its" in job_title.lower()
                            engineering_role = "engineer" in job_title.lower() or "engineering" in job_title.lower()
                            management_role = "manager" in job_title.lower() or "director" in job_title.lower()
                            
                            if not (relevant_title or operations_role or traffic_role or systems_role or engineering_role or management_role):
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
                            
                            # Calculate relevance score - higher for operations, traffic and systems roles
                            relevance_score = 7.0  # Base score
                            if operations_role or traffic_role:
                                relevance_score = 9.0
                            elif systems_role:
                                relevance_score = 8.5
                            elif engineering_role:
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
                logger.error(f"Error scraping transportation website {url}: {e}")
                
        return contacts