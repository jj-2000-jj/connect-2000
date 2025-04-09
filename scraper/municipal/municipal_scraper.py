"""
Municipal entities scraper for the GBL Data Contact Management System.
"""
import time
import re
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import requests
from app.scraper.base import BaseScraper
from app.config import TARGET_STATES, ORG_TYPES, ILLINOIS_SOUTH_OF_I80
from app.utils.logger import get_logger

logger = get_logger(__name__)


class MunicipalScraper(BaseScraper):
    """Scraper for municipalities and cities."""
    
    def __init__(self, db_session):
        """Initialize the municipal scraper."""
        super().__init__(db_session)
        self.org_type = "municipal"
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape municipalities from various sources.
        
        Returns:
            List of dictionaries with contact data
        """
        logger.info("Starting municipal scraping")
        contacts = []
        
        # Scrape from multiple sources
        contacts.extend(self._scrape_municipal_leagues())
        contacts.extend(self._scrape_major_cities())
        contacts.extend(self._scrape_search_based_municipalities())
        
        logger.info(f"Found {len(contacts)} municipal contacts")
        return contacts

    def _scrape_municipal_leagues(self) -> List[Dict[str, Any]]:
        """
        Scrape municipalities from state municipal leagues and associations.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # State municipal league/association websites
        municipal_leagues = {
            "Utah": [
                "https://www.ulct.org/membership/member-directory",
                "https://www.ulct.org/directory"
            ],
            "Illinois": [
                "https://www.iml.org/page.cfm?key=326",
                "https://www.iml.org/memberdirectory"
            ],
            "Arizona": [
                "https://www.azleague.org/562/Member-Cities-Towns",
                "https://www.azleague.org/189/Cities-Towns"
            ],
            "Missouri": [
                "https://www.mocities.com/page/members",
                "https://www.mocities.com/page/MemberDirectory"
            ],
            "New Mexico": [
                "https://nmml.org/membership/municipal-officials-directory/",
                "https://nmml.org/membership/"
            ],
            "Nevada": [
                "https://www.nvleague.org/member-governments/",
                "https://www.nvleague.org/members/"
            ]
        }
        
        # For each target state, find municipalities
        for state, urls in municipal_leagues.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Finding municipalities in {state}")
            
            for url in urls:
                try:
                    logger.info(f"Accessing municipal league at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Extract municipalities
                    municipalities = self._extract_municipalities_from_league(soup, url, state)
                    
                    # Process each municipality
                    for municipality in municipalities:
                        try:
                            # Skip if it doesn't have a name
                            if not municipality.get("name"):
                                continue
                            
                            # For Illinois, filter to only municipalities south of I-80
                            if state == "Illinois" and municipality.get("city"):
                                if municipality["city"] not in ILLINOIS_SOUTH_OF_I80:
                                    continue
                            
                            # Set default state if not in municipality data
                            if not municipality.get("state"):
                                municipality["state"] = state
                                
                            # Make sure we have the org_type set
                            municipality["org_type"] = self.org_type
                            
                            # If we don't have a website, try to find one
                            if not municipality.get("website") and municipality.get("name"):
                                municipality["website"] = self._find_website_for_municipality(municipality["name"], state)
                            
                            # Skip if still no website since we need it for contact discovery
                            if not municipality.get("website"):
                                continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(municipality)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the municipality website
                            municipal_contacts = self._scrape_municipality_website(municipality.get("website"), org_id)
                            contacts.extend(municipal_contacts)
                            
                            # Track success for logging
                            if municipal_contacts:
                                logger.info(f"Found {len(municipal_contacts)} contacts at {municipality['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing municipality: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing municipal league {url}: {e}")
        
        return contacts
    
    def _scrape_major_cities(self) -> List[Dict[str, Any]]:
        """
        Scrape major cities in target states.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Define major cities in target states
        major_cities = {
            "Utah": [
                {"name": "Salt Lake City", "website": "https://www.slc.gov/"},
                {"name": "West Valley City", "website": "https://www.wvc-ut.gov/"},
                {"name": "Provo", "website": "https://www.provo.org/"},
                {"name": "West Jordan", "website": "https://www.westjordan.utah.gov/"},
                {"name": "Orem", "website": "https://orem.org/"},
                {"name": "Sandy", "website": "https://www.sandy.utah.gov/"},
                {"name": "Ogden", "website": "https://www.ogdencity.com/"},
                {"name": "St. George", "website": "https://www.sgcity.org/"}
            ],
            "Illinois": [
                {"name": "Springfield", "website": "https://www.springfield.il.us/"},
                {"name": "Peoria", "website": "https://www.peoriagov.org/"},
                {"name": "Champaign", "website": "https://champaignil.gov/"},
                {"name": "Bloomington", "website": "https://www.cityblm.org/"},
                {"name": "Decatur", "website": "https://www.decaturil.gov/"},
                {"name": "Carbondale", "website": "https://www.explorecarbondale.com/"}
            ],
            "Arizona": [
                {"name": "Phoenix", "website": "https://www.phoenix.gov/"},
                {"name": "Tucson", "website": "https://www.tucsonaz.gov/"},
                {"name": "Mesa", "website": "https://www.mesaaz.gov/"},
                {"name": "Chandler", "website": "https://www.chandleraz.gov/"},
                {"name": "Scottsdale", "website": "https://www.scottsdaleaz.gov/"},
                {"name": "Glendale", "website": "https://www.glendaleaz.com/"},
                {"name": "Tempe", "website": "https://www.tempe.gov/"},
                {"name": "Flagstaff", "website": "https://www.flagstaff.az.gov/"}
            ],
            "Missouri": [
                {"name": "Kansas City", "website": "https://www.kcmo.gov/"},
                {"name": "St. Louis", "website": "https://www.stlouis-mo.gov/"},
                {"name": "Springfield", "website": "https://www.springfieldmo.gov/"},
                {"name": "Columbia", "website": "https://www.como.gov/"},
                {"name": "Independence", "website": "https://www.ci.independence.mo.us/"},
                {"name": "Lee's Summit", "website": "https://cityofls.net/"},
                {"name": "O'Fallon", "website": "https://www.ofallon.mo.us/"},
                {"name": "St. Joseph", "website": "https://www.stjoemo.org/"}
            ],
            "New Mexico": [
                {"name": "Albuquerque", "website": "https://www.cabq.gov/"},
                {"name": "Las Cruces", "website": "https://www.las-cruces.org/"},
                {"name": "Rio Rancho", "website": "https://rrnm.gov/"},
                {"name": "Santa Fe", "website": "https://www.santafenm.gov/"},
                {"name": "Roswell", "website": "https://www.roswell-nm.gov/"},
                {"name": "Farmington", "website": "https://fmtn.org/"},
                {"name": "Alamogordo", "website": "https://ci.alamogordo.nm.us/"},
                {"name": "Carlsbad", "website": "https://www.cityofcarlsbadnm.com/"}
            ],
            "Nevada": [
                {"name": "Las Vegas", "website": "https://www.lasvegasnevada.gov/"},
                {"name": "Henderson", "website": "https://www.cityofhenderson.com/"},
                {"name": "Reno", "website": "https://www.reno.gov/"},
                {"name": "North Las Vegas", "website": "https://www.cityofnorthlasvegas.com/"},
                {"name": "Sparks", "website": "https://www.cityofsparks.us/"},
                {"name": "Carson City", "website": "https://carson.org/"},
                {"name": "Elko", "website": "https://www.elkocity.com/"},
                {"name": "Boulder City", "website": "https://www.bcnv.org/"}
            ]
        }
        
        # Process each major city
        for state, cities in major_cities.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Processing major cities in {state}")
            
            for city in cities:
                try:
                    # Filter Illinois cities based on I-80 boundary
                    if state == "Illinois" and city["name"] not in ILLINOIS_SOUTH_OF_I80:
                        continue
                    
                    # Create organization data
                    org_data = {
                        "name": city["name"],
                        "org_type": self.org_type,
                        "website": city["website"],
                        "state": state,
                        "city": city["name"]
                    }
                    
                    # Save organization and get ID
                    org_id = self.save_organization(org_data)
                    if not org_id:
                        continue
                    
                    # Scrape contacts from the city website
                    city_contacts = self._scrape_municipality_website(city["website"], org_id)
                    contacts.extend(city_contacts)
                    
                    # Track success for logging
                    if city_contacts:
                        logger.info(f"Found {len(city_contacts)} contacts at {city['name']}")
                    
                    # Avoid overloading the server
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing major city {city['name']}: {e}")
        
        return contacts
    
    def _scrape_search_based_municipalities(self) -> List[Dict[str, Any]]:
        """
        Find municipalities using search engine.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Initialize search engine
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, search for municipalities
        for state in TARGET_STATES:
            logger.info(f"Searching for municipalities in {state}")
            
            # Define search queries
            search_queries = [
                f"city of * {state} official website",
                f"town of * {state} official website",
                f"village of * {state} official website",
                f"municipalities in {state} official website",
                f"{state} city public works department"
            ]
            
            # For Illinois, specifically target southern municipalities
            if state == "Illinois":
                for county in ILLINOIS_SOUTH_OF_I80:
                    search_queries.append(f"{county} county municipalities Illinois")
                    search_queries.append(f"cities in {county} county Illinois")
            
            for query in search_queries:
                try:
                    logger.info(f"Executing search: {query}")
                    search_results = search_engine.execute_search(query, "municipal", state)
                    
                    # Process each search result
                    for result in search_results:
                        try:
                            # Extract URL, title and snippet
                            url = result.get("link", "") or result.get("url", "")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                            
                            if not url or not title:
                                continue
                                
                            # Check if this is a likely municipal website (common municipal website patterns)
                            municipal_patterns = [".gov", ".org", ".us", "city", "town", "village", "municipal"]
                            if not any(pattern in url.lower() for pattern in municipal_patterns):
                                continue
                            
                            # Skip social media, job sites, etc.
                            skip_domains = ["linkedin.com", "facebook.com", "twitter.com", "indeed.com", 
                                          "glassdoor.com", "wikipedia.org", "yelp.com", "yellowpages.com"]
                            if any(domain in url.lower() for domain in skip_domains):
                                continue
                                
                            # Extract municipality name from title
                            municipality_name = title.split(" - ")[0].split(" | ")[0]  # Take first part of title as name
                            
                            # Check if this looks like a municipality name
                            municipal_indicators = ["City of", "Town of", "Village of", "Borough of", "Township of"]
                            has_indicator = any(indicator in municipality_name for indicator in municipal_indicators)
                            
                            # If no indicator, check if the title contains the word "city", "town", etc.
                            if not has_indicator:
                                contains_municipal_word = any(word.lower() in municipality_name.lower() 
                                                            for word in ["city", "town", "village", "borough", "township"])
                                if not contains_municipal_word:
                                    # Skip if it doesn't look like a municipality
                                    continue
                            
                            # Create organization data
                            org_data = {
                                "name": municipality_name,
                                "org_type": self.org_type,
                                "website": url,
                                "state": state,
                                "source_url": url
                            }
                            
                            # Try to get website content
                            soup = self.get_page(url)
                            if soup:
                                # Try to extract a better municipality name from the website
                                better_name = self._extract_municipality_name(soup)
                                if better_name:
                                    org_data["name"] = better_name
                                    
                                # Try to extract city location (which is the municipality name in most cases)
                                city = self._extract_city(soup, state)
                                if city:
                                    org_data["city"] = city
                                else:
                                    # If no city found, use the name without "City of", etc.
                                    for prefix in ["City of ", "Town of ", "Village of ", "Borough of ", "Township of "]:
                                        if org_data["name"].startswith(prefix):
                                            org_data["city"] = org_data["name"][len(prefix):]
                                            break
                            
                            # For Illinois, check if the city is south of I-80
                            if state == "Illinois" and org_data.get("city"):
                                # Skip if this city is not in our list of cities south of I-80
                                # Note: This is an imperfect check as the city names in the list may not match exactly
                                if not any(county.lower() in org_data["city"].lower() for county in ILLINOIS_SOUTH_OF_I80):
                                    # Also check if the municipality name contains a county name
                                    if not any(county.lower() in org_data["name"].lower() for county in ILLINOIS_SOUTH_OF_I80):
                                        continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(org_data)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the municipality website
                            municipal_contacts = self._scrape_municipality_website(url, org_id)
                            if municipal_contacts:
                                contacts.extend(municipal_contacts)
                                logger.info(f"Found {len(municipal_contacts)} contacts at {org_data['name']}")
                            
                            # Avoid overloading servers
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing search result: {e}")
                    
                except Exception as e:
                    logger.error(f"Error executing search query '{query}': {e}")
        
        return contacts
    
    def _extract_municipalities_from_league(self, soup: BeautifulSoup, source_url: str, state: str) -> List[Dict[str, Any]]:
        """Extract municipalities from municipal league websites."""
        municipalities = []
        
        try:
            # Municipal leagues often have member directories with city listings
            # Try to find municipality listings
            member_sections = soup.find_all(["div", "section", "ul"], id=lambda i: i and 
                                        any(term in i.lower() for term in ["member", "directory", "municipalities", "cities"]))
            
            if not member_sections:
                member_sections = soup.find_all(["div", "section", "ul"], class_=lambda c: c and 
                                            any(term in c.lower() for term in ["member", "directory", "municipalities", "cities"]))
            
            # If no specific sections found, look at the whole page
            if not member_sections:
                member_sections = [soup]
            
            # Process each section
            for section in member_sections:
                # Look for links to municipalities
                links = section.find_all("a", href=lambda h: h and 
                                      not any(term in h.lower() for term in ["mailto:", "javascript:", "#"]))
                
                for link in links:
                    try:
                        municipality_name = link.get_text().strip()
                        href = link["href"]
                        
                        # Skip if empty name or it doesn't look like a municipality name
                        if not municipality_name or len(municipality_name) < 3:
                            continue
                            
                        # Skip obvious non-municipality links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word == municipality_name.lower() for word in skip_words):
                            continue
                            
                        # Try to determine if this is a municipality
                        municipal_indicators = ["City of", "Town of", "Village of", "Borough of", "Township of"]
                        has_indicator = any(indicator in municipality_name for indicator in municipal_indicators)
                        
                        # If no indicator, check if the name contains common municipal words
                        if not has_indicator:
                            contains_municipal_word = any(word.lower() in municipality_name.lower() 
                                                       for word in ["city", "town", "village", "borough", "township"])
                            if not contains_municipal_word:
                                # Skip if it doesn't look like a municipality
                                continue
                        
                        # Extract city name
                        city = None
                        for prefix in ["City of ", "Town of ", "Village of ", "Borough of ", "Township of "]:
                            if municipality_name.startswith(prefix):
                                city = municipality_name[len(prefix):]
                                break
                                
                        if not city:
                            city = municipality_name
                        
                        # Make absolute URL if needed
                        if href.startswith("/"):
                            # Get base URL
                            from urllib.parse import urlparse
                            parsed_url = urlparse(source_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = base_url + href
                        
                        # Create municipality data
                        municipality_data = {
                            "name": municipality_name,
                            "website": href,
                            "state": state,
                            "city": city,
                            "source_url": source_url
                        }
                        
                        municipalities.append(municipality_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting municipality from link: {e}")
            
            # If still not enough municipalities found, try tables
            if len(municipalities) < 5:
                # Look for tables that might contain municipality info
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
                                municipality_name = link.get_text().strip()
                                href = link["href"]
                                
                                # Skip if it doesn't look like a municipality
                                if any(word == municipality_name.lower() for word in skip_words):
                                    continue
                                
                                # Extract city name
                                city = None
                                for prefix in ["City of ", "Town of ", "Village of ", "Borough of ", "Township of "]:
                                    if municipality_name.startswith(prefix):
                                        city = municipality_name[len(prefix):]
                                        break
                                        
                                if not city:
                                    city = municipality_name
                                
                                # Make absolute URL if needed
                                if href.startswith("/"):
                                    from urllib.parse import urlparse
                                    parsed_url = urlparse(source_url)
                                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                                    href = base_url + href
                                
                                municipalities.append({
                                    "name": municipality_name,
                                    "website": href,
                                    "state": state,
                                    "city": city,
                                    "source_url": source_url
                                })
                                
                        except Exception as e:
                            logger.error(f"Error extracting municipality from table row: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting municipalities from league: {e}")
        
        logger.info(f"Found {len(municipalities)} municipalities on league page {source_url}")
        return municipalities
    
    def _extract_municipality_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract municipality name from website content."""
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
            
            # Look for municipality name in typical municipal page structures
            municipal_header = soup.find(["div", "span"], class_=lambda c: c and 
                                      any(term in c.lower() for term in ["site-title", "site-header", "city-name"]))
            if municipal_header:
                return municipal_header.get_text().strip()
            
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
            logger.error(f"Error extracting municipality name: {e}")
            
        return None
    
    def _extract_city(self, soup: BeautifulSoup, state: str) -> Optional[str]:
        """Extract city from website content using the state as context."""
        try:
            # For municipalities, this is usually the city name in the header/title
            # First, look for schema.org data
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
                        # Sometimes there's a name without "City of" prefix
                        elif data.get("name") and "city of" in data.get("name", "").lower():
                            city = data["name"].lower().replace("city of", "").strip()
                            if city:
                                return city.title()
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
    
    def _find_website_for_municipality(self, municipality_name: str, state: str) -> Optional[str]:
        """Search for a municipality website using search engine."""
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # Create a search query
        query = f"{municipality_name} {state} official website"
        
        try:
            search_results = search_engine.execute_search(query, "municipal", state)
            
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
                    
                # Check if this is likely a municipal website (common municipal website patterns)
                municipal_patterns = [".gov", ".org", ".us", "city", "town", "village", "municipal"]
                if any(pattern in url.lower() for pattern in municipal_patterns):
                    return url
                    
        except Exception as e:
            logger.error(f"Error searching for municipality website: {e}")
            
        return None
    
    def _scrape_municipality_website(self, website: str, org_id: int) -> List[Dict[str, Any]]:
        """
        Scrape contacts from a municipality website.
        
        Args:
            website: Municipality website URL
            org_id: Organization ID in the database
            
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Common paths where staff/leadership information might be found
        staff_paths = ["staff-directory", "staff", "directory", "contact/staff", "contact-us/staff",
                      "government/staff", "government/directory", "departments", "department-directory",
                      "public-works", "departments/public-works", "government/departments/public-works",
                      "water", "utilities", "departments/utilities", "departments/water",
                      "contact-us", "contact", "about-us/staff", "about/staff", "leadership",
                      "government/leadership", "city-manager", "government/city-manager", "officials"]
        
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
                                                                   for term in ["staff", "employee", "member", "people", 
                                                                              "directory", "contact", "department", "official"]))
                
                if not staff_elements:
                    # Try finding elements by ID
                    staff_elements = soup.find_all(["div", "article", "section"], 
                                                id=lambda i: i and any(term in i.lower() 
                                                                     for term in ["staff", "employee", "directory", "contact"]))
                    
                if not staff_elements:
                    # If still nothing found, look for department sections
                    staff_elements = soup.find_all(["div", "article", "section"], 
                                                id=lambda i: i and any(term in i.lower() for term in 
                                                                     ["public-works", "utilities", "water", "engineering"]))
                    if not staff_elements:
                        staff_elements = soup.find_all(["div", "article", "section"], 
                                                    class_=lambda c: c and any(term in c.lower() for term in 
                                                                           ["public-works", "utilities", "water", "engineering"]))
                
                # If still nothing, try a more general approach with the whole page
                if not staff_elements:
                    staff_elements = [soup]
                
                for element in staff_elements:
                    # Look for job titles that match our target
                    target_titles = ORG_TYPES["municipal"]["job_titles"]
                    
                    # Try to find staff listings within this element
                    staff_listings = element.find_all(["div", "article", "li"], 
                                                   class_=lambda c: c and any(term in c.lower() 
                                                                          for term in ["staff", "person", "member", "employee", "contact"]))
                    
                    # If no specific staff listings found, use the whole element
                    if not staff_listings:
                        staff_listings = [element]
                    
                    for staff in staff_listings:
                        try:
                            # Extract job title
                            title_element = staff.find(["h3", "h4", "div", "span", "p"], 
                                                   class_=lambda c: c and ("title" in c.lower() or "position" in c.lower()))
                            
                            if not title_element:
                                # Try finding title by text pattern
                                title_patterns = ["Director", "Manager", "Supervisor", "Superintendent", "Engineer", "Operator"]
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
                            
                            # Check if job title is relevant (related to public works, utilities, etc.)
                            relevant_title = any(target.lower() in job_title.lower() for target in target_titles)
                            
                            # Also check for specific municipal roles which are important for our targets
                            public_works_role = "public works" in job_title.lower()
                            utilities_role = "utility" in job_title.lower() or "utilities" in job_title.lower() or "water" in job_title.lower()
                            engineering_role = "engineer" in job_title.lower()
                            planning_role = "planner" in job_title.lower() or "planning" in job_title.lower()
                            
                            if not (relevant_title or public_works_role or utilities_role or engineering_role or planning_role):
                                continue
                            
                            # Extract name
                            name_element = staff.find(["h2", "h3", "div", "span"], 
                                                   class_=lambda c: c and "name" in c.lower())
                            
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
                                "contact_relevance_score": 9.0 if (public_works_role or utilities_role) else 7.0,
                                "notes": f"Found on {url}"
                            }
                            
                            # Save contact
                            contact_id = self.save_contact(contact_data)
                            if contact_id:
                                contacts.append(contact_data)
                                
                        except Exception as e:
                            logger.error(f"Error processing staff listing: {e}")
            
            except Exception as e:
                logger.error(f"Error scraping municipality website {url}: {e}")
                
        return contacts