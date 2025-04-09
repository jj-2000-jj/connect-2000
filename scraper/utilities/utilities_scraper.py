"""
Utility companies scraper for the GBL Data Contact Management System.
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


class UtilitiesScraper(BaseScraper):
    """Scraper for utility companies (electric, gas, power)."""
    
    def __init__(self, db_session):
        """Initialize the utilities scraper."""
        super().__init__(db_session)
        self.org_type = "utility"
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape utility companies from various sources.
        
        Returns:
            List of dictionaries with contact data
        """
        logger.info("Starting utilities scraping")
        contacts = []
        
        # Scrape from multiple sources
        contacts.extend(self._scrape_utility_associations())
        contacts.extend(self._scrape_major_utilities())
        contacts.extend(self._scrape_search_based_utilities())
        
        logger.info(f"Found {len(contacts)} utility company contacts")
        return contacts

    def _scrape_utility_associations(self) -> List[Dict[str, Any]]:
        """
        Scrape utility companies from industry associations and directories.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Utility association and directory URLs
        utility_associations = {
            "National": [
                "https://www.eia.gov/electricity/data/eia861/",
                "https://www.publicpower.org/directory",
                "https://www.eei.org/members/Pages/default.aspx",
                "https://www.aga.org/membership/memberdirectory/"
            ],
            "Utah": [
                "https://psc.utah.gov/electric/electric-companies/",
                "https://psc.utah.gov/gas/natural-gas-utilities/"
            ],
            "Illinois": [
                "https://www.icc.illinois.gov/utility/list.aspx",
                "https://www.icc.illinois.gov/authority/publicutilities"
            ],
            "Arizona": [
                "https://www.azcc.gov/utilities/electric",
                "https://www.azcc.gov/utilities/natural-gas"
            ],
            "Missouri": [
                "https://psc.mo.gov/Electric/",
                "https://psc.mo.gov/NaturalGas/"
            ],
            "New Mexico": [
                "https://www.nmprc.state.nm.us/electric/",
                "https://www.nmprc.state.nm.us/utilities/gas.html"
            ],
            "Nevada": [
                "https://puc.nv.gov/About/Companies/Electric_Companies/",
                "https://puc.nv.gov/About/Companies/Natural_Gas/"
            ]
        }
        
        # For each association/directory, extract utility companies
        for region, urls in utility_associations.items():
            for url in urls:
                try:
                    logger.info(f"Accessing utility association/directory at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Extract utilities
                    utilities = self._extract_utilities_from_directory(soup, url, region)
                    
                    # Process each utility
                    for utility in utilities:
                        try:
                            # Skip if it doesn't have a name
                            if not utility.get("name"):
                                continue
                            
                            # Filter to our target states
                            if utility.get("state") and utility["state"] not in TARGET_STATES:
                                continue
                                
                            # If no state specified but region is a specific state, use that
                            if not utility.get("state") and region in TARGET_STATES:
                                utility["state"] = region
                            
                            # Skip if still no state or not in our target states
                            if not utility.get("state") or utility["state"] not in TARGET_STATES:
                                continue
                                
                            # Make sure we have the org_type set
                            utility["org_type"] = self.org_type
                            
                            # If we don't have a website, try to find one
                            if not utility.get("website") and utility.get("name"):
                                utility["website"] = self._find_website_for_utility(utility["name"], utility["state"])
                            
                            # Skip if still no website since we need it for contact discovery
                            if not utility.get("website"):
                                continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(utility)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the utility website
                            utility_contacts = self._scrape_utility_website(utility.get("website"), org_id)
                            contacts.extend(utility_contacts)
                            
                            # Track success for logging
                            if utility_contacts:
                                logger.info(f"Found {len(utility_contacts)} contacts at {utility['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing utility: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing utility association/directory {url}: {e}")
        
        return contacts
    
    def _scrape_major_utilities(self) -> List[Dict[str, Any]]:
        """
        Scrape major utility companies in target states.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Define major utility companies in target states
        major_utilities = {
            "Utah": [
                {"name": "Rocky Mountain Power", "website": "https://www.rockymountainpower.net/"},
                {"name": "Dominion Energy Utah", "website": "https://www.dominionenergy.com/utah"},
                {"name": "Provo City Power", "website": "https://www.provo.org/departments/utilities"},
                {"name": "Murray City Power", "website": "https://www.murray.utah.gov/212/Power-Department"},
                {"name": "Logan City Light and Power", "website": "https://www.loganutah.org/departments/light_and_power/"}
            ],
            "Illinois": [
                {"name": "Ameren Illinois", "website": "https://www.ameren.com/illinois"},
                {"name": "Commonwealth Edison (ComEd)", "website": "https://www.comed.com/"},
                {"name": "Illinois American Water", "website": "https://www.amwater.com/ilaw/"},
                {"name": "Nicor Gas", "website": "https://www.nicorgas.com/"},
                {"name": "Peoples Gas", "website": "https://www.peoplesgasdelivery.com/"}
            ],
            "Arizona": [
                {"name": "Arizona Public Service (APS)", "website": "https://www.aps.com/"},
                {"name": "Salt River Project (SRP)", "website": "https://www.srpnet.com/"},
                {"name": "Tucson Electric Power", "website": "https://www.tep.com/"},
                {"name": "Southwest Gas", "website": "https://www.swgas.com/"},
                {"name": "UNS Electric", "website": "https://www.uesaz.com/"}
            ],
            "Missouri": [
                {"name": "Ameren Missouri", "website": "https://www.ameren.com/missouri"},
                {"name": "Evergy", "website": "https://www.evergy.com/"},
                {"name": "Liberty Utilities", "website": "https://missouri.libertyutilities.com/"},
                {"name": "Spire", "website": "https://www.spireenergy.com/"},
                {"name": "Missouri American Water", "website": "https://www.amwater.com/moaw/"}
            ],
            "New Mexico": [
                {"name": "Public Service Company of New Mexico (PNM)", "website": "https://www.pnm.com/"},
                {"name": "El Paso Electric", "website": "https://www.epelectric.com/"},
                {"name": "New Mexico Gas Company", "website": "https://www.nmgco.com/"},
                {"name": "Xcel Energy", "website": "https://www.xcelenergy.com/"},
                {"name": "Continental Divide Electric Cooperative", "website": "https://www.cdec.coop/"}
            ],
            "Nevada": [
                {"name": "NV Energy", "website": "https://www.nvenergy.com/"},
                {"name": "Southwest Gas", "website": "https://www.swgas.com/"},
                {"name": "Valley Electric Association", "website": "https://www.vea.coop/"},
                {"name": "Overton Power District", "website": "https://www.opd5.com/"},
                {"name": "Mt. Wheeler Power", "website": "https://www.mwpower.net/"}
            ]
        }
        
        # Process each major utility
        for state, utilities in major_utilities.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Processing major utilities in {state}")
            
            for utility in utilities:
                try:
                    # Create organization data
                    org_data = {
                        "name": utility["name"],
                        "org_type": self.org_type,
                        "website": utility["website"],
                        "state": state
                    }
                    
                    # Try to get the city from website
                    soup = self.get_page(utility["website"])
                    if soup:
                        city = self._extract_city(soup, state)
                        if city:
                            org_data["city"] = city
                    
                    # Save organization and get ID
                    org_id = self.save_organization(org_data)
                    if not org_id:
                        continue
                    
                    # Scrape contacts from the utility website
                    utility_contacts = self._scrape_utility_website(utility["website"], org_id)
                    contacts.extend(utility_contacts)
                    
                    # Track success for logging
                    if utility_contacts:
                        logger.info(f"Found {len(utility_contacts)} contacts at {utility['name']}")
                    
                    # Avoid overloading the server
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing major utility {utility['name']}: {e}")
        
        return contacts
    
    def _scrape_search_based_utilities(self) -> List[Dict[str, Any]]:
        """
        Find utility companies using search engine.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Initialize search engine
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, search for utility companies
        for state in TARGET_STATES:
            logger.info(f"Searching for utility companies in {state}")
            
            # Define search queries
            search_queries = [
                f"electric utility companies {state}",
                f"power company {state}",
                f"energy provider {state}",
                f"gas utility {state}",
                f"electric cooperative {state}",
                f"rural electric cooperative {state}",
                f"municipal utility {state}",
                f"power district {state}"
            ]
            
            for query in search_queries:
                try:
                    logger.info(f"Executing search: {query}")
                    search_results = search_engine.execute_search(query, "utility", state)
                    
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
                                
                            # Check if this looks like a utility company
                            utility_indicators = ["power", "energy", "electric", "utility", "utilities", "gas", "cooperative"]
                            is_utility = False
                            
                            # Check title
                            if any(indicator in title.lower() for indicator in utility_indicators):
                                is_utility = True
                                
                            # Check snippet if title doesn't have indicators
                            if not is_utility and any(indicator in snippet.lower() for indicator in utility_indicators):
                                is_utility = True
                                
                            if not is_utility:
                                continue
                                
                            # Check for obvious non-utilities like news articles
                            non_utility_indicators = ["news", "article", "blog", "press release"]
                            if any(indicator in title.lower() for indicator in non_utility_indicators):
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
                                # Try to extract a better utility name from the website
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
    
    def _extract_utilities_from_directory(self, soup: BeautifulSoup, source_url: str, region: str) -> List[Dict[str, Any]]:
        """Extract utility companies from association/directory pages."""
        utilities = []
        
        try:
            # Try to find utility listings
            utility_elements = []
            
            # Try table rows first
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                # Skip header row
                utility_elements.extend(rows[1:])
            
            # If no tables, try list items
            if not utility_elements:
                list_elements = soup.find_all(["ul", "ol"])
                for list_elem in list_elements:
                    utility_elements.extend(list_elem.find_all("li"))
            
            # If still no results, try sections or divs with potential utility company listings
            if not utility_elements:
                utility_elements = soup.find_all(["div", "section", "article"], 
                                              class_=lambda c: c and any(term in c.lower() for term in 
                                                                     ["utility", "company", "member", "listing", "provider", "electric"]))
            
            # Process each element
            for element in utility_elements:
                try:
                    # Extract utility name
                    utility_name = None
                    name_element = element.find(["a", "h2", "h3", "h4", "strong", "td"])
                    if name_element:
                        utility_name = name_element.get_text().strip()
                    else:
                        # Try to just get the text of the element if it's not too long
                        element_text = element.get_text().strip()
                        if len(element_text) < 100:  # Avoid getting long paragraphs
                            utility_name = element_text
                    
                    if not utility_name:
                        continue
                        
                    # Skip if it clearly isn't a utility name
                    skip_words = ["click", "here", "more", "info", "search", "back", "next", "previous"]
                    if any(word == utility_name.lower() for word in skip_words):
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
                    
                    # Create utility data
                    utility_data = {
                        "name": utility_name,
                        "source_url": source_url
                    }
                    
                    if website:
                        utility_data["website"] = website
                        
                    if state:
                        utility_data["state"] = state
                    
                    utilities.append(utility_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting utility from element: {e}")
            
            # If we couldn't find utilities in structured elements, try extracting all links
            if not utilities:
                links = soup.find_all("a", href=lambda h: h and 
                                   not any(term in str(h).lower() for term in ["mailto:", "javascript:", "#"]))
                
                for link in links:
                    try:
                        utility_name = link.get_text().strip()
                        href = link["href"]
                        
                        # Skip if it doesn't look like a utility name
                        if not utility_name or len(utility_name) < 3:
                            continue
                            
                        # Skip obvious non-utility links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word == utility_name.lower() for word in skip_words):
                            continue
                            
                        # Check if it looks like a utility company name
                        utility_terms = ["power", "energy", "electric", "utility", "gas", "cooperative", "services"]
                        if not any(term in utility_name.lower() for term in utility_terms):
                            continue
                        
                        # Make absolute URL if needed
                        if href.startswith("/"):
                            from urllib.parse import urlparse
                            parsed_url = urlparse(source_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = base_url + href
                        
                        # Try to extract state information
                        state = None
                        utility_text = utility_name
                        if link.parent:
                            utility_text = link.parent.get_text()
                            
                        for target_state in TARGET_STATES:
                            if target_state in utility_text:
                                state = target_state
                                break
                        
                        utility_data = {
                            "name": utility_name,
                            "website": href,
                            "source_url": source_url
                        }
                        
                        if state:
                            utility_data["state"] = state
                        
                        utilities.append(utility_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting utility from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting utilities from directory: {e}")
        
        logger.info(f"Found {len(utilities)} utilities on directory page {source_url}")
        return utilities
    
    def _extract_utility_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract utility company name from website content."""
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
            logger.error(f"Error extracting utility name: {e}")
            
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
    
    def _find_website_for_utility(self, utility_name: str, state: str) -> Optional[str]:
        """Search for a utility company website using search engine."""
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # Create a search query
        query = f"{utility_name} {state} official website"
        
        try:
            search_results = search_engine.execute_search(query, "utility", state)
            
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
            logger.error(f"Error searching for utility website: {e}")
            
        return None
    
    def _scrape_utility_website(self, website: str, org_id: int) -> List[Dict[str, Any]]:
        """
        Scrape contacts from a utility company website.
        
        Args:
            website: Utility company website URL
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
        
        # Additional paths for utility-specific departments
        utility_paths = ["operations", "about/operations", "about-us/operations",
                       "engineering", "about/engineering", "about-us/engineering",
                       "departments/operations", "departments/engineering",
                       "services/operations", "services/engineering"]
        
        # Combine all paths
        all_paths = staff_paths + utility_paths
        
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
                                                                                "technical", "system", "control"]))
                    if not staff_elements:
                        staff_elements = soup.find_all(["div", "article", "section"], 
                                                    class_=lambda c: c and any(term in (c.lower() if c else "")
                                                                           for term in ["operations", "engineering", 
                                                                                      "technical", "system", "control"]))
                
                # If still nothing, try a more general approach with the whole page
                if not staff_elements:
                    staff_elements = [soup]
                
                for element in staff_elements:
                    # Look for job titles that match our target
                    target_titles = ORG_TYPES["utility"]["job_titles"]
                    
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
                                               "Chief", "Officer", "CEO", "COO", "CTO"]
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
                            
                            # Check if job title is relevant (operations, management, or engineering roles)
                            relevant_title = any(target.lower() in job_title.lower() for target in target_titles)
                            
                            # Also check for specific roles which are important for utility companies
                            operations_role = "operations" in job_title.lower() or "operator" in job_title.lower()
                            engineering_role = "engineer" in job_title.lower() or "engineering" in job_title.lower()
                            management_role = "manager" in job_title.lower() or "director" in job_title.lower()
                            executive_role = "president" in job_title.lower() or "chief" in job_title.lower() or "officer" in job_title.lower()
                            
                            if not (relevant_title or operations_role or engineering_role or management_role or executive_role):
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
                            
                            # Calculate relevance score - higher for operations and engineering roles
                            relevance_score = 7.0  # Base score
                            if operations_role:
                                relevance_score = 9.0
                            elif engineering_role:
                                relevance_score = 8.5
                            elif management_role and ("technical" in job_title.lower() or "operations" in job_title.lower()):
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
                logger.error(f"Error scraping utility website {url}: {e}")
                
        return contacts