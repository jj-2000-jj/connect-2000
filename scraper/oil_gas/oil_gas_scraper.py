"""
Oil, gas and mining companies scraper for the GBL Data Contact Management System.
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


class OilGasScraper(BaseScraper):
    """Scraper for oil, gas and mining companies."""
    
    def __init__(self, db_session):
        """Initialize the oil and gas scraper."""
        super().__init__(db_session)
        self.org_type = "oil_gas"
        
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape oil, gas and mining companies from various sources.
        
        Returns:
            List of dictionaries with contact data
        """
        logger.info("Starting oil, gas and mining companies scraping")
        contacts = []
        
        # Scrape from multiple sources
        contacts.extend(self._scrape_industry_associations())
        contacts.extend(self._scrape_major_companies())
        contacts.extend(self._scrape_regulatory_agencies())
        contacts.extend(self._scrape_search_based_companies())
        
        logger.info(f"Found {len(contacts)} oil, gas and mining company contacts")
        return contacts

    def _scrape_industry_associations(self) -> List[Dict[str, Any]]:
        """
        Scrape oil, gas and mining companies from industry associations and directories.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Industry association and directory URLs
        industry_associations = {
            "National": [
                "https://www.api.org/membership/members",
                "https://www.ipaa.org/member-companies/",
                "https://www.ingaa.org/Members.aspx",
                "https://www.energyworkforce.org/membership/member-companies/",
                "https://www.nma.org/member-resources/member-directory/"
            ],
            "Utah": [
                "https://utahoil.org/membership/member-directory/",
                "https://utahmining.org/member-directory/"
            ],
            "Illinois": [
                "https://www.ioga.com/member-directory",
                "https://www.ilchamber.org/energy-council/members/"
            ],
            "Arizona": [
                "https://www.azmining.org/about/member-directory",
                "https://www.azoga.org/membership/member-directory/"
            ],
            "Missouri": [
                "https://momining.org/member-directory/",
                "https://www.mogas.org/membership/directory"
            ],
            "New Mexico": [
                "https://nmoga.org/membership/member-companies/",
                "https://nmmining.org/membership/member-directory/"
            ],
            "Nevada": [
                "https://www.nevadamining.org/membership/member-directory/",
                "https://www.westerngas.org/membership/member-directory/"
            ]
        }
        
        # For each association/directory, extract oil, gas and mining companies
        for region, urls in industry_associations.items():
            for url in urls:
                try:
                    logger.info(f"Accessing industry association at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Extract companies
                    companies = self._extract_companies_from_directory(soup, url, region)
                    
                    # Process each company
                    for company in companies:
                        try:
                            # Skip if it doesn't have a name
                            if not company.get("name"):
                                continue
                            
                            # Filter to our target states
                            if company.get("state") and company["state"] not in TARGET_STATES:
                                continue
                                
                            # If no state specified but region is a specific state, use that
                            if not company.get("state") and region in TARGET_STATES:
                                company["state"] = region
                            
                            # If still no state, we'll need to determine it from the website content
                            if not company.get("state"):
                                # We need to check if this company operates in any of our target states
                                has_target_state = False
                                
                                # If we have a website, check its content
                                if company.get("website"):
                                    website_soup = self.get_page(company["website"])
                                    if website_soup:
                                        # Check if any target state is mentioned on the website
                                        website_text = website_soup.get_text().lower()
                                        for state in TARGET_STATES:
                                            if state.lower() in website_text:
                                                company["state"] = state
                                                has_target_state = True
                                                break
                                                
                                if not has_target_state:
                                    # Skip companies without a connection to our target states
                                    continue
                                
                            # Make sure we have the org_type set
                            company["org_type"] = self.org_type
                            
                            # If we don't have a website, try to find one
                            if not company.get("website") and company.get("name"):
                                company["website"] = self._find_website_for_company(company["name"], company.get("state", ""))
                            
                            # Skip if still no website since we need it for contact discovery
                            if not company.get("website"):
                                continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(company)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the company website
                            company_contacts = self._scrape_company_website(company.get("website"), org_id)
                            contacts.extend(company_contacts)
                            
                            # Track success for logging
                            if company_contacts:
                                logger.info(f"Found {len(company_contacts)} contacts at {company['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing company: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing industry association {url}: {e}")
        
        return contacts
    
    def _scrape_major_companies(self) -> List[Dict[str, Any]]:
        """
        Scrape major oil, gas and mining companies operating in target states.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Define major companies in target states
        major_companies = {
            "Utah": [
                {"name": "Wolverine Fuels", "website": "https://wolverinefuels.com/"},
                {"name": "Dominion Energy Utah", "website": "https://www.dominionenergy.com/utah"},
                {"name": "Ovintiv USA Inc.", "website": "https://www.ovintiv.com/"},
                {"name": "Rio Tinto Kennecott", "website": "https://riotintokennecott.com/"},
                {"name": "ConocoPhillips", "website": "https://www.conocophillips.com/"}
            ],
            "Illinois": [
                {"name": "Peoples Gas", "website": "https://www.peoplesgasdelivery.com/"},
                {"name": "Nicor Gas", "website": "https://www.nicorgas.com/"},
                {"name": "Foresight Energy", "website": "https://foresight.com/"},
                {"name": "Illinois Coal Association", "website": "https://www.ilcoal.org/"},
                {"name": "Prairie State Energy Campus", "website": "https://www.prairiestateenergycampus.com/"}
            ],
            "Arizona": [
                {"name": "Freeport-McMoRan", "website": "https://www.fcx.com/"},
                {"name": "ASARCO", "website": "http://www.asarco.com/"},
                {"name": "Resolution Copper", "website": "https://resolutioncopper.com/"},
                {"name": "Southwest Gas", "website": "https://www.swgas.com/"},
                {"name": "Florence Copper", "website": "https://florencecopper.com/"}
            ],
            "Missouri": [
                {"name": "Doe Run Company", "website": "https://doerun.com/"},
                {"name": "Spire", "website": "https://www.spireenergy.com/"},
                {"name": "Mississippi Lime Company", "website": "https://mississippilime.com/"},
                {"name": "Capital Materials", "website": "https://www.capitalmaterials.com/"},
                {"name": "Viburnum Operations", "website": "https://doerun.com/viburnum-operations/"}
            ],
            "New Mexico": [
                {"name": "Devon Energy", "website": "https://www.devonenergy.com/"},
                {"name": "Occidental Petroleum", "website": "https://www.oxy.com/"},
                {"name": "EOG Resources", "website": "https://www.eogresources.com/"},
                {"name": "Chevron", "website": "https://www.chevron.com/"},
                {"name": "New Mexico Gas Company", "website": "https://www.nmgco.com/"}
            ],
            "Nevada": [
                {"name": "Nevada Gold Mines", "website": "https://www.nevadagoldmines.com/"},
                {"name": "Kinross Gold", "website": "https://www.kinross.com/"},
                {"name": "Southwest Gas", "website": "https://www.swgas.com/"},
                {"name": "Barrick Gold", "website": "https://www.barrick.com/"},
                {"name": "MP Materials", "website": "https://mpmaterials.com/"}
            ]
        }
        
        # Process each major company
        for state, companies in major_companies.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Processing major oil, gas and mining companies in {state}")
            
            for company in companies:
                try:
                    # Create organization data
                    org_data = {
                        "name": company["name"],
                        "org_type": self.org_type,
                        "website": company["website"],
                        "state": state
                    }
                    
                    # Try to get the city from website
                    soup = self.get_page(company["website"])
                    if soup:
                        city = self._extract_city(soup, state)
                        if city:
                            org_data["city"] = city
                    
                    # Save organization and get ID
                    org_id = self.save_organization(org_data)
                    if not org_id:
                        continue
                    
                    # Scrape contacts from the company website
                    company_contacts = self._scrape_company_website(company["website"], org_id)
                    contacts.extend(company_contacts)
                    
                    # Track success for logging
                    if company_contacts:
                        logger.info(f"Found {len(company_contacts)} contacts at {company['name']}")
                    
                    # Avoid overloading the server
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing major company {company['name']}: {e}")
        
        return contacts
    
    def _scrape_regulatory_agencies(self) -> List[Dict[str, Any]]:
        """
        Scrape oil, gas and mining regulatory agencies for company listings.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Regulatory agency URLs
        regulatory_agencies = {
            "Utah": [
                "https://www.ogm.utah.gov/oilgas/OPERATORS/operatorlist.php",
                "https://minerals.ogm.utah.gov/online/mlrcat/"
            ],
            "Illinois": [
                "https://www2.illinois.gov/dnr/OilandGas/Pages/default.aspx",
                "https://www2.illinois.gov/dnr/mines/Pages/default.aspx"
            ],
            "Arizona": [
                "https://azogcc.az.gov/records/operator-registration",
                "https://new.azwater.gov/well-registry/well-registry-data"
            ],
            "Missouri": [
                "https://dnr.mo.gov/land-geology/water-resources/well-installation/well-driller-contractor-and-pump-installer-lists",
                "https://dnr.mo.gov/land-geology/business-industry/mineral-industry"
            ],
            "New Mexico": [
                "https://www.emnrd.nm.gov/ocd/operator-registration/",
                "https://www.emnrd.nm.gov/mmd/mining-act-mines-registrant-list/"
            ],
            "Nevada": [
                "https://minerals.nv.gov/Programs/Mining/Mining/",
                "https://dmvapp.nv.gov/DMV/OBL/Engineers/Licensees"
            ]
        }
        
        # For each regulatory agency, extract companies
        for state, urls in regulatory_agencies.items():
            if state not in TARGET_STATES:
                continue
                
            logger.info(f"Checking regulatory agencies for {state}")
            
            for url in urls:
                try:
                    logger.info(f"Accessing regulatory agency at {url}")
                    soup = self.get_page(url)
                    if not soup:
                        continue
                    
                    # Extract companies from regulatory listings
                    companies = self._extract_companies_from_regulatory(soup, url, state)
                    
                    # Process each company
                    for company in companies:
                        try:
                            # Skip if it doesn't have a name
                            if not company.get("name"):
                                continue
                            
                            # Set state if not already set
                            if not company.get("state"):
                                company["state"] = state
                                
                            # Make sure we have the org_type set
                            company["org_type"] = self.org_type
                            
                            # If we don't have a website, try to find one
                            if not company.get("website") and company.get("name"):
                                company["website"] = self._find_website_for_company(company["name"], state)
                            
                            # Skip if still no website since we need it for contact discovery
                            if not company.get("website"):
                                continue
                            
                            # Save organization and get ID
                            org_id = self.save_organization(company)
                            if not org_id:
                                continue
                            
                            # Scrape contacts from the company website
                            company_contacts = self._scrape_company_website(company.get("website"), org_id)
                            contacts.extend(company_contacts)
                            
                            # Track success for logging
                            if company_contacts:
                                logger.info(f"Found {len(company_contacts)} contacts at {company['name']}")
                            
                            # Avoid overloading the server
                            time.sleep(2)
                            
                        except Exception as e:
                            logger.error(f"Error processing company from regulatory listing: {e}")
                    
                except Exception as e:
                    logger.error(f"Error accessing regulatory agency {url}: {e}")
        
        return contacts
    
    def _scrape_search_based_companies(self) -> List[Dict[str, Any]]:
        """
        Find oil, gas and mining companies using search engine.
        
        Returns:
            List of contacts found
        """
        contacts = []
        
        # Initialize search engine
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # For each target state, search for oil, gas and mining companies
        for state in TARGET_STATES:
            logger.info(f"Searching for oil, gas and mining companies in {state}")
            
            # Define search queries
            search_queries = [
                f"oil production companies {state}",
                f"natural gas companies {state}",
                f"pipeline operators {state}",
                f"mining operations {state}",
                f"oil field services {state}",
                f"well drilling companies {state}",
                f"gas distribution companies {state}",
                f"coal mining companies {state}",
                f"mining exploration companies {state}"
            ]
            
            for query in search_queries:
                try:
                    logger.info(f"Executing search: {query}")
                    search_results = search_engine.execute_search(query, "oil_gas", state)
                    
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
                                
                            # Check if this looks like an oil, gas or mining company
                            industry_indicators = ["oil", "gas", "petroleum", "energy", "pipeline", "drilling", 
                                                 "exploration", "production", "mining", "minerals", "resources"]
                            is_industry = False
                            
                            # Check title
                            if any(indicator in title.lower() for indicator in industry_indicators):
                                is_industry = True
                                
                            # Check snippet if title doesn't have indicators
                            if not is_industry and any(indicator in snippet.lower() for indicator in industry_indicators):
                                is_industry = True
                                
                            if not is_industry:
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
        
        return contacts
    
    def _extract_companies_from_directory(self, soup: BeautifulSoup, source_url: str, region: str) -> List[Dict[str, Any]]:
        """Extract oil, gas and mining companies from association directory pages."""
        companies = []
        
        try:
            # Try to find company listings
            company_elements = []
            
            # Try table rows first
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                # Skip header row
                company_elements.extend(rows[1:])
            
            # If no tables, try list items
            if not company_elements:
                list_elements = soup.find_all(["ul", "ol"])
                for list_elem in list_elements:
                    items = list_elem.find_all("li")
                    # Add only items with links
                    for item in items:
                        if item.find("a"):
                            company_elements.append(item)
            
            # If still no results, try sections or divs with potential company listings
            if not company_elements:
                company_elements = soup.find_all(["div", "section", "article"], 
                                              class_=lambda c: c and any(term in (c.lower() if c else "") 
                                                                     for term in ["member", "company", "operator", 
                                                                                "listing", "directory"]))
            
            # Process each element
            for element in company_elements:
                try:
                    # Extract company name
                    company_name = None
                    name_element = element.find(["a", "h2", "h3", "h4", "strong", "td"])
                    if name_element:
                        company_name = name_element.get_text().strip()
                    else:
                        # Try to just get the text of the element if it's not too long
                        element_text = element.get_text().strip()
                        if len(element_text) < 100:  # Avoid getting long paragraphs
                            company_name = element_text
                    
                    if not company_name:
                        continue
                        
                    # Skip if it clearly isn't a company name
                    skip_words = ["click", "here", "more", "info", "search", "back", "next", "previous"]
                    if any(word == company_name.lower() for word in skip_words):
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
                    
                    # Create company data
                    company_data = {
                        "name": company_name,
                        "source_url": source_url
                    }
                    
                    if website:
                        company_data["website"] = website
                        
                    if state:
                        company_data["state"] = state
                    
                    companies.append(company_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting company from element: {e}")
            
            # If we couldn't find companies in structured elements, try extracting all links
            if not companies:
                links = soup.find_all("a", href=lambda h: h and 
                                   not any(term in str(h).lower() for term in ["mailto:", "javascript:", "#"]))
                
                for link in links:
                    try:
                        company_name = link.get_text().strip()
                        href = link["href"]
                        
                        # Skip if it doesn't look like a company name
                        if not company_name or len(company_name) < 3:
                            continue
                            
                        # Skip obvious non-company links
                        skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                        if any(word == company_name.lower() for word in skip_words):
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
                        
                        company_data = {
                            "name": company_name,
                            "website": href,
                            "source_url": source_url
                        }
                        
                        if state:
                            company_data["state"] = state
                        
                        companies.append(company_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting company from link: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting companies from directory: {e}")
        
        logger.info(f"Found {len(companies)} companies on directory page {source_url}")
        return companies
    
    def _extract_companies_from_regulatory(self, soup: BeautifulSoup, source_url: str, state: str) -> List[Dict[str, Any]]:
        """Extract oil, gas and mining companies from regulatory agency pages."""
        companies = []
        
        try:
            # Regulatory pages often have tables of operator/company listings
            # Try tables first
            tables = soup.find_all("table")
            
            for table in tables:
                # Skip tables that appear to be for pagination or navigation
                if len(table.find_all("tr")) <= 2:
                    continue
                    
                # Skip tables without enough data
                if len(table.find_all("td")) < 5:
                    continue
                
                rows = table.find_all("tr")
                # Skip header row
                for row in rows[1:]:
                    try:
                        cells = row.find_all(["td", "th"])
                        if len(cells) < 2:
                            continue
                        
                        # First column usually has the company name
                        company_name = cells[0].get_text().strip()
                        
                        # Skip if empty name or not a valid name
                        if not company_name or len(company_name) < 3:
                            continue
                            
                        # Try to extract website link if available
                        website = None
                        link = row.find("a")
                        if link and "href" in link.attrs:
                            href = link["href"]
                            if "http" in href.lower():
                                website = href
                        
                        # Create company data
                        company_data = {
                            "name": company_name,
                            "state": state,
                            "source_url": source_url
                        }
                        
                        if website:
                            company_data["website"] = website
                        
                        companies.append(company_data)
                        
                    except Exception as e:
                        logger.error(f"Error extracting company from regulatory table row: {e}")
            
            # If no companies found from tables, try lists
            if not companies:
                list_elements = soup.find_all(["ul", "ol"])
                
                for list_elem in list_elements:
                    # Skip small lists that likely aren't company listings
                    if len(list_elem.find_all("li")) < 5:
                        continue
                        
                    items = list_elem.find_all("li")
                    for item in items:
                        try:
                            # Extract name and website
                            company_name = item.get_text().strip()
                            
                            # Skip if not a valid name
                            if not company_name or len(company_name) < 3:
                                continue
                                
                            # Try to extract website if available
                            website = None
                            link = item.find("a")
                            if link and "href" in link.attrs:
                                href = link["href"]
                                if "http" in href.lower():
                                    website = href
                                    # If the link has text, use it as the name
                                    if link.get_text().strip():
                                        company_name = link.get_text().strip()
                            
                            # Skip if common navigation or non-company text
                            skip_words = ["click", "more", "here", "info", "details", "back", "next", "previous", "login"]
                            if any(word == company_name.lower() for word in skip_words):
                                continue
                            
                            # Create company data
                            company_data = {
                                "name": company_name,
                                "state": state,
                                "source_url": source_url
                            }
                            
                            if website:
                                company_data["website"] = website
                            
                            companies.append(company_data)
                            
                        except Exception as e:
                            logger.error(f"Error extracting company from regulatory list item: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting companies from regulatory page: {e}")
        
        logger.info(f"Found {len(companies)} companies on regulatory page {source_url}")
        return companies
    
    def _extract_company_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract company name from website content."""
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
            logger.error(f"Error extracting company name: {e}")
            
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
    
    def _find_website_for_company(self, company_name: str, state: str) -> Optional[str]:
        """Search for a company website using search engine."""
        from app.discovery.search_engine import SearchEngine
        search_engine = SearchEngine(self.db_session)
        
        # Create a search query
        query = f"{company_name} {state} official website"
        
        try:
            search_results = search_engine.execute_search(query, "oil_gas", state)
            
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
            logger.error(f"Error searching for company website: {e}")
            
        return None
    
    def _scrape_company_website(self, website: str, org_id: int) -> List[Dict[str, Any]]:
        """
        Scrape contacts from an oil, gas or mining company website.
        
        Args:
            website: Company website URL
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
        
        # Additional paths for oil, gas and mining specific departments
        industry_paths = ["operations", "about/operations", "about-us/operations",
                        "engineering", "about/engineering", "about-us/engineering",
                        "production", "field-operations", "drilling", "wellsite",
                        "technical-services", "technology", "systems", "departments/operations",
                        "pipeline", "mining", "exploration"]
        
        # Combine all paths
        all_paths = staff_paths + industry_paths
        
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
                                                                                "technical", "system", "production", 
                                                                                "drilling", "wellsite", "pipeline"]))
                    if not staff_elements:
                        staff_elements = soup.find_all(["div", "article", "section"], 
                                                    class_=lambda c: c and any(term in (c.lower() if c else "")
                                                                           for term in ["operations", "engineering", 
                                                                                      "technical", "system", "production", 
                                                                                      "drilling", "wellsite", "pipeline"]))
                
                # If still nothing, try a more general approach with the whole page
                if not staff_elements:
                    staff_elements = [soup]
                
                for element in staff_elements:
                    # Look for job titles that match our target
                    target_titles = ORG_TYPES["oil_gas"]["job_titles"]
                    
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
                                               "Chief", "Officer", "Operations", "Production", "Field",
                                               "Drilling", "Pipeline", "Technical", "Wellsite"]
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
                            
                            # Check if job title is relevant (operations, field, technical roles)
                            relevant_title = any(target.lower() in job_title.lower() for target in target_titles)
                            
                            # Also check for specific roles which are important for oil, gas and mining companies
                            operations_role = "operations" in job_title.lower() or "operator" in job_title.lower()
                            field_role = "field" in job_title.lower() or "production" in job_title.lower() or "wellsite" in job_title.lower()
                            drilling_role = "drilling" in job_title.lower() or "well" in job_title.lower()
                            pipeline_role = "pipeline" in job_title.lower()
                            technical_role = "technical" in job_title.lower() or "engineer" in job_title.lower()
                            
                            if not (relevant_title or operations_role or field_role or drilling_role or pipeline_role or technical_role):
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
                            
                            # Calculate relevance score - higher for operations and field roles
                            relevance_score = 7.0  # Base score
                            if operations_role:
                                relevance_score = 9.0
                            elif field_role or drilling_role:
                                relevance_score = 8.5
                            elif technical_role or pipeline_role:
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
                logger.error(f"Error scraping company website {url}: {e}")
                
        return contacts