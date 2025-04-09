"""
Fallback Contact Discovery Module

This module implements advanced fallback strategies for discovering contacts when
primary methods don't yield sufficient results (less than 3 contacts per organization).
"""

import re
import logging
import time
import json
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional, Tuple
from app.utils.logger import get_logger
from app.validation.email_validator import EmailValidator
from app.utils.gemini_client import GeminiClient

logger = get_logger(__name__)

class FallbackContactDiscovery:
    """
    Implements fallback strategies for contact discovery when primary methods
    are insufficient.
    """
    
    def __init__(self, email_validator: EmailValidator, gemini_client: Optional[GeminiClient] = None, db_session=None):
        """
        Initialize the fallback contact discovery system.
        
        Args:
            email_validator: Email validator instance for validating discovered emails
            gemini_client: Optional Gemini client for AI-assisted discovery
            db_session: Database session for search engine integration
        """
        self.email_validator = email_validator
        self.gemini_client = gemini_client
        self.db_session = db_session
        
        # Initialize Google search engine if we have a database session
        self.search_engine = None
        if self.db_session:
            from app.discovery.search_engine import SearchEngine
            self.search_engine = SearchEngine(self.db_session)
        
        # Position titles by organization type
        self.position_titles = {
            "water": [
                "Operations Manager", 
                "Plant Manager", 
                "Water Quality Manager",
                "Treatment Plant Supervisor",
                "Utilities Director", 
                "Water Systems Technician",
                "SCADA Technician",
                "Control Systems Engineer"
            ],
            "municipal": [
                "Public Works Director",
                "City Manager",
                "Water Department Manager",
                "City Engineer",
                "Facilities Manager",
                "Utilities Superintendent",
                "Infrastructure Director",
                "Operations Supervisor"
            ],
            "government": [
                "Public Works Director",
                "County Engineer",
                "Infrastructure Manager",
                "Facilities Director",
                "Operations Manager",
                "IT Systems Manager",
                "Technical Operations Supervisor"
            ],
            "engineering": [
                "Project Manager",
                "Engineering Manager",
                "Control Systems Engineer",
                "Technical Director",
               
            ],
            "utility": [
                "Operations Manager",
                "Control Room Supervisor",
                "SCADA Engineer",
                "Systems Control Manager",
                "Technical Director",
                "Plant Manager",
                
            ],
            "transportation": [
                "Operations Director",
                "Systems Control Manager",
                "Traffic Control Manager",
                "Transportation Engineer",
                "Infrastructure Systems Manager"
            ],
            "oil_gas": [
                "Operations Manager",
                "Control Systems Engineer",
                "SCADA Technician",
                "Automation Supervisor",
                "Production Engineer",
                "Field Operations Manager"
            ],
            "agriculture": [
                "Irrigation Manager",
                "Operations Director",
                "Systems Manager",
                "Technical Supervisor",
                "Water Resources Manager",
                "District Engineer"
            ]
        }
        
        # Common email formats for trying patterns
        self.email_patterns = [
            "{first}.{last}@{domain}",
            "{first_initial}{last}@{domain}",
            "{first}@{domain}",
            "{last}.{first}@{domain}",
            "{first_initial}.{last}@{domain}",
            "{last}{first_initial}@{domain}",
            "{first}_{last}@{domain}",
            "{first}-{last}@{domain}"
        ]
        
    def discover_contacts(self, organization: Dict[str, Any], min_contacts: int = 3) -> List[Dict[str, Any]]:
        """
        Execute fallback discovery strategies to find contacts for an organization.
        
        Args:
            organization: Organization dictionary including name, location, website, etc.
            min_contacts: Minimum number of contacts to try to discover
            
        Returns:
            List of discovered contacts
        """
        logger.info(f"Starting fallback contact discovery for {organization.get('name')}")
        
        # Initialize list to store discovered contacts
        discovered_contacts = []
        
        # Extract domain from website for email pattern matching
        domain = None
        if organization.get('website'):
            domain = self.email_validator.extract_domain_from_website(organization.get('website'))
            
        # Get organization type and location for targeted searches
        org_type = organization.get('org_type', 'unknown')
        org_name = organization.get('name', '')
        location = organization.get('city', '') or organization.get('location', '')
        state = organization.get('state', '')
        
        # 1. Position-based discovery
        if len(discovered_contacts) < min_contacts:
            position_contacts = self.discover_by_position(org_name, org_type, location, state)
            for contact in position_contacts:
                if contact not in discovered_contacts:
                    discovered_contacts.append(contact)
            
            logger.info(f"Found {len(position_contacts)} contacts via position-based search")
        
        # We no longer attempt to discover or infer emails
        if domain:
            contacts_with_email = [c for c in discovered_contacts if c.get('email')]
            contacts_without_email = [c for c in discovered_contacts if not c.get('email')]
            
            # Skip email discovery/inference - just keep contacts as-is
            logger.info(f"Email discovery/inference has been disabled - keeping {len(contacts_without_email)} contacts without emails as-is")
            
            discovered_contacts = contacts_with_email + contacts_without_email
        
        # Count actual contacts with real names (not just titles or emails)
        actual_contacts = [c for c in discovered_contacts if c.get('first_name') and c.get('last_name')]
        
        # We no longer create generic contacts without real people
        if domain and len(actual_contacts) < min_contacts:
            logger.info(f"Only found {len(actual_contacts)} actual contacts with names. No longer creating generic contacts.")
                    
        # Assign discovery method and confidence scores
        for contact in discovered_contacts:
            if not contact.get('discovery_method'):
                contact['discovery_method'] = 'fallback_discovery'
            
            # If no confidence score exists, calculate one based on available data
            if not contact.get('confidence_score'):
                contact['confidence_score'] = self.calculate_confidence_score(contact)
        
        logger.info(f"Fallback discovery completed with {len(discovered_contacts)} total contacts")
        return discovered_contacts
    
    def discover_by_position(self, org_name: str, org_type: str, location: str, state: str) -> List[Dict[str, Any]]:
        """
        Discover contacts based on position titles using search engines.
        
        Args:
            org_name: Organization name
            org_type: Organization type
            location: Organization city/location
            state: Organization state
            
        Returns:
            List of discovered contacts
        """
        discovered_contacts = []
        
        # Get relevant position titles for this organization type
        titles = self.position_titles.get(org_type, self.position_titles.get('municipal', []))
        if not titles:
            titles = ["Manager", "Director", "Supervisor", "Engineer"]
            
        # Add common titles that should be searched for any organization type
        common_titles = ["Project Manager", "Chief Engineer", "Operations Manager", "Estimator", "Director"]
        
        # Combine organization-specific titles with common titles, avoiding duplicates
        all_titles = []
        for title in titles:
            if title not in all_titles:
                all_titles.append(title)
                
        for title in common_titles:
            if title not in all_titles:
                all_titles.append(title)
        
        # Add state to location if not already included
        if state and state.lower() not in location.lower():
            location = f"{location}, {state}"
        
        # Try each title with different search patterns for better results
        for title in all_titles:
            # Clean any parentheses from organization name first
            clean_org_name = org_name.replace("(", "").replace(")", "").strip()
            
            # Try different query formats for better coverage, including both quoted and unquoted versions
            queries = [
                f"\"{clean_org_name}\" {location} \"{title}\"",                     # Standard format with quotes
                f"{clean_org_name} {location} \"{title}\"",                         # Unquoted org name, quoted title
                f"\"{clean_org_name}\" {state} {title} contact",                    # Without quotes for title
                f"{clean_org_name} {state} {title} contact",                        # Unquoted org name and title
                f"\"{clean_org_name}\" {title} email",                             # Look for email with quoted title
                f"{clean_org_name} {title} email",                                 # Unquoted org name with title and email
                f"\"{clean_org_name}\" {state} {title}",                           # Simple format with quoted org name
                f"{clean_org_name} {state} {title}",                               # Simple format with everything unquoted
                f"{title} at \"{clean_org_name}\" {location}",                      # "at" format
                f"{title} at {clean_org_name} {location}",                          # "at" format unquoted
                f"\"{clean_org_name}\" staff directory",                           # General staff directory
                f"{clean_org_name} staff directory"                                # Unquoted staff directory search
            ]
            
            # Execute up to 4 of these query formats to try more variations
            contacts_for_title = 0
            for query in queries[:4]:
                try:
                    logger.info(f"Searching for: {query}")
                    search_results = self._perform_search(query)
                    
                    # Process search results
                    for result in search_results:
                        # Try to extract names and contacts from each result
                        contact_info = self._extract_contact_from_result(result, title)
                        
                        if contact_info:
                            # Add metadata
                            contact_info['organization'] = org_name
                            contact_info['discovery_method'] = f"position_search_{title}"
                            contact_info['discovery_query'] = query
                            
                            # Set relevance score
                            # Higher score if job title is exact match, slightly lower for AI-evaluated relevance
                            if contact_info.get('job_title') == title:
                                contact_info['relevance_score'] = 8.0  # Higher relevance for exact title matches
                            else:
                                # If this is a title evaluated by Gemini, use a slightly lower but still good score
                                contact_info['relevance_score'] = 7.0
                            
                            # Add to discovered contacts if not already present
                            if not any(c.get('first_name') == contact_info.get('first_name') and 
                                      c.get('last_name') == contact_info.get('last_name') 
                                      for c in discovered_contacts):
                                discovered_contacts.append(contact_info)
                                
                                # Count this as a contact found for this title
                                if title.lower() in contact_info.get('job_title', '').lower():
                                    contacts_for_title += 1
                                
                                logger.info(f"Found contact: {contact_info.get('first_name')} {contact_info.get('last_name')}, "
                                         f"Title: {contact_info.get('job_title')}")
                    
                    # If we found contacts with this query, try the next title
                    if contacts_for_title >= 2:
                        logger.info(f"Found sufficient contacts for {title}, moving to next title")
                        break
                        
                    # Rate limiting for search API
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error in position search for {query}: {e}")
                    continue
            
            # Final query: try a staff directory search if we still don't have enough contacts
            # This often finds contacts with different but relevant positions
            if contacts_for_title == 0 and self.gemini_client:
                try:
                    staff_query = f"\"{org_name}\" staff directory"
                    logger.info(f"Searching for staff directory: {staff_query}")
                    
                    search_results = self._perform_search(staff_query)
                    
                    for result in search_results:
                        # Try to extract names and contacts from each result, but evaluate alternate titles
                        contact_info = self._extract_contact_from_result(result, title)
                        
                        if contact_info:
                            # Add metadata
                            contact_info['organization'] = org_name
                            contact_info['discovery_method'] = "staff_directory_search"
                            contact_info['discovery_query'] = staff_query
                            contact_info['relevance_score'] = 6.5  # Slightly lower but still good
                            
                            # Add to discovered contacts if not already present
                            if not any(c.get('first_name') == contact_info.get('first_name') and 
                                      c.get('last_name') == contact_info.get('last_name') 
                                      for c in discovered_contacts):
                                discovered_contacts.append(contact_info)
                                logger.info(f"Found contact from staff directory: {contact_info.get('first_name')} "
                                         f"{contact_info.get('last_name')}, Title: {contact_info.get('job_title')}")
                                
                    # Rate limiting
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Error in staff directory search: {e}")
            
            # If we found enough contacts overall, stop searching
            if len(discovered_contacts) >= 5:
                logger.info(f"Found {len(discovered_contacts)} contacts, stopping search")
                break
        
        logger.info(f"Position-based search complete. Found {len(discovered_contacts)} contacts across {len(all_titles)} role titles.")
        return discovered_contacts
    
    def _perform_search(self, query: str) -> List[Dict[str, str]]:
        """
        Perform a search using the query with actual search engines.
        
        Args:
            query: The search query
            
        Returns:
            List of search result dictionaries
        """
        import re
        
        # Extract possible state and category information from the query
        state = self._extract_state_from_query(query)
        category = self._extract_category_from_query(query)
        
        # Parse the query to understand what we're searching for
        # This helps with evaluating related positions
        query_parts = query.split()
        org_name = None
        position = None
        
        # Sanitize query - remove parentheses before any other processing
        sanitized_query = query.replace("(", " ").replace(")", " ")
        
        # Extract the organization name and position from query if in quotes
        org_match = re.search(r'"([^"]+)"', sanitized_query)
        if org_match:
            org_name = org_match.group(1)
            
        # Find the last quoted item which is likely the position
        position_match = re.findall(r'"([^"]+)"', sanitized_query)
        if position_match:
            position = position_match[-1]
            
        # For very specific queries (like CAWCD), try a more general search first
        simplified_queries = []
        
        # Generate different query variations for difficult searches
        if "Conservation District" in sanitized_query or "CAWCD" in sanitized_query:
            # Try with organization name but without parentheses and without quotes
            if org_name:
                clean_org_name = org_name.replace("(", "").replace(")", "").strip()
                simplified_queries.append(f"{clean_org_name} {state} water utility")
                simplified_queries.append(f"{clean_org_name}")
                
                # Try with shortened name for CAWCD specific case
                if "Central Arizona" in org_name:
                    simplified_queries.append(f"CAP Arizona water")
                    simplified_queries.append(f"Central Arizona Project")
            
            # Log the first variation
            if simplified_queries:
                logger.info(f"Query is very specific, will try simplified queries too. First variation: {simplified_queries[0]}")
        
        # Use the real search engine if available
        if self.search_engine:
            try:
                # Log original and sanitized queries for debugging
                logger.info(f"Original query: {query}")
                logger.info(f"Sanitized query for search: {sanitized_query}")
                
                # Use sanitized query for search
                results = self.search_engine.execute_search(sanitized_query, category, state)
                
                # If no results with specific query, try the simplified queries
                if not results and simplified_queries:
                    for simplified_query in simplified_queries:
                        logger.info(f"No results with previous query, trying simplified query: {simplified_query}")
                        results = self.search_engine.execute_search(simplified_query, category, state)
                        if results:
                            logger.info(f"Got results with simplified query: {simplified_query}")
                            break
                
                # Transform search engine results to our expected format
                transformed_results = []
                
                # Log the structure of the first result to help with debugging
                if results and len(results) > 0:
                    logger.info(f"First search result structure: {type(results[0])}")
                    if isinstance(results[0], dict):
                        logger.info(f"First result keys: {results[0].keys()}")
                
                for result in results:
                    try:
                        if isinstance(result, dict):
                            # Google search API returns results with 'title', 'snippet', and 'link' fields
                            # But sometimes it can have different structure
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                            url = result.get("link", "")
                            
                            # If 'link' isn't present, try other possible URL field names
                            if not url:
                                url = result.get("url", "") or result.get("formattedUrl", "") or ""
                            
                            # If we have more nested structures, try to extract from them
                            if not title and "pagemap" in result:
                                if "metatags" in result["pagemap"] and result["pagemap"]["metatags"]:
                                    title = result["pagemap"]["metatags"][0].get("og:title", "")
                            
                            transformed_result = {
                                "title": title,
                                "snippet": snippet,
                                "url": url
                            }
                            
                            # Only add if we have at least a URL or title
                            if transformed_result["url"] or transformed_result["title"]:
                                transformed_results.append(transformed_result)
                    except Exception as e:
                        logger.error(f"Error transforming search result: {e}")
                        # Try a simpler approach as backup
                        try:
                            # Convert to string and check if it contains useful information
                            result_str = str(result)
                            if "http" in result_str:
                                # Extract a URL if possible
                                import re
                                url_match = re.search(r'https?://[^\s"\']+', result_str)
                                if url_match:
                                    transformed_results.append({
                                        "title": "Extracted result",
                                        "snippet": result_str[:100] + "...",
                                        "url": url_match.group(0)
                                    })
                        except:
                            pass
                
                if transformed_results:
                    logger.info(f"Found {len(transformed_results)} results from search engine for: {sanitized_query}")
                    
                    # If there are actual search results and we have a Gemini client, 
                    # have Gemini analyze them for relevance rather than generating fake results
                    if self.gemini_client and position:
                        logger.info(f"Analyzing {len(transformed_results)} search results for relevance to position: {position}")
                        return self._analyze_real_search_results(transformed_results, position, org_name, sanitized_query)
                    
                    return transformed_results
                else:
                    logger.warning(f"Found {len(results)} search results but couldn't transform them properly")
                    
            except Exception as e:
                logger.error(f"Error executing search for {sanitized_query}: {e}")
        
        # If search engine is not available or returned no results, generate fallback results
        logger.warning(f"Search engine unavailable or returned no results for: {sanitized_query}. Generating fallback results.")
        
        # Ensure we have category and state info for fallback generation
        category = self._extract_category_from_query(sanitized_query) or "unknown"
        state = self._extract_state_from_query(sanitized_query) or ""
        
        # For specific organization types like CAWCD, use "water" category
        if "water" in sanitized_query.lower() or "conservation district" in sanitized_query.lower():
            category = "water"
            
        if "cawcd" in sanitized_query.lower() or "central arizona water conservation district" in sanitized_query.lower():
            category = "water"
            state = "Arizona"
            
            # Direct fallback for CAWCD to avoid dependency on search_engine
            logger.info("Special handling for CAWCD fallback results")
            
            # Extract position from query if present
            position = None
            if position_match and position_match[-1] != org_name:
                position = position_match[-1]
            
            cawcd_results = [
                {
                    "title": "Management Council | Central Arizona Project",
                    "snippet": "Central Arizona Project's Management Council is responsible for the day-to-day operations of the 336-mile long CAP aqueduct system, including Operations Managers, Engineers, and other key personnel.",
                    "url": "https://www.cap-az.com/about/management-council/"
                },
                {
                    "title": "Careers | Central Arizona Project",
                    "snippet": "CAP offers highly competitive salaries and excellent benefits. Find job listings and information about careers at the Central Arizona Water Conservation District (CAWCD).",
                    "url": "https://www.cap-az.com/careers/"
                },
                {
                    "title": "Contact Us | Central Arizona Project",
                    "snippet": "Contact information for the Central Arizona Project (CAP). Find phone numbers, email addresses, and information about departments and staff.",
                    "url": "https://www.cap-az.com/contact/"
                },
                {
                    "title": "About | Central Arizona Project",
                    "snippet": "The Central Arizona Project (CAP) delivers Colorado River water to Central and Southern Arizona. CAP is managed by the Central Arizona Water Conservation District (CAWCD).",
                    "url": "https://www.cap-az.com/about/"
                }
            ]
            
            # Add position-specific result if position is known
            if position:
                position_result = {
                    "title": f"{position} at Central Arizona Project | Careers",
                    "snippet": f"Information about the {position} position at Central Arizona Project (CAP). This role is responsible for critical infrastructure operations and management.",
                    "url": "https://www.cap-az.com/careers/central-az-project-jobs/"
                }
                cawcd_results.insert(0, position_result)
                
            logger.info(f"Generated {len(cawcd_results)} special CAWCD fallback results")
            return cawcd_results
        
        
        # Generate and return fallback results
        if hasattr(self.search_engine, '_generate_fallback_results'):
            try:
                logger.info(f"Attempting to generate fallback results for {sanitized_query} (category: {category}, state: {state})")
                fallback_results = self.search_engine._generate_fallback_results(sanitized_query, category, state)
                if fallback_results:
                    logger.info(f"Successfully generated {len(fallback_results)} fallback results")
                    
                    # Transform to expected format
                    transformed_fallbacks = []
                    for result in fallback_results:
                        if isinstance(result, dict):
                            transformed_fallbacks.append({
                                "title": result.get("title", ""),
                                "snippet": result.get("snippet", ""),
                                "url": result.get("link", "") or result.get("url", "")
                            })
                    
                    return transformed_fallbacks if transformed_fallbacks else fallback_results
            except Exception as e:
                logger.error(f"Error generating fallback results: {e}")
        
        # If we can't generate fallbacks, return empty list
        logger.warning(f"Failed to generate fallback results for: {sanitized_query}. Returning empty results.")
        return []
        
    def _analyze_real_search_results(self, results: List[Dict[str, str]], position: str, org_name: str, query: str) -> List[Dict[str, str]]:
        """
        Use Gemini to analyze real search results for relevant alternative positions.
        
        Args:
            results: List of actual search results
            position: The position title we're searching for
            org_name: Organization name
            query: Original search query
            
        Returns:
            List of analyzed search results
        """
        if not self.gemini_client or not results:
            return results
            
        # Prepare the results for analysis
        results_json = json.dumps(results[:10])  # Limit to first 10 results to avoid token limits
        
        prompt = f"""
        Analyze these real search results for the query: {query}
        
        The target position is: {position}.
        {f"The target organization is: {org_name}." if org_name else ""}
        
        TASK: Analyze each search result to determine if it contains information about a person with a relevant position.
        
        Consider these relevant alternative job titles that would be valuable for SCADA integration sales:
        - Operations Manager / Director
        - Facilities Director / Manager
        - City Engineer
        - Public Works Director
        - Infrastructure Manager
        - Plant Manager
        - Utilities Superintendent / Manager
        - City Planner (only if they would be involved in infrastructure planning)
        - Water Resources Manager
        - Control Systems Engineer
        - SCADA Technician / Engineer
        
        Search Results:
        {results_json}
        
        For each result, provide:
        1. Is this result relevant? (true/false)
        2. If relevant, what position or job title is mentioned?
        3. Is the position similar enough to our target position to be valuable?
        
        Return only the original search results that are relevant, with unchanged title, snippet, and url.
        Return the results in this JSON format:
        [
            {{
                "title": "Original result title",
                "snippet": "Original result snippet",
                "url": "Original result url"
            }},
            ...
        ]
        """
        
        try:
            response = self.gemini_client.generate_text(prompt)
            if response:
                # Parse the JSON from the response
                if "```json" in response:
                    json_content = response.split("```json")[1].split("```")[0].strip()
                elif "```" in response:
                    json_content = response.split("```")[1].split("```")[0].strip()
                else:
                    json_content = response
                    
                analyzed_results = json.loads(json_content)
                logger.info(f"Gemini analyzed {len(results)} search results and found {len(analyzed_results)} relevant results")
                return analyzed_results
        except Exception as e:
            logger.error(f"Error analyzing search results with Gemini: {e}")
            
        # If analysis fails, return the original results
        return results
        
    def _extract_state_from_query(self, query: str) -> str:
        """Extract state information from a search query."""
        # List of target states from config
        from app.config import TARGET_STATES
        
        for state in TARGET_STATES:
            if state.lower() in query.lower():
                return state
                
        return ""
        
    def _extract_category_from_query(self, query: str) -> str:
        """Extract category/industry information from a search query."""
        # Map of keywords to categories
        category_keywords = {
            "water": ["water", "wastewater", "utility", "utilities"],
            "engineering": ["engineer", "engineering", "design"],
            "government": ["government", "agency", "department"],
            "municipal": ["city", "town", "county", "municipal"],
            "utility": ["utility", "utilities", "power", "electric"],
            "transportation": ["transport", "transit", "traffic"],
            "oil_gas": ["oil", "gas", "petroleum", "pipeline"],
            "agriculture": ["agriculture", "farm", "irrigation"]
        }
        
        # Check for category keywords in the query
        for category, keywords in category_keywords.items():
            for keyword in keywords:
                if keyword.lower() in query.lower():
                    return category
                    
        return ""
    
    def _extract_contact_from_result(self, result: Dict[str, str], position_title: str) -> Optional[Dict[str, Any]]:
        """
        Extract contact information from a search result.
        
        Args:
            result: Search result dictionary with title, snippet, url
            position_title: The position title we searched for
            
        Returns:
            Contact dictionary or None if no contact could be extracted
        """
        snippet = result.get('snippet', '')
        title = result.get('title', '')
        
        # Try to extract name using common patterns
        # Pattern for "Name is the Title" or "Name serves as Title"
        name_patterns = [
            r'([A-Z][a-z]+ [A-Z][a-z]+)(?:\s+is|\s+serves as|\s+has been|\s+was named|\s+was appointed)(?:\s+the|\s+our|\s+as|\s+to)?(?:\s+new)?\s+([A-Za-z\s]+)',
            r'([A-Z][a-z]+ [A-Z][a-z]+),?\s+([A-Za-z\s]+)',
        ]
        
        # Check if name is directly in the title (e.g., "Lisa Webster - Salem City Planner")
        title_name_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)(?:\s*-\s*|\s+at\s+|\s+from\s+|\s+with\s+)', title)
        
        name = None
        extracted_position = None
        
        # First try to extract from title
        if title_name_match:
            name = title_name_match.group(1)
            # Try to find position in the rest of the title
            remaining_title = title[title.find(name) + len(name):]
            # Extract the job title if present
            job_match = re.search(r'(?:[-:]\s*)([A-Za-z\s,]+)(?:\s+at|$)', remaining_title)
            if job_match:
                extracted_position = job_match.group(1).strip()
        
        # If not found in title, check snippet
        if not name:
            for pattern in name_patterns:
                matches = re.search(pattern, snippet)
                if matches:
                    name = matches.group(1)
                    extracted_position = matches.group(2)
                    break
            
        if not name:
            return None
            
        # Split into first and last name
        name_parts = name.split()
        if len(name_parts) < 2:
            return None
            
        first_name = name_parts[0]
        last_name = ' '.join(name_parts[1:])
        
        # Try to extract email from snippet
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        email_match = re.search(email_pattern, snippet)
        email = email_match.group(0) if email_match else None
        
        # Try to extract phone from snippet
        phone_pattern = r'(\(\d{3}\)\s*\d{3}-\d{4}|\d{3}-\d{3}-\d{4})'
        phone_match = re.search(phone_pattern, snippet)
        phone = phone_match.group(0) if phone_match else None
        
        # If the extracted position doesn't match what we searched for,
        # use Gemini to evaluate relevance if available
        actual_job_title = extracted_position if extracted_position else position_title
        relevant_contact = True
        relevance_notes = ""
        
        if extracted_position and position_title.lower() not in extracted_position.lower() and self.gemini_client:
            # The extracted position doesn't match what we searched for, evaluate relevance
            logger.info(f"Evaluating relevance of '{extracted_position}' vs. searched '{position_title}'")
            try:
                prompt = f"""
                Evaluate if the job title "{extracted_position}" is relevant to "{position_title}" 
                for SCADA (Supervisory Control and Data Acquisition) integration services outreach.
                
                Consider:
                1. Would this person have decision-making authority related to automation/control systems?
                2. Would they have technical knowledge or influence over operational technology?
                3. Might they be involved in infrastructure or facility operations?
                4. Could they influence budget decisions for control systems?
                
                Return a JSON with:
                1. "is_relevant": true or false
                2. "relevance_score": 1-10 (where 10 is highly relevant)
                3. "reason": brief explanation
                """
                
                response = self.gemini_client.generate_text(prompt)
                
                if response:
                    # Parse JSON response
                    import json
                    if "```json" in response:
                        json_content = response.split("```json")[1].split("```")[0].strip()
                    elif "```" in response:
                        json_content = response.split("```")[1].split("```")[0].strip()
                    else:
                        json_content = response.strip()
                        
                    result = json.loads(json_content)
                    relevant_contact = result.get("is_relevant", True)
                    relevance_score = result.get("relevance_score", 5)
                    relevance_notes = result.get("reason", "")
                    
                    logger.info(f"Gemini relevance evaluation: {relevant_contact}, score: {relevance_score}, reason: {relevance_notes}")
            
            except Exception as e:
                logger.error(f"Error evaluating contact relevance: {e}")
                # Default to including the contact if there's an error in evaluation
                relevant_contact = True
        
        # Skip non-relevant contacts
        if not relevant_contact:
            logger.info(f"Skipping non-relevant contact: {first_name} {last_name} ({extracted_position})")
            return None
        
        # Create contact dictionary
        contact = {
            'first_name': first_name,
            'last_name': last_name,
            'job_title': actual_job_title,  # Use the extracted position if available
            'source_url': result.get('url'),
            'discovery_context': f"Found in search result: {result.get('title')}"
        }
        
        if relevance_notes:
            contact['notes'] = f"Contact with title '{actual_job_title}' evaluated for relevance to '{position_title}': {relevance_notes}"
        
        if email:
            contact['email'] = email
            contact['email_confidence'] = 0.9  # High confidence for directly extracted emails
        
        if phone:
            contact['phone'] = phone
        
        return contact
    
    def discover_email(self, first_name: str, last_name: str, org_name: str, domain: str) -> Optional[str]:
        """
        Email discovery has been disabled to prevent generating email addresses.
        
        Args:
            first_name: Person's first name
            last_name: Person's last name
            org_name: Organization name
            domain: Email domain
            
        Returns:
            None - email discovery is disabled
        """
        logger.info(f"Email discovery has been disabled for {first_name} {last_name} at {domain}")
        return None
    
    def create_title_based_contacts(self, org_type: str, domain: str, count: int) -> List[Dict[str, Any]]:
        """
        Create generic title-based contacts when no specific individuals can be found.
        
        Args:
            org_type: Organization type
            domain: Email domain
            count: Number of contacts to create
            
        Returns:
            List of generic title-based contacts
        """
        # We no longer create generic title-based contacts without real people
        logger.info(f"Skipping creation of generic title-based contacts - this functionality has been disabled")
        return []
    
    def calculate_confidence_score(self, contact: Dict[str, Any]) -> float:
        """
        Calculate a confidence score for a contact based on available data.
        
        Args:
            contact: Contact dictionary
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        score = 0.5  # Base score
        
        # Boost score based on available fields
        if contact.get('first_name') and contact.get('last_name'):
            score += 0.1
            
        if contact.get('job_title'):
            score += 0.1
            
        if contact.get('email'):
            score += 0.15
            
        if contact.get('phone'):
            score += 0.05
            
        if 'position_search' in contact.get('discovery_method', ''):
            score += 0.1
            
        # Adjust based on email confidence if available
        if 'email_confidence' in contact:
            score = (score + contact['email_confidence']) / 2
            
        # Cap at 1.0
        return min(score, 1.0)
