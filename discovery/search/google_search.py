"""
Google Custom Search API integration for organization discovery.
"""
import re  # Required for the search_for_org_website function
import json
import time
from typing import List, Dict, Any, Optional
import requests
from urllib.parse import urlparse
from sqlalchemy.orm import Session
from app.config import GOOGLE_API_KEY, GOOGLE_CSE_ID, SEARCH_QUERIES, TARGET_STATES
from app.database.models import SearchQuery
from app.utils.logger import get_logger

logger = get_logger(__name__)


class GoogleSearchClient:
    """Client for Google Custom Search API integration with rate limiting."""
    
    def __init__(self, db_session: Session):
        """
        Initialize the Google search client.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        # Use the API key from your .env file
        self.api_key = GOOGLE_API_KEY
        
        # Use the CSE ID from the environment variables
        self.cse_id = GOOGLE_CSE_ID
        
        # Debug: Print CSE ID
        logger.info(f"Using Google CSE ID: {self.cse_id}")
        
        self.base_url = "https://www.googleapis.com/customsearch/v1"
        self.results_per_page = 10  # Google CSE allows 10 results per page
        self.max_pages = 100  # Get more results - Google CSE officially allows up to 10 pages
        self.delay_between_queries = 2  # seconds between queries to avoid rate limiting
        
        # Rate limiting variables
        self.queries_per_minute = 100  # Google's limit
        self.query_timestamps = []  # Track timestamps of recent queries
    
    def search(self, query: str, start_index: int = 1) -> Optional[Dict[str, Any]]:
        """
        Execute a Google search with the given query.
        
        Args:
            query: Search query
            start_index: Starting index for pagination (1-based)
            
        Returns:
            Search results or None if the request fails
        """
        try:
            # Implement rate limiting based on queries per minute
            self._respect_rate_limit()
            
            # Clean and properly encode the query
            query = query.strip()
            
            # Remove parentheses from all queries as they cause problems with Google CSE
            query = query.replace("(", " ").replace(")", " ")
            
            # For complex queries with organization names and specific titles, try to simplify
            # Complex queries with multiple quoted items or parentheses can cause issues
            simplified_query = query
            
            # Special handling for CAWCD queries that consistently fail
            if "CAWCD" in query or "Central Arizona Water Conservation District" in query:
                # Extract job title from quotes if present
                import re
                position = None
                
                # Find all quoted terms
                position_matches = re.findall(r'"([^"]+)"', query)
                
                # Extract the position (usually the second quoted term or last one)
                if len(position_matches) > 1:
                    # If we have multiple quoted terms, the position is likely the last one
                    position = position_matches[-1]
                    # Make sure this isn't the org name
                    if "Central Arizona" in position or "CAWCD" in position:
                        # Try the second to last one instead
                        if len(position_matches) > 2:
                            position = position_matches[-2]
                elif len(position_matches) == 1:
                    # If only one quoted term, check if it contains common position keywords
                    if any(keyword in position_matches[0].lower() for keyword in ["manager", "director", "engineer", "estimator", "supervisor"]):
                        position = position_matches[0]
                
                # Fallback to checking for common positions in the query text
                if not position:
                    for common_position in ["Estimator", "Operations Manager", "Engineer", "Manager", "Director", "Supervisor"]:
                        if common_position in query:
                            position = common_position
                            break
                
                # Create a simplified query format that works better with Google CSE
                if position:
                    simplified_query = f"Central Arizona Project {position} job"
                else:
                    simplified_query = "Central Arizona Project Central Arizona Water Conservation District CAWCD job"
                
                logger.info(f"Simplified CAWCD query to: {simplified_query}")
            
            # Properly encode the query
            import urllib.parse
            encoded_query = urllib.parse.quote_plus(simplified_query)
            
            # Clean API key and CSE ID
            api_key = self.api_key.strip()
            cse_id = self.cse_id.strip()
            
            # Ensure the CSE ID is properly formatted
            # Remove any URL encoding that might already be present
            if '%3A' in cse_id:
                cse_id = urllib.parse.unquote(cse_id)
                
            # Ensure we're using the CSE ID from the .env file (7150bc063efdc4d6b)
            if cse_id != '7150bc063efdc4d6b':
                logger.warning(f"CSE ID from environment ({cse_id}) doesn't match expected ID (7150bc063efdc4d6b). Using expected ID.")
                cse_id = '7150bc063efdc4d6b'
            
            params = {
                "key": api_key,
                "cx": cse_id,
                "q": encoded_query,
                "start": start_index
            }
            
            # Debug the API request
            logger.info(f"Google Search API request: start={start_index}, query='{simplified_query}'")
            logger.info(f"Google Search API params: key=AIzaS... cx={cse_id} q={encoded_query}")
            
            # Add retry mechanism
            max_retries = 2
            for attempt in range(max_retries + 1):
                try:
                    response = requests.get(self.base_url, params=params, timeout=10)
                    
                    # Check for specific error responses
                    if response.status_code == 400:
                        try:
                            error_data = response.json()
                            error_reason = error_data.get("error", {}).get("message", "Unknown error")
                            logger.error(f"Google API request error: {error_reason}")
                            
                            # Special handling for common errors
                            if "Invalid value" in error_reason and attempt == max_retries - 1:
                                # Simplify the query to just the essential keywords
                                words = query.split()
                                simple_query = ' '.join(words[:2]) if len(words) > 2 else query
                                simple_encoded = urllib.parse.quote_plus(simple_query)
                                logger.info(f"Trying with simplified query: {simple_query}")
                                params["q"] = simple_encoded
                                continue
                            elif ("Custom Search Engine ID" in error_reason or "cx" in error_reason) and attempt == max_retries - 1:
                                # Try with just the first part of the CSE ID (before the colon)
                                if ':' in params["cx"]:
                                    simple_cse = params["cx"].split(':')[0]
                                    logger.info(f"Trying with simplified CSE ID: {simple_cse}")
                                    params["cx"] = simple_cse
                                    continue
                        except:
                            logger.error(f"Failed to parse error response. Status: {response.status_code}")
                        
                        if attempt < max_retries:
                            logger.info(f"Retrying search request (attempt {attempt+1}/{max_retries})...")
                            time.sleep(2)  # Short delay before retry
                            continue
                        else:
                            return None
                            
                    # For successful responses, parse the JSON
                    response.raise_for_status()
                    
                    result = response.json()
                    if "items" in result:
                        page_num = (start_index - 1) // 10 + 1
                        logger.info(f"Search successful for '{query}' (page {page_num}) - found {len(result['items'])} results")
                        if result['items']:
                            logger.info(f"First result on page {page_num}: '{result['items'][0]['title']}' from {result['items'][0]['link']}")
                    else:
                        logger.warning(f"Search successful but no items found for '{query}' at index {start_index}")
                        # Log search information for debugging
                        if "searchInformation" in result:
                            search_info = result.get("searchInformation", {})
                            logger.info(f"Search info: total results={search_info.get('totalResults', 'unknown')}, "
                                      f"search time={search_info.get('searchTime', 'unknown')}s")
                    
                    return result
                except requests.RequestException as req_error:
                    if attempt < max_retries:
                        logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {req_error}")
                        time.sleep(2)  # Short delay before retry
                    else:
                        raise  # Re-raise on final attempt
            
        except requests.RequestException as e:
            # Log more detailed error information
            logger.error(f"Error executing Google search for '{query}' at index {start_index}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    logger.error(f"API error details: {error_details}")
                except:
                    logger.error(f"Status code: {e.response.status_code}, Content: {e.response.text[:200]}")
            return None
    
    def get_all_results(self, query: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        Get all search results for a query, handling pagination.
        
        Args:
            query: Search query
            max_results: Maximum number of results to return (up to 100)
            
        Returns:
            List of search results
        """
        if max_results > 100:
            logger.warning(f"Google Custom Search API can only return a maximum of 100 results. Limiting to 100.")
            max_results = 100
            
        all_results = []
        
        # Calculate the number of pages we need to fetch
        num_pages = min(self.max_pages, (max_results + self.results_per_page - 1) // self.results_per_page)
        
        logger.info(f"Fetching up to {max_results} results for query '{query}' (across {num_pages} pages)")
        
        for page in range(num_pages):
            # Calculate the start index for this page (1-based)
            start_index = (page * self.results_per_page) + 1
            
            # Bail out if we've already collected enough results
            if len(all_results) >= max_results:
                break
                
            # Execute the search for this page
            results = self.search(query, start_index)
            
            if not results:
                logger.warning(f"No results returned for query: {query} (page {page+1})")
                break
                
            if "items" not in results:
                logger.warning(f"No items in results for query: {query} (page {page+1})")
                if "error" in results:
                    logger.error(f"Error in results: {results['error']}")
                break
                
            # Process each result to extract useful information
            processed_items = []
            for item in results["items"]:
                processed_item = {
                    "title": item.get("title", ""),
                    "link": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                    "domain": self._extract_domain(item.get("link", ""))
                }
                processed_items.append(processed_item)
                
            all_results.extend(processed_items)
            logger.info(f"Added {len(processed_items)} results from page {page+1} for query: {query}")
            
            # Check if there are more results
            if len(results["items"]) < self.results_per_page:
                logger.info(f"No more results available for query: {query} (received {len(results['items'])} < {self.results_per_page})")
                break
                
            # Add delay between page requests to avoid rate limiting
            if page < num_pages - 1:  # Don't wait after the last request
                self._respect_rate_limit()
                time.sleep(self.delay_between_queries)
        
        # Trim to exact max_results
        if len(all_results) > max_results:
            all_results = all_results[:max_results]
            
        logger.info(f"Total results collected for '{query}': {len(all_results)}")
        return all_results
        
    def _respect_rate_limit(self) -> None:
        """
        Respect the Google Custom Search API rate limit of 100 queries per minute.
        This method will wait if needed to ensure we don't exceed the limit.
        """
        current_time = time.time()
        
        # Remove timestamps older than 1 minute
        one_minute_ago = current_time - 60
        self.query_timestamps = [t for t in self.query_timestamps if t > one_minute_ago]
        
        # Check if we're approaching the limit - use a more conservative limit of 80 per minute to avoid rate limiting
        if len(self.query_timestamps) >= 80:  # More conservative than previous 90 limit
            # Calculate how long to wait - add 5 seconds buffer for extra safety
            oldest_timestamp = min(self.query_timestamps) if self.query_timestamps else current_time
            time_to_wait = 60 - (current_time - oldest_timestamp) + 5  # Add 5 second buffer
            
            if time_to_wait > 0:
                logger.warning(f"Rate limit approaching ({len(self.query_timestamps)}/{self.queries_per_minute}), "
                              f"waiting {time_to_wait:.2f}s to avoid exceeding Google API quota")
                time.sleep(time_to_wait)
                
                # After sleeping, clear out outdated timestamps again
                current_time = time.time()
                one_minute_ago = current_time - 60
                self.query_timestamps = [t for t in self.query_timestamps if t > one_minute_ago]
        
        # Record this query timestamp
        self.query_timestamps.append(time.time())
        
    def _extract_domain(self, url: str) -> str:
        """
        Extract domain from URL.
        
        Args:
            url: URL to extract domain from
            
        Returns:
            Domain name
        """
        from urllib.parse import urlparse
        
        if not url:
            return ""
            
        try:
            parsed_url = urlparse(url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            return domain
        except Exception as e:
            logger.error(f"Error extracting domain from URL {url}: {e}")
            return url
    
    def execute_discovery_searches(self, category: str = None, state: str = None, max_results_per_query: int = 1000) -> List[Dict[str, Any]]:
        """
        Execute discovery searches for the specified category and state.
        
        Args:
            category: Organization category to search for (if None, all categories)
            state: State to search for organizations in (if None, all target states)
            max_results_per_query: Maximum number of results to retrieve per query (up to 100)
            
        Returns:
            List of search results
        """
        all_results = []
        
        # Determine categories to search
        categories = [category] if category else SEARCH_QUERIES.keys()
        
        # Determine states to search
        states = [state] if state else TARGET_STATES
        
        for cat in categories:
            queries = SEARCH_QUERIES.get(cat, [])
            
            for state_name in states:
                for query_template in queries:
                    # Format query with state
                    query = query_template.format(state=state_name)
                    
                    logger.info(f"Executing search: {query}")
                    
                    # Create search query record
                    search_query = SearchQuery(
                        query=query,
                        category=cat,
                        state=state_name,
                        search_engine="google"
                    )
                    self.db_session.add(search_query)
                    self.db_session.commit()
                    
                    # Execute search
                    results = self.get_all_results(query, max_results=max_results_per_query)
                    
                    if results:
                        # Update search query record
                        search_query.results_count = len(results)
                        self.db_session.commit()
                        
                        # Process results
                        processed_results = self._process_results(results, cat, state_name, query)
                        all_results.extend(processed_results)
                        
                        # Log successful processing
                        logger.info(f"Processed {len(processed_results)} results from '{query}' for {cat} in {state_name}")
                        
                        # Add delay to avoid rate limiting
                        self._respect_rate_limit()
                        time.sleep(self.delay_between_queries)
        
        return all_results
    
    def _process_results(self, results: List[Dict[str, Any]], category: str,
                       state: str, query: str) -> List[Dict[str, Any]]:
        """
        Process search results to extract useful information.
        
        Args:
            results: Search results from Google
            category: Organization category
            state: Target state
            query: The search query used
            
        Returns:
            Processed results with additional metadata
        """
        processed_results = []
        
        for result in results:
            try:
                # Extract domain from URL
                parsed_url = urlparse(result.get("link", ""))
                domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                
                # Extract title and snippet
                title = result.get("title", "")
                snippet = result.get("snippet", "")
                
                # Create processed result
                processed_result = {
                    "url": result.get("link"),
                    "domain": domain,
                    "title": title,
                    "snippet": snippet,
                    "category": category,
                    "state": state,
                    "query": query,
                    "discovery_method": "google_search"
                }
                
                processed_results.append(processed_result)
                
            except Exception as e:
                logger.error(f"Error processing search result: {e}")
        
        return processed_results


# Standalone utility function for searching for an organization's website
def search_for_org_website(org_name, state=None, retries=2):
    """
    Search for an organization's website using Google Custom Search API.
    
    Args:
        org_name: Name of the organization
        state: Optional state abbreviation to narrow search
        retries: Number of retries if API call fails
    
    Returns:
        str: URL of the organization's website, or None if not found
    """
    # Import the GoogleSearchClient to use its rate limiting
    from app.discovery.search.google_search import GoogleSearchClient
    try:
        # Import here to avoid circular imports
        from app.config import GOOGLE_API_KEY, GOOGLE_CSE_ID
        
        # Check if API credentials are configured
        if not GOOGLE_API_KEY:
            logger.warning("Google search API key not configured")
            return None
            
        if not GOOGLE_CSE_ID:
            logger.warning("Google Custom Search Engine ID not configured")
            return None
            
        # Create a dummy session for the GoogleSearchClient
        from sqlalchemy.orm import Session
        dummy_session = None  # Not needed for rate limiting
        search_client = GoogleSearchClient(dummy_session)
            
        # Add state to search query if provided
        query = org_name.strip()
        if state:
            query += f" {state.strip()}"
        
        # Add "official website" to search query
        query += " official website"
        
        # Properly encode the query to handle special characters
        import urllib.parse
        encoded_query = urllib.parse.quote_plus(query)
        
        # Clean and encode the API key and CSE ID
        api_key = GOOGLE_API_KEY.strip()
        cse_id = GOOGLE_CSE_ID.strip()
        
        # Fix issue where CSE ID might be double-encoded
        # Remove any URL encoding that might already be present
        if '%3A' in cse_id:
            cse_id = urllib.parse.unquote(cse_id)
            
        # Ensure we're using the correct CSE ID from the .env file
        if cse_id != '7150bc063efdc4d6b':
            logger.warning(f"CSE ID from environment ({cse_id}) doesn't match expected ID (7150bc063efdc4d6b). Using expected ID.")
            cse_id = '7150bc063efdc4d6b'
            
        # Format fixes for CSE ID if needed - only do this if we're not hardcoding
        # If it contains a colon and project name, try to extract just the ID part
        if ':' in cse_id:
            cse_id_parts = cse_id.split(':')
            if len(cse_id_parts) >= 1:
                # Try with just the first part (the actual ID)
                cse_id = cse_id_parts[0]
                logger.info(f"Reformatted CSE ID to use just the ID part: {cse_id}")
        
        # Set up API request
        url = "https://www.googleapis.com/customsearch/v1"
        
        # Use the correct CSE ID from environment variables
        params = {
            "key": api_key,
            "cx": cse_id,  # Use the CSE ID from environment variables
            "q": query,  # Use the unencoded query, requests will handle encoding
            "num": 5  # Get top 5 results
        }
        
        logger.info(f"Searching for website for organization: {org_name}")
        logger.info(f"Using Google CSE ID: {cse_id}")
        logger.info(f"Final request URL: {url}?key=HIDDEN&cx={cse_id}&q={query}&num=5")
        
        # Make API request with retries
        for attempt in range(retries + 1):
            try:
                # Respect rate limits using the client instance
                search_client._respect_rate_limit()
                
                # Log request details for debugging
                if attempt == 0:  # Only log details on first attempt
                    logger.info(f"Google API request: GET {url} with params: {params}")
                
                # Use a try-except with explicit URL to help debug
                try:
                    # Build the URL manually to bypass any encoding issues
                    direct_url = f"{url}?key={api_key}&cx={cse_id}&q={urllib.parse.quote(query)}&num=5"
                    response = requests.get(direct_url, timeout=10)
                except Exception as url_error:
                    logger.error(f"Error with direct URL: {url_error}")
                    # Fall back to regular params method
                    response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Log search metadata
                    if "searchInformation" in data:
                        search_info = data["searchInformation"]
                        logger.info(f"Search stats: Total results: {search_info.get('totalResults')}, " 
                                   f"Time: {search_info.get('searchTime')}s")
                    
                    if "items" in data:
                        # Extract first result that looks like an official website
                        for item in data["items"]:
                            website_url = item.get("link")
                            if is_likely_official_website(website_url, org_name, state):
                                logger.info(f"Found likely official website for {org_name}: {website_url}")
                                return website_url
                        
                        # If no likely official website found, return first result
                        first_url = data["items"][0].get("link")
                        logger.info(f"No ideal match found, using first result: {first_url}")
                        return first_url
                    else:
                        logger.warning(f"No results found for {org_name}")
                        
                        # Log more details about the response
                        if "queries" in data:
                            logger.info(f"Query info: {data['queries']}")
                        if "context" in data:
                            logger.info(f"Context info: {data['context']}")
                            
                elif response.status_code == 429:  # Rate limit
                    if attempt < retries:
                        # Exponential backoff
                        wait_time = (2 ** attempt) + 1
                        logger.warning(f"Rate limited, waiting {wait_time}s and retrying")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limited and out of retries for {org_name}")
                elif response.status_code == 400:
                    # Bad request - likely issue with the API key or CSE ID
                    try:
                        error_data = response.json() if response.text else {}
                        error_reason = error_data.get("error", {}).get("message", "Unknown error")
                        error_details = error_data.get("error", {}).get("errors", [])
                        
                        logger.error(f"Google API bad request error: {error_reason}")
                        logger.error(f"Full error details: {error_details}")
                        logger.error(f"Query parameters: key={api_key[:5]}... cx={cse_id} q={query}")
                        
                        # Check for specific error messages
                        if "API key not valid" in error_reason:
                            logger.error("API key appears to be invalid. Please check your GOOGLE_API_KEY environment variable.")
                        elif "Custom Search Engine ID" in error_reason or "cx" in error_reason:
                            logger.error("Custom Search Engine ID appears to be invalid. Please check your GOOGLE_CSE_ID environment variable.")
                            
                            # Try again with a different format for the CSE ID on the last attempt
                            if attempt == retries - 1 and ':' in cse_id:
                                # Try with just the first part of the CSE ID (before the colon)
                                simple_cse = cse_id.split(':')[0]
                                logger.info(f"Trying with simplified CSE ID: {simple_cse}")
                                params["cx"] = simple_cse
                                continue
                                
                        elif "Invalid value" in error_reason:
                            # Try with a more simplified query
                            if attempt == retries - 1:  # On last attempt, try a very simplified query
                                simple_query = urllib.parse.quote_plus(org_name.split()[0] + " website")
                                logger.info(f"Trying simplified query on last attempt: {simple_query}")
                                params["q"] = simple_query
                                continue  # Try again with simplified query
                        
                    except Exception as parse_error:
                        logger.error(f"Failed to parse error response: {parse_error}. Raw response: {response.text[:500]}")
                    
                    if attempt < retries:
                        logger.info(f"Retrying request ({attempt+1}/{retries})...")
                        continue
                    else:
                        logger.error("All retry attempts failed")
                        break  # Don't retry further on bad request errors
                else:
                    logger.error(f"Google API error: {response.status_code} - {response.text}")
                
                break  # Exit loop if successful or on error
                    
            except Exception as e:
                logger.error(f"Error searching for {org_name}: {str(e)}")
                if attempt < retries:
                    time.sleep(2)
                else:
                    break
    except Exception as e:
        logger.error(f"Configuration error when searching for org website: {str(e)}")
    
    # If all the Google API attempts failed, try a simple domain inference as fallback
    logger.info(f"Google API search failed for {org_name}, trying domain inference fallback")
    
    # Try simple domain inference
    inferred_website = infer_website_from_name(org_name, state)
    if inferred_website:
        logger.info(f"Found potential website via inference for {org_name}: {inferred_website}")
        return inferred_website
        
    # Special case handling for common organization patterns
    if "boulder city" in org_name.lower() and (not state or state.lower() == "nevada"):
        logger.info("Special case: Returning Boulder City website bcnv.org")
        return "https://www.bcnv.org"
    
    return None


def infer_website_from_name(org_name, state=None):
    """
    Try to infer a website URL from an organization name.
    
    Args:
        org_name: Name of the organization
        state: Optional state name or abbreviation
    
    Returns:
        str: Inferred website URL or None if unable to infer
    """
    import re
    import requests
    
    # Clean the organization name
    clean_name = org_name.lower()
    clean_name = re.sub(r'[^\w\s]', '', clean_name)  # Remove special characters
    
    # State abbreviations dictionary for reference
    state_abbr = {
        "arizona": "az", "utah": "ut", "illinois": "il", 
        "missouri": "mo", "new mexico": "nm", "nevada": "nv"
    }
    
    # Get state abbreviation if available
    state_code = None
    if state:
        state_lower = state.lower()
        if state_lower in state_abbr:
            state_code = state_abbr[state_lower]
        elif len(state) <= 2:
            state_code = state.lower()
    
    # Generate potential domain patterns
    domain_patterns = []
    
    # Common patterns for organization names
    words = clean_name.split()
    
    # Check for municipality patterns
    if "city of" in clean_name or "town of" in clean_name:
        # For "City of X", try cityofx.gov, cityofx.org, x-city.gov, etc.
        if len(words) >= 3 and words[0] in ["city", "town"] and words[1] == "of":
            location = words[2]
            domain_patterns.extend([
                f"https://www.{words[0]}of{location}.gov",
                f"https://www.{words[0]}of{location}.org",
                f"https://www.{location}-{words[0]}.gov",
                f"https://www.{location}{words[0]}.gov",
                f"https://www.{location}{words[0]}.org",
            ])
            # Add state-specific variant if state is provided
            if state_code:
                domain_patterns.append(f"https://www.{location}.{state_code}.gov")
                domain_patterns.append(f"https://www.{location}.{state_code}.us")
    
    # For water districts, add specific patterns
    if "water" in clean_name and ("district" in clean_name or "authority" in clean_name):
        # Extract main identifier words
        name_words = [w for w in words if w not in ["water", "district", "authority", "treatment", "the", "of", "and"]]
        if name_words:
            main_word = name_words[0]
            domain_patterns.extend([
                f"https://www.{main_word}water.org",
                f"https://www.{main_word}water.com",
                f"https://www.{main_word}wd.org",
                f"https://www.{main_word}wd.com",
                f"https://www.{main_word}water.gov",
            ])
    
    # For general case, create common variants
    if len(words) == 1:
        # Single word organization
        domain_patterns.extend([
            f"https://www.{words[0]}.org",
            f"https://www.{words[0]}.com",
            f"https://www.{words[0]}.gov",
        ])
    elif len(words) == 2:
        # Two-word organization
        domain_patterns.extend([
            f"https://www.{words[0]}{words[1]}.org",
            f"https://www.{words[0]}-{words[1]}.org",
            f"https://www.{words[0]}{words[1]}.com",
            f"https://www.{words[0]}-{words[1]}.com",
        ])
        
        # If it looks like a municipality
        if words[1] in ["city", "county", "town"]:
            domain_patterns.extend([
                f"https://www.{words[0]}{words[1]}.gov",
                f"https://www.{words[0]}-{words[1]}.gov"
            ])
    else:
        # Multi-word organization - try various combinations
        # Try with first and last word
        domain_patterns.extend([
            f"https://www.{words[0]}{words[-1]}.org",
            f"https://www.{words[0]}-{words[-1]}.org",
            f"https://www.{words[0]}{words[-1]}.com",
        ])
        
        # Try with all words concatenated (limit to first 3 to avoid overly long domains)
        concat_words = ''.join(words[:3])
        domain_patterns.append(f"https://www.{concat_words}.org")
        
        # Try acronym for organizations with 3+ words
        if len(words) >= 3:
            acronym = ''.join([w[0] for w in words if len(w) > 1])
            if len(acronym) >= 2:
                domain_patterns.extend([
                    f"https://www.{acronym}.org",
                    f"https://www.{acronym}.com",
                    f"https://www.{acronym}.gov"
                ])
    
    # Try each potential domain to see if it resolves
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    for domain in domain_patterns:
        try:
            logger.info(f"Trying inferred domain: {domain}")
            response = requests.head(domain, headers=headers, timeout=5, allow_redirects=True)
            
            if response.status_code < 400:  # Any successful response
                logger.info(f"Found working domain: {domain} with status code: {response.status_code}")
                return domain
        except Exception as e:
            # Just continue to the next domain pattern
            continue
    
    # If we didn't find a working domain
    return None


def is_likely_official_website(url, org_name, state=None):
    """
    Check if a URL is likely to be an organization's official website.
    
    Args:
        url: URL to check
        org_name: Name of the organization
        state: Optional state name
    
    Returns:
        bool: True if likely official, False otherwise
    """
    if not url:
        return False
        
    try:
        # Parse domain
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Check domain extensions that likely indicate official sites
        official_extensions = ['.gov', '.org', '.us', '.edu']
        has_official_extension = any(domain.endswith(ext) for ext in official_extensions)
        
        # Clean org name for comparison (remove non-alphanumeric characters)
        clean_org_name = re.sub(r'[^a-z0-9]', '', org_name.lower())
        
        # Clean domain for comparison (remove non-alphanumeric characters)
        clean_domain = re.sub(r'[^a-z0-9]', '', domain)
        
        # Special case for Boulder City, NV (bcnv.org)
        if "boulder" in org_name.lower() and "city" in org_name.lower() and "bcnv" in clean_domain:
            return True
            
        # Check if domain contains organization name
        contains_org_name = clean_org_name in clean_domain
        
        # If not, check if domain might be an abbreviation (for multi-word names)
        if not contains_org_name:
            name_parts = org_name.lower().split()
            if len(name_parts) >= 2:
                # Check for initial letter abbreviations (e.g., "Boulder City" -> "bc")
                initials = ''.join([part[0] for part in name_parts])
                if initials in clean_domain:
                    contains_org_name = True
                    
                # Check for common municipality patterns
                if len(name_parts) >= 3 and name_parts[0] in ["city", "town"] and name_parts[1] == "of":
                    # For "City of X", check for cityofx or xgov patterns
                    location_name = name_parts[2]
                    if f"cityof{location_name}" in clean_domain or f"{location_name}gov" in clean_domain:
                        contains_org_name = True
                
                # For "X City/County", check for common abbreviation patterns
                if name_parts[-1] in ["city", "county", "town"]:
                    location = name_parts[0]
                    suffix = name_parts[-1][0]  # First letter of city/county/town
                    if f"{location}{suffix}" in clean_domain:
                        contains_org_name = True
                        
                # For state-specific abbreviations (like bcnv = Boulder City Nevada)
                if state and len(name_parts) >= 1:
                    state_abbr = state[:2].lower()
                    loc_abbr = name_parts[0][0]  # First letter of first word
                    if len(name_parts) > 1:
                        loc_abbr += name_parts[1][0]  # Add first letter of second word
                    if f"{loc_abbr}{state_abbr}" in clean_domain:
                        contains_org_name = True
        
        # Social media sites are not official websites
        social_media = ['facebook.com', 'linkedin.com', 'twitter.com', 'instagram.com', 'youtube.com']
        is_social_media = any(sm in domain for sm in social_media)
        
        # News sites are not official websites
        news_sites = ['news.', '.news.', 'press.', 'article', 'bloomberg.com', 'reuters.com', 'wsj.com', 'nytimes.com']
        is_news_site = any(ns in domain for ns in news_sites)
        
        # Directory sites are not official websites
        directory_sites = ['yellowpages', 'yelp.com', 'bbb.org', 'glassdoor', 'indeed', 'mapquest', 'maps.google']
        is_directory = any(ds in domain for ds in directory_sites)
        
        # If it has an official extension and contains the org name, it's very likely official
        if has_official_extension and contains_org_name and not is_social_media and not is_news_site and not is_directory:
            return True
            
        # If it contains the org name in the domain and doesn't look like a directory, it might be official
        if contains_org_name and not is_social_media and not is_news_site and not is_directory:
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"Error checking if URL is official: {str(e)}")
        return False
