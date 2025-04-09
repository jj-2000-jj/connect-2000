"""
Website validation using Gemini API to determine if a URL belongs to an organization.
"""
import logging
import time
from typing import Tuple, Optional
import json
import re
from urllib.parse import urlparse, urljoin

import google.generativeai as genai

from app.config import GEMINI_API_KEY
from app.utils.logger import get_logger

logger = get_logger(__name__)

class WebsiteValidator:
    """
    Validates if a website is likely to be the official website of an organization
    using the Gemini API for advanced analysis.
    """
    
    def __init__(self):
        """Initialize the validator with Gemini API configuration."""
        self.validation_threshold = 0.6  # Accept websites with 60%+ confidence
        
        # Setup Gemini API client if API key exists
        if GEMINI_API_KEY:
            try:
                genai.configure(api_key=GEMINI_API_KEY)
                self.model = genai.GenerativeModel('gemini-2.0-flash')
                self.api_available = True
            except Exception as e:
                logger.error(f"Error initializing Gemini API: {e}")
                self.api_available = False
        else:
            logger.warning("GEMINI_API_KEY not set, WebsiteValidator will use fallback methods only")
            self.api_available = False
            
        # Cache for validation results to avoid repeated API calls
        self.validation_cache = {}
    
    def validate_org_website(self, url: str, org_name: str, 
                             org_state: Optional[str] = None) -> Tuple[bool, float]:
        """
        Validate if a URL is likely the official website of an organization.
        
        Args:
            url: The website URL to validate
            org_name: Name of the organization
            org_state: Optional state where organization is located
            
        Returns:
            Tuple of (is_valid, confidence_score)
        """
        if not url:
            return False, 0.0
        
        # Normalize URL
        url = url.lower().strip()
        
        # Check cache first
        cache_key = f"{url}_{org_name}_{org_state}"
        if cache_key in self.validation_cache:
            return self.validation_cache[cache_key]
        
        # Try Gemini API validation if available
        if self.api_available:
            try:
                is_valid, confidence = self._validate_with_gemini(url, org_name, org_state)
                
                # Cache the result
                self.validation_cache[cache_key] = (is_valid, confidence)
                return is_valid, confidence
                
            except Exception as e:
                logger.error(f"Error validating website with Gemini: {e}")
                # Fall back to heuristic approach
        
        # Fallback to basic heuristic method
        is_valid, confidence = self._validate_with_heuristics(url, org_name, org_state)
        
        # Cache the result
        self.validation_cache[cache_key] = (is_valid, confidence)
        return is_valid, confidence
    
    def _validate_with_gemini(self, url: str, org_name: str, 
                             org_state: Optional[str] = None) -> Tuple[bool, float]:
        """
        Use Gemini API to validate a website.
        
        Args:
            url: The website URL to validate
            org_name: Name of the organization
            org_state: Optional state where organization is located
            
        Returns:
            Tuple of (is_valid, confidence_score)
        """
        try:
            # Handle rate limiting with retries and exponential backoff
            max_retries = 3
            for retry in range(max_retries):
                try:
                    # Craft the prompt for Gemini
                    location_context = f" located in {org_state}" if org_state else ""
                    prompt = f"""
                    Task: Evaluate if this URL is likely to be the official website of the specified organization.
                    
                    Organization: {org_name}{location_context}
                    URL: {url}
                    
                    Consider:
                    1. Common naming patterns for official websites, including:
                       - Full organization name (e.g., bouldercity.org)
                       - Abbreviations (e.g., bcnv.org for Boulder City, Nevada)
                       - Domain extensions (.gov, .org, .us for government/municipal)
                       - Local government URL formats (cityofX.gov, X-city.gov, etc.)
                    
                    2. Municipal websites often use:
                       - City/town/county initials
                       - State abbreviations in the domain
                       - Official extensions like .gov, .org, or state-specific like .nv.us
                    
                    Return a JSON object with:
                    1. "is_official": true/false
                    2. "confidence": a number between 0.0 and 1.0
                    3. "explanation": brief reasoning for your decision
                    
                    Response format: 
                    ```json
                    {"is_official": true/false, "confidence": 0.X, "explanation": "reason"}
                    ```
                    """
                    
                    # Call Gemini API with low temperature for consistent results
                    response = self.model.generate_content(prompt, temperature=0.1)
                    
                    # Extract JSON from response
                    response_text = response.text
                    json_match = re.search(r'```json\s*(\{.*?\})\s*```', 
                                          response_text, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(1)
                    else:
                        # Try to find anything that looks like JSON
                        json_match = re.search(r'\{.*"is_official".*?\}', 
                                              response_text, re.DOTALL)
                        if json_match:
                            json_str = json_match.group(0)
                        else:
                            json_str = response_text
                    
                    # Remove any markdown backticks that might still be present
                    json_str = json_str.replace('```', '').strip()
                    
                    # Parse the response and extract results
                    result = json.loads(json_str)
                    is_official = result.get("is_official", False)
                    confidence = result.get("confidence", 0.0)
                    explanation = result.get("explanation", "No explanation provided")
                    
                    logger.info(f"Gemini validation for {url} as {org_name}'s website: " 
                                f"{'✓' if is_official else '✗'} ({confidence:.2f})")
                    logger.info(f"Explanation: {explanation}")
                    
                    # Return the validation result
                    return is_official, confidence
                    
                except Exception as e:
                    if "rate limit" in str(e).lower() and retry < max_retries - 1:
                        # Exponential backoff
                        wait_time = (2 ** retry) + 1
                        logger.warning(f"Rate limited by Gemini API, waiting {wait_time}s and retrying")
                        time.sleep(wait_time)
                    else:
                        raise
            
            # If we get here, we've exceeded retries
            raise Exception("Exceeded maximum retries for Gemini API")
                    
        except Exception as e:
            logger.error(f"Error in Gemini validation: {e}")
            return False, 0.0
    
    def _validate_with_heuristics(self, url: str, org_name: str, 
                                org_state: Optional[str] = None) -> Tuple[bool, float]:
        """
        Fallback method using heuristics to validate a website.
        
        Args:
            url: The website URL to validate
            org_name: Name of the organization
            org_state: Optional state where organization is located
            
        Returns:
            Tuple of (is_valid, confidence_score)
        """
        # This is a more sophisticated version of the original heuristic approach
        try:
            # Parse domain
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Base confidence score
            confidence = 0.0
            
            # Check domain extensions that likely indicate official sites
            official_extensions = ['.gov', '.org', '.us', '.edu']
            has_official_extension = any(domain.endswith(ext) for ext in official_extensions)
            if has_official_extension:
                confidence += 0.3  # Significant boost for official extensions
            
            # Clean org name for multiple comparison approaches
            org_name_lower = org_name.lower()
            clean_org_name = re.sub(r'[^a-z0-9]', '', org_name_lower)
            
            # Handle common words to remove from org names for matching
            common_words = ['city', 'town', 'of', 'county', 'village', 'township', 'department']
            org_name_parts = org_name_lower.split()
            core_org_name = ' '.join([p for p in org_name_parts if p not in common_words])
            
            # Generate possible abbreviations
            abbreviations = []
            # Initial letters (e.g., "Boulder City" -> "bc")
            initial_abbr = ''.join(word[0].lower() for word in org_name_parts if word not in common_words)
            abbreviations.append(initial_abbr)
            
            # Initial letters + state (e.g., "Boulder City" + "NV" -> "bcnv")
            if org_state:
                state_abbr = org_state[:2].lower()
                abbreviations.append(f"{initial_abbr}{state_abbr}")
            
            # Check for direct match between domain and org name
            domain_without_ext = domain.split('.')[0]
            
            # Direct match of full name in domain
            clean_domain = re.sub(r'[^a-z0-9]', '', domain)
            if clean_org_name in clean_domain:
                confidence += 0.4
            # Match of org parts with hyphens or other separators
            elif any(part in domain_without_ext for part in org_name_parts if len(part) > 3):
                confidence += 0.2
            
            # Check for abbreviation matches
            if any(abbr in domain_without_ext for abbr in abbreviations):
                confidence += 0.3
            
            # Check for patterns like "cityofX" or "Xcity"
            if "cityof" in domain_without_ext or "townof" in domain_without_ext:
                confidence += 0.2
            if domain_without_ext.endswith("city") or domain_without_ext.endswith("county"):
                confidence += 0.2
            
            # Penalize for known non-official domains
            social_media = ['facebook.com', 'linkedin.com', 'twitter.com', 'instagram.com', 'youtube.com']
            news_sites = ['news.', '.news.', 'press.', 'bloomberg.com', 'reuters.com', 'wsj.com']
            directory_sites = ['yellowpages', 'yelp.com', 'bbb.org', 'glassdoor', 'indeed']
            
            if any(sm in domain for sm in social_media):
                confidence -= 0.5
            if any(ns in domain for ns in news_sites):
                confidence -= 0.4
            if any(ds in domain for ds in directory_sites):
                confidence -= 0.4
            
            # Cap confidence at 0.9 for heuristic approach
            confidence = min(0.9, max(0.0, confidence))
            
            # Return result
            return confidence >= self.validation_threshold, confidence
            
        except Exception as e:
            logger.error(f"Error in heuristic validation: {e}")
            return False, 0.0
