"""
Gemini-based organization classifier for the GBL Data Contact Management System.

This module provides functionality to classify search results as relevant organizations using Google's Gemini API.
"""
import time
import json
import re
from typing import Dict, List, Any, Optional, Tuple
from app.config import GEMINI_API_KEY, ORG_TYPES
from app.utils.logger import get_logger
import google.generativeai as genai

logger = get_logger(__name__)

class GeminiOrganizationClassifier:
    """Uses Google's Gemini API to determine if a search result is a relevant organization."""
    
    def __init__(self):
        """Initialize the Gemini classifier."""
        self.api_key = GEMINI_API_KEY
        
        try:
            if self.api_key:
                genai.configure(api_key=self.api_key)
                logger.info("Gemini API initialized successfully")
            else:
                logger.warning("No Gemini API key provided, classification will not work")
        except Exception as e:
            logger.error(f"Error initializing Gemini API: {e}")
    
    def classify_search_result(self, search_result: Dict[str, Any], org_type: str, state: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Classify if a search result is a relevant organization of the specified type.
        
        Args:
            search_result: Search result dictionary with title, snippet, and URL
            org_type: Organization type to check for (e.g., "engineering", "water", etc.)
            state: State to check for
            
        Returns:
            Tuple containing:
                - Boolean indicating if the result is a relevant organization
                - Dictionary with additional metadata if relevant (name, confidence score, etc.)
        """
        if not self.api_key:
            logger.warning("Cannot classify without Gemini API key")
            return False, {}
            
        try:
            # Extract type description from config
            type_description = ORG_TYPES.get(org_type, {}).get("description", org_type.title())
            
            # Extract information from search result
            title = search_result.get("title", "")
            snippet = search_result.get("snippet", "")
            url = search_result.get("link", "")
            
            # Create a prompt for Gemini to classify the result
            prompt = f"""
            I have a search result that might be a {type_description} in {state}. I need to determine if this is actually a relevant organization.

            Title: {title}
            Snippet: {snippet}
            URL: {url}

            I want you to analyze this information and answer with YES or NO:
            1. Is this a {type_description}? (Not a job board, news article, social media page, or directory)
            2. Is it an actual organization (not a list, guide, or general information page)?

            If you answered YES to both questions, also extract the following information:
            - Organization Name (the official name, not just what's in the title)
            - Confidence Score (0.0-1.0) that this is a relevant {org_type} organization
            - Relevance (how well this organization fits the {org_type} category, 0.0-1.0)
            - Any notes or observations about this organization
            
            Format your answer as a JSON object with these fields exactly:
            {{
                "is_relevant": true or false,
                "organization_name": "extracted name here",
                "confidence_score": 0.0-1.0,
                "relevance_score": 0.0-1.0,
                "notes": "any observations"
            }}
            
            Only respond with this JSON object, nothing else.
            """
            
            # Call Gemini API
            model = genai.GenerativeModel('gemini-2.0-flash')
            response = model.generate_content(prompt)
            response_text = response.text
            
            # Extract JSON from response
            matches = re.search(r'({[\s\S]*})', response_text)
            if not matches:
                logger.warning(f"Couldn't extract JSON from Gemini response: {response_text[:100]}...")
                return False, {}
                
            result_json = json.loads(matches.group(1))
            
            is_relevant = result_json.get("is_relevant", False)
            
            logger.info(f"Gemini classification for '{title[:30]}...': is_relevant={is_relevant}, " 
                       f"confidence={result_json.get('confidence_score', 0)}")
            
            return is_relevant, result_json
            
        except Exception as e:
            logger.error(f"Error classifying with Gemini: {e}")
            return False, {}
    
    def batch_classify(self, search_results: List[Dict[str, Any]], org_type: str, state: str) -> List[Dict[str, Any]]:
        """
        Classify a batch of search results.
        
        Args:
            search_results: List of search result dictionaries
            org_type: Organization type to check for
            state: State to check for
            
        Returns:
            List of relevant organization dictionaries with metadata
        """
        relevant_orgs = []
        
        for i, result in enumerate(search_results):
            try:
                is_relevant, metadata = self.classify_search_result(result, org_type, state)
                
                if is_relevant:
                    # Create organization data dictionary
                    org_data = {
                        "name": metadata.get("organization_name", result.get("title", "")),
                        "org_type": org_type,
                        "state": state,  # Using the state from the search query directly
                        "website": result.get("link", ""),
                        "confidence_score": metadata.get("confidence_score", 0.7),
                        "relevance_score": metadata.get("relevance_score", 0.7),
                        "source_url": result.get("link", ""),
                        "discovery_method": "gemini_classified_search"
                    }
                    
                    # Add description field instead of notes
                    if metadata.get("notes"):
                        org_data["description"] = metadata.get("notes", "")
                    
                    relevant_orgs.append(org_data)
                
                # Add small delay between API calls to avoid rate limiting
                if i < len(search_results) - 1:
                    time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error processing search result {i}: {e}")
        
        logger.info(f"Classified {len(search_results)} results, found {len(relevant_orgs)} relevant organizations")
        return relevant_orgs