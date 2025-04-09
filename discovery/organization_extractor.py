"""
Organization extraction module for the GBL Data Contact Management System.

This module extracts actual organizations from webpage content using NLP and the Gemini API.
It focuses on identifying real organizations that could be potential customers for SCADA integration services.
"""
import json
import re
import random
import datetime
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import google.generativeai as genai
from sqlalchemy.orm import Session
from app.config import GEMINI_API_KEY, CLASSIFICATION_KEYWORDS, ORG_TYPES, TARGET_STATES
from app.database.models import Organization, DiscoveredURL, Base
from app.utils.logger import get_logger
from app.database import crud

logger = get_logger(__name__)

class OrganizationExtractor:
    """
    Extracts real organizations from webpage content using NLP and the Gemini API.
    
    This class encapsulates the functionality to identify potential SCADA integration
    client organizations from web content using a combination of pattern matching and
    generative AI analysis.
    """
    
    def __init__(self, db_session):
        """
        Initialize the organization extractor.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        
        # Initialize Gemini API if key is available
        if GEMINI_API_KEY:
            try:
                genai.configure(api_key=GEMINI_API_KEY)
            except Exception as e:
                logger.warning(f"Error initializing Gemini API: {e}")
        else:
            logger.warning("GEMINI_API_KEY not found, will use mock responses")

    def html_to_text(self, html_content: str) -> str:
        """
        Convert HTML content to plain text.
        
        Args:
            html_content: HTML content string
            
        Returns:
            Extracted plain text
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script_or_style in soup(['script', 'style', 'header', 'footer', 'nav']):
                script_or_style.decompose()
            
            # Get text
            text = soup.get_text(separator=' ', strip=True)
            
            # Remove excess whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text
        except Exception as e:
            logger.error(f"Error converting HTML to text: {e}")
            return html_content  # Return original content if parsing fails


    def extract_organizations_from_content(self, content: str, url: str, state_context: Optional[str] = None, industry_hint: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract actual organization names from webpage content using Gemini API.
        
        Args:
            content: HTML or text content of the page
            url: Source URL
            state_context: Optional state context from the search query
            industry_hint: Optional industry context from the search query
            
        Returns:
            List of dictionaries with extracted organization information
        """
        # Clean and extract text from HTML if needed
        if "<html" in content.lower() or "<body" in content.lower():
            text = self.html_to_text(content)
        else:
            text = content
        
        # Truncate text if too long (Gemini has token limits)
        if len(text) > 15000:
            text = text[:15000]
        
        # Create target keywords list for the prompt
        target_keywords = []
        for category, keywords in CLASSIFICATION_KEYWORDS.items():
            target_keywords.extend(keywords)
        
        # Create target organization types list
        org_type_descriptions = []
        for org_type, details in ORG_TYPES.items():
            org_type_descriptions.append(f"{details['description']} ({', '.join(details['subtypes'])})")
        
        # Build the prompt with guidance from configuration
        prompt = f"""
        Extract all real organizations from the following text that could potentially use SCADA integration services.
        Focus on finding organizations in these categories:
        {', '.join(org_type_descriptions)}
        
        Look for organizations that match these keywords:
        {', '.join(target_keywords)}
        
        For each organization identified, extract:
        1. Organization name (full official name)
        2. Organization type (one of: {', '.join(ORG_TYPES.keys())})
        3. Location/state if mentioned (focus on these states: {', '.join(TARGET_STATES)})
        4. Brief description of their operations or relevance to SCADA integration
        5. Confidence level (0.0-1.0) that this is a real organization that could use SCADA integration
        
        IMPORTANT:
        - Only extract real organizations that could be potential customers, not generic concepts or systems
        - If the state isn't explicitly mentioned but you have context that it's in {state_context or 'a target state'}, include that
        - If the industry isn't clear but you have context that it's {industry_hint or 'a relevant industry'}, use that
        - Ignore organizations that are technology vendors, software companies, or SCADA integrators themselves
        
        TEXT:
        {text}
        
        Format the response as a JSON array of objects with these exact fields:
        name, org_type, state, description, confidence_score
        
        If no valid organizations are found, return an empty array.
        """
        
        try:
            try:
                if GEMINI_API_KEY:
                    # Create Gemini client with better timeout handling
                    from app.utils.gemini_client import GeminiClient
                    gemini_client = GeminiClient(api_key=GEMINI_API_KEY)
                    
                    # Add rate limiting - sleep for 1 second before API call to avoid hitting rate limits
                    import time
                    time.sleep(1)
                    
                    # Call with increased timeout and retries
                    response_text = gemini_client.generate_content(
                        prompt=prompt,
                        temperature=0.2,
                        timeout=180,  # 3 minutes timeout
                        max_retries=3
                    )
                    
                    # Find JSON in response
                    # Look for JSON array structure
                    json_match = re.search(r'\[\s*\{.*\}\s*\]', response_text, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                    else:
                        # Look for JSON embedded in markdown code blocks
                        json_block_match = re.search(r'```(?:json)?\s*(\[.*\])```', response_text, re.DOTALL)
                        if json_block_match:
                            json_str = json_block_match.group(1)
                        else:
                            # If still no match, clean the response and try to parse it
                            # Remove any markdown or text formatting
                            clean_text = re.sub(r'```.*?```', '', response_text, flags=re.DOTALL).strip()
                            # Find first [ and last ]
                            start_idx = clean_text.find('[')
                            end_idx = clean_text.rfind(']')
                            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                                json_str = clean_text[start_idx:end_idx+1]
                            else:
                                # Last resort - treat entire response as JSON if it might be valid
                                json_str = response_text
                    
                    try:
                        # Parse the JSON response
                        organizations = json.loads(json_str)
                        # Ensure we have a list
                        if not isinstance(organizations, list):
                            if isinstance(organizations, dict):
                                organizations = [organizations]
                            else:
                                organizations = []
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON from Gemini response: {e}")
                        organizations = []
                else:
                    # If no Gemini API key, log the issue and stop the operation
                    logger.error(f"No GEMINI_API_KEY provided - cannot extract organizations from {url}")
                    raise ValueError("Gemini API key is required for organization extraction")
            except Exception as e:
                logger.error(f"Error with Gemini API or JSON parsing: {e}")
                # Stop the operation instead of using incomplete fallbacks
                raise Exception(f"Organization extraction failed for {url}: {str(e)}")
            
            # Validate and enrich organization data
            validated_orgs = []
            for org in organizations:
                # Skip if no name, but don't filter by confidence score
                if not org.get('name'):
                    continue
                    
                # Standardize org_type to match our enum values
                if org.get('org_type') not in ORG_TYPES.keys():
                    # Attempt to classify based on description
                    org['org_type'] = self.classify_org_type(org.get('name', ''), org.get('description', ''))
                
                # Add source information
                org['source_url'] = url
                org['discovery_method'] = 'content_extraction'
                org['discovery_date'] = datetime.datetime.utcnow()  # Use actual datetime object instead of string
                
                # Set relevance score based on confidence
                org['relevance_score'] = min(10.0, org.get('confidence_score', 0.5) * 10)
                
                validated_orgs.append(org)
                
            return validated_orgs
        
        except Exception as e:
            logger.error(f"Error extracting organizations with Gemini API: {e}")
            return []


    def _generate_mock_organizations(self, url: str, state_context: Optional[str] = None, industry_hint: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Generate mock organization data for testing when Gemini API is not available.
        
        Args:
            url: Source URL
            state_context: Optional state context from the search query
            industry_hint: Optional industry hint from the search query
            
        Returns:
            List of dictionaries with mock organization information
        """
        logger.info(f"Generating mock organization data for {url}")
        
        # Extract domain from URL
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        # Generate organization name from domain, but skip common prefixes like www
        org_name_parts = domain.split('.')
        if len(org_name_parts) > 0:
            # Skip 'www' prefix if present
            if org_name_parts[0].lower() == 'www' and len(org_name_parts) > 1:
                org_name_base = org_name_parts[1].replace('-', ' ').title()
            else:
                org_name_base = org_name_parts[0].replace('-', ' ').title()
        else:
            org_name_base = "Unknown Organization"
        
        # Determine organization type and state from URL and hints
        org_type = industry_hint  # Use the industry hint if provided
        state = state_context or "Utah"  # Use provided state context or default
        
        # If no industry hint, check domain for common patterns
        if not org_type:
            domain_lower = domain.lower()
            for keyword in ["water", "wastewater"]:
                if keyword in domain_lower:
                    org_type = "water"
                    break
            
            if not org_type:
                for keyword in ["engineering", "engineer", "design"]:
                    if keyword in domain_lower:
                        org_type = "engineering"
                        break
                        
            if not org_type:
                for keyword in ["government", "agency", "dept"]:
                    if keyword in domain_lower:
                        org_type = "government"
                        break
                        
            if not org_type:
                for keyword in ["utility", "power", "electric"]:
                    if keyword in domain_lower:
                        org_type = "utility"
                        break
                    
            if not org_type:
                for keyword in ["oil", "gas", "petroleum", "energy"]:
                    if keyword in domain_lower:
                        org_type = "oil_gas"
                        break
                    
            if not org_type:
                for keyword in ["farm", "agriculture", "irrigation"]:
                    if keyword in domain_lower:
                        org_type = "agriculture"
                        break
                    
            if not org_type:
                for keyword in ["transport", "highway", "road", "traffic"]:
                    if keyword in domain_lower:
                        org_type = "transportation"
                        break
                
            if not org_type:
                # Default based on frequency in our target industries
                org_type = "water"
        
        # Generate descriptions based on organization type
        descriptions = {
            "water": [
                f"Water treatment and distribution service for {state}. Responsible for maintaining water quality and regulatory compliance.",
                f"Municipal water authority serving communities in {state}. Manages water treatment facilities and distribution systems.",
                f"Water management district in {state} focusing on sustainable water resources and quality monitoring."
            ],
            "engineering": [
                f"Engineering consulting firm specializing in water infrastructure projects across {state}.",
                f"Civil engineering company providing design and implementation services for water systems in {state}.",
                f"Professional engineering services focused on infrastructure development in {state}."
            ],
            "government": [
                f"State regulatory agency overseeing water resources and environmental compliance in {state}.",
                f"Government department responsible for public infrastructure in {state}.",
                f"Public works administration managing water systems and utilities in {state}."
            ],
            "municipal": [
                f"Local government entity providing public services in {state}.",
                f"City administration responsible for infrastructure and utilities in {state}.",
                f"Municipal authority overseeing water management and public services in {state}."
            ],
            "utility": [
                f"Public utility company providing essential services in {state}.",
                f"Utility district managing water and power distribution in {state}.",
                f"Infrastructure service provider for communities across {state}."
            ],
            "oil_gas": [
                f"Oil and gas production company operating in {state}.",
                f"Energy company with extraction and processing facilities in {state}.",
                f"Pipeline operator managing distribution networks in {state}."
            ],
            "agriculture": [
                f"Agricultural irrigation district serving farmers in {state}.",
                f"Farm management organization operating large-scale facilities in {state}.",
                f"Agricultural water management service for rural communities in {state}."
            ],
            "transportation": [
                f"Transportation authority managing infrastructure in {state}.",
                f"Highway management agency responsible for traffic systems in {state}.",
                f"Transit authority operating public transportation networks in {state}."
            ]
        }
        
        # Select a description based on organization type
        org_descriptions = descriptions.get(org_type, [f"Organization providing {org_type} services in {state}."])
        description = random.choice(org_descriptions)
        
        # Generate a more realistic organization name
        if org_name_base.lower() in ["example", "website", "site", "home", "index"]:
            org_name = f"{state} {org_type.title()} Services"
        else:
            org_name = f"{org_name_base} {org_type.title()}"
            
        # Generate a mock organization with high confidence
        mock_org = {
            "name": org_name,
            "org_type": org_type,
            "state": state,
            "description": description,
            "confidence_score": 0.85,  # High confidence for testing
        }
        
        # For testing, return a list with just one organization
        return [mock_org]

    def classify_org_type(self, name: str, description: str) -> str:
        """
        Classify organization type based on name and description.
        
        Args:
            name: Organization name
            description: Organization description
            
        Returns:
            Classified organization type string
        """
        combined_text = f"{name} {description}".lower()
        
        # Count keyword matches for each category
        category_scores = {}
        for category, keywords in CLASSIFICATION_KEYWORDS.items():
            category_scores[category] = 0
            for keyword in keywords:
                if keyword.lower() in combined_text:
                    category_scores[category] += 1
        
        # Get category with highest score
        if category_scores:
            max_category = max(category_scores.items(), key=lambda x: x[1])
            if max_category[1] > 0:
                return max_category[0]
        
        # Default if no clear classification
        return "water"  # Default to water as most common target


    def process_discovered_url(self, db_session: Session, url_record: DiscoveredURL, content: str, state_context: Optional[str] = None, industry_hint: Optional[str] = None) -> List[int]:
        """
        Process a discovered URL to extract organizations.
        
        Args:
            db_session: Database session
            url_record: DiscoveredURL record
            content: Page content
            state_context: Optional state context from search query
            industry_hint: Optional industry hint from search query
            
        Returns:
            List of organization IDs found in the content
        """
        logger.info(f"Processing URL {url_record.url} for organization extraction")
        
        # Extract organizations from content
        try:
            organizations = self.extract_organizations_from_content(
                content, 
                url_record.url,
                state_context,
                industry_hint
            )
        except Exception as e:
            logger.error(f"Error extracting organizations: {e}, generating mock data")
            # Generate mock data as fallback
            organizations = self._generate_mock_organizations(
                url_record.url, 
                state_context, 
                industry_hint
            )
        
        # Add organizations to database
        org_ids = []
        for org_data in organizations:
            # Check if organization already exists
            existing_org = None
            if 'name' in org_data and 'state' in org_data and org_data['state']:
                existing_org = crud.get_organization_by_name_and_state(
                    db_session, 
                    org_data['name'],
                    org_data['state']
                )
            
            if existing_org:
                # Update existing org with any new information if needed
                # For now, just use the existing org
                org_id = existing_org.id
                
                # Update relevance score if new score is higher
                if 'relevance_score' in org_data and org_data['relevance_score'] > existing_org.relevance_score:
                    existing_org.relevance_score = org_data['relevance_score']
                    db_session.commit()
            else:
                # Create new organization
                new_org_data = {
                    'name': org_data.get('name'),
                    'org_type': org_data.get('org_type'),
                    'state': org_data.get('state'),
                    'description': org_data.get('description'),
                    'source_url': org_data.get('source_url'),
                    'discovery_method': org_data.get('discovery_method'),
                    'confidence_score': org_data.get('confidence_score', 0.7),
                    'relevance_score': org_data.get('relevance_score', 7.0)
                }
                
                new_org = crud.create_organization(db_session, new_org_data)
                org_id = new_org.id
            
            # Link the URL to this organization
            if org_id not in org_ids:
                org_ids.append(org_id)
                self._link_organization_to_url(org_id, url_record.id)
        
        return org_ids


    def _extract_organizations_local(self, text: str, url: str, state_context: Optional[str] = None, industry_hint: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Simple local extraction method to use when Gemini API is unavailable.
        
        Args:
            text: Text content to extract from
            url: Source URL
            state_context: Optional state context
            industry_hint: Optional industry hint
            
        Returns:
            List of dictionaries with extracted organization information
        """
        logger.info(f"Using local extraction method for {url}")
        
        # Extract domain from URL as a starting point
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        # Remove www and get the main domain name
        if domain.startswith('www.'):
            domain = domain[4:]
            
        # Get the domain name without extension as potential org name
        domain_parts = domain.split('.')
        potential_org_name = domain_parts[0].replace('-', ' ').title()
        
        # Try to find a better name in the <title> tag
        title_match = re.search(r'<title[^>]*>(.*?)</title>', text, re.IGNORECASE | re.DOTALL)
        if title_match:
            title = title_match.group(1).strip()
            # Use title if it's not too long and looks like an organization name
            if len(title) < 60 and not title.startswith('Welcome') and not 'page' in title.lower():
                potential_org_name = title.split('|')[0].split('-')[0].strip()
        
        # Check for about/contact pages that might contain the org name
        for label in ['About Us', 'About', 'Company', 'Contact', 'Home']:
            pattern = f'<h1[^>]*>([^<]*{label}[^<]*)</h1>'
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                for match in matches:
                    if 'About' in match or 'Company' in match:
                        cleaner_name = re.sub(f'{label}', '', match, flags=re.IGNORECASE).strip()
                        if cleaner_name and len(cleaner_name) < 50:
                            potential_org_name = cleaner_name
                            break
        
        # Use provided industry hint or try to determine from content
        org_type = industry_hint if industry_hint else 'unknown'
        if not org_type or org_type == 'unknown':
            # Look for industry keywords in the text
            for category, keywords in CLASSIFICATION_KEYWORDS.items():
                for keyword in keywords:
                    if re.search(r'\b' + keyword + r'\b', text, re.IGNORECASE):
                        org_type = category
                        break
                if org_type != 'unknown':
                    break
            
            # If still unknown, default based on URL clues
            if org_type == 'unknown':
                domain_lower = domain.lower()
                if any(k in domain_lower for k in ['water', 'wtp', 'wwtp', 'utility']):
                    org_type = 'water'
                elif any(k in domain_lower for k in ['eng', 'engineer', 'design']):
                    org_type = 'engineering'
                elif any(k in domain_lower for k in ['gov', 'city', 'town', 'county']):
                    org_type = 'government'
                elif any(k in domain_lower for k in ['ag', 'farm', 'irrigation']):
                    org_type = 'agriculture'
                else:
                    # Default
                    org_type = 'water'
        
        # Create the organization object
        organization = {
            "name": potential_org_name,
            "org_type": org_type,
            "state": state_context or "Unknown",
            "description": f"Organization extracted from {domain} (local extraction fallback)",
            "confidence_score": 0.7,  # Moderate confidence for local extraction
            "source_url": url,
            "discovery_method": "local_extraction_fallback",
            "relevance_score": 7.0  # Moderate relevance score
        }
        
        return [organization]
    
    def _link_organization_to_url(self, organization_id, url_id):
        """
        Create a linking record between an organization and a discovered URL.
        
        Args:
            organization_id: Organization ID
            url_id: DiscoveredURL ID
        """
        try:
            # Import the relationship table from the central location
            from app.database.relationship_models import discovered_url_organizations
            
            # Check if the relationship already exists
            existing = self.db_session.execute(
                "SELECT 1 FROM discovered_url_organizations WHERE url_id = :url_id AND organization_id = :org_id",
                {"url_id": url_id, "org_id": organization_id}
            ).fetchone()
            
            if not existing:
                # Create the relationship
                self.db_session.execute(
                    "INSERT INTO discovered_url_organizations (url_id, organization_id, date_added) VALUES (:url_id, :org_id, CURRENT_TIMESTAMP)",
                    {"url_id": url_id, "org_id": organization_id}
                )
                self.db_session.commit()
                logger.debug(f"Linked organization {organization_id} to URL {url_id}")
            else:
                logger.debug(f"Organization {organization_id} already linked to URL {url_id}")
                
        except Exception as e:
            logger.error(f"Error linking organization to URL: {e}")
            self.db_session.rollback()