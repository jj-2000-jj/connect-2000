"""
Organization and contact classification using NLP.
"""
import re
from typing import Dict, Any, List, Tuple, Optional
import google.generativeai as genai
import nltk
from nltk.tokenize import word_tokenize
from sqlalchemy.orm import Session
from app.config import (
    GEMINI_API_KEY, CLASSIFICATION_KEYWORDS, ORG_TYPES, 
    NLP_CONFIDENCE_THRESHOLD, TARGET_STATES, ILLINOIS_SOUTH_OF_I80
)
from app.database.models import Organization, Keyword
from app.utils.logger import get_logger

# Initialize NLTK resources
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

logger = get_logger(__name__)

# Configure the Gemini API
genai.configure(api_key=GEMINI_API_KEY)


class OrganizationClassifier:
    """Classifier for organizations using keyword-based and AI methods."""
    
    def __init__(self, db_session: Session):
        """
        Initialize the organization classifier.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        self.model = genai.GenerativeModel('gemini-2.0-flash')
        self.keyword_weights = {
            "name": 5.0,        # Keywords in organization name have highest weight
            "description": 2.0,  # Keywords in description have medium weight
            "website": 1.0      # Keywords from website text have lower weight
        }
    
    def classify_organization(self, org_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Classify an organization based on available data.
        
        Args:
            org_data: Organization data including name, description, etc.
            
        Returns:
            Updated organization data with classification information
        """
        # Ensure we don't modify the original dict
        org_data = org_data.copy()
        
        # Ensure required fields exist
        org_name = org_data.get("name", "")
        if not org_name:
            org_name = "Unknown Organization"
            org_data["name"] = org_name
            
        logger.info(f"Classifying organization: {org_name}")
        
        # Extract basic data
        description = org_data.get("description", "")
        website_text = org_data.get("website_text", "")
        state = org_data.get("state", "")
        
        # If state is missing, use a placeholder
        if not state:
            state = "Unknown"
            org_data["state"] = state
            
        # Ensure website field exists
        if "website" not in org_data or not org_data["website"]:
            org_data["website"] = org_data.get("source_url", "")
            
        # TEMPORARILY DISABLED: Validate target state
        # Skip state validation to allow all search results to be processed
        # This helps test organization creation from search results
        if state and state not in TARGET_STATES:
            logger.info(f"Organization {org_name} is not in a target state ({state}), but allowing it for testing")
        
        # Step 1: Keyword-based classification
        keyword_scores = self._classify_by_keywords(org_name, description, website_text)
        
        # Get best matching category from keyword scores
        best_category, keyword_confidence = self._get_best_category(keyword_scores)
        
        # Step 2: AI-based classification if keyword confidence is low
        ai_category = None
        ai_confidence = 0.0
        
        if keyword_confidence < NLP_CONFIDENCE_THRESHOLD:
            try:
                ai_category, ai_confidence, ai_subtype = self._classify_with_ai(org_name, description, website_text)
                
                # If AI confidence is higher, use AI classification
                if ai_confidence > keyword_confidence:
                    best_category = ai_category
                    org_data["subtype"] = ai_subtype
            except Exception as e:
                logger.error(f"Error in AI classification for {org_name}: {e}")
                # Continue with keyword classification
        
        # Set classification results - ensure we have a valid org_type
        if not best_category or best_category not in [t for t in CLASSIFICATION_KEYWORDS.keys()]:
            best_category = "engineering"  # Default fallback
            logger.warning(f"Setting default org_type 'engineering' for {org_name}")
            
        org_data["org_type"] = best_category
        org_data["confidence_score"] = max(keyword_confidence, ai_confidence)
        
        # Calculate relevance score based on organization type and SCADA keywords
        try:
            relevance_score = self._calculate_relevance_score(org_data, org_name, description, website_text)
        except Exception as e:
            logger.error(f"Error calculating relevance score for {org_name}: {e}")
            relevance_score = 0.5  # Default value
            
        org_data["relevance_score"] = relevance_score
        
        # Calculate data quality score
        try:
            data_quality_score = self._calculate_data_quality_score(org_data)
        except Exception as e:
            logger.error(f"Error calculating data quality score for {org_name}: {e}")
            data_quality_score = 0.5  # Default value
            
        org_data["data_quality_score"] = data_quality_score
        
        # Set additional required fields if missing
        if "subtype" not in org_data or not org_data["subtype"]:
            org_data["subtype"] = ""
            
        logger.info(f"Classified {org_name} as {best_category} with confidence {org_data['confidence_score']:.2f}")
        
        return org_data
    
    def _classify_by_keywords(self, name: str, description: str, website_text: str) -> Dict[str, float]:
        """
        Classify organization using keyword frequency analysis.
        
        Args:
            name: Organization name
            description: Organization description
            website_text: Text extracted from organization website
            
        Returns:
            Dictionary mapping categories to scores
        """
        scores = {category: 0.0 for category in CLASSIFICATION_KEYWORDS.keys()}
        
        # Normalize text
        name_tokens = self._normalize_text(name)
        desc_tokens = self._normalize_text(description)
        website_tokens = self._normalize_text(website_text)
        
        # For each category, calculate score based on keyword matches
        for category, keywords in CLASSIFICATION_KEYWORDS.items():
            # Check name (highest weight)
            for keyword in keywords:
                if self._contains_keyword(name_tokens, keyword):
                    scores[category] += self.keyword_weights["name"]
            
            # Check description (medium weight)
            for keyword in keywords:
                if self._contains_keyword(desc_tokens, keyword):
                    scores[category] += self.keyword_weights["description"]
            
            # Check website text (lowest weight)
            for keyword in keywords:
                if self._contains_keyword(website_tokens, keyword):
                    scores[category] += self.keyword_weights["website"]
        
        return scores
    
    def _normalize_text(self, text: str) -> List[str]:
        """
        Normalize text for keyword matching.
        
        Args:
            text: Text to normalize
            
        Returns:
            List of normalized tokens
        """
        if not text:
            return []
            
        # Convert to lowercase
        text = text.lower()
        
        # Tokenize
        tokens = word_tokenize(text)
        
        return tokens
    
    def _contains_keyword(self, tokens: List[str], keyword: str) -> bool:
        """
        Check if tokens contain a keyword or phrase.
        
        Args:
            tokens: List of tokens
            keyword: Keyword or phrase to check
            
        Returns:
            True if tokens contain the keyword, False otherwise
        """
        if not tokens:
            return False
            
        # If keyword is a phrase, split it
        keyword_tokens = keyword.lower().split()
        
        # If single word, simple check
        if len(keyword_tokens) == 1:
            return keyword_tokens[0] in tokens
        
        # For phrases, check for consecutive tokens
        text = " ".join(tokens)
        return keyword.lower() in text
    
    def _get_best_category(self, scores: Dict[str, float]) -> Tuple[str, float]:
        """
        Get the best matching category from scores.
        
        Args:
            scores: Dictionary mapping categories to scores
            
        Returns:
            Tuple with best category and confidence score
        """
        if not scores:
            return "engineering", 0.0
            
        # Get total score
        total_score = sum(scores.values())
        
        # If no matches, default to engineering but with a minimum score 
        if total_score == 0:
            logger.info(f"No keyword matches found for organization, defaulting to 'engineering'")
            return "engineering", 0.3
            
        # Get category with highest score
        best_category = max(scores, key=scores.get)
        
        # Calculate confidence
        confidence = scores[best_category] / total_score
        
        return best_category, min(confidence, 1.0)
    
    def _classify_with_ai(self, name: str, description: str, website_text: str) -> Tuple[str, float, str]:
        """
        Classify organization using Gemini AI.
        
        Args:
            name: Organization name
            description: Organization description
            website_text: Text extracted from organization website
            
        Returns:
            Tuple with category, confidence score, and subtype
        """
        # Combine text for classification
        combined_text = f"Organization Name: {name}\n"
        if description:
            combined_text += f"Description: {description}\n"
        if website_text:
            # Limit website text to avoid exceeding token limits
            combined_text += f"Website Text: {website_text[:1000]}...\n"
        
        # Create prompt
        prompt = f"""
        Classify the following organization into one of these categories related to SCADA integration:
        - engineering: Engineering Firms (civil, electrical, environmental, mechanical, multidisciplinary)
        - government/municipal: Municipalities & Government Agencies (city, county, state)
        - water: Water and Wastewater Companies/Districts (water treatment, distribution, wastewater)
        - utility: Utility Companies (electrical, natural gas, renewable energy)
        - transportation: Transportation Authorities (rail, transit, airport, traffic management)
        - oil_gas: Oil and Gas & Mining Companies (oil/gas extraction, processing, pipeline, mining)
        - agriculture: Agriculture/Irrigation Districts (agriculture, irrigation)

        Organization Information:
        {combined_text}

        Respond in JSON format with:
        1. category: The best matching category from the list above
        2. confidence: A confidence score from 0.0 to 1.0
        3. subtype: The specific subtype within the category
        4. reasoning: Brief explanation for the classification

        Example response:
        {{
            "category": "water",
            "confidence": 0.85,
            "subtype": "water treatment",
            "reasoning": "The organization name mentions 'water district' and the description describes water treatment operations."
        }}
        """
        
        try:
            # Generate classification using Gemini
            response = self.model.generate_content(prompt)
            
            # Parse response
            result_text = response.text.strip()
            
            # Extract JSON data
            import json
            # Try to find JSON in response
            match = re.search(r'{.*}', result_text, re.DOTALL)
            if match:
                result_json = json.loads(match.group(0))
            else:
                # If no JSON found, try to parse the entire response
                result_json = json.loads(result_text)
            
            category = result_json.get("category", "").lower()
            if "government" in category or "municipal" in category:
                category = "government"
                
            confidence = float(result_json.get("confidence", 0.0))
            subtype = result_json.get("subtype", "")
            
            logger.info(f"AI classification: {category} ({subtype}) with confidence {confidence}")
            
            return category, confidence, subtype
            
        except Exception as e:
            logger.error(f"Error in AI classification: {e}")
            return "engineering", 0.0, ""
    
    def _calculate_relevance_score(self, org_data: Dict[str, Any], name: str, 
                                description: str, website_text: str) -> float:
        """
        Calculate relevance score based on refined industry-specific criteria.
        
        Args:
            org_data: Organization data
            name: Organization name
            description: Organization description
            website_text: Text extracted from organization website
            
        Returns:
            Relevance score (0.0-1.0)
        """
        # Start with base score based on organization type
        org_type = org_data.get("org_type", "")
        
        # Check if this is an excluded organization type
        from app.config import EXCLUDED_ORGANIZATION_TYPES
        
        # Combine text for keyword search
        combined_text = f"{name} {description} {website_text}".lower()
        
        # Check for competitors - these should be excluded completely
        for competitor_keyword in EXCLUDED_ORGANIZATION_TYPES.get("competitors", []):
            if competitor_keyword in combined_text:
                return 0.0
                
        # Check for irrelevant sectors - these should be excluded
        for irrelevant_sector in EXCLUDED_ORGANIZATION_TYPES.get("irrelevant_sectors", []):
            if irrelevant_sector in combined_text:
                return 0.0
                
        # Check if the organization appears to be a direct competitor based on services
        service_count = 0
        for exclusion_keyword in EXCLUDED_ORGANIZATION_TYPES.get("exclusion_keywords", []):
            if exclusion_keyword in combined_text:
                service_count += 1
                
        # If multiple competitor service keywords are found, exclude the organization
        if service_count >= 2:
            return 0.0
        
        # Base relevance scores for our refined target industries
        type_relevance = {
            "water": 0.8,
            "agriculture": 0.7,
            "healthcare": 0.8,
            "emergency": 0.7,
            "engineering": 0.6,
            "government": 0.5,
            "municipal": 0.6,
            "utility": 0.7,
            "transportation": 0.5,
            "oil_gas": 0.6
        }
        
        base_score = type_relevance.get(org_type, 0.4)
        
        # Industry-specific relevance indicators
        industry_specific_criteria = {
            "water": [
                "compliance monitoring", "regulatory requirements", "water quality control",
                "treatment process", "remote monitoring", "data logging", "epa compliance",
                "water safety", "chlorination", "water testing", "backflow prevention"
            ],
            "agriculture": [
                "complex irrigation", "multiple water sources", "water conservation",
                "irrigation automation", "remote field monitoring", "precision agriculture",
                "water management", "crop monitoring", "soil moisture", "sprinkler control"
            ],
            "healthcare": [
                "legionella", "legionella prevention", "water safety plan", "monitoring",
                "patient safety", "hospital compliance", "water management program",
                "temperature monitoring", "disinfection", "healthcare compliance"
            ],
            "emergency": [
                "alerting system", "emergency response", "critical infrastructure",
                "public notification", "resilience planning", "backup systems",
                "emergency management", "critical operations", "alert notification"
            ]
        }
        
        # Check for industry-specific criteria
        specific_criteria = industry_specific_criteria.get(org_type, [])
        specific_criteria_count = 0
        for criteria in specific_criteria:
            if criteria in combined_text:
                specific_criteria_count += 1
                
        # Add bonus for industry-specific relevance
        specific_criteria_score = min(0.1 * specific_criteria_count, 0.4)
        
        # Check for general SCADA-related keywords (less weight than before)
        scada_keywords = [
            "scada", "control system", "automation", "plc", "hmi", "industrial control",
            "instrumentation", "process control", "telemetry", "remote monitoring", 
            "water treatment", "wastewater", "pump station", "RTU", "ICS"
        ]
        
        keyword_score = 0.0
        for keyword in scada_keywords:
            if keyword in combined_text:
                keyword_score += 0.05
        
        # Cap keyword score
        keyword_score = min(keyword_score, 0.3)
        
        # Final score is base + industry-specific + keyword, but max 1.0
        final_score = min(base_score + specific_criteria_score + keyword_score, 1.0)
        
        # Log the relevance calculation steps for debugging
        logger.info(f"Relevance calculation for {org_data.get('name', 'Unknown')}:")
        logger.info(f"  Base score ({org_type}): {base_score}")
        logger.info(f"  Industry-specific criteria: {specific_criteria_count} matches, score: {specific_criteria_score}")
        logger.info(f"  SCADA keyword score: {keyword_score}")
        logger.info(f"  Final relevance score: {final_score}")
        
        return final_score
    
    def _calculate_data_quality_score(self, org_data: Dict[str, Any]) -> float:
        """
        Calculate data quality score based on completeness.
        
        Args:
            org_data: Organization data
            
        Returns:
            Data quality score (0.0-1.0)
        """
        # Essential fields
        essential = ["name", "org_type", "state"]
        
        # Important fields
        important = ["website", "city", "description"]
        
        # Helpful fields
        helpful = ["phone", "address", "zip_code", "county"]
        
        # Calculate scores based on field presence
        essential_score = sum(1.0 for field in essential if org_data.get(field)) / len(essential)
        important_score = sum(0.5 for field in important if org_data.get(field)) / len(important)
        helpful_score = sum(0.25 for field in helpful if org_data.get(field)) / len(helpful)
        
        # Weighted average
        total_weight = 1.0 + 0.5 + 0.25
        quality_score = (essential_score + important_score + helpful_score) / total_weight
        
        return min(quality_score, 1.0)


class ContactClassifier:
    """Classifier for contacts to determine relevance and validity."""
    
    def __init__(self, db_session: Session):
        """
        Initialize the contact classifier.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
    
    def classify_contact(self, contact_data: Dict[str, Any], org_type: str) -> Dict[str, Any]:
        """
        Classify a contact based on job title and other attributes.
        
        Args:
            contact_data: Contact data
            org_type: Organization type
            
        Returns:
            Updated contact data with classification information
        """
        logger.info(f"Classifying contact: {contact_data.get('first_name', '')} {contact_data.get('last_name', '')} ({contact_data.get('job_title', '')})")
        
        # Extract data
        job_title = contact_data.get("job_title", "").lower()
        email = contact_data.get("email", "")
        
        # Validate email if available
        if email:
            email_valid = self._validate_email(email)
            contact_data["email_valid"] = email_valid
        
        # Calculate confidence score based on job title relevance
        confidence_score = self._calculate_title_relevance(job_title, org_type)
        contact_data["contact_confidence_score"] = confidence_score
        
        # Determine which sales person this contact should be assigned to
        from app.config import EMAIL_USERS
        for email, org_types in EMAIL_USERS.items():
            if org_type in org_types:
                contact_data["assigned_to"] = email
                break
        
        return contact_data
    
    def _validate_email(self, email: str) -> bool:
        """
        Validate an email address format and domain.
        
        Args:
            email: Email address to validate
            
        Returns:
            True if email is valid, False otherwise
        """
        # Basic format validation
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(email_pattern, email):
            return False
        
        # In a production system, you might want to:
        # 1. Check MX records
        # 2. Verify against known disposable email domains
        # 3. Potentially use an email verification service
        
        return True
    
    def _calculate_title_relevance(self, job_title: str, org_type: str) -> float:
        """
        Calculate relevance score based on job title and industry-specific roles.
        
        Args:
            job_title: Job title
            org_type: Organization type
            
        Returns:
            Relevance score (0.0-1.0)
        """
        # Get target job titles for this organization type
        target_titles = ORG_TYPES.get(org_type, {}).get("job_titles", [])
        
        # If no target titles defined, use low confidence
        if not target_titles:
            return 0.3
        
        job_title = job_title.lower()
        
        # Check for exact matches
        for title in target_titles:
            if title.lower() == job_title:
                return 1.0
        
        # Check for partial matches
        for title in target_titles:
            if title.lower() in job_title or job_title in title.lower():
                return 0.8
        
        # Industry-specific role keywords with higher weights
        industry_role_keywords = {
            "water": [
                "water operations", "treatment", "plant", "compliance", "quality",
                "water systems", "utility", "operations", "water"
            ],
            "agriculture": [
                "irrigation", "farm operations", "agricultural", "water resources",
                "field", "farm", "crop"
            ],
            "healthcare": [
                "facilities", "engineering", "safety", "plant operations", 
                "maintenance", "hospital", "environmental", "compliance"
            ],
            "emergency": [
                "emergency", "operations", "critical", "public safety",
                "disaster", "response", "management", "systems"
            ]
        }
        
        # Check for industry-specific role keywords
        industry_keywords = industry_role_keywords.get(org_type, [])
        for keyword in industry_keywords:
            if keyword in job_title:
                return 0.7
        
        # General relevant role keywords (less weight than industry-specific)
        relevant_keywords = [
            "manager", "director", "engineer", "supervisor", "chief",
            "head", "lead", "administrator", "coordinator", 
            "scada", "control", "automation", "operations", "technical",
            "maintenance", "project", "systems", "technology"
        ]
        
        for keyword in relevant_keywords:
            if keyword in job_title:
                return 0.5
        
        # Low relevance for other job titles
        return 0.2