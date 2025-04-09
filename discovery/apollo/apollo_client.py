"""
Apollo.io API integration for organization and contact discovery.
"""
import time
import json
from typing import List, Dict, Any, Optional
import requests
from sqlalchemy.orm import Session

from app.config import APOLLO_API_KEY, ORG_TYPES
from app.database.models import Organization, Contact
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ApolloClient:
    """Client for Apollo.io API integration."""
    
    def __init__(self, db_session: Session, api_key: str = None):
        """
        Initialize the Apollo client.
        
        Args:
            db_session: Database session
            api_key: Apollo.io API key (if None, try to get from environment)
        """
        self.db_session = db_session
        
        # Get API key from env if not provided
        if api_key is None:
            api_key = APOLLO_API_KEY
            
        self.api_key = api_key
        self.base_url = "https://api.apollo.io/v1"
        self.headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }
        self.delay_between_requests = 1  # seconds between requests to avoid rate limiting
    
    def search_organizations(self, 
                           q: str = None,
                           org_type: str = None, 
                           state: str = None,
                           page: int = 1,
                           per_page: int = 25) -> Optional[Dict[str, Any]]:
        """
        Search for organizations using Apollo.io API.
        
        Args:
            q: Search query string
            org_type: Organization type to filter by
            state: State/location to filter by
            page: Page number for pagination
            per_page: Number of results per page
            
        Returns:
            Search results or None if the request fails
        """
        try:
            # Build organization search criteria
            search_criteria = {}
            
            if q:
                search_criteria["q"] = q
                
            # Map internal org_type to Apollo industry types if needed
            if org_type:
                # Could be expanded with a proper mapping of organization types
                # to Apollo's industry categories
                industries = []
                if org_type == "engineering":
                    industries = ["Engineering"]
                elif org_type in ["water", "utility"]:
                    industries = ["Utilities"]
                elif org_type == "transportation":
                    industries = ["Transportation"]
                elif org_type == "oil_gas":
                    industries = ["Oil & Energy"]
                elif org_type == "agriculture":
                    industries = ["Agriculture"]
                elif org_type in ["municipal", "government"]:
                    industries = ["Government Administration", "Public Policy"]
                
                if industries:
                    search_criteria["industries"] = industries
            
            # Add state/location filter
            if state:
                search_criteria["locations"] = [f"{state}, US"]
            
            # Define API payload
            payload = {
                "api_key": self.api_key,
                "page": page,
                "per_page": per_page
            }
            
            # Add search criteria if defined
            if search_criteria:
                payload["search_criteria"] = search_criteria
            
            # Make API request
            response = requests.post(
                f"{self.base_url}/organizations/search",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            
            return response.json()
        
        except requests.RequestException as e:
            logger.error(f"Error searching organizations via Apollo.io: {e}")
            return None
    
    def get_organization_details(self, organization_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about an organization.
        
        Args:
            organization_id: Apollo.io organization ID
            
        Returns:
            Organization details or None if the request fails
        """
        try:
            payload = {
                "api_key": self.api_key,
                "id": organization_id
            }
            
            response = requests.post(
                f"{self.base_url}/organizations/enrich",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            
            return response.json()
        
        except requests.RequestException as e:
            logger.error(f"Error getting organization details: {e}")
            return None
    
    def search_people(self, 
                     organization_id: str = None,
                     organization_name: str = None,
                     job_titles: List[str] = None,
                     state: str = None,
                     page: int = 1,
                     per_page: int = 25) -> Optional[Dict[str, Any]]:
        """
        Search for people using Apollo.io API.
        
        Args:
            organization_id: Apollo.io organization ID
            organization_name: Organization name (if ID not available)
            job_titles: List of job titles to filter by
            state: State to filter contacts by
            page: Page number for pagination
            per_page: Number of results per page
            
        Returns:
            Search results or None if the request fails
        """
        try:
            # Build person search criteria
            search_criteria = {}
            
            # Add organization filter
            if organization_id:
                search_criteria["organization_ids"] = [organization_id]
            elif organization_name:
                search_criteria["organization_names"] = [organization_name]
            
            # Add job title filter
            if job_titles and len(job_titles) > 0:
                search_criteria["titles"] = job_titles
            
            # Add state/location filter
            if state:
                search_criteria["locations"] = [f"{state}, US"]
            
            # Add seniority filter - focus on decision makers
            search_criteria["seniorities"] = ["director", "executive", "vp", "owner", "partner", "c_suite", "founder"]
            
            # Define API payload
            payload = {
                "api_key": self.api_key,
                "page": page,
                "per_page": per_page,
                "search_criteria": search_criteria
            }
            
            # Make API request
            response = requests.post(
                f"{self.base_url}/people/search",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            
            return response.json()
        
        except requests.RequestException as e:
            logger.error(f"Error searching people via Apollo.io: {e}")
            return None
    
    def get_person_details(self, person_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a person.
        
        Args:
            person_id: Apollo.io person ID
            
        Returns:
            Person details or None if the request fails
        """
        try:
            payload = {
                "api_key": self.api_key,
                "id": person_id
            }
            
            response = requests.post(
                f"{self.base_url}/people/detail",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            
            return response.json()
        
        except requests.RequestException as e:
            logger.error(f"Error getting person details: {e}")
            return None
    
    def get_email(self, person_id: str) -> Optional[Dict[str, Any]]:
        """
        Get email for a person using Apollo's email API.
        
        Args:
            person_id: Apollo.io person ID
            
        Returns:
            Email details or None if the request fails
        """
        try:
            payload = {
                "api_key": self.api_key,
                "person_id": person_id
            }
            
            response = requests.post(
                f"{self.base_url}/people/get_email",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            
            return response.json()
        
        except requests.RequestException as e:
            logger.error(f"Error getting person email: {e}")
            return None
    
    def find_contacts_for_organization(self, organization: Organization) -> List[Dict[str, Any]]:
        """
        Find contacts for an organization using Apollo.io with enhanced search criteria and pagination.
        
        Args:
            organization: Organization to find contacts for
            
        Returns:
            List of contacts found
        """
        if not self.api_key:
            logger.error("Apollo API key not available. Cannot search for contacts.")
            return []
        
        logger.info(f"Searching for contacts for organization: {organization.name}")
        
        # Store Apollo organization ID if found for future use
        apollo_org_id = None
        
        # Get job titles based on organization type
        job_titles = ORG_TYPES.get(organization.org_type, {}).get("job_titles", [])
        
        # Apply organizational context for better search results
        per_page = 25  # Maximum allowed by Apollo API
        all_people = []
        
        # Customize search criteria based on organization type
        seniorities = []
        departments = []
        keyword_booleans = []
        
        # Customize search strategy based on organization type
        if organization.org_type == "engineering":
            seniorities = ["director", "executive", "vp", "owner", "partner", "c_suite", "founder", "manager"]
            departments = ["engineering", "operations", "it", "technical", "project management"]
            keyword_booleans = ["SCADA", "control systems", "automation", "integration", "project management"]
        
        elif organization.org_type in ["municipal", "government"]:
            seniorities = ["director", "manager", "head", "chief", "superintendent", "supervisor", "administrator"]
            departments = ["public works", "utilities", "operations", "water", "technical services"]
            keyword_booleans = ["utilities", "water management", "public works", "infrastructure"]
        
        elif organization.org_type in ["water", "utility"]:
            seniorities = ["director", "manager", "head", "chief", "superintendent", "supervisor"]
            departments = ["operations", "engineering", "maintenance", "technical", "control systems"]
            keyword_booleans = ["water treatment", "wastewater", "utilities management", "SCADA", "control systems"]
        
        elif organization.org_type == "transportation":
            seniorities = ["director", "manager", "head", "chief", "superintendent", "supervisor"]
            departments = ["operations", "engineering", "maintenance", "technical", "signals", "communications"]
            keyword_booleans = ["signals", "transportation", "traffic management", "control systems"]
        
        elif organization.org_type == "oil_gas":
            seniorities = ["director", "manager", "engineer", "head", "chief", "superintendent", "supervisor"]
            departments = ["operations", "engineering", "production", "maintenance", "technical", "automation"]
            keyword_booleans = ["pipeline", "extraction", "automation", "control systems", "SCADA"]
        
        elif organization.org_type == "agriculture":
            seniorities = ["director", "manager", "head", "chief", "superintendent", "supervisor"]
            departments = ["operations", "irrigation", "water management", "technical", "engineering"]
            keyword_booleans = ["irrigation", "water management", "agriculture", "automation"]
        
        # First try to search with organization name - with pagination support
        page = 1
        logger.info(f"Searching for contacts with titles: {job_titles}")
        
        while True:
            # Build search criteria
            search_criteria = {
                "organization_name": organization.name,
                "seniorities": seniorities if seniorities else ["director", "executive", "vp", "owner", "partner", "c_suite", "founder", "manager"],
            }
            
            # Add departments if specified
            if departments:
                search_criteria["departments"] = departments
                
            # Add job titles if specified
            if job_titles:
                search_criteria["titles"] = job_titles
                
            # Add state filter
            if organization.state:
                search_criteria["locations"] = [f"{organization.state}, US"]
                
            # Add keyword boolean search if specified
            if keyword_booleans:
                search_criteria["keyword_booleans"] = keyword_booleans
            
            # Execute search
            try:
                people_results = self.search_people(
                    organization_name=organization.name,
                    job_titles=job_titles,
                    state=organization.state,
                    page=page,
                    per_page=per_page
                )
                
                # If no results or error, break pagination loop
                if not people_results or "people" not in people_results:
                    break
                    
                current_batch = people_results.get("people", [])
                all_people.extend(current_batch)
                
                # Try to get the organization ID from the first result
                if current_batch and not apollo_org_id and "organization" in current_batch[0]:
                    apollo_org_id = current_batch[0]["organization"].get("id")
                    if apollo_org_id:
                        logger.info(f"Found Apollo organization ID: {apollo_org_id} for {organization.name}")
                
                # Check if we should continue pagination
                if len(current_batch) < per_page:  # Less than full page means we're done
                    break
                    
                # Move to next page
                page += 1
                
                # Respect API rate limits
                time.sleep(self.delay_between_requests)
                
            except Exception as e:
                logger.error(f"Error in contact search for {organization.name}, page {page}: {e}")
                break
        
        # If we found an Apollo organization ID, try a more precise search
        if apollo_org_id and not all_people:
            logger.info(f"Trying search with Apollo organization ID: {apollo_org_id}")
            page = 1
            
            while True:
                try:
                    people_results = self.search_people(
                        organization_id=apollo_org_id,
                        job_titles=job_titles,
                        state=organization.state,
                        page=page,
                        per_page=per_page
                    )
                    
                    if not people_results or "people" not in people_results:
                        break
                        
                    current_batch = people_results.get("people", [])
                    all_people.extend(current_batch)
                    
                    if len(current_batch) < per_page:  # Less than full page means we're done
                        break
                        
                    page += 1
                    time.sleep(self.delay_between_requests)
                    
                except Exception as e:
                    logger.error(f"Error in ID-based contact search: {e}")
                    break
        
        # If still no results, try without job titles but with seniorities
        if not all_people:
            logger.info(f"Trying broader search for {organization.name}")
            page = 1
            
            while True and page <= 3:  # Limit to 3 pages for broader search to avoid too many irrelevant results
                try:
                    # Broader search without job titles
                    people_results = self.search_people(
                        organization_name=organization.name,
                        state=organization.state,
                        page=page,
                        per_page=per_page
                    )
                    
                    if not people_results or "people" not in people_results:
                        break
                        
                    current_batch = people_results.get("people", [])
                    all_people.extend(current_batch)
                    
                    if len(current_batch) < per_page:
                        break
                        
                    page += 1
                    time.sleep(self.delay_between_requests)
                    
                except Exception as e:
                    logger.error(f"Error in broader contact search: {e}")
                    break
        
        # If no contacts found through direct search, try company enrichment
        if not all_people and organization.website:
            logger.info(f"Trying organization enrichment for {organization.name} using website {organization.website}")
            
            enrichment_payload = {
                "api_key": self.api_key,
                "domain": organization.website
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/organizations/enrich",
                    headers=self.headers,
                    json=enrichment_payload
                )
                response.raise_for_status()
                
                enrichment_data = response.json()
                
                if enrichment_data and "organization" in enrichment_data:
                    apollo_org_id = enrichment_data["organization"].get("id")
                    
                    if apollo_org_id:
                        logger.info(f"Found Apollo organization ID through enrichment: {apollo_org_id}")
                        
                        # Use this ID to find people
                        page = 1
                        while True:
                            try:
                                people_results = self.search_people(
                                    organization_id=apollo_org_id,
                                    page=page,
                                    per_page=per_page
                                )
                                
                                if not people_results or "people" not in people_results:
                                    break
                                    
                                current_batch = people_results.get("people", [])
                                all_people.extend(current_batch)
                                
                                if len(current_batch) < per_page:
                                    break
                                    
                                page += 1
                                time.sleep(self.delay_between_requests)
                                
                            except Exception as e:
                                logger.error(f"Error in enrichment-based contact search: {e}")
                                break
            except Exception as e:
                logger.error(f"Error enriching organization {organization.name}: {e}")
        
        # If still no results, log and return empty list
        if not all_people:
            logger.info(f"No contacts found for organization: {organization.name}")
            return []
        
        # Process and save contacts
        contacts = []
        similar_ids_to_check = []  # Store IDs for finding similar contacts
        
        for person in all_people:
            try:
                # Extract basic person data
                first_name = person.get("first_name", "")
                last_name = person.get("last_name", "")
                job_title = person.get("title", "")
                email = person.get("email", "")
                apollo_id = person.get("id")
                phone = person.get("phone_number", "")
                linkedin_url = person.get("linkedin_url", "")
                
                # Skip if missing essential information
                if not first_name or not last_name or not job_title:
                    continue
                
                # If this is a key position (manager or higher), add to list for similar contacts
                if apollo_id and any(term in job_title.lower() for term in ["director", "manager", "chief", "head", "president", "vp", "executive"]):
                    similar_ids_to_check.append(apollo_id)
                
                # Check if contact already exists
                existing_contact = self.db_session.query(Contact).filter(
                    Contact.organization_id == organization.id,
                    Contact.job_title == job_title,
                    Contact.first_name == first_name,
                    Contact.last_name == last_name
                ).first()
                
                if existing_contact:
                    logger.info(f"Contact already exists: {first_name} {last_name} ({job_title})")
                    
                    # Update email or other details if they were missing
                    updated = False
                    if not existing_contact.email and email:
                        existing_contact.email = email
                        existing_contact.email_valid = True
                        updated = True
                    
                    if not existing_contact.phone and phone:
                        existing_contact.phone = phone
                        updated = True
                        
                    if not existing_contact.linkedin_url and linkedin_url:
                        existing_contact.linkedin_url = linkedin_url
                        updated = True
                    
                    if updated:
                        self.db_session.commit()
                        
                    contacts.append({
                        "id": existing_contact.id,
                        "first_name": existing_contact.first_name,
                        "last_name": existing_contact.last_name,
                        "job_title": existing_contact.job_title,
                        "email": existing_contact.email,
                        "phone": existing_contact.phone,
                        "organization_id": existing_contact.organization_id,
                        "organization_name": organization.name
                    })
                    continue
                
                # If no email found, try to get it
                if not email and apollo_id:
                    time.sleep(self.delay_between_requests)
                    email_result = self.get_email(apollo_id)
                    if email_result and "email" in email_result:
                        email = email_result.get("email")
                
                # Create new contact
                contact = Contact(
                    organization_id=organization.id,
                    first_name=first_name,
                    last_name=last_name,
                    job_title=job_title,
                    email=email,
                    phone=phone,
                    email_valid=bool(email),
                    linkedin_url=linkedin_url,
                    discovery_method="apollo",
                    discovery_url="https://apollo.io",
                    contact_confidence_score=0.85  # High confidence for Apollo data
                )
                
                # Determine which sales person this contact should be assigned to
                from app.config import EMAIL_USERS
                for user_email, org_types in EMAIL_USERS.items():
                    if organization.org_type in org_types:
                        contact.assigned_to = user_email
                        break
                
                self.db_session.add(contact)
                self.db_session.commit()
                
                contacts.append({
                    "id": contact.id,
                    "first_name": contact.first_name,
                    "last_name": contact.last_name,
                    "job_title": contact.job_title,
                    "email": contact.email,
                    "phone": contact.phone,
                    "organization_id": contact.organization_id,
                    "organization_name": organization.name
                })
                
                logger.info(f"Created new contact from Apollo: {contact.first_name} {contact.last_name} ({contact.job_title})")
                
                # Add delay to avoid rate limiting
                time.sleep(self.delay_between_requests)
                
            except Exception as e:
                logger.error(f"Error processing Apollo contact: {e}")
        
        # If we found key contacts, try to find similar contacts (limited to 2 key contacts)
        if similar_ids_to_check and len(contacts) < 10:
            logger.info(f"Searching for similar contacts based on {len(similar_ids_to_check)} key contacts")
            
            # Limit to 2 key contacts to avoid excessive API calls
            for key_id in similar_ids_to_check[:2]:
                similar_contacts = self.find_similar_contacts(key_id, organization)
                if similar_contacts:
                    contacts.extend(similar_contacts)
        
        # Update Apollo ID for the organization if found
        if apollo_org_id and not organization.source_url:
            organization.source_url = f"https://app.apollo.io/#/organizations/{apollo_org_id}"
            self.db_session.commit()
            logger.info(f"Updated organization with Apollo URL: {organization.source_url}")
        
        return contacts
    
    def find_similar_contacts(self, person_id: str, organization: Organization) -> List[Dict[str, Any]]:
        """
        Find contacts similar to a given person.
        
        Args:
            person_id: Apollo ID of the person to find similar contacts for
            organization: The organization to associate similar contacts with
            
        Returns:
            List of similar contacts
        """
        similar_contacts = []
        
        try:
            payload = {
                "api_key": self.api_key,
                "id": person_id
            }
            
            response = requests.post(
                f"{self.base_url}/people/similar",
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            
            result = response.json()
            
            if result and "similar_people" in result:
                for person in result.get("similar_people", [])[:5]:  # Limit to 5 similar contacts
                    # Extract basic person data
                    first_name = person.get("first_name", "")
                    last_name = person.get("last_name", "")
                    job_title = person.get("title", "")
                    email = person.get("email", "")
                    apollo_id = person.get("id")
                    phone = person.get("phone_number", "")
                    linkedin_url = person.get("linkedin_url", "")
                    
                    # Skip if missing essential information
                    if not first_name or not last_name or not job_title:
                        continue
                    
                    # Skip if person is from a different organization
                    org_name = person.get("organization", {}).get("name", "")
                    if org_name and org_name.lower() != organization.name.lower():
                        continue
                    
                    # Check if contact already exists
                    existing_contact = self.db_session.query(Contact).filter(
                        Contact.organization_id == organization.id,
                        Contact.job_title == job_title,
                        Contact.first_name == first_name,
                        Contact.last_name == last_name
                    ).first()
                    
                    if existing_contact:
                        logger.info(f"Similar contact already exists: {first_name} {last_name} ({job_title})")
                        continue
                    
                    # If no email found, try to get it
                    if not email and apollo_id:
                        time.sleep(self.delay_between_requests)
                        email_result = self.get_email(apollo_id)
                        if email_result and "email" in email_result:
                            email = email_result.get("email")
                    
                    # Create new contact
                    contact = Contact(
                        organization_id=organization.id,
                        first_name=first_name,
                        last_name=last_name,
                        job_title=job_title,
                        email=email,
                        phone=phone,
                        email_valid=bool(email),
                        linkedin_url=linkedin_url,
                        discovery_method="apollo_similar",
                        discovery_url="https://apollo.io",
                        contact_confidence_score=0.75  # Slightly lower confidence for similar contacts
                    )
                    
                    # Determine which sales person this contact should be assigned to
                    from app.config import EMAIL_USERS
                    for email, org_types in EMAIL_USERS.items():
                        if organization.org_type in org_types:
                            contact.assigned_to = email
                            break
                    
                    self.db_session.add(contact)
                    self.db_session.commit()
                    
                    similar_contacts.append({
                        "id": contact.id,
                        "first_name": contact.first_name,
                        "last_name": contact.last_name,
                        "job_title": contact.job_title,
                        "email": contact.email,
                        "phone": contact.phone,
                        "organization_id": contact.organization_id,
                        "organization_name": organization.name
                    })
                    
                    logger.info(f"Created new similar contact: {contact.first_name} {contact.last_name} ({contact.job_title})")
                    
                    # Add delay to avoid rate limiting
                    time.sleep(self.delay_between_requests)
            
        except Exception as e:
            logger.error(f"Error finding similar contacts: {e}")
        
        return similar_contacts
    
    def discover_organizations(self, query: str, state: str, org_type: str = None, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Discover organizations using Apollo.io search.
        
        Args:
            query: Search query
            state: State to filter by
            org_type: Organization type to filter by
            limit: Maximum number of organizations to return
            
        Returns:
            List of discovered organizations
        """
        discovered_orgs = []
        page = 1
        per_page = min(25, limit)  # Maximum 25 per page
        
        while len(discovered_orgs) < limit:
            try:
                # Search for organizations
                results = self.search_organizations(
                    q=query,
                    org_type=org_type,
                    state=state,
                    page=page,
                    per_page=per_page
                )
                
                if not results or "organizations" not in results:
                    break
                
                organizations = results.get("organizations", [])
                
                if not organizations:
                    break
                
                # Process organizations
                for org in organizations:
                    # Extract organization data
                    org_data = {
                        "name": org.get("name", ""),
                        "website": org.get("website_url", ""),
                        "apollo_id": org.get("id", ""),
                        "description": org.get("short_description", ""),
                        "industry": org.get("industry", ""),
                        "employees_count": org.get("estimated_num_employees", 0),
                        "founded_year": org.get("founded_year"),
                        "address": org.get("organization_raw_address", ""),
                        "city": org.get("city", ""),
                        "state": state,
                        "phone": org.get("phone", ""),
                        "discovery_method": "apollo",
                        "discovery_query": query
                    }
                    
                    discovered_orgs.append(org_data)
                    
                    # Break if we've reached the limit
                    if len(discovered_orgs) >= limit:
                        break
                
                # Move to next page
                page += 1
                
                # Add delay to avoid rate limiting
                time.sleep(self.delay_between_requests)
                
            except Exception as e:
                logger.error(f"Error discovering organizations via Apollo: {e}")
                break
        
        return discovered_orgs[:limit]