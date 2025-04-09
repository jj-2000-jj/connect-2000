"""
Modified discovery manager for the GDS Contact Management System.

This module contains the enhanced DiscoveryManager class that focuses on extracting
real organizations from content rather than treating webpages as organizations.
"""
from datetime import datetime
import time
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session
from app.config import (
    TARGET_STATES, SEARCH_QUERIES, 
    ORG_TYPES, CLASSIFICATION_KEYWORDS, 
    INDUSTRY_DIRECTORIES, MIN_RELEVANCE_SCORE
)
from app.database.models import Organization, Contact, DiscoveredURL, SearchQuery, SystemMetric
from app.discovery.search_engine import SearchEngine
from app.discovery.crawler import Crawler
from app.discovery.organization_extractor import OrganizationExtractor
from app.discovery.fallback_contact_discovery import FallbackContactDiscovery
from app.validation.email_validator import EmailValidator
from app.utils.gemini_client import GeminiClient
from app.utils.logger import get_logger

logger = get_logger(__name__)

class DiscoveryManager:
    """
    Enhanced discovery manager that extracts real organizations from content.
    """
    
    def __init__(self, db_session: Session):
        """
        Initialize the discovery manager.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        # Initialize basic components
        self.search_engine = SearchEngine(db_session)
        self.crawler = Crawler(db_session)
        self.org_extractor = OrganizationExtractor(db_session)
        
        # Initialize email validator
        self.email_validator = EmailValidator(db_session)
        
        # Defer Gemini client and fallback discovery initialization until explicitly set up
        self.gemini_client = None
        self.fallback_discovery = None
        self.is_fully_setup = False
        
        # Initialize metrics
        self.metrics = {
            "organizations_discovered": 0,
            "contacts_discovered": 0,
            "urls_discovered": 0,
            "urls_crawled": 0,
            "by_source": {},
            "by_organization_type": {},
            "by_state": {}
        }
        
        logger.info("DiscoveryManager initialized with basic components (advanced components deferred)")
    
    def setup_advanced_components(self) -> bool:
        """
        Set up advanced components like the Gemini client and fallback discovery.
        This is called only when needed.
        
        Returns:
            True if setup was successful, False otherwise
        """
        if self.is_fully_setup:
            return True
            
        try:
            # Initialize Gemini client
            from app.config import GEMINI_API_KEY
            self.gemini_client = GeminiClient(GEMINI_API_KEY) if GEMINI_API_KEY else None
            
            # Initialize fallback discovery system
            self.fallback_discovery = FallbackContactDiscovery(
                self.email_validator, 
                self.gemini_client,
                self.db_session
            )
            
            self.is_fully_setup = True
            logger.info("DiscoveryManager advanced components set up successfully")
            return True
        except Exception as e:
            logger.error(f"Error setting up DiscoveryManager advanced components: {e}")
            return False
    
    def run_scheduled_discovery(self, max_orgs_per_run: int = 50, target_org_types: List[str] = None) -> Dict[str, Any]:
        """
        Run a scheduled discovery process.
        
        Args:
            max_orgs_per_run: Maximum number of organizations to discover
            target_org_types: Optional list of organization types to target
            
        Returns:
            Dictionary with discovery metrics
        """
        logger.info(f"Starting scheduled discovery run (max_orgs={max_orgs_per_run}, target_types={target_org_types})")
        start_time = time.time()
        
        try:
            # Reset metrics
            self.metrics = {
                "organizations_discovered": 0,
                "contacts_discovered": 0,
                "urls_discovered": 0,
                "urls_crawled": 0,
                "search_queries_executed": 0,
                "by_source": {},
                "by_organization_type": {},
                "by_state": {}
            }
            
            # Ensure advanced components are set up before proceeding
            if not self.is_fully_setup and not self.setup_advanced_components():
                logger.warning("Advanced components not fully set up. Some features may be limited.")
            
            # Run multi-stage discovery pipeline
            self._execute_search_phase(target_org_types)
            self._execute_crawl_phase()
            self._execute_contact_discovery_phase(max_contacts_per_org=10)
            
            # Check if we've reached the max organizations limit
            if self.metrics["organizations_discovered"] >= max_orgs_per_run:
                logger.info(f"Reached maximum organizations limit: {max_orgs_per_run}")
            
            # Calculate runtime
            runtime_seconds = int(time.time() - start_time)
            self.metrics["runtime_seconds"] = runtime_seconds
            
            # Save metrics to database
            self._save_metrics_to_database(runtime_seconds)
            
            logger.info(f"Discovery run completed in {runtime_seconds} seconds")
            logger.info(f"Found {self.metrics['organizations_discovered']} organizations and {self.metrics['contacts_discovered']} contacts")
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"Error in discovery run: {e}")
            return {"error": str(e)}
    
    def _execute_search_phase(self, target_org_types=None):
        """
        Execute the search phase of the discovery pipeline.
        
        Args:
            target_org_types: Optional list of organization types to target
        """
        logger.info(f"Starting search phase (target_org_types={target_org_types})")
        
        # If target_org_types is specified, filter the searches
        industries_to_process = {}
        if target_org_types and len(target_org_types) > 0:
            # Filter SEARCH_QUERIES to only include the targeted organization types
            for industry, queries in SEARCH_QUERIES.items():
                if industry in target_org_types:
                    industries_to_process[industry] = queries
            logger.info(f"Filtered to {len(industries_to_process)} organization types: {list(industries_to_process.keys())}")
        else:
            # Use all organization types
            industries_to_process = SEARCH_QUERIES
        
        # Iterate through target states and industry categories
        for state in TARGET_STATES:
            for industry, queries in industries_to_process.items():
                # Log how many organizations we have for this industry/state but don't skip
                existing_count = self.db_session.query(Organization).filter(
                    Organization.state == state,
                    Organization.org_type == industry
                ).count()
                
                logger.info(f"Processing {industry} in {state} - have {existing_count} existing organizations")
                
                # Execute search queries for this state and industry
                for query_template in queries:
                    # Format query with state
                    query = query_template.format(state=state)
                    
                    # Execute search
                    logger.info(f"Executing search: {query}")
                    search_results = self.search_engine.execute_search(query, industry, state)
                    
                    # Process search results
                    self._process_search_results(search_results, state)
                    
                    # Update metrics
                    self.metrics["search_queries_executed"] += 1
                    self.metrics["urls_discovered"] += len(search_results)
                    
                    # Record URL discoveries
                    for result in search_results:
                        # Check if result is a dictionary and has a URL
                        url = None
                        title = ""
                        snippet = ""
                        
                        if isinstance(result, dict):
                            # Handle different result formats
                            url = result.get("url") or result.get("link")
                            title = result.get("title", "")
                            snippet = result.get("snippet", "")
                        
                        if not url:
                            logger.warning(f"Skipping search result without URL: {result}")
                            continue
                        
                        # Save URL to database
                        url_record = self._save_discovered_url(url, title, snippet, "search", industry)
                        
                        # Download and process content
                        content = self.crawler.download_url(url)
                        if content:
                            # Extract organizations from content
                            org_ids = self.org_extractor.process_discovered_url(self.db_session, url_record, content, state)
                            
                            # Update metrics
                            self.metrics["organizations_discovered"] += len(org_ids)
                            
                            # Update source metrics
                            source = "search_engine"
                            self.metrics["by_source"][source] = self.metrics["by_source"].get(source, 0) + len(org_ids)
                            
                            # Update organization type metrics
                            self.metrics["by_organization_type"][industry] = self.metrics["by_organization_type"].get(industry, 0) + len(org_ids)
                            
                            # Update state metrics
                            self.metrics["by_state"][state] = self.metrics["by_state"].get(state, 0) + len(org_ids)
        
        logger.info(f"Search phase completed. Discovered {self.metrics['urls_discovered']} URLs")
    
    def _process_search_results(self, results: List[Dict[str, Any]], state_context: str):
        """
        Process search results to extract URLs and initial organization data.
        
        Args:
            results: List of search result dictionaries
            state_context: State used in the search query
        """
        for result in results:
            # Extract URL and metadata
            url = None
            title = ""
            snippet = ""
            
            if isinstance(result, dict):
                # Handle different result formats
                url = result.get("url") or result.get("link")
                title = result.get("title", "")
                snippet = result.get("snippet", "")
            
            if not url:
                continue
                
            # Check if URL already exists
            existing_url = self.db_session.query(DiscoveredURL).filter(
                DiscoveredURL.url == url
            ).first()
            
            if existing_url:
                # URL already discovered
                continue
            
            # Create new URL record
            url_record = DiscoveredURL(
                url=url,
                title=title,
                description=snippet,
                page_type="search_result",
                priority_score=0.8  # High priority for search results
            )
            
            self.db_session.add(url_record)
            self.db_session.commit()
    
    def _execute_crawl_phase(self):
        """Execute the crawling phase of the discovery pipeline."""
        logger.info("Starting crawl phase")
        
        # Get high priority URLs that haven't been crawled yet
        urls_to_crawl = self.db_session.query(DiscoveredURL).filter(
            DiscoveredURL.last_crawled.is_(None),  # Not crawled yet
            DiscoveredURL.priority_score >= 0.5     # Medium-high priority
        ).order_by(
            DiscoveredURL.priority_score.desc()     # Highest priority first
        ).limit(100).all()  # Process in batches
        
        logger.info(f"Found {len(urls_to_crawl)} URLs to crawl")
        
        # Crawl each URL
        for url_record in urls_to_crawl:
            try:
                # Crawl URL
                content, links = self.crawler.crawl_url(url_record.url)
                
                if content:
                    # Update URL record
                    url_record.last_crawled = datetime.utcnow()
                    self.db_session.commit()
                    
                    # Extract organizations from content
                    org_ids = self.org_extractor.process_discovered_url(self.db_session, url_record, content)
                    
                    # Update metrics
                    self.metrics["organizations_discovered"] += len(org_ids)
                    self.metrics["urls_crawled"] += 1
                    
                    # Update source metrics
                    source = "crawler"
                    self.metrics["by_source"][source] = self.metrics["by_source"].get(source, 0) + len(org_ids)
                    
                    # Save discovered links
                    for link in links:
                        self._save_discovered_url(link, "", "", "crawler", "")
                        self.metrics["urls_discovered"] += 1
            
            except Exception as e:
                logger.error(f"Error crawling URL {url_record.url}: {e}")
        
        logger.info(f"Crawl phase completed. Crawled {self.metrics['urls_crawled']} URLs")
    
    def _save_discovered_url(self, url: str, title: str, description: str, source: str, category: str) -> DiscoveredURL:
        """
        Save a discovered URL to the database.
        
        Args:
            url: URL string
            title: Page title
            description: Page description
            source: Source of discovery (search, crawler, etc.)
            category: Category/industry
            
        Returns:
            DiscoveredURL record
        """
        # Check if URL already exists
        existing_url = self.db_session.query(DiscoveredURL).filter(
            DiscoveredURL.url == url
        ).first()
        
        if existing_url:
            return existing_url
        
        # Create new URL record
        url_record = DiscoveredURL(
            url=url,
            title=title,
            description=description,
            page_type=source,
            priority_score=0.7 if source == "search" else 0.5
        )
        
        self.db_session.add(url_record)
        self.db_session.commit()
        
        return url_record
    
    def _execute_contact_discovery_phase(self, max_contacts_per_org: int = 10):
        """
        Execute the contact discovery phase.
        
        Args:
            max_contacts_per_org: Maximum contacts to discover per organization
        """
        logger.info("Starting contact discovery phase")
        
        from sqlalchemy import func, and_
        from app.database.models import Contact
        
        # Get count of contacts per organization
        contact_counts = self.db_session.query(
            Contact.organization_id, 
            func.count(Contact.id).label('contact_count')
        ).group_by(Contact.organization_id).subquery()
        
        # First priority: organizations with 0 contacts
        zero_contact_orgs = self.db_session.query(Organization).outerjoin(
            contact_counts, Organization.id == contact_counts.c.organization_id
        ).filter(
            and_(
                Organization.relevance_score >= MIN_RELEVANCE_SCORE,
                contact_counts.c.contact_count == None  # Organizations with no contacts
            )
        ).all()
        
        # Second priority: organizations with "partial" status (< 5 actual contacts)
        partial_orgs = self.db_session.query(Organization).join(
            contact_counts, Organization.id == contact_counts.c.organization_id
        ).filter(
            and_(
                Organization.relevance_score >= MIN_RELEVANCE_SCORE,
                Organization.contact_discovery_status == "partial"  # Partial completion status
            )
        ).all()
        
        # Third priority: organizations with "attempted" status but no contacts
        attempted_orgs = self.db_session.query(Organization).join(
            contact_counts, Organization.id == contact_counts.c.organization_id
        ).filter(
            and_(
                Organization.relevance_score >= MIN_RELEVANCE_SCORE,
                Organization.contact_discovery_status == "attempted",
                contact_counts.c.contact_count < 3
            )
        ).all()
        
        # Fourth priority: any other relevant organizations that don't have enough contacts
        other_relevant_orgs = self.db_session.query(Organization).join(
            contact_counts, Organization.id == contact_counts.c.organization_id
        ).filter(
            and_(
                Organization.relevance_score >= MIN_RELEVANCE_SCORE,
                Organization.contact_discovery_status != "completed",  # Skip completed
                contact_counts.c.contact_count < max_contacts_per_org,
                ~Organization.id.in_([org.id for org in zero_contact_orgs + partial_orgs + attempted_orgs])  # Exclude already included orgs
            )
        ).all()
        
        # Combine with priority order
        relevant_orgs = zero_contact_orgs + partial_orgs + attempted_orgs + other_relevant_orgs
        
        logger.info(f"Found {len(relevant_orgs)} relevant organizations for contact discovery:")
        logger.info(f"  - {len(zero_contact_orgs)} organizations with 0 contacts")
        logger.info(f"  - {len(partial_orgs)} organizations with partial contact discovery (< 5 actual contacts)")
        logger.info(f"  - {len(attempted_orgs)} organizations previously attempted but with few contacts")
        logger.info(f"  - {len(other_relevant_orgs)} other relevant organizations")
        
        # Create role profiles for each organization type
        taxonomy = self._generate_organization_taxonomy()
        role_profiles = self._create_role_profiles(taxonomy)
        
        # Discover contacts for each organization (already prioritized order)
        total_contacts = 0
        processed_orgs = 0
        for org in relevant_orgs:
            # Get existing contact count for reporting
            existing_contacts = self.db_session.query(Contact).filter(
                Contact.organization_id == org.id
            ).count()
            
            # Log with contact counts to show prioritization is working
            logger.info(f"Processing organization: {org.name} - {org.org_type} ({existing_contacts} existing contacts)")
            processed_orgs += 1
            
            # Get relevant role profiles for this organization type
            profiles = role_profiles.get(org.org_type, [])
            
            # Skip organizations with no matching profiles
            if not profiles:
                logger.warning(f"No role profiles found for organization type: {org.org_type}")
                continue
            
            # Discover contacts
            contacts = self._discover_contacts_for_organization(org, profiles)
            contact_count = len(contacts)
            total_contacts += contact_count
            
            # Count actual contacts (not generated or inferred)
            actual_contact_count = 0
            for contact in contacts:
                # Check if this is an actual contact (not generated or inferred)
                is_actual = (
                    contact.first_name and 
                    contact.last_name and 
                    contact.discovery_method not in ['generic_role_contact', 'inferred', 'title_based_pattern']
                )
                if is_actual:
                    actual_contact_count += 1
            
            # Update organization contact discovery status based on actual contact count
            if actual_contact_count >= 5:
                org.contact_discovery_status = "completed"
                logger.info(f"Discovered {contact_count} contacts ({actual_contact_count} actual) for {org.name} - COMPLETED")
            elif contact_count > 0:
                org.contact_discovery_status = "partial"
                logger.info(f"Discovered {contact_count} contacts ({actual_contact_count} actual) for {org.name} - PARTIAL")
            else:
                org.contact_discovery_status = "attempted"
                logger.warning(f"No contacts found for {org.name} - ATTEMPTED")
            
            # Always update last discovery timestamp to avoid reprocessing failing organizations
            org.last_contact_discovery = datetime.now()
            self.db_session.commit()
            
            # Update metrics
            self.metrics["contacts_discovered"] += contact_count
            
            # Update organization type metrics
            org_type = org.org_type
            self.metrics["by_organization_type"][org_type] = self.metrics["by_organization_type"].get(org_type, 0) + 1
            
            # Update state metrics
            state = org.state
            if state:
                self.metrics["by_state"][state] = self.metrics["by_state"].get(state, 0) + 1
        
        logger.info(f"Contact discovery phase completed. Found {total_contacts} contacts")
    
    def _generate_organization_taxonomy(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Generate organization taxonomy.
        
        Returns:
            Dictionary with organization taxonomy
        """
        # Use the predefined organization taxonomy from config
        taxonomy = {}
        
        for org_type, details in ORG_TYPES.items():
            taxonomy[org_type] = {
                "subtypes": details.get("subtypes", []),
                "job_titles": details.get("job_titles", []),
                "relevance_criteria": details.get("relevance_criteria", [])
            }
        
        return taxonomy
    
    def _create_role_profiles(self, taxonomy: Dict[str, Dict[str, List[str]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Create role profiles for each organization type.
        
        Args:
            taxonomy: Organization taxonomy dictionary
            
        Returns:
            Dictionary with role profiles by organization type
        """
        profiles = {}
        
        for org_type, details in taxonomy.items():
            org_profiles = []
            
            for job_title in details.get("job_titles", []):
                profile = {
                    "title": job_title,
                    "relevance_score": 8 if "Manager" in job_title or "Director" in job_title else 6,
                    "decision_making_level": 8 if "Director" in job_title or "VP" in job_title else 6,
                    "technical_knowledge": 9 if "Engineer" in job_title or "Technical" in job_title else 5,
                    "synonyms": self._generate_title_synonyms(job_title)
                }
                
                org_profiles.append(profile)
            
            profiles[org_type] = org_profiles
        
        return profiles
    
    def _generate_title_synonyms(self, job_title: str) -> List[str]:
        """
        Generate synonyms for a job title.
        
        Args:
            job_title: Original job title
            
        Returns:
            List of synonym titles
        """
        synonyms = []
        
        # Common prefix substitutions
        prefixes = {
            "Director": ["Head of", "Chief", "Lead", "Senior"],
            "Manager": ["Lead", "Head", "Supervisor", "Coordinator"],
            "Engineer": ["Specialist", "Technician", "Analyst", "Technologist"],
            "Technician": ["Specialist", "Operator", "Technologist"]
        }
        
        # Common suffix substitutions
        suffixes = {
            "Operations": ["Systems", "Facilities", "Plant", "Production"],
            "Manager": ["Supervisor", "Lead", "Coordinator"],
            "Engineer": ["Specialist", "Professional", "Officer"],
            "Director": ["Manager", "Supervisor", "Head", "Chief"]
        }
        
        # Generate prefix variations
        for prefix, alternatives in prefixes.items():
            if job_title.startswith(prefix):
                remainder = job_title[len(prefix):].strip()
                for alt in alternatives:
                    synonyms.append(f"{alt}{remainder}")
        
        # Generate suffix variations
        for suffix, alternatives in suffixes.items():
            if job_title.endswith(suffix):
                base = job_title[:-len(suffix)].strip()
                for alt in alternatives:
                    synonyms.append(f"{base}{alt}")
        
        return synonyms
    
    def _discover_contacts_for_organization(self, organization: Organization, profiles: List[Dict[str, Any]]) -> List[Contact]:
        """
        Discover contacts for an organization based on role profiles.
        
        Args:
            organization: Organization record
            profiles: List of role profiles
            
        Returns:
            List of discovered contacts
        """
        logger.info(f"Discovering contacts for {organization.name}")
        
        discovered_contacts = []
        
        # Check for organization website
        if not organization.website:
            logger.warning(f"No website available for {organization.name}")
            return discovered_contacts
        
        try:
            # Crawl the organization website
            result = self.crawler.crawl_url(organization.website)
            
            # Extract content and links from the result dictionary
            content = result.get("html_content", "")
            links = result.get("links", [])
            
            if not content:
                logger.warning(f"Could not retrieve content for {organization.website}")
                return discovered_contacts
            
            # Extract contacts from content
            raw_contacts = self._extract_contacts_from_content(content, organization, profiles)
            
            # Process and validate contacts
            for contact_data in raw_contacts:
                # Check if contact already exists
                from app.database import crud
                if crud.contact_exists(
                    self.db_session, 
                    contact_data.get("first_name", ""), 
                    contact_data.get("last_name", ""), 
                    organization.id
                ):
                    continue
                
                # Create new contact
                contact = Contact(
                    organization_id=organization.id,
                    first_name=contact_data.get("first_name", ""),
                    last_name=contact_data.get("last_name", ""),
                    job_title=contact_data.get("job_title", ""),
                    email=contact_data.get("email", ""),
                    phone=contact_data.get("phone", ""),
                    discovery_method="website",
                    discovery_url=organization.website,
                    contact_confidence_score=contact_data.get("confidence", 0.7),
                    contact_relevance_score=contact_data.get("relevance", 7.0),
                    notes=contact_data.get("notes", "")
                )
                
                # Assign contact to appropriate user based on organization type
                from app.utils.contact_assigner import assign_contact_to_user
                assign_contact_to_user(contact, organization)
                
                self.db_session.add(contact)
                self.db_session.commit()
                
                discovered_contacts.append(contact)
            
            # Follow any staff/team/about links to find more contacts
            for link in links:
                # Check if link contains keywords suggesting contact information
                link_lower = link.lower()
                if any(term in link_lower for term in ["team", "staff", "about", "people", "leadership", "contact", "directory"]):
                    try:
                        # Crawl the link
                        result = self.crawler.crawl_url(link)
                        
                        # Extract content from the result dictionary
                        link_content = result.get("html_content", "")
                        
                        if link_content:
                            # Extract contacts
                            link_contacts = self._extract_contacts_from_content(link_content, organization, profiles)
                            
                            # Process and validate contacts
                            for contact_data in link_contacts:
                                # Check if contact already exists
                                if crud.contact_exists(
                                    self.db_session, 
                                    contact_data.get("first_name", ""), 
                                    contact_data.get("last_name", ""), 
                                    organization.id
                                ):
                                    continue
                                
                                # Create new contact
                                contact = Contact(
                                    organization_id=organization.id,
                                    first_name=contact_data.get("first_name", ""),
                                    last_name=contact_data.get("last_name", ""),
                                    job_title=contact_data.get("job_title", ""),
                                    email=contact_data.get("email", ""),
                                    phone=contact_data.get("phone", ""),
                                    discovery_method="website_secondary",
                                    discovery_url=link,
                                    contact_confidence_score=contact_data.get("confidence", 0.7),
                                    contact_relevance_score=contact_data.get("relevance", 7.0),
                                    notes=contact_data.get("notes", "")
                                )
                                
                                # Assign contact to appropriate user based on organization type
                                from app.utils.contact_assigner import assign_contact_to_user
                                assign_contact_to_user(contact, organization)
                                
                                self.db_session.add(contact)
                                self.db_session.commit()
                                
                                discovered_contacts.append(contact)
                    
                    except Exception as e:
                        logger.error(f"Error crawling contact link {link}: {e}")
            
            # Always perform position-based searches to find role-specific contacts
            # Prepare organization data for discovery
            org_data = {
                "name": organization.name,
                "website": organization.website,
                "org_type": organization.org_type,
                "state": organization.state,
                "city": organization.city or "",
                "location": f"{organization.city}, {organization.state}" if organization.city else organization.state
            }
            
            # 1. First, always perform role/title specific searches to find key personnel
            logger.info(f"Performing position-based search for {organization.name}")
            position_contacts_data = self.fallback_discovery.discover_by_position(
                org_data["name"], 
                org_data["org_type"], 
                org_data["location"], 
                org_data["state"]
            )
            
            if position_contacts_data:
                logger.info(f"Found {len(position_contacts_data)} contacts via position-based search")
                self.metrics["by_source"]["position_search"] = self.metrics["by_source"].get("position_search", 0) + len(position_contacts_data)
            
            # 2. Then check if we need additional fallback discovery
            actual_contacts = [c for c in discovered_contacts if c.first_name and c.last_name and c.discovery_method != 'inferred']
            has_enough_contacts = len(actual_contacts) >= 3
            
            # Determine if we need fallback discovery beyond just position-based search
            if not has_enough_contacts and len(position_contacts_data) < 3:
                logger.info(f"Need more contacts for {organization.name}, trying additional fallback discovery")
                
                # Use full fallback discovery to find more contacts (which includes email inference)
                fallback_contacts_data = self.fallback_discovery.discover_contacts(
                    org_data,
                    min_contacts=3 - len(actual_contacts)  # Only get what we need
                )
            else:
                # Use just the position-based contacts
                fallback_contacts_data = position_contacts_data
                
                # Process the fallback contacts
                # First, prioritize actual contacts with names over generic ones
                real_contacts = [c for c in fallback_contacts_data if c.get("first_name") and c.get("last_name") and not c.get("is_generic", False)]
                generic_contacts = [c for c in fallback_contacts_data if c.get("is_generic", False)]
                
                # Sort by confidence score within each group
                real_contacts.sort(key=lambda x: x.get("confidence_score", 0), reverse=True)
                prioritized_contacts = real_contacts + generic_contacts
                
                # Process in priority order
                from app.database import crud
                for contact_data in prioritized_contacts:
                    # Check if contact already exists
                    first_name = contact_data.get("first_name", "")
                    last_name = contact_data.get("last_name", "")
                    
                    if first_name and last_name:
                        if crud.contact_exists(self.db_session, first_name, last_name, organization.id):
                            continue
                    elif contact_data.get("email"):
                        # Check by email if no name
                        if crud.contact_exists_by_email(self.db_session, contact_data.get("email"), organization.id):
                            continue
                    else:
                        # Skip if we can't uniquely identify the contact
                        continue
                        
                    # Keep track of whether this is an actual vs generic contact
                    is_generic = contact_data.get("is_generic", False) or not (first_name and last_name)
                    
                    # Create new contact - adjust confidence based on whether it's generic
                    confidence_score = contact_data.get("confidence_score", 0.6)
                    if is_generic:
                        confidence_score = min(confidence_score, 0.5)  # Cap generic contact confidence
                        
                    discovery_method = contact_data.get("discovery_method", "fallback_discovery")
                    if is_generic:
                        discovery_method = "generic_role_contact"
                        
                    notes = contact_data.get("notes", "")
                    if not notes:
                        if is_generic:
                            notes = "Generic role-based contact (no specific individual identified)"
                        else:
                            notes = "Discovered using fallback discovery methods"
                            
                    contact = Contact(
                        organization_id=organization.id,
                        first_name=contact_data.get("first_name", ""),
                        last_name=contact_data.get("last_name", ""),
                        job_title=contact_data.get("job_title", ""),
                        email=contact_data.get("email", ""),
                        phone=contact_data.get("phone", ""),
                        discovery_method=discovery_method,
                        discovery_url=contact_data.get("source_url", organization.website),
                        contact_confidence_score=confidence_score,
                        contact_relevance_score=contact_data.get("relevance_score", 6.0),
                        notes=notes
                    )
                    
                    # Assign contact to appropriate user based on organization type
                    from app.utils.contact_assigner import assign_contact_to_user
                    assign_contact_to_user(contact, organization)
                    
                    self.db_session.add(contact)
                    self.db_session.commit()
                    
                    discovered_contacts.append(contact)
                
                # Update metrics with a breakdown of real vs generic contacts
                real_contacts_added = len([c for c in fallback_contacts_data if c.get("first_name") and c.get("last_name") and not c.get("is_generic", False)])
                generic_contacts_added = len([c for c in fallback_contacts_data if c.get("is_generic", False)])
                
                if fallback_contacts_data:
                    logger.info(f"Found {len(fallback_contacts_data)} additional contacts using fallback discovery ({real_contacts_added} real, {generic_contacts_added} generic)")
                    self.metrics["by_source"]["fallback_discovery"] = self.metrics["by_source"].get("fallback_discovery", 0) + real_contacts_added
                    self.metrics["by_source"]["generic_contacts"] = self.metrics["by_source"].get("generic_contacts", 0) + generic_contacts_added
            
            return discovered_contacts
        
        except Exception as e:
            logger.error(f"Error discovering contacts for {organization.name}: {e}")
            
            # Even if the regular process failed, try fallback discovery as a last resort
            try:
                if organization.website:
                    logger.info(f"Attempting fallback discovery after error for {organization.name}")
                    
                    # Prepare organization data for fallback discovery
                    org_data = {
                        "name": organization.name,
                        "website": organization.website,
                        "org_type": organization.org_type,
                        "state": organization.state,
                        "city": organization.city or "",
                        "location": f"{organization.city}, {organization.state}" if organization.city else organization.state
                    }
                    
                    # Use fallback discovery
                    fallback_contacts_data = self.fallback_discovery.discover_contacts(org_data, min_contacts=3)
                    
                    # Process the fallback contacts (simplified to avoid repeating too much code)
                    for contact_data in fallback_contacts_data:
                        contact = Contact(
                            organization_id=organization.id,
                            first_name=contact_data.get("first_name", ""),
                            last_name=contact_data.get("last_name", ""),
                            job_title=contact_data.get("job_title", ""),
                            email=contact_data.get("email", ""),
                            phone=contact_data.get("phone", ""),
                            discovery_method="fallback_after_error",
                            discovery_url=contact_data.get("source_url", organization.website),
                            contact_confidence_score=contact_data.get("confidence_score", 0.5),
                            contact_relevance_score=contact_data.get("relevance_score", 5.0),
                            notes="Discovered using fallback methods after primary discovery error"
                        )
                        
                        # Assign contact to appropriate user based on organization type
                        from app.utils.contact_assigner import assign_contact_to_user
                        assign_contact_to_user(contact, organization)
                        
                        self.db_session.add(contact)
                        discovered_contacts.append(contact)
                    
                    if discovered_contacts:
                        self.db_session.commit()
                        
                    logger.info(f"Fallback discovery found {len(discovered_contacts)} contacts after error")
                    return discovered_contacts
            except Exception as fallback_error:
                logger.error(f"Fallback discovery also failed for {organization.name}: {fallback_error}")
            
            return discovered_contacts
    
    def _extract_contacts_from_content(self, content: str, organization: Organization, profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract contacts from content using Gemini API with fallback to regex-based extraction.
        
        Args:
            content: HTML content
            organization: Organization record
            profiles: List of role profiles
            
        Returns:
            List of contact dictionaries
        """
        contacts = []
        structured_contacts = []
        text_contacts = []
        fallback_contacts = []
        
        # Try structured contact extraction (using HTML patterns)
        structured_contacts = self._extract_structured_contacts(content)
        if structured_contacts:
            logger.info(f"Found {len(structured_contacts)} structured contacts from {organization.website}")
            contacts.extend(structured_contacts)
        
        # Try text-based extraction (using text patterns)
        text_contacts = self._extract_text_contacts(content)
        if text_contacts:
            logger.info(f"Found {len(text_contacts)} text contacts from {organization.website}")
            contacts.extend(text_contacts)
            
        # Always continue to try Gemini API, even if we found contacts through simpler methods
            
        # Otherwise try the Gemini API method
        try:
            # Convert HTML to text
            text = self.org_extractor.html_to_text(content)
            
            # Truncate text if too long (Gemini has token limits)
            if len(text) > 15000:
                text = text[:15000]
            
            # Create role titles list for the prompt
            role_titles = []
            for profile in profiles:
                role_titles.append(profile["title"])
                role_titles.extend(profile.get("synonyms", []))
            
            # Create the prompt
            prompt = f"""
            Extract contact information for individuals at {organization.name} with roles related to SCADA systems, 
            water management, automation, or operations from the following text.
            
            Focus on finding people with these job titles or similar roles:
            {', '.join(role_titles)}
            
            For each contact identified, extract:
            1. First name
            2. Last name
            3. Job title (exact as listed)
            4. Email address (if available)
            5. Phone number (if available)
            6. Confidence score (0.0-1.0) that this is a real contact with accurate information
            7. Relevance score (1-10) for how relevant this person is for SCADA integration (based on their role)
            
            Only extract real people and information. If information is missing, leave it blank rather than guessing.
            
            TEXT:
            {text}
            
            Format the response as a JSON array of objects with these exact fields:
            first_name, last_name, job_title, email, phone, confidence, relevance, notes
            
            If no valid contacts are found, return an empty array.
            """
            
            # Call Gemini API
            import google.generativeai as genai
            from app.config import GEMINI_API_KEY
            import json
            import re
            
            # Initialize API if needed
            genai.configure(api_key=GEMINI_API_KEY)
            
            # Add rate limiting - sleep for 1 second before API call
            import time
            time.sleep(1)
            
            # Call the API with a timeout
            import concurrent.futures
            
            def call_gemini():
                model = genai.GenerativeModel('gemini-2.0-flash')
                return model.generate_content(prompt)
                
            # Use executor to implement timeout
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(call_gemini)
                try:
                    # Increase timeout to 20 seconds
                    response = future.result(timeout=20)
                    
                    # Process response text
                    response_text = response.text
                    
                    # Find JSON in response
                    json_match = re.search(r'\[\s*{.*}\s*\]', response_text, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                    else:
                        # Try to find anything that looks like JSON
                        json_str = response_text
                    
                    # Parse the JSON response
                    gemini_contacts = json.loads(json_str)
                    
                    # Don't filter out contacts by confidence score
                    valid_contacts = gemini_contacts
                    
                    if valid_contacts:
                        logger.info(f"Found {len(valid_contacts)} contacts via Gemini API from {organization.website}")
                        contacts.extend(valid_contacts)
                        
                except concurrent.futures.TimeoutError:
                    logger.error(f"Gemini API timed out for {organization.name}")
                    # Stop the operation instead of falling back
                    raise Exception(f"Gemini API timeout for {organization.name} - stopping operation")
                except Exception as e:
                    logger.error(f"Error extracting contacts with Gemini API: {e}")
                    # Stop the operation when API fails
                    raise
        
        except Exception as e:
            logger.error(f"Error in Gemini extraction setup: {e}")
            # Stop the operation when Gemini API fails
            raise
                
        logger.info(f"Found {len(structured_contacts)} structured contacts, {len(text_contacts)} text contacts from {organization.website}")
        
        return contacts
        
    def _extract_structured_contacts(self, html_content: str) -> List[Dict[str, Any]]:
        """Extract contacts from structured HTML content (team pages, staff listings, etc.)"""
        contacts = []
        
        # Parse HTML
        from bs4 import BeautifulSoup
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for structured patterns like staff listings, team pages, etc.
            # This is a simplified implementation - a real one would be more sophisticated
            
            return contacts
        except Exception as e:
            logger.error(f"Error extracting structured contacts: {e}")
            return []
            
    def _extract_text_contacts(self, html_content: str) -> List[Dict[str, Any]]:
        """Extract contacts using text patterns (regexes for emails, names, titles, etc.)"""
        contacts = []
        
        # Parse HTML to text
        text = self.org_extractor.html_to_text(html_content)
        
        # Look for email patterns
        import re
        email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
        
        # Find all emails
        emails = re.findall(email_pattern, text)
        
        # For each email, try to extract other information
        for email in emails:
            # Simple extraction - a real implementation would be more sophisticated
            contact = {
                "email": email,
                "first_name": "",
                "last_name": "",
                "job_title": "",
                "phone": "",
                "confidence": 0.75,
                "relevance": 7.0,
                "notes": "Extracted from website text patterns"
            }
            
            # Add if it's not already in the list
            if not any(c.get("email") == email for c in contacts):
                contacts.append(contact)
                
        return contacts
        
    def _extract_fallback_contacts(self, html_content: str, org_name: str) -> List[Dict[str, Any]]:
        """Basic fallback to find any possible contact information"""
        contacts = []
        
        # Extract general contact information
        from bs4 import BeautifulSoup
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for "Contact Us" text
            contact_elements = soup.find_all(text=lambda text: "contact" in text.lower() if text else False)
            
            if contact_elements:
                # Very simple fallback - in a real implementation this would be more sophisticated
                contact = {
                    "first_name": "Contact",
                    "last_name": f"For{org_name.replace(' ', '')}",
                    "job_title": "Contact Person",
                    "email": "",
                    "phone": "",
                    "confidence": 0.5,  # Lower confidence for fallback
                    "relevance": 5.0,   # Lower relevance for fallback
                    "notes": "Fallback contact from contact page"
                }
                contacts.append(contact)
                
            return contacts
        except Exception as e:
            logger.error(f"Error extracting fallback contacts: {e}")
            return []
    
    def _save_metrics_to_database(self, runtime_seconds: int):
        """
        Save discovery metrics to the database.
        
        Args:
            runtime_seconds: Runtime in seconds
        """
        try:
            # Create metric record
            metric = SystemMetric(
                urls_discovered=self.metrics["urls_discovered"],
                urls_crawled=self.metrics["urls_crawled"],
                organizations_discovered=self.metrics["organizations_discovered"],
                contacts_discovered=self.metrics["contacts_discovered"],
                search_queries_executed=self.metrics["search_queries_executed"],
                runtime_seconds=runtime_seconds
            )
            
            self.db_session.add(metric)
            self.db_session.commit()
            
            logger.info(f"Saved metrics to database")
        
        except Exception as e:
            logger.error(f"Error saving metrics to database: {e}")