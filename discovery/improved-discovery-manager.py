"""
Enhanced Discovery Manager for the GBL Data Contact Management System.

This module implements an improved discovery pipeline that better identifies
organizations with SCADA integration needs, rather than SCADA providers.
"""
import datetime
import json
import time
import random
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from app.database.models import (
    Organization, Contact, DiscoveredURL, SearchQuery, 
    SystemMetric, DiscoveryCheckpoint, RoleProfile
)
from app.utils.logger import get_logger
from app.config import (
    TARGET_STATES, ILLINOIS_SOUTH_OF_I80, IMPROVED_SEARCH_QUERIES,
    INFRASTRUCTURE_PROCESS_KEYWORDS, COMPETITOR_INDICATORS
)

# Define constants needed for the implementation
DEFAULT_MAX_ORGS_PER_RUN = 50
MIN_RELEVANCE_SCORE = 7.0
CHECKPOINT_DIR = Path("data/checkpoints")
CHECKPOINT_DIR.mkdir(exist_ok=True, parents=True)

# Mock variables required by the implementation
ORG_RELEVANCE_INDICATORS = {
    "water": {
        "infrastructure": ["treatment plant", "distribution system", "pump station"],
        "processes": ["water treatment", "monitoring", "disinfection"]
    },
    "wastewater": {
        "infrastructure": ["treatment plant", "lift station", "collection system"],
        "processes": ["wastewater treatment", "solids handling", "aeration"]
    }
}
# This is a mock class for testing
class EnhancedOrganizationRanker:
    def __init__(self, db_session):
        self.db_session = db_session
        
    def calculate_relevance_score(self, org_type, text_content, org_info):
        return 7.5, {
            "infrastructure_matches": ["pump station", "control system"], 
            "process_matches": ["water treatment", "monitoring"],
            "operational_challenges": ["remote monitoring"],
            "regulatory_requirements": ["compliance reporting"],
            "competitor_indicators": [],
            "is_likely_competitor": False
        }

logger = get_logger(__name__)

class ImprovedDiscoveryManager:
    """
    Enhanced discovery manager with improved organization detection and ranking.
    """
    
    def __init__(self, db_session):
        """
        Initialize the discovery manager.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        self.org_ranker = EnhancedOrganizationRanker(db_session)
        self.checkpoints = {}
        
        # Create checkpoint directory if it doesn't exist
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        
    def run_scheduled_discovery(self, max_orgs_per_run: int = DEFAULT_MAX_ORGS_PER_RUN) -> Dict[str, Any]:
        """
        Run scheduled discovery process.
        
        Args:
            max_orgs_per_run: Maximum number of organizations to discover
            
        Returns:
            Dictionary with metrics
        """
        logger.info(f"Starting scheduled discovery with max_orgs={max_orgs_per_run}")
        
        start_time = time.time()
        
        # Initialize metrics
        metrics = {
            "organizations_discovered": 0,
            "contacts_discovered": 0,
            "high_relevance_orgs": 0,
            "by_source": {},
            "by_organization_type": {},
            "by_state": {}
        }
        
        try:
            # Reset checkpoints for this run
            self._reset_checkpoints()
            
            # Step 1: Generate search queries for target industries and states
            search_queries = self._generate_search_queries()
            self._save_checkpoint("search_queries_generated", {"count": len(search_queries)})
            
            # Step 2: Execute search queries and extract potential organizations
            potential_orgs = self._execute_search_queries(search_queries)
            self._save_checkpoint("search_queries_executed", {"count": len(potential_orgs)})
            
            # Step 3: Filter and prioritize organizations
            filtered_orgs = self._filter_and_prioritize_organizations(potential_orgs, max_orgs_per_run)
            self._save_checkpoint("organizations_filtered", {"count": len(filtered_orgs)})
            
            # Step 4: Process organizations to get detailed information
            processed_orgs = self._process_organizations(filtered_orgs)
            self._save_checkpoint("organizations_processed", {"count": len(processed_orgs)})
            
            # Step 5: Create organization profiles in database
            created_orgs = self._create_organization_profiles(processed_orgs)
            self._save_checkpoint("organization_profiles_created", {"count": len(created_orgs)})
            
            # Update metrics
            metrics["organizations_discovered"] = len(created_orgs)
            
            # Step 6: Crawl organization websites to discover more information
            crawled_orgs = self._crawl_organization_websites(created_orgs)
            self._save_checkpoint("websites_crawled", {"count": len(crawled_orgs)})
            
            # Step 7: Score and rank organizations for relevance
            ranked_orgs = self._score_and_rank_organizations(crawled_orgs)
            self._save_checkpoint("organizations_ranked", {"count": len(ranked_orgs)})
            
            # Count high relevance organizations
            metrics["high_relevance_orgs"] = sum(1 for org in ranked_orgs if org["relevance_score"] >= MIN_RELEVANCE_SCORE)
            
            # Step 8: Discover contacts for high-relevance organizations
            if ranked_orgs:
                # Create role profiles for each organization type
                taxonomy = self._generate_organization_taxonomy()
                role_profiles = self._create_role_profiles(taxonomy)
                self._save_checkpoint("role_profiles_created")
                
                # Focus on high-relevance organizations for contact discovery
                high_relevance_orgs = [org for org in ranked_orgs if org["relevance_score"] >= MIN_RELEVANCE_SCORE]
                
                # Discover contacts for high-relevance organizations
                all_contacts = []
                for org_data in high_relevance_orgs:
                    org_id = org_data["id"]
                    org_type = org_data["org_type"]
                    
                    # Get organization object
                    org = self.db_session.query(Organization).filter(Organization.id == org_id).first()
                    if not org:
                        continue
                    
                    # Get profiles for this organization type
                    profiles = role_profiles.get(org_type, [])
                    if not profiles:
                        logger.warning(f"No role profiles found for organization type: {org_type}")
                        continue
                    
                    # Discover contacts
                    contacts = self._discover_contacts_for_organization(org, profiles)
                    all_contacts.extend(contacts)
                
                # Update metrics
                metrics["contacts_discovered"] = len(all_contacts)
                metrics["contacts_with_email"] = sum(1 for c in all_contacts if c.email and c.email_valid)
            
            # Categorize metrics
            for org in created_orgs:
                # By source
                source = org.get("discovery_method", "unknown")
                metrics["by_source"][source] = metrics["by_source"].get(source, 0) + 1
                
                # By organization type
                org_type = org.get("org_type", "unknown")
                metrics["by_organization_type"][org_type] = metrics["by_organization_type"].get(org_type, 0) + 1
                
                # By state
                state = org.get("state", "unknown")
                metrics["by_state"][state] = metrics["by_state"].get(state, 0) + 1
            
            # Save metrics to database
            runtime_seconds = int(time.time() - start_time)
            self._save_metrics(metrics, runtime_seconds)
            
            logger.info(f"Discovery completed in {runtime_seconds} seconds")
            logger.info(f"Found {metrics['organizations_discovered']} organizations and {metrics['contacts_discovered']} contacts")
            logger.info(f"High relevance organizations: {metrics['high_relevance_orgs']}")
            
            return metrics
        
        except Exception as e:
            logger.error(f"Error in discovery process: {e}", exc_info=True)
            
            # Save error metrics
            runtime_seconds = int(time.time() - start_time)
            metric_record = SystemMetric(
                urls_discovered=0,
                urls_crawled=0,
                organizations_discovered=metrics.get("organizations_discovered", 0),
                contacts_discovered=metrics.get("contacts_discovered", 0),
                search_queries_executed=0,
                runtime_seconds=runtime_seconds,
                errors_count=1
            )
            self.db_session.add(metric_record)
            self.db_session.commit()
            
            return metrics
    
    def _generate_search_queries(self) -> List[Dict[str, Any]]:
        """
        Generate search queries for target industries and states.
        
        Returns:
            List of search query dictionaries
        """
        queries = []
        
        # For each target industry
        for industry, industry_queries in IMPROVED_SEARCH_QUERIES.items():
            # For each target state
            for state in TARGET_STATES:
                # Special handling for Illinois (south of I-80)
                if state == "Illinois":
                    # For counties south of I-80
                    for county in ILLINOIS_SOUTH_OF_I80:
                        for query_template in industry_queries:
                            # Replace {state} with county, Illinois
                            query = query_template.replace("{state}", f"{county} county Illinois")
                            queries.append({
                                "query": query,
                                "category": industry,
                                "state": "Illinois",
                                "county": county
                            })
                else:
                    # Regular state handling
                    for query_template in industry_queries:
                        # Replace {state} with state name
                        query = query_template.replace("{state}", state)
                        queries.append({
                            "query": query,
                            "category": industry,
                            "state": state
                        })
        
        # Randomize queries to avoid bias towards early industry/state combinations
        random.shuffle(queries)
        
        logger.info(f"Generated {len(queries)} search queries")
        return queries
    
    def _execute_search_queries(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute search queries and extract potential organizations.
        
        Args:
            queries: List of search query dictionaries
            
        Returns:
            List of potential organization dictionaries
        """
        # In a real implementation, this would use Google Custom Search API or similar
        # For this example, we'll simulate the process
        potential_orgs = []
        
        # Track which queries were executed
        executed_queries = 0
        
        # Execute each query (limit to a reasonable number for demo)
        max_queries = min(50, len(queries))
        for query_data in queries[:max_queries]:
            executed_queries += 1
            
            # Log query execution
            logger.info(f"Executing query: {query_data['query']}")
            
            # Create search query record
            query_record = SearchQuery(
                query=query_data["query"],
                category=query_data["category"],
                state=query_data["state"],
                search_engine="google",
                execution_date=datetime.datetime.utcnow(),
                results_count=0,
                organizations_found=0
            )
            self.db_session.add(query_record)
            self.db_session.commit()
            
            # In a real implementation, this would call an API
            # For this example, we'll simulate finding 2-5 organizations per query
            num_results = random.randint(2, 5)
            query_record.results_count = num_results
            
            # Simulate extracting organizations from search results
            for i in range(num_results):
                # Create a potential organization based on the query
                org_type = query_data["category"]
                state = query_data["state"]
                
                # Generate a simulated organization
                # In a real implementation, this would extract data from search results
                org_name = f"Simulated {org_type.title()} Organization {executed_queries}-{i+1}"
                
                # Create potential organization dictionary
                potential_org = {
                    "name": org_name,
                    "org_type": org_type,
                    "state": state,
                    "discovery_method": "search",
                    "discovery_query": query_data["query"],
                    "search_query_id": query_record.id,
                    "relevance_indicators": []
                }
                
                # Generate a fake website URL
                domain = org_name.lower().replace(" ", "").replace("-", "")
                potential_org["website"] = f"https://www.{domain}.com"
                
                # Add to list of potential organizations
                potential_orgs.append(potential_org)
            
            # Update query record with number of organizations found
            query_record.organizations_found = num_results
            self.db_session.commit()
            
            # Simulate politeness delay
            time.sleep(0.1)  # In real implementation, use a longer delay
        
        logger.info(f"Executed {executed_queries} queries, found {len(potential_orgs)} potential organizations")
        return potential_orgs
    
    def _filter_and_prioritize_organizations(self, 
                                           potential_orgs: List[Dict[str, Any]], 
                                           max_orgs: int) -> List[Dict[str, Any]]:
        """
        Filter and prioritize potential organizations.
        
        Args:
            potential_orgs: List of potential organization dictionaries
            max_orgs: Maximum number of organizations to return
            
        Returns:
            List of filtered and prioritized organization dictionaries
        """
        filtered_orgs = []
        
        # Deduplicate organizations by name and state
        name_state_set = set()
        
        for org in potential_orgs:
            # Create a unique key for each organization
            key = (org["name"].lower(), org["state"].lower())
            
            # Skip if we've already seen this organization
            if key in name_state_set:
                continue
            
            # Add to set of seen organizations
            name_state_set.add(key)
            
            # Check if organization already exists in database
            existing_org = self.db_session.query(Organization).filter(
                Organization.name == org["name"],
                Organization.state == org["state"]
            ).first()
            
            # Skip if organization already exists
            if existing_org:
                continue
            
            # Add to filtered list
            filtered_orgs.append(org)
        
        # Prioritize organizations by type
        # Higher priority for water, wastewater, oil_gas, utility
        priority_types = ["water", "wastewater", "oil_gas", "utility", "agriculture"]
        
        # Sort by priority type
        filtered_orgs.sort(key=lambda x: (
            0 if x["org_type"] in priority_types else 1,
            priority_types.index(x["org_type"]) if x["org_type"] in priority_types else 999
        ))
        
        # Limit to max_orgs
        return filtered_orgs[:max_orgs]
    
    def _process_organizations(self, orgs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process organizations to get detailed information.
        
        Args:
            orgs: List of organization dictionaries
            
        Returns:
            List of processed organization dictionaries
        """
        processed_orgs = []
        
        for org in orgs:
            # In a real implementation, this would crawl the organization website
            # to extract more information or use other data sources
            
            # Simulate extracting additional information
            processed_org = org.copy()
            
            # Add simulated data
            processed_org["address"] = f"{random.randint(100, 9999)} Main St"
            processed_org["city"] = "Anytown"
            processed_org["zip_code"] = f"{random.randint(10000, 99999)}"
            processed_org["phone"] = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            
            # Generate a description based on organization type
            org_type = processed_org["org_type"]
            if org_type == "water":
                processed_org["description"] = f"{processed_org['name']} is a water utility serving {processed_org['city']}, {processed_org['state']}. We operate multiple treatment plants and manage the distribution system for the region."
            elif org_type == "wastewater":
                processed_org["description"] = f"{processed_org['name']} provides wastewater treatment services for {processed_org['city']}, {processed_org['state']}. Our facilities include multiple lift stations and a treatment plant."
            elif org_type == "agriculture":
                processed_org["description"] = f"{processed_org['name']} is an agricultural operation in {processed_org['state']} managing extensive irrigation systems across multiple fields."
            elif org_type == "oil_gas":
                processed_org["description"] = f"{processed_org['name']} operates oil and gas wells in {processed_org['state']} with multiple remote facilities requiring continuous monitoring."
            elif org_type == "utility":
                processed_org["description"] = f"{processed_org['name']} is a utility company serving {processed_org['state']} with multiple substations and distribution networks."
            else:
                processed_org["description"] = f"{processed_org['name']} is an organization based in {processed_org['state']} providing services in the {org_type} sector."
            
            # Add to processed organizations
            processed_orgs.append(processed_org)
        
        return processed_orgs
    
    def _create_organization_profiles(self, orgs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Create organization profiles in database.
        
        Args:
            orgs: List of organization dictionaries
            
        Returns:
            List of created organization dictionaries
        """
        created_orgs = []
        
        for org_data in orgs:
            # Extract organization data
            org_dict = {
                "name": org_data["name"],
                "org_type": org_data["org_type"],
                "website": org_data.get("website"),
                "address": org_data.get("address"),
                "city": org_data.get("city"),
                "state": org_data["state"],
                "zip_code": org_data.get("zip_code"),
                "phone": org_data.get("phone"),
                "description": org_data.get("description"),
                "discovery_method": org_data["discovery_method"],
                "discovery_query": org_data.get("discovery_query"),
                "confidence_score": 0.7,  # Initial confidence score
                "relevance_score": 5.0,   # Initial relevance score (will be updated later)
                "data_quality_score": 0.6  # Initial data quality score
            }
            
            # Create organization
            organization = Organization(**org_dict)
            self.db_session.add(organization)
            self.db_session.commit()
            
            # Add organization ID to data
            org_data["id"] = organization.id
            
            # Create initial discovered URL for the website
            if org_data.get("website"):
                url = DiscoveredURL(
                    organization_id=organization.id,
                    url=org_data["website"],
                    page_type="homepage",
                    title=org_data["name"],
                    description=org_data.get("description", ""),
                    priority_score=1.0  # Highest priority
                )
                self.db_session.add(url)
                self.db_session.commit()
            
            # Add to created organizations
            created_orgs.append(org_data)
        
        return created_orgs
    
    def _crawl_organization_websites(self, orgs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Crawl organization websites to discover more information.
        
        Args:
            orgs: List of organization dictionaries
            
        Returns:
            List of organizations with crawled data
        """
        # In a real implementation, this would crawl the organization websites
        # and extract more information
        
        # For this example, we'll simulate the crawling process
        crawled_orgs = []
        
        for org_data in orgs:
            # Get organization from database
            org = self.db_session.query(Organization).filter(Organization.id == org_data["id"]).first()
            if not org:
                continue
            
            # Update last crawled date
            org.last_crawled = datetime.datetime.utcnow()
            self.db_session.commit()
            
            # Simulate discovering additional URLs
            if org.website:
                # Create about page URL
                about_url = DiscoveredURL(
                    organization_id=org.id,
                    url=f"{org.website}/about",
                    page_type="about",
                    title=f"About {org.name}",
                    description=f"About page for {org.name}",
                    priority_score=0.9
                )
                self.db_session.add(about_url)
                
                # Create contact page URL
                contact_url = DiscoveredURL(
                    organization_id=org.id,
                    url=f"{org.website}/contact",
                    page_type="contact",
                    title=f"Contact {org.name}",
                    description=f"Contact information for {org.name}",
                    priority_score=0.9,
                    contains_contact_info=True
                )
                self.db_session.add(contact_url)
                
                # Create team/staff page URL
                team_url = DiscoveredURL(
                    organization_id=org.id,
                    url=f"{org.website}/team",
                    page_type="team",
                    title=f"Team at {org.name}",
                    description=f"Staff and leadership at {org.name}",
                    priority_score=0.8,
                    contains_contact_info=True
                )
                self.db_session.add(team_url)
                
                # Commit changes
                self.db_session.commit()
            
            # Add to crawled organizations
            crawled_orgs.append(org_data)
        
        return crawled_orgs
    
    def _score_and_rank_organizations(self, orgs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Score and rank organizations for SCADA integration relevance.
        
        Args:
            orgs: List of organization dictionaries
            
        Returns:
            List of ranked organization dictionaries
        """
        ranked_orgs = []
        
        for org_data in orgs:
            # Get organization from database
            org = self.db_session.query(Organization).filter(Organization.id == org_data["id"]).first()
            if not org:
                continue
            
            # Use the organization ranker to calculate relevance score
            # In a real implementation, this would analyze actual website content
            # For this example, we'll simulate the analysis using the description
            
            text_content = org.description or ""
            
            # Add relevance indicators based on organization type
            if org.org_type in INFRASTRUCTURE_PROCESS_KEYWORDS:
                # Add some random keywords from the list for this org type
                keywords = INFRASTRUCTURE_PROCESS_KEYWORDS[org.org_type]
                selected_keywords = random.sample(keywords, min(3, len(keywords)))
                
                # Add keywords to text content to simulate finding them on the website
                for keyword in selected_keywords:
                    text_content += f" We have {keyword} that requires monitoring."
            
            # For some organizations, simulate finding competitor indicators
            if random.random() < 0.05:  # 5% chance of being a competitor
                # Add a competitor indicator
                competitor_indicator = random.choice(COMPETITOR_INDICATORS)
                text_content += f" {competitor_indicator}."
            
            # Calculate relevance score
            relevance_score, analysis = self.org_ranker.calculate_relevance_score(
                org.org_type,
                text_content,
                {
                    "name": org.name,
                    "state": org.state
                }
            )
            
            # Update organization in database
            org.relevance_score = relevance_score
            self.db_session.commit()
            
            # Add analysis data to organization data
            org_data["relevance_score"] = relevance_score
            org_data["is_competitor"] = analysis["is_likely_competitor"]
            org_data["analysis"] = {
                "infrastructure_matches": analysis["infrastructure_matches"],
                "process_matches": analysis["process_matches"],
                "operational_challenges": analysis["operational_challenges"],
                "regulatory_requirements": analysis["regulatory_requirements"],
                "competitor_indicators": analysis["competitor_indicators"]
            }
            
            # Add to ranked organizations
            ranked_orgs.append(org_data)
        
        # Sort by relevance score (descending)
        ranked_orgs.sort(key=lambda x: x["relevance_score"], reverse=True)
        
        return ranked_orgs
    
    def _discover_contacts_for_organization(self, org: Organization, role_profiles: List[Dict[str, Any]]) -> List[Contact]:
        """
        Discover contacts for an organization based on role profiles.
        
        Args:
            org: Organization object
            role_profiles: List of role profile dictionaries
            
        Returns:
            List of Contact objects
        """
        # In a real implementation, this would:
        # 1. Crawl the organization website to find contact information
        # 2. Use LinkedIn or similar to find employees matching role profiles
        # 3. Validate and score contacts based on job titles and relevance
        
        # For this example, we'll simulate finding contacts
        discovered_contacts = []
        
        # Determine how many contacts to generate based on relevance
        num_contacts = min(5, max(1, int(org.relevance_score / 2)))
        
        # Get existing contact count
        existing_contact_count = self.db_session.query(Contact).filter(
            Contact.organization_id == org.id
        ).count()
        
        # Only generate more contacts if we don't have enough
        contacts_to_generate = max(0, num_contacts - existing_contact_count)
        
        # Sort role profiles by relevance score (descending)
        sorted_profiles = sorted(role_profiles, key=lambda x: x["relevance_score"], reverse=True)
        
        # Generate contacts for highest-relevance roles first
        for i in range(contacts_to_generate):
            # Select a role profile (prioritize higher-relevance roles)
            profile_index = min(i, len(sorted_profiles) - 1)
            profile = sorted_profiles[profile_index]
            
            # Generate contact data
            first_name = f"Contact{i+1}"
            last_name = f"For{org.name.replace(' ', '')}"
            
            # Use the role title from the profile
            job_title = profile["role_title"]
            
            # Generate email (50% chance of having email)
            email = None
            email_valid = False
            if random.random() < 0.5:
                domain = org.website.replace("https://www.", "").replace("http://www.", "") if org.website else f"{org.name.lower().replace(' ', '')}.com"
                email = f"{first_name.lower()}.{last_name.lower()}@{domain}"
                email_valid = True
            
            # Create contact
            contact_data = {
                "organization_id": org.id,
                "first_name": first_name,
                "last_name": last_name,
                "job_title": job_title,
                "email": email,
                "discovery_method": "simulated",
                "contact_confidence_score": 0.7,
                "contact_relevance_score": profile["relevance_score"],
                "email_valid": email_valid
            }
            
            # Check if contact already exists
            from app.database.crud import contact_exists
            if contact_exists(self.db_session, first_name, last_name, org.id):
                continue
            
            # Create contact
            contact = Contact(**contact_data)
            self.db_session.add(contact)
            self.db_session.commit()
            
            # Add to discovered contacts
            discovered_contacts.append(contact)
        
        return discovered_contacts
    
    def _generate_organization_taxonomy(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Generate organization taxonomy with enhanced relevance indicators.
        
        Returns:
            Dictionary with organization taxonomy
        """
        # In a real implementation, this would generate a comprehensive taxonomy
        # based on configuration data and learned patterns
        
        # For this example, we'll use a simplified taxonomy
        taxonomy = {}
        
        # For each organization type in INFRASTRUCTURE_PROCESS_KEYWORDS
        for org_type, keywords in INFRASTRUCTURE_PROCESS_KEYWORDS.items():
            taxonomy[org_type] = {
                "keywords": keywords,
                "infrastructure": ORG_RELEVANCE_INDICATORS.get(org_type, {}).get("infrastructure", []),
                "processes": ORG_RELEVANCE_INDICATORS.get(org_type, {}).get("processes", [])
            }
        
        return taxonomy
    
    def _create_role_profiles(self, taxonomy: Dict[str, Dict[str, List[str]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Create role profiles for each organization type.
        
        Args:
            taxonomy: Organization taxonomy dictionary
            
        Returns:
            Dictionary mapping organization types to lists of role profiles
        """
        role_profiles = {}
        
        # Define role titles and relevance scores for each organization type
        type_roles = {
            "water": [
                {"title": "Water Operations Manager", "relevance": 9, "decision_making": 8, "technical": 7},
                {"title": "Treatment Plant Director", "relevance": 9, "decision_making": 9, "technical": 7},
                {"title": "Plant Manager", "relevance": 8, "decision_making": 8, "technical": 7},
                {"title": "Water Quality Manager", "relevance": 7, "decision_making": 7, "technical": 8},
                {"title": "Control Systems Engineer", "relevance": 10, "decision_making": 6, "technical": 10},
                {"title": "Instrumentation Technician", "relevance": 8, "decision_making": 5, "technical": 9},
                {"title": "Chief Engineer", "relevance": 8, "decision_making": 9, "technical": 7}
            ],
            "wastewater": [
                {"title": "Wastewater Operations Manager", "relevance": 9, "decision_making": 8, "technical": 7},
                {"title": "Treatment Plant Director", "relevance": 9, "decision_making": 9, "technical": 7},
                {"title": "Plant Manager", "relevance": 8, "decision_making": 8, "technical": 7},
                {"title": "Process Control Supervisor", "relevance": 9, "decision_making": 7, "technical": 9},
                {"title": "Instrumentation Technician", "relevance": 8, "decision_making": 5, "technical": 9},
                {"title": "Maintenance Manager", "relevance": 7, "decision_making": 7, "technical": 7}
            ],
            "agriculture": [
                {"title": "Irrigation Manager", "relevance": 9, "decision_making": 8, "technical": 7},
                {"title": "Farm Operations Director", "relevance": 8, "decision_making": 9, "technical": 6},
                {"title": "Water Resources Manager", "relevance": 8, "decision_making": 7, "technical": 7},
                {"title": "Agricultural Engineer", "relevance": 7, "decision_making": 6, "technical": 8}
            ],
            "oil_gas": [
                {"title": "Operations Manager", "relevance": 9, "decision_making": 9, "technical": 7},
                {"title": "Control Room Supervisor", "relevance": 9, "decision_making": 7, "technical": 8},
                {"title": "Instrumentation Engineer", "relevance": 10, "decision_making": 6, "technical": 10},
                {"title": "Production Manager", "relevance": 8, "decision_making": 8, "technical": 6},
                {"title": "Facilities Manager", "relevance": 7, "decision_making": 7, "technical": 7}
            ],
            "utility": [
                {"title": "Operations Manager", "relevance": 9, "decision_making": 8, "technical": 7},
                {"title": "Control Center Supervisor", "relevance": 9, "decision_making": 7, "technical": 8},
                {"title": "Distribution Manager", "relevance": 8, "decision_making": 8, "technical": 7},
                {"title": "Automation Engineer", "relevance": 10, "decision_making": 6, "technical": 10},
                {"title": "Substation Engineer", "relevance": 7, "decision_making": 6, "technical": 8}
            ],
            "municipal": [
                {"title": "Public Works Director", "relevance": 8, "decision_making": 9, "technical": 6},
                {"title": "Utility Director", "relevance": 9, "decision_making": 9, "technical": 7},
                {"title": "City Engineer", "relevance": 7, "decision_making": 7, "technical": 8},
                {"title": "Infrastructure Manager", "relevance": 8, "decision_making": 7, "technical": 7}
            ],
            "transportation": [
                {"title": "Operations Director", "relevance": 8, "decision_making": 9, "technical": 6},
                {"title": "Systems Manager", "relevance": 9, "decision_making": 8, "technical": 7},
                {"title": "Control Systems Engineer", "relevance": 10, "decision_making": 6, "technical": 10},
                {"title": "Maintenance Director", "relevance": 7, "decision_making": 7, "technical": 7}
            ],
            "engineering": [
                {"title": "Project Manager", "relevance": 7, "decision_making": 8, "technical": 7},
                {"title": "Controls Engineer", "relevance": 9, "decision_making": 6, "technical": 9},
                {"title": "Engineering Manager", "relevance": 8, "decision_making": 8, "technical": 7},
                {"title": "Systems Engineer", "relevance": 8, "decision_making": 6, "technical": 9}
            ]
        }
        
        # Create profiles for each organization type
        for org_type, roles in type_roles.items():
            profiles = []
            
            for role in roles:
                # Create role profile
                profile = {
                    "org_type": org_type,
                    "role_title": role["title"],
                    "relevance_score": role["relevance"],
                    "decision_making_level": role["decision_making"],
                    "technical_knowledge_level": role["technical"],
                    "synonyms": self._generate_role_synonyms(role["title"])
                }
                
                # Add to profiles
                profiles.append(profile)
            
            # Add to role profiles dictionary
            role_profiles[org_type] = profiles
        
        return role_profiles
    
    def _generate_role_synonyms(self, role_title: str) -> List[str]:
        """
        Generate synonyms for a role title.
        
        Args:
            role_title: Role title
            
        Returns:
            List of synonym titles
        """
        # In a real implementation, this would generate synonyms based on
        # knowledge of common role title variations
        
        # For this example, we'll use a simple approach
        synonyms = []
        
        # Common prefix substitutions
        prefixes = {
            "Manager": ["Director", "Supervisor", "Lead", "Head"],
            "Director": ["Manager", "Supervisor", "Head", "Chief"],
            "Supervisor": ["Manager", "Coordinator", "Lead"],
            "Engineer": ["Specialist", "Technician", "Officer"],
            "Technician": ["Specialist", "Engineer", "Operator"]
        }
        
        # Generate synonyms by substituting prefixes
        parts = role_title.split()
        if len(parts) > 1 and parts[-1] in prefixes:
            for synonym in prefixes[parts[-1]]:
                synonym_title = " ".join(parts[:-1] + [synonym])
                synonyms.append(synonym_title)
        
        return synonyms
    
    def _save_checkpoint(self, checkpoint_id: str, data: dict = None) -> None:
        """
        Save a checkpoint in the discovery process.
        
        Args:
            checkpoint_id: Checkpoint identifier
            data: Optional data to save with checkpoint
        """
        # Determine stage from checkpoint_id
        stage = checkpoint_id.split('_')[0] if '_' in checkpoint_id else checkpoint_id
        
        # Create checkpoint record
        checkpoint = DiscoveryCheckpoint(
            checkpoint_id=checkpoint_id,
            stage=stage,
            data=json.dumps(data) if data else None
        )
        self.db_session.add(checkpoint)
        self.db_session.commit()
        
        # Update checkpoints dictionary
        self.checkpoints[checkpoint_id] = {
            "timestamp": datetime.datetime.utcnow(),
            "data": data
        }
        
        logger.info(f"Saved checkpoint: {checkpoint_id}")
    
    def _reset_checkpoints(self) -> None:
        """Reset checkpoints for this discovery run."""
        self.checkpoints = {}
    
    def _save_metrics(self, metrics: Dict[str, Any], runtime_seconds: int) -> None:
        """
        Save metrics to database.
        
        Args:
            metrics: Dictionary with metrics
            runtime_seconds: Runtime in seconds
        """
        # Create metric record
        metric_record = SystemMetric(
            urls_discovered=len(self.db_session.query(DiscoveredURL).all()),
            urls_crawled=len(self.db_session.query(DiscoveredURL).filter(DiscoveredURL.last_crawled.isnot(None)).all()),
            organizations_discovered=metrics.get("organizations_discovered", 0),
            contacts_discovered=metrics.get("contacts_discovered", 0),
            search_queries_executed=len(self.db_session.query(SearchQuery).filter(
                SearchQuery.execution_date >= datetime.datetime.today().date()
            ).all()),
            runtime_seconds=runtime_seconds
        )
        self.db_session.add(metric_record)
        self.db_session.commit()
                