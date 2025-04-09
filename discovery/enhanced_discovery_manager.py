"""
Enhanced Organization Discovery and Ranking System for SCADA Integration Clients

This module provides an enhanced approach to discovering and ranking organizations
that are likely to need SCADA integration services, focusing on infrastructure
and operational indicators rather than explicit SCADA mentions.
"""

import json
import time
import random
import datetime
import logging
from typing import Dict, List, Any, Tuple, Optional, Set
from urllib.parse import urlparse
import re

from sqlalchemy import and_, or_, func, desc
from bs4 import BeautifulSoup
import tldextract

from app.database.models import (
    Organization, 
    Contact, 
    DiscoveredURL, 
    SearchQuery,
    DiscoverySession,
    DiscoveryCheckpoint
)
from app.discovery.search.google_search import GoogleSearchClient
from app.discovery.mock_crawler import MockCrawler
from app.discovery.organization_extractor import OrganizationExtractor
from app.utils.gemini_client import GeminiClient
from app.utils.logger import setup_logger
from app.config import (
    INFRASTRUCTURE_PROCESS_KEYWORDS,
    OPERATIONAL_CHALLENGE_KEYWORDS,
    REGULATORY_REQUIREMENT_KEYWORDS,
    ORG_RELEVANCE_INDICATORS,
    COMPETITOR_INDICATORS,
    IMPROVED_SEARCH_QUERIES
)

# Set up logging
logger = setup_logger("enhanced_discovery")

class EnhancedDiscoveryManager:
    """
    Enhanced discovery manager that focuses on identifying potential SCADA integration
    clients based on infrastructure, operations, and regulatory indicators rather than
    explicit SCADA mentions.
    """
    
    def __init__(self, db_session, config, target_org_types=None, target_states=None):
        """
        Initialize the enhanced discovery manager.
        
        Args:
            db_session: Database session
            config: Configuration object
            target_org_types: Optional comma-separated list of organization types to target
            target_states: Optional comma-separated list of states to target
        """
        self.db_session = db_session
        self.config = config
        
        # Initialize clients
        self.google_search = GoogleSearchClient(db_session)  # Uses config variables internally
        # Use the real web crawler instead of the mock crawler
        from app.discovery.crawler.web_crawler import Crawler
        self.web_crawler = Crawler(db_session)
        self.org_extractor = OrganizationExtractor(db_session)
        self.gemini_client = GeminiClient(api_key=config.GEMINI_API_KEY)
        
        # Set target industries from configuration or parameter
        if target_org_types:
            # Convert comma-separated string to list if needed
            if isinstance(target_org_types, str):
                target_org_types = [t.strip() for t in target_org_types.split(',')]
            self.target_industries = target_org_types
            logger.info(f"Using targeted organization types: {self.target_industries}")
        else:
            # Use default set of target industries
            self.target_industries = [
                "water", "wastewater", "engineering", "government",
                "utility", "oil_gas", "agriculture", "transportation",
                "municipal", "healthcare"
            ]
        
        # Target states with special handling for Illinois (south of I-80)
        if target_states:
            # Convert comma-separated string to list if needed
            if isinstance(target_states, str):
                target_states = [s.strip() for s in target_states.split(',')]
            self.target_states = target_states
            logger.info(f"Using targeted states: {self.target_states}")
        else:
            # Use default target states
            self.target_states = [
                "Arizona", "New Mexico", "Nevada", "Utah", "Missouri"
            ]
        
        self.special_regions = {
            "Illinois": "south of I-80"
        }
        
        # Priority ranking for organization types (higher = more priority)
        self.org_type_priority = {
            "water": 10,
            "wastewater": 10,
            "utility": 9,
            "municipal": 8,
            "agriculture": 7,
            "oil_gas": 7,
            "transportation": 6,
            "engineering": 5,
            "government": 5,
            "healthcare": 4
        }
        
        # Initialize discovery session
        self.session_id = None
        self.checkpoint_id = None
    
    def start_discovery_session(self):
        """
        Start a new discovery session.
        
        Returns:
            Session ID
        """
        # Create new discovery session
        session = DiscoverySession(
            start_time=datetime.datetime.now(),
            status="running"
        )
        
        self.db_session.add(session)
        self.db_session.commit()
        
        self.session_id = session.id
        logger.info(f"Started discovery session {self.session_id}")
        
        return self.session_id
    
    def create_checkpoint(self, stage: str, progress_data: Dict[str, Any] = None) -> int:
        """
        Create a checkpoint for the current discovery process.
        
        Args:
            stage: Current stage of discovery
            progress_data: Dictionary containing progress information
            
        Returns:
            Checkpoint ID
        """
        # Ensure we have a session
        if not self.session_id:
            self.start_discovery_session()
        
        # Create checkpoint
        if progress_data is None:
            progress_data = {}
        
        # Use a unique checkpoint ID with timestamp
        checkpoint_id = f"{self.session_id}_{int(time.time())}"
            
        # Skip checkpoint creation for testing
        logger.info(f"Skipping checkpoint creation for {stage}")
        self.checkpoint_id = "temp_checkpoint"
        
        # For debugging, log the progress data
        if isinstance(progress_data, dict) and "metrics" in progress_data:
            logger.info(f"Progress metrics: {progress_data['metrics']}")
            
        return self.checkpoint_id
    
    def run_discovery(self, max_orgs: int = 20, resume_from: str = None) -> Dict[str, Any]:
        """
        Run the enhanced discovery process to find potential SCADA integration clients.
        
        Args:
            max_orgs: Maximum number of organizations to process
            resume_from: Optional checkpoint file to resume from
            
        Returns:
            Discovery results summary
        """
        # Start session
        self.start_discovery_session()
        
        # Initialize metrics
        metrics = {
            "search_queries_executed": 0,
            "search_results_found": 0,
            "urls_crawled": 0,
            "organizations_discovered": 0,
            "potential_clients_found": 0,
            "competitors_filtered": 0,
            "contacts_discovered": 0,
            "high_relevance_orgs": 0,
            "orgs_by_type": {},
            "orgs_by_state": {}
        }
        
        # Load checkpoint if resuming
        if resume_from:
            try:
                with open(f"data/checkpoints/{resume_from}", "r") as f:
                    checkpoint_data = json.load(f)
                
                self.session_id = checkpoint_data.get("session_id")
                self.checkpoint_id = checkpoint_data.get("checkpoint_id")
                stage = checkpoint_data.get("stage")
                progress_data = checkpoint_data.get("progress_data", {})
                
                logger.info(f"Resuming from checkpoint: {resume_from}, stage: {stage}")
                
                # Skip completed stages based on checkpoint
                if stage == "search_complete":
                    search_results = progress_data.get("search_results", [])
                    logger.info(f"Skipping search phase, using {len(search_results)} cached results")
                    metrics["search_queries_executed"] = progress_data.get("search_queries_executed", 0)
                    metrics["search_results_found"] = len(search_results)
                    self.create_checkpoint("resumed_from_search_complete", {
                        "search_results": search_results,
                        "metrics": metrics
                    })
                    return self._process_search_results(search_results, metrics, max_orgs)
                
                elif stage == "crawl_complete":
                    crawled_orgs = progress_data.get("crawled_organizations", [])
                    logger.info(f"Skipping search and crawl phases, using {len(crawled_orgs)} cached organizations")
                    metrics.update(progress_data.get("metrics", {}))
                    self.create_checkpoint("resumed_from_crawl_complete", {
                        "crawled_organizations": crawled_orgs,
                        "metrics": metrics
                    })
                    return self._process_crawled_organizations(crawled_orgs, metrics, max_orgs)
                
                # Otherwise start from beginning but with session ID preserved
                logger.info("Starting discovery from beginning with preserved session ID")
            
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
                logger.info("Starting discovery from beginning")
        
        # Phase 1: Search for potential organizations
        search_results = self._execute_targeted_searches()
        metrics["search_queries_executed"] = len(search_results)
        metrics["search_results_found"] = sum(len(results) for _, results in search_results)
        
        # Create checkpoint after search phase
        self.create_checkpoint("search_complete", {
            "search_results": search_results,
            "metrics": metrics
        })
        
        # Phase 2: Process search results
        return self._process_search_results(search_results, metrics, max_orgs)
    
    def _process_search_results(self, search_results, metrics, max_orgs):
        """
        Process search results to extract organizations.
        
        Args:
            search_results: List of search results
            metrics: Metrics dictionary
            max_orgs: Maximum organizations to process
            
        Returns:
            Discovery results summary
        """
        logger.info(f"Processing {len(search_results)} search result sets")
        
        # Phase 2: Crawl websites and extract organizations
        crawled_organizations = self._crawl_and_extract_organizations(search_results, max_orgs)
        
        # Count only if we have organizations
        if crawled_organizations:
            metrics["urls_crawled"] = len(crawled_organizations)
            org_count = sum(1 for org in crawled_organizations if org and isinstance(org, dict) and "organization" in org)
            metrics["organizations_discovered"] = org_count
            logger.info(f"Found {org_count} organizations from {len(crawled_organizations)} crawled URLs")
        else:
            metrics["urls_crawled"] = 0
            metrics["organizations_discovered"] = 0
            logger.info("No organizations found from crawled URLs")
        
        # Create checkpoint after crawl phase
        self.create_checkpoint("crawl_complete", {
            "crawled_organizations": crawled_organizations if crawled_organizations else [],
            "metrics": metrics
        })
        
        # Phase 3: Process organizations for contact discovery and ranking
        return self._process_crawled_organizations(crawled_organizations, metrics, max_orgs)
    
    def _process_crawled_organizations(self, crawled_organizations, metrics, max_orgs):
        """
        Process crawled organizations for ranking and contact discovery.
        
        Args:
            crawled_organizations: List of crawled organizations
            metrics: Metrics dictionary
            max_orgs: Maximum organizations to process
            
        Returns:
            Discovery results summary
        """
        # Phase 3: Rank organizations by potential as SCADA clients
        ranked_organizations = self._rank_organizations(crawled_organizations)
        
        # Update metrics with organization types
        for org in ranked_organizations:
            if "organization" in org and org["organization"]:
                org_type = org["organization"].get("org_type")
                if org_type:
                    metrics["orgs_by_type"][org_type] = metrics["orgs_by_type"].get(org_type, 0) + 1
                
                state = org["organization"].get("state")
                if state:
                    metrics["orgs_by_state"][state] = metrics["orgs_by_state"].get(state, 0) + 1
                
                # Count potential clients vs competitors
                if org.get("is_competitor", False):
                    metrics["competitors_filtered"] += 1
                else:
                    metrics["potential_clients_found"] += 1
                
                # Count high relevance orgs
                if org.get("relevance_score", 0) >= 7.0:
                    metrics["high_relevance_orgs"] += 1
        
        # Create checkpoint after ranking phase
        self.create_checkpoint("ranking_complete", {
            "ranked_organizations": ranked_organizations,
            "metrics": metrics
        })
        
        # Phase 4: Discover contacts for high-potential organizations
        contacts_discovered = self._discover_contacts(ranked_organizations, max_orgs)
        metrics["contacts_discovered"] = len(contacts_discovered)
        
        # Create checkpoint after contact discovery
        self.create_checkpoint("discovery_complete", {
            "ranked_organizations": ranked_organizations,
            "contacts_discovered": contacts_discovered,
            "metrics": metrics
        })
        
        # Generate report
        self._generate_discovery_report(ranked_organizations, contacts_discovered, metrics)
        
        # Update session status
        self._update_session_status("completed")
        
        # Return summary
        return {
            "session_id": self.session_id,
            "metrics": metrics,
            "potential_clients": metrics["potential_clients_found"],
            "competitors_filtered": metrics["competitors_filtered"],
            "contacts_discovered": metrics["contacts_discovered"],
            "high_relevance_orgs": metrics["high_relevance_orgs"]
        }
    
    def _execute_targeted_searches(self) -> List[Tuple[str, List[Dict[str, Any]]]]:
        """
        Execute targeted searches for each industry and state combination.
        
        Returns:
            List of (query, results) tuples
        """
        results = []
        
        # Use the target industries specified at initialization
        # If we have more than 3 industries, limit to 3 for performance reasons
        if len(self.target_industries) > 3:
            active_industries = self.target_industries[:3]
            logger.info(f"Limiting search to first 3 industries: {active_industries}")
        else:
            active_industries = self.target_industries
        
        logger.info(f"Searching for organizations in industries: {active_industries}")
        
        # Process each target industry
        for industry in active_industries:
            # Get queries for industry
            if industry in IMPROVED_SEARCH_QUERIES:
                query_templates = IMPROVED_SEARCH_QUERIES[industry][:3]  # Use more query templates
            else:
                # Default queries if industry not specifically covered
                query_templates = [
                    "{industry} {state} organizations",
                    "{industry} facilities {state}"
                ]
            
            # Use multiple states to find more organizations
            test_states = ["Arizona", "Nevada", "Utah"]
            
            # For each state including special regions
            for state in test_states:
                # Handle special regions (like Illinois south of I-80)
                if state in self.special_regions:
                    location_spec = f"{state} {self.special_regions[state]}"
                else:
                    location_spec = state
                
                # Process each query template
                for query_template in query_templates:
                    # Format query
                    query = query_template.replace("{industry}", industry).replace("{state}", location_spec)
                    
                    # Add to database
                    search_query = SearchQuery(
                        query=query,
                        category=industry,  # Changed from industry to category
                        state=state,
                        execution_date=datetime.datetime.now()  # Changed from timestamp to execution_date
                        # Removed session_id as it's not in the model
                    )
                    self.db_session.add(search_query)
                    
                    # Execute search - use only Google since Bing API gives permission errors
                    try:
                        # Always use Google search since Bing API is returning 401 errors
                        # Get full page of results instead of just the first 10
                        search_results = self.google_search.get_all_results(query, max_results=100)
                        engine = "google"
                        
                        # Update query record
                        search_query.results_count = len(search_results)
                        search_query.search_engine = engine
                        search_query.status = "completed"
                        
                        # Store results
                        results.append((query, search_results))
                        
                        # Log
                        logger.info(f"Search for '{query}' returned {len(search_results)} results from {engine}")
                        
                        # Politeness delay
                        time.sleep(random.uniform(1.0, 3.0))
                    
                    except Exception as e:
                        logger.error(f"Search error for '{query}': {e}")
                        search_query.status = "error"
                        search_query.error_message = str(e)
        
        # Commit database changes
        self.db_session.commit()
        
        return results
    
    def _crawl_and_extract_organizations(
        self, search_results: List[Tuple[str, List[Dict[str, Any]]]], max_orgs: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Crawl websites and extract organization information.
        
        Args:
            search_results: List of (query, results) tuples
            max_orgs: Maximum number of organizations to process
            
        Returns:
            List of processed organization data
        """
        crawled_organizations = []
        processed_domains = set()
        org_count = 0
        
        # Flatten and deduplicate search results
        all_results = []
        
        logger.info(f"Processing search results type: {type(search_results)}")
        if len(search_results) > 0:
            logger.info(f"First search result item type: {type(search_results[0])}")
            
        for query_results in search_results:
            # Handle different formats of search results
            if isinstance(query_results, tuple) and len(query_results) == 2:
                query, results = query_results
                logger.info(f"Processing query: {query} with {len(results) if isinstance(results, list) else 'non-list'} results")
                
                # Skip if results is not a list/dictionary that we can iterate
                if not hasattr(results, '__iter__') or isinstance(results, str):
                    logger.warning(f"Skipping non-iterable results for query {query}")
                    continue
                    
                # For Google search results from API
                if isinstance(results, dict) and "items" in results:
                    logger.info(f"Processing Google API results with {len(results['items'])} items")
                    items = results["items"]
                else:
                    items = results
            else:
                # Single result item (possibly from a simplistic test)
                logger.info(f"Processing direct result: {type(query_results)}")
                items = [query_results]
            
            # Process each individual search result
            for result in items:
                if not isinstance(result, dict):
                    logger.warning(f"Skipping non-dict result: {type(result)}")
                    continue
                    
                # Extract domain
                url = result.get("link", "")
                if not url:
                    url = result.get("url", "")  # Alternate field name
                    
                if not url:
                    logger.warning(f"Skipping result with no URL: {result}")
                    continue
                    
                domain = self._extract_domain(url)
                
                # Skip if domain already processed
                if domain in processed_domains:
                    continue
                
                result_data = {
                    "url": url,
                    "title": result.get("title", "") or result.get("name", ""),  # Google vs Bing API
                    "snippet": result.get("snippet", "") or result.get("description", ""),  # Google vs Bing API
                    "query": query if 'query' in locals() else "Unknown query"
                }
                
                logger.info(f"Adding search result for {url}")
                all_results.append(result_data)
                processed_domains.add(domain)
        
        # Sort results by potential relevance (prioritize .gov, .edu, specific keywords)
        sorted_results = self._prioritize_search_results(all_results)
        
        # Process all results with no limit
        for result in sorted_results:
                
            try:
                url = result["url"]
                query = result["query"]
                
                # Extract industry and state from query
                query_parts = query.lower().split()
                detected_industry = next((ind for ind in self.target_industries 
                                       if ind in query_parts), None)
                detected_state = next((state for state in self.target_states + list(self.special_regions.keys())
                                    if state.lower() in query.lower()), None)
                
                # Crawl the URL
                logger.info(f"Crawling {url}")
                crawl_result = self.web_crawler.crawl_url(url)
                logger.info(f"Crawl result type: {type(crawl_result)}")
                
                # Handle different return types from crawler
                if isinstance(crawl_result, tuple):
                    # Handle old style return (content, links)
                    html_content, links = crawl_result
                    crawl_result = {"html_content": html_content, "links": links}
                
                if not crawl_result or not crawl_result.get("html_content"):
                    logger.warning(f"Failed to crawl {url}")
                    continue
                    
                logger.info(f"Successfully crawled {url}, content length: {len(crawl_result.get('html_content', ''))}")
                
                # Extract organization data
                html_content = crawl_result["html_content"]
                soup = BeautifulSoup(html_content, "html.parser")
                text_content = soup.get_text(" ", strip=True)
                
                # Check for infrastructure indicators
                infrastructure_indicators = self._extract_infrastructure_indicators(
                    text_content, detected_industry
                )
                
                # Check for competitor indicators
                competitor_analysis = self._analyze_for_competitor_indicators(text_content)
                
                # Extract organization using Gemini
                logger.info(f"Extracting organizations from {url} with industry={detected_industry}, state={detected_state}")
                org_data = self.org_extractor.extract_organizations_from_content(
                    html_content, url, state_context=detected_state, industry_hint=detected_industry
                )
                
                logger.info(f"Extraction result: {len(org_data) if org_data else 0} organizations found")
                
                if org_data and len(org_data) > 0:
                    organization = org_data[0]  # Take the first organization
                    logger.info(f"Found organization: {organization.get('name', 'Unknown')}, type: {organization.get('org_type', 'Unknown')}")
                    
                    # Insert organization into database if it doesn't exist
                    from app.database import crud
                    existing_org = crud.get_organization_by_name_and_state(
                        self.db_session, 
                        organization.get('name'),
                        organization.get('state')
                    )
                    
                    if not existing_org:
                        # Create new organization
                        org_record = crud.create_organization(self.db_session, {
                            'name': organization.get('name'),
                            'org_type': organization.get('org_type'),
                            'state': organization.get('state'),
                            'description': organization.get('description'),
                            'source_url': url,
                            'discovery_method': 'enhanced_discovery',
                            'confidence_score': organization.get('confidence_score', 0.7),
                            'relevance_score': organization.get('relevance_score', 7.0)
                        })
                        logger.info(f"Extracted organization: {organization.get('name')}")
                    else:
                        org_record = existing_org
                        logger.info(f"Organization already exists: {organization.get('name')}")
                    
                    # Update organization record with infrastructure analysis
                    organization_id = org_record.id if org_record else None
                    if organization_id:
                        org_record = self.db_session.query(Organization).filter(
                            Organization.id == organization_id
                        ).first()
                        
                        if org_record:
                            # Store infrastructure indicators as JSON in extended_data field
                            if not org_record.extended_data:
                                org_record.extended_data = {}
                            
                            org_record.extended_data["infrastructure_indicators"] = infrastructure_indicators
                            org_record.extended_data["competitor_analysis"] = competitor_analysis
                            
                            # Update database
                            self.db_session.commit()
                    
                    # Add to crawled organizations
                    crawled_organizations.append({
                        "url": url,
                        "organization": organization,
                        "infrastructure_indicators": infrastructure_indicators,
                        "competitor_analysis": competitor_analysis,
                        "html_content": crawl_result["html_content"],
                        "text_content": text_content
                    })
                    
                    org_count += 1
                    logger.info(f"Extracted organization: {organization.get('name')}")
                else:
                    logger.info(f"No organization found at {url}")
                
                # Politeness delay
                time.sleep(random.uniform(1.0, 2.0))
            
            except Exception as e:
                logger.error(f"Error processing {result['url']}: {e}")
        
        return crawled_organizations
    
    def _extract_infrastructure_indicators(
        self, text_content: str, industry: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extract infrastructure and process indicators from text content.
        
        Args:
            text_content: Text content from website
            industry: Optional industry hint
            
        Returns:
            Dictionary of infrastructure indicators
        """
        result = {
            "infrastructure_matches": [],
            "process_matches": [],
            "operational_challenges": [],
            "regulatory_requirements": [],
            "infrastructure_score": 0.0
        }
        
        # Convert to lowercase for matching
        text_lower = text_content.lower()
        
        # Check industry-specific infrastructure keywords
        if industry and industry in INFRASTRUCTURE_PROCESS_KEYWORDS:
            for keyword in INFRASTRUCTURE_PROCESS_KEYWORDS[industry]:
                if keyword.lower() in text_lower:
                    result["infrastructure_matches"].append(keyword)
        
        # Check all industries if no specific industry or insufficient matches
        if not industry or len(result["infrastructure_matches"]) < 3:
            for ind, keywords in INFRASTRUCTURE_PROCESS_KEYWORDS.items():
                if ind == industry:
                    continue  # Skip if already processed
                    
                for keyword in keywords:
                    if keyword.lower() in text_lower and keyword not in result["infrastructure_matches"]:
                        result["infrastructure_matches"].append(keyword)
        
        # Check for operational challenges
        for challenge in OPERATIONAL_CHALLENGE_KEYWORDS:
            if challenge.lower() in text_lower:
                result["operational_challenges"].append(challenge)
        
        # Check for regulatory requirements
        for req in REGULATORY_REQUIREMENT_KEYWORDS:
            if req.lower() in text_lower:
                result["regulatory_requirements"].append(req)
        
        # Check for industry-specific relevance indicators
        if industry and industry in ORG_RELEVANCE_INDICATORS:
            # Infrastructure
            for item in ORG_RELEVANCE_INDICATORS[industry]["infrastructure"]:
                if item.lower() in text_lower and item not in result["infrastructure_matches"]:
                    result["infrastructure_matches"].append(item)
            
            # Processes
            for item in ORG_RELEVANCE_INDICATORS[industry]["processes"]:
                if item.lower() in text_lower:
                    result["process_matches"].append(item)
        
        # Calculate infrastructure score (0-10 scale)
        infra_weight = 1.0
        process_weight = 1.0
        challenge_weight = 1.5
        regulatory_weight = 1.0
        
        raw_score = (
            len(result["infrastructure_matches"]) * infra_weight +
            len(result["process_matches"]) * process_weight +
            len(result["operational_challenges"]) * challenge_weight +
            len(result["regulatory_requirements"]) * regulatory_weight
        )
        
        # Normalize to 0-10 scale
        result["infrastructure_score"] = min(10, raw_score)
        
        return result
    
    def _analyze_for_competitor_indicators(self, text_content: str) -> Dict[str, Any]:
        """
        Analyze text content for indicators that the organization might be a SCADA provider.
        
        Args:
            text_content: Text content from website
            
        Returns:
            Dictionary with competitor analysis
        """
        result = {
            "competitor_indicators": [],
            "competitor_score": 0.0,
            "is_likely_competitor": False
        }
        
        # Convert to lowercase for matching
        text_lower = text_content.lower()
        
        # Check for competitor indicators
        for indicator in COMPETITOR_INDICATORS:
            if indicator.lower() in text_lower:
                result["competitor_indicators"].append(indicator)
        
        # Calculate competitor score (0-10 scale)
        raw_score = len(result["competitor_indicators"]) * 2.0
        result["competitor_score"] = min(10, raw_score)
        
        # Determine if likely competitor
        result["is_likely_competitor"] = (
            result["competitor_score"] >= 4.0 or  # Multiple competitor indicators
            "scada integrator" in text_lower or
            "scada provider" in text_lower or
            "system integrator" in text_lower or
            "integration services" in text_lower
        )
        
        return result
    
    def _prioritize_search_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prioritize search results based on relevance to SCADA client discovery.
        
        Args:
            results: List of search results
            
        Returns:
            Prioritized list of search results
        """
        # Define priority domains and terms
        priority_domains = [".gov", ".edu", ".org", ".us"]
        priority_terms = [
            "water", "wastewater", "utility", "municipal", "city of",
            "county of", "authority", "district", "department", "agency",
            "plant", "facility", "infrastructure", "engineering"
        ]
        
        # Score each result
        scored_results = []
        for result in results:
            url = result["url"]
            title = result["title"].lower()
            snippet = result["snippet"].lower()
            
            score = 0
            
            # Domain priority
            domain = self._extract_domain(url)
            for pri_domain in priority_domains:
                if domain.endswith(pri_domain):
                    score += 5
                    break
            
            # Title and snippet priority terms
            for term in priority_terms:
                if term in title:
                    score += 3
                if term in snippet:
                    score += 2
            
            # Check for negatives (likely competitors)
            negative_terms = ["scada provider", "scada integrator", "integration services"]
            for term in negative_terms:
                if term in title or term in snippet:
                    score -= 10
            
            # Add score to result
            result["priority_score"] = score
            scored_results.append(result)
        
        # Sort by score (descending)
        sorted_results = sorted(scored_results, key=lambda x: x["priority_score"], reverse=True)
        
        # Process all results to find organizations (no limit)
        logger.info(f"Processing {len(sorted_results)} prioritized search results")
        
        return sorted_results
    
    def _extract_domain(self, url: str) -> str:
        """
        Extract domain from URL.
        
        Args:
            url: URL string
            
        Returns:
            Domain string
        """
        try:
            # Extract root domain plus suffix (e.g., example.com)
            ext = tldextract.extract(url)
            return f"{ext.domain}.{ext.suffix}"
        except:
            # Fallback to simple parsing
            try:
                parsed = urlparse(url)
                return parsed.netloc
            except:
                return url
    
    def _rank_organizations(self, crawled_organizations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Rank organizations by potential as SCADA integration clients.
        
        Args:
            crawled_organizations: List of crawled organizations
            
        Returns:
            Ranked organizations
        """
        ranked_orgs = []
        
        for org_data in crawled_organizations:
            if not org_data.get("organization"):
                continue
                
            organization = org_data["organization"]
            infrastructure_indicators = org_data.get("infrastructure_indicators", {})
            competitor_analysis = org_data.get("competitor_analysis", {})
            
            # Get base scores
            infra_score = infrastructure_indicators.get("infrastructure_score", 0)
            competitor_score = competitor_analysis.get("competitor_score", 0)
            is_competitor = competitor_analysis.get("is_likely_competitor", False)
            
            # Get organization type
            org_type = organization.get("org_type")
            
            # Calculate type multiplier
            type_multiplier = self._get_type_multiplier(org_type)
            
            # Calculate raw relevance score (on 0-10 scale)
            base_confidence = float(organization.get("confidence", 0))
            
            # Combine scores with heavier weight on infrastructure indicators
            raw_score = (
                (infra_score * 0.6) +                   # 60% from infrastructure indicators
                (base_confidence * 10 * 0.4)            # 40% from base confidence (scaled to 0-10)
            ) * type_multiplier                         # Apply type multiplier
            
            # Apply competitor penalty if needed
            if is_competitor:
                competitor_penalty = min(8, competitor_score * 0.8)  # Cap penalty at 8 points
            else:
                competitor_penalty = 0
            
            # Calculate final relevance score
            relevance_score = max(0, min(10, raw_score - competitor_penalty))
            
            # Add scores to organization data
            ranked_org = org_data.copy()
            ranked_org["relevance_score"] = relevance_score
            ranked_org["infrastructure_score"] = infra_score
            ranked_org["competitor_score"] = competitor_score
            ranked_org["is_competitor"] = is_competitor
            ranked_org["type_multiplier"] = type_multiplier
            
            # Update organization record in database
            org_id = organization.get("id")
            if org_id:
                org_record = self.db_session.query(Organization).filter(
                    Organization.id == org_id
                ).first()
                
                if org_record:
                    org_record.relevance_score = relevance_score
                    org_record.is_competitor = is_competitor
                    
                    # Update extended data
                    if not org_record.extended_data:
                        org_record.extended_data = {}
                    
                    org_record.extended_data["relevance_analysis"] = {
                        "infrastructure_score": infra_score,
                        "competitor_score": competitor_score,
                        "type_multiplier": type_multiplier,
                        "raw_score": raw_score,
                        "competitor_penalty": competitor_penalty,
                        "final_score": relevance_score
                    }
                    
                    self.db_session.commit()
            
            ranked_orgs.append(ranked_org)
        
        # Sort by relevance score (descending)
        return sorted(ranked_orgs, key=lambda x: x.get("relevance_score", 0), reverse=True)
    
    def _get_type_multiplier(self, org_type: Optional[str]) -> float:
        """
        Get relevance multiplier for organization type.
        
        Args:
            org_type: Organization type
            
        Returns:
            Multiplier value (0.0-1.0)
        """
        if not org_type:
            return 0.7  # Default multiplier
            
        # Define type multipliers based on organization types
        # These are different than the priority values - they represent how likely
        # an organization of this type is to need SCADA services
        type_multipliers = {
            "water": 1.0,           # Top tier: Water treatment/distribution has very high SCADA needs
            "wastewater": 1.0,      # Top tier: Wastewater treatment has very high SCADA needs
            "utility": 0.95,        # Very high: Utilities generally need monitoring/control
            "oil_gas": 0.95,        # Very high: Oil & gas operations need extensive monitoring
            "agriculture": 0.9,     # High: Large irrigation districts need water management
            "municipal": 0.9,       # High: Municipalities often manage multiple infrastructure systems
            "transportation": 0.85, # Good: Traffic systems, tunnels, etc.
            "engineering": 0.75,    # Moderate: Engineering firms may handle SCADA projects for clients
            "government": 0.75,     # Moderate: Depends on the specific government function
            "healthcare": 0.7,      # Moderate: Building automation, but less core to operations
        }
        
        return type_multipliers.get(org_type.lower(), 0.7)  # Default to 0.7
    
    def _discover_contacts(
        self, ranked_organizations: List[Dict[str, Any]], max_orgs: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Discover key contacts at high-potential organizations.
        
        Args:
            ranked_organizations: List of ranked organizations
            max_orgs: Maximum number of organizations to process
            
        Returns:
            List of discovered contacts
        """
        # Define role profiles for each organization type
        role_profiles = {
            "water": [
                "Public Works Director", "Water Treatment Superintendent", 
                "Operations Manager", "Utility Director", "City Engineer",
                "Water Systems Manager", "Plant Supervisor"
            ],
            "wastewater": [
                "Wastewater Superintendent", "Treatment Plant Supervisor",
                "Operations Manager", "Public Works Director", "Facilities Manager",
                "Process Control Supervisor", "Plant Engineer"
            ],
            "utility": [
                "Operations Director", "Director of Engineering", "Utility Manager",
                "Operations Supervisor", "Facilities Director", "Systems Engineer",
                "Chief Engineer", "Operations Supervisor", "Control Room Supervisor"
            ],
            "municipal": [
                "Public Works Director", "City Engineer", "Facilities Manager",
                "Infrastructure Manager", "Utility Director", "Operations Manager"
            ],
            "oil_gas": [
                "Operations Manager", "Automation Engineer", "Production Supervisor",
                "Facilities Manager", "Engineering Manager", "Production Engineer",
                "Operations Supervisor"
            ],
            "agriculture": [
                "Water Manager", "Operations Director", "Irrigation Manager",
                "District Manager", "Engineering Manager", "Facilities Director"
            ],
            "transportation": [
                "Operations Director", "Systems Manager", "Facilities Manager",
                "Engineering Director", "Maintenance Manager", "Infrastructure Manager"
            ],
            "engineering": [
                "Project Manager", "Automation Engineer", "Control Systems Engineer",
                "Engineering Manager", "Director of Engineering", "Principal Engineer"
            ],
            "government": [
                "Facilities Director", "Operations Manager", "Public Works Director",
                "Infrastructure Manager", "Systems Administrator", "Maintenance Director"
            ],
            "healthcare": [
                "Facilities Director", "Engineering Manager", "Operations Director",
                "Plant Operations Manager", "Maintenance Director"
            ]
        }
        
        # Role synonyms to expand search
        role_synonyms = {
            "Director": ["Manager", "Head", "Chief", "Supervisor", "Leader"],
            "Manager": ["Director", "Supervisor", "Head", "Chief", "Administrator"],
            "Supervisor": ["Manager", "Coordinator", "Lead", "Chief", "Head"],
            "Engineer": ["Specialist", "Technician", "Technologist", "Coordinator"],
            "Superintendent": ["Manager", "Director", "Supervisor", "Chief"],
            "Operations": ["Operational", "Operating", "Process", "Production", "Facility"]
        }
        
        discovered_contacts = []
        
        # Filter to non-competitor organizations with reasonable relevance
        potential_client_orgs = [
            org for org in ranked_organizations
            if not org.get("is_competitor", False) and org.get("relevance_score", 0) >= 5.0
        ]
        
        # Sort by relevance score (descending)
        sorted_orgs = sorted(
            potential_client_orgs, 
            key=lambda x: x.get("relevance_score", 0),
            reverse=True
        )
        
        # Limit to max_orgs
        target_orgs = sorted_orgs[:max_orgs]
        
        for org_data in target_orgs:
            organization = org_data.get("organization")
            if not organization:
                continue
                
            org_id = organization.get("id")
            org_name = organization.get("name")
            org_type = organization.get("org_type", "").lower()
            
            if not org_id or not org_name:
                continue
                
            logger.info(f"Discovering contacts for {org_name}")
            
            # Get relevant roles for this organization type
            relevant_roles = role_profiles.get(org_type, role_profiles.get("municipal", []))
            
            # Expand role list with synonyms
            expanded_roles = []
            for role in relevant_roles:
                expanded_roles.append(role)
                
                # Add synonym variations
                role_words = role.split()
                for i, word in enumerate(role_words):
                    if word in role_synonyms:
                        for synonym in role_synonyms[word]:
                            new_role = role_words.copy()
                            new_role[i] = synonym
                            expanded_roles.append(" ".join(new_role))
            
            # Get unique expanded roles
            unique_roles = list(set(expanded_roles))
            
            # Get crawled content and discovered URLs for this organization
            urls = self.db_session.query(DiscoveredURL).filter(
                DiscoveredURL.organization_id == org_id
            ).order_by(
                DiscoveredURL.priority_score.desc()
            ).limit(5).all()
            
            # Extract contacts from pages
            for url in urls:
                try:
                    # Skip if no HTML content
                    if not url.html_content:
                        logger.warning(f"No HTML content for URL: {url.url}")
                        continue
                        
                    # Parse HTML
                    soup = BeautifulSoup(url.html_content, "html.parser")
                    logger.info(f"Parsed HTML for URL: {url.url}, content length: {len(url.html_content)}")
                    
                    # First try to extract structured contact data
                    try:
                        structured_contacts = self.web_crawler._extract_structured_contact_data(soup, url.url)
                        logger.info(f"Found {len(structured_contacts)} structured contacts from {url.url}")
                    except Exception as e:
                        logger.error(f"Error extracting structured contacts: {e}")
                        structured_contacts = []
                    
                    # Then try standard contact extraction
                    try:
                        standard_contacts = self.web_crawler._extract_contact_information(
                            soup, url.url, org_name
                        )
                        logger.info(f"Found {len(standard_contacts)} standard contacts from {url.url}")
                    except Exception as e:
                        logger.error(f"Error extracting standard contacts: {e}")
                        standard_contacts = []
                    
                    # Combine contact data
                    contacts = structured_contacts + standard_contacts
                    logger.info(f"Combined {len(contacts)} total contacts from {url.url}")
                    
                    # Update organization in contacts
                    for contact in contacts:
                        contact["organization_id"] = org_id
                        contact["organization_name"] = org_name
                        
                        # Add to database if not exists
                        self.web_crawler._add_contact_to_database(contact)
                    
                    # Add to discovered contacts
                    discovered_contacts.extend(contacts)
                    
                except Exception as e:
                    logger.error(f"Error extracting contacts from {url.url}: {e}")
            
            # Try to discover contacts using Gemini
            try:
                # Combine all HTML content
                combined_html = ""
                for url in urls:
                    if url.html_content:
                        combined_html += f"\n\n{url.html_content}"
                
                if combined_html:
                    # Create prompt for Gemini
                    prompt = f"""
                    Extract potential contacts from this organization's website content that might be involved in 
                    SCADA system decisions or infrastructure management.
                    
                    Organization: {org_name}
                    Organization Type: {org_type}
                    
                    Key roles to look for:
                    {", ".join(relevant_roles[:5])}
                    
                    Extract as many details as possible:
                    - Name
                    - Position/Title
                    - Email (if available)
                    - Phone (if available)
                    - Department
                    
                    Return the data as a JSON list of contacts.
                    """
                    
                    # Call Gemini API
                    result = self.gemini_client.generate_content(
                        prompt, combined_html, temperature=0.2
                    )
                    
                    # Extract contacts from response
                    if result and isinstance(result, str):
                        # Try to parse JSON from response
                        try:
                            # Find JSON part in response
                            json_match = re.search(r'\[\s*\{.*\}\s*\]', result, re.DOTALL)
                            if json_match:
                                json_str = json_match.group(0)
                                gemini_contacts = json.loads(json_str)
                                
                                # Process each contact
                                for contact in gemini_contacts:
                                    # Add organization details
                                    contact["organization_id"] = org_id
                                    contact["organization_name"] = org_name
                                    contact["source"] = "gemini"
                                    
                                    # Add to database
                                    self.web_crawler._add_contact_to_database(contact)
                                    
                                    # Add to discovered contacts
                                    discovered_contacts.append(contact)
                        except:
                            logger.error(f"Failed to parse JSON from Gemini response for {org_name}")
            
            except Exception as e:
                logger.error(f"Error using Gemini to discover contacts for {org_name}: {e}")
            
            # Politeness delay
            time.sleep(random.uniform(0.5, 1.5))
        
        return discovered_contacts
    
    def _update_session_status(self, status: str) -> None:
        """
        Update the status of the current discovery session.
        
        Args:
            status: New status
        """
        if self.session_id:
            try:
                session = self.db_session.query(DiscoverySession).filter(
                    DiscoverySession.id == self.session_id
                ).first()
                
                if session:
                    session.status = status
                    session.end_time = datetime.datetime.now() if status in ["completed", "error"] else None
                    self.db_session.commit()
            except Exception as e:
                logger.error(f"Failed to update session status: {e}")
    
    def _generate_discovery_report(
        self, 
        ranked_organizations: List[Dict[str, Any]],
        contacts: List[Dict[str, Any]],
        metrics: Dict[str, Any]
    ) -> None:
        """
        Generate a report for the discovery process.
        
        Args:
            ranked_organizations: Ranked organizations
            contacts: Discovered contacts
            metrics: Discovery metrics
        """
        try:
            # Create report filename with date
            date_str = datetime.datetime.now().strftime("%Y%m%d")
            filename = f"reports/discovery_{date_str}.txt"
            
            with open(filename, "w") as f:
                f.write("=== SCADA CLIENT DISCOVERY REPORT ===\n")
                f.write(f"Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Session ID: {self.session_id}\n\n")
                
                # Write metrics
                f.write("=== DISCOVERY METRICS ===\n")
                f.write(f"Search Queries Executed: {metrics.get('search_queries_executed', 0)}\n")
                f.write(f"Search Results Found: {metrics.get('search_results_found', 0)}\n")
                f.write(f"URLs Crawled: {metrics.get('urls_crawled', 0)}\n")
                f.write(f"Organizations Discovered: {metrics.get('organizations_discovered', 0)}\n")
                f.write(f"Potential Clients Found: {metrics.get('potential_clients_found', 0)}\n")
                f.write(f"Competitors Filtered: {metrics.get('competitors_filtered', 0)}\n")
                f.write(f"Contacts Discovered: {metrics.get('contacts_discovered', 0)}\n")
                f.write(f"High Relevance Organizations: {metrics.get('high_relevance_orgs', 0)}\n\n")
                
                # Write organization breakdown by type
                f.write("=== ORGANIZATIONS BY TYPE ===\n")
                orgs_by_type = metrics.get("orgs_by_type", {})
                for org_type, count in sorted(orgs_by_type.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"{org_type}: {count}\n")
                f.write("\n")
                
                # Write organization breakdown by state
                f.write("=== ORGANIZATIONS BY STATE ===\n")
                orgs_by_state = metrics.get("orgs_by_state", {})
                for state, count in sorted(orgs_by_state.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"{state}: {count}\n")
                f.write("\n")
                
                # Write top potential clients
                f.write("=== TOP POTENTIAL CLIENTS ===\n")
                potential_clients = [
                    org for org in ranked_organizations
                    if not org.get("is_competitor", False) and org.get("relevance_score", 0) >= 6.0
                ]
                
                # Sort by relevance score
                potential_clients = sorted(
                    potential_clients,
                    key=lambda x: x.get("relevance_score", 0),
                    reverse=True
                )
                
                # Write top 20
                for i, org_data in enumerate(potential_clients[:20], 1):
                    org = org_data.get("organization", {})
                    f.write(f"{i}. {org.get('name', 'Unknown')} ({org.get('org_type', 'Unknown')}, {org.get('state', 'Unknown')})\n")
                    f.write(f"   Relevance Score: {org_data.get('relevance_score', 0):.1f}/10\n")
                    f.write(f"   Infrastructure Score: {org_data.get('infrastructure_score', 0):.1f}/10\n")
                    
                    # Write infrastructure indicators
                    infra_indicators = org_data.get("infrastructure_indicators", {})
                    infra_matches = infra_indicators.get("infrastructure_matches", [])
                    if infra_matches:
                        f.write(f"   Infrastructure Indicators: {', '.join(infra_matches[:5])}\n")
                    
                    f.write("\n")
                
                # Write competitor organizations
                f.write("=== IDENTIFIED COMPETITORS ===\n")
                competitors = [
                    org for org in ranked_organizations
                    if org.get("is_competitor", True)
                ]
                
                # Sort by competitor score
                competitors = sorted(
                    competitors,
                    key=lambda x: x.get("competitor_score", 0),
                    reverse=True
                )
                
                # Write top 10
                for i, org_data in enumerate(competitors[:10], 1):
                    org = org_data.get("organization", {})
                    f.write(f"{i}. {org.get('name', 'Unknown')} ({org.get('org_type', 'Unknown')}, {org.get('state', 'Unknown')})\n")
                    f.write(f"   Competitor Score: {org_data.get('competitor_score', 0):.1f}/10\n")
                    
                    # Write competitor indicators
                    comp_analysis = org_data.get("competitor_analysis", {})
                    comp_indicators = comp_analysis.get("competitor_indicators", [])
                    if comp_indicators:
                        f.write(f"   Competitor Indicators: {', '.join(comp_indicators[:3])}\n")
                    
                    f.write("\n")
                
                # Write contact summary
                f.write("=== DISCOVERED CONTACTS ===\n")
                f.write(f"Total Contacts: {len(contacts)}\n\n")
                
                # Group contacts by organization
                contacts_by_org = {}
                for contact in contacts:
                    org_id = contact.get("organization_id")
                    if org_id:
                        if org_id not in contacts_by_org:
                            contacts_by_org[org_id] = []
                        contacts_by_org[org_id].append(contact)
                
                # Write contacts for top 10 organizations
                top_orgs = [
                    org for org in ranked_organizations
                    if not org.get("is_competitor", False) and org.get("relevance_score", 0) >= 7.0
                ]
                
                top_orgs = sorted(
                    top_orgs,
                    key=lambda x: x.get("relevance_score", 0),
                    reverse=True
                )
                
                for org_data in top_orgs[:10]:
                    org = org_data.get("organization", {})
                    org_id = org.get("id")
                    
                    if org_id and org_id in contacts_by_org:
                        f.write(f"Contacts for {org.get('name', 'Unknown')}:\n")
                        
                        for contact in contacts_by_org[org_id]:
                            name = contact.get("name", "Unknown")
                            title = contact.get("title", "Unknown")
                            email = contact.get("email", "N/A")
                            
                            f.write(f"- {name}, {title}, {email}\n")
                        
                        f.write("\n")
            
            logger.info(f"Discovery report generated: {filename}")
        
        except Exception as e:
            logger.error(f"Failed to generate discovery report: {e}")