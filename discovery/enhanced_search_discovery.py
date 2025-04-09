"""
Enhanced search-based discovery system for the GBL Data Contact Management System.

This module implements a comprehensive search and classification approach for discovering organizations:
1. Uses Google API to search for each category with state-specific keywords
2. Uses Gemini to validate if results are actual organizations of the target type
3. Processes all relevant results from the search, not just the top 10
"""
import time
import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

from sqlalchemy.orm import Session
from app.config import TARGET_STATES, ORG_TYPES, ORGANIZATION_TYPES
from app.database.models import Organization, SearchQuery, SystemMetric
from app.discovery.search.google_search import GoogleSearchClient
from app.discovery.gemini_organization_classifier import GeminiOrganizationClassifier
from app.utils.logger import get_logger

logger = get_logger(__name__)

class EnhancedSearchDiscovery:
    """
    Enhanced discovery system based on search and Gemini classification.
    """
    
    def __init__(self, db_session: Session):
        """
        Initialize the enhanced discovery system.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        self.search_client = GoogleSearchClient(db_session)
        self.classifier = GeminiOrganizationClassifier()
        
        # Metrics tracking
        self.metrics = {
            "search_queries_executed": 0,
            "search_results_found": 0,
            "organizations_discovered": 0,
            "by_category": {},
            "by_state": {}
        }
        
        # Create reports directory if it doesn't exist
        reports_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "reports")
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
    
    def run_discovery(self, categories: List[str] = None, states: List[str] = None, 
                     max_results_per_query: int = 100, max_orgs: int = 50) -> Dict[str, Any]:
        """
        Run the discovery process for specified categories and states.
        
        Args:
            categories: List of categories to search for (if None, all categories in ORGANIZATION_TYPES)
            states: List of states to search in (if None, all TARGET_STATES)
            max_results_per_query: Maximum number of search results per query
            max_orgs: Maximum number of organizations to discover in total
            
        Returns:
            Dictionary with discovery metrics
        """
        start_time = time.time()
        
        # Initialize metrics
        self.metrics = {
            "search_queries_executed": 0,
            "search_results_found": 0,
            "organizations_discovered": 0,
            "by_category": {},
            "by_state": {},
            "start_time": datetime.now().isoformat()
        }
        
        # Determine categories to search
        if categories is None:
            categories = list(ORGANIZATION_TYPES.keys())
            
        # Determine states to search
        if states is None:
            states = TARGET_STATES
            
        logger.info(f"Starting enhanced discovery for categories: {categories} in states: {states}")
        
        orgs_discovered = 0
        
        # Process each category and state
        for category in categories:
            if category not in ORGANIZATION_TYPES:
                logger.warning(f"Unknown category: {category}, skipping")
                continue
                
            # Get search queries for this category
            search_queries = ORGANIZATION_TYPES[category].get("search_queries", [])
            if not search_queries:
                logger.warning(f"No search queries defined for category: {category}, skipping")
                continue
                
            # Process each state
            for state in states:
                # Check if we've reached the maximum organizations limit
                if orgs_discovered >= max_orgs:
                    logger.info(f"Reached maximum organizations limit: {max_orgs}")
                    break
                    
                logger.info(f"Processing {category} in {state}")
                
                # Execute each search query for this category and state
                for query_template in search_queries:
                    # Format query with state
                    query = query_template.format(state=state)
                    
                    logger.info(f"Executing search: {query}")
                    self.metrics["search_queries_executed"] += 1
                    
                    # Track this search in the database
                    search_query_record = SearchQuery(
                        query=query,
                        category=category,
                        state=state,
                        search_engine="google"
                    )
                    self.db_session.add(search_query_record)
                    self.db_session.commit()
                    
                    try:
                        # Get search results (up to max_results_per_query)
                        search_results = self.search_client.get_all_results(query, max_results=max_results_per_query)
                        
                        # Update metrics and search record
                        if search_results:
                            search_query_record.results_count = len(search_results)
                            self.db_session.commit()
                            
                            self.metrics["search_results_found"] += len(search_results)
                            logger.info(f"Found {len(search_results)} search results for query: {query}")
                            
                            # Classify search results using Gemini
                            relevant_orgs = self.classifier.batch_classify(search_results, category, state)
                            
                            # Save relevant organizations to database
                            orgs_saved = self._save_organizations(relevant_orgs)
                            orgs_discovered += orgs_saved
                            
                            # Update metrics
                            self.metrics["organizations_discovered"] += orgs_saved
                            self.metrics["by_category"][category] = self.metrics["by_category"].get(category, 0) + orgs_saved
                            self.metrics["by_state"][state] = self.metrics["by_state"].get(state, 0) + orgs_saved
                            
                            logger.info(f"Saved {orgs_saved} organizations from query: {query}")
                            
                            # Check if we've reached the maximum organizations limit
                            if orgs_discovered >= max_orgs:
                                logger.info(f"Reached maximum organizations limit during search: {max_orgs}")
                                break
                        else:
                            logger.warning(f"No search results found for query: {query}")
                            
                    except Exception as e:
                        logger.error(f"Error processing search query '{query}': {e}")
                        
                    # Add delay between queries to avoid rate limiting
                    time.sleep(2)
                    
                # Check if we've reached the limit after this state
                if orgs_discovered >= max_orgs:
                    break
                    
            # Check if we've reached the limit after this category
            if orgs_discovered >= max_orgs:
                break
                
        # Calculate runtime and add to metrics
        end_time = time.time()
        runtime_seconds = int(end_time - start_time)
        self.metrics["runtime_seconds"] = runtime_seconds
        self.metrics["end_time"] = datetime.now().isoformat()
        
        # Save metrics to database
        self._save_metrics(runtime_seconds)
        
        # Generate report
        self._generate_report()
        
        logger.info(f"Enhanced discovery completed in {runtime_seconds} seconds")
        logger.info(f"Found {self.metrics['organizations_discovered']} organizations")
        
        return self.metrics
    
    def _save_organizations(self, org_data_list: List[Dict[str, Any]]) -> int:
        """
        Save organizations to database if they don't already exist.
        
        Args:
            org_data_list: List of organization data dictionaries
            
        Returns:
            Number of organizations saved
        """
        saved_count = 0
        
        for org_data in org_data_list:
            try:
                # Check if organization already exists by name and state
                from app.database import crud
                existing_org = crud.get_organization_by_name_and_state(
                    self.db_session, org_data["name"], org_data["state"]
                )
                
                if existing_org:
                    logger.info(f"Organization already exists: {org_data['name']} in {org_data['state']}")
                    
                    # Update relevance score if new one is higher
                    if org_data.get("relevance_score", 0) > existing_org.relevance_score:
                        existing_org.relevance_score = org_data["relevance_score"]
                        self.db_session.commit()
                        logger.info(f"Updated relevance score for {org_data['name']}")
                        
                    continue
                
                # Add discovery timestamp
                org_data["discovery_date"] = datetime.now()
                
                # Create new organization
                new_org = crud.create_organization(self.db_session, org_data)
                logger.info(f"Created new organization: {new_org.name} in {new_org.state}")
                saved_count += 1
                
            except Exception as e:
                logger.error(f"Error saving organization {org_data.get('name', 'Unknown')}: {e}")
                
        return saved_count
    
    def _save_metrics(self, runtime_seconds: int) -> None:
        """
        Save metrics to the database.
        
        Args:
            runtime_seconds: Runtime in seconds
        """
        try:
            # Create metric record
            metric = SystemMetric(
                metric_date=datetime.now(),
                organizations_discovered=self.metrics["organizations_discovered"],
                search_queries_executed=self.metrics["search_queries_executed"],
                runtime_seconds=runtime_seconds
            )
            
            self.db_session.add(metric)
            self.db_session.commit()
            
        except Exception as e:
            logger.error(f"Error saving metrics to database: {e}")
    
    def _generate_report(self) -> None:
        """Generate a report file with the discovery metrics."""
        try:
            # Create report filename with date
            date_str = datetime.now().strftime("%Y%m%d")
            report_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                "reports", 
                f"discovery_{date_str}.txt"
            )
            
            # Format the report content
            report_content = f"""
Discovery Report - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
=============================================================

Summary:
- Executed {self.metrics["search_queries_executed"]} search queries
- Found {self.metrics["search_results_found"]} search results
- Discovered {self.metrics["organizations_discovered"]} organizations
- Runtime: {self.metrics.get("runtime_seconds", 0)} seconds

Organizations by Category:
{self._format_dict(self.metrics["by_category"])}

Organizations by State:
{self._format_dict(self.metrics["by_state"])}
"""
            
            # Write the report
            with open(report_path, "a") as f:
                f.write(report_content)
                
            logger.info(f"Discovery report saved to {report_path}")
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
    
    def _format_dict(self, data: Dict[str, Any]) -> str:
        """Format a dictionary for the report."""
        return "\n".join([f"- {k}: {v}" for k, v in data.items()])