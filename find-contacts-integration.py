"""
Integration module to add enhanced website validation and municipal contact crawling
to the existing find_contacts.py script.
"""
import argparse
import sys
from pathlib import Path
import logging
import time

# Add parent directory to path for imports
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from app.database.models import get_db_session, Organization, Contact
from app.utils.logger import get_logger

# Import the enhanced components
from app.website_validator import WebsiteValidator
from app.municipal_contact_crawler import MunicipalContactCrawler
from app.contact_discovery_integration import EnhancedContactDiscovery, enhance_contact_discovery

logger = get_logger(__name__)

def run_enhanced_contact_discovery(max_orgs: int = 20, min_relevance: float = 5.0):
    """
    Run the enhanced contact discovery process.
    
    Args:
        max_orgs: Maximum number of organizations to process
        min_relevance: Minimum relevance score for organizations
    """
    # Get database session
    db_session = get_db_session()
    
    try:
        logger.info(f"Starting enhanced contact discovery for {max_orgs} organizations")
        
        # Create the enhanced discovery processor
        discovery = EnhancedContactDiscovery(db_session)
        
        # Run the discovery process
        metrics = discovery.run_discovery_for_organizations(max_orgs, min_relevance)
        
        # Log summary
        logger.info("Enhanced contact discovery completed")
        logger.info(f"Organizations processed: {metrics['organizations_processed']}")
        logger.info(f"Websites validated: {metrics['websites_validated']}")
        logger.info(f"Websites found: {metrics['websites_found']}")
        logger.info(f"Contacts discovered: {metrics['contacts_discovered']}")
        
        # Log discovery methods
        logger.info("Contact discovery methods:")
        for method, count in metrics["by_method"].items():
            logger.info(f"  {method}: {count}")
        
        # Log organization types
        logger.info("Contacts by organization type:")
        for org_type, count in metrics["by_org_type"].items():
            logger.info(f"  {org_type}: {count}")
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error in enhanced contact discovery: {e}")
        return None
        
    finally:
        db_session.close()


def test_website_validator():
    """Test the website validator with some example cases."""
    validator = WebsiteValidator()
    
    test_cases = [
        # (url, org_name, state)
        ("https://www.bcnv.org/", "Boulder City", "Nevada"),
        ("https://www.lasvegasnevada.gov/", "Las Vegas", "Nevada"),
        ("https://townofpahrump.com/", "Pahrump", "Nevada"),
        ("https://douglascountynv.gov/", "Douglas County", "Nevada"),
        ("https://co.washoe.nv.us/", "Washoe County", "Nevada"),
        ("https://www.facebook.com/CityofHenderson/", "Henderson", "Nevada"),  # Should fail
    ]
    
    logger.info("Testing website validator...")
    for url, org_name, state in test_cases:
        is_valid, confidence = validator.validate_org_website(url, org_name, state)
        result = "✓" if is_valid else "✗"
        logger.info(f"{result} {url} for '{org_name}' ({confidence:.2f})")


def test_municipal_crawler():
    """Test the municipal contact crawler with a known website."""
    # Get database session
    db_session = get_db_session()
    
    try:
        # Create municipal crawler
        crawler = MunicipalContactCrawler(db_session)
        
        # Get an example municipal organization
        org = db_session.query(Organization).filter(
            Organization.org_type.in_(["municipal", "government", "water"]),
            Organization.website.isnot(None)
        ).first()
        
        if not org:
            logger.error("No suitable organization found for testing")
            return
        
        logger.info(f"Testing municipal crawler with {org.name}, website: {org.website}")
        
        # Discover contacts
        contacts = crawler.discover_contacts(org)
        
        logger.info(f"Discovered {len(contacts)} contacts for {org.name}")
        for i, contact in enumerate(contacts):
            logger.info(f"{i+1}. {contact.first_name} {contact.last_name}, {contact.job_title}, {contact.email}")
        
    except Exception as e:
        logger.error(f"Error in municipal crawler test: {e}")
        
    finally:
        db_session.close()


def main():
    """Main function for the enhanced contact discovery module."""
    parser = argparse.ArgumentParser(description="Enhanced Contact Discovery")
    parser.add_argument("--max-orgs", type=int, default=20, help="Maximum organizations to process")
    parser.add_argument("--min-relevance", type=float, default=5.0, help="Minimum relevance score")
    parser.add_argument("--test-validator", action="store_true", help="Test the website validator")
    parser.add_argument("--test-crawler", action="store_true", help="Test the municipal crawler")
    
    args = parser.parse_args()
    
    if args.test_validator:
        test_website_validator()
    elif args.test_crawler:
        test_municipal_crawler()
    else:
        run_enhanced_contact_discovery(args.max_orgs, args.min_relevance)


if __name__ == "__main__":
    main()
