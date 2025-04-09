"""
Integration module for enhanced contact discovery.

This module integrates:
1. Website validation to better detect official organization websites
2. Specialized municipal website crawler to find staff/contact directories
3. Improved email extraction and validation
4. Fallback contact discovery for positions and title-based searching
"""

import logging
import time
import random
from typing import Dict, List, Any, Optional

from app.database.models import Organization, Contact, DiscoveredURL
from app.utils.logger import get_logger
from app.website_validator import validate_website, find_contact_urls
from app.municipal_contact_crawler import crawl_municipal_website, extract_municipal_contacts
from app.discovery.fallback_contact_discovery import FallbackContactDiscovery
from app.validation.email_validator import EmailValidator
from app.utils.gemini_client import GeminiClient
from app.database.validated_crud import ValidatedCrud

logger = get_logger(__name__)

def enhance_contact_discovery(db_session, max_orgs: int = 20, use_municipal_crawler: bool = True, use_fallback: bool = True) -> Dict[str, Any]:
    """
    Enhanced contact discovery that combines multiple strategies for better results.
    
    Args:
        db_session: Database session
        max_orgs: Maximum number of organizations to process
        use_municipal_crawler: Whether to use the specialized municipal crawler
        use_fallback: Whether to use advanced fallback discovery for position-based searching
        
    Returns:
        Dictionary of metrics for the discovery process
    """
    logger.info(f"Starting enhanced contact discovery for up to {max_orgs} organizations")
    
    # Initialize fallback discovery system if enabled
    fallback_discovery = None
    if use_fallback:
        # Get API key from config
        import app.config as config
        gemini_api_key = getattr(config, 'GEMINI_API_KEY', None)
        
        # Initialize required components
        email_validator = EmailValidator(db_session)
        gemini_client = None
        
        if gemini_api_key:
            gemini_client = GeminiClient(api_key=gemini_api_key)
        else:
            logger.warning("No Gemini API key found, fallback discovery will have limited capabilities")
            
        fallback_discovery = FallbackContactDiscovery(email_validator, gemini_client)
    
    metrics = {
        "orgs_processed": 0,
        "contacts_discovered": 0,
        "websites_validated": 0,
        "municipal_sites_found": 0,
        "contact_pages_found": 0,
        "fallback_discoveries": 0,
        "position_based_contacts": 0
    }
    
    try:
        # Check important organizations have website data
        special_orgs = [
            {"name": "Boulder City", "state": "Nevada", "website": "https://www.bcnv.org"},
            {"name": "Arizona Department of Water Resources", "state": "Arizona", "website": "https://new.azwater.gov"}
        ]
        
        for org_data in special_orgs:
            special_org = db_session.query(Organization).filter(
                Organization.name == org_data["name"],
                Organization.state == org_data["state"]
            ).first()
            
            if special_org and not special_org.website:
                logger.info(f"Adding missing website for {org_data['name']}: {org_data['website']}")
                special_org.website = org_data["website"]
                db_session.commit()
        
        # Query for organizations with few or no contacts
        from sqlalchemy import func
        
        orgs_query = db_session.query(
            Organization,
            func.count(Contact.id).label("contact_count")
        ).outerjoin(
            Contact,
            Contact.organization_id == Organization.id
        ).filter(
            Organization.website.isnot(None),   # Has a website
            Organization.website != ''          # Website is not empty
        ).group_by(
            Organization.id
        ).having(
            func.count(Contact.id) < 3          # Fewer than 3 contacts
        ).order_by(
            Organization.relevance_score.desc() # Most relevant first
        ).limit(max_orgs)
        
        orgs_with_few_contacts = orgs_query.all()
        logger.info(f"Found {len(orgs_with_few_contacts)} organizations with websites and few contacts")
        
        # Process each organization
        all_discovered_contacts = []
        
        for org_tuple in orgs_with_few_contacts:
            org, contact_count = org_tuple
            logger.info(f"Processing {org.name} ({org.org_type}) - Has {contact_count} contacts")
            metrics["orgs_processed"] += 1
            
            # Skip if no website
            if not org.website:
                logger.warning(f"No website for {org.name} despite database query filter")
                continue
                
            # Validate website and find contact URLs
            website = org.website
            validated_website, contact_urls = validate_website(website)
            metrics["websites_validated"] += 1
            
            if validated_website != website:
                logger.info(f"Updated website URL for {org.name}: {website} -> {validated_website}")
                org.website = validated_website
                db_session.commit()
            
            # If we found contact URLs, add them to the database
            if contact_urls:
                logger.info(f"Found {len(contact_urls)} contact URLs for {org.name}")
                metrics["contact_pages_found"] += len(contact_urls)
                
                # Add these URLs to the database
                for url in contact_urls:
                    # Check if URL already exists
                    existing_url = db_session.query(DiscoveredURL).filter(
                        DiscoveredURL.organization_id == org.id,
                        DiscoveredURL.url == url
                    ).first()
                    
                    if not existing_url:
                        # Add new URL
                        url_record = DiscoveredURL(
                            organization_id=org.id,
                            url=url,
                            page_type="contact",
                            title=f"Contact page for {org.name}",
                            description=f"Discovered contact page for {org.name}",
                            contains_contact_info=True,
                            priority_score=0.9
                        )
                        db_session.add(url_record)
                
                # Commit changes
                db_session.commit()
            
            # If this looks like a municipal website and use_municipal_crawler is enabled
            if use_municipal_crawler and is_municipal_site(org):
                logger.info(f"Crawling municipal website for {org.name}: {org.website}")
                metrics["municipal_sites_found"] += 1
                
                municipal_contacts = crawl_municipal_website(org.website, org.name, org.id)
                
                if municipal_contacts:
                    logger.info(f"Found {len(municipal_contacts)} contacts from municipal crawler for {org.name}")
                    all_discovered_contacts.extend(municipal_contacts)
                    
                    # Create and use ValidatedCrud for contact validation
                    validated_crud = ValidatedCrud(db_session)
                    
                    # Add contacts to database with validation
                    valid_contacts = 0
                    for contact_data in municipal_contacts:
                        contact_dict = {
                            "organization_id": org.id,
                            "first_name": contact_data.get("first_name", ""),
                            "last_name": contact_data.get("last_name", ""),
                            "job_title": contact_data.get("job_title", ""),
                            "email": contact_data.get("email", ""),
                            "phone": contact_data.get("phone", ""),
                            "discovery_method": "municipal_crawler",
                            "discovery_url": contact_data.get("discovery_url", org.website),
                            "contact_confidence_score": contact_data.get("confidence", 0.8),
                            "contact_relevance_score": contact_data.get("relevance", 7.0),
                            "email_valid": bool(contact_data.get("email", "")),
                            "contact_type": "actual"
                        }
                        
                        # Use validated_crud to create contact with validation
                        contact, validated, reason = validated_crud.create_contact(contact_dict)
                        if contact:
                            valid_contacts += 1
                        else:
                            logger.info(f"Validation rejected contact: {contact_dict['first_name']} {contact_dict['last_name']} - {reason}")
                    
                    logger.info(f"Added {valid_contacts} validated contacts for {org.name} (rejected: {len(municipal_contacts) - valid_contacts})")
                    metrics["contacts_discovered"] += valid_contacts
            
            # Process each contact page
            for url in contact_urls:
                try:
                    page_contacts = extract_municipal_contacts(url, org.name, org.id)
                    
                    if page_contacts:
                        logger.info(f"Found {len(page_contacts)} contacts from contact page {url} for {org.name}")
                        all_discovered_contacts.extend(page_contacts)
                        
                        # Create and use ValidatedCrud for contact validation if not already created
                        if not validated_crud:
                            validated_crud = ValidatedCrud(db_session)
                        
                        # Add these contacts to database with validation
                        valid_contacts = 0
                        for contact_data in page_contacts:
                            contact_dict = {
                                "organization_id": org.id,
                                "first_name": contact_data.get("first_name", ""),
                                "last_name": contact_data.get("last_name", ""),
                                "job_title": contact_data.get("job_title", ""),
                                "email": contact_data.get("email", ""),
                                "phone": contact_data.get("phone", ""),
                                "discovery_method": "contact_page",
                                "discovery_url": url,
                                "contact_confidence_score": contact_data.get("confidence", 0.85),
                                "contact_relevance_score": contact_data.get("relevance", 7.5),
                                "email_valid": bool(contact_data.get("email", "")),
                                "contact_type": "actual"
                            }
                            
                            # Use validated_crud to create contact with validation
                            contact, validated, reason = validated_crud.create_contact(contact_dict)
                            if contact:
                                valid_contacts += 1
                            else:
                                logger.info(f"Validation rejected contact: {contact_dict['first_name']} {contact_dict['last_name']} - {reason}")
                        
                        logger.info(f"Added {valid_contacts} validated contacts from {url} (rejected: {len(page_contacts) - valid_contacts})")
                        metrics["contacts_discovered"] += valid_contacts
                except Exception as e:
                    logger.error(f"Error processing contact page {url}: {e}")
            
            # Try fallback discovery if enabled and no contacts found through crawling
            if use_fallback and fallback_discovery and not all_discovered_contacts:
                logger.info(f"Attempting fallback discovery with position-based searching for {org.name}")
                metrics["fallback_discoveries"] += 1
                
                # Try to discover contacts based on common positions in this type of organization
                try:
                    # Check which method is available and use that one
                    if hasattr(fallback_discovery, 'discover_from_position_search'):
                        position_contacts = fallback_discovery.discover_from_position_search(org)
                    elif hasattr(fallback_discovery, 'discover_contacts'):
                        # Prepare organization data for fallback discovery
                        org_data = {
                            "id": org.id,
                            "name": org.name,
                            "org_type": org.org_type,
                            "city": org.city,
                            "state": org.state,
                            "website": org.website,
                            "location": f"{org.city}, {org.state}" if org.city and org.state else (org.city or org.state or "")
                        }
                        position_contacts = fallback_discovery.discover_contacts(org_data, min_contacts=3)
                    else:
                        logger.error(f"No compatible method found in FallbackContactDiscovery for {org.name}")
                        position_contacts = []
                    
                    if position_contacts:
                        logger.info(f"Found {len(position_contacts)} contacts via fallback position-based discovery for {org.name}")
                        
                        # Create validated CRUD if not already created
                        if not validated_crud:
                            validated_crud = ValidatedCrud(db_session)
                        
                        # Add these contacts to database with validation
                        valid_contacts = 0
                        for contact in position_contacts:
                            # Convert to dictionary for validation service
                            contact_dict = {
                                "organization_id": org.id,
                                "first_name": contact.first_name,
                                "last_name": contact.last_name,
                                "job_title": contact.job_title,
                                "email": contact.email,
                                "phone": contact.phone,
                                "discovery_method": "position_search",
                                "discovery_url": contact.discovery_url,
                                "contact_confidence_score": contact.contact_confidence_score,
                                "contact_relevance_score": contact.contact_relevance_score,
                                "email_valid": contact.email_valid,
                                "contact_type": "actual"
                            }
                            
                            # Use validated_crud to create contact with validation
                            contact_obj, validated, reason = validated_crud.create_contact(contact_dict)
                            if contact_obj:
                                valid_contacts += 1
                            else:
                                logger.info(f"Validation rejected contact: {contact_dict['first_name']} {contact_dict['last_name']} - {reason}")
                        
                        logger.info(f"Added {valid_contacts} validated contacts from position search (rejected: {len(position_contacts) - valid_contacts})")
                        metrics["contacts_discovered"] += valid_contacts
                        metrics["position_based_contacts"] += valid_contacts
                        
                        # Commit any remaining changes
                        db_session.commit()
                except Exception as e:
                    logger.error(f"Error in fallback position-based discovery: {e}")
            
            # Commit all contacts for this organization
            try:
                db_session.commit()
            except Exception as e:
                logger.error(f"Error committing contacts to database: {e}")
                db_session.rollback()
            
            # Add a small delay to avoid overwhelming servers
            time.sleep(random.uniform(0.5, 1.5))
        
        # Update metrics
        metrics["contacts_discovered"] = len(all_discovered_contacts)
        
        logger.info(f"Enhanced contact discovery completed. Found {metrics['contacts_discovered']} contacts "
                  f"across {metrics['orgs_processed']} organizations")
        
        return metrics
    
    except Exception as e:
        logger.error(f"Error in enhanced contact discovery: {e}")
        return metrics


def is_municipal_site(org):
    """
    Determine if an organization is likely a municipal government site.
    
    Args:
        org: Organization object
        
    Returns:
        Boolean indicating if this is likely a municipal site
    """
    # Check organization type
    if org.org_type and org.org_type.lower() in ['municipal', 'government', 'water', 'utility']:
        return True
    
    # Check if website has common municipal TLDs
    if org.website:
        if '.gov' in org.website or '.us' in org.website:
            return True
        
        # Check for common municipal terms in the name
        municipal_terms = ['city of', 'town of', 'county of', 'village of', 'borough of', 
                          'municipality', 'municipal', 'district']
        
        if org.name and any(term in org.name.lower() for term in municipal_terms):
            return True
    
    return False