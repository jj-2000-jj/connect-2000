"""
Discovery Application for the GBL Data Contact Management System.

This application focuses on:
1. Discovering new organizations across target industries
2. Finding contacts within these organizations
3. Validating and storing contact information
4. Building a comprehensive database for outreach
"""
import argparse
import sys
import threading
import schedule
import time
import os
from datetime import datetime
from pathlib import Path

# Set OpenBLAS environment variables to prevent threading conflicts
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['VECLIB_MAXIMUM_THREADS'] = '1'
os.environ['NUMEXPR_NUM_THREADS'] = '1'

from app.database.models import init_db, get_db_session
from app.discovery.discovery_manager import DiscoveryManager
from app.discovery.enhanced_discovery_manager import EnhancedDiscoveryManager
from app.discovery.enhanced_search_discovery import EnhancedSearchDiscovery
from app.dashboard.dashboard_wrapper import DashboardWrapper as Dashboard
from app.utils.logger import get_logger
from app.contact_discovery_integration import enhance_contact_discovery
from app.discovery.fallback_contact_discovery import FallbackContactDiscovery
from app.validation.email_validator import EmailValidator
from app.utils.gemini_client import GeminiClient

logger = get_logger(__name__)


def setup_database():
    """Set up the database and return a session."""
    from app.config import DATABASE_PATH
    
    # Create database directory if it doesn't exist
    DATABASE_PATH.parent.mkdir(exist_ok=True)
    
    # Initialize database
    engine, SessionLocal = init_db()
    return SessionLocal()


def run_discovery(max_orgs=1000, use_enhanced=True, use_search_discovery=False, contact_focus=False, use_municipal_crawler=True, target_org_types=None, target_states=None):
    """Run discovery process to find new organizations and contacts."""
    logger.info("Starting discovery process")
    logger.info(f"Running with max_orgs={max_orgs}, use_enhanced={use_enhanced}, use_search_discovery={use_search_discovery}, contact_focus={contact_focus}, use_municipal_crawler={use_municipal_crawler}")
    if target_org_types:
        logger.info(f"Targeting specific organization types: {target_org_types}")
    if target_states:
        logger.info(f"Targeting specific states: {target_states}")
    
    # Get database session
    db_session = get_db_session()
    
    try:
        if use_search_discovery:
            # Use enhanced search-based discovery with Gemini classification
            logger.info("Using enhanced search-based discovery with Gemini classification")
            
            # Parse target_org_types if provided as string
            categories = None
            if target_org_types:
                if isinstance(target_org_types, str):
                    categories = target_org_types.split(',')
                else:
                    categories = target_org_types
                    
            # Parse target_states if provided as string
            states = None
            if target_states:
                if isinstance(target_states, str):
                    states = target_states.split(',')
                else:
                    states = target_states
            
            # Create enhanced search discovery
            discovery_manager = EnhancedSearchDiscovery(db_session)
            
            # Run discovery
            metrics = discovery_manager.run_discovery(
                categories=categories,
                states=states,
                max_orgs=max_orgs
            )
            
            # Return metrics
            return metrics
            
        elif use_enhanced:
            # Use enhanced discovery manager with multi-stage pipeline
            logger.info("Using enhanced discovery manager with multi-stage pipeline")
            import app.config as config
            discovery_manager = EnhancedDiscoveryManager(db_session, config, target_org_types=target_org_types, target_states=target_states)
            
            if contact_focus:
                # Run contact discovery on existing organizations
                logger.info("Running in contact-focused mode - focusing on finding contacts for existing organizations")
                
                # Initialize metrics dictionary
                metrics = {
                    "organizations_discovered": 0,
                    "contacts_discovered": 0,
                    "by_source": {},
                    "by_organization_type": {},
                    "by_state": {}
                }
                
                # Use enhanced contact discovery with Gemini website validation, municipal crawler, and fallback discovery
                logger.info("Using enhanced contact discovery with Gemini website validation and municipal crawler")
                contact_metrics = enhance_contact_discovery(db_session, max_orgs=max_orgs, use_municipal_crawler=use_municipal_crawler, use_fallback=True)
                if contact_metrics:
                    metrics["contacts_discovered"] = contact_metrics.get("contacts_discovered", 0)
                    metrics["position_based_contacts"] = contact_metrics.get("position_based_contacts", 0)
                    metrics["fallback_discoveries"] = contact_metrics.get("fallback_discoveries", 0)
                    logger.info(f"Enhanced contact discovery found {metrics['contacts_discovered']} contacts")
                    if contact_metrics.get("position_based_contacts", 0) > 0:
                        logger.info(f"Including {contact_metrics.get('position_based_contacts', 0)} contacts from position-based search")
                    
                    # If we found a good number of contacts, return the metrics
                    if metrics["contacts_discovered"] >= 10:
                        return metrics
                
                # Continue using enhanced contact discovery for each organization
                logger.info("Using enhanced contact discovery for each organization individually")
                
                # Get the ID of the last organization processed
                last_processed_org_id = None
                try:
                    # Check if there's a session tracking file
                    import os
                    last_session_file = "data/last_contact_session.txt"
                    if os.path.exists(last_session_file):
                        with open(last_session_file, "r") as f:
                            last_processed_org_id = int(f.read().strip())
                            logger.info(f"Found last processed organization ID: {last_processed_org_id}")
                except Exception as e:
                    logger.warning(f"Error reading last session file: {e}")
                
                # Build the query for organizations with few or no contacts
                from sqlalchemy import func, desc
                from app.database.models import Organization, Contact
                
                # Process organizations in smaller batches for better control
                batch_size = 5  # Process 5 organizations at a time
                total_processed = 0
                all_contacts = []
                
                while total_processed < max_orgs:
                    # Query the next batch of organizations
                    query = db_session.query(
                        Organization, 
                        func.count(Contact.id).label("contact_count")
                    ).outerjoin(
                        Contact,
                        Contact.organization_id == Organization.id
                    ).filter(
                        Organization.website.isnot(None)  # Filter only orgs with websites
                    ).group_by(
                        Organization.id
                    ).having(
                        func.count(Contact.id) < 3  # Target orgs with fewer than 3 contacts
                    )
                    
                    # If we have a last processed org ID, start after that one
                    if last_processed_org_id:
                        query = query.filter(Organization.id > last_processed_org_id)
                        logger.info(f"Starting contact discovery with organizations after ID {last_processed_org_id}")
                    
                    # Order by relevance score to process most relevant first
                    orgs_batch = query.order_by(
                        Organization.relevance_score.desc()  # Process most relevant orgs first
                    ).limit(batch_size).all()
                    
                    # If we didn't find any, try with different criteria
                    if not orgs_batch:
                        logger.info("No more organizations with websites and few contacts found with current criteria, trying new criteria")
                        orgs_batch = db_session.query(
                            Organization, 
                            func.count(Contact.id).label("contact_count")
                        ).outerjoin(
                            Contact,
                            Contact.organization_id == Organization.id
                        ).filter(
                            Organization.website.isnot(None),   # Filter only orgs with websites
                            Organization.website != ''          # And make sure it's not an empty string
                        ).group_by(
                            Organization.id
                        ).having(
                            func.count(Contact.id) < 3          # Target orgs with fewer than 3 contacts
                        ).order_by(
                            Organization.relevance_score.desc() # Start with most relevant orgs
                        ).limit(batch_size).all()
                        
                        # If we STILL didn't find any, break out of the loop
                        if not orgs_batch:
                            logger.info("No more organizations with few contacts found, ending contact discovery")
                            break
                    
                    # Process this batch with enhanced contact discovery
                    logger.info(f"Processing batch of {len(orgs_batch)} organizations with enhanced contact discovery")
                    org_ids = [org.id for org, _ in orgs_batch]
                    
                    # Call enhanced contact discovery for just this batch
                    contact_metrics = enhance_contact_discovery(db_session, max_orgs=len(orgs_batch), 
                                                               use_municipal_crawler=use_municipal_crawler, 
                                                               use_fallback=True)
                    
                    # Update overall metrics
                    if contact_metrics:
                        metrics["contacts_discovered"] += contact_metrics.get("contacts_discovered", 0)
                        metrics["position_based_contacts"] = metrics.get("position_based_contacts", 0) + contact_metrics.get("position_based_contacts", 0)
                        metrics["fallback_discoveries"] = metrics.get("fallback_discoveries", 0) + contact_metrics.get("fallback_discoveries", 0)
                        logger.info(f"Enhanced contact discovery found {contact_metrics.get('contacts_discovered', 0)} contacts in this batch")
                    
                    # Update last processed org ID
                    if orgs_batch:
                        last_org, _ = orgs_batch[-1]
                        last_processed_org_id = last_org.id
                        
                        # Save to file
                        try:
                            with open(last_session_file, "w") as f:
                                f.write(str(last_processed_org_id))
                        except Exception as e:
                            logger.error(f"Error saving last session file: {e}")
                    
                    # Update total processed count
                    total_processed += len(orgs_batch)
                    
                    # If we've processed enough organizations, break out of the loop
                    if total_processed >= max_orgs:
                        break
                
                # Get the final list of all organizations processed in all batches
                orgs_with_few_contacts = []  # We'll build this from our query results
                
                # Since we processed organizations in batches, we need to get all the ones we processed
                # This is just to maintain compatibility with the rest of the function
                try:
                    if last_processed_org_id:
                        # Get all organizations up to the last processed ID
                        orgs_with_few_contacts = db_session.query(
                            Organization, 
                            func.count(Contact.id).label("contact_count")
                        ).outerjoin(
                            Contact,
                            Contact.organization_id == Organization.id
                        ).filter(
                            Organization.id <= last_processed_org_id,  # All orgs up to the last one we processed
                            Organization.website.isnot(None)           # With websites
                        ).group_by(
                            Organization.id
                        ).order_by(
                            Organization.relevance_score.desc()        # Order by relevance for display
                        ).limit(max_orgs).all()
                except Exception as e:
                    logger.error(f"Error getting final list of processed organizations: {e}")
                
                logger.info(f"Found {len(orgs_with_few_contacts)} organizations for contact discovery")
                
                # Extract organizations
                target_orgs = [org for org, _ in orgs_with_few_contacts]
                
                # Create role profiles for each organization type
                from app.discovery.discovery_manager_improved import ImprovedDiscoveryManager
                improved_manager = ImprovedDiscoveryManager(db_session)
                taxonomy = improved_manager._generate_organization_taxonomy()
                role_profiles = improved_manager._create_role_profiles(taxonomy)
                # No need to save checkpoint for this function
                
                # Create a real contact extraction function
                def discover_contacts_for_org(org, profiles):
                    """
                    Enhanced contact discovery function that distinguishes between actual and generic contacts.
                    
                    Args:
                        org: Organization object
                        profiles: Role profiles dictionary
                        
                    Returns:
                        List of Contact objects
                    """
                    from app.database.models import Contact, DiscoveredURL
                    from app.utils.email_extractor import extract_emails_improved
                    
                    contacts = []
                    actual_contacts = []  # Track how many real contacts we find
                    logger.info(f"Discovering contacts for {org.name} with enhanced methods")
                    
                    # 1. Try to discover using existing URLs
                    org_urls = db_session.query(DiscoveredURL).filter(
                        DiscoveredURL.organization_id == org.id
                    ).all()
                    
                    # If no URLs found and we have a website, add it
                    if not org_urls and org.website:
                        new_url = DiscoveredURL(
                            url=org.website,
                            organization_id=org.id,
                            title=org.name,
                            description=f"Website for {org.name}",
                            page_type="homepage"
                        )
                        db_session.add(new_url)
                        db_session.commit()
                        org_urls = [new_url]
                    
                    # Process each URL
                    try:
                        for url in org_urls:
                            # Make sure URL has valid URL
                            if not url.url:
                                continue
                            
                            # Use our improved email extraction
                            emails = extract_emails_improved(url.url)
                            if emails:
                                logger.info(f"Found {len(emails)} emails from {url.url}: {', '.join(emails)}")
                                
                                # Process each email into a contact
                                for email in emails:
                                    # Clean the email
                                    email = email.strip().lower()
                                    
                                    # Determine if it's a role-based email
                                    is_role_email = any(role in email for role in ['info@', 'contact@', 'admin@', 'operations@', 'director@'])
                                    
                                    # Determine contact type based on extraction method
                                    # If the email matches a pattern we generated, it's generic
                                    domain_part = email.split('@')[1] if '@' in email else ""
                                    org_domain = extract_domain_from_url(org.website)
                                    
                                    # If this appears to be a generated domain-based email, mark it as generic
                                    is_generic = False
                                    if domain_part == org_domain and is_role_email:
                                        # These common patterns are likely our generated ones, not discovered ones
                                        common_generic_patterns = ['info@', 'contact@', 'admin@', 'operations@']
                                        if any(email.startswith(pattern) for pattern in common_generic_patterns):
                                            is_generic = True
                                    
                                    # Set contact type
                                    contact_type = "generic" if is_generic else "actual"
                                    
                                    if is_role_email:
                                        # Handle role-based emails
                                        role_type = email.split('@')[0]
                                        
                                        if 'info' in role_type:
                                            first_name = "Information"
                                            last_name = "Office"
                                            job_title = "General Contact"
                                            relevance = 6.0
                                        elif 'contact' in role_type:
                                            first_name = "Contact"
                                            last_name = "Office"
                                            job_title = "Customer Service"
                                            relevance = 6.0
                                        elif 'admin' in role_type:
                                            first_name = "Administration"
                                            last_name = "Office"
                                            job_title = "Administrative Director"
                                            relevance = 6.5
                                        elif 'operations' in role_type:
                                            first_name = "Operations"
                                            last_name = "Manager"
                                            job_title = "Operations Manager"
                                            relevance = 7.0
                                        elif 'director' in role_type:
                                            first_name = "Director"
                                            last_name = org.name.split()[0] if org.name else ""
                                            job_title = f"{org.org_type.title() if org.org_type else 'Department'} Director"
                                            relevance = 7.5
                                        else:
                                            first_name = role_type.capitalize()
                                            last_name = "Office"
                                            job_title = f"{role_type.capitalize()} Contact"
                                            relevance = 5.5
                                    else:
                                        # For non-role emails, extract names from the email
                                        email_parts = email.split('@')[0].split('.')
                                        first_name = email_parts[0].capitalize() if email_parts else ""
                                        last_name = email_parts[1].capitalize() if len(email_parts) > 1 else ""
                                        
                                        # Set default job title and relevance
                                        job_title = "Staff Member"
                                        relevance = 5.0
                                        
                                        # Try to improve job title based on email
                                        for profile in profiles:
                                            if profile.get("role_title", "").lower() in email:
                                                job_title = profile.get("role_title")
                                                relevance = profile.get("relevance_score", 5.0)
                                                break
                                    
                                    # Create contact
                                    contact = Contact(
                                        organization_id=org.id,
                                        first_name=first_name,
                                        last_name=last_name,
                                        job_title=job_title,
                                        email=email,
                                        discovery_method="improved_extraction",
                                        discovery_url=url.url,
                                        contact_confidence_score=0.8,
                                        contact_relevance_score=relevance,
                                        email_valid=True,
                                        contact_type=contact_type  # Set the contact type field
                                    )
                                    
                                    db_session.add(contact)
                                    contacts.append(contact)
                                    
                                    # Track actual contacts
                                    if contact_type == "actual":
                                        actual_contacts.append(contact)
                    except Exception as e:
                        logger.error(f"Error processing URL {url.url}: {e}")
                    
                    # If no actual contacts were found, create generic ones based on domain
                    if not actual_contacts and org.website:
                        # Extract domain
                        try:
                            domain = extract_domain_from_url(org.website)
                            
                            if domain:
                                # Create generic contacts
                                generic_contacts = []
                                
                                # Always add info@ contact
                                info_contact = Contact(
                                    organization_id=org.id,
                                    first_name="Information",
                                    last_name="Office",
                                    job_title="General Contact",
                                    email=f"info@{domain}",
                                    discovery_method="generic_domain",
                                    discovery_url=org.website,
                                    contact_confidence_score=0.75,
                                    contact_relevance_score=6.0,
                                    email_valid=True,
                                    contact_type="generic"  # Mark this as a generic contact
                                )
                                generic_contacts.append(info_contact)
                                
                                # If it's a water/utility type, add more specialized contacts
                                if org.org_type and org.org_type.lower() in ['water', 'wastewater', 'utility']:
                                    # Add operations contact
                                    operations_contact = Contact(
                                        organization_id=org.id,
                                        first_name="Operations",
                                        last_name="Manager",
                                        job_title="Operations Manager",
                                        email=f"operations@{domain}",
                                        discovery_method="generic_domain",
                                        discovery_url=org.website,
                                        contact_confidence_score=0.75,
                                        contact_relevance_score=7.0,
                                        email_valid=True,
                                        contact_type="generic"  # Mark this as a generic contact
                                    )
                                    generic_contacts.append(operations_contact)
                                
                                # Add all generic contacts
                                for contact in generic_contacts:
                                    db_session.add(contact)
                                    contacts.append(contact)
                                
                                logger.info(f"Created {len(generic_contacts)} generic contacts for {org.name}")
                            
                        except Exception as e:
                            logger.error(f"Error creating generic contacts for {org.name}: {e}")
                    
                    # Commit all contacts
                    if contacts:
                        try:
                            db_session.commit()
                            actual_count = sum(1 for c in contacts if c.contact_type == "actual")
                            generic_count = sum(1 for c in contacts if c.contact_type == "generic")
                            logger.info(f"Added {len(contacts)} contacts for {org.name} ({actual_count} actual, {generic_count} generic)")
                        except Exception as e:
                            db_session.rollback()
                            logger.error(f"Error committing contacts to database: {e}")
                    
                    return contacts
                
                # Helper function to extract contact information from general text
                def extract_contact_information(soup, url, org_name):
                    contacts = []
                    
                    # Look for contact pages and sections
                    contact_sections = soup.find_all(['section', 'div'], class_=re.compile('contact|team|staff|people|employee|leadership'))
                    if not contact_sections:
                        # Try to find by header text
                        headers = soup.find_all(['h1', 'h2', 'h3'], string=re.compile('Contact|Team|Staff|People|Leadership|Management', re.IGNORECASE))
                        contact_sections = [h.parent for h in headers]
                    
                    for section in contact_sections:
                        # Find all paragraph elements that might contain contact info
                        paragraphs = section.find_all('p')
                        
                        for p in paragraphs:
                            text = p.get_text().strip()
                            
                            # Skip short or unlikely paragraphs
                            if len(text) < 10 or len(text) > 300:
                                continue
                            
                            # Look for name and title patterns
                            # Examples: "John Smith, Operations Manager" or "Jane Doe - Engineering Director"
                            name_title_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)[\s,-]+([A-Za-z\s]+)', text)
                            if name_title_match:
                                full_name = name_title_match.group(1).strip()
                                job_title = name_title_match.group(2).strip()
                                
                                # Skip if job title is too long or too short
                                if len(job_title) < 4 or len(job_title) > 50:
                                    continue
                                
                                # Try to split name
                                name_parts = full_name.split(' ', 1)
                                first_name = name_parts[0]
                                last_name = name_parts[1] if len(name_parts) > 1 else ""
                                
                                # Look for email address in same paragraph
                                email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
                                email = email_match.group(0) if email_match else ""
                                
                                # Look for phone number
                                phone_match = re.search(r'(?:\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', text)
                                phone = phone_match.group(0) if phone_match else ""
                                
                                # Check relevance against profiles
                                relevance_score = 5.0  # Default
                                for profile in profiles:
                                    role_title = profile.get("role_title", "").lower()
                                    if (role_title in job_title.lower() or 
                                        any(synonym.lower() in job_title.lower() 
                                            for synonym in profile.get("synonyms", []))):
                                        relevance_score = profile.get("relevance_score", 5.0)
                                        break
                                
                                # Add to contacts
                                contacts.append({
                                    "first_name": first_name,
                                    "last_name": last_name,
                                    "job_title": job_title,
                                    "email": email,
                                    "phone": phone,
                                    "discovery_method": "text_extraction",
                                    "discovery_url": url,
                                    "contact_confidence_score": 0.7,  # Medium confidence for text extraction
                                    "contact_relevance_score": relevance_score
                                })
                    
                    # Also look for emails that match common patterns for important roles
                    email_elements = soup.find_all(href=re.compile('mailto:'))
                    role_emails = []
                    
                    # Common role email patterns
                    role_patterns = [
                        'operations', 'director', 'manager', 'engineering', 'technical',
                        'chief', 'ceo', 'cto', 'coo', 'president', 'supervisor',
                        'water', 'facilities', 'maintenance', 'plant', 'systems'
                    ]
                    
                    for elem in email_elements:
                        if 'href' in elem.attrs:
                            email = elem['href'].replace('mailto:', '').strip()
                            if any(pattern in email.lower() for pattern in role_patterns):
                                # This looks like a role-based email
                                # Try to extract the role from the email
                                role_match = re.search(r'([a-z]+)@', email.lower())
                                if role_match:
                                    role = role_match.group(1)
                                    
                                    # Map common email prefixes to job titles
                                    job_title_map = {
                                        'operations': 'Operations Manager',
                                        'director': 'Director',
                                        'manager': 'Manager',
                                        'engineering': 'Engineering Manager',
                                        'technical': 'Technical Supervisor',
                                        'chief': 'Chief Officer',
                                        'ceo': 'Chief Executive Officer',
                                        'cto': 'Chief Technical Officer',
                                        'coo': 'Chief Operations Officer',
                                        'president': 'President',
                                        'supervisor': 'Supervisor',
                                        'water': 'Water Department Manager',
                                        'facilities': 'Facilities Manager',
                                        'maintenance': 'Maintenance Manager',
                                        'plant': 'Plant Manager',
                                        'systems': 'Systems Manager',
                                        'info': 'Information Services'
                                    }
                                    
                                    job_title = job_title_map.get(role, f"{role.title()} Manager")
                                    
                                    # Check relevance against profiles
                                    relevance_score = 5.0  # Default
                                    for profile in profiles:
                                        profile_role = profile.get("role_title", "").lower()
                                        if role in profile_role or any(role in s.lower() for s in profile.get("synonyms", [])):
                                            relevance_score = profile.get("relevance_score", 5.0)
                                            break
                                    
                                    # Add to role emails
                                    role_emails.append({
                                        "first_name": role.title(),  # Use role as first name placeholder
                                        "last_name": org_name.split()[0] if org_name else "",  # Use org name as placeholder
                                        "job_title": job_title,
                                        "email": email,
                                        "phone": "",
                                        "discovery_method": "role_email",
                                        "discovery_url": url,
                                        "contact_confidence_score": 0.6,  # Lower confidence for role-based emails
                                        "contact_relevance_score": relevance_score
                                    })
                    
                    return contacts + role_emails
                
                # Discover contacts
                all_contacts = []
                last_org_id = None
                
                # Create data directory if it doesn't exist
                import os
                os.makedirs("data", exist_ok=True)
                last_session_file = "data/last_contact_session.txt"
                
                for org in target_orgs:
                    logger.info(f"Processing organization: {org.name}, ID: {org.id}, Type: {org.org_type}, Website: {org.website}")
                    org_type = org.org_type if org.org_type else "unknown"
                    profiles = role_profiles.get(org_type, [])
                    
                    # Save the last processed org ID
                    last_org_id = org.id
                    
                    # Skip organizations with no matching profiles
                    if not profiles:
                        logger.warning(f"No role profiles found for organization type: {org_type}")
                        continue
                    
                    # Check if we have website info
                    if not org.website:
                        # Double-check directly from the database to confirm
                        direct_check = db_session.query(Organization.website).filter(Organization.id == org.id).scalar()
                        
                        if direct_check:
                            # We found a website in the database that wasn't loaded properly
                            logger.warning(f"Website missing in object but found in database for {org.name}, ID: {org.id}. Using: {direct_check}")
                            org.website = direct_check
                        else:
                            # Try to find a website for this organization
                            # First, check if we can derive it from search
                            from app.discovery.search.google_search import search_for_org_website
                            possible_website = search_for_org_website(org.name, org.state)
                            
                            if possible_website:
                                logger.info(f"Found potential website for {org.name}: {possible_website}")
                                org.website = possible_website
                                
                                # Also check if there's a directory page
                                try:
                                    # Check for common directory patterns in the website
                                    import requests
                                    from bs4 import BeautifulSoup
                                    
                                    # Try common directory paths
                                    directory_paths = [
                                        "/contact-us", "/contact", "/about/contact", "/directory",
                                        "/staff-directory", "/team", "/about/staff", "/about-us/staff",
                                        "/about/team", "/about-us/team", "/staff", "/people", "/personnel"
                                    ]
                                    
                                    # Try to find a directory page
                                    for path in directory_paths:
                                        try:
                                            directory_url = org.website.rstrip("/") + path
                                            headers = {
                                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                                            }
                                            response = requests.get(directory_url, headers=headers, timeout=10)
                                            
                                            if response.status_code == 200:
                                                # Check if page contains contact-related keywords
                                                if "contact" in response.text.lower() or "staff" in response.text.lower() or "directory" in response.text.lower():
                                                    logger.info(f"Found directory page for {org.name}: {directory_url}")
                                                    # Update the website to the directory page instead
                                                    org.website = directory_url
                                                    db_session.query(Organization).filter(Organization.id == org.id).update({"website": directory_url})
                                                    db_session.commit()
                                                    break
                                        except Exception as dir_e:
                                            # Just continue to the next path
                                            pass
                                except Exception as e:
                                    logger.warning(f"Error checking for directory pages: {e}")
                                
                                # Update the database with the main site if we didn't find a directory
                                db_session.query(Organization).filter(Organization.id == org.id).update({"website": org.website})
                                db_session.commit()
                            else:
                                logger.warning(f"No website for organization: {org.name}, ID: {org.id}")
                                continue
                    
                    # Execute simplified contact discovery
                    contacts = discover_contacts_for_org(org, profiles)
                    if contacts:
                        logger.info(f"Found {len(contacts)} contacts for {org.name}")
                    all_contacts.extend(contacts)
                    
                # Create metrics
                metrics = {
                    "organizations_discovered": 0,
                    "contacts_discovered": len(all_contacts),
                    "contacts_with_email": sum(1 for c in all_contacts if c.email and c.email_valid),
                    "high_relevance_contacts": sum(1 for c in all_contacts if c.contact_relevance_score >= 7)
                }
                
                # Save the last processed organization ID to the file
                if last_org_id:
                    try:
                        with open(last_session_file, "w") as f:
                            f.write(str(last_org_id))
                        logger.info(f"Saved last processed organization ID: {last_org_id}")
                    except Exception as e:
                        logger.error(f"Error saving last session file: {e}")
                
                # Save metrics to database
                from app.database.models import SystemMetric
                metric_record = SystemMetric(
                    urls_discovered=0,
                    urls_crawled=0,
                    organizations_discovered=0,
                    contacts_discovered=len(all_contacts),
                    search_queries_executed=0,
                    runtime_seconds=0
                )
                db_session.add(metric_record)
                db_session.commit()
                
            else:
                # Run regular multi-stage discovery pipeline for new organizations
                metrics = discovery_manager.run_discovery(max_orgs=max_orgs)
        else:
            # Use legacy discovery manager
            logger.info("Using legacy discovery manager")
            discovery_manager = DiscoveryManager(db_session)
            
            # Run scheduled discovery
            metrics = discovery_manager.run_scheduled_discovery(max_orgs_per_run=max_orgs)
        
        # Manually count the organizations by checking the database
        from app.database import crud
        organizations_discovered = crud.count_recently_added_organizations(db_session)
        
        logger.info(f"Discovery completed. Found {organizations_discovered} new organizations and {metrics.get('contacts_discovered', 0)} contacts")
        
        # Update metrics for the report
        metrics['organizations_discovered'] = organizations_discovered
        
        # Generate discovery report
        report_path = generate_discovery_report(db_session, metrics)
        logger.info(f"Discovery report written to {report_path}")
        
        return metrics
    
    except Exception as e:
        logger.error(f"Error in discovery process: {e}")
        return None
    
    finally:
        db_session.close()


def extract_domain_from_url(url):
    """Extract domain from a URL."""
    from urllib.parse import urlparse
    
    if not url:
        return None
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Remove www. prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain
    except:
        return None


def generate_discovery_report(db_session, metrics):
    """Generate a detailed report of the discovery process."""
    report_dir = Path(__file__).resolve().parent.parent / "reports"
    report_dir.mkdir(exist_ok=True)
    
    report_file = report_dir / f"discovery_{datetime.now().strftime('%Y%m%d')}.txt"
    
    with open(report_file, "w") as f:
        f.write(f"# GBL Data Contact Discovery - Report\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d')}\n\n")
        
        f.write(f"## Discovery Summary\n\n")
        f.write(f"- Organizations discovered: {metrics.get('organizations_discovered', 0)}\n")
        f.write(f"- Contacts discovered: {metrics.get('contacts_discovered', 0)}\n")
        
        # Add more detailed metrics from the discovery process
        if 'by_source' in metrics:
            f.write("\n### By Source\n\n")
            for source, count in metrics['by_source'].items():
                f.write(f"- {source}: {count}\n")
        
        if 'by_organization_type' in metrics:
            f.write("\n### By Organization Type\n\n")
            for org_type, count in metrics['by_organization_type'].items():
                f.write(f"- {org_type}: {count}\n")
                
        if 'by_state' in metrics:
            f.write("\n### By State\n\n")
            for state, count in metrics['by_state'].items():
                f.write(f"- {state}: {count}\n")
    
    return report_file


def update_org_websites(db_session, max_orgs=50):
    """Find websites for organizations that don't have one."""
    logger.info(f"Starting website update for up to {max_orgs} organizations")
    
    # Get organizations without websites
    from app.database.models import Organization
    from app.discovery.search.google_search import search_for_org_website
    import requests
    
    # Find organizations without websites, prioritizing by relevance score
    orgs_without_websites = db_session.query(Organization).filter(
        Organization.website.is_(None) | Organization.website == ""
    ).order_by(
        Organization.relevance_score.desc()
    ).limit(max_orgs).all()
    
    logger.info(f"Found {len(orgs_without_websites)} organizations without websites")
    
    # Track how many we update
    updated_count = 0
    
    # Update each organization
    for org in orgs_without_websites:
        logger.info(f"Looking for website for: {org.name}, ID: {org.id}, Type: {org.org_type}, State: {org.state}")
        
        # Search for organization website
        possible_website = search_for_org_website(org.name, org.state)
        
        if possible_website:
            logger.info(f"Found website for {org.name}: {possible_website}")
            
            # Check if it's valid
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                response = requests.get(possible_website, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    # Update the organization
                    org.website = possible_website
                    db_session.commit()
                    updated_count += 1
                    
                    logger.info(f"Updated website for {org.name} to {possible_website}")
                    
                    # Also check for directory pages
                    try:
                        # Try common directory paths
                        directory_paths = [
                            "/contact-us", "/contact", "/about/contact", "/directory",
                            "/staff-directory", "/team", "/about/staff", "/about-us/staff",
                            "/about/team", "/about-us/team", "/staff", "/people", "/personnel"
                        ]
                        
                        # Try to find a directory page
                        for path in directory_paths:
                            try:
                                directory_url = possible_website.rstrip("/") + path
                                dir_response = requests.get(directory_url, headers=headers, timeout=5)
                                
                                if dir_response.status_code == 200:
                                    # Check if page contains contact-related keywords
                                    if "contact" in dir_response.text.lower() or "staff" in dir_response.text.lower() or "directory" in dir_response.text.lower():
                                        logger.info(f"Found directory page for {org.name}: {directory_url}")
                                        # Update the website to the directory page instead
                                        org.website = directory_url
                                        db_session.commit()
                                        break
                            except:
                                # Just continue to the next path
                                pass
                    except Exception as e:
                        logger.warning(f"Error checking for directory pages: {e}")
                else:
                    logger.warning(f"Website found but returned error status: {response.status_code}")
            except Exception as e:
                logger.warning(f"Error checking website {possible_website}: {e}")
        else:
            logger.warning(f"No website found for {org.name}")
        
        # Small delay to avoid rate limits
        time.sleep(2)
    
    logger.info(f"Updated websites for {updated_count} organizations")
    return updated_count


def run_discovery_scheduler(frequency="daily", time="01:00", max_orgs=20, contact_focus=False):
    """Run scheduler for discovery process."""
    logger.info(f"Starting discovery scheduler (frequency={frequency}, time={time}, contact_focus={contact_focus})")
    
    # Add a task to update websites once a day
    schedule.every().day.at("03:00").do(lambda: update_org_websites(get_db_session(), max_orgs=50))
    
    if frequency == "daily":
        schedule.every().day.at(time).do(run_discovery, max_orgs=max_orgs, contact_focus=contact_focus)
    elif frequency == "hourly":
        schedule.every().hour.do(run_discovery, max_orgs=max_orgs, contact_focus=contact_focus)
    else:
        logger.error(f"Unsupported frequency: {frequency}")
        return
    
    while True:
        schedule.run_pending()
        time.sleep(60)


def run_dashboard():
    """Run the dashboard server."""
    try:
        # Get database session
        db_session = get_db_session()
        
        # Create dashboard
        dashboard = Dashboard(db_session)
        
        # Run dashboard server
        dashboard.run_server()
    
    except Exception as e:
        logger.error(f"Error in dashboard: {e}")


def main():
    """Main entry point for the Discovery application."""
    parser = argparse.ArgumentParser(description="GBL Data Contact Discovery System")
    
    # Main command groups
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Discovery command
    discovery_parser = subparsers.add_parser("discover", help="Run discovery process")
    discovery_parser.add_argument("--max-orgs", type=int, default=1000, help="Maximum number of organizations to discover (default: 1000 for exhaustive discovery)")
    discovery_parser.add_argument("--enhanced", action="store_true", help="Use enhanced discovery pipeline")
    discovery_parser.add_argument("--no-enhanced", dest="enhanced", action="store_false", help="Don't use enhanced discovery pipeline")
    discovery_parser.add_argument("--search-discovery", action="store_true", help="Use new search + Gemini based discovery pipeline")
    discovery_parser.add_argument("--resume", action="store_true", default=True, help="Resume from checkpoint if available")
    discovery_parser.add_argument("--no-resume", dest="resume", action="store_false", help="Don't resume from checkpoint")
    discovery_parser.add_argument("--contact-focus", action="store_true", help="Focus on discovering contacts for existing organizations")
    discovery_parser.add_argument("--use-municipal-crawler", action="store_true", default=True, help="Use the specialized municipal websites crawler")
    discovery_parser.add_argument("--no-municipal-crawler", dest="use_municipal_crawler", action="store_false", help="Don't use the specialized municipal websites crawler")
    discovery_parser.add_argument("--target-org-types", type=str, help="Comma-separated list of organization types to target (e.g. 'engineering,water,municipal')")
    discovery_parser.add_argument("--target-states", type=str, help="Comma-separated list of states to target (e.g. 'Utah,Arizona,Nevada')")
    
    # Set defaults
    discovery_parser.set_defaults(enhanced=True, search_discovery=False, resume=True, contact_focus=False, use_municipal_crawler=True)
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser("dashboard", help="Run dashboard server")
    dashboard_parser.add_argument("--port", type=int, default=None, help="Port to run the dashboard on")
    
    # Scheduler command
    scheduler_parser = subparsers.add_parser("scheduler", help="Run scheduler for discovery")
    scheduler_parser.add_argument("--frequency", type=str, default="daily", choices=["hourly", "daily"], 
                                 help="Frequency of discovery runs")
    scheduler_parser.add_argument("--time", type=str, default="01:00", help="Time to run daily discoveries (HH:MM)")
    scheduler_parser.add_argument("--max-orgs", type=int, default=1000, help="Maximum organizations per discovery (default: 1000 for exhaustive discovery)")
    scheduler_parser.add_argument("--contact-focus", type=bool, default=False, help="Focus on discovering contacts for existing organizations")
    
    # Website updater command
    website_parser = subparsers.add_parser("update-websites", help="Update websites for organizations with missing websites")
    website_parser.add_argument("--max-orgs", type=int, default=50, help="Maximum organizations to update (default: 50)")
    
    args = parser.parse_args()
    
    # Process commands
    if args.command == "discover":
        run_discovery(max_orgs=args.max_orgs, use_enhanced=args.enhanced, 
                      use_search_discovery=args.search_discovery,
                      contact_focus=args.contact_focus, use_municipal_crawler=args.use_municipal_crawler,
                      target_org_types=args.target_org_types, target_states=args.target_states)
    elif args.command == "dashboard":
        # Run dashboard in a separate thread
        dashboard_thread = threading.Thread(target=run_dashboard)
        dashboard_thread.daemon = True
        dashboard_thread.start()
        
        # Keep main thread running to maintain dashboard
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Dashboard stopped by user")
    elif args.command == "scheduler":
        run_discovery_scheduler(
            frequency=args.frequency,
            time=args.time,
            max_orgs=args.max_orgs,
            contact_focus=args.contact_focus
        )
    elif args.command == "update-websites":
        # Get database session
        db_session = get_db_session()
        try:
            logger.info(f"Starting website update for {args.max_orgs} organizations")
            updated = update_org_websites(db_session, max_orgs=args.max_orgs)
            logger.info(f"Website update completed. Updated {updated} organizations")
        finally:
            db_session.close()
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Discovery application stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)