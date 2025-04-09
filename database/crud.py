"""
CRUD operations for the GBL Data Contact Management System.
"""
import datetime
import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import desc, func, or_, and_
from app.database.models import Organization, Contact, ContactInteraction, ContactStatus, EmailEngagement, ProcessSummary
from app.utils.logger import get_logger
import os

logger = get_logger(__name__)


def create_organization(db: Session, org_data: Dict[str, Any]) -> Organization:
    """
    Create a new organization in the database.
    
    Args:
        db: Database session
        org_data: Dictionary with organization data
        
    Returns:
        Created organization object or None if rejected
    """
    # Check if the organization has a .edu website, if so, reject it
    website = org_data.get('website', '')
    if website and '.edu' in website.lower():
        logger.info(f"Rejecting organization with .edu website: {website}")
        return None
    
    organization = Organization(**org_data)
    db.add(organization)
    db.commit()
    db.refresh(organization)
    return organization


def get_organization_by_name_and_state(db: Session, name: str, state: str) -> Optional[Organization]:
    """
    Get an organization by name and state.
    
    Args:
        db: Database session
        name: Organization name
        state: Organization state
        
    Returns:
        Organization object if found, None otherwise
    """
    return db.query(Organization).filter(
        Organization.name == name,
        Organization.state == state
    ).first()


def get_contact_by_email(db: Session, email: str) -> Optional[Contact]:
    """
    Get a contact by email address.
    
    Args:
        db: Database session
        email: Contact email address
        
    Returns:
        Contact object if found, None otherwise
    """
    return db.query(Contact).filter(Contact.email == email).first()


def contact_exists(db: Session, first_name: str, last_name: str, organization_id: int) -> bool:
    """
    Check if a contact with the given name exists for an organization.
    
    Args:
        db: Database session
        first_name: Contact's first name
        last_name: Contact's last name
        organization_id: Organization ID
        
    Returns:
        True if contact exists, False otherwise
    """
    contact = db.query(Contact).filter(
        Contact.first_name == first_name,
        Contact.last_name == last_name,
        Contact.organization_id == organization_id
    ).first()
    
    return contact is not None


def create_contact(db: Session, contact_data: Dict[str, Any]) -> Contact:
    """
    Create a new contact in the database, ensuring email uniqueness.
    If a contact with the same email exists, update the existing contact with
    any non-null values from the new contact data.
    
    Args:
        db: Database session
        contact_data: Dictionary with contact data
        
    Returns:
        Created or updated contact object, or None if rejected
    """
    # Check if contact has .edu email, if so, reject it
    email = contact_data.get('email', '')
    if email and '.edu' in email.lower():
        logger.info(f"Rejecting contact with .edu email: {email}")
        return None
    
    # Check if contact with this email already exists
    if email:
        existing_contact = db.query(Contact).filter(Contact.email == email).first()
        
        if existing_contact:
            logger.info(f"Contact with email {email} already exists. Updating with new information.")
            
            # Merge information, preferring existing data when both have values
            for key, value in contact_data.items():
                # Skip organization_id, keep the original
                if key == 'organization_id':
                    continue
                    
                # Only update if new value is not None and existing value is None or empty
                existing_value = getattr(existing_contact, key, None)
                if value and (existing_value is None or existing_value == '' or (isinstance(existing_value, (int, float)) and existing_value == 0)):
                    setattr(existing_contact, key, value)
                    
            # Update last_updated timestamp
            existing_contact.last_updated = datetime.datetime.utcnow()
            db.commit()
            db.refresh(existing_contact)
            return existing_contact
    
    # If no existing contact with this email, create a new one
    contact = Contact(**contact_data)
    db.add(contact)
    db.commit()
    db.refresh(contact)
    return contact


def get_contacts_for_email_draft(db: Session, assigned_to: str, limit: int, 
                         min_confidence: float = 0.7, org_types: List[str] = None, 
                         states: List[str] = None) -> List[Contact]:
    """
    Get contacts that haven't had email drafts created yet and match the assignment criteria.
    Only returns contacts that belong to organization types assigned to the specified user.
    Only returns one contact per organization, selecting the best one based on relevance
    and confidence scores.
    
    Args:
        db: Database session
        assigned_to: Email of the sales person contacts should be assigned to
        limit: Maximum number of contacts to return
        min_confidence: Minimum confidence score for contacts (0.0-1.0)
        org_types: List of organization types to filter by (special value "all" means don't filter)
        states: List of states to filter by
        
    Returns:
        List of contact objects (one per organization)
    """
    # MODIFIED: Use email_assignments.json file instead of EMAIL_USERS from config
    # Path to email assignments file
    assignments_path = 'app/config/email_assignments.json'
    
    # Load the email assignments
    user_org_types = []
    if os.path.exists(assignments_path):
        try:
            with open(assignments_path, 'r') as f:
                email_assignments = json.load(f)
                logger.info(f"Loaded email assignments from {assignments_path}: {email_assignments}")
                
                # FIXED: Collect org_types for user based on assignment values
                # If an org_type is assigned to this user, add it to their list
                for org_type, assigned_email in email_assignments.items():
                    if assigned_email == assigned_to:
                        user_org_types.append(org_type)
                        
                if user_org_types:
                    logger.info(f"Found {len(user_org_types)} org types assigned to {assigned_to} in {assignments_path}")
                else:
                    logger.info(f"No assignments found for {assigned_to} in {assignments_path}")
        except Exception as e:
            logger.error(f"Error loading email assignments from {assignments_path}: {e}")
    else:
        logger.warning(f"Email assignments file not found: {assignments_path}")
    
    # Skip this user entirely if they don't have any assigned organization types
    if not user_org_types:
        logger.info(f"User {assigned_to} has no assigned organization types. Skipping.")
        return []
    
    # Debug info
    logger.info(f"Looking for contacts for user {assigned_to} with org_types: {user_org_types}")
    
    # Start building the query to get eligible contacts
    query = db.query(Contact).join(
        Organization, Contact.organization_id == Organization.id
    ).filter(
        Contact.email_draft_created == False,
        Contact.email.isnot(None),
        Contact.email != '',  # Ensure email is not an empty string
        Contact.contact_confidence_score >= min_confidence,
        # Include contacts from org types assigned to this user, regardless of assignment
        Organization.org_type.in_(user_org_types)
    # Order by date_added to get the most recent contacts first
    ).order_by(Contact.date_added.desc())
    
    # Debug counts
    count_contacts = query.count()
    logger.info(f"Found {count_contacts} contacts matching basic criteria for {assigned_to}")
    
    # Apply additional organization type filter if provided
    # IMPORTANT: Special case - if org_types contains only "all", don't apply additional filter
    if org_types and isinstance(org_types, list) and len(org_types) > 0:
        # Check for special "all" value
        if len(org_types) == 1 and org_types[0].lower() == "all":
            logger.info(f"'all' specified in org_types - using all assigned org types for user {assigned_to}: {user_org_types}")
            # Don't apply additional filtering - use all assigned types
            pass
        else:
            # Normal case - filter by specified org types
            # Only include org types that are both in the filter AND assigned to this user
            filtered_org_types = [org_type for org_type in org_types if org_type in user_org_types]
            if not filtered_org_types:
                logger.info(f"No overlap between requested org_types {org_types} and user {assigned_to}'s assigned types {user_org_types}")
                return []
            logger.info(f"Filtering to specific org types: {filtered_org_types}")
            query = query.filter(Organization.org_type.in_(filtered_org_types))
    
    # Apply state filter if provided
    if states and isinstance(states, list) and len(states) > 0:
        # Check for special "all" value
        if len(states) == 1 and states[0].lower() == "all":
            logger.info(f"'all' specified in states - not applying state filter")
            # Don't apply state filtering
            pass
        else:
            logger.info(f"Filtering to specific states: {states}")
            query = query.filter(Organization.state.in_(states))
    
    # Get all eligible contacts with their organizations
    eligible_contacts = query.all()
    
    # Debug log: Log the first 5 contacts with their names and emails
    if eligible_contacts:
        sample_contacts = eligible_contacts[:min(5, len(eligible_contacts))]
        debug_info = "\n".join([f"  - ID: {c.id}, Name: {c.first_name} {c.last_name}, Email: {c.email}" for c in sample_contacts])
        logger.info(f"Sample of eligible contacts (first 5):\n{debug_info}")
    
    # Group contacts by organization and select the best one from each
    selected_contacts = []
    organizations_processed = set()
    contacts_to_update = []  # Track contacts to update assigned_to field
    
    for contact in eligible_contacts:
        # Skip if we've already selected a contact for this organization
        if contact.organization_id in organizations_processed:
            continue
        
        # Find all contacts for this organization
        org_contacts = [c for c in eligible_contacts if c.organization_id == contact.organization_id]
        
        # Sort contacts by relevance and confidence scores (descending)
        org_contacts.sort(key=lambda c: (
            c.contact_relevance_score if c.contact_relevance_score is not None else 0,
            c.contact_confidence_score if c.contact_confidence_score is not None else 0
        ), reverse=True)
        
        # Select the best contact for this organization
        best_contact = org_contacts[0]
        
        # Set the assignment but don't update the database yet
        if best_contact.assigned_to != assigned_to:
            best_contact.assigned_to = assigned_to
            contacts_to_update.append(best_contact)
        
        selected_contacts.append(best_contact)
        organizations_processed.add(contact.organization_id)
        
        logger.info(f"Selected contact {best_contact.first_name} {best_contact.last_name} " 
                   f"(rel: {best_contact.contact_relevance_score}, conf: {best_contact.contact_confidence_score}) "
                   f"as best contact for organization ID {contact.organization_id}")
        
        # Stop if we've reached the limit
        if len(selected_contacts) >= limit:
            break
    
    # Now update the database for all contacts that need assignment updates
    for contact in contacts_to_update:
        db.query(Contact).filter(Contact.id == contact.id).update({"assigned_to": assigned_to})
    
    # Commit the changes to ensure assignments are saved
    if contacts_to_update:
        db.commit()
    
    # Check if we found any contacts
    if not selected_contacts:
        # Show more detailed debugging
        org_type_counts = {}
        for org_type in user_org_types:
            count = db.query(Contact).join(
                Organization, Contact.organization_id == Organization.id
            ).filter(
                Organization.org_type == org_type
            ).count()
            
            count_eligible = db.query(Contact).join(
                Organization, Contact.organization_id == Organization.id
            ).filter(
                Organization.org_type == org_type,
                Contact.email_draft_created == False,
                Contact.email.isnot(None),
                Contact.contact_confidence_score >= min_confidence
            ).count()
            
            org_type_counts[org_type] = {
                "total": count,
                "eligible": count_eligible
            }
            
        logger.info(f"Detailed counts for user {assigned_to}: {org_type_counts}")
        logger.info(f"No contacts found for email drafts matching the criteria (assigned_to={assigned_to}, org_types={org_types}, states={states})")
        
    return selected_contacts


def update_contact_draft_status(db: Session, contact_id: int, draft_id: str) -> Contact:
    """
    Update a contact's email draft status.
    
    Args:
        db: Database session
        contact_id: Contact ID
        draft_id: Email draft ID
        
    Returns:
        Updated Contact
    """
    try:
        contact = db.query(Contact).filter(Contact.id == contact_id).first()
        if contact:
            contact.email_draft_created = True
            contact.email_draft_date = datetime.datetime.utcnow()
            contact.email_draft_id = draft_id
            contact.status = ContactStatus.EMAIL_DRAFT.value
            contact.status_date = datetime.datetime.utcnow()
            db.commit()
            return contact
        return None
    except Exception as e:
        logger.error(f"Error updating contact draft status: {e}")
        db.rollback()
        return None


def update_contact_sent_status(db: Session, contact_id: int, email_id: str) -> Contact:
    """
    Update a contact's email sent status.
    
    Args:
        db: Database session
        contact_id: Contact ID
        email_id: Email ID
        
    Returns:
        Updated Contact
    """
    try:
        contact = db.query(Contact).filter(Contact.id == contact_id).first()
        if contact:
            contact.email_sent = True
            contact.email_sent_date = datetime.datetime.utcnow()
            contact.email_draft_id = email_id  # Store the message ID
            contact.status = ContactStatus.EMAILED.value
            contact.status_date = datetime.datetime.utcnow()
            db.commit()
            return contact
        return None
    except Exception as e:
        logger.error(f"Error updating contact sent status: {e}")
        db.rollback()
        return None


def get_new_contacts_today(db: Session) -> List[Contact]:
    """
    Get contacts added today.
    
    Args:
        db: Database session
        
    Returns:
        List of Contact objects
    """
    today = datetime.datetime.utcnow().date()
    return db.query(Contact).filter(
        Contact.date_added >= today
    ).all()


def get_drafts_created_today(db: Session) -> List[Contact]:
    """
    Get contacts with email drafts created today.
    
    Args:
        db: Database session
        
    Returns:
        List of contact objects
    """
    today = datetime.datetime.utcnow().date()
    return db.query(Contact).filter(
        Contact.email_draft_date >= today
    ).all()


def contact_exists_by_email(db: Session, email: str) -> bool:
    """
    Check if a contact with the given email already exists in any organization.
    
    Args:
        db: Database session
        email: Contact's email address
        
    Returns:
        True if the contact exists, False otherwise
    """
    return db.query(Contact).filter(
        Contact.email == email
    ).first() is not None


def update_organization(db: Session, org_id: int, org_data: Dict[str, Any]) -> Optional[Organization]:
    """
    Update an existing organization.
    
    Args:
        db: Database session
        org_id: Organization ID
        org_data: Dictionary with organization data
        
    Returns:
        Updated organization object if found, None otherwise
    """
    organization = db.query(Organization).filter(Organization.id == org_id).first()
    if organization:
        for key, value in org_data.items():
            setattr(organization, key, value)
        db.commit()
        db.refresh(organization)
    return organization
    
    
def rerank_organization_by_infrastructure(db: Session, org_id: int) -> Optional[Organization]:
    """
    Rerank an organization based on infrastructure indicators from its URLs.
    
    Args:
        db: Database session
        org_id: Organization ID
        
    Returns:
        Updated organization object with new relevance scores
    """
    from app.database.models import DiscoveredURL
    import json
    
    # Get organization
    organization = db.query(Organization).filter(Organization.id == org_id).first()
    if not organization:
        return None
        
    # Get all discovered URLs for this organization
    urls = db.query(DiscoveredURL).filter(DiscoveredURL.organization_id == org_id).all()
    
    # Initialize infrastructure indicators
    infrastructure_count = 0
    infrastructure_matches = []
    process_count = 0
    process_matches = []
    industry_indicators = {}
    project_count = 0
    project_automation_count = 0
    competitor_indicators = []
    
    # Analyze URLs for infrastructure indicators
    for url in urls:
        # Check if URL contains infrastructure
        if url.contains_infrastructure:
            infrastructure_count += 1
            
        # Process industry indicators
        if url.industry_indicators and isinstance(url.industry_indicators, (str, bytes)):
            try:
                indicators = json.loads(url.industry_indicators)
                # Combine industry indicators from different pages
                for industry, score in indicators.items():
                    if industry in industry_indicators:
                        industry_indicators[industry] = max(industry_indicators[industry], score)
                    else:
                        industry_indicators[industry] = score
            except:
                pass
                
        # Process project data
        if url.project_data and isinstance(url.project_data, (str, bytes)):
            try:
                projects = json.loads(url.project_data)
                if isinstance(projects, list):
                    project_count += len(projects)
                    for project in projects:
                        if project.get("contains_automation", False):
                            project_automation_count += 1
            except:
                pass
    
    # Load extended data if it exists
    extended_data = {}
    if organization.extended_data:
        if isinstance(organization.extended_data, dict):
            extended_data = organization.extended_data
        elif isinstance(organization.extended_data, (str, bytes)):
            try:
                extended_data = json.loads(organization.extended_data)
            except:
                extended_data = {}
    
    # Check for infrastructure indicators in extended data
    if "infrastructure_indicators" in extended_data:
        infra_data = extended_data["infrastructure_indicators"]
        if isinstance(infra_data, dict):
            if "infrastructure_matches" in infra_data and isinstance(infra_data["infrastructure_matches"], list):
                infrastructure_matches = infra_data["infrastructure_matches"]
            if "process_matches" in infra_data and isinstance(infra_data["process_matches"], list):
                process_matches = infra_data["process_matches"]
    
    # Check for competitor indicators
    if "competitor_analysis" in extended_data:
        comp_data = extended_data["competitor_analysis"]
        if isinstance(comp_data, dict):
            if "competitor_indicators" in comp_data and isinstance(comp_data["competitor_indicators"], list):
                competitor_indicators = comp_data["competitor_indicators"]
            if "is_likely_competitor" in comp_data:
                organization.is_competitor = comp_data["is_likely_competitor"]
    
    # Calculate infrastructure score (0-1 scale)
    infra_score = min(1.0, (infrastructure_count / max(1, len(urls))) + (len(infrastructure_matches) / 10.0))
    organization.infrastructure_score = infra_score
    
    # Calculate process complexity score (0-1 scale)
    process_score = min(1.0, (len(process_matches) / 8.0))
    organization.process_complexity_score = process_score
    
    # Estimate automation level (0-1 scale, lower means less automation)
    # This will be used to prioritize organizations with less automation (more potential)
    automation_level = 0.3  # Default mid-low value
    if project_count > 0:
        automation_level = min(1.0, (project_automation_count / project_count))
    organization.automation_level = automation_level
    
    # Calculate competitor score and apply to overall relevance
    competitor_penalty = 0.0
    if organization.is_competitor:
        competitor_penalty = min(0.9, (len(competitor_indicators) * 0.2))
    
    # Calculate integration opportunity score
    # High infrastructure + high process + low automation = best opportunity
    integration_score = (infra_score * 0.4) + (process_score * 0.3) + ((1.0 - automation_level) * 0.3)
    
    # Apply competitor penalty
    integration_score = max(0.0, integration_score - competitor_penalty)
    organization.integration_opportunity_score = integration_score
    
    # Update overall relevance score with blend of old and new scoring
    # Weight the infrastructure-based score more heavily (80%)
    organization.relevance_score = (organization.relevance_score * 0.2) + (integration_score * 0.8)
    
    # Save to database
    db.commit()
    
    return organization


def rerank_all_organizations(db: Session) -> List[Organization]:
    """
    Rerank all organizations based on infrastructure indicators.
    
    Args:
        db: Database session
        
    Returns:
        List of updated organizations sorted by relevance
    """
    # Get all organizations
    organizations = db.query(Organization).all()
    
    # Rerank each organization
    for org in organizations:
        rerank_organization_by_infrastructure(db, org.id)
    
    # Return sorted list
    return db.query(Organization).filter(
        Organization.is_competitor == False
    ).order_by(Organization.integration_opportunity_score.desc()).all()


def count_recently_added_organizations(db: Session) -> int:
    """
    Count organizations added today.
    
    Args:
        db: Database session
        
    Returns:
        Count of organizations added today
    """
    today = datetime.datetime.utcnow().date()
    return db.query(Organization).filter(
        Organization.date_added >= today
    ).count()

def merge_duplicate_contacts_by_email(db: Session) -> Dict[str, int]:
    """
    Find and merge contacts with the same email address.
    
    Args:
        db: Database session
        
    Returns:
        Dictionary with statistics about the merge operation
    """
    stats = {
        "total_emails_processed": 0,
        "unique_emails_kept": 0,
        "duplicate_contacts_merged": 0,
        "contacts_without_email": 0,
        "errors": 0
    }
    
    # Find contacts with email addresses
    contacts_with_email = db.query(Contact).filter(Contact.email.isnot(None)).all()
    email_map = {}  # Map of email -> list of contact objects
    
    # Organize contacts by email address
    for contact in contacts_with_email:
        if not contact.email or not contact.email.strip():
            stats["contacts_without_email"] += 1
            continue
            
        email = contact.email.lower().strip()
        stats["total_emails_processed"] += 1
        
        if email in email_map:
            email_map[email].append(contact)
        else:
            email_map[email] = [contact]
            stats["unique_emails_kept"] += 1
    
    # Process emails with multiple contacts
    for email, contacts in email_map.items():
        if len(contacts) <= 1:
            continue  # No duplicates for this email
            
        try:
            # Sort contacts by quality, keeping the most complete one
            sorted_contacts = sorted(
                contacts,
                key=lambda c: (
                    # Higher confidence score is better
                    c.contact_confidence_score or 0,
                    # Having a name is important (first and last)
                    1 if c.first_name and c.first_name.strip() else 0,
                    1 if c.last_name and c.last_name.strip() else 0,
                    # Having contact details is useful
                    1 if c.job_title and c.job_title.strip() else 0,
                    1 if c.phone and c.phone.strip() else 0,
                    # Newer contacts might have better data
                    c.date_added or datetime.datetime.min
                ),
                reverse=True  # Higher scores first
            )
            
            # Keep the first (highest quality) contact
            primary_contact = sorted_contacts[0]
            duplicates = sorted_contacts[1:]
            
            # Merge data from duplicates into the primary
            for dup in duplicates:
                # Take any non-null field values from duplicates
                if not primary_contact.first_name and dup.first_name:
                    primary_contact.first_name = dup.first_name
                    
                if not primary_contact.last_name and dup.last_name:
                    primary_contact.last_name = dup.last_name
                    
                if not primary_contact.job_title and dup.job_title:
                    primary_contact.job_title = dup.job_title
                    
                if not primary_contact.phone and dup.phone:
                    primary_contact.phone = dup.phone
                
                # Update notes to record the merge
                notes = primary_contact.notes or ""
                if dup.first_name or dup.last_name:
                    if notes:
                        notes += "\n"
                    notes += f"Merged with duplicate contact: {dup.first_name or ''} {dup.last_name or ''}"
                primary_contact.notes = notes
                
                # Update confidence and relevance scores
                primary_contact.contact_confidence_score = max(
                    primary_contact.contact_confidence_score or 0, 
                    dup.contact_confidence_score or 0
                )
                
                primary_contact.contact_relevance_score = max(
                    primary_contact.contact_relevance_score or 0,
                    dup.contact_relevance_score or 0
                )
                
                # Delete the duplicate
                db.delete(dup)
                stats["duplicate_contacts_merged"] += 1
            
            # Update last_updated timestamp
            primary_contact.last_updated = datetime.datetime.utcnow()
            db.commit()
            
        except Exception as e:
            logger.error(f"Error merging contacts for email '{email}': {e}")
            db.rollback()
            stats["errors"] += 1
    
    return stats

def remove_edu_contacts_and_organizations(db: Session) -> Dict[str, int]:
    """
    Find and remove all contacts with .edu emails and organizations with .edu websites.
    
    Args:
        db: Database session
        
    Returns:
        Dictionary with statistics about the removal operation
    """
    stats = {
        "contacts_removed": 0,
        "organizations_removed": 0,
        "errors": 0
    }
    
    try:
        # Remove contacts with .edu emails
        edu_contacts = db.query(Contact).filter(Contact.email.like('%.edu%')).all()
        for contact in edu_contacts:
            try:
                db.delete(contact)
                stats["contacts_removed"] += 1
            except Exception as e:
                logger.error(f"Error removing edu contact {contact.id}: {e}")
                stats["errors"] += 1
        
        # Remove organizations with .edu websites
        edu_orgs = db.query(Organization).filter(Organization.website.like('%.edu%')).all()
        for org in edu_orgs:
            try:
                # First delete any remaining contacts for this organization
                org_contacts = db.query(Contact).filter(Contact.organization_id == org.id).all()
                for contact in org_contacts:
                    db.delete(contact)
                    stats["contacts_removed"] += 1
                
                # Then delete the organization
                db.delete(org)
                stats["organizations_removed"] += 1
            except Exception as e:
                logger.error(f"Error removing edu organization {org.id}: {e}")
                stats["errors"] += 1
        
        # Commit the changes
        db.commit()
        
    except Exception as e:
        logger.error(f"Error in remove_edu_contacts_and_organizations: {e}")
        db.rollback()
        stats["errors"] += 1
    
    return stats

# Process Summary functions
def create_process_summary(db: Session, process_type: str) -> ProcessSummary:
    """
    Create a new process summary entry when a process starts
    
    Args:
        db: Database session
        process_type: Type of process (org_building, contact_building, email_sending)
        
    Returns:
        The created ProcessSummary object
    """
    summary = ProcessSummary(
        process_type=process_type,
        started_at=datetime.datetime.utcnow(),
        status="running",
        items_processed=0,
        items_added=0
    )
    
    db.add(summary)
    db.commit()
    db.refresh(summary)
    return summary

def update_process_summary(db: Session, summary_id: int, 
                          status: str, items_processed: int, 
                          items_added: int, details: Dict = None) -> ProcessSummary:
    """
    Update a process summary entry when a process completes
    
    Args:
        db: Database session
        summary_id: ID of the process summary to update
        status: Status of the process (completed, failed)
        items_processed: Number of items processed
        items_added: Number of items added
        details: Additional details about the process
        
    Returns:
        The updated ProcessSummary object
    """
    summary = db.query(ProcessSummary).filter(ProcessSummary.id == summary_id).first()
    
    if not summary:
        return None
    
    summary.completed_at = datetime.datetime.utcnow()
    summary.status = status
    summary.items_processed = items_processed
    summary.items_added = items_added
    
    if details:
        summary.details = json.dumps(details)
    
    db.commit()
    db.refresh(summary)
    return summary

def get_recent_process_summaries(db: Session, limit: int = 10) -> List[ProcessSummary]:
    """
    Get recent process summaries for display in the dashboard
    
    Args:
        db: Database session
        limit: Maximum number of summaries to return
        
    Returns:
        List of ProcessSummary objects
    """
    return db.query(ProcessSummary)\
             .order_by(desc(ProcessSummary.started_at))\
             .limit(limit)\
             .all()

def get_process_summaries_by_type(db: Session, process_type: str, limit: int = 5) -> List[ProcessSummary]:
    """
    Get recent process summaries of a specific type
    
    Args:
        db: Database session
        process_type: Type of process (org_building, contact_building, email_sending)
        limit: Maximum number of summaries to return
        
    Returns:
        List of ProcessSummary objects
    """
    return db.query(ProcessSummary)\
             .filter(ProcessSummary.process_type == process_type)\
             .order_by(desc(ProcessSummary.started_at))\
             .limit(limit)\
             .all()