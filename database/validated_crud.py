"""
Validated CRUD operations for the GBL Data Contact Management System.

This module extends the basic CRUD operations with Gemini AI validation
to filter out problematic data before it enters the database.
"""
import logging
from typing import Dict, Any, Optional, Tuple, List
from sqlalchemy.orm import Session
from app.database import crud
from app.utils.data_validator import DataValidator
from app.database.models import Organization, Contact
from app.utils.logger import get_logger
import os
import json
from app.utils.hybrid_validator import validate_contact
from app.discovery.search_engine import SearchEngine # Placeholder
import re

logger = get_logger(__name__)

# Helper function to load hurdles (could be moved to a shared utility)
def load_validation_hurdles() -> Dict[str, float]:
    settings_path = 'app/config/validation_settings.json'
    default_hurdles = {
        'org_confidence_hurdle': 0.7,
        'name_confidence_hurdle': 0.7
    }
    try:
        if os.path.exists(settings_path):
            with open(settings_path, 'r') as f:
                settings = json.load(f)
                org_hurdle = float(settings.get('org_confidence_hurdle', default_hurdles['org_confidence_hurdle']))
                name_hurdle = float(settings.get('name_confidence_hurdle', default_hurdles['name_confidence_hurdle']))
                if not (0.0 <= org_hurdle <= 1.0): org_hurdle = default_hurdles['org_confidence_hurdle']
                if not (0.0 <= name_hurdle <= 1.0): name_hurdle = default_hurdles['name_confidence_hurdle']
                return {'org_confidence_hurdle': org_hurdle, 'name_confidence_hurdle': name_hurdle}
        return default_hurdles
    except Exception as e:
        logger.error(f"Error loading validation settings: {e}. Using defaults.")
        return default_hurdles

class ValidatedCrud:
    """
    Extended CRUD operations with Gemini AI validation.
    """
    
    def __init__(self, db_session: Session):
        """
        Initialize the validated CRUD operations.
        
        Args:
            db_session: Database session
        """
        self.db = db_session
        self.validator = DataValidator()
        self.search_engine = SearchEngine(db_session) # Initialize SearchEngine
        self.validation_stats = {
            "orgs_validated": 0,
            "orgs_rejected": 0,
            "orgs_improved": 0,
            "contacts_validated": 0,
            "contacts_rejected": 0
        }
        # Load hurdles once during init
        self.hurdles = load_validation_hurdles()
    
    def get_validation_stats(self) -> Dict[str, int]:
        """Get current validation statistics."""
        return self.validation_stats
    
    def create_organization(self, org_data: Dict[str, Any]) -> Tuple[Optional[Organization], bool, str]:
        """
        Create a new organization with validation.
        
        Args:
            org_data: Dictionary with organization data
            
        Returns:
            Tuple containing:
                - Created organization object or None if validation failed
                - Boolean indicating if validation was performed
                - Reason for rejection if validation failed
        """
        # Skip validation for certain scenarios
        skip_validation = False
        
        # Skip validation for organizations with high confidence scores
        if org_data.get("confidence_score", 0) > 0.9:
            skip_validation = True
            logger.info(f"Skipping validation for high-confidence organization: {org_data.get('name')}")
            
        # Validate the organization if not skipped
        if not skip_validation:
            self.validation_stats["orgs_validated"] += 1
            is_valid, validation_result = self.validator.validate_organization(org_data)
            
            if not is_valid:
                # Record rejection
                self.validation_stats["orgs_rejected"] += 1
                reason = validation_result.get("validation_result", {}).get("reasons", "Failed validation check")
                logger.warning(f"Rejected organization '{org_data.get('name')}': {reason}")
                return None, True, reason
            
            # Apply any improvements from validation
            improved_data = validation_result.get("improved_data", {})
            if improved_data.get("name") != org_data.get("name"):
                self.validation_stats["orgs_improved"] += 1
                logger.info(f"Improved organization name from '{org_data.get('name')}' to '{improved_data.get('name')}'")
                # Update the data with improvements
                org_data = improved_data
        
        # Create the organization
        organization = crud.create_organization(self.db, org_data)
        return organization, not skip_validation, ""
    
    def create_contact(self, contact_data: Dict[str, Any]) -> Tuple[Optional[Contact], bool, str]:
        """
        Create a new contact with MANDATORY Gemini validation.

        Args:
            contact_data: Dictionary with contact data (must include 'email' and 'organization_id')

        Returns:
            Tuple containing:
                - Created contact object or None if validation failed/rejected.
                - Boolean indicating if validation was attempted (should always be True now).
                - Reason for rejection if validation failed.
        """
        logger.debug(f"Attempting to create contact with validation: {contact_data.get('email')}")
        self.validation_stats["contacts_validated"] += 1
        
        contact_email = contact_data.get('email')
        org_id = contact_data.get('organization_id')
        contact_first_name = contact_data.get('first_name')

        if not contact_email or not org_id:
            reason = "Contact validation failed: Email and organization_id are required."
            logger.error(reason)
            self.validation_stats["contacts_rejected"] += 1
            return None, True, reason

        # --- Fetch Organization Data --- 
        organization = self.db.query(Organization).filter(Organization.id == org_id).first()
        organization_data = None
        if organization:
             organization_data = {
                 "name": organization.name,
                 "org_type": organization.org_type,
                 "website": organization.website
             }
        else:
             logger.warning(f"Organization ID {org_id} not found for contact {contact_email}. Proceeding without org data.")
        # --- End Fetch Organization Data --- 

        # --- Google Search --- 
        search_results = None
        try:
            domain = contact_email.split('@')[1]
            logger.debug(f"Performing web search for domain: '{domain}'")
            search_results = self.search_engine.execute_search(f'site:{domain}')
            logger.debug(f"Search results for {domain}: {search_results}")
        except Exception as search_err:
            logger.warning(f"Error during web search for {domain}: {search_err}")
        # --- End Google Search --- 

        try:
            # Call the new hybrid_validator function directly
            passes_hurdles, validation_details = validate_contact(
                contact=contact_data, 
                organization_data=organization_data,
                search_results=search_results,
                use_gemini=True, # Mandatory
                org_confidence_hurdle=self.hurdles['org_confidence_hurdle'],
                name_confidence_hurdle=self.hurdles['name_confidence_hurdle']
            )

            reason = validation_details.get('reasons', 'Validation check performed.')

            # --- Handle No-Name Email Path --- 
            org_confidence = validation_details.get("org_confidence", 0.0)
            name_confidence = validation_details.get("name_confidence")
            org_passes = org_confidence >= self.hurdles['org_confidence_hurdle']

            if org_passes and name_confidence is None and not contact_first_name:
                 logger.info(f"Contact {contact_email} passed ORG validation without name. Triggering no-name email.")
                 # trigger_no_name_email(contact_data) # Logic now handled in EmailManager
                 # Decide if contact should still be created or if trigger handles it
                 # Assuming we still create the contact record (without name)
                 contact = crud.create_contact(self.db, contact_data) 
                 return contact, True, "Passed org validation, no name, triggered no-name email." 
            # --- End No-Name Email Path --- 

            if passes_hurdles:
                logger.info(f"Contact {contact_email} passed validation hurdles.")
                contact = crud.create_contact(self.db, contact_data)
                return contact, True, reason # Return success
            else:
                # Validation failed hurdles
                self.validation_stats["contacts_rejected"] += 1
                reason = validation_details.get('reasons', 'Confidence score below hurdle')
                name_passes = True
                if name_confidence is not None: name_passes = name_confidence >= self.hurdles['name_confidence_hurdle']
                if not org_passes: reason = f"Low org confidence ({org_confidence:.2f}). {reason}"
                if not name_passes: reason = f"Low name confidence ({name_confidence:.2f}). {reason}"
                logger.warning(f"Rejected contact {contact_email}: {reason}")
                # Decide what to do - save as draft? Here we just reject.
                return None, True, reason

        except (ValueError, RuntimeError) as validation_err:
            # Catch critical validation errors (Gemini unavailable, etc.)
            logger.error(f"CRITICAL VALIDATION ERROR creating contact {contact_email}: {validation_err}")
            self.validation_stats["contacts_rejected"] += 1
            # Do not create the contact
            return None, True, str(validation_err)
        except Exception as e:
            # Catch other unexpected errors
            logger.error(f"Unexpected error creating contact {contact_email}: {e}", exc_info=True)
            self.validation_stats["contacts_rejected"] += 1
            return None, True, f"Unexpected error: {e}"
    
    def update_organization(self, org_id: int, org_data: Dict[str, Any]) -> Tuple[Optional[Organization], bool, str]:
        """
        Update an existing organization with Gemini validation if name or website changes.

        Args:
            org_id: Organization ID
            org_data: Dictionary with new organization data

        Returns:
            Tuple containing:
                - Updated organization object or None if validation rejected update.
                - Boolean indicating if validation was attempted.
                - Reason for rejection if validation failed or hurdle not met.
        """
        current_org = self.db.query(Organization).filter(Organization.id == org_id).first()
        if not current_org:
             return None, False, "Organization not found"
             
        name_changing = "name" in org_data and org_data["name"] != current_org.name
        website_changing = "website" in org_data and org_data["website"] != current_org.website
        should_validate = name_changing or website_changing

        if should_validate:
            logger.info(f"Validating organization update for ID: {org_id}")
            self.validation_stats["orgs_validated"] += 1
            
            # Prepare data for validation
            validation_data = {
                "name": org_data.get("name", current_org.name),
                "website": org_data.get("website", current_org.website),
                "org_type": org_data.get("org_type", current_org.org_type)
            }
            org_website_for_search = validation_data.get("website")

            # --- Perform Search --- 
            search_results = None
            if org_website_for_search and self.search_engine:
                 try:
                     # Extract domain for search if possible
                     domain_match = re.search(r'https?://(?:www\.)?([^/]+)', org_website_for_search)
                     if domain_match:
                         domain = domain_match.group(1)
                         logger.debug(f"Performing web search for org website domain: {domain}")
                         search_results = self.search_engine.execute_search(f'site:{domain}')
                     else:
                         logger.warning(f"Could not extract domain from website {org_website_for_search} for search.")
                 except Exception as search_err:
                     logger.warning(f"Error during web search for org website {org_website_for_search}: {search_err}")
            # --- End Search --- 

            try:
                # Call the refactored validator
                # Note: validate_organization is now part of DataValidator, not self.validator directly?
                # Let's assume self.validator is DataValidator based on __init__ seen before.
                validation_result_dict = self.validator.validate_organization(
                    org_data=validation_data,
                    search_results=search_results
                )

                org_confidence = validation_result_dict.get("org_confidence", 0.0)
                reasons = validation_result_dict.get("reasons")
                
                # Check against hurdle
                if org_confidence >= self.hurdles['org_confidence_hurdle']:
                    logger.info(f"Organization update for ID {org_id} passed validation hurdle ({org_confidence:.2f}). Proceeding.")
                    # Update the organization
                    organization = crud.update_organization(self.db, org_id, org_data)
                    return organization, True, reasons or "Validation passed"
                else:
                    # Failed hurdle
                    self.validation_stats["orgs_rejected"] += 1
                    reason_msg = f"Rejected organization update for ID {org_id}: Confidence score {org_confidence:.2f} below hurdle {self.hurdles['org_confidence_hurdle']:.2f}. Details: {reasons}"
                    logger.warning(reason_msg)
                    return None, True, reason_msg

            except (ValueError, RuntimeError) as validation_err:
                # Catch critical validation errors
                logger.error(f"CRITICAL VALIDATION ERROR updating org ID {org_id}: {validation_err}")
                self.validation_stats["orgs_rejected"] += 1
                return None, True, f"Validation Error: {validation_err}"
            except Exception as e:
                # Catch other unexpected errors
                logger.error(f"Unexpected error during validation for org update ID {org_id}: {e}", exc_info=True)
                self.validation_stats["orgs_rejected"] += 1
                return None, True, f"Unexpected validation error: {e}"

        else:
            # No validation needed
            logger.debug(f"No validation required for update on org ID {org_id}. Proceeding.")
            organization = crud.update_organization(self.db, org_id, org_data)
            return organization, False, "No validation performed"
    
    # Pass-through methods for other CRUD operations that don't need validation
    
    def get_organization_by_name_and_state(self, name: str, state: str) -> Optional[Organization]:
        """Get an organization by name and state."""
        return crud.get_organization_by_name_and_state(self.db, name, state)
    
    def get_contact_by_email(self, email: str) -> Optional[Contact]:
        """Get a contact by email address."""
        return crud.get_contact_by_email(self.db, email)
    
    def contact_exists(self, first_name: str, last_name: str, organization_id: int) -> bool:
        """Check if a contact with the given name exists for an organization."""
        return crud.contact_exists(self.db, first_name, last_name, organization_id)
    
    def contact_exists_by_email(self, email: str, organization_id: int) -> bool:
        """Check if a contact with the given email exists for an organization."""
        return crud.contact_exists_by_email(self.db, email, organization_id)
    
    def update_contact_draft_status(self, contact_id: int, draft_id: str) -> Contact:
        """Update contact with email draft information."""
        return crud.update_contact_draft_status(self.db, contact_id, draft_id)
    
    def get_new_contacts_today(self) -> List[Contact]:
        """Get contacts added today."""
        return crud.get_new_contacts_today(self.db)
    
    def get_drafts_created_today(self) -> List[Contact]:
        """Get contacts with email drafts created today."""
        return crud.get_drafts_created_today(self.db)
    
    def batch_validate_organizations(self, org_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Batch validate multiple organizations to improve performance.
        
        Args:
            org_data_list: List of organization data dictionaries
            
        Returns:
            List of valid organization data dictionaries (with improvements applied)
        """
        valid_orgs = []
        
        for org_data in org_data_list:
            is_valid, validation_result = self.validator.validate_organization(org_data)
            self.validation_stats["orgs_validated"] += 1
            
            if is_valid:
                # Apply any improvements
                improved_data = validation_result.get("improved_data", org_data)
                if improved_data.get("name") != org_data.get("name"):
                    self.validation_stats["orgs_improved"] += 1
                valid_orgs.append(improved_data)
            else:
                self.validation_stats["orgs_rejected"] += 1
                reason = validation_result.get("validation_result", {}).get("reasons", "Failed validation check")
                logger.warning(f"Batch validation rejected organization '{org_data.get('name')}': {reason}")
        
        return valid_orgs
    
    def batch_validate_contacts(self, contact_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Batch validate multiple contacts using Gemini AI and configured hurdles.
        Skips contacts that fail validation or cause errors.

        Args:
            contact_data_list: List of contact data dictionaries.

        Returns:
            List of contact data dictionaries that passed validation hurdles.
        """
        logger.info(f"Starting batch validation for {len(contact_data_list)} contacts.")
        valid_contacts_data = []
        org_cache = {} # Simple cache for organization data within the batch

        for contact_data in contact_data_list:
            self.validation_stats["contacts_validated"] += 1
            contact_email = contact_data.get('email')
            org_id = contact_data.get('organization_id')
            contact_first_name = contact_data.get('first_name')

            log_prefix = f"Batch validation for contact {contact_email}:"

            if not contact_email or not org_id:
                logger.warning(f"{log_prefix} Skipping - Email and organization_id are required.")
                self.validation_stats["contacts_rejected"] += 1
                continue

            # --- Fetch Organization Data (with caching) --- 
            organization_data = None
            if org_id in org_cache:
                 organization_data = org_cache[org_id]
            else:
                organization = self.db.query(Organization).filter(Organization.id == org_id).first()
                if organization:
                     organization_data = {
                         "name": organization.name,
                         "org_type": organization.org_type,
                         "website": organization.website
                     }
                     org_cache[org_id] = organization_data # Cache it
                else:
                     logger.warning(f"{log_prefix} Organization ID {org_id} not found.")
                     org_cache[org_id] = None # Cache the miss
            # --- End Fetch Organization Data --- 

            # --- Google Search --- 
            search_results = None
            try:
                domain = contact_email.split('@')[1]
                search_results = self.search_engine.execute_search(f'site:{domain}')
            except Exception as search_err:
                logger.warning(f"{log_prefix} Error during web search for {domain}: {search_err}")
            # --- End Google Search --- 

            try:
                # Call the hybrid_validator function
                passes_hurdles, validation_details = validate_contact(
                    contact=contact_data, 
                    organization_data=organization_data,
                    search_results=search_results,
                    use_gemini=True, # Mandatory
                    org_confidence_hurdle=self.hurdles['org_confidence_hurdle'],
                    name_confidence_hurdle=self.hurdles['name_confidence_hurdle']
                )

                if passes_hurdles:
                    # Check for no-name case, but don't trigger email here
                    name_confidence = validation_details.get("name_confidence")
                    if name_confidence is None and not contact_first_name:
                         logger.info(f"{log_prefix} Passed ORG validation without name.")
                         # Add the data even if no name, EmailManager will handle trigger
                         valid_contacts_data.append(contact_data)
                    else:
                        logger.info(f"{log_prefix} Passed validation hurdles.")
                        valid_contacts_data.append(contact_data)
                else:
                    # Validation failed hurdles
                    self.validation_stats["contacts_rejected"] += 1
                    reason = validation_details.get('reasons', 'Confidence score below hurdle')
                    logger.warning(f"{log_prefix} Rejected - {reason}")
                    # Skip this contact
                    continue

            except (ValueError, RuntimeError) as validation_err:
                # Catch critical validation errors but continue batch
                logger.error(f"{log_prefix} CRITICAL VALIDATION ERROR: {validation_err}")
                self.validation_stats["contacts_rejected"] += 1
                continue # Skip this contact
            except Exception as e:
                # Catch other unexpected errors but continue batch
                logger.error(f"{log_prefix} Unexpected error during validation: {e}", exc_info=True)
                self.validation_stats["contacts_rejected"] += 1
                continue # Skip this contact

        logger.info(f"Batch validation finished. {len(valid_contacts_data)} / {len(contact_data_list)} contacts passed.")
        return valid_contacts_data