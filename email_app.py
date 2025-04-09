"""
Email Application for the GBL Data Contact Management System.

This application focuses on:
1. Creating personalized email drafts for contacts
2. Managing email assignments to sales team
3. Tracking email campaign performance
4. Generating email reports
"""
import argparse
import logging
import os
import sys
import threading
import schedule
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
import json
import traceback

# Set OpenBLAS environment variables to prevent threading conflicts
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['VECLIB_MAXIMUM_THREADS'] = '1'
os.environ['NUMEXPR_NUM_THREADS'] = '1'

from app.config import EMAIL_USERS
from app.database.models import init_db, get_db_session, ProcessSummary, EmailEngagement, Contact
from app.email.manager import EmailManager
from app.dashboard.dashboard_wrapper import DashboardWrapper as Dashboard
from app.utils.logger import get_logger
from app.database.crud import create_process_summary, update_process_summary

logger = get_logger(__name__)


def create_email_drafts(max_per_salesperson=20, min_confidence=0.5, target_org_types=None, target_states=None, auto_send=False, no_sandbox=False, use_individual_thresholds=False, show_config=False):
    """
    Create personalized email drafts for contacts in the database.
    
    Args:
        max_per_salesperson: Maximum number of emails per salesperson
        min_confidence: Minimum confidence score for contacts (0.0-1.0)
        target_org_types: Specific organization types to target (comma-separated string or list)
        target_states: Specific states to target (comma-separated string or list)
        auto_send: Enable auto-sending for high-confidence contacts
        no_sandbox: Disable sandbox mode (WILL SEND REAL EMAILS)
        use_individual_thresholds: Use individual confidence threshold
        show_config: Show configuration details
    """
    logger.info("Starting email draft creation")
    
    # Convert comma-separated string to list if needed
    if target_org_types and isinstance(target_org_types, str):
        target_org_types = [t.strip() for t in target_org_types.split(',')]
    
    if target_states and isinstance(target_states, str):
        target_states = [s.strip() for s in target_states.split(',')]
    
    # Set default values if not provided
    if target_org_types is None:
        target_org_types = ["all"]
    
    if target_states is None:
        target_states = ["all"]
    
    logger.info(f"Targeting org types: {target_org_types}")
    logger.info(f"Targeting states: {target_states}")
    
    # Show configuration if requested
    if show_config:
        # Read validation config from JSON file
        config_path = 'app/validation/validation_config.json'
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    logger.info(f"Validation config: {config}")
            except Exception as e:
                logger.error(f"Error reading validation config: {e}")
        else:
            logger.warning(f"Validation config file not found: {config_path}")
            
        logger.info(f"Use individual thresholds: {use_individual_thresholds}")
        logger.info(f"Auto-send: {auto_send}")
        logger.info(f"Sandbox mode: {not no_sandbox}")
    
    # Set up database session
    db = get_db_session()
    
    # Create email manager with appropriate configuration
    email_manager = EmailManager(
        db,
        auto_send=auto_send,
        sandbox_mode=not no_sandbox,
        use_individual_thresholds=use_individual_thresholds
    )
    
    # Make sure the client is set up
    email_manager.setup()
    
    try:
        # Use the newer create_draft_emails method instead of process_contacts_for_user
        results = email_manager.create_draft_emails(
            max_per_salesperson=max_per_salesperson,
            min_confidence=min_confidence,
            target_org_types=target_org_types,
            target_states=target_states,
            auto_send_enabled=auto_send,
            sandbox_mode=not no_sandbox
        )
        
        # Extract totals from results
        total_emails = results.get('total', 0)
        total_sent = results.get('total_sent', 0)
        total_drafts = results.get('total_draft', 0)
        
        logger.info(f"Created {total_emails} emails ({total_sent} auto-sent, {total_drafts} drafts)")
        return total_emails
    
    except Exception as e:
        logger.error(f"Error creating email drafts: {e}")
        return 0
    
    finally:
        # Close the database session
        db.close()


def generate_email_report(report, drafts_created):
    """Generate a detailed report of the email campaign."""
    report_dir = Path(__file__).resolve().parent.parent / "reports"
    report_dir.mkdir(exist_ok=True)
    
    report_file = report_dir / f"email_campaign_{datetime.now().strftime('%Y%m%d')}.txt"
    
    with open(report_file, "w") as f:
        f.write(f"# GBL Data Email Campaign - Report\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d')}\n\n")
        
        f.write(f"## Email Campaign Summary\n\n")
        f.write(f"- Total draft emails created: {sum(drafts_created.values())}\n")
        
        # Add details for each sales person
        f.write("\n### By Sales Person\n\n")
        for email, count in drafts_created.items():
            f.write(f"- {email}: {count}\n")
        
        if report['new_contacts']['total'] > 0:
            f.write("\n## New Contacts\n\n")
            f.write(f"Total: {report['new_contacts']['total']}\n\n")
            for org_type, contacts in report['new_contacts']['by_type'].items():
                f.write(f"### {org_type.capitalize()}: {len(contacts)}\n")
                for contact in contacts:
                    f.write(f"- {contact['name']} ({contact['title']}) at {contact['organization']}\n")
                f.write("\n")
        
        if report['new_drafts']['total'] > 0:
            f.write("\n## New Email Drafts\n\n")
            f.write(f"Total: {report['new_drafts']['total']}\n\n")
            for user, drafts in report['new_drafts']['by_user'].items():
                f.write(f"### {user}: {len(drafts)}\n")
                for draft in drafts:
                    f.write(f"- {draft['name']} ({draft['title']}) at {draft['organization']}\n")
                f.write("\n")
    
    return report_file


def run_email_campaign_scheduler(frequency="daily", time="06:00", max_per_salesperson=20, min_confidence=0.5):
    """Run scheduler for email campaigns."""
    logger.info(f"Starting email campaign scheduler (frequency={frequency}, time={time})")
    
    if frequency == "daily":
        schedule.every().day.at(time).do(create_email_drafts, 
                                         max_per_salesperson=max_per_salesperson, 
                                         min_confidence=min_confidence)
    elif frequency == "weekly":
        schedule.every().monday.at(time).do(create_email_drafts, 
                                           max_per_salesperson=max_per_salesperson, 
                                           min_confidence=min_confidence)
    else:
        logger.error(f"Unsupported frequency: {frequency}")
        return
    
    while True:
        schedule.run_pending()
        time.sleep(60)


def run_dashboard():
    """Run the email campaign dashboard server."""
    try:
        # Get database session
        db_session = get_db_session()
        
        # Create dashboard
        dashboard = Dashboard(db_session, dashboard_type="email")
        
        # Run dashboard server
        dashboard.run_server()
    
    except Exception as e:
        logger.error(f"Error in dashboard: {e}")


def process_emails(assigned_to=None, num_emails=10, sandbox=True, reset_sent=False, 
                  show_config=False, use_individual_thresholds=False):
    """Process emails for a given user or all users."""
    session = get_db_session()
    email_client = get_email_client(sandbox=sandbox)
    contact_repo = ContactRepository(session)
    counter = {'drafts': 0, 'auto-sent': 0, 'total': 0}
    
    # Show config information if requested
    if show_config:
        # Display the latest validation thresholds for debugging
        validator = AdvancedEmailValidator()
        logger.info(f"Loaded validation thresholds: org={validator.hurdles['org_confidence_hurdle']}, " +
                    f"contact={validator.hurdles['name_confidence_hurdle']}")
    
    # Handle batch processing for all users if no specific user assigned_to
    if not assigned_to:
        users = get_email_users_from_config()
        for user_email, user_org_types in users.items():
            logger.info(f"Processing for user: {user_email}, Types: {user_org_types}")
            try:
                # Always clear the assigned_to field before processing to ensure fresh assignments
                if not reset_sent:
                    session.query(Contact).filter(Contact.assigned_to == user_email).update({
                        "assigned_to": None
                    })
                    session.commit()
                
                user_counter = process_emails(user_email, num_emails, sandbox, reset_sent, show_config, use_individual_thresholds)
                counter['drafts'] += user_counter.get('drafts', 0)
                counter['auto-sent'] += user_counter.get('auto-sent', 0)
                counter['total'] += user_counter.get('total', 0)
            except Exception as e:
                logger.error(f"Error processing emails for {user_email}: {e}")
        return counter


def run_email_campaign(campaign_config, db_session):
    """
    Run the email campaign with the given configuration.
    
    Args:
        campaign_config: Configuration for the campaign
        db_session: Database session
    """
    auto_send = campaign_config.get('auto_send', False)
    sandbox_mode = campaign_config.get('sandbox_mode', True)
    target_org_types = campaign_config.get('target_org_types')
    max_per_salesperson = campaign_config.get('max_per_salesperson')
    
    logger.info(f"Running email campaign: auto_send={auto_send}, sandbox={sandbox_mode}")
    
    # Get a fresh database connection to avoid conflicts
    from app.database.models import get_db_session, ProcessSummary
    session = get_db_session()
    
    # Create process summary record
    from app.database import crud
    process_summary = ProcessSummary(
        process_type='email_sending',
        started_at=datetime.datetime.utcnow(),
        status="running",
        items_processed=0,
        items_added=0
    )
    session.add(process_summary)
    session.commit()
    session.refresh(process_summary)
    
    logger.info(f"Created process summary record with ID {process_summary.id}")
    
    try:
        # Create email manager
        email_manager = EmailManager(db_session, auto_send=auto_send, sandbox_mode=sandbox_mode)
        
        # Create draft emails
        results = email_manager.create_draft_emails(
            max_per_salesperson=max_per_salesperson,
            target_org_types=target_org_types,
            auto_send_enabled=auto_send
        )
        
        # Log results
        total_processed = results.get('total', 0)
        total_sent = results.get('total_sent', 0)
        total_drafted = results.get('total_draft', 0)
        
        logger.info(f"Email campaign completed: {total_processed} processed, {total_sent} sent, {total_drafted} drafted")
        
        # Collect details for the process summary
        details = {
            'emails_sent': [],
            'emails_drafted': []
        }
        
        try:
            # Get details of sent emails - note there may not be any in this test
            if total_sent > 0:
                # Log the search parameters
                logger.info(f"Looking for sent emails after {process_summary.started_at}")
                
                # Get all ContactEngagements regardless of status
                all_engagements = db_session.query(EmailEngagement).all()
                logger.info(f"Found {len(all_engagements)} total email engagements in database")
                
                # Get contact details to use for the emails
                contacts = db_session.query(Contact).all()
                contact_map = {c.id: c for c in contacts}
                
                # Add dummy data if no actual emails were sent
                if total_sent == 0:
                    logger.info("No actual emails sent, adding placeholder data")
                    for i in range(email_limit):
                        details['emails_sent'].append({
                            'to_email': "test@example.com",
                            'subject': "Intro to GDS (Test)",
                            'sent_at': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                            'contact_id': None 
                        })
                else:
                    for email in all_engagements:
                        contact = contact_map.get(email.contact_id)
                        if contact:
                            details['emails_sent'].append({
                                'to_email': contact.email,
                                'subject': "Intro to GDS",
                                'sent_at': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                'contact_id': contact.id
                            })
            
            # Update process summary - ensure details is a string (JSON)
            process_summary.completed_at = datetime.datetime.utcnow()
            process_summary.status = 'completed'
            process_summary.items_processed = total_processed
            process_summary.items_added = total_sent + total_drafted
            process_summary.details = json.dumps(details)
            
            # Save to database
            session.commit()
            logger.info(f"Updated process summary record {process_summary.id} with details: {len(details['emails_sent'])} sent, {len(details['emails_drafted'])} drafted")
        except Exception as detail_error:
            logger.error(f"Error collecting process summary details: {detail_error}", exc_info=True)
            # Continue with the process even if detail collection fails
        
        return results
    except Exception as e:
        logger.error(f"Error running email campaign: {e}")
        
        # Update process summary with error status
        try:
            process_summary.completed_at = datetime.datetime.utcnow()
            process_summary.status = 'failed'
            process_summary.items_processed = 0
            process_summary.items_added = 0
            process_summary.details = json.dumps({'error': str(e)})
            session.commit()
            logger.info(f"Updated process summary record {process_summary.id} with error details")
        except Exception as update_error:
            logger.error(f"Error updating process summary: {update_error}")
        
        raise
    finally:
        session.close()


def main():
    """Main function for the email app."""
    parser = argparse.ArgumentParser(description='GBL Data Email Application')
    parser.add_argument('mode', choices=['campaign', 'dashboard'], help='Operating mode')
    parser.add_argument('--auto-send', action='store_true', help='Auto-send emails (default: draft only)')
    parser.add_argument('--sandbox', action='store_true', help='Run in sandbox mode (redirect emails to test account)')
    parser.add_argument('--target-org-types', type=str, help='Target specific organization types (comma-separated)')
    parser.add_argument('--max-per-salesperson', type=int, default=None, help='Maximum emails per salesperson')
    parser.add_argument('--show-config', action='store_true', help='Show configuration')
    
    args = parser.parse_args()
    
    if args.show_config:
        logger.info("Configuration:")
        logger.info(f"  Mode: {args.mode}")
        logger.info(f"  Auto-send: {args.auto_send}")
        logger.info(f"  Sandbox: {args.sandbox}")
        logger.info(f"  Target org types: {args.target_org_types}")
        logger.info(f"  Max per salesperson: {args.max_per_salesperson}")
    
    # Create database session
    db_session = get_db_session()
    
    try:
        if args.mode == 'campaign':
            # Configure campaign
            campaign_config = {
                'auto_send': args.auto_send,
                'sandbox_mode': args.sandbox,
                'max_per_salesperson': args.max_per_salesperson
            }
            
            if args.target_org_types:
                campaign_config['target_org_types'] = [t.strip() for t in args.target_org_types.split(',')]
            
            # Run campaign
            results = run_email_campaign(campaign_config, db_session)
            
            # Print results
            logger.info(f"Created {results.get('total', 0)} emails ({results.get('total_sent', 0)} auto-sent, {results.get('total_draft', 0)} drafts)")
            
        elif args.mode == 'dashboard':
            # Start dashboard
            dashboard = Dashboard(db_session, dashboard_type="email")
            dashboard.run_server(debug=True)
    except Exception as e:
        logger.error(f"Error running {args.mode}: {e}")
        traceback.print_exc()
    finally:
        db_session.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Email application stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)