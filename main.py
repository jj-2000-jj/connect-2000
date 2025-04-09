"""
Main module for the GDS Contact Management System.
"""
import argparse
import sys
import threading
import schedule
import time
import os
from datetime import datetime
from pathlib import Path
from app.database.models import init_db, get_db_session
from app.discovery.discovery_manager import DiscoveryManager
from app.email.manager import EmailManager
from app.dashboard.dashboard_wrapper import DashboardWrapper as Dashboard # Restore wrapper
# from app.dashboard.dashboard3 import Dashboard # Use dashboard3 directly
from app.utils.logger import get_logger
import dash
from dash import html

# Set OpenBLAS environment variables to prevent threading conflicts
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['VECLIB_MAXIMUM_THREADS'] = '1'
os.environ['NUMEXPR_NUM_THREADS'] = '1'

logger = get_logger(__name__)


def setup_database():
    """Set up the database and return a session."""
    from app.config import DATABASE_PATH
    
    # Create database directory if it doesn't exist
    DATABASE_PATH.parent.mkdir(exist_ok=True)
    
    # Initialize database and get a session
    with get_db_session() as session:
        return session


def run_discovery(max_orgs=20):
    """Run discovery process to find new organizations and contacts."""
    logger.info("Starting discovery process")
    
    # Get database session
    db_session = get_db_session()
    
    try:
        # Create discovery manager
        discovery_manager = DiscoveryManager(db_session)
        
        # Run scheduled discovery
        metrics = discovery_manager.run_scheduled_discovery(max_orgs_per_run=max_orgs)
        
        logger.info(f"Discovery completed. Found {metrics.get('organizations_discovered', 0)} organizations and {metrics.get('contacts_discovered', 0)} contacts")
    
    except Exception as e:
        logger.error(f"Error in discovery process: {e}")
    
    finally:
        db_session.close()


def create_email_drafts():
    """Create email drafts for contacts."""
    logger.info("Starting email draft creation")
    
    # Get database session
    db_session = get_db_session()
    
    try:
        # Create email manager
        email_manager = EmailManager(db_session)
        
        # Create email drafts
        drafts_created = email_manager.create_draft_emails()
        
        # Generate daily report
        report = email_manager.get_daily_report()
        
        # Log report summary
        total_drafts = sum(drafts_created.values())
        logger.info(f"Created {total_drafts} email drafts")
        logger.info(f"Daily Report - {datetime.now().strftime('%Y-%m-%d')}")
        logger.info(f"New contacts: {report['new_contacts']['total']}")
        logger.info(f"New email drafts: {report['new_drafts']['total']}")
        
        # Write detailed report to file
        report_dir = Path(__file__).resolve().parent.parent / "reports"
        report_dir.mkdir(exist_ok=True)
        
        report_file = report_dir / f"report_{datetime.now().strftime('%Y%m%d')}.txt"
        
        with open(report_file, "w") as f:
            f.write(f"# GDS Contact Management System - Daily Report\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d')}\n\n")
            
            f.write(f"## New Contacts: {report['new_contacts']['total']}\n\n")
            for org_type, contacts in report['new_contacts']['by_type'].items():
                f.write(f"### {org_type.capitalize()}: {len(contacts)}\n")
                for contact in contacts:
                    f.write(f"- {contact['name']} ({contact['title']}) at {contact['organization']}\n")
                f.write("\n")
            
            f.write(f"## New Email Drafts: {report['new_drafts']['total']}\n\n")
            for user, drafts in report['new_drafts']['by_user'].items():
                f.write(f"### {user}: {len(drafts)}\n")
                for draft in drafts:
                    f.write(f"- {draft['name']} ({draft['title']}) at {draft['organization']}\n")
                f.write("\n")
        
        logger.info(f"Daily report written to {report_file}")
    
    except Exception as e:
        logger.error(f"Error in email draft creation: {e}")
    
    finally:
        db_session.close()


def daily_job():
    """Run daily job for discovery and email drafts."""
    logger.info("Starting daily job")
    
    try:
        # Run discovery process
        run_discovery()
        
        # Create email drafts
        create_email_drafts()
        
        logger.info("Completed daily job")
    
    except Exception as e:
        logger.error(f"Error in daily job: {e}")


def run_dashboard():
    """Run the dashboard server."""
    db_session = None # Initialize db_session to None
    try:
        # Get database session
        db_session = get_db_session()

        # Try to run the regular dashboard with error handling (using wrapper)
        try:
            dashboard = Dashboard(db_session) # Instantiate wrapper
            dashboard.run_server(debug=False)
        except Exception as dashboard_err:
            logger.error(f"Dashboard failed: {dashboard_err}")
            # Fallback logic (optional, can be added back if needed)
            logger.info("Attempting fallback to emergency dashboard...")
            try:
                from app.emergency_dashboard import run_emergency_dashboard
                run_emergency_dashboard(port=8050)
            except Exception as fallback_err:
                logger.error(f"Fallback emergency dashboard also failed: {fallback_err}")
                raise fallback_err # Re-raise the fallback error
            
    except Exception as e:
        logger.error(f"Fatal error during dashboard setup or fallback: {e}")
        sys.exit(1)
    finally:
        # Ensure database session is closed
        if db_session: # Check if db_session was successfully assigned
            try:
                db_session.close()
                logger.info("Database session closed after dashboard exit/error.")
            except Exception as db_close_err:
                logger.error(f"Error closing database session: {db_close_err}")


def run_scheduler():
    """Run the scheduler for daily jobs."""
    logger.info("Starting scheduler")
    
    # Schedule daily discovery job to run at 1:00 AM
    schedule.every().day.at("01:00").do(daily_job)
    
    while True:
        schedule.run_pending()
        time.sleep(60)


def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(description="GDS Contact Management System")
    
    # Main command groups
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Discovery command
    discovery_parser = subparsers.add_parser("discover", help="Run discovery process")
    discovery_parser.add_argument("--max-orgs", type=int, default=20, help="Maximum number of organizations to discover")
    
    # Email command
    email_parser = subparsers.add_parser("email", help="Create email drafts")
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser("dashboard", help="Run dashboard server")
    dashboard_parser.add_argument("--port", type=int, default=None, help="Port to run the dashboard on")
    dashboard_parser.add_argument("--debug", action="store_true", help="Run in debug mode")
    
    # Scheduler command
    subparsers.add_parser("scheduler", help="Run scheduler for daily jobs")
    
    # All-in-one daily job command
    subparsers.add_parser("daily", help="Run daily job (discovery + emails)")
    
    args = parser.parse_args()
    
    try:
        if args.command == "dashboard":
            # Run dashboard with proper error handling
            run_dashboard()
        elif args.command == "discover":
            with get_db_session() as session:
                discovery_manager = DiscoveryManager(session)
                discovery_manager.run_discovery(max_orgs=args.max_orgs)
        elif args.command == "email":
            with get_db_session() as session:
                email_manager = EmailManager(session)
                email_manager.create_email_drafts()
        elif args.command == "scheduler":
            run_scheduler()
        elif args.command == "daily":
            daily_job()
        else:
            parser.print_help()
                
    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
