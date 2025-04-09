#!/usr/bin/env python3
"""
Main script to run the dashboard.
"""
import os
import sys
import logging
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run the dashboard."""
    # Parse arguments
    parser = argparse.ArgumentParser(description="Dashboard for Contact Discovery System")
    parser.add_argument("--port", type=int, help="Port to run the dashboard on")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode")
    parser.add_argument("--type", choices=["discovery", "email"], default="discovery", 
                        help="Type of dashboard to display")
    args = parser.parse_args()
    
    try:
        # Import here so any import errors are caught
        from app.dashboard.db_connector import get_dashboard_session
        from app.dashboard.dashboard_wrapper import DashboardWrapper
        
        # Get database session
        db_session = get_dashboard_session()
        
        # Create dashboard wrapper
        dashboard = DashboardWrapper(db_session, dashboard_type=args.type)
        
        # Run dashboard
        logger.info(f"Starting dashboard of type '{args.type}'")
        dashboard.run_server(debug=args.debug, port=args.port)
        
    except Exception as e:
        logger.error(f"Error running dashboard: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 