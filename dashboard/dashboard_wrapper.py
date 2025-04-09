"""
Dashboard wrapper for selecting the appropriate dashboard implementation.
"""
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DashboardWrapper:
    """Wrapper class for initializing the appropriate dashboard implementation."""
    
    def __init__(self, db_session, dashboard_type="discovery"):
        """
        Initialize the dashboard wrapper.
        
        Args:
            db_session: Database session
            dashboard_type: Type of dashboard to display
        """
        self.db_session = db_session
        self.dashboard_type = dashboard_type
        self.dashboard = None
        
    def run_server(self, debug=False, port=None):
        """Run the dashboard server."""
        try:
            # Always use dashboard3_fixed
            from app.dashboard.dashboard3_fixed import Dashboard
            logger.info("Using dashboard3 implementation")
            
            # Apply the proper database session using contacts.db
            if self.db_session is None:
                logger.info("Creating new database session with contacts.db")
                from sqlalchemy import create_engine
                from sqlalchemy.orm import sessionmaker
                
                # Ensure we use the contacts.db database
                db_path = './data/contacts.db'
                engine = create_engine(f'sqlite:///{db_path}')
                SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
                self.db_session = SessionLocal()
                logger.info(f"Database connection established to {db_path}")
            
            # Create dashboard instance
            self.dashboard = Dashboard(self.db_session, self.dashboard_type)
            
            # Use consistent port handling
            if port is None:
                # Try a less common port to avoid conflicts
                port = 8056
            
            # Always kill any existing dashboard processes before starting a new one
            try:
                import subprocess, signal
                subprocess.run(["pkill", "-f", "python.*dash"], stderr=subprocess.DEVNULL)
                logger.info("Killed any existing dashboard processes")
            except Exception as e:
                logger.warning(f"Error killing processes: {e}")
                
            # Run dashboard with specific port
            logger.info(f"Starting dashboard on port {port}")
            self.dashboard.run_server(debug=debug, port=port)
            
        except ImportError as e:
            logger.error(f"Could not import Dashboard3: {e}")
            self._create_minimal_dashboard(str(e), port)
        except Exception as e:
            logger.error(f"Error starting dashboard: {e}")
            self._create_minimal_dashboard(str(e), port)
            
    def _create_minimal_dashboard(self, error_message, port=None):
        """Create a minimal dashboard with error message when all else fails."""
        try:
            import dash
            from dash import html
            
            app = dash.Dash(__name__)
            app.layout = html.Div([
                html.H1("Dashboard Error"),
                html.Div([
                    html.P("An error occurred while starting the dashboard:"),
                    html.Pre(error_message, style={"backgroundColor": "#f8f9fa", "padding": "15px"})
                ]),
                html.Div([
                    html.Button("Refresh", id="refresh-button", 
                                style={"margin": "10px 0"})
                ])
            ], style={"fontFamily": "Arial", "margin": "20px"})
            
            logger.info("Starting minimal error dashboard")
            # Use a default port if None is provided
            run_port = port if port is not None else 8050 
            app.run(debug=False, port=run_port)
        except Exception as e:
            logger.error(f"Could not create minimal dashboard: {e}")
            # If even the minimal dashboard fails, just give up
            raise 