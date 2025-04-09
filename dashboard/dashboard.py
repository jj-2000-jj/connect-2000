"""
Dashboard for monitoring the Contact Discovery System.
"""
import dash
from dash import dcc, html, dash_table
import socket
import os
from datetime import datetime

from app.dashboard.dashboard_components import (
    create_organizations_tab, create_contacts_tab, 
    create_emails_tab, create_metrics_tab,
    create_process_summaries_tab,
    generate_top_metrics, COLORS, EMAIL_OPTIONS
)
from app.dashboard.layout_parts import create_settings_tab, APP_MODES
from app.dashboard.dashboard_callbacks import register_callbacks
from app.utils.logger import get_logger

logger = get_logger(__name__)

class Dashboard:
    """Dashboard for monitoring system metrics and discoveries."""
    
    def __init__(self, db_session, dashboard_type="discovery"):
        """
        Initialize the dashboard.
        
        Args:
            db_session: Database session
            dashboard_type: Type of dashboard to display (discovery or email)
        """
        self.db_session = db_session
        self.dashboard_type = dashboard_type
        
        # Set title based on dashboard type
        if dashboard_type == "email":
            title = "GDS Email Campaign Dashboard"
        else:
            title = "Global Data Specialists Contact"
            
        # Initialize Dash app with modern theme
        self.app = dash.Dash(
            __name__, 
            title=title,
            external_stylesheets=[
                "https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap",
            ],
            meta_tags=[
                {"name": "viewport", "content": "width=device-width, initial-scale=1"}
            ]
        )
        
        # Define CSS styles
        self.app.index_string = self._create_index_string()
        
        # Set up layout
        self.app.layout = self._create_layout()
        
        # Register callbacks
        register_callbacks(self.app, self.db_session)
    
    def _create_index_string(self):
        """Create HTML index with CSS styles."""
        # Load CSS from external file
        css_content = ""
        css_path = os.path.join(os.path.dirname(__file__), "dashboard.css")
        if os.path.exists(css_path):
            with open(css_path, "r") as f:
                css_content = f.read()

        return f'''
        <!DOCTYPE html>
        <html>
            <head>
                {{%metas%}}
                <title>{{%title%}}</title>
                {{%favicon%}}
                {{%css%}}
                <style>
                {css_content}
                </style>
            </head>
            <body>
                {{%app_entry%}}
                <footer>
                    {{%config%}}
                    {{%scripts%}}
                    {{%renderer%}}
                </footer>
            </body>
        </html>
        '''
    
    def _create_layout(self):
        """Create the dashboard layout."""
        # Common header
        header = html.Div(
            className="header",
            children=[
                html.Div(
                    className="row",
                    children=[
                        html.Div(
                            className="col-md-12",
                            children=[
                                html.H1(self.app.title),
                                html.P("Monitor discovery status and contact generation")
                            ]
                        )
                    ]
                )
            ]
        )
        
        # Top metrics row
        metrics_row = html.Div(
            id='metrics-container',
            className="metrics-container",
            children=generate_top_metrics(self.db_session)
        )
        
        # Main content with tabs
        main_content = html.Div(
            className="row",
            children=[
                html.Div(
                    className="col-12",
                    children=[
                        dcc.Tabs(
                            id="tabs",
                            value="organizations",
                            className="tab-container",
                            children=[
                                dcc.Tab(
                                    label="Organizations",
                                    value="organizations",
                                    className="tab",
                                    selected_className="tab--selected",
                                    children=create_organizations_tab()
                                ),
                                dcc.Tab(
                                    label="Contacts",
                                    value="contacts",
                                    className="tab",
                                    selected_className="tab--selected",
                                    children=create_contacts_tab()
                                ),
                                dcc.Tab(
                                    label="Email Campaigns",
                                    value="emails",
                                    className="tab",
                                    selected_className="tab--selected",
                                    children=create_emails_tab()
                                ),
                                dcc.Tab(
                                    label="Process Summaries",
                                    value="process_summaries",
                                    className="tab",
                                    selected_className="tab--selected",
                                    children=create_process_summaries_tab()
                                ),
                                dcc.Tab(
                                    label="Metrics & Analytics",
                                    value="metrics",
                                    className="tab",
                                    selected_className="tab--selected",
                                    children=create_metrics_tab()
                                ),
                                dcc.Tab(
                                    label="Settings",
                                    value="settings",
                                    className="tab",
                                    selected_className="tab--selected",
                                    children=create_settings_tab()
                                )
                            ]
                        )
                    ]
                )
            ]
        )
        
        # Store components for state
        stores = [
            dcc.Store(id='selected-organization-id', data=None),
            dcc.Store(id='organizations-data', data=[]),
            dcc.Interval(id='interval-component', interval=5*60*1000, n_intervals=0)  # 5-minute refresh
        ]
        
        # Combine all components
        return html.Div(
            className="dash-container",
            children=[header, metrics_row, main_content] + stores
        )
    
    def run_server(self, debug=False, port=None):
        """Run the dashboard server."""
        if port is None:
            port = self._find_available_port()
            
        try:
            self.app.run_server(debug=debug, port=port)
        except Exception as e:
            logger.error(f"Error running dashboard server: {e}")
            raise
    
    def _find_available_port(self):
        """Find an available port to run the server on."""
        try:
            # Try to bind to port 8050 first
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('', 8050))
            s.close()
            return 8050
        except OSError:
            # If 8050 is taken, try 8051
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(('', 8051))
                s.close()
                return 8051
            except OSError:
                # If both are taken, use a random port
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(('', 0))
                port = s.getsockname()[1]
                s.close()
                return port
