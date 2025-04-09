"""
Dashboard for monitoring the Contact Discovery System.
Version 3: Improved callback handling to fix argument errors.
"""
import dash
from dash import dcc, html, dash_table, callback_context
import socket
import os
from datetime import datetime, timedelta
from sqlalchemy import func
import logging
import sys
import subprocess

from app.models.organization import Organization
from app.models.contact import Contact
from app.models.email import Email
from app.models.discovery import Discovery
from app.dashboard.dashboard_components import (
    create_organizations_tab, create_contacts_tab, 
    create_emails_tab, create_metrics_tab,
    generate_top_metrics, COLORS, EMAIL_OPTIONS
)
# Import create_settings_tab from layout_parts instead of dashboard_components
from app.dashboard.layout_parts import create_settings_tab, APP_MODES
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
            ],
            suppress_callback_exceptions=True  # Important for complex layouts
        )
        
        # Define CSS styles
        self.app.index_string = self._create_index_string()
        
        # Set up layout
        self.app.layout = self._create_layout()
        
        # Register callbacks directly in this class
        self._register_callbacks()
    
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
                            className="col-md-6",
                            children=[
                                html.H1(self.app.title),
                                html.P("Monitor discovery status and contact generation")
                            ]
                        ),
                        html.Div(
                            className="col-md-6 d-flex justify-content-end",
                            children=[
                                dcc.Dropdown(
                                    id='app-mode-selector',
                                    options=APP_MODES,
                                    value=APP_MODES[0]['value'],
                                    clearable=False,
                                    style={'width': '350px', 'marginLeft': 'auto'}
                                )
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
    
    def _register_callbacks(self):
        """Register all dashboard callbacks."""
        
        # Fixed the toggle_target_filters callback to handle *args safely
        @self.app.callback(
            [
                dash.Output("target-org-types-container", "style"),
                dash.Output("target-states-container", "style"),
                dash.Output("target-states", "options")
            ],
            [dash.Input("app-mode-selector", "value")]
        )
        def toggle_target_filters(app_mode):
            try:
                # Get states for dropdown using direct SQL for simplicity
                states = self.db_session.query(Organization.state).distinct().filter(
                    Organization.state.isnot(None),
                    Organization.state != ''
                ).order_by(Organization.state).all()
                
                state_options = [{"label": "All States", "value": "all"}] + [
                    {"label": state[0], "value": state[0]} for state in states
                ]
                
                # Show filters for all modes
                visible_style = {"display": "block"}
                hidden_style = {"display": "none"}
                
                # Show org type filter for all modes
                if app_mode in ["org_building", "contact_building", "sending_emails"]:
                    return visible_style, visible_style, state_options
                
                # Default case (shouldn't happen)
                return hidden_style, hidden_style, state_options
                
            except Exception as e:
                logger.error(f"Error in toggle_target_filters: {e}")
                # Return default values on error
                return {"display": "none"}, {"display": "none"}, [{"label": "All States", "value": "all"}]
        
        # Update dashboard metrics callback
        @self.app.callback(
            [
                dash.Output("overview-metrics", "children"),
                dash.Output("discoveries-chart", "figure"),
                dash.Output("org-types-chart", "figure"),
                dash.Output("org-states-chart", "figure"),
                dash.Output("emails-chart", "figure"),
                dash.Output("performance-chart", "figure"),
                dash.Output("filter-state", "options")
            ],
            [dash.Input("time-range-dropdown", "value")]
        )
        def update_dashboard(time_range):
            try:
                # Calculate time range
                end_date = datetime.now()
                if time_range == "24h":
                    start_date = end_date - timedelta(hours=24)
                elif time_range == "7d":
                    start_date = end_date - timedelta(days=7)
                elif time_range == "30d":
                    start_date = end_date - timedelta(days=30)
                else:  # all time
                    start_date = datetime(2000, 1, 1)

                # Get metrics
                overview_metrics = self._get_overview_metrics(start_date, end_date)
                discoveries_chart = self._get_discoveries_chart(start_date, end_date)
                org_types_chart = self._get_org_types_chart()
                org_states_chart = self._get_org_states_chart()
                emails_chart = self._get_emails_chart(start_date, end_date)
                performance_chart = self._get_performance_chart(start_date, end_date)

                # Get state options for filter
                states = self.db_session.query(Organization.state).distinct().filter(
                    Organization.state.isnot(None)
                ).all()
                state_options = [{"label": "All States", "value": "all"}] + [
                    {"label": state[0], "value": state[0]} for state in states
                ]

                return (
                    overview_metrics,
                    discoveries_chart,
                    org_types_chart,
                    org_states_chart,
                    emails_chart,
                    performance_chart,
                    state_options
                )
            except Exception as e:
                logger.error(f"Error updating dashboard: {e}")
                # Restore original return for multiple outputs
                return [], {}, {}, {}, {}, {}, [{"label": "All States", "value": "all"}]
    
    def _get_overview_metrics(self, start_date, end_date):
        """Get overview metrics for the dashboard."""
        try:
            # Get total organizations
            total_orgs = self.db_session.query(func.count(Organization.id)).scalar() or 0
            
            # Get organizations discovered in date range - handle missing column
            try:
                # Try with discovered_at if it exists
                new_orgs = self.db_session.query(func.count(Organization.id)).filter(
                    Organization.discovered_at >= start_date,
                    Organization.discovered_at <= end_date
                ).scalar() or 0
            except Exception:
                # Fallback to created_at if discovered_at doesn't exist
                try:
                    new_orgs = self.db_session.query(func.count(Organization.id)).filter(
                        Organization.created_at >= start_date,
                        Organization.created_at <= end_date
                    ).scalar() or 0
                except Exception:
                    # If neither column exists, default to 0
                    new_orgs = 0
            
            # Get total contacts
            total_contacts = self.db_session.query(func.count(Contact.id)).scalar() or 0
            
            # Get contacts created in date range
            new_contacts = self.db_session.query(func.count(Contact.id)).filter(
                Contact.created_at >= start_date,
                Contact.created_at <= end_date
            ).scalar() or 0
            
            # Get total emails sent
            total_emails = self.db_session.query(func.count(Email.id)).scalar()
            
            # Get emails sent in date range
            new_emails = self.db_session.query(func.count(Email.id)).filter(
                Email.sent_at >= start_date,
                Email.sent_at <= end_date
            ).scalar()
            
            # Calculate success rates
            org_success_rate = round((new_orgs / total_orgs * 100) if total_orgs > 0 else 0, 1)
            contact_success_rate = round((new_contacts / total_contacts * 100) if total_contacts > 0 else 0, 1)
            email_success_rate = round((new_emails / total_emails * 100) if total_emails > 0 else 0, 1)
            
            return [
                html.Div([
                    html.H3(f"{total_orgs:,}", className="metric-value"),
                    html.P("Total Organizations", className="metric-label"),
                    html.P(f"+{new_orgs:,} ({org_success_rate}%)", className="metric-change")
                ], className="metric-card"),
                html.Div([
                    html.H3(f"{total_contacts:,}", className="metric-value"),
                    html.P("Total Contacts", className="metric-label"),
                    html.P(f"+{new_contacts:,} ({contact_success_rate}%)", className="metric-change")
                ], className="metric-card"),
                html.Div([
                    html.H3(f"{total_emails:,}", className="metric-value"),
                    html.P("Total Emails Sent", className="metric-label"),
                    html.P(f"+{new_emails:,} ({email_success_rate}%)", className="metric-change")
                ], className="metric-card")
            ]
        except Exception as e:
            logger.error(f"Error getting overview metrics: {e}")
            return []
    
    def _get_discoveries_chart(self, start_date, end_date):
        """Get discoveries chart data."""
        try:
            # Get daily discovery counts - handle missing column
            try:
                # Try with discovered_at if it exists
                daily_discoveries = self.db_session.query(
                    func.date(Organization.discovered_at).label('date'),
                    func.count(Organization.id).label('count')
                ).filter(
                    Organization.discovered_at.between(start_date, end_date)
                ).group_by(
                    func.date(Organization.discovered_at)
                ).order_by(
                    func.date(Organization.discovered_at)
                ).all()
            except Exception:
                # Fallback to created_at if discovered_at doesn't exist
                try:
                    daily_discoveries = self.db_session.query(
                        func.date(Organization.created_at).label('date'),
                        func.count(Organization.id).label('count')
                    ).filter(
                        Organization.created_at.between(start_date, end_date)
                    ).group_by(
                        func.date(Organization.created_at)
                    ).order_by(
                        func.date(Organization.created_at)
                    ).all()
                except Exception:
                    # If neither column exists, return empty list
                    daily_discoveries = []
                    
            # Format data for chart
            dates = [d[0].strftime('%Y-%m-%d') for d in daily_discoveries]
            counts = [d[1] for d in daily_discoveries]
            
            return {
                'data': [
                    {
                        'x': dates,
                        'y': counts,
                        'type': 'bar',
                        'name': 'New Organizations',
                        'marker': {'color': COLORS['primary']}
                    }
                ],
                'layout': {
                    'title': 'Daily Organization Discoveries',
                    'xaxis': {'title': 'Date'},
                    'yaxis': {'title': 'Count'},
                    'showlegend': False,
                    'height': 300
                }
            }
        except Exception as e:
            logger.error(f"Error getting discoveries chart: {e}")
            return {}
    
    def _get_org_types_chart(self):
        """Get organization types chart data."""
        try:
            # Get organization type counts
            type_counts = self.db_session.query(
                Organization.org_type,
                func.count(Organization.id).label('count')
            ).group_by(
                Organization.org_type
            ).all()
            
            # Format data for chart
            types = [t[0] for t in type_counts]
            counts = [t[1] for t in type_counts]
            
            return {
                'data': [
                    {
                        'labels': types,
                        'values': counts,
                        'type': 'pie',
                        'marker': {'colors': [COLORS['primary'], COLORS['success'], COLORS['warning']]}
                    }
                ],
                'layout': {
                    'title': 'Organization Types',
                    'showlegend': True,
                    'height': 300
                }
            }
        except Exception as e:
            logger.error(f"Error getting org types chart: {e}")
            return {}
    
    def _get_org_states_chart(self):
        """Get organization states chart data."""
        try:
            # Get state counts
            state_counts = self.db_session.query(
                Organization.state,
                func.count(Organization.id).label('count')
            ).filter(
                Organization.state.isnot(None)
            ).group_by(
                Organization.state
            ).order_by(
                func.count(Organization.id).desc()
            ).limit(10).all()
            
            # Format data for chart
            states = [s[0] for s in state_counts]
            counts = [s[1] for s in state_counts]
            
            return {
                'data': [
                    {
                        'x': states,
                        'y': counts,
                        'type': 'bar',
                        'name': 'Organizations by State',
                        'marker': {'color': COLORS['primary']}
                    }
                ],
                'layout': {
                    'title': 'Top 10 States',
                    'xaxis': {'title': 'State'},
                    'yaxis': {'title': 'Count'},
                    'showlegend': False,
                    'height': 300
                }
            }
        except Exception as e:
            logger.error(f"Error getting org states chart: {e}")
            return {}
    
    def _get_emails_chart(self, start_date, end_date):
        """Get emails chart data."""
        try:
            # Get daily email counts
            daily_emails = self.db_session.query(
                func.date(Email.sent_at).label('date'),
                func.count(Email.id).label('count')
            ).filter(
                Email.sent_at.between(start_date, end_date)
            ).group_by(
                func.date(Email.sent_at)
            ).order_by(
                func.date(Email.sent_at)
            ).all()
            
            # Format data for chart
            dates = [d[0].strftime('%Y-%m-%d') for d in daily_emails]
            counts = [d[1] for d in daily_emails]
            
            return {
                'data': [
                    {
                        'x': dates,
                        'y': counts,
                        'type': 'bar',
                        'name': 'Emails Sent',
                        'marker': {'color': COLORS['success']}
                    }
                ],
                'layout': {
                    'title': 'Daily Emails Sent',
                    'xaxis': {'title': 'Date'},
                    'yaxis': {'title': 'Count'},
                    'showlegend': False,
                    'height': 300
                }
            }
        except Exception as e:
            logger.error(f"Error getting emails chart: {e}")
            return {}
    
    def _get_performance_chart(self, start_date, end_date):
        """Get performance chart data."""
        try:
            # Get daily performance metrics
            daily_performance = self.db_session.query(
                func.date(Organization.discovered_at).label('date'),
                func.avg(Organization.relevance_score).label('avg_relevance'),
                func.count(Organization.id).label('org_count'),
                func.count(Contact.id).label('contact_count')
            ).outerjoin(
                Contact, Contact.organization_id == Organization.id
            ).filter(
                Organization.discovered_at.between(start_date, end_date)
            ).group_by(
                func.date(Organization.discovered_at)
            ).order_by(
                func.date(Organization.discovered_at)
            ).all()
            
            # Format data for chart
            dates = [d[0].strftime('%Y-%m-%d') for d in daily_performance]
            avg_relevance = [float(d[1]) * 10 for d in daily_performance]  # Convert to 1-10 scale
            org_counts = [d[2] for d in daily_performance]
            contact_counts = [d[3] for d in daily_performance]
            
            return {
                'data': [
                    {
                        'x': dates,
                        'y': avg_relevance,
                        'type': 'line',
                        'name': 'Avg Relevance',
                        'marker': {'color': COLORS['primary']}
                    },
                    {
                        'x': dates,
                        'y': org_counts,
                        'type': 'bar',
                        'name': 'Organizations',
                        'marker': {'color': COLORS['success']}
                    },
                    {
                        'x': dates,
                        'y': contact_counts,
                        'type': 'bar',
                        'name': 'Contacts',
                        'marker': {'color': COLORS['warning']}
                    }
                ],
                'layout': {
                    'title': 'Daily Performance Metrics',
                    'xaxis': {'title': 'Date'},
                    'yaxis': {'title': 'Count'},
                    'yaxis2': {
                        'title': 'Relevance Score',
                        'overlaying': 'y',
                        'side': 'right',
                        'range': [0, 10]
                    },
                    'showlegend': True,
                    'height': 300
                }
            }
        except Exception as e:
            logger.error(f"Error getting performance chart: {e}")
            return {}
    
    def run_server(self, debug=False, port=None):
        """
        Run the dashboard server.
        
        Args:
            debug: Whether to run in debug mode
            port: Port to run on (defaults to DASHBOARD_PORT)
        """
        try:
            # Import here to avoid circular imports
            from app.config import DASHBOARD_PORT
            
            # Configure port
            if port is None:
                port = DASHBOARD_PORT
            
            # Find an available port if the default is in use
            retry_count = 0
            original_port = port
            
            while retry_count < 10:  # Try up to 10 alternative ports
                try:
                    # Test if port is in use
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.bind(('127.0.0.1', port))
                    sock.close()
                    break  # Port is available
                except socket.error:
                    logger.warning(f"Port {port} is in use, trying port {port+1}")
                    port += 1
                    retry_count += 1
            
            if port != original_port:
                logger.info(f"Using alternative port {port} instead of {original_port}")
                
            logger.info(f"Starting dashboard on port {port}")
            self.app.run(debug=debug, port=port)
            
        except Exception as e:
            logger.error(f"Error running dashboard server: {e}")
            
        finally:
            # Ensure the database session is closed when the server stops
            if hasattr(self, 'db_session') and self.db_session:
                try:
                    self.db_session.close()
                    logger.info("Database session closed")
                except Exception as e:
                    logger.error(f"Error closing database session: {e}")
    
    # Also add a compatibility method for the new API
    def run(self, debug=False, port=None):
        """
        Run the dashboard server (using the new Dash API).
        
        Args:
            debug: Whether to run in debug mode
            port: Port to run on
        """
        self.run_server(debug=debug, port=port) 