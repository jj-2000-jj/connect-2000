"""
Dashboard for monitoring the Contact Discovery System.
Fixed version that works with the actual database schema.
"""
import dash
from dash import dcc, html, dash_table, callback_context
import socket
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import func, desc
import logging
import sys
import subprocess
import numpy as np
import pandas as pd

# Import our compatible models instead of the original ones
from app.models.dashboard_models import Organization, Contact, EmailEngagement

# Import components from dashboard_components.py
from app.dashboard.dashboard_components import (
    create_organizations_tab, create_contacts_tab, 
    create_emails_tab, create_metrics_tab, 
    create_validation_reports_tab,
    EMAIL_OPTIONS,
    create_process_summaries_tab
)

# Import parts from layout_parts.py
from app.dashboard.layout_parts import (
    create_settings_store, create_settings_tab, APP_MODES
)

# Define our own colors for charts
COLORS = {
    'blue': '#366BEC',
    'green': '#48C55A',
    'orange': '#FFA500',
    'red': '#FF5252',
    'purple': '#800080',
    'teal': '#008080',
    'gray': '#808080'
}

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        
        # Top metrics row - generate simple metrics instead of using the function
        metrics_row = html.Div(
            id='metrics-container',
            className="metrics-container",
            children=self._generate_top_metrics()
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
                                ),
                                dcc.Tab(
                                    label="Validation Reports",
                                    value="validation_reports",
                                    className="tab",
                                    selected_className="tab--selected",
                                    children=create_validation_reports_tab()
                                ),
                                dcc.Tab(
                                    label="Process Summaries",
                                    value="process_summaries",
                                    className="tab",
                                    selected_className="tab--selected",
                                    children=create_process_summaries_tab()
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
            dcc.Interval(id='interval-component', interval=5*60*1000, n_intervals=0),  # 5-minute refresh
            # Add the settings store
            create_settings_store()
        ]
        
        # Combine all components
        return html.Div(
            className="dash-container",
            children=[header, metrics_row, main_content] + stores
        )
    
    def _generate_top_metrics(self):
        """Generate top metrics for the dashboard using our models."""
        try:
            # Get totals from the database
            total_orgs = self.db_session.query(func.count(Organization.id)).scalar() or 0
            total_contacts = self.db_session.query(func.count(Contact.id)).scalar() or 0
            
            # For emails, use email engagements as a proxy
            total_emails = self.db_session.query(func.count(EmailEngagement.id)).scalar() or 0
            
            # Calculate new items in the last 7 days
            one_week_ago = datetime.now() - timedelta(days=7)
            
            # New organizations - use date_added instead of discovered_at
            new_orgs = self.db_session.query(func.count(Organization.id)).filter(
                Organization.date_added >= one_week_ago
            ).scalar() or 0
            
            # New contacts - use date_added instead of created_at
            new_contacts = self.db_session.query(func.count(Contact.id)).filter(
                Contact.date_added >= one_week_ago
            ).scalar() or 0
            
            # New emails - use email_sent_date
            new_emails = self.db_session.query(func.count(EmailEngagement.id)).filter(
                EmailEngagement.email_sent_date >= one_week_ago
            ).scalar() or 0
            
            # Calculate rates - avoid division by zero
            org_rate = round((new_orgs / total_orgs * 100) if total_orgs > 0 else 0, 1)
            contact_rate = round((new_contacts / total_contacts * 100) if total_contacts > 0 else 0, 1)
            email_rate = round((new_emails / total_emails * 100) if total_emails > 0 else 0, 1)
            
            return [
                html.Div([
                    html.H3(f"{total_orgs:,}", className="metric-value"),
                    html.P("Total Organizations", className="metric-label"),
                    html.P(f"+{new_orgs:,} ({org_rate}%)", className="metric-change")
                ], className="metric-card"),
                html.Div([
                    html.H3(f"{total_contacts:,}", className="metric-value"),
                    html.P("Total Contacts", className="metric-label"),
                    html.P(f"+{new_contacts:,} ({contact_rate}%)", className="metric-change")
                ], className="metric-card"),
                html.Div([
                    html.H3(f"{total_emails:,}", className="metric-value"),
                    html.P("Total Emails Tracked", className="metric-label"),
                    html.P(f"+{new_emails:,} ({email_rate}%)", className="metric-change")
                ], className="metric-card")
            ]
        except Exception as e:
            logger.error(f"Error generating top metrics: {e}")
            # Return empty metrics on error
            return [
                html.Div([
                    html.H3("N/A", className="metric-value"),
                    html.P("Total Organizations", className="metric-label"),
                    html.P("Error loading data", className="metric-change text-danger")
                ], className="metric-card"),
                html.Div([
                    html.H3("N/A", className="metric-value"),
                    html.P("Total Contacts", className="metric-label"),
                    html.P("Error loading data", className="metric-change text-danger")
                ], className="metric-card"),
                html.Div([
                    html.H3("N/A", className="metric-value"),
                    html.P("Total Emails Tracked", className="metric-label"),
                    html.P("Error loading data", className="metric-change text-danger")
                ], className="metric-card")
            ]
    
    def _register_callbacks(self):
        """Register all dashboard callbacks."""
        
        # Fixed the toggle_target_filters callback to handle *args safely
        @self.app.callback(
            [
                dash.Output("target-org-types-container", "style"),
                dash.Output("target-states-container", "style"),
                dash.Output("target-states", "options")
            ],
            [dash.Input("settings-app-mode-selector", "value")]
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
        
        # Add callback for the run application button with improved visual feedback
        @self.app.callback(
            [dash.Output("run-app-status", "children"),
             dash.Output("run-app-btn", "style"),
             dash.Output("run-app-btn", "children")],
            [dash.Input("run-app-btn", "n_clicks")],
            [dash.State("settings-app-mode-selector", "value"),
             dash.State("target-org-types", "value"),
             dash.State("target-states", "value"),
             dash.State("email-limit", "value"),
             dash.State("max-orgs", "value"),
             # Add new email assignment states
             dash.State("engineering-assignment", "value"),
             dash.State("government-assignment", "value"),
             dash.State("municipal-assignment", "value"),
             dash.State("water-assignment", "value"),
             dash.State("utility-assignment", "value"),
             dash.State("transportation-assignment", "value"),
             dash.State("oil_gas-assignment", "value"),
             dash.State("agriculture-assignment", "value")]
        )
        def run_application(n_clicks, app_mode, org_types, states, email_limit, max_orgs,
                          engineering_email, government_email, municipal_email, water_email,
                          utility_email, transportation_email, oil_gas_email, agriculture_email):
            if n_clicks is None:
                # Initial state
                return "", {}, "Run Application"
            
            # Get the selected options
            mode = app_mode or "org_building"
            org_type = org_types or "all"
            state = states or "all"
            email_lim = email_limit or 100
            max_org = max_orgs or 1000
            
            # Save email assignments to a configuration file or database
            try:
                email_assignments = {
                    "engineering": engineering_email or "marc@gbl-data.com",
                    "government": government_email or "tim@gbl-data.com",
                    "municipal": municipal_email or "tim@gbl-data.com",
                    "water": water_email or "marc@gbl-data.com",
                    "utility": utility_email or "marc@gbl-data.com",
                    "transportation": transportation_email or "jared@gbl-data.com",
                    "oil_gas": oil_gas_email or "jared@gbl-data.com",
                    "agriculture": agriculture_email or "tim@gbl-data.com"
                }
                
                # Save to a configuration file
                import json
                import os
                
                # Create directory if it doesn't exist
                os.makedirs('app/config', exist_ok=True)
                
                # Save to config file
                with open('app/config/email_assignments.json', 'w') as f:
                    json.dump(email_assignments, f, indent=2)
                    
                logger.info(f"Email assignments saved: {email_assignments}")
            except Exception as e:
                logger.error(f"Error saving email assignments: {e}")
            
            # Create a prominent status message with styling
            status_message = html.Div([
                html.Div("✅ Application Started", 
                         style={"color": "green", "fontWeight": "bold", "fontSize": "18px", "marginBottom": "10px"}),
                html.Div([
                    html.P(f"Mode: {mode}", style={"marginBottom": "5px"}),
                    html.P(f"Organization Type: {org_type}", style={"marginBottom": "5px"}),
                    html.P(f"State: {state}", style={"marginBottom": "5px"}),
                    html.P(f"Email Limit: {email_lim}", style={"marginBottom": "5px"}),
                    html.P(f"Max Organizations: {max_org}", style={"marginBottom": "5px"}),
                    html.P("Email assignments saved successfully.", style={"marginBottom": "5px", "color": "green"})
                ], style={"backgroundColor": "#f8f9fa", "padding": "10px", "borderRadius": "5px"}),
                html.P(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                       style={"color": "#666", "marginTop": "10px", "fontStyle": "italic"})
            ], style={"margin": "15px 0", "padding": "15px", "border": "1px solid #ddd", "borderRadius": "5px"})
            
            # Change button appearance to show it was clicked
            button_style = {
                "backgroundColor": "#28a745",
                "color": "white",
                "transition": "all 0.3s"
            }
            
            # Update button text
            button_text = "✓ Application Running"
            
            # Log the application run
            logger.info(f"Application started with mode={mode}, org_type={org_type}, state={state}, " +
                        f"email_limit={email_lim}, max_orgs={max_org}")
            
            # Actually run the application process based on the mode
            try:
                if mode == "sending_emails":
                    # Import the email campaign functions
                    from app.email_app import run_email_campaign
                    
                    # Configure campaign
                    campaign_config = {
                        'auto_send': False,  # Default to safe mode
                        'sandbox_mode': True,  # Default to sandbox mode
                        'max_per_salesperson': email_lim,
                        'target_org_types': None if org_type == "all" else [org_type]
                    }
                    
                    # Instead of running in a thread, run directly and capture results
                    # Get a fresh session for the email campaign
                    from app.database.models import get_db_session
                    email_session = get_db_session()
                    
                    try:
                        logger.info(f"Starting email campaign directly with config: {campaign_config}")
                        results = run_email_campaign(campaign_config, email_session)
                        logger.info(f"Email campaign completed with results: {results}")
                        
                        # Update the status message with success info
                        status_message = html.Div([
                            html.Div("✅ Email Campaign Completed Successfully", 
                                    style={"color": "green", "fontWeight": "bold", "fontSize": "18px", "marginBottom": "10px"}),
                            html.Div([
                                html.P(f"Mode: {mode}", style={"marginBottom": "5px"}),
                                html.P(f"Organization Type: {org_type}", style={"marginBottom": "5px"}),
                                html.P(f"State: {state}", style={"marginBottom": "5px"}),
                                html.P(f"Email Limit: {email_lim}", style={"marginBottom": "5px"}),
                                html.P(f"Processed: {results.get('total', 0)} emails", style={"marginBottom": "5px", "fontWeight": "bold"}),
                                html.P(f"Drafted: {results.get('total_draft', 0)} emails", style={"marginBottom": "5px", "fontWeight": "bold"}),
                                html.P(f"Sent: {results.get('total_sent', 0)} emails", style={"marginBottom": "5px", "fontWeight": "bold"}),
                                html.P("Please check Process Summaries tab for details", style={"marginBottom": "5px", "color": "blue"})
                            ], style={"backgroundColor": "#f8f9fa", "padding": "10px", "borderRadius": "5px"}),
                            html.P(f"Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                                style={"color": "#666", "marginTop": "10px", "fontStyle": "italic"})
                        ], style={"margin": "15px 0", "padding": "15px", "border": "1px solid #ddd", "borderRadius": "5px"})
                        
                    except Exception as e:
                        logger.error(f"Error in email campaign: {e}", exc_info=True)
                        
                        # Show error message to user
                        status_message = html.Div([
                            html.Div("❌ Email Campaign Failed", 
                                    style={"color": "red", "fontWeight": "bold", "fontSize": "18px", "marginBottom": "10px"}),
                            html.Div([
                                html.P(f"Mode: {mode}", style={"marginBottom": "5px"}),
                                html.P(f"Organization Type: {org_type}", style={"marginBottom": "5px"}),
                                html.P(f"State: {state}", style={"marginBottom": "5px"}),
                                html.P(f"Email Limit: {email_lim}", style={"marginBottom": "5px"}),
                                html.P(f"Error: {str(e)}", style={"marginBottom": "5px", "color": "red", "fontWeight": "bold"})
                            ], style={"backgroundColor": "#f8f9fa", "padding": "10px", "borderRadius": "5px"}),
                            html.P(f"Failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                                style={"color": "#666", "marginTop": "10px", "fontStyle": "italic"})
                        ], style={"margin": "15px 0", "padding": "15px", "border": "1px solid #ddd", "borderRadius": "5px"})
                    
                    finally:
                        email_session.close()
                        
                    return status_message, button_style, button_text
            except Exception as e:
                logger.error(f"Error starting application process: {e}")
            
            return status_message, button_style, button_text
        
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
                # Create empty placeholders for all outputs
                empty_fig = go.Figure()
                empty_fig.update_layout(title="No data available")
                
                # Return empty values for multiple outputs
                return (
                    [], 
                    empty_fig, 
                    empty_fig, 
                    empty_fig, 
                    empty_fig, 
                    empty_fig, 
                    [{"label": "All States", "value": "all"}]
                )
        
        # Add callbacks for the validation reports tab
        @self.app.callback(
            [
                dash.Output("validation-summary", "children"),
                dash.Output("validation-approval-chart", "figure"),
                dash.Output("validation-scores-chart", "figure"),
                dash.Output("validation-reports-table", "data"),
                dash.Output("validation-reports-table", "tooltip_data")
            ],
            [
                dash.Input("validation-time-range", "value"),
                dash.Input("validation-approval-status", "value")
            ]
        )
        def update_validation_reports(time_range, approval_status):
            try:
                # Import ValidationReport model
                try:
                    from app.models.dashboard_models import ValidationReport, Contact
                    validation_reports_available = True
                except ImportError:
                    validation_reports_available = False
                    
                if not validation_reports_available:
                    # Return empty data if ValidationReport model not available
                    empty_fig = go.Figure()
                    empty_fig.update_layout(title="Validation reports not available")
                    return [], empty_fig, empty_fig, [], []
                
                # Parse time range
                end_date = datetime.now()
                if time_range == "24h":
                    start_date = end_date - timedelta(hours=24)
                elif time_range == "7d":
                    start_date = end_date - timedelta(days=7)
                elif time_range == "30d":
                    start_date = end_date - timedelta(days=30)
                else:  # all
                    start_date = datetime(2000, 1, 1)
                
                # Base query with time range filter
                base_query = self.db_session.query(ValidationReport).filter(
                    ValidationReport.validation_date.between(start_date, end_date)
                )
                
                # Apply approval status filter if not 'all'
                if approval_status != 'all':
                    is_approved = approval_status == 'approved'
                    base_query = base_query.filter(ValidationReport.auto_send_approved == is_approved)
                
                # Get all validation reports
                validation_reports = base_query.order_by(ValidationReport.validation_date.desc()).all()
                
                # If no reports, return empty data
                if not validation_reports:
                    empty_fig = go.Figure()
                    empty_fig.update_layout(title="No validation reports in selected time range")
                    
                    return (
                        self._get_empty_validation_summary(),
                        empty_fig,
                        empty_fig,
                        [],
                        []
                    )
                
                # Get summary metrics
                summary = self._get_validation_summary(validation_reports)
                
                # Create approval chart
                approval_chart = self._get_validation_approval_chart(validation_reports)
                
                # Create scores chart
                scores_chart = self._get_validation_scores_chart(validation_reports)
                
                # Create table data
                table_data, tooltip_data = self._get_validation_table_data(validation_reports)
                
                return (
                    summary,
                    approval_chart,
                    scores_chart,
                    table_data,
                    tooltip_data
                )
                
            except Exception as e:
                logger.error(f"Error updating validation reports: {e}")
                # Return empty data on error
                empty_fig = go.Figure()
                empty_fig.update_layout(title="Error loading validation reports")
                
                return (
                    [],
                    empty_fig,
                    empty_fig,
                    [],
                    []
                )
        
        # Callback for validation details
        @self.app.callback(
            dash.Output("validation-details", "children"),
            [dash.Input("validation-reports-table", "active_cell")],
            [dash.State("validation-reports-table", "data")]
        )
        def update_validation_details(active_cell, table_data):
            """Show details for the selected validation report."""
            try:
                if not active_cell or not table_data:
                    return html.P("Select a row in the table to view details")
                
                # Get the selected row
                row_idx = active_cell["row"]
                row_data = table_data[row_idx]
                
                # Get the validation report ID
                report_id = row_data.get("id")
                if not report_id:
                    return html.P("No details available for this selection")
                
                # Import ValidationReport model
                try:
                    from app.models.dashboard_models import ValidationReport, Contact
                    import json
                except ImportError:
                    return html.P("Validation reports model not available")
                
                # Get the validation report
                report = self.db_session.query(ValidationReport).filter(ValidationReport.id == report_id).first()
                if not report:
                    return html.P(f"Validation report not found: ID {report_id}")
                
                # Parse reasons
                try:
                    reasons = json.loads(report.reasons)
                except:
                    reasons = ["No detailed reasons available"]
                
                # Create details component
                details = html.Div([
                    html.H4(f"Validation Report #{report.id}"),
                    
                    # Status and scores
                    html.Div([
                        html.Div([
                            html.H5("Status"),
                            html.P("Approved", className="status-approved") if report.auto_send_approved 
                                  else html.P("Rejected", className="status-rejected")
                        ], className="detail-item"),
                        
                        html.Div([
                            html.H5("Contact Score"),
                            html.P(f"{report.contact_score:.2f}" if report.contact_score else "N/A")
                        ], className="detail-item"),
                        
                        html.Div([
                            html.H5("Contact Threshold"),
                            html.P(f"{report.contact_threshold:.2f}" if report.contact_threshold else "N/A")
                        ], className="detail-item"),
                        
                        html.Div([
                            html.H5("Organization Score"),
                            html.P(f"{report.org_score:.2f}" if report.org_score else "N/A")
                        ], className="detail-item"),
                        
                        html.Div([
                            html.H5("Organization Threshold"),
                            html.P(f"{report.org_threshold:.2f}" if report.org_threshold else "N/A")
                        ], className="detail-item"),
                    ], className="details-row"),
                    
                    # Validation reasons
                    html.Div([
                        html.H5("Validation Reasons"),
                        html.Ul([html.Li(reason) for reason in reasons])
                    ], className="reasons-container"),
                    
                    # Technical details
                    html.Div([
                        html.H5("Technical Details"),
                        html.P(f"Model: {report.model_used}"),
                        html.P(f"Validation Type: {report.validation_type}"),
                        html.P(f"Date: {report.validation_date}")
                    ], className="tech-details")
                ], className="validation-detail-card")
                
                return details
                
            except Exception as e:
                logger.error(f"Error showing validation details: {e}")
                return html.P(f"Error loading details: {str(e)}")
                
    def _get_empty_validation_summary(self):
        """Return empty validation summary when no data is available."""
        return [
            html.Div([
                html.H3("0", className="metric-value"),
                html.P("Total Validations", className="metric-label")
            ], className="metric-card"),
            html.Div([
                html.H3("0", className="metric-value"),
                html.P("Approved", className="metric-label")
            ], className="metric-card"),
            html.Div([
                html.H3("0", className="metric-value"),
                html.P("Rejected", className="metric-label")
            ], className="metric-card")
        ]
    
    def _get_validation_summary(self, validation_reports):
        """Generate summary metrics for validation reports."""
        try:
            # Count total reports
            total_reports = len(validation_reports)
            
            # Count approved and rejected
            approved = sum(1 for r in validation_reports if r.auto_send_approved)
            rejected = total_reports - approved
            
            # Calculate approval rate
            approval_rate = round((approved / total_reports * 100) if total_reports > 0 else 0, 1)
            
            return [
                html.Div([
                    html.H3(f"{total_reports:,}", className="metric-value"),
                    html.P("Total Validations", className="metric-label")
                ], className="metric-card"),
                html.Div([
                    html.H3(f"{approved:,}", className="metric-value"),
                    html.P("Approved", className="metric-label"),
                    html.P(f"{approval_rate}%", className="metric-change success-text")
                ], className="metric-card"),
                html.Div([
                    html.H3(f"{rejected:,}", className="metric-value"),
                    html.P("Rejected", className="metric-label"),
                    html.P(f"{100 - approval_rate}%", className="metric-change")
                ], className="metric-card")
            ]
        except Exception as e:
            logger.error(f"Error generating validation summary: {e}")
            return self._get_empty_validation_summary()
    
    def _get_validation_approval_chart(self, validation_reports):
        """Create a pie chart showing approved vs rejected validations."""
        try:
            # Count approved and rejected
            approved = sum(1 for r in validation_reports if r.auto_send_approved)
            rejected = len(validation_reports) - approved
            
            # Create data
            labels = ['Approved', 'Rejected']
            values = [approved, rejected]
            colors = [COLORS['green'], COLORS['red']]
            
            # Create pie chart
            fig = go.Figure(data=[go.Pie(
                labels=labels,
                values=values,
                hole=.4,
                marker_colors=colors
            )])
            
            # Customize layout
            fig.update_layout(
                title="Validation Approval Rate",
                template="plotly_white"
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creating validation approval chart: {e}")
            empty_fig = go.Figure()
            empty_fig.update_layout(title="Error creating approval chart")
            return empty_fig
    
    def _get_validation_scores_chart(self, validation_reports):
        """Create a scatter plot of contact scores vs org scores."""
        try:
            # Extract scores
            contact_scores = [r.contact_score for r in validation_reports if r.contact_score is not None]
            org_scores = [r.org_score for r in validation_reports if r.org_score is not None]
            approved = [r.auto_send_approved for r in validation_reports]
            
            # If not enough data, return empty chart
            if len(contact_scores) < 2 or len(org_scores) < 2:
                empty_fig = go.Figure()
                empty_fig.update_layout(title="Not enough data for score comparison")
                return empty_fig
            
            # Create dataframe for plotting
            data = {
                'contact_score': contact_scores[:len(org_scores)],  # Ensure equal lengths
                'org_score': org_scores[:len(contact_scores)],
                'approved': approved[:min(len(contact_scores), len(org_scores))]
            }
            df = pd.DataFrame(data)
            
            # Create scatter plot
            fig = px.scatter(
                df, 
                x='contact_score', 
                y='org_score',
                color='approved',
                color_discrete_map={True: COLORS['green'], False: COLORS['red']},
                labels={
                    'contact_score': 'Contact Confidence Score',
                    'org_score': 'Organization Confidence Score',
                    'approved': 'Approved'
                },
                title="Validation Scores Comparison"
            )
            
            # Customize layout
            fig.update_layout(
                xaxis_title="Contact Score",
                yaxis_title="Organization Score",
                template="plotly_white"
            )
            
            # Add diagonal line representing equal scores
            fig.add_shape(
                type="line",
                x0=0, y0=0,
                x1=1, y1=1,
                line=dict(color="gray", dash="dash")
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creating validation scores chart: {e}")
            empty_fig = go.Figure()
            empty_fig.update_layout(title="Error creating scores chart")
            return empty_fig
    
    def _get_validation_table_data(self, validation_reports):
        """Create table data for validation reports."""
        try:
            table_data = []
            tooltip_data = []
            
            from app.models.dashboard_models import Contact
            import json
            
            for report in validation_reports:
                # Get contact information
                contact = self.db_session.query(Contact).filter(Contact.id == report.contact_id).first()
                if not contact:
                    # Skip if contact not found
                    continue
                
                # Get organization name
                org_name = contact.organization.name if contact.organization else "Unknown"
                
                # Parse reasons
                try:
                    reasons = json.loads(report.reasons)
                    reasons_text = "\n".join(reasons)
                except:
                    reasons_text = "No detailed reasons available"
                
                # Create table row
                row = {
                    'id': report.id,
                    'contact_id': report.contact_id,
                    'contact_name': f"{contact.first_name or ''} {contact.last_name or ''}".strip() or "Unknown",
                    'email': contact.email or "Unknown",
                    'organization': org_name,
                    'validation_date': report.validation_date.strftime("%Y-%m-%d %H:%M") if report.validation_date else "Unknown",
                    'status': "Approved" if report.auto_send_approved else "Rejected",
                    'contact_score': f"{report.contact_score:.2f}" if report.contact_score is not None else "N/A",
                    'contact_threshold': f"{report.contact_threshold:.2f}" if report.contact_threshold is not None else "N/A",
                    'org_score': f"{report.org_score:.2f}" if report.org_score is not None else "N/A",
                    'org_threshold': f"{report.org_threshold:.2f}" if report.org_threshold is not None else "N/A"
                }
                
                table_data.append(row)
                
                # Create tooltip for row
                tooltip = {
                    column: {'value': reasons_text, 'type': 'markdown'} if column == 'status' else {'value': '', 'type': 'text'}
                    for column in row.keys()
                }
                
                tooltip_data.append(tooltip)
            
            return table_data, tooltip_data
            
        except Exception as e:
            logger.error(f"Error creating validation table data: {e}")
            return [], []
    
    def run_server(self, debug=False, port=None):
        """Run the dashboard server."""
        try:
            # Find an available port if not specified
            if port is None:
                default_port = 8050
                try:
                    # Try the default port first
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.bind(("127.0.0.1", default_port))
                    s.close()
                    port = default_port
                except socket.error:
                    # If the default port is in use, try the next port
                    alt_port = 8051
                    logger.warning(f"Port {default_port} is in use, trying port {alt_port}")
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.bind(("127.0.0.1", alt_port))
                        s.close()
                        port = alt_port
                    except socket.error:
                        # If both ports are in use, use a random port
                        port = 0
                        logger.warning(f"Port {alt_port} is also in use, using a random port")
            
            # If port is specified or we found an available port
            if port != 0:
                logger.info(f"Starting dashboard on port {port}")
                self.app.run(debug=debug, port=port)
            else:
                # Let the OS choose a port
                logger.info("Starting dashboard on a random port")
                self.app.run(debug=debug)
        except Exception as e:
            logger.error(f"Error running dashboard server: {e}")
            raise
    
    def run(self, debug=False, port=None):
        """Alias for run_server for backwards compatibility."""
        self.run_server(debug=debug, port=port) 