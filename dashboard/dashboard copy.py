"""
Dashboard for monitoring the Contact Discovery System.
"""
import dash
from dash import dcc, html, dash_table, callback_context
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import subprocess
import sys
from datetime import datetime, timedelta
from sqlalchemy import func, and_, text
from sqlalchemy.orm import Session
from app.database.models import (
    Organization, Contact, SystemMetric, SearchQuery, 
    ContactInteraction, DiscoveredURL, OrganizationType
)
from app.config import DASHBOARD_PORT, EMAIL_DAILY_LIMIT_PER_USER, EMAIL_USERS
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Modern color scheme
COLORS = {
    'primary': '#2c3e50',
    'secondary': '#3498db',
    'accent': '#2ecc71',
    'background': '#f8f9fa',
    'text': '#2c3e50',
    'light': '#ecf0f1',
    'dark': '#34495e',
    'highlight': '#e74c3c',
    'success': '#27ae60',
    'info': '#3498db',
    'warning': '#f39c12',
    'danger': '#e74c3c'
}

# Email options
EMAIL_OPTIONS = [
    {'label': 'Tim - tim@gbl-data.com', 'value': 'tim@gbl-data.com'},
    {'label': 'Marc - marc@gbl-data.com', 'value': 'marc@gbl-data.com'},
    {'label': 'Jared - jared@gbl-data.com', 'value': 'jared@gbl-data.com'}
]

# App running modes
APP_MODES = [
    {'label': '1. Organization List Building', 'value': 'org_building'},
    {'label': '2. Contact Building', 'value': 'contact_building'},
    {'label': '3. Sending Emails', 'value': 'sending_emails'}
]

# Organization types for assignment
ORG_TYPES = [
    {'label': 'Engineering Firms', 'value': 'engineering'},
    {'label': 'Government Agencies', 'value': 'government'},
    {'label': 'Municipalities', 'value': 'municipal'},
    {'label': 'Water & Wastewater', 'value': 'water'},
    {'label': 'Utility Companies', 'value': 'utility'},
    {'label': 'Transportation Auth.', 'value': 'transportation'},
    {'label': 'Oil, Gas & Mining', 'value': 'oil_gas'},
    {'label': 'Agriculture & Irrigation', 'value': 'agriculture'}
]

class Dashboard:
    """Dashboard for monitoring system metrics and discoveries."""
    
    def __init__(self, db_session: Session, dashboard_type="discovery"):
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
            title = "GDS Contact Discovery Dashboard"
            
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
        
        # Add custom CSS
        self.app.index_string = '''
        <!DOCTYPE html>
        <html>
            <head>
                {%metas%}
                <title>{%title%}</title>
                {%favicon%}
                {%css%}
                <style>
                    body {
                        font-family: 'Poppins', sans-serif;
                        margin: 0;
                        padding: 0;
                        background-color: ''' + COLORS['background'] + ''';
                        color: ''' + COLORS['text'] + ''';
                    }
                    .dash-container {
                        max-width: 1800px;
                        margin: 0 auto;
                        padding: 20px;
                    }
                    .header {
                        background-color: ''' + COLORS['primary'] + ''';
                        color: white;
                        padding: 20px;
                        border-radius: 10px;
                        margin-bottom: 20px;
                        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                    }
                    h1 {
                        font-size: 28px;
                        font-weight: 600;
                        margin: 0;
                    }
                    h2 {
                        font-size: 22px;
                        font-weight: 500;
                        margin-top: 20px;
                        margin-bottom: 15px;
                        color: ''' + COLORS['dark'] + ''';
                        border-bottom: 2px solid ''' + COLORS['light'] + ''';
                        padding-bottom: 8px;
                    }
                    h3 {
                        font-size: 18px;
                        font-weight: 500;
                        margin-top: 15px;
                        margin-bottom: 10px;
                        color: ''' + COLORS['dark'] + ''';
                    }
                    .card {
                        background-color: white;
                        border-radius: 10px;
                        padding: 20px;
                        margin-bottom: 20px;
                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                    }
                    .metrics-container {
                        display: flex;
                        flex-wrap: wrap;
                        gap: 20px;
                        margin-bottom: 20px;
                    }
                    .metric-box {
                        background-color: white;
                        border-radius: 10px;
                        padding: 20px;
                        text-align: center;
                        flex: 1;
                        min-width: 200px;
                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                        transition: transform 0.3s ease;
                    }
                    .metric-box:hover {
                        transform: translateY(-5px);
                    }
                    .metric-box h3 {
                        font-size: 28px;
                        font-weight: 600;
                        margin: 0;
                        color: ''' + COLORS['secondary'] + ''';
                    }
                    .metric-box p {
                        margin: 10px 0 0;
                        font-size: 14px;
                        color: ''' + COLORS['dark'] + ''';
                    }
                    .charts-row {
                        display: flex;
                        flex-wrap: wrap;
                        gap: 20px;
                        margin-bottom: 20px;
                    }
                    .chart-container {
                        background-color: white;
                        border-radius: 10px;
                        padding: 20px;
                        flex: 1;
                        min-width: 45%;
                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                    }
                    .footer {
                        margin-top: 40px;
                        text-align: center;
                        color: ''' + COLORS['dark'] + ''';
                        font-size: 14px;
                    }
                    .dash-table-container {
                        border-radius: 10px;
                        overflow: hidden;
                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                    }
                    .control-panel {
                        background-color: white;
                        border-radius: 10px;
                        padding: 20px;
                        margin-bottom: 20px;
                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                    }
                    .control-section {
                        margin-bottom: 15px;
                    }
                    .control-label {
                        font-weight: 500;
                        margin-bottom: 8px;
                        display: block;
                    }
                    .btn {
                        background-color: ''' + COLORS['secondary'] + ''';
                        color: white;
                        border: none;
                        padding: 10px 20px;
                        border-radius: 5px;
                        cursor: pointer;
                        font-weight: 500;
                        transition: background-color 0.3s ease;
                    }
                    .btn:hover {
                        background-color: ''' + COLORS['primary'] + ''';
                    }
                    .btn-success {
                        background-color: ''' + COLORS['success'] + ''';
                    }
                    .btn-success:hover {
                        background-color: ''' + COLORS['accent'] + ''';
                    }
                    .btn-block {
                        display: block;
                        width: 100%;
                    }
                    .status-message {
                        padding: 10px;
                        border-radius: 5px;
                        margin-top: 10px;
                    }
                    .status-success {
                        background-color: rgba(46, 204, 113, 0.2);
                        color: ''' + COLORS['success'] + ''';
                    }
                    .status-error {
                        background-color: rgba(231, 76, 60, 0.2);
                        color: ''' + COLORS['danger'] + ''';
                    }
                    .status-info {
                        background-color: rgba(52, 152, 219, 0.2);
                        color: ''' + COLORS['info'] + ''';
                    }
                </style>
            </head>
            <body>
                <div class="dash-container">
                    {%app_entry%}
                </div>
                {%config%}
                {%scripts%}
                {%renderer%}
            </body>
        </html>
        '''
        
        self.setup_layout()
        self.setup_callbacks()
    
    def setup_layout(self):
        """Set up the dashboard layout."""
        # Set heading based on dashboard type
        if self.dashboard_type == "email":
            heading = "GDS Email Campaign Dashboard"
        else:
            heading = "GDS Contact Discovery Dashboard"
            
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1(heading),
                html.P("Manage contact discovery and email campaigns with the controls below.", 
                       style={"color": "white", "marginTop": "10px"})
            ], className="header"),
            
            # Control Panel
            html.Div([
                html.H2("Control Panel"),
                
                # Left section (Email Assignment)
                html.Div([
                    html.Div([
                        html.H3("Email Assignment"),
                        html.P("Assign organization types to specific team members for email campaigns:"),
                        
                        # Organization type assignments
                        *[html.Div([
                            html.Label(org_type['label'], className="control-label"),
                            dcc.Dropdown(
                                id=f"assign-{org_type['value']}",
                                options=EMAIL_OPTIONS,
                                value=self._get_current_assignment(org_type['value']),
                                clearable=False
                            )
                        ], className="control-section") for org_type in ORG_TYPES],
                        
                        # Save assignments button
                        html.Button(
                            "Save Assignments", 
                            id="save-assignments-btn", 
                            className="btn",
                            style={"marginTop": "15px"}
                        ),
                        
                        # Status message for assignments
                        html.Div(id="assignment-status", className="status-message")
                    ], style={"flex": "1", "minWidth": "300px"}),
                    
                    # Right section (Application control)
                    html.Div([
                        html.H3("Application Control"),
                        
                        # App mode selection
                        html.Div([
                            html.Label("Operating Mode:", className="control-label"),
                            dcc.Dropdown(
                                id="app-mode",
                                options=APP_MODES,
                                value="org_building",
                                clearable=False
                            )
                        ], className="control-section"),
                        
                        # Target organization types (new feature)
                        html.Div([
                            html.Label("Target Organization Types:", className="control-label"),
                            dcc.Dropdown(
                                id="target-org-types",
                                options=[{"label": "All Types", "value": "all"}] + ORG_TYPES,
                                value="all",
                                multi=True,
                                placeholder="Select organization types to target (or 'All Types')"
                            )
                        ], className="control-section", id="target-org-types-container"),
                        
                        # Email limit per day
                        html.Div([
                            html.Label("Email Drafts Per Day (Per Person):", className="control-label"),
                            dcc.Slider(
                                id="email-limit",
                                min=5,
                                max=50,
                                step=5,
                                value=EMAIL_DAILY_LIMIT_PER_USER,
                                marks={i: str(i) for i in range(5, 51, 5)},
                            )
                        ], className="control-section"),
                        
                        # Max orgs to process
                        html.Div([
                            html.Label("Max Organizations to Process:", className="control-label"),
                            dcc.Slider(
                                id="max-orgs",
                                min=10,
                                max=100,
                                step=10,
                                value=50,
                                marks={i: str(i) for i in range(10, 101, 10)},
                            )
                        ], className="control-section"),
                        
                        # Run application button
                        html.Button(
                            "Run Application", 
                            id="run-app-btn", 
                            className="btn btn-success btn-block",
                            style={"marginTop": "20px"}
                        ),
                        
                        # Status message for application run
                        html.Div(id="run-status", className="status-message")
                    ], style={"flex": "1", "minWidth": "300px"})
                ], style={"display": "flex", "flexWrap": "wrap", "gap": "30px"})
            ], className="control-panel card"),
            
            # Overview metrics
            html.Div([
                html.H2("System Overview"),
                html.Div(id="overview-metrics", className="metrics-container")
            ], className="card"),
            
            # Time range selector
            html.Div([
                html.Label("Time Range:", className="control-label"),
                dcc.Dropdown(
                    id="time-range-dropdown",
                    options=[
                        {"label": "Last 24 Hours", "value": "24h"},
                        {"label": "Last 7 Days", "value": "7d"},
                        {"label": "Last 30 Days", "value": "30d"},
                        {"label": "All Time", "value": "all"}
                    ],
                    value="7d",
                    clearable=False,
                    style={"width": "100%"}
                )
            ], className="card", style={"marginBottom": "20px"}),
            
            # Charts
            html.Div([
                html.Div([
                    html.H3("Discoveries Over Time"),
                    dcc.Graph(id="discoveries-chart")
                ], className="chart-container"),
                
                html.Div([
                    html.H3("Organizations by Type"),
                    dcc.Graph(id="org-types-chart")
                ], className="chart-container")
            ], className="charts-row"),
            
            html.Div([
                html.Div([
                    html.H3("Organizations by State"),
                    dcc.Graph(id="org-states-chart")
                ], className="chart-container"),
                
                html.Div([
                    html.H3("Email Drafts by Sales Person"),
                    dcc.Graph(id="emails-chart")
                ], className="chart-container")
            ], className="charts-row"),
            
            # System performance
            html.Div([
                html.H2("System Performance"),
                dcc.Graph(id="performance-chart")
            ], className="card"),
            
            # Organizations and Contacts Table
            html.Div([
                html.H2("Organizations and Contacts"),
                
                # Filters for the table
                html.Div([
                    html.Div([
                        html.Label("Filter by Organization Type:", className="control-label"),
                        dcc.Dropdown(
                            id="filter-org-type",
                            options=[{"label": "All Types", "value": "all"}] + ORG_TYPES,
                            value="all",
                            clearable=False
                        )
                    ], style={"width": "30%", "display": "inline-block", "marginRight": "20px"}),
                    
                    html.Div([
                        html.Label("Filter by State:", className="control-label"),
                        dcc.Dropdown(
                            id="filter-state",
                            options=[{"label": "All States", "value": "all"}],  # Will be populated in callback
                            value="all",
                            clearable=False
                        )
                    ], style={"width": "30%", "display": "inline-block", "marginRight": "20px"}),
                    
                    html.Div([
                        html.Label("Min Relevance Score:", className="control-label"),
                        dcc.Slider(
                            id="min-relevance",
                            min=1,
                            max=10,
                            step=1,
                            value=1,
                            marks={i: str(i) for i in range(1, 11)},
                        )
                    ], style={"width": "30%", "display": "inline-block"})
                ], style={"marginBottom": "20px"}),
                
                # Hidden div for initial data loading
                html.Div(id="data-loading-trigger", children="load", style={"display": "none"}),
                
                # Organizations table
                html.Div([
                    html.Div([
                        html.H3("Organizations", style={"display": "inline-block", "marginRight": "20px"}),
                        html.Button(
                            "Export CSV", 
                            id="export-orgs-btn", 
                            className="btn", 
                            style={"marginBottom": "10px", "display": "inline-block"}
                        ),
                    ]),
                    dash_table.DataTable(
                        id="organizations-table",
                        columns=[
                            {"name": "Name", "id": "name", "sortable": True},
                            {"name": "Type", "id": "org_type", "sortable": True},
                            {"name": "State", "id": "state", "sortable": True},
                            {"name": "Relevance", "id": "relevance_score", "sortable": True},
                            {"name": "Contacts", "id": "contacts_count", "sortable": True},
                            {"name": "Discovered", "id": "date_added", "sortable": True},
                            {"name": "Website", "id": "website", "sortable": True}
                        ],
                        row_selectable="single",
                        selected_rows=[],
                        page_size=100,
                        page_action="native",
                        sort_action="native",
                        sort_mode="multi",
                        export_format="csv",
                        export_headers="display",
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "textAlign": "left",
                            "padding": "10px",
                            "whiteSpace": "normal",
                            "height": "auto",
                            "fontSize": "14px"
                        },
                        style_header={
                            "backgroundColor": COLORS['light'],
                            "fontWeight": "bold",
                            "textAlign": "left",
                            "padding": "12px",
                            "fontSize": "14px"
                        },
                        style_data_conditional=[
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "rgba(0, 0, 0, 0.02)"
                            },
                            {
                                "if": {"column_id": "relevance_score", "filter_query": "{relevance_score} >= 8"},
                                "backgroundColor": "rgba(46, 204, 113, 0.2)",
                                "color": COLORS['success']
                            }
                        ]
                    )
                ], style={"marginBottom": "30px"}),
                
                # Contacts table for selected organization
                html.Div([
                    html.Div([
                        html.H3("Contacts", style={"display": "inline-block", "marginRight": "20px"}),
                        html.Button(
                            "Export CSV", 
                            id="export-contacts-btn", 
                            className="btn", 
                            style={"marginBottom": "10px", "display": "inline-block"}
                        ),
                    ]),
                    html.Div(id="contacts-header"),
                    dash_table.DataTable(
                        id="contacts-table",
                        columns=[
                            {"name": "Name", "id": "name", "sortable": True},
                            {"name": "Title", "id": "job_title", "sortable": True},
                            {"name": "Organization", "id": "organization", "sortable": True},
                            {"name": "Email", "id": "email", "sortable": True},
                            {"name": "Phone", "id": "phone", "sortable": True},
                            {"name": "Contact Type", "id": "contact_type", "sortable": True},
                            {"name": "Relevance", "id": "relevance_score", "sortable": True},
                            {"name": "Assigned To", "id": "assigned_to", "sortable": True},
                            {"name": "Status", "id": "status", "sortable": True}
                        ],
                        page_size=100,
                        page_action="native",
                        sort_action="native",
                        sort_mode="multi",
                        export_format="csv",
                        export_headers="display",
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "textAlign": "left",
                            "padding": "10px",
                            "whiteSpace": "normal",
                            "height": "auto",
                            "fontSize": "14px"
                        },
                        style_header={
                            "backgroundColor": COLORS['light'],
                            "fontWeight": "bold",
                            "textAlign": "left",
                            "padding": "12px",
                            "fontSize": "14px"
                        },
                        style_data_conditional=[
                            {
                                "if": {"row_index": "odd"},
                                "backgroundColor": "rgba(0, 0, 0, 0.02)"
                            },
                            {
                                "if": {"column_id": "relevance_score", "filter_query": "{relevance_score} >= 7"},
                                "backgroundColor": "rgba(46, 204, 113, 0.2)",
                                "color": COLORS['success']
                            },
                            {
                                "if": {"column_id": "status", "filter_query": "{status} = 'email_draft'"},
                                "backgroundColor": "rgba(52, 152, 219, 0.2)",
                                "color": COLORS['info']
                            },
                            {
                                "if": {"column_id": "status", "filter_query": "{status} = 'emailed'"},
                                "backgroundColor": "rgba(46, 204, 113, 0.2)",
                                "color": COLORS['success']
                            },
                            {
                                "if": {"column_id": "contact_type", "filter_query": "{contact_type} = 'Actual'"},
                                "backgroundColor": "rgba(46, 204, 113, 0.2)",
                                "color": COLORS['success']
                            },
                            {
                                "if": {"column_id": "contact_type", "filter_query": "{contact_type} = 'Generated'"},
                                "backgroundColor": "rgba(243, 156, 18, 0.2)",
                                "color": COLORS['warning']
                            },
                            {
                                "if": {"column_id": "contact_type", "filter_query": "{contact_type} = 'Inferred'"},
                                "backgroundColor": "rgba(52, 152, 219, 0.2)",
                                "color": COLORS['info']
                            }
                        ]
                    )
                ])
            ], className="card"),
            
            # Footer
            html.Div([
                html.Hr(),
                html.P(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            ], className="footer")
        ])
    
    def setup_callbacks(self):
        """Set up the dashboard callbacks."""
        # Show/hide target org types based on app mode
        @self.app.callback(
            dash.Output("target-org-types-container", "style"),
            [dash.Input("app-mode", "value")]
        )
        def toggle_target_org_types(app_mode):
            # Only show for org building mode
            if app_mode == "org_building":
                return {"display": "block"}
            return {"display": "none"}
            
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
            
        @self.app.callback(
            [
                dash.Output("organizations-table", "data")
            ],
            [
                dash.Input("filter-org-type", "value"), 
                dash.Input("filter-state", "value"),
                dash.Input("min-relevance", "value"),
                dash.Input("time-range-dropdown", "value")  # Add time range as a trigger for initial load
            ]
        )
        def update_organizations_table(org_type_filter, state_filter, min_relevance, time_range):
            # Base query
            query = self.db_session.query(
                Organization,
                func.count(Contact.id).label("contacts_count")
            ).outerjoin(
                Contact, Contact.organization_id == Organization.id
            ).group_by(
                Organization.id
            )
            
            # Apply filters
            if org_type_filter != "all":
                query = query.filter(Organization.org_type == org_type_filter)
                
            if state_filter != "all":
                query = query.filter(Organization.state == state_filter)
                
            # Apply minimum relevance score filter
            if min_relevance > 1:  # Only apply if not the minimum
                query = query.filter(Organization.relevance_score >= min_relevance / 10.0)
                
            # Order by relevance score (descending) and then name
            query = query.order_by(Organization.relevance_score.desc(), Organization.name)
            
            # Execute query
            results = query.all()
            
            # Format for datatable
            data = []
            for org, contacts_count in results:
                # Format relevance score as 1-10 scale instead of 0-1
                relevance_score_display = int(org.relevance_score * 10) if org.relevance_score else 0
                
                data.append({
                    "id": org.id,
                    "name": org.name,
                    "org_type": self._get_org_type_display(org.org_type),
                    "state": org.state or "Unknown",
                    "relevance_score": relevance_score_display,
                    "contacts_count": contacts_count,
                    "date_added": org.date_added.strftime("%Y-%m-%d"),
                    "website": org.website or "N/A"
                })
            
            logger.info(f"Loaded {len(data)} organizations for data table")
            return [data]
            
        @self.app.callback(
            [
                dash.Output("contacts-table", "data"),
                dash.Output("contacts-header", "children")
            ],
            [
                dash.Input("organizations-table", "selected_rows"),
                dash.Input("organizations-table", "data"),
                dash.Input("time-range-dropdown", "value")  # Add time range to trigger initial load
            ]
        )
        def update_contacts_table(selected_rows, organizations_data, time_range):
            try:
                # Add debug logging
                logger.info(f"Contact table update triggered - selected_rows: {selected_rows}, organizations data length: {len(organizations_data) if organizations_data else 0}")
                
                if not selected_rows or not organizations_data:
                    # Show all contacts instead of an empty table
                    high_relevance_contacts = self.db_session.query(Contact).order_by(
                        Contact.contact_relevance_score.desc(),
                        Contact.date_added.desc()
                    ).all()
                    
                    if not high_relevance_contacts:
                        return [], html.P("Select an organization to view contacts, or add contacts to see them here", 
                                        style={"fontStyle": "italic"})
                    
                    # Format for datatable - show all contacts
                    data = []
                    for contact in high_relevance_contacts:
                        # Get organization name
                        org_name = "Unknown"
                        try:
                            if contact.organization_id:
                                org = self.db_session.query(Organization).filter(
                                    Organization.id == contact.organization_id
                                ).first()
                                if org:
                                    org_name = org.name
                        except Exception as e:
                            logger.error(f"Error getting organization for contact {contact.id}: {e}")
                        
                        # Format contact data
                        full_name = ""
                        if contact.first_name and contact.last_name:
                            full_name = f"{contact.first_name} {contact.last_name}"
                        elif contact.first_name:
                            full_name = contact.first_name
                        elif contact.last_name:
                            full_name = contact.last_name
                        else:
                            full_name = "Unknown"
                        
                        data.append({
                            "id": contact.id,
                            "name": full_name,
                            "job_title": contact.job_title or "Unknown",
                            "organization": org_name,
                            "email": contact.email or "N/A",
                            "phone": contact.phone or "N/A",
                            "contact_type": self._get_contact_type_display(contact.contact_type),
                            "relevance_score": int(contact.contact_relevance_score) if contact.contact_relevance_score else 0,
                            "assigned_to": contact.assigned_to or "Not assigned",
                            "status": self._get_status_display(contact.status)
                        })
                    
                    return data, html.P(f"All Contacts ({len(data)}) - Select an organization to view specific contacts",
                                    style={"fontStyle": "italic", "color": COLORS["info"]})
                
                # Get selected organization
                try:
                    selected_org_id = organizations_data[selected_rows[0]]["id"]
                    selected_org_name = organizations_data[selected_rows[0]]["name"]
                    selected_org_website = organizations_data[selected_rows[0]].get("website", "N/A")
                    logger.info(f"Selected organization: {selected_org_name} (ID: {selected_org_id})")
                except (IndexError, KeyError) as e:
                    logger.error(f"Error accessing selected organization data: {e}")
                    # Show all contacts instead
                    high_relevance_contacts = self.db_session.query(Contact).order_by(
                        Contact.contact_relevance_score.desc(),
                        Contact.date_added.desc()
                    ).all()
                    
                    data = []
                    for contact in high_relevance_contacts:
                        # Get organization name
                        org_name = "Unknown"
                        try:
                            if contact.organization_id:
                                org = self.db_session.query(Organization).filter(
                                    Organization.id == contact.organization_id
                                ).first()
                                if org:
                                    org_name = org.name
                        except Exception as e:
                            logger.error(f"Error getting organization for contact {contact.id}: {e}")
                        
                        # Format contact data for table
                        full_name = ""
                        if contact.first_name and contact.last_name:
                            full_name = f"{contact.first_name} {contact.last_name}"
                        elif contact.first_name:
                            full_name = contact.first_name
                        elif contact.last_name:
                            full_name = contact.last_name
                        else:
                            full_name = "Unknown"
                        
                        data.append({
                            "id": contact.id,
                            "name": full_name,
                            "job_title": contact.job_title or "Unknown",
                            "organization": org_name,
                            "email": contact.email or "N/A",
                            "phone": contact.phone or "N/A",
                            "contact_type": self._get_contact_type_display(contact.contact_type),
                            "relevance_score": int(contact.contact_relevance_score) if contact.contact_relevance_score else 0,
                            "assigned_to": contact.assigned_to or "Not assigned",
                            "status": self._get_status_display(contact.status)
                        })
                    
                    error_msg = f"Error with selection: {str(e)}. Showing all contacts instead."
                    return data, html.P(error_msg, style={"color": COLORS["danger"]})
                
                # Get the complete organization record for detailed view
                org_detail = self.db_session.query(Organization).filter(
                    Organization.id == selected_org_id
                ).first()
                
                # Query contacts for this organization
                contacts = self.db_session.query(Contact).filter(
                    Contact.organization_id == selected_org_id
                ).order_by(
                    Contact.contact_relevance_score.desc(),
                    Contact.job_title
                ).all()
                
                # Format for datatable
                data = []
                for contact in contacts:
                    # Format contact data
                    full_name = ""
                    if contact.first_name and contact.last_name:
                        full_name = f"{contact.first_name} {contact.last_name}"
                    elif contact.first_name:
                        full_name = contact.first_name
                    elif contact.last_name:
                        full_name = contact.last_name
                    else:
                        full_name = "Unknown"
                    
                    data.append({
                        "id": contact.id,
                        "name": full_name,
                        "job_title": contact.job_title or "Unknown",
                        "organization": selected_org_name,  # Use the selected organization name
                        "email": contact.email or "N/A",
                        "phone": contact.phone or "N/A",
                        "contact_type": self._get_contact_type_display(contact.contact_type),
                        "relevance_score": int(contact.contact_relevance_score) if contact.contact_relevance_score else 0,
                        "assigned_to": contact.assigned_to or "Not assigned",
                        "status": self._get_status_display(contact.status)
                    })
                
                logger.info(f"Loaded {len(data)} contacts for organization {selected_org_name}")
                
                # Create detailed organization view
                org_detail_view = html.Div([
                    html.H4(f"Organization: {selected_org_name}", style={"marginBottom": "10px"}),
                    html.Div([
                        html.P([
                            html.Strong("Website: "), 
                            html.A(selected_org_website, href=selected_org_website if selected_org_website != "N/A" else None, target="_blank")
                        ]),
                        html.P([html.Strong("Type: "), html.Span(organizations_data[selected_rows[0]]["org_type"])]),
                        html.P([html.Strong("State: "), html.Span(organizations_data[selected_rows[0]]["state"])]),
                        html.P([html.Strong("Relevance Score: "), html.Span(f"{organizations_data[selected_rows[0]]['relevance_score']}/10")]),
                        html.P([html.Strong("Discovered: "), html.Span(organizations_data[selected_rows[0]]["date_added"])]),
                        html.P([html.Strong("Description: "), html.Span(org_detail.description or "No description available")]),
                    ], style={"backgroundColor": "rgba(0, 0, 0, 0.02)", "padding": "15px", "borderRadius": "5px"}),
                    html.H5(f"Contacts ({len(data)})", style={"marginTop": "15px", "marginBottom": "10px"})
                ])
                
                return data, org_detail_view
            
            except Exception as e:
                logger.error(f"Error loading contacts: {e}")
                return [], html.P(f"Error loading contacts: {str(e)}", style={"color": COLORS["danger"]})
            
        # Export button callbacks
        @self.app.callback(
            dash.Output("export-orgs-btn", "n_clicks"),
            [dash.Input("export-orgs-btn", "n_clicks")],
            [dash.State("organizations-table", "data")]
        )
        def export_organizations(n_clicks, data):
            if not n_clicks:
                return None
            
            try:
                if data:
                    # Convert to DataFrame
                    df = pd.DataFrame(data)
                    
                    # Generate timestamp for filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"organizations_export_{timestamp}.csv"
                    
                    # Save to CSV in reports directory
                    export_path = f"reports/{filename}"
                    df.to_csv(export_path, index=False)
                    
                    logger.info(f"Exported {len(data)} organizations to {export_path}")
            except Exception as e:
                logger.error(f"Error exporting organizations: {e}")
            
            return None
        
        @self.app.callback(
            dash.Output("export-contacts-btn", "n_clicks"),
            [dash.Input("export-contacts-btn", "n_clicks")],
            [dash.State("contacts-table", "data")]
        )
        def export_contacts(n_clicks, data):
            if not n_clicks:
                return None
            
            try:
                if data:
                    # Convert to DataFrame
                    df = pd.DataFrame(data)
                    
                    # Generate timestamp for filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"contacts_export_{timestamp}.csv"
                    
                    # Save to CSV in reports directory
                    export_path = f"reports/{filename}"
                    df.to_csv(export_path, index=False)
                    
                    logger.info(f"Exported {len(data)} contacts to {export_path}")
            except Exception as e:
                logger.error(f"Error exporting contacts: {e}")
            
            return None
            
        @self.app.callback(
            dash.Output("assignment-status", "children"),
            [dash.Input("save-assignments-btn", "n_clicks")],
            [dash.State(f"assign-{org_type['value']}", "value") for org_type in ORG_TYPES]
        )
        def save_assignments(n_clicks, *assignments):
            if not n_clicks:
                return ""
            
            # Create mapping of org_types to emails
            assignment_map = {}
            for i, org_type in enumerate(ORG_TYPES):
                assignment_map[org_type['value']] = assignments[i]
            
            # Update EMAIL_USERS config
            new_email_users = {}
            for email in EMAIL_OPTIONS:
                email_value = email['value']
                new_email_users[email_value] = []
                
                # Assign org types to this email
                for org_type, assigned_email in assignment_map.items():
                    if assigned_email == email_value:
                        new_email_users[email_value].append(org_type)
            
            try:
                # Update in-memory config
                global EMAIL_USERS
                EMAIL_USERS.clear()
                for email, org_types in new_email_users.items():
                    EMAIL_USERS[email] = org_types
                
                return html.Div("Assignments saved successfully", className="status-message status-success")
            except Exception as e:
                return html.Div(f"Error saving assignments: {str(e)}", className="status-message status-error")
                
        @self.app.callback(
            dash.Output("run-status", "children"),
            [dash.Input("run-app-btn", "n_clicks")],
            [
                dash.State("app-mode", "value"),
                dash.State("email-limit", "value"),
                dash.State("max-orgs", "value"),
                dash.State("target-org-types", "value")
            ]
        )
        def run_application(n_clicks, app_mode, email_limit, max_orgs, target_org_types):
            if not n_clicks:
                return ""
                
            try:
                # Update email limit in memory
                global EMAIL_DAILY_LIMIT_PER_USER
                EMAIL_DAILY_LIMIT_PER_USER = email_limit
                
                # Determine which script to run based on app mode
                if app_mode == "org_building":
                    command = [
                        sys.executable, "-m", "app.discovery_app", "discover",
                        "--max-orgs", str(max_orgs),
                        "--enhanced"
                    ]
                    
                    # Add target org types if specific ones are selected (not "all")
                    target_msg = ""
                    if target_org_types and target_org_types != "all" and target_org_types != ["all"]:
                        # Convert list to comma-separated string if it's a list
                        if isinstance(target_org_types, list):
                            target_org_types_str = ",".join(target_org_types)
                        else:
                            target_org_types_str = str(target_org_types)
                            
                        command.extend(["--target-org-types", target_org_types_str])
                        
                        # Format types for display in status message
                        if isinstance(target_org_types, list):
                            type_names = [next((t['label'] for t in ORG_TYPES if t['value'] == typ), typ) 
                                         for typ in target_org_types]
                            target_msg = f" targeting: {', '.join(type_names)}"
                        else:
                            target_msg = f" targeting: {target_org_types}"
                    
                    status_msg = f"Started organization discovery with max orgs: {max_orgs}{target_msg}"
                elif app_mode == "contact_building":
                    command = [
                        sys.executable, "-m", "app.discovery_app", "discover",
                        "--max-orgs", str(max_orgs // 2),  # Fewer orgs but focus on contact discovery
                        "--enhanced",
                        "--contact-focus"
                    ]
                    status_msg = f"Started contact discovery with max orgs: {max_orgs // 2}"
                elif app_mode == "sending_emails":
                    command = [
                        sys.executable, "-m", "app.email_app", "campaign",
                        "--max-per-salesperson", str(email_limit),
                        "--min-confidence", "0.7"
                    ]
                    status_msg = f"Started email campaign with {email_limit} emails per person"
                
                # Run the command in a non-blocking way
                subprocess.Popen(command)
                
                return html.Div(
                    [
                        html.P(status_msg),
                        html.P("Application is running in the background. Check the console for progress.")
                    ], 
                    className="status-message status-info"
                )
            except Exception as e:
                return html.Div(f"Error running application: {str(e)}", className="status-message status-error")
    
    def _get_overview_metrics(self, start_date, end_date):
        """Get overview metrics."""
        try:
            # SIMPLIFIED METRICS APPROACH - Use direct SQL for accurate counting
            conn = self.db_session.connection()
            
            # Get total organizations and contacts using direct SQL
            total_orgs = conn.execute(text("SELECT COUNT(*) FROM organizations")).scalar()
            total_contacts = conn.execute(text("SELECT COUNT(*) FROM contacts")).scalar()
            
            # Format start date for SQL comparison
            start_date_str = start_date.strftime("%Y-%m-%d")
            
            # Different metrics based on dashboard type
            if self.dashboard_type == "email":
                # Email campaign specific metrics
                
                # Simplified approach with direct SQL
                total_drafts = conn.execute(text("SELECT COUNT(*) FROM contacts WHERE email_draft_created = 1")).scalar()
                new_drafts = conn.execute(text(f"SELECT COUNT(*) FROM contacts WHERE email_draft_created = 1 AND date(email_draft_date) >= '{start_date_str}'")).scalar()
                valid_emails = conn.execute(text("SELECT COUNT(*) FROM contacts WHERE email_valid = 1")).scalar()
                high_confidence = conn.execute(text("SELECT COUNT(*) FROM contacts WHERE contact_confidence_score >= 0.8")).scalar()
                
                # Email draft success rate
                success_rate = (total_drafts / total_contacts * 100) if total_contacts > 0 else 0
                
                return [
                    html.Div([
                        html.H3(f"{total_contacts:,}"),
                        html.P("Total Contacts")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{valid_emails:,}"),
                        html.P("Contacts with Valid Emails")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{total_drafts:,}"),
                        html.P("Total Email Drafts")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{new_drafts:,}"),
                        html.P("New Email Drafts")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{success_rate:.1f}%"),
                        html.P("Email Draft Success Rate")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{high_confidence:,}"),
                        html.P("High Confidence Contacts")
                    ], className="metric-box")
                ]
            else:
                # Discovery specific metrics
                
                # Get counts using direct SQL for simplicity
                new_orgs = conn.execute(text(f"SELECT COUNT(*) FROM organizations WHERE date(date_added) >= '{start_date_str}'")).scalar()
                new_contacts = conn.execute(text(f"SELECT COUNT(*) FROM contacts WHERE date(date_added) >= '{start_date_str}'")).scalar()
                
                # High relevance organizations and contacts
                high_relevance = conn.execute(text("SELECT COUNT(*) FROM organizations WHERE relevance_score >= 0.8")).scalar()
                high_relevance_contacts = conn.execute(text("SELECT COUNT(*) FROM contacts WHERE contact_relevance_score >= 7")).scalar()
                
                # URLs discovered
                urls_discovered = conn.execute(text(f"SELECT COUNT(*) FROM discovered_urls WHERE date(last_crawled) >= '{start_date_str}'")).scalar() or 0
                
                return [
                    html.Div([
                        html.H3(f"{total_orgs:,}"),
                        html.P("Total Organizations")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{new_orgs:,}"),
                        html.P("New Organizations")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{total_contacts:,}"),
                        html.P("Total Contacts")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{new_contacts:,}"),
                        html.P("New Contacts")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{high_relevance:,}"),
                        html.P("High Relevance Organizations")
                    ], className="metric-box"),
                    html.Div([
                        html.H3(f"{high_relevance_contacts:,}"),
                        html.P("High Relevance Contacts")
                    ], className="metric-box")
                ]
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            # Return empty metrics on error
            return [
                html.Div([
                    html.H3("Error"),
                    html.P(f"Could not load metrics: {str(e)}")
                ], className="metric-box")
            ]
    
    def _get_discoveries_chart(self, start_date, end_date):
        """Get discoveries over time chart."""
        try:
            # Convert to date string for comparison
            start_date_str = start_date.strftime('%Y-%m-%d')
            
            # Use direct SQL for simplicity and reliability
            conn = self.db_session.connection()
            
            # Organizations by date
            org_query = f"""
                SELECT date(date_added) as date, COUNT(*) as count 
                FROM organizations 
                WHERE date(date_added) >= '{start_date_str}'
                GROUP BY date(date_added)
                ORDER BY date(date_added)
            """
            org_data = conn.execute(text(org_query)).fetchall()
            
            # Contacts by date
            contact_query = f"""
                SELECT date(date_added) as date, COUNT(*) as count 
                FROM contacts 
                WHERE date(date_added) >= '{start_date_str}'
                GROUP BY date(date_added)
                ORDER BY date(date_added)
            """
            contact_data = conn.execute(text(contact_query)).fetchall()
            
            # Create pandas dataframes
            org_df = pd.DataFrame(org_data, columns=["date", "count"])
            org_df["type"] = "Organizations"
            
            contact_df = pd.DataFrame(contact_data, columns=["date", "count"])
            contact_df["type"] = "Contacts"
            
            # Combine data
            df = pd.concat([org_df, contact_df]) if not org_df.empty and not contact_df.empty else pd.DataFrame(columns=["date", "count", "type"])
            
            if df.empty:
                # Return empty figure with message
                fig = go.Figure()
                fig.add_annotation(
                    text="No data available for the selected time range",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False,
                    font=dict(color=COLORS['dark'], size=14)
                )
                return fig
            
            # Create figure
            fig = px.line(
                df, x="date", y="count", color="type",
                labels={"count": "Count", "date": "Date", "type": "Type"},
                template="plotly_white",
                color_discrete_map={"Organizations": COLORS['secondary'], "Contacts": COLORS['accent']}
            )
            
            # Update layout
            fig.update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font={'color': COLORS['text'], 'family': 'Poppins'},
                margin=dict(l=40, r=40, t=40, b=40),
                hovermode="closest",
                legend=dict(orientation="h", y=1.1),
                xaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(230, 230, 230, 0.8)',
                    zeroline=False
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(230, 230, 230, 0.8)',
                    zeroline=False
                )
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error generating discoveries chart: {e}")
            # Return empty figure on error
            fig = go.Figure()
            fig.add_annotation(
                text=f"Error loading chart: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(color=COLORS['danger'], size=14)
            )
            return fig
    
    def _get_org_types_chart(self):
        """Get organizations by type chart."""
        try:
            # Use direct SQL for simplicity
            conn = self.db_session.connection()
            org_types_query = """
                SELECT org_type, COUNT(*) as count
                FROM organizations
                GROUP BY org_type
                ORDER BY count DESC
            """
            org_types = conn.execute(text(org_types_query)).fetchall()
            
            df = pd.DataFrame(org_types, columns=["org_type", "count"])
            
            if df.empty:
                # Return empty figure
                fig = go.Figure()
                fig.add_annotation(
                    text="No organization type data available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False
                )
                return fig
            
            # Map org_type to readable labels
            type_labels = {
                "engineering": "Engineering Firms",
                "government": "Government Agencies",
                "municipal": "Municipalities",
                "water": "Water & Wastewater",
                "utility": "Utility Companies",
                "transportation": "Transportation Auth.",
                "oil_gas": "Oil, Gas & Mining",
                "agriculture": "Agriculture & Irrigation"
            }
            
            df["org_type_label"] = df["org_type"].map(lambda x: type_labels.get(x, x))
            
            fig = px.pie(
                df, values="count", names="org_type_label",
                title="Organizations by Type",
                template="plotly_white",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            
            # Update layout
            fig.update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font={'color': COLORS['text'], 'family': 'Poppins'},
                margin=dict(l=20, r=20, t=40, b=20),
                uniformtext_minsize=12,
                uniformtext_mode='hide'
            )
            
            # Update traces
            fig.update_traces(
                textinfo='percent+label',
                textposition='outside',
                hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}',
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error generating org types chart: {e}")
            # Return empty figure on error
            fig = go.Figure()
            fig.add_annotation(
                text=f"Error loading chart: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )
            return fig
    
    def _get_org_states_chart(self):
        """Get organizations by state chart."""
        try:
            # Use direct SQL for simplicity
            conn = self.db_session.connection()
            states_query = """
                SELECT state, COUNT(*) as count
                FROM organizations
                WHERE state IS NOT NULL AND state != ''
                GROUP BY state
                ORDER BY count DESC
            """
            org_states = conn.execute(text(states_query)).fetchall()
            
            df = pd.DataFrame(org_states, columns=["state", "count"])
            
            if df.empty:
                # Return empty figure
                fig = go.Figure()
                fig.add_annotation(
                    text="No organization state data available",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False
                )
                return fig
            
            fig = px.bar(
                df, x="state", y="count",
                title="Organizations by State",
                labels={"count": "Count", "state": "State"},
                template="plotly_white",
                color="count",
                color_continuous_scale="Viridis"
            )
            
            # Update layout
            fig.update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font={'color': COLORS['text'], 'family': 'Poppins'},
                margin=dict(l=40, r=40, t=40, b=40),
                xaxis=dict(
                    title="State",
                    showgrid=False,
                    zeroline=False
                ),
                yaxis=dict(
                    title="Count",
                    showgrid=True,
                    gridcolor='rgba(230, 230, 230, 0.8)',
                    zeroline=False
                )
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error generating org states chart: {e}")
            # Return empty figure on error
            fig = go.Figure()
            fig.add_annotation(
                text=f"Error loading chart: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )
            return fig
    
    def _get_emails_chart(self, start_date, end_date):
        """Get email drafts by sales person chart."""
        try:
            # Get formatted start date for SQL
            start_date_str = start_date.strftime('%Y-%m-%d')
            
            # Use direct SQL for simplicity
            conn = self.db_session.connection()
            email_query = f"""
                SELECT assigned_to, COUNT(*) as count
                FROM contacts
                WHERE email_draft_created = 1
                  AND date(email_draft_date) >= '{start_date_str}'
                GROUP BY assigned_to
                ORDER BY count DESC
            """
            email_data = conn.execute(text(email_query)).fetchall()
            
            df = pd.DataFrame(email_data, columns=["assigned_to", "count"])
            
            if df.empty:
                # Return empty figure
                fig = go.Figure()
                fig.add_annotation(
                    text="No email data available for the selected time range",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False
                )
                return fig
            
            fig = px.bar(
                df, x="assigned_to", y="count",
                title="Email Drafts by Sales Person",
                labels={"count": "Count", "assigned_to": "Sales Person"},
                template="plotly_white",
                color="count",
                color_continuous_scale=px.colors.sequential.Blues
            )
            
            # Update layout
            fig.update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font={'color': COLORS['text'], 'family': 'Poppins'},
                margin=dict(l=40, r=40, t=40, b=40),
                xaxis=dict(
                    title="Sales Person",
                    showgrid=False,
                    zeroline=False
                ),
                yaxis=dict(
                    title="Email Drafts",
                    showgrid=True,
                    gridcolor='rgba(230, 230, 230, 0.8)',
                    zeroline=False
                )
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error generating emails chart: {e}")
            # Return empty figure on error
            fig = go.Figure()
            fig.add_annotation(
                text=f"Error loading chart: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )
            return fig
    
    def _get_performance_chart(self, start_date, end_date):
        """Get system performance chart."""
        try:
            # Get formatted start date for SQL
            start_date_str = start_date.strftime('%Y-%m-%d')
            
            # Use direct SQL for simplicity
            conn = self.db_session.connection()
            metrics_query = f"""
                SELECT date(metric_date) as date,
                       urls_discovered, urls_crawled, 
                       organizations_discovered, contacts_discovered,
                       emails_drafted
                FROM system_metrics
                WHERE date(metric_date) >= '{start_date_str}'
                ORDER BY date
            """
            metrics_data = conn.execute(text(metrics_query)).fetchall()
            
            df = pd.DataFrame(metrics_data, columns=[
                "date", "urls_discovered", "urls_crawled", 
                "organizations_discovered", "contacts_discovered",
                "emails_drafted"
            ])
            
            if df.empty:
                # Return empty figure
                fig = go.Figure()
                fig.add_annotation(
                    text="No performance data available for the selected time range",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False
                )
                return fig
            
            # Melt the dataframe for plotly
            df_melted = pd.melt(
                df, id_vars=["date"],
                value_vars=["urls_discovered", "urls_crawled", "organizations_discovered", 
                            "contacts_discovered", "emails_drafted"],
                var_name="metric", value_name="value"
            )
            
            # Map metric names to readable labels
            metric_labels = {
                "urls_discovered": "URLs Discovered",
                "urls_crawled": "URLs Crawled",
                "organizations_discovered": "Organizations Found",
                "contacts_discovered": "Contacts Found",
                "emails_drafted": "Emails Drafted"
            }
            
            df_melted["metric_label"] = df_melted["metric"].map(metric_labels)
            
            fig = px.line(
                df_melted, x="date", y="value", color="metric_label",
                title="System Performance Metrics",
                labels={"value": "Count", "date": "Date", "metric_label": "Metric"},
                template="plotly_white",
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            
            # Update layout
            fig.update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font={'color': COLORS['text'], 'family': 'Poppins'},
                margin=dict(l=40, r=40, t=40, b=40),
                hovermode="closest",
                legend=dict(orientation="h", y=1.1),
                xaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(230, 230, 230, 0.8)',
                    zeroline=False
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(230, 230, 230, 0.8)',
                    zeroline=False
                )
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error generating performance chart: {e}")
            # Return empty figure on error
            fig = go.Figure()
            fig.add_annotation(
                text=f"Error loading chart: {str(e)}",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False
            )
            return fig
    
    def _get_org_type_display(self, org_type):
        """Get display name for organization type."""
        type_labels = {
            "engineering": "Engineering",
            "government": "Government",
            "municipal": "Municipal",
            "water": "Water & WW",
            "utility": "Utility",
            "transportation": "Transportation",
            "oil_gas": "Oil & Gas",
            "agriculture": "Agriculture"
        }
        return type_labels.get(org_type, org_type)
    
    def _get_status_display(self, status):
        """Get display name for contact status."""
        status_labels = {
            "new": "New",
            "email_draft": "Draft Created",
            "emailed": "Email Sent",
            "responded": "Responded",
            "meeting_scheduled": "Meeting Scheduled",
            "not_interested": "Not Interested",
            "invalid": "Invalid"
        }
        return status_labels.get(status, status)
        
    def _get_contact_type_display(self, contact_type):
        """Get display name for contact type."""
        type_labels = {
            "actual": "Actual",
            "generic": "Generated",
            "inferred": "Inferred",
            None: "Unknown"
        }
        return type_labels.get(contact_type, contact_type)
    
    def _get_current_assignment(self, org_type):
        """Get the current email assignment for an organization type."""
        for email, org_types in EMAIL_USERS.items():
            if org_type in org_types:
                return email
        return EMAIL_OPTIONS[0]['value']  # Default to first option
    
    def run_server(self, debug=False, port=None):
        """
        Run the dashboard server.
        
        Args:
            debug: Whether to run in debug mode
            port: Port to run on (defaults to DASHBOARD_PORT)
        """
        if port is None:
            port = DASHBOARD_PORT
        
        # Find an available port if the default is in use
        import socket
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
        self.app.run_server(debug=debug, port=port)