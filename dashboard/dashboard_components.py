"""
Dashboard components for the GBL Data system.
"""
import dash
from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import func
from app.utils.logger import get_logger
import json

from app.models.organization import Organization
from app.models.contact import Contact
from app.models.dashboard_models import EmailEngagement, ProcessSummary
from app.models.discovery import Discovery
from app.database.models import get_db_session, Contact, Organization
from app.database import crud

# Import necessary constants from layout_parts.py
from app.dashboard.layout_parts import APP_MODES

logger = get_logger(__name__)

# The create_settings_store and create_confidence_hurdle_controls functions 
# have been moved to layout_parts.py to avoid circular imports

# Color scheme
COLORS = {
    'primary': '#2ecc71',
    'secondary': '#3498db',
    'success': '#27ae60',
    'info': '#2980b9',
    'warning': '#f39c12',
    'danger': '#e74c3c',
    'light': '#ecf0f1',
    'dark': '#2c3e50'
}

# Email options
EMAIL_OPTIONS = [
    {'label': 'All', 'value': 'all'},
    {'label': 'Draft', 'value': 'draft'},
    {'label': 'Sent', 'value': 'sent'},
    {'label': 'Failed', 'value': 'failed'}
]

# No need to redefine APP_MODES here since we're importing it from layout_parts.py

def create_organizations_tab():
    """Create the organizations tab content."""
    return html.Div([
        # Filters
        html.Div([
            html.Div([
                html.Label("Organization Type"),
                dcc.Dropdown(
                    id='filter-org-type',
                    options=[
                        {'label': 'All Types', 'value': 'all'},
                        {'label': 'Manufacturer', 'value': 'manufacturer'},
                        {'label': 'Distributor', 'value': 'distributor'},
                        {'label': 'Retailer', 'value': 'retailer'}
                    ],
                    value='all',
                    clearable=False
                )
            ], className="filter-item"),
            html.Div([
                html.Label("State"),
                dcc.Dropdown(
                    id='filter-state',
                    options=[{'label': 'All States', 'value': 'all'}],
                    value='all',
                    clearable=False
                )
            ], className="filter-item"),
            html.Div([
                html.Label("Minimum Relevance"),
                dcc.Slider(
                    id='min-relevance',
                    min=1,
                    max=10,
                    step=1,
                    value=1,
                    marks={i: str(i) for i in range(1, 11)}
                )
            ], className="filter-item"),
            html.Div([
                html.Label("Time Range"),
                dcc.Dropdown(
                    id='time-range-dropdown',
                    options=[
                        {'label': 'Last 24 Hours', 'value': '24h'},
                        {'label': 'Last 7 Days', 'value': '7d'},
                        {'label': 'Last 30 Days', 'value': '30d'},
                        {'label': 'All Time', 'value': 'all'}
                    ],
                    value='7d',
                    clearable=False
                )
            ], className="filter-item")
        ], className="filters-container"),
        
        # Organizations table
        html.Div([
            dash_table.DataTable(
                id='organizations-table',
                columns=[
                    {'name': 'Name', 'id': 'name'},
                    {'name': 'Type', 'id': 'org_type'},
                    {'name': 'State', 'id': 'state'},
                    {'name': 'Relevance', 'id': 'relevance_score'},
                    {'name': 'Contacts', 'id': 'contacts_count'},
                    {'name': 'Discovered', 'id': 'discovered_at'}
                ],
                data=[],
                sort_action='native',
                sort_mode='multi',
                page_action='native',
                page_size=10,
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
                    }
                ]
            )
        ], className="table-container")
    ])

def create_contacts_tab():
    """Create the contacts tab content."""
    return html.Div([
        # Filters
        html.Div([
            html.Div([
                html.Label("Contact Type"),
                dcc.Dropdown(
                    id='filter-contact-type',
                    options=[
                        {'label': 'All Types', 'value': 'all'},
                        {'label': 'Actual', 'value': 'actual'},
                        {'label': 'Generated', 'value': 'generated'},
                        {'label': 'Inferred', 'value': 'inferred'}
                    ],
                    value='all',
                    clearable=False
                )
            ], className="filter-item"),
            html.Div([
                html.Label("Time Range"),
                dcc.Dropdown(
                    id='contacts-time-range',
                    options=[
                        {'label': 'Last 24 Hours', 'value': '24h'},
                        {'label': 'Last 7 Days', 'value': '7d'},
                        {'label': 'Last 30 Days', 'value': '30d'},
                        {'label': 'All Time', 'value': 'all'}
                    ],
                    value='7d',
                    clearable=False
                )
            ], className="filter-item")
        ], className="filters-container"),
        
        # Contacts table
        html.Div([
            dash_table.DataTable(
                id='contacts-table',
                columns=[
                    {'name': 'Name', 'id': 'name'},
                    {'name': 'Title', 'id': 'title'},
                    {'name': 'Email', 'id': 'email'},
                    {'name': 'Type', 'id': 'contact_type'},
                    {'name': 'Organization', 'id': 'organization_name'},
                    {'name': 'Created', 'id': 'created_at'}
                ],
                data=[],
                sort_action='native',
                sort_mode='multi',
                page_action='native',
                page_size=10,
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
        ], className="table-container")
    ])

def create_emails_tab():
    """Create the emails tab content."""
    return html.Div([
        # Filters
        html.Div([
            html.Div([
                html.Label("Status"),
                dcc.Dropdown(
                    id='filter-email-status',
                    options=EMAIL_OPTIONS,
                    value='all',
                    clearable=False
                )
            ], className="filter-item"),
            html.Div([
                html.Label("Time Range"),
                dcc.Dropdown(
                    id='emails-time-range',
                    options=[
                        {'label': 'Last 24 Hours', 'value': '24h'},
                        {'label': 'Last 7 Days', 'value': '7d'},
                        {'label': 'Last 30 Days', 'value': '30d'},
                        {'label': 'All Time', 'value': 'all'}
                    ],
                    value='7d',
                    clearable=False
                )
            ], className="filter-item")
        ], className="filters-container"),
        
        # Emails table
        html.Div([
            dash_table.DataTable(
                id='emails-table',
                columns=[
                    {'name': 'To', 'id': 'to_email'},
                    {'name': 'Subject', 'id': 'subject'},
                    {'name': 'Status', 'id': 'status'},
                    {'name': 'Sent At', 'id': 'sent_at'},
                    {'name': 'Organization', 'id': 'organization_name'}
                ],
                data=[],
                sort_action='native',
                sort_mode='multi',
                page_action='native',
                page_size=10,
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
                        "if": {"column_id": "status", "filter_query": "{status} = 'email_draft'"},
                        "backgroundColor": "rgba(52, 152, 219, 0.2)",
                        "color": COLORS['info']
                    },
                    {
                        "if": {"column_id": "status", "filter_query": "{status} = 'emailed'"},
                        "backgroundColor": "rgba(46, 204, 113, 0.2)",
                        "color": COLORS['success']
                    }
                ]
            )
        ], className="table-container")
    ])

def create_metrics_tab():
    """Create the metrics tab content."""
    return html.Div([
        # Overview metrics
        html.Div(id='overview-metrics', className="metrics-container"),
        
        # Charts
        html.Div([
            html.Div([
                dcc.Graph(id='discoveries-chart')
            ], className="chart-container"),
            html.Div([
                dcc.Graph(id='org-types-chart')
            ], className="chart-container"),
            html.Div([
                dcc.Graph(id='org-states-chart')
            ], className="chart-container"),
            html.Div([
                dcc.Graph(id='emails-chart')
            ], className="chart-container"),
            html.Div([
                dcc.Graph(id='performance-chart')
            ], className="chart-container")
        ], className="charts-grid")
    ])

def generate_top_metrics(db_session):
    """Generate top metrics for the dashboard."""
    try:
        # Get total organizations
        total_orgs = db_session.query(func.count(Organization.id)).scalar() or 0
        
        # Get organizations discovered in last 24 hours - use created_at instead of discovered_at
        try:
            # First try with discovered_at (if it exists)
            new_orgs = db_session.query(func.count(Organization.id)).filter(
                Organization.discovered_at >= datetime.now() - timedelta(hours=24)
            ).scalar() or 0
        except Exception:
            # Fallback to created_at if discovered_at doesn't exist
            try:
                new_orgs = db_session.query(func.count(Organization.id)).filter(
                    Organization.created_at >= datetime.now() - timedelta(hours=24)
                ).scalar() or 0
            except Exception:
                # If neither column exists, default to 0
                new_orgs = 0
        
        # Get total contacts
        total_contacts = db_session.query(func.count(Contact.id)).scalar() or 0
        
        # Get contacts created in last 24 hours - handle missing column
        try:
            # First try with created_at if it exists
            new_contacts = db_session.query(func.count(Contact.id)).filter(
                Contact.created_at >= datetime.now() - timedelta(hours=24)
            ).scalar() or 0
        except Exception:
            # Try with alternate date columns if available
            try:
                # Try with added_at if it exists
                new_contacts = db_session.query(func.count(Contact.id)).filter(
                    Contact.added_at >= datetime.now() - timedelta(hours=24)
                ).scalar() or 0
            except Exception:
                # If no date column exists, default to 0
                new_contacts = 0
        
        # Get total emails sent - Updated to use EmailEngagement instead of Email
        total_emails = db_session.query(func.count(EmailEngagement.id)).scalar() or 0
        
        # Get emails sent in last 24 hours - Updated to use EmailEngagement instead of Email
        new_emails = db_session.query(func.count(EmailEngagement.id)).filter(
            EmailEngagement.email_sent_date >= datetime.now() - timedelta(hours=24)
        ).scalar() or 0
        
        # Calculate success rates (avoid division by zero)
        org_success_rate = round((new_orgs / total_orgs * 100) if total_orgs > 0 else 0, 1)
        contact_success_rate = round((new_contacts / total_contacts * 100) if total_contacts > 0 else 0, 1)
        email_success_rate = round((new_emails / total_emails * 100) if total_emails > 0 else 0, 1)
        
        return [
            html.Div(children=[
                html.H3(f"{total_orgs:,}", className="metric-value"),
                html.P("Total Organizations", className="metric-label"),
                html.P(f"+{new_orgs:,} ({org_success_rate}%)", className="metric-change")
            ], className="metric-card"),
            html.Div(children=[
                html.H3(f"{total_contacts:,}", className="metric-value"),
                html.P("Total Contacts", className="metric-label"),
                html.P(f"+{new_contacts:,} ({contact_success_rate}%)", className="metric-change")
            ], className="metric-card"),
            html.Div(children=[
                html.H3(f"{total_emails:,}", className="metric-value"),
                html.P("Total Emails Sent", className="metric-label"),
                html.P(f"+{new_emails:,} ({email_success_rate}%)", className="metric-change")
            ], className="metric-card")
        ]
    except Exception as e:
        logger.error(f"Error generating top metrics: {e}")
        return []

def create_validation_reports_tab():
    """Create the validation reports tab content."""
    return html.Div([
        # Filters
        html.Div([
            html.Div([
                html.Label("Time Range"),
                dcc.Dropdown(
                    id='validation-time-range',
                    options=[
                        {'label': 'Last 24 Hours', 'value': '24h'},
                        {'label': 'Last 7 Days', 'value': '7d'},
                        {'label': 'Last 30 Days', 'value': '30d'},
                        {'label': 'All Time', 'value': 'all'}
                    ],
                    value='7d',
                    clearable=False
                )
            ], className="filter-item"),
            html.Div([
                html.Label("Approval Status"),
                dcc.Dropdown(
                    id='validation-approval-status',
                    options=[
                        {'label': 'All', 'value': 'all'},
                        {'label': 'Approved', 'value': 'approved'},
                        {'label': 'Rejected', 'value': 'rejected'}
                    ],
                    value='all',
                    clearable=False
                )
            ], className="filter-item")
        ], className="filters-container"),
        
        # Validation summary metrics
        html.Div(
            id='validation-summary',
            className="metrics-container",
            children=[]
        ),
        
        # Charts row
        html.Div([
            html.Div([
                dcc.Graph(id='validation-approval-chart')
            ], className="col-md-6"),
            html.Div([
                dcc.Graph(id='validation-scores-chart')
            ], className="col-md-6")
        ], className="row charts-row"),
        
        # Validation reports table
        html.Div([
            html.H3("Recent Validation Reports"),
            dash_table.DataTable(
                id='validation-reports-table',
                columns=[
                    {'name': 'Contact', 'id': 'contact_name'},
                    {'name': 'Email', 'id': 'email'},
                    {'name': 'Organization', 'id': 'organization'},
                    {'name': 'Date', 'id': 'validation_date'},
                    {'name': 'Status', 'id': 'status'},
                    {'name': 'Contact Score', 'id': 'contact_score'},
                    {'name': 'Contact Threshold', 'id': 'contact_threshold'},
                    {'name': 'Org Score', 'id': 'org_score'},
                    {'name': 'Org Threshold', 'id': 'org_threshold'}
                ],
                data=[],
                sort_action='native',
                sort_mode='multi',
                page_action='native',
                page_size=10,
                filter_action='native',
                style_table={"overflowX": "auto"},
                style_data_conditional=[
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "rgba(0, 0, 0, 0.02)"
                    },
                    {
                        "if": {"filter_query": "{status} contains 'Approved'"},
                        "backgroundColor": "rgba(46, 204, 113, 0.2)",
                        "color": COLORS['success']
                    },
                    {
                        "if": {"filter_query": "{status} contains 'Rejected'"},
                        "backgroundColor": "rgba(231, 76, 60, 0.2)",
                        "color": COLORS['danger']
                    }
                ],
                tooltip_data=[],
                tooltip_duration=None,
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
                }
            )
        ], className="table-container"),
        
        # Validation details section
        html.Div([
            html.H3("Validation Details"),
            html.Div(id="validation-details", className="validation-details")
        ], className="details-container")
    ]) 

def create_process_summaries_tab():
    """Create the process summaries tab content."""
    return html.Div([
        # Filters
        html.Div([
            html.Div([
                html.Label("Process Type"),
                dcc.Dropdown(
                    id='filter-process-type',
                    options=[
                        {'label': 'All Types', 'value': 'all'},
                        {'label': 'Organization Building', 'value': 'org_building'},
                        {'label': 'Contact Building', 'value': 'contact_building'},
                        {'label': 'Email Sending', 'value': 'email_sending'}
                    ],
                    value='all',
                    clearable=False
                )
            ], className="filter-item"),
            html.Div([
                html.Label("Time Range"),
                dcc.Dropdown(
                    id='process-time-range',
                    options=[
                        {'label': 'Last 24 Hours', 'value': '24h'},
                        {'label': 'Last 7 Days', 'value': '7d'},
                        {'label': 'Last 30 Days', 'value': '30d'},
                        {'label': 'All Time', 'value': 'all'}
                    ],
                    value='7d',
                    clearable=False
                )
            ], className="filter-item"),
            html.Div([
                html.Button(
                    "Refresh Data", 
                    id="refresh-process-summaries", 
                    className="btn btn-primary"
                )
            ], className="filter-item")
        ], className="filters-container"),
        
        # Process summary table
        html.Div([
            dash_table.DataTable(
                id='process-summaries-table',
                columns=[
                    {'name': 'Process Type', 'id': 'process_type_display'},
                    {'name': 'Started', 'id': 'started_at'},
                    {'name': 'Completed', 'id': 'completed_at'},
                    {'name': 'Status', 'id': 'status'},
                    {'name': 'Items Processed', 'id': 'items_processed'},
                    {'name': 'Items Added', 'id': 'items_added'}
                ],
                data=[],
                sort_action='native',
                sort_mode='multi',
                page_action='native',
                page_size=10,
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
                        "if": {"column_id": "status", "filter_query": "{status} = 'completed'"},
                        "backgroundColor": "rgba(46, 204, 113, 0.2)",
                        "color": COLORS['success']
                    },
                    {
                        "if": {"column_id": "status", "filter_query": "{status} = 'failed'"},
                        "backgroundColor": "rgba(231, 76, 60, 0.2)",
                        "color": COLORS['danger']
                    },
                    {
                        "if": {"column_id": "status", "filter_query": "{status} = 'running'"},
                        "backgroundColor": "rgba(52, 152, 219, 0.2)",
                        "color": COLORS['info']
                    }
                ]
            )
        ], className="table-container"),
        
        # Details section
        html.Div([
            html.H4("Process Details", className="details-header"),
            html.Div(id="process-details", className="details-content")
        ], className="details-container")
    ])

def create_sidebar():
    # Implementation of create_sidebar function
    pass
