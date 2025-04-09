#!/usr/bin/env python3
"""
Standalone emergency dashboard that doesn't depend on any other components.
Run this directly from the command line when the regular dashboard is broken.
"""
import dash
from dash import html, dcc
import sqlite3
import os
from pathlib import Path
import sys
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def create_emergency_dashboard():
    """Create a basic emergency dashboard."""
    app = dash.Dash(__name__)
    
    # Get database path from environment variable or use default
    db_path = os.environ.get('DATABASE_PATH', 'data/contact_manager.db')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    if not os.path.exists(db_path):
        # Try to find database in common locations
        possible_paths = [
            os.path.join(project_root, 'data', 'contact_manager.db'),
            os.path.join(project_root, 'contact_manager.db'),
            os.path.join(current_dir, 'data', 'contact_manager.db'),
            os.path.join(current_dir, 'contact_manager.db'),
        ]
        for path in possible_paths:
            if os.path.exists(path):
                db_path = path
                logger.info(f"Found database at {db_path}")
                break
    
    logger.info(f"Using database at {db_path}")
    
    try:
        # Attempt to connect to database
        conn = sqlite3.connect(db_path)
        
        # Get some basic stats
        cursor = conn.cursor()
        
        # Get organization count
        cursor.execute("SELECT COUNT(*) FROM organization")
        org_count = cursor.fetchone()[0]
        
        # Get contact count
        cursor.execute("SELECT COUNT(*) FROM contact")
        contact_count = cursor.fetchone()[0]
        
        # Get email count if table exists
        email_count = 0
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email'")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM email")
            email_count = cursor.fetchone()[0]
        
        # Get latest organizations
        cursor.execute("SELECT name, org_type, state, discovered_at FROM organization ORDER BY discovered_at DESC LIMIT 10")
        latest_orgs = cursor.fetchall()
        
        # Close database connection
        conn.close()
        
        # Create layout with basic stats
        app.layout = html.Div([
            html.H1("Emergency Dashboard"),
            html.Div([
                html.P("This is an emergency dashboard displayed when the regular dashboard has errors."),
                html.P("Please check the logs for more details on the error.")
            ], style={'margin': '20px 0', 'padding': '15px', 'backgroundColor': '#ffeeee', 'border': '1px solid #ff0000'}),
            
            html.Div([
                html.Div([
                    html.H3(org_count),
                    html.P("Organizations")
                ], style={'flex': 1, 'textAlign': 'center', 'padding': '20px', 'backgroundColor': '#f0f0f0', 'margin': '10px'}),
                
                html.Div([
                    html.H3(contact_count),
                    html.P("Contacts")
                ], style={'flex': 1, 'textAlign': 'center', 'padding': '20px', 'backgroundColor': '#f0f0f0', 'margin': '10px'}),
                
                html.Div([
                    html.H3(email_count),
                    html.P("Emails")
                ], style={'flex': 1, 'textAlign': 'center', 'padding': '20px', 'backgroundColor': '#f0f0f0', 'margin': '10px'})
            ], style={'display': 'flex', 'flexWrap': 'wrap'}),
            
            html.H2("Latest Organizations"),
            html.Table([
                html.Thead(
                    html.Tr([
                        html.Th("Name"),
                        html.Th("Type"),
                        html.Th("State"),
                        html.Th("Discovered At")
                    ])
                ),
                html.Tbody([
                    html.Tr([
                        html.Td(org[0]),
                        html.Td(org[1]),
                        html.Td(org[2]),
                        html.Td(org[3])
                    ]) for org in latest_orgs
                ])
            ], style={'width': '100%', 'borderCollapse': 'collapse', 'marginBottom': '30px'}),
            
            html.Div([
                html.H3("Troubleshooting"),
                html.P([
                    "The dashboard error is likely related to callbacks in the dashboard code. ",
                    "Here's the error in the logs: function takes at most 8 arguments (10 given)"
                ]),
                html.P([
                    "This is a common issue with Dash callbacks where a function is defined with fewer parameters ",
                    "than are being passed to it. To fix this issue, you need to modify the callback functions ",
                    "in app/dashboard/dashboard_callbacks.py to accept variable arguments."
                ]),
                html.P([
                    "Common fixes:",
                    html.Ul([
                        html.Li("Add *args parameter to all callback functions to handle extra arguments"),
                        html.Li("Check for mismatched Output/Input arguments in callback decorators"),
                        html.Li("Look for circular dependencies in callbacks")
                    ])
                ]),
                html.P([
                    "Specific fix for your error:",
                    html.Pre("""
# In app/dashboard/dashboard_callbacks.py
# Find each callback function and modify it to accept *args
@app.callback(
    [
        dash.Output("target-org-types-container", "style"),
        dash.Output("target-states-container", "style"),
        dash.Output("target-states", "options")
    ],
    [dash.Input("app-mode-selector", "value")]
)
def toggle_target_filters(app_mode, *args):  # Add *args here
    # Function body...
                    """, style={'backgroundColor': '#f8f8f8', 'padding': '10px', 'overflow': 'auto'})
                ])
            ], style={'margin': '20px 0', 'padding': '15px', 'backgroundColor': '#eeeeff', 'border': '1px solid #0000ff'})
        ], style={'padding': '20px', 'fontFamily': 'Arial, sans-serif', 'maxWidth': '1200px', 'margin': '0 auto'})
        
    except Exception as e:
        logger.error(f"Could not connect to database at {db_path}: {str(e)}")
        # If database connection fails, show simplified layout
        app.layout = html.Div([
            html.H1("Emergency Dashboard"),
            html.Div([
                html.P("This is an emergency dashboard displayed when the regular dashboard has errors."),
                html.P("Please check the logs for more details on the error."),
                html.P(f"Additionally, could not connect to database at {db_path}: {str(e)}")
            ], style={'margin': '20px 0', 'padding': '15px', 'backgroundColor': '#ffeeee', 'border': '1px solid #ff0000'}),
            
            html.Div([
                html.H3("Troubleshooting"),
                html.P([
                    "The dashboard error is likely related to callbacks in the dashboard code. ",
                    "Here's the error in the logs: function takes at most 8 arguments (10 given)"
                ]),
                html.P([
                    "This is a common issue with Dash callbacks where a function is defined with fewer parameters ",
                    "than are being passed to it. To fix this issue, you need to modify the callback functions ",
                    "in app/dashboard/dashboard_callbacks.py to accept variable arguments."
                ]),
                html.P([
                    "Common fixes:",
                    html.Ul([
                        html.Li("Add *args parameter to all callback functions to handle extra arguments"),
                        html.Li("Check for mismatched Output/Input arguments in callback decorators"),
                        html.Li("Look for circular dependencies in callbacks")
                    ])
                ]),
                html.P([
                    "Specific fix for your error:",
                    html.Pre("""
# In app/dashboard/dashboard_callbacks.py
# Find each callback function and modify it to accept *args
@app.callback(
    [
        dash.Output("target-org-types-container", "style"),
        dash.Output("target-states-container", "style"),
        dash.Output("target-states", "options")
    ],
    [dash.Input("app-mode-selector", "value")]
)
def toggle_target_filters(app_mode, *args):  # Add *args here
    # Function body...
                    """, style={'backgroundColor': '#f8f8f8', 'padding': '10px', 'overflow': 'auto'})
                ])
            ], style={'margin': '20px 0', 'padding': '15px', 'backgroundColor': '#eeeeff', 'border': '1px solid #0000ff'})
        ], style={'padding': '20px', 'fontFamily': 'Arial, sans-serif', 'maxWidth': '1200px', 'margin': '0 auto'})
    
    return app

def run_emergency_dashboard(port=8050):
    """Run the emergency dashboard."""
    logger.info(f"Starting emergency dashboard on port {port}")
    app = create_emergency_dashboard()
    app.run_server(debug=False, port=port)

if __name__ == "__main__":
    port = 8050
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass
    run_emergency_dashboard(port) 