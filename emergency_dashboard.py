"""
Emergency dashboard that doesn't depend on any other dashboard components.
This is used when the regular dashboard is having errors.
"""
import dash
from dash import html, dcc
import sqlite3
import pandas as pd
import os
from pathlib import Path

def create_emergency_dashboard():
    """Create a basic emergency dashboard."""
    app = dash.Dash(__name__)
    
    # Get database path from environment variable or use default
    db_path = os.environ.get('DATABASE_PATH', 'data/contact_manager.db')
    if not os.path.exists(db_path):
        # Try to find database in common locations
        possible_paths = [
            'data/contact_manager.db',
            'contact_manager.db',
            '../data/contact_manager.db',
            Path(__file__).parent.parent / 'data' / 'contact_manager.db'
        ]
        for path in possible_paths:
            if os.path.exists(path):
                db_path = path
                break
    
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
            ], style={'width': '100%', 'borderCollapse': 'collapse'}),
            
            html.Div([
                html.H3("Troubleshooting"),
                html.P([
                    "The dashboard error is likely related to callbacks in the dashboard code. ",
                    "Check app/dashboard/dashboard_callbacks.py for functions with argument count mismatches."
                ]),
                html.P([
                    "Common fixes:",
                    html.Ul([
                        html.Li("Add *args parameter to callback functions to handle extra arguments"),
                        html.Li("Check for mismatched Output/Input arguments in callback decorators"),
                        html.Li("Look for circular dependencies in callbacks")
                    ])
                ])
            ], style={'margin': '20px 0', 'padding': '15px', 'backgroundColor': '#eeeeff', 'border': '1px solid #0000ff'})
        ], style={'padding': '20px', 'fontFamily': 'Arial, sans-serif'})
        
    except Exception as e:
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
                    "Check app/dashboard/dashboard_callbacks.py for functions with argument count mismatches."
                ]),
                html.P([
                    "Common fixes:",
                    html.Ul([
                        html.Li("Add *args parameter to callback functions to handle extra arguments"),
                        html.Li("Check for mismatched Output/Input arguments in callback decorators"),
                        html.Li("Look for circular dependencies in callbacks")
                    ])
                ])
            ], style={'margin': '20px 0', 'padding': '15px', 'backgroundColor': '#eeeeff', 'border': '1px solid #0000ff'})
        ], style={'padding': '20px', 'fontFamily': 'Arial, sans-serif'})
    
    return app

def run_emergency_dashboard(port=8050):
    """Run the emergency dashboard."""
    app = create_emergency_dashboard()
    app.run_server(debug=False, port=port)

if __name__ == "__main__":
    run_emergency_dashboard() 