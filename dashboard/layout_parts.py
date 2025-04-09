"""
Reusable layout components for the dashboard, separated to avoid circular imports.
"""

import dash_bootstrap_components as dbc
from dash import dcc, html

# --- Constants needed by create_settings_tab ---
# App modes definition moved here to break circular dependency
APP_MODES = [
    {'label': 'Organization Building', 'value': 'org_building'},
    {'label': 'Contact Building', 'value': 'contact_building'},
    {'label': 'Sending Emails', 'value': 'sending_emails'}
]

# Organization types for assignment
ORG_TYPES = [
    {'label': 'Engineering Firms', 'value': 'engineering'},
    {'label': 'Government Agencies', 'value': 'government'},
    {'label': 'Municipal Organizations', 'value': 'municipal'},
    {'label': 'Water Management', 'value': 'water'},
    {'label': 'Utility Companies', 'value': 'utility'},
    {'label': 'Transportation', 'value': 'transportation'},
    {'label': 'Oil & Gas', 'value': 'oil_gas'},
    {'label': 'Agriculture', 'value': 'agriculture'}
]

# Salespeople options
SALESPEOPLE = [
    {'label': 'Marc Perkins', 'value': 'marc@gbl-data.com'},
    {'label': 'Tim Swietek', 'value': 'tim@gbl-data.com'},
    {'label': 'Jared Rasmussen', 'value': 'jared@gbl-data.com'},

]
# --- End Constants --- 

# --- Storage for Settings --- 
def create_settings_store():
    """Create a dcc.Store component to hold dashboard settings like confidence hurdles."""
    return dcc.Store(id='settings-store', storage_type='session', data={
        'org_confidence_hurdle': 0.7, # Default value
        'name_confidence_hurdle': 0.7  # Default value
    })

# --- Confidence Hurdle Controls ---
def create_confidence_hurdle_controls():
    """Create sliders for setting Gemini validation confidence hurdles."""
    return dbc.Card([
        dbc.CardHeader("Gemini Validation Settings"),
        dbc.CardBody([
            html.Div([
                html.Label("Organization/URL Confidence Hurdle:"),
                dcc.Slider(
                    id='org-confidence-slider',
                    min=0.0,
                    max=1.0,
                    step=0.05,
                    value=0.7, # Default value
                    marks={i/10: f'{i/10:.1f}' for i in range(0, 11)},
                    tooltip={"placement": "bottom", "always_visible": True}
                )
            ], style={'marginBottom': '20px'}),
            html.Div([
                html.Label("Name Confidence Hurdle:"),
                dcc.Slider(
                    id='name-confidence-slider',
                    min=0.0,
                    max=1.0,
                    step=0.05,
                    value=0.7, # Default value
                    marks={i/10: f'{i/10:.1f}' for i in range(0, 11)},
                    tooltip={"placement": "bottom", "always_visible": True}
                )
            ])
        ])
    ], className="mb-3") 

# --- Settings Tab --- 
def create_settings_tab():
    """Create the settings tab content."""
    # DO NOT import from dashboard_components here - that creates circular imports
    
    return html.Div([
        # --- Add Gemini Validation Settings Card --- 
        create_confidence_hurdle_controls(), # Call the local function
        # --- End Gemini Validation Settings Card --- 

        # App mode selector with improved styling
        html.Div([
            html.Div([
                html.H3("Application Mode", style={"marginBottom": "15px", "color": "#2c3e50"}),
                dcc.Dropdown(
                    id='settings-app-mode-selector',
                    options=APP_MODES, # Use local definition
                    value=APP_MODES[0]['value'],
                    clearable=False,
                    style={"fontSize": "16px"}
                )
            ], style={
                "border": "1px solid #ddd",
                "borderRadius": "5px",
                "padding": "20px",
                "backgroundColor": "#f9f9f9",
                "marginBottom": "20px",
                "width": "100%"
            })
        ], className="setting-item", style={"marginBottom": "25px"}),
        
        # Target organization types
        html.Div([
            html.Label("Target Organization Types"),
            dcc.Dropdown(
                id='target-org-types',
                options=[
                    {'label': 'All Types', 'value': 'all'},
                    {'label': 'Engineering Firms', 'value': 'engineering'},
                    {'label': 'Government Agencies', 'value': 'government'},
                    {'label': 'Municipal Organizations', 'value': 'municipal'},
                    {'label': 'Water Management', 'value': 'water'},
                    {'label': 'Utility Companies', 'value': 'utility'},
                    {'label': 'Transportation', 'value': 'transportation'},
                    {'label': 'Oil & Gas', 'value': 'oil_gas'},
                    {'label': 'Agriculture', 'value': 'agriculture'}
                ],
                value='all',
                multi=False
            )
        ], id="target-org-types-container", className="setting-item"),
        
        # Target states
        html.Div([
            html.Label("Target States"),
            dcc.Dropdown(
                id='target-states',
                options=[{'label': 'All States', 'value': 'all'}],
                value='all',
                multi=False
            )
        ], id="target-states-container", className="setting-item"),
        
        # Email assignment settings with EXTREME visibility
        html.Div([
            # SUPER VISIBLE TITLE
            html.H2("EMAIL ASSIGNMENT SETTINGS", style={
                "marginBottom": "20px", 
                "color": "#FF0000",  # Bright red
                "fontSize": "28px",
                "fontWeight": "bold",
                "textAlign": "center",
                "backgroundColor": "#FFFF00",  # Yellow background
                "padding": "15px",
                "borderRadius": "10px",
                "border": "3px solid #000"
            }),
            html.P("Assign salespeople to different organization types.", 
                   style={"marginBottom": "20px", "color": "#000", "fontSize": "18px", "textAlign": "center", "fontWeight": "bold"}),
            
            # Engineering assignment - super visible
            html.Div([
                html.Label("Engineering Firms", style={"fontWeight": "bold", "fontSize": "18px"}),
                dcc.Dropdown(
                    id='engineering-assignment',
                    options=SALESPEOPLE,
                    value=SALESPEOPLE[0]['value'],
                    clearable=False,
                    style={"width": "100%", "fontSize": "16px"}
                )
            ], className="assignment-item", style={"marginBottom": "20px", "padding": "15px", "backgroundColor": "#e6f7ff", "borderRadius": "10px", "border": "2px solid #1890ff"}),
            
            # Government assignment - super visible
            html.Div([
                html.Label("Government Agencies", style={"fontWeight": "bold", "fontSize": "18px"}),
                dcc.Dropdown(
                    id='government-assignment',
                    options=SALESPEOPLE,
                    value=SALESPEOPLE[1]['value'],
                    clearable=False,
                    style={"width": "100%", "fontSize": "16px"}
                )
            ], className="assignment-item", style={"marginBottom": "20px", "padding": "15px", "backgroundColor": "#e6f7ff", "borderRadius": "10px", "border": "2px solid #1890ff"}),
            
            # Municipal assignment - super visible
            html.Div([
                html.Label("Municipal Organizations", style={"fontWeight": "bold", "fontSize": "18px"}),
                dcc.Dropdown(
                    id='municipal-assignment',
                    options=SALESPEOPLE,
                    value=SALESPEOPLE[1]['value'],
                    clearable=False,
                    style={"width": "100%", "fontSize": "16px"}
                )
            ], className="assignment-item", style={"marginBottom": "20px", "padding": "15px", "backgroundColor": "#e6f7ff", "borderRadius": "10px", "border": "2px solid #1890ff"}),
            
            # Water management assignment - super visible
            html.Div([
                html.Label("Water Management", style={"fontWeight": "bold", "fontSize": "18px"}),
                dcc.Dropdown(
                    id='water-assignment',
                    options=SALESPEOPLE,
                    value=SALESPEOPLE[0]['value'],
                    clearable=False,
                    style={"width": "100%", "fontSize": "16px"}
                )
            ], className="assignment-item", style={"marginBottom": "20px", "padding": "15px", "backgroundColor": "#e6f7ff", "borderRadius": "10px", "border": "2px solid #1890ff"}),
            
            # Utility assignment - super visible
            html.Div([
                html.Label("Utility Companies", style={"fontWeight": "bold", "fontSize": "18px"}),
                dcc.Dropdown(
                    id='utility-assignment',
                    options=SALESPEOPLE,
                    value=SALESPEOPLE[0]['value'],
                    clearable=False,
                    style={"width": "100%", "fontSize": "16px"}
                )
            ], className="assignment-item", style={"marginBottom": "20px", "padding": "15px", "backgroundColor": "#e6f7ff", "borderRadius": "10px", "border": "2px solid #1890ff"}),
            
            # Transportation assignment - super visible
            html.Div([
                html.Label("Transportation", style={"fontWeight": "bold", "fontSize": "18px"}),
                dcc.Dropdown(
                    id='transportation-assignment',
                    options=SALESPEOPLE,
                    value=SALESPEOPLE[1]['value'],
                    clearable=False,
                    style={"width": "100%", "fontSize": "16px"}
                )
            ], className="assignment-item", style={"marginBottom": "20px", "padding": "15px", "backgroundColor": "#e6f7ff", "borderRadius": "10px", "border": "2px solid #1890ff"}),
            
            # Oil & Gas assignment - super visible
            html.Div([
                html.Label("Oil & Gas", style={"fontWeight": "bold", "fontSize": "18px"}),
                dcc.Dropdown(
                    id='oil_gas-assignment',
                    options=SALESPEOPLE,
                    value=SALESPEOPLE[1]['value'],
                    clearable=False,
                    style={"width": "100%", "fontSize": "16px"}
                )
            ], className="assignment-item", style={"marginBottom": "20px", "padding": "15px", "backgroundColor": "#e6f7ff", "borderRadius": "10px", "border": "2px solid #1890ff"}),
            
            # Agriculture assignment - super visible
            html.Div([
                html.Label("Agriculture", style={"fontWeight": "bold", "fontSize": "18px"}),
                dcc.Dropdown(
                    id='agriculture-assignment',
                    options=SALESPEOPLE,
                    value=SALESPEOPLE[1]['value'],
                    clearable=False,
                    style={"width": "100%", "fontSize": "16px"}
                )
            ], className="assignment-item", style={"padding": "15px", "backgroundColor": "#e6f7ff", "borderRadius": "10px", "border": "2px solid #1890ff"})

        ], className="setting-item", style={
            "border": "3px solid #FF0000", # Red border
            "borderRadius": "10px",
            "padding": "30px",
            "backgroundColor": "#f8f8f8",
            "marginBottom": "30px",
            "boxShadow": "0 4px 8px rgba(0,0,0,0.2)" # Added shadow for emphasis
        }),
        
        # Email limit
        html.Div([
            html.Label("Email Daily Limit per User"),
            dcc.Input(
                id='email-limit',
                type='number',
                value=100,
                min=1,
                max=1000
            )
        ], className="setting-item"),
        
        # Max organizations
        html.Div([
            html.Label("Maximum Organizations"),
            dcc.Input(
                id='max-orgs',
                type='number',
                value=1000,
                min=1,
                max=10000
            )
        ], className="setting-item"),
        
        # Run button section - SUPER VISIBLE
        html.Div([
            html.Div([
                html.H2("RUN APPLICATION", style={
                    "marginBottom": "20px", 
                    "color": "#FFFFFF",
                    "fontSize": "28px",
                    "fontWeight": "bold",
                    "textAlign": "center",
                    "backgroundColor": "#28a745",
                    "padding": "15px",
                    "borderRadius": "10px"
                }),
                html.Button(
                    "RUN APPLICATION",
                    id="run-app-btn",
                    className="btn btn-success",
                    style={
                        "backgroundColor": "#28a745",
                        "color": "white",
                        "border": "none",
                        "padding": "20px 40px",
                        "fontSize": "24px",
                        "fontWeight": "bold",
                        "borderRadius": "10px",
                        "cursor": "pointer",
                        "display": "block",
                        "margin": "0 auto 20px auto",
                        "boxShadow": "0 4px 8px rgba(0,0,0,0.2)"
                    }
                ),
                html.Div(id="run-app-status", style={"marginTop": "20px", "fontSize": "18px", "textAlign": "center"})
            ], style={
                "border": "3px solid #28a745",
                "borderRadius": "10px",
                "padding": "30px",
                "backgroundColor": "#f0fff0",
                "boxShadow": "0 4px 8px rgba(0,0,0,0.2)"
            })
        ], className="setting-item run-container", style={"marginTop": "40px"})
    ]) 
