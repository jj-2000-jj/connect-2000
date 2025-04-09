"""
Callbacks for the dashboard.
"""
from dash import Dash, Input, Output, State, callback_context, dcc, html
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, cast, Float, inspect
from datetime import datetime, timedelta
import logging
import os
import json
import subprocess
import sys
import functools

from app.database.models import Organization, Contact, EmailEngagement, ValidationRule, ValidationReport, ProcessSummary, Base
from app.database import crud

logger = logging.getLogger(__name__)

def register_callbacks(app: Dash, db_session: Session):
    """Register all callbacks for the dashboard app."""

    @app.callback(
        Output('settings-store', 'data'),
        [
            Input('org-confidence-slider', 'value'),
            Input('name-confidence-slider', 'value')
        ],
        [State('settings-store', 'data')],
        prevent_initial_call=True
    )
    def update_settings_store(org_hurdle, name_hurdle, current_data):
        """Update the settings store when confidence hurdles change."""
        if current_data is None:
            current_data = {
                'org_confidence_hurdle': 0.7,
                'name_confidence_hurdle': 0.7
            }
        
        triggered_id = callback_context.triggered_id
        
        updated_data = current_data.copy()

        if triggered_id == 'org-confidence-slider':
            updated_data['org_confidence_hurdle'] = org_hurdle
            logger.info(f"Updated org confidence hurdle to: {org_hurdle}")
        elif triggered_id == 'name-confidence-slider':
            updated_data['name_confidence_hurdle'] = name_hurdle
            logger.info(f"Updated name confidence hurdle to: {name_hurdle}")
        
        return updated_data

    @app.callback(
        Output("run-app-status", "children"),
        [Input("run-app-btn", "n_clicks")],
        [
            State("settings-app-mode-selector", "value"),
            State("target-org-types", "value"),
            State("target-states", "value"),
            State("email-limit", "value"),
            State("max-orgs", "value"),
            State("engineering-assignment", "value"),
            State("government-assignment", "value"),
            State("municipal-assignment", "value"),
            State("water-assignment", "value"),
            State("utility-assignment", "value"),
            State("transportation-assignment", "value"),
            State("oil_gas-assignment", "value"),
            State("agriculture-assignment", "value"),
            State("settings-store", "data")
        ]
    )
    def run_application(n_clicks, app_mode, org_types, states, email_limit, max_orgs,
                          engineering_email, government_email, municipal_email, water_email,
                          utility_email, transportation_email, oil_gas_email, agriculture_email,
                          settings_data):
        if not n_clicks:
             return ""
        
        if settings_data is None:
             settings_data = {
                 'org_confidence_hurdle': 0.7, 
                 'name_confidence_hurdle': 0.7
             }
        org_confidence_hurdle = settings_data.get('org_confidence_hurdle', 0.7)
        name_confidence_hurdle = settings_data.get('name_confidence_hurdle', 0.7)

        mode = app_mode or "org_building"
        org_type = org_types or "all"
        state = states or "all"
        email_lim = email_limit or 100
        max_org = max_orgs or 1000
        
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
            config_dir = 'app/config'
            os.makedirs(config_dir, exist_ok=True)
            assignments_path = os.path.join(config_dir, 'email_assignments.json')
            with open(assignments_path, 'w') as f:
                json.dump(email_assignments, f, indent=2)
            logger.info(f"Email assignments saved to {assignments_path}")
        except Exception as e:
            logger.error(f"Error saving email assignments: {e}")

        try:
            validation_settings = {
                "org_confidence_hurdle": org_confidence_hurdle,
                "name_confidence_hurdle": name_confidence_hurdle
            }
            config_dir = 'app/config'
            os.makedirs(config_dir, exist_ok=True)
            settings_path = os.path.join(config_dir, 'validation_settings.json')
            with open(settings_path, 'w') as f:
                json.dump(validation_settings, f, indent=2)
            logger.info(f"Validation settings saved to {settings_path}: {validation_settings}")
        except Exception as e:
            logger.error(f"Error saving validation settings: {e}")

        # Save email limit to a separate config file
        try:
            email_settings = {
                "email_daily_limit_per_user": email_lim
            }
            config_dir = 'app/config'
            os.makedirs(config_dir, exist_ok=True)
            email_settings_path = os.path.join(config_dir, 'email_settings.json')
            with open(email_settings_path, 'w') as f:
                json.dump(email_settings, f, indent=2)
            logger.info(f"Email settings saved to {email_settings_path}: {email_settings}")
        except Exception as e:
            logger.error(f"Error saving email settings: {e}")

        logger.info(f"Run application clicked with mode: {app_mode}")
        return f"Application run triggered in mode: {app_mode}"

    @app.callback(
        Output('process-summaries-table', 'data'),
        [
            Input('filter-process-type', 'value'),
            Input('process-time-range', 'value'),
            Input('refresh-process-summaries', 'n_clicks'),
            Input('interval-component', 'n_intervals')
        ]
    )
    def update_process_summaries_table(process_type, time_range, n_clicks, n_intervals):
        """Update the process summaries table based on filters."""
        logger.info(f"Updating process summaries table: type={process_type}, time_range={time_range}")
        
        try:
            # Get current time for filtering
            now = datetime.now()
            
            # Define time filter based on selected range
            if time_range == '24h':
                time_filter = now - timedelta(hours=24)
            elif time_range == '7d':
                time_filter = now - timedelta(days=7)
            elif time_range == '30d':
                time_filter = now - timedelta(days=30)
            else:
                # 'all' - no time filter
                time_filter = datetime(2000, 1, 1)  # Just a very old date
            
            # Ensure process summary table exists
            from app.database.models import Base, ProcessSummary, get_db_session
            
            # Get a fresh database connection
            session = get_db_session()
            
            try:
                # Check if ProcessSummary table exists
                inspector = inspect(session.bind)
                if not inspector.has_table("process_summaries"):
                    logger.warning("ProcessSummary table doesn't exist - creating it")
                    Base.metadata.create_all(session.bind)
                
                # Get count of records for debugging
                count = session.query(ProcessSummary).count()
                logger.info(f"Found {count} total process summary records in database")
                
                # Query using the fresh session
                query = session.query(ProcessSummary)
                
                # Apply process type filter if not 'all'
                if process_type != 'all':
                    query = query.filter(ProcessSummary.process_type == process_type)
                
                # Apply time filter
                query = query.filter(ProcessSummary.started_at >= time_filter)
                
                # Order by most recent first
                query = query.order_by(desc(ProcessSummary.started_at))
                
                # Execute query
                summaries = query.all()
                logger.info(f"Loaded {len(summaries)} process summaries after filtering")
                
                # Convert to list of dictionaries for the table
                table_data = []
                for summary in summaries:
                    # Handle potential None values safely
                    started_at = summary.started_at.strftime('%Y-%m-%d %H:%M:%S') if summary.started_at else ''
                    completed_at = summary.completed_at.strftime('%Y-%m-%d %H:%M:%S') if summary.completed_at else ''
                    
                    logger.debug(f"Processing summary: {summary.id}, {summary.process_type}, {summary.status}")
                    
                    table_data.append({
                        'id': summary.id,
                        'process_type_display': format_process_type(summary.process_type),
                        'process_type': summary.process_type,
                        'started_at': started_at,
                        'completed_at': completed_at,
                        'status': summary.status or '',
                        'items_processed': summary.items_processed or 0,
                        'items_added': summary.items_added or 0,
                        'has_details': 1 if summary.details else 0
                    })
                
                session.close()
                return table_data
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error updating process summaries table: {e}")
            import traceback
            traceback.print_exc()
            # Return empty data on error
            return []

    @app.callback(
        Output('process-details', 'children'),
        [Input('process-summaries-table', 'active_cell')],
        [State('process-summaries-table', 'data')]
    )
    def display_process_details(active_cell, table_data):
        """Display details for the selected process summary."""
        if not active_cell or not table_data:
            return html.Div("Click on a process to see details")
        
        try:
            # Get the row data for the clicked cell
            row_id = active_cell['row']
            row_data = table_data[row_id]
            
            # Log what we're looking at
            logger.info(f"Getting details for process summary {row_data['id']}")
            
            # If no details available, show a message
            if row_data.get('has_details', 0) == 0:
                return html.Div("No detailed information available for this process")
            
            # Get a fresh database connection
            from app.database.models import ProcessSummary, get_db_session
            session = get_db_session()
            
            try:
                # Fetch the process summary from the database
                summary = session.query(ProcessSummary).filter(ProcessSummary.id == row_data['id']).first()
                
                if not summary or not summary.details:
                    return html.Div("No detailed information available for this process")
                
                # Extract details from the JSON
                try:
                    logger.info(f"Parsing details JSON from summary {summary.id}")
                    details = json.loads(summary.details)
                    logger.debug(f"Details keys: {list(details.keys() if details else [])}")
                except json.JSONDecodeError:
                    logger.error(f"Error parsing JSON: {summary.details[:100]}")
                    return html.Div("Error parsing process details data")
                
                # Create a details display based on process type
                if summary.process_type == 'org_building':
                    # For organization building, show list of added organizations
                    orgs_added = details.get('organizations_added', [])
                    
                    if not orgs_added:
                        return html.Div("No organizations were added in this process")
                    
                    children = [
                        html.H5(f"{len(orgs_added)} Organizations Added:"),
                        html.Table([
                            html.Thead(html.Tr([
                                html.Th("Name"), html.Th("Type"), html.Th("Website")
                            ])),
                            html.Tbody([
                                html.Tr([
                                    html.Td(org.get('name', '')),
                                    html.Td(org.get('org_type', '')),
                                    html.Td(org.get('website', ''))
                                ]) for org in orgs_added[:20]  # Limit to first 20
                            ])
                        ], className="details-table")
                    ]
                    
                    if len(orgs_added) > 20:
                        children.append(html.Div(f"...and {len(orgs_added) - 20} more"))
                        
                    return html.Div(children)
                    
                elif summary.process_type == 'contact_building':
                    # For contact building, show list of added contacts
                    contacts_added = details.get('contacts_added', [])
                    
                    if not contacts_added:
                        return html.Div("No contacts were added in this process")
                    
                    children = [
                        html.H5(f"{len(contacts_added)} Contacts Added:"),
                        html.Table([
                            html.Thead(html.Tr([
                                html.Th("Name"), html.Th("Email"), html.Th("Organization")
                            ])),
                            html.Tbody([
                                html.Tr([
                                    html.Td(f"{contact.get('first_name', '')} {contact.get('last_name', '')}"),
                                    html.Td(contact.get('email', '')),
                                    html.Td(contact.get('organization_name', ''))
                                ]) for contact in contacts_added[:20]  # Limit to first 20
                            ])
                        ], className="details-table")
                    ]
                    
                    if len(contacts_added) > 20:
                        children.append(html.Div(f"...and {len(contacts_added) - 20} more"))
                        
                    return html.Div(children)
                    
                elif summary.process_type == 'email_sending':
                    # For email sending, show list of sent emails
                    emails_sent = details.get('emails_sent', [])
                    emails_drafted = details.get('emails_drafted', [])
                    
                    children = []
                    
                    if emails_sent:
                        children.extend([
                            html.H5(f"{len(emails_sent)} Emails Sent:"),
                            html.Table([
                                html.Thead(html.Tr([
                                    html.Th("To"), html.Th("Subject"), html.Th("Sent At")
                                ])),
                                html.Tbody([
                                    html.Tr([
                                        html.Td(email.get('to_email', '')),
                                        html.Td(email.get('subject', '')),
                                        html.Td(email.get('sent_at', ''))
                                    ]) for email in emails_sent[:10]  # Limit to first 10
                                ])
                            ], className="details-table")
                        ])
                        
                        if len(emails_sent) > 10:
                            children.append(html.Div(f"...and {len(emails_sent) - 10} more"))
                    
                    if emails_drafted:
                        children.extend([
                            html.H5(f"{len(emails_drafted)} Emails Drafted:"),
                            html.Table([
                                html.Thead(html.Tr([
                                    html.Th("To"), html.Th("Subject"), html.Th("Created At")
                                ])),
                                html.Tbody([
                                    html.Tr([
                                        html.Td(email.get('to_email', '')),
                                        html.Td(email.get('subject', '')),
                                        html.Td(email.get('created_at', ''))
                                    ]) for email in emails_drafted[:10]  # Limit to first 10
                                ])
                            ], className="details-table")
                        ])
                        
                        if len(emails_drafted) > 10:
                            children.append(html.Div(f"...and {len(emails_drafted) - 10} more"))
                    
                    if not emails_sent and not emails_drafted:
                        return html.Div("No emails were sent or drafted in this process")
                        
                    return html.Div(children)
                
                # For other process types or if details structure is unknown
                return html.Div("Details format not recognized for this process type")
            finally:
                session.close()
            
    except Exception as e:
            logger.error(f"Error displaying process details: {e}")
            import traceback
            traceback.print_exc()
            return html.Div(f"Error displaying process details: {str(e)}")

# Helper functions for formatting display values
def format_process_type(process_type):
    """Format process type for display."""
    if process_type == 'org_building':
        return 'Organization Building'
    elif process_type == 'contact_building':
        return 'Contact Building'
    elif process_type == 'email_sending':
        return 'Email Sending'
    else:
        return process_type.replace('_', ' ').title()