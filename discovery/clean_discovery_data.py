"""
Data cleanup script for GBL Data Contact Management System.

This script analyses existing discovery data and cleans it by:
1. Detecting and merging duplicate organizations
2. Identifying webpage "organizations" and extracting real organizations from them
3. Updating organization relevance and confidence scores
4. Linking contacts to correct organizations

Run this script after the database migration to clean up existing data.
"""
import os
import sys
from pathlib import Path
import sqlite3
import re
import time
import argparse
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.config import DATABASE_PATH, GEMINI_API_KEY, ORG_TYPES, TARGET_STATES
from app.utils.logger import get_logger
import google.generativeai as genai

# Initialize Gemini API
genai.configure(api_key=GEMINI_API_KEY)

logger = get_logger(__name__)

def connect_to_database():
    """Connect to the SQLite database."""
    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    return conn

def get_all_organizations(conn):
    """Get all organizations from the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM organizations ORDER BY id")
    return cursor.fetchall()

def is_webpage_title(name):
    """
    Determine if a name looks like a webpage title rather than an organization name.
    
    Returns:
        bool: True if it looks like a webpage title
    """
    # Common webpage title patterns
    webpage_patterns = [
        r'^\d+ ',  # Starts with numbers (list items)
        r' - .*?$',  # Contains " - Something" at the end
        r'^[A-Z][a-z]+ [A-Z][a-z]+ \| ',  # Format like "Proper Title | "
        r'^(Achieving|Understanding|How to|Why|What|When)',  # Starts with certain words
        r'\b(vs|versus)\b',  # Contains "vs" or "versus"
        r'\d{1,2}/\d{1,2}/\d{2,4}',  # Contains dates
        r'\[[^\]]+\]',  # Contains [brackets]
        r'\(\d{4}\)',  # Contains (YEAR)
        r'^\w+ \d+, \d{4}',  # Starts with date format
        r'^\w+: .+',  # Starts with "Something: "
        r'^The \d+ ',  # Starts with "The Number"
        r' - YouTube$| - Wikipedia$| - LinkedIn$',  # Common website suffixes
    ]
    
    for pattern in webpage_patterns:
        if re.search(pattern, name):
            return True
    
    # Check for very long names (likely descriptions, not org names)
    if len(name.split()) > 10:
        return True
    
    return False

def extract_real_org_from_title(conn, org_row):
    """
    Use Gemini API to extract a real organization name from a webpage title.
    
    Args:
        conn: Database connection
        org_row: Organization row data
        
    Returns:
        dict: Extracted organization data or None if not found
    """
    cursor = conn.cursor()
    
    # Get the article content from the source URL if available
    article_content = ""
    if org_row['source_url']:
        cursor.execute(
            "SELECT description FROM discovered_urls WHERE url = ?", 
            (org_row['source_url'],)
        )
        url_data = cursor.fetchone()
        if url_data and url_data['description']:
            article_content = url_data['description']
    
    # Prepare the prompt
    prompt = f"""
    The following appears to be a webpage title, not a real organization name: "{org_row['name']}"
    
    Organization type: {org_row['org_type']}
    State: {org_row['state']}
    Description: {org_row['description'] or ''}
    
    Context from the article: {article_content}
    
    Extract the actual organization name that is mentioned or referenced in this content.
    Focus on identifying real organizations in the {org_row['org_type']} industry in {org_row['state'] or 'any state'}.
    
    Look for municipalities, water districts, engineering firms, utilities, or other entities that might need SCADA integration services.
    
    If a specific organization name is found, return:
    {{
        "name": "Official Organization Name",
        "confidence": 0.0-1.0 (confidence that this is the correct organization),
        "state": "State abbreviation if known, otherwise null",
        "org_type": "water", "municipal", "engineering", etc. (based on the organization type),
        "description": "Brief description of the organization"
    }}
    
    If no clear organization can be identified, return:
    {{
        "name": null,
        "confidence": 0.0
    }}
    """
    
    try:
        # Call Gemini API
        model = genai.GenerativeModel('gemini-2.0-flash')
        response = model.generate_content(prompt)
        
        # Process response text and extract JSON
        import json
        import re
        
        response_text = response.text
        
        # Look for JSON structure in the response
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            try:
                result = json.loads(json_str)
                
                # Check if a valid organization was found
                if result.get('name') and result.get('confidence', 0) > 0.7:
                    return result
                
                return None
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON response: {json_str}")
                return None
        
        logger.warning(f"No JSON found in response for org ID {org_row['id']}")
        return None
    
    except Exception as e:
        logger.error(f"Error calling Gemini API for org ID {org_row['id']}: {e}")
        return None

def create_or_update_organization(conn, org_data, source_org_id=None):
    """
    Create a new organization or update an existing one.
    
    Args:
        conn: Database connection
        org_data: Organization data dictionary
        source_org_id: Original organization ID if this is an extraction
        
    Returns:
        int: Organization ID
    """
    cursor = conn.cursor()
    
    # Check if organization exists by name and state
    cursor.execute(
        "SELECT id FROM organizations WHERE name = ? AND state = ?",
        (org_data.get('name'), org_data.get('state'))
    )
    existing = cursor.fetchone()
    
    if existing:
        # Update existing organization
        org_id = existing['id']
        cursor.execute(
            """
            UPDATE organizations 
            SET 
                confidence_score = MAX(confidence_score, ?),
                relevance_score = MAX(relevance_score, ?),
                description = CASE 
                    WHEN (description IS NULL OR description = '') THEN ? 
                    ELSE description 
                END
            WHERE id = ?
            """,
            (
                org_data.get('confidence', 0.7),
                org_data.get('relevance_score', 7.0),
                org_data.get('description', ''),
                org_id
            )
        )
    else:
        # Create new organization
        cursor.execute(
            """
            INSERT INTO organizations (
                name, org_type, state, description, 
                confidence_score, relevance_score, discovery_method,
                source_url, date_added
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                org_data.get('name'),
                org_data.get('org_type', 'water'),
                org_data.get('state'),
                org_data.get('description', ''),
                org_data.get('confidence', 0.7),
                org_data.get('relevance_score', 7.0),
                'data_cleanup',
                org_data.get('source_url'),
                datetime.utcnow()
            )
        )
        org_id = cursor.lastrowid
    
    # If this organization was extracted from another, create a relationship
    if source_org_id:
        # Get the source URL
        cursor.execute("SELECT source_url FROM organizations WHERE id = ?", (source_org_id,))
        source_row = cursor.fetchone()
        
        if source_row and source_row['source_url']:
            # Find the URL record
            cursor.execute("SELECT id FROM discovered_urls WHERE url = ?", (source_row['source_url'],))
            url_row = cursor.fetchone()
            
            if url_row:
                # Create relationship to URL
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO discovered_url_organizations (
                        url_id, organization_id
                    ) VALUES (?, ?)
                    """,
                    (url_row['id'], org_id)
                )
    
    conn.commit()
    return org_id

def update_contacts_organization(conn, old_org_id, new_org_id):
    """
    Move contacts from one organization to another.
    
    Args:
        conn: Database connection
        old_org_id: Original organization ID
        new_org_id: New organization ID
    """
    cursor = conn.cursor()
    
    # Update contacts
    cursor.execute(
        "UPDATE contacts SET organization_id = ? WHERE organization_id = ?",
        (new_org_id, old_org_id)
    )
    
    rows_updated = cursor.rowcount
    conn.commit()
    
    return rows_updated

def process_organizations(conn, dry_run=False):
    """
    Process all organizations to identify and fix webpage titles.
    
    Args:
        conn: Database connection
        dry_run: If True, don't make any changes
        
    Returns:
        dict: Processing statistics
    """
    cursor = conn.cursor()
    stats = {
        "total_orgs": 0,
        "webpage_titles": 0,
        "fixed_orgs": 0,
        "moved_contacts": 0,
        "skipped": 0
    }
    
    # Get all organizations
    organizations = get_all_organizations(conn)
    stats["total_orgs"] = len(organizations)
    
    logger.info(f"Processing {stats['total_orgs']} organizations")
    
    for org in organizations:
        # Check if it looks like a webpage title
        if is_webpage_title(org['name']):
            stats["webpage_titles"] += 1
            logger.info(f"Found webpage title: {org['name']} (ID: {org['id']})")
            
            # Extract real organization
            real_org = extract_real_org_from_title(conn, org)
            
            if real_org and real_org.get('name'):
                logger.info(f"Extracted organization: {real_org['name']} (confidence: {real_org.get('confidence', 0.0)})")
                
                # Add source URL
                real_org['source_url'] = org['source_url']
                
                if not dry_run:
                    # Create or update the real organization
                    new_org_id = create_or_update_organization(conn, real_org, org['id'])
                    
                    # Move contacts to the new organization
                    moved = update_contacts_organization(conn, org['id'], new_org_id)
                    stats["moved_contacts"] += moved
                    
                    logger.info(f"Created/updated organization ID {new_org_id} and moved {moved} contacts")
                    stats["fixed_orgs"] += 1
                else:
                    logger.info("Dry run - no changes made")
            else:
                logger.info(f"Could not extract a real organization from: {org['name']}")
                stats["skipped"] += 1
        else:
            # This is probably a real organization name, just set a higher relevance score
            # for organizations with verified state in TARGET_STATES
            if org['state'] in TARGET_STATES and not dry_run:
                cursor.execute(
                    "UPDATE organizations SET relevance_score = MAX(relevance_score, 8.0) WHERE id = ?",
                    (org['id'],)
                )
                conn.commit()
    
    return stats

def clean_duplicate_organizations(conn, dry_run=False):
    """
    Find and merge duplicate organizations.
    
    Args:
        conn: Database connection
        dry_run: If True, don't make any changes
        
    Returns:
        dict: Processing statistics
    """
    cursor = conn.cursor()
    stats = {
        "duplicates_found": 0,
        "duplicates_merged": 0,
        "contacts_reassigned": 0
    }
    
    logger.info("Looking for duplicate organizations...")
    
    # Find potential duplicates based on similar names in the same state
    cursor.execute("""
    SELECT o1.id as id1, o1.name as name1, o1.state, o1.org_type,
           o2.id as id2, o2.name as name2
    FROM organizations o1
    JOIN organizations o2 ON 
        o1.id < o2.id AND
        o1.state = o2.state AND
        o1.org_type = o2.org_type
    WHERE (
        o1.name = o2.name OR
        o1.name LIKE o2.name || '%' OR
        o2.name LIKE o1.name || '%' OR
        ? < o1.name || ' ' || o2.name
    )
    """, (0.8,))  # Using a similarity threshold
    
    potential_duplicates = cursor.fetchall()
    stats["duplicates_found"] = len(potential_duplicates)
    
    for dup in potential_duplicates:
        logger.info(f"Potential duplicate: {dup['name1']} (ID: {dup['id1']}) and {dup['name2']} (ID: {dup['id2']})")
        
        # Get organization details
        cursor.execute("SELECT * FROM organizations WHERE id IN (?, ?)", (dup['id1'], dup['id2']))
        orgs = cursor.fetchall()
        
        if len(orgs) != 2:
            continue
            
        # Determine which organization to keep (higher relevance score or confidence)
        keep_org = orgs[0] if orgs[0]['relevance_score'] >= orgs[1]['relevance_score'] else orgs[1]
        merge_org = orgs[1] if keep_org == orgs[0] else orgs[0]
        
        logger.info(f"Keeping: {keep_org['name']} (ID: {keep_org['id']}) and merging: {merge_org['name']} (ID: {merge_org['id']})")
        
        if not dry_run:
            # Move contacts from merge_org to keep_org
            moved = update_contacts_organization(conn, merge_org['id'], keep_org['id'])
            stats["contacts_reassigned"] += moved
            
            # Update the kept organization with combined description if needed
            if merge_org['description'] and (not keep_org['description'] or len(merge_org['description']) > len(keep_org['description'])):
                cursor.execute(
                    "UPDATE organizations SET description = ? WHERE id = ?",
                    (merge_org['description'], keep_org['id'])
                )
            
            # Set a flag on the merged organization to indicate it's been merged
            cursor.execute(
                "UPDATE organizations SET relevance_score = -1, description = 'MERGED into ID ' || ? WHERE id = ?",
                (keep_org['id'], merge_org['id'])
            )
            
            conn.commit()
            stats["duplicates_merged"] += 1
            
            logger.info(f"Merged organization {merge_org['id']} into {keep_org['id']} and moved {moved} contacts")
        else:
            logger.info("Dry run - no changes made")
    
    return stats

def recalculate_relevance_scores(conn, dry_run=False):
    """
    Recalculate relevance scores for all organizations.
    
    Args:
        conn: Database connection
        dry_run: If True, don't make any changes
        
    Returns:
        int: Number of organizations updated
    """
    cursor = conn.cursor()
    updated = 0
    
    logger.info("Recalculating organization relevance scores...")
    
    # Get all valid organizations (not merged)
    cursor.execute("SELECT * FROM organizations WHERE relevance_score >= 0")
    organizations = cursor.fetchall()
    
    for org in organizations:
        # Base score on several factors
        new_score = 5.0  # Default middle score
        
        # Higher score for organizations in target states
        if org['state'] in TARGET_STATES:
            new_score += 2.0
        
        # Higher score for water-related organizations
        if org['org_type'] == 'water':
            new_score += 1.0
        
        # Higher score for organizations with contacts
        cursor.execute("SELECT COUNT(*) as count FROM contacts WHERE organization_id = ?", (org['id'],))
        contact_count = cursor.fetchone()['count']
        
        if contact_count > 0:
            new_score += min(2.0, contact_count * 0.5)  # Up to 2 points for contacts
        
        # Adjust based on description quality
        if org['description'] and len(org['description']) > 100:
            new_score += 1.0
        
        # Cap score at 10
        new_score = min(10.0, new_score)
        
        if not dry_run and new_score != org['relevance_score']:
            cursor.execute(
                "UPDATE organizations SET relevance_score = ? WHERE id = ?",
                (new_score, org['id'])
            )
            updated += 1
    
    if not dry_run:
        conn.commit()
    
    logger.info(f"Updated relevance scores for {updated} organizations")
    return updated

def main():
    """Main entry point for the data cleanup script."""
    parser = argparse.ArgumentParser(description="GBL Data Discovery Data Cleanup")
    parser.add_argument("--dry-run", action="store_true", help="Don't make any changes, just report what would be done")
    parser.add_argument("--skip-extraction", action="store_true", help="Skip extracting organizations from webpage titles")
    parser.add_argument("--skip-duplicates", action="store_true", help="Skip duplicate organization detection")
    parser.add_argument("--skip-scores", action="store_true", help="Skip relevance score recalculation")
    
    args = parser.parse_args()
    
    if args.dry_run:
        logger.info("Running in dry run mode - no changes will be made")
    
    # Connect to the database
    conn = connect_to_database()
    
    try:
        # Process organizations to fix webpage titles
        if not args.skip_extraction:
            logger.info("Step 1: Processing organizations to extract real entities from webpage titles")
            stats = process_organizations(conn, args.dry_run)
            logger.info(f"Processing complete: {stats}")
        
        # Clean duplicate organizations
        if not args.skip_duplicates:
            logger.info("Step 2: Finding and merging duplicate organizations")
            dup_stats = clean_duplicate_organizations(conn, args.dry_run)
            logger.info(f"Duplicate processing complete: {dup_stats}")
        
        # Recalculate relevance scores
        if not args.skip_scores:
            logger.info("Step 3: Recalculating organization relevance scores")
            updated = recalculate_relevance_scores(conn, args.dry_run)
            logger.info(f"Updated scores for {updated} organizations")
        
        logger.info("Data cleanup complete")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()