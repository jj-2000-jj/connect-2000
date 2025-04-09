"""
Database migration script for GBL Data Contact Management System.

This script updates the database schema to support the improved organization discovery system,
adding a relationship table between discovered URLs and organizations.
"""
import os
import sys
from pathlib import Path
import sqlite3
import argparse

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.config import DATABASE_PATH
from app.utils.logger import get_logger

logger = get_logger(__name__)

def check_table_exists(conn, table_name):
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    return cursor.fetchone() is not None

def create_relationship_table(conn):
    """Create the discovered_url_organizations relationship table."""
    cursor = conn.cursor()
    
    # Create the relationship table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS discovered_url_organizations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url_id INTEGER NOT NULL,
        organization_id INTEGER NOT NULL,
        date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (url_id) REFERENCES discovered_urls (id),
        FOREIGN KEY (organization_id) REFERENCES organizations (id)
    )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_url_org_url_id ON discovered_url_organizations (url_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_url_org_org_id ON discovered_url_organizations (organization_id)')
    
    conn.commit()
    logger.info("Created discovered_url_organizations table")

def add_discovery_date_column(conn):
    """Add discovery_date column to organizations table if it doesn't exist."""
    cursor = conn.cursor()
    
    # Check if column exists
    cursor.execute("PRAGMA table_info(organizations)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if "discovery_date" not in columns:
        cursor.execute("ALTER TABLE organizations ADD COLUMN discovery_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        conn.commit()
        logger.info("Added discovery_date column to organizations table")

def fix_organization_data_types(conn):
    """
    Fix data types for organization columns to ensure compatibility.
    
    SQLite is dynamically typed, but this ensures proper defaults for new columns.
    """
    cursor = conn.cursor()
    
    # Add relevance_score with default if missing
    cursor.execute("PRAGMA table_info(organizations)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if "relevance_score" not in columns:
        cursor.execute("ALTER TABLE organizations ADD COLUMN relevance_score FLOAT DEFAULT 0.0")
        conn.commit()
        logger.info("Added relevance_score column to organizations table")
    
    # Update any NULL fields with appropriate defaults
    cursor.execute("UPDATE organizations SET relevance_score = 0.0 WHERE relevance_score IS NULL")
    cursor.execute("UPDATE organizations SET confidence_score = 0.0 WHERE confidence_score IS NULL")
    
    conn.commit()
    logger.info("Fixed organization data types")

def establish_initial_relationships(conn):
    """
    Establish initial relationships between existing organizations and URLs.
    
    This connects organizations to the URLs they were discovered from.
    """
    cursor = conn.cursor()
    
    # Get all organizations with source_url
    cursor.execute("SELECT id, source_url FROM organizations WHERE source_url IS NOT NULL")
    organizations = cursor.fetchall()
    
    related_count = 0
    for org_id, source_url in organizations:
        # Find the URL in discovered_urls table
        cursor.execute("SELECT id FROM discovered_urls WHERE url = ?", (source_url,))
        url_record = cursor.fetchone()
        
        if url_record:
            url_id = url_record[0]
            
            # Check if relationship already exists
            cursor.execute(
                "SELECT id FROM discovered_url_organizations WHERE url_id = ? AND organization_id = ?", 
                (url_id, org_id)
            )
            
            if not cursor.fetchone():
                # Create the relationship
                cursor.execute(
                    "INSERT INTO discovered_url_organizations (url_id, organization_id) VALUES (?, ?)",
                    (url_id, org_id)
                )
                related_count += 1
    
    conn.commit()
    logger.info(f"Established {related_count} initial URL-organization relationships")

def fix_discovered_urls_table(conn):
    """
    Fix and update the discovered_urls table to ensure it has the right columns.
    """
    cursor = conn.cursor()
    
    # Check if table exists
    if not check_table_exists(conn, "discovered_urls"):
        # Create the table
        cursor.execute('''
        CREATE TABLE discovered_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER,
            url TEXT NOT NULL,
            page_type TEXT,
            title TEXT,
            description TEXT,
            last_crawled TIMESTAMP,
            crawl_depth INTEGER DEFAULT 0,
            contains_contact_info BOOLEAN DEFAULT 0,
            priority_score FLOAT DEFAULT 0.0,
            FOREIGN KEY (organization_id) REFERENCES organizations (id)
        )
        ''')
        
        logger.info("Created discovered_urls table")
    else:
        # Check if priority_score column exists
        cursor.execute("PRAGMA table_info(discovered_urls)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "priority_score" not in columns:
            cursor.execute("ALTER TABLE discovered_urls ADD COLUMN priority_score FLOAT DEFAULT 0.0")
            conn.commit()
            logger.info("Added priority_score column to discovered_urls table")
    
    # Create index on URL for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_urls_url ON discovered_urls (url)')
    
    conn.commit()
    logger.info("Fixed discovered_urls table")

def run_migration():
    """Run the database migration."""
    logger.info(f"Starting database migration for {DATABASE_PATH}")
    
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    
    try:
        # Connect to the database
        conn = sqlite3.connect(str(DATABASE_PATH))
        
        # Fix discovered_urls table
        fix_discovered_urls_table(conn)
        
        # Create relationship table
        create_relationship_table(conn)
        
        # Add discovery_date column to organizations
        add_discovery_date_column(conn)
        
        # Fix organization data types
        fix_organization_data_types(conn)
        
        # Establish initial relationships
        establish_initial_relationships(conn)
        
        logger.info("Database migration completed successfully")
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return False
    
    finally:
        if conn:
            conn.close()
    
    return True

def main():
    """Main entry point for the migration script."""
    parser = argparse.ArgumentParser(description="GBL Data Database Migration")
    parser.add_argument("--force", action="store_true", help="Force migration even if tables exist")
    
    args = parser.parse_args()
    
    if args.force:
        logger.info("Forcing migration")
    
    success = run_migration()
    
    if success:
        logger.info("Migration completed successfully")
        return 0
    else:
        logger.error("Migration failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
