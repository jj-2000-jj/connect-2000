#!/usr/bin/env python3
"""
Database migration tools for the Contact Management System.
"""
import os
import sys
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("db_migration")

def get_database_path():
    """Get the path to the SQLite database."""
    # Try to find the database in the current directory structure
    base_dir = os.getcwd()
    db_path = os.path.join(base_dir, "data", "contacts.db")
    
    if not os.path.exists(db_path):
        # Try parent directory
        parent_dir = os.path.dirname(base_dir)
        db_path = os.path.join(parent_dir, "data", "contacts.db")
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found in {base_dir} or {parent_dir}")
    
    return db_path

def execute_query(query, params=(), db_path=None):
    """Execute a query on the database."""
    if db_path is None:
        db_path = get_database_path()
        
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        return cursor
    finally:
        conn.close()

def check_table_exists(table_name, db_path=None):
    """Check if a table exists in the database."""
    if db_path is None:
        db_path = get_database_path()
        
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cursor.fetchone() is not None
    finally:
        conn.close()

def add_email_engagement_tables():
    """
    Add email engagement tables to the database.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        db_path = get_database_path()
        logger.info(f"Using database at {db_path}")
        
        # Check if tables already exist
        if check_table_exists("email_engagements", db_path):
            logger.info("Email engagements table already exists")
            return True
            
        if check_table_exists("contact_engagement_scores", db_path):
            logger.info("Contact engagement scores table already exists")
            return True
        
        # Create email_engagements table
        logger.info("Creating email_engagements table")
        execute_query("""
        CREATE TABLE email_engagements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER NOT NULL,
            email_id VARCHAR(255) NOT NULL,
            email_sent_date DATETIME,
            email_opened BOOLEAN DEFAULT FALSE,
            email_opened_date DATETIME,
            email_opened_count INTEGER DEFAULT 0,
            email_replied BOOLEAN DEFAULT FALSE,
            email_replied_date DATETIME,
            clicked_link BOOLEAN DEFAULT FALSE,
            clicked_link_date DATETIME,
            clicked_link_count INTEGER DEFAULT 0,
            converted BOOLEAN DEFAULT FALSE,
            conversion_date DATETIME,
            conversion_type VARCHAR(50),
            last_tracked DATETIME,
            FOREIGN KEY(contact_id) REFERENCES contacts(id)
        )
        """, db_path=db_path)
        
        # Create contact_engagement_scores table
        logger.info("Creating contact_engagement_scores table")
        execute_query("""
        CREATE TABLE contact_engagement_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER NOT NULL UNIQUE,
            engagement_score FLOAT DEFAULT 0.0,
            recency_score FLOAT DEFAULT 0.0,
            frequency_score FLOAT DEFAULT 0.0,
            depth_score FLOAT DEFAULT 0.0,
            conversion_score FLOAT DEFAULT 0.0,
            last_calculated DATETIME,
            FOREIGN KEY(contact_id) REFERENCES contacts(id)
        )
        """, db_path=db_path)
        
        # Create indexes
        logger.info("Creating indexes")
        execute_query(
            "CREATE INDEX idx_email_engagements_contact_id ON email_engagements(contact_id)",
            db_path=db_path
        )
        execute_query(
            "CREATE INDEX idx_email_engagements_email_id ON email_engagements(email_id)",
            db_path=db_path
        )
        execute_query(
            "CREATE INDEX idx_contact_engagement_scores_contact_id ON contact_engagement_scores(contact_id)",
            db_path=db_path
        )
        
        # Create initial engagements for existing email drafts
        logger.info("Creating initial engagement records for existing email drafts")
        execute_query("""
        INSERT INTO email_engagements (contact_id, email_id, email_sent_date)
        SELECT id, email_draft_id, email_draft_date
        FROM contacts
        WHERE email_draft_id IS NOT NULL AND email_draft_id != ''
        """, db_path=db_path)
        
        logger.info("Database migration completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error adding email engagement tables: {e}")
        return False

def main():
    """Main function."""
    logger.info("Starting database migration")
    
    success = add_email_engagement_tables()
    
    if success:
        logger.info("Database migration completed successfully")
        return 0
    else:
        logger.error("Database migration failed")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 