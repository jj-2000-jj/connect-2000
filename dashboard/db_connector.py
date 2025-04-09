"""
Database connector for the dashboard.
"""
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_dashboard_session():
    """
    Create a database session for the dashboard using the contacts.db database.
    """
    try:
        # Path to the contacts database
        db_path = os.path.join(os.getcwd(), "data", "contacts.db")
        
        # Ensure the database exists
        if not os.path.exists(db_path):
            logger.error(f"Database file not found: {db_path}")
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        # Create engine and session
        engine = create_engine(f"sqlite:///{db_path}", connect_args={'check_same_thread': False})
        Session = sessionmaker(bind=engine)
        session = Session()
        
        logger.info(f"Connected to dashboard database at {db_path}")
        return session
    
    except Exception as e:
        logger.error(f"Error connecting to dashboard database: {e}")
        raise 