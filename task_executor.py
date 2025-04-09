import concurrent.futures
import threading
import logging
import uuid
import time
import os
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
# Import models but not the get_db_session function directly
from app.database.models import Contact, Organization

# Configure logging
logger = logging.getLogger("task_executor")

# Global executor and task dict - use max_workers=1 to avoid SQLite threading issues
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
_tasks: Dict[str, Dict[str, Any]] = {}
_tasks_lock = threading.Lock()

# Thread-local storage for database connections
_thread_local = threading.local()

def get_direct_db_connection():
    """
    Get a direct database connection for the current thread.
    This bypasses SQLAlchemy to avoid thread-safety issues with SQLite.
    
    Returns:
        A SQLite connection with check_same_thread=False
    """
    # Create connection if it doesn't exist for this thread
    if not hasattr(_thread_local, 'connection'):
        # Get the database path
        DB_PATH = os.path.join(os.getcwd(), "data", "contacts.db")
        if not os.path.exists(DB_PATH):
            logger.error(f"Database file not found at {DB_PATH}")
            raise FileNotFoundError(f"Database file not found at {DB_PATH}")
            
        # Create a connection with check_same_thread=False to allow usage in threads
        _thread_local.connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        _thread_local.connection.row_factory = sqlite3.Row
        logger.info(f"Created new database connection for thread {threading.current_thread().name}")
        
    return _thread_local.connection

def execute_query(query, params=()):
    """
    Execute a query on the thread-local connection.
    """
    conn = get_direct_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor

def _org_build_worker(target_org_types=None):
    """Worker function for organization building task."""
    from app.discovery.discovery_manager import DiscoveryManager
    
    logger.info(f"Starting org build task with target types: {target_org_types}")
    try:
        # Get a direct connection, rather than using get_db_session
        conn = get_direct_db_connection()
        
        # For now, manually establish a SQLAlchemy session
        # This is a simplified approach - in a real solution, we'd need to:
        # 1. Create a custom session factory that uses the existing connection
        # 2. Or modify DiscoveryManager to accept a direct connection
        
        # For this temporary solution, we'll still use DiscoveryManager but with caution
        # Import here to avoid circular imports
        from app.database.models import get_db_session
        try:
            # Try one more time with a fresh session
            with get_db_session() as db_session:
                discovery_manager = DiscoveryManager(db_session)
                metrics = discovery_manager.run_scheduled_discovery(target_org_types=target_org_types)
                orgs_found = metrics.get('organizations_discovered', 0)
                return {
                    "status": "success",
                    "message": f"Organization building completed. Found {orgs_found} organizations.",
                    "orgs_found": orgs_found,
                    "details": metrics
                }
        except Exception as e:
            logger.error(f"Error with SQLAlchemy session, falling back to direct query: {e}", exc_info=True)
            # As a fallback, try a simple direct query
            cursor = execute_query("SELECT COUNT(*) as count FROM organizations")
            count = cursor.fetchone()[0]
            return {
                "status": "partial_success",
                "message": f"Organization count retrieved via direct connection. Found {count} organizations total.",
                "org_count": count,
                "error": str(e),
                "note": "DiscoveryManager execution failed, only count was retrieved."
            }
            
    except Exception as e:
        logger.error(f"Organization building failed: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Organization building failed: {str(e)}",
            "error": str(e)
        }

def _contact_build_worker(target_org_types=None):
    """Worker function for contact building task."""
    from app.discovery.discovery_manager import DiscoveryManager
    
    logger.info(f"Starting contact build task with target types: {target_org_types}")
    try:
        # Get a direct connection, rather than using get_db_session
        conn = get_direct_db_connection()
        
        # For now, manually establish a SQLAlchemy session
        # Import here to avoid circular imports  
        from app.database.models import get_db_session
        try:
            # Try one more time with a fresh session
            with get_db_session() as db_session:
                discovery_manager = DiscoveryManager(db_session)
                metrics = discovery_manager.run_scheduled_discovery(target_org_types=target_org_types)
                contacts_found = metrics.get('contacts_discovered', 0)
                return {
                    "status": "success",
                    "message": f"Contact building completed. Found {contacts_found} contacts.",
                    "contacts_found": contacts_found,
                    "details": metrics
                }
        except Exception as e:
            logger.error(f"Error with SQLAlchemy session, falling back to direct query: {e}", exc_info=True)
            # As a fallback, try a simple direct query
            cursor = execute_query("SELECT COUNT(*) as count FROM contacts")
            count = cursor.fetchone()[0]
            return {
                "status": "partial_success",
                "message": f"Contact count retrieved via direct connection. Found {count} contacts total.",
                "contact_count": count,
                "error": str(e),
                "note": "DiscoveryManager execution failed, only count was retrieved."
            }
            
    except Exception as e:
        logger.error(f"Contact building failed: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Contact building failed: {str(e)}",
            "error": str(e)
        }

def _email_send_worker(target_org_types=None):
    """Worker function for email sending task."""
    from app.email.manager import EmailManager
    
    logger.info(f"Starting email sending task with target types: {target_org_types}")
    try:
        # Get a direct connection, rather than using get_db_session
        conn = get_direct_db_connection()
        
        # For now, manually establish a SQLAlchemy session
        # Import here to avoid circular imports
        from app.database.models import get_db_session
        try:
            with get_db_session() as db_session:
                email_manager = EmailManager(db_session)
                drafts_created_dict = email_manager.create_draft_emails(
                    target_org_types=target_org_types
                )
                total_drafts = sum(drafts_created_dict.values())
                return {
                    "status": "success",
                    "message": f"Email sending completed. Created {total_drafts} drafts.",
                    "drafts_created": total_drafts,
                    "details": drafts_created_dict
                }
        except Exception as e:
            logger.error(f"Error with SQLAlchemy session, falling back to direct query: {e}", exc_info=True)
            # As a fallback, try a simple direct query to at least provide some information
            cursor = execute_query("SELECT COUNT(*) as count FROM contact_interactions WHERE interaction_type = 'email_draft'")
            count = cursor.fetchone()[0]
            return {
                "status": "partial_success",
                "message": f"Email draft count retrieved via direct connection. Found {count} existing drafts total.",
                "draft_count": count,
                "error": str(e),
                "note": "EmailManager execution failed, only count was retrieved."
            }
            
    except Exception as e:
        logger.error(f"Email sending failed: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"Email sending failed: {str(e)}",
            "error": str(e)
        }

def _task_done_callback(future, task_id):
    """Callback for when a task is done."""
    with _tasks_lock:
        if task_id in _tasks:
            try:
                result = future.result()
                _tasks[task_id].update({
                    "status": result.get("status", "unknown"),
                    "result": result,
                    "completed_at": datetime.now().isoformat(),
                })
                logger.info(f"Task {task_id} completed with status: {result.get('status')}")
            except Exception as e:
                _tasks[task_id].update({
                    "status": "error",
                    "result": {"error": str(e)},
                    "completed_at": datetime.now().isoformat(),
                })
                logger.error(f"Task {task_id} failed: {e}", exc_info=True)

def submit_task(task_type, params=None):
    """
    Submit a task to be executed in the background.
    
    Args:
        task_type: Type of task ('org_build', 'contact_build', or 'email_send')
        params: Dictionary of parameters for the task
        
    Returns:
        Task ID that can be used to check the status
    """
    task_id = str(uuid.uuid4())
    params = params or {}
    
    # Determine which worker function to use
    if task_type == 'org_build':
        worker_fn = _org_build_worker
    elif task_type == 'contact_build':
        worker_fn = _contact_build_worker
    elif task_type == 'email_send':
        worker_fn = _email_send_worker
    else:
        raise ValueError(f"Unknown task type: {task_type}")
    
    # Submit the task
    future = _executor.submit(worker_fn, **params)
    future.add_done_callback(lambda f: _task_done_callback(f, task_id))
    
    # Store task information
    with _tasks_lock:
        _tasks[task_id] = {
            "id": task_id,
            "type": task_type,
            "params": params,
            "status": "running",
            "submitted_at": datetime.now().isoformat(),
            "completed_at": None,
            "result": None
        }
    
    logger.info(f"Submitted {task_type} task with ID {task_id}")
    return task_id

def get_task_status(task_id):
    """
    Get the status of a task.
    
    Args:
        task_id: ID of the task
        
    Returns:
        Task information dictionary or None if task not found
    """
    with _tasks_lock:
        return _tasks.get(task_id)

def get_recent_tasks(limit=10):
    """
    Get recent tasks.
    
    Args:
        limit: Maximum number of tasks to return
        
    Returns:
        List of task dictionaries
    """
    with _tasks_lock:
        # Sort tasks by submitted_at (newest first)
        sorted_tasks = sorted(
            _tasks.values(), 
            key=lambda t: t.get("submitted_at", ""), 
            reverse=True
        )
        return sorted_tasks[:limit]

def cleanup_old_tasks(max_age_hours=24):
    """
    Clean up old completed tasks to prevent memory leaks.
    
    Args:
        max_age_hours: Maximum age in hours for tasks to keep
    """
    with _tasks_lock:
        now = datetime.now()
        keys_to_remove = []
        
        for task_id, task in _tasks.items():
            # Only clean up completed tasks
            if task.get("completed_at"):
                completed_at = datetime.fromisoformat(task["completed_at"])
                age_hours = (now - completed_at).total_seconds() / 3600
                
                if age_hours > max_age_hours:
                    keys_to_remove.append(task_id)
        
        for task_id in keys_to_remove:
            del _tasks[task_id]
        
        return len(keys_to_remove)

# Run cleanup periodically
def _cleanup_thread():
    while True:
        try:
            time.sleep(3600)  # Run once per hour
            count = cleanup_old_tasks()
            if count > 0:
                logger.info(f"Cleaned up {count} old tasks")
        except Exception as e:
            logger.error(f"Error in cleanup thread: {e}")

# Start cleanup thread
threading.Thread(target=_cleanup_thread, daemon=True).start()

# Clean up thread-local connections when tasks finish
def _close_connections():
    """Close any open thread-local database connections."""
    if hasattr(_thread_local, 'connection'):
        try:
            _thread_local.connection.close()
            logger.debug(f"Closed database connection for thread {threading.current_thread().name}")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        finally:
            delattr(_thread_local, 'connection')

# Register an exit handler to close connections
import atexit
atexit.register(_close_connections) 