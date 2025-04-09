"""
Email tracking server for Connect-Tron-2000.

This module provides a Flask server for tracking email opens and link clicks:
- '/track/open/{tracking_id}' - Tracks email opens via pixel
- '/track/click/{tracking_id}' - Tracks link clicks via redirect

To run the server standalone:
$ python -m app.email.tracking_server
"""
import os
import time
import base64
import uuid
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from urllib.parse import urlencode, quote
from flask import Flask, request, redirect, send_file, jsonify, Response

from app.database.models import get_db_session, EmailEngagement, ShortenedURL, Contact
from app.email.engagement_tracker import EngagementTracker
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Global variables
CACHE_TIMEOUT = 60  # seconds to cache tracking results
TRACKING_CACHE = {}  # simple cache for tracking_id -> timestamp
IP_RATE_LIMITS = {}  # Track IP addresses for rate limiting
MAX_REQUESTS = 50    # Maximum requests per IP per minute

# Configure the Flask logger to use our custom logger
app.logger.handlers = logger.handlers
app.logger.setLevel(logger.level)

# Tracking pixel data (1x1 transparent GIF)
TRACKING_PIXEL = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)

@app.before_request
def rate_limit():
    """Rate limit requests by IP address."""
    ip = request.remote_addr
    now = time.time()
    
    # Clear old entries
    for ip_addr in list(IP_RATE_LIMITS.keys()):
        if now - IP_RATE_LIMITS[ip_addr]["timestamp"] > 60:
            del IP_RATE_LIMITS[ip_addr]
    
    # Check/update rate limit
    if ip in IP_RATE_LIMITS:
        IP_RATE_LIMITS[ip]["count"] += 1
        IP_RATE_LIMITS[ip]["timestamp"] = now
        
        if IP_RATE_LIMITS[ip]["count"] > MAX_REQUESTS:
            return jsonify({"error": "Rate limit exceeded"}), 429
    else:
        IP_RATE_LIMITS[ip] = {"count": 1, "timestamp": now}
    
    return None

@app.after_request
def add_security_headers(response):
    """Add security headers to all responses."""
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = "default-src 'self'"
    return response

def log_tracking_event(tracking_type: str, tracking_id: str, user_agent: str, 
                      ip_address: str, referrer: str) -> None:
    """
    Log a tracking event for analysis.
    
    Args:
        tracking_type: Type of tracking event (open, click)
        tracking_id: Tracking ID
        user_agent: User agent string
        ip_address: IP address
        referrer: Referrer URL
    """
    try:
        # Write to tracking log
        tracking_log_dir = os.path.join(os.getcwd(), "logs", "tracking")
        os.makedirs(tracking_log_dir, exist_ok=True)
        
        log_file = os.path.join(tracking_log_dir, 
                              f"tracking_{datetime.now().strftime('%Y%m%d')}.log")
        
        with open(log_file, "a") as f:
            timestamp = datetime.now().isoformat()
            log_data = f"{timestamp}|{tracking_type}|{tracking_id}|{ip_address}|{user_agent}|{referrer}\n"
            f.write(log_data)
    
    except Exception as e:
        logger.error(f"Error logging tracking event: {e}")

def get_device_info(user_agent: str) -> Dict[str, str]:
    """
    Extract basic device information from user agent.
    
    Args:
        user_agent: User agent string
        
    Returns:
        Dict with device information
    """
    device_info = {
        "device_type": "unknown",
        "browser": "unknown",
        "os": "unknown"
    }
    
    # Simple device type detection
    if "Mobile" in user_agent or "Android" in user_agent or "iPhone" in user_agent:
        device_info["device_type"] = "mobile"
    elif "iPad" in user_agent or "Tablet" in user_agent:
        device_info["device_type"] = "tablet"
    else:
        device_info["device_type"] = "desktop"
    
    # Simple browser detection
    browsers = {
        "Chrome": "Chrome",
        "Firefox": "Firefox",
        "Safari": "Safari",
        "Edge": "Edge",
        "MSIE": "Internet Explorer",
        "Trident": "Internet Explorer",
        "Opera": "Opera"
    }
    
    for browser_key, browser_name in browsers.items():
        if browser_key in user_agent:
            device_info["browser"] = browser_name
            break
    
    # Simple OS detection
    os_list = {
        "Windows NT": "Windows",
        "Macintosh": "macOS",
        "Mac OS X": "macOS",
        "Linux": "Linux",
        "Android": "Android",
        "iPhone OS": "iOS",
        "iPad": "iOS"
    }
    
    for os_key, os_name in os_list.items():
        if os_key in user_agent:
            device_info["os"] = os_name
            break
    
    return device_info

def record_tracking_event(tracking_id: str, tracking_type: str) -> Tuple[bool, str]:
    """
    Record a tracking event in the database.
    
    Args:
        tracking_id: ID to identify the email or link
        tracking_type: Type of tracking event (open, click)
        
    Returns:
        Tuple of (success, message)
    """
    # Check cache to prevent duplicate events in quick succession
    cache_key = f"{tracking_type}:{tracking_id}"
    now = time.time()
    
    if cache_key in TRACKING_CACHE:
        if now - TRACKING_CACHE[cache_key] < CACHE_TIMEOUT:
            return True, "Cached"
    
    # Update cache
    TRACKING_CACHE[cache_key] = now
    
    # Record in database
    try:
        with get_db_session() as db_session:
            if tracking_type == "open":
                # For email opens, find the email engagement record
                engagement = db_session.query(EmailEngagement).filter(
                    EmailEngagement.email_id == tracking_id
                ).first()
                
                if engagement:
                    # Update open status
                    engagement.email_opened = True
                    if not engagement.email_opened_date:
                        engagement.email_opened_date = datetime.now()
                    engagement.email_opened_count += 1
                    
                    # Update engagement scores
                    tracker = EngagementTracker(db_session)
                    tracker._calculate_single_engagement_score(engagement.contact_id)
                    
                    db_session.commit()
                    logger.info(f"Recorded email open for tracking ID: {tracking_id}")
                    return True, "Open recorded"
                else:
                    logger.warning(f"Email engagement not found for tracking ID: {tracking_id}")
                    return False, "Email not found"
            
            elif tracking_type == "click":
                # For link clicks, find the shortened URL record
                shortened_url = db_session.query(ShortenedURL).filter(
                    ShortenedURL.short_id == tracking_id
                ).first()
                
                if shortened_url:
                    # Update click count
                    shortened_url.clicks += 1
                    shortened_url.last_clicked = datetime.now()
                    
                    # Check if email engagement record exists and update it
                    if shortened_url.email_id:
                        engagement = db_session.query(EmailEngagement).filter(
                            EmailEngagement.email_id == shortened_url.email_id
                        ).first()
                        
                        if engagement:
                            engagement.clicked_link = True
                            if not engagement.clicked_link_date:
                                engagement.clicked_link_date = datetime.now()
                            engagement.clicked_link_count += 1
                            
                            # Update engagement scores
                            if engagement.contact_id:
                                tracker = EngagementTracker(db_session)
                                tracker._calculate_single_engagement_score(engagement.contact_id)
                    
                    db_session.commit()
                    logger.info(f"Recorded link click for tracking ID: {tracking_id}")
                    return True, "Click recorded"
                else:
                    logger.warning(f"Shortened URL not found for tracking ID: {tracking_id}")
                    return False, "Link not found"
            
            else:
                logger.error(f"Unknown tracking type: {tracking_type}")
                return False, "Unknown tracking type"
    
    except Exception as e:
        logger.error(f"Error recording {tracking_type} for {tracking_id}: {e}", exc_info=True)
        return False, f"Error: {str(e)}"

@app.route('/track/open/<tracking_id>')
def track_open(tracking_id):
    """
    Track email opens via a 1x1 transparent tracking pixel.
    
    Args:
        tracking_id: Email ID to track
        
    Returns:
        1x1 transparent GIF
    """
    # Log event details
    user_agent = request.headers.get('User-Agent', 'Unknown')
    ip_address = request.remote_addr
    referrer = request.headers.get('Referer', 'Unknown')
    
    # Log to tracking log
    log_tracking_event('open', tracking_id, user_agent, ip_address, referrer)
    
    # Record open in the database (in a separate thread to not block response)
    from threading import Thread
    Thread(target=record_tracking_event, args=(tracking_id, 'open')).start()
    
    # Return tracking pixel
    response = Response(TRACKING_PIXEL, mimetype='image/gif')
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@app.route('/track/click/<tracking_id>')
def track_click(tracking_id):
    """
    Track link clicks via URL redirection.
    
    Args:
        tracking_id: Short ID for the URL
        
    Returns:
        Redirect to the target URL
    """
    # Get the original URL
    url = request.args.get('url', '')
    
    # Log event details
    user_agent = request.headers.get('User-Agent', 'Unknown')
    ip_address = request.remote_addr
    referrer = request.headers.get('Referer', 'Unknown')
    
    # Extract device information
    device_info = get_device_info(user_agent)
    
    # Log to tracking log
    log_tracking_event('click', tracking_id, user_agent, ip_address, referrer)
    
    # If no URL provided, try to look it up in the database
    if not url:
        try:
            with get_db_session() as db_session:
                shortened_url = db_session.query(ShortenedURL).filter(
                    ShortenedURL.short_id == tracking_id
                ).first()
                
                if shortened_url:
                    url = shortened_url.original_url
                else:
                    logger.warning(f"Shortened URL not found for tracking ID: {tracking_id}")
                    return jsonify({"error": "Link not found"}), 404
        except Exception as e:
            logger.error(f"Error retrieving URL for {tracking_id}: {e}")
            return jsonify({"error": "Database error"}), 500
    
    # Record click in the database (in a separate thread to not block response)
    from threading import Thread
    Thread(target=record_tracking_event, args=(tracking_id, 'click')).start()
    
    # Validate and redirect to the URL
    if url:
        # Simple URL validation to prevent open redirect vulnerabilities
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        return redirect(url, code=302)
    else:
        return jsonify({"error": "Missing URL parameter"}), 400

@app.route('/track/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "service": "Connect-Tron-2000 Tracking Server"
    })

@app.route('/')
def index():
    """Root endpoint."""
    return jsonify({
        "service": "Connect-Tron-2000 Tracking Server",
        "version": "1.0.0",
        "status": "running"
    })

@app.errorhandler(404)
def page_not_found(e):
    """Handle 404 errors."""
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def server_error(e):
    """Handle 500 errors."""
    logger.error(f"Server error: {e}")
    return jsonify({"error": "Internal server error"}), 500

def run_server(host='127.0.0.1', port=5000, debug=False):
    """
    Run the tracking server.
    
    Args:
        host: Host to bind to
        port: Port to listen on
        debug: Whether to run in debug mode
    """
    logger.info(f"Starting tracking server on {host}:{port}")
    app.run(host=host, port=port, debug=debug)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Email tracking server")
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to listen on')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    
    args = parser.parse_args()
    run_server(host=args.host, port=args.port, debug=args.debug) 