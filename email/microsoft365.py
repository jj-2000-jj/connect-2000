"""
Microsoft 365 API client for Connect-Tron-2000.

This module provides functionality to interact with Microsoft 365 API:
- Authentication and token management
- Email operations
- Subscription handling
"""
import os
import time
import json
import logging
import requests
import msal
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from threading import Lock

from app.utils.logger import get_logger

logger = get_logger(__name__)

# Token cache to persist across instances
TOKEN_CACHE = {}
TOKEN_CACHE_LOCK = Lock()

class Microsoft365Client:
    """Client for interacting with Microsoft 365 API."""
    
    def __init__(self):
        """Initialize the Microsoft 365 client."""
        self.client_id = os.environ.get("MICROSOFT_CLIENT_ID")
        self.client_secret = os.environ.get("MICROSOFT_CLIENT_SECRET")
        self.tenant_id = os.environ.get("MICROSOFT_TENANT_ID", "6b08c568-648f-4eca-b20f-2ab3d6f6506e")
        self.user_email = os.environ.get("MICROSOFT_USER_EMAIL", "tim@gbl-data.com")
        
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.scopes = ["https://graph.microsoft.com/.default"]
        self.token = None
        self.token_expiry = None
        self.app = None
        self.is_setup = False
        
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
    def setup(self) -> bool:
        """
        Set up the Microsoft 365 client.
        
        Returns:
            bool: True if setup was successful, False otherwise
        """
        try:
            if not self.client_id or not self.client_secret:
                logger.warning("Missing Microsoft 365 credentials. Using default/demo credentials.")
                # Use default credentials for testing
                self.client_id = "958193ee-b6b1-427e-81f8-349fabeab77b"
                self.client_secret = os.environ.get("MICROSOFT_CLIENT_SECRET", "demo_secret")
            
            # Initialize MSAL app
            self.app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=self.authority
            )
            
            self.is_setup = True
            logger.info("Microsoft 365 client set up successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set up Microsoft 365 client: {e}", exc_info=True)
            return False
            
    def authenticate(self) -> bool:
        """
        Authenticate with Microsoft 365 API using client credentials flow.
        
        Returns:
            bool: True if authentication was successful, False otherwise
        """
        if not self.is_setup and not self.setup():
            return False
            
        try:
            # Check if we have a valid cached token
            with TOKEN_CACHE_LOCK:
                if self.client_id in TOKEN_CACHE and datetime.now() < TOKEN_CACHE[self.client_id].get("expiry", datetime.min):
                    self.token = TOKEN_CACHE[self.client_id]["token"]
                    self.token_expiry = TOKEN_CACHE[self.client_id]["expiry"]
                    logger.debug("Using cached token")
                    return True
            
            # Get token
            result = self.app.acquire_token_for_client(scopes=self.scopes)
            
            if "access_token" in result:
                self.token = result["access_token"]
                # Set token expiry (subtract 5 minutes for safety margin)
                self.token_expiry = datetime.now() + timedelta(seconds=result.get("expires_in", 3600) - 300)
                
                # Cache token
                with TOKEN_CACHE_LOCK:
                    TOKEN_CACHE[self.client_id] = {
                        "token": self.token,
                        "expiry": self.token_expiry
                    }
                
                logger.info("Successfully authenticated with Microsoft 365 API")
                return True
            else:
                error_description = result.get("error_description", "Unknown error")
                logger.error(f"Failed to get token: {error_description}")
                return False
                
        except Exception as e:
            logger.error(f"Error authenticating with Microsoft 365 API: {e}", exc_info=True)
            return False
    
    def ensure_token(self) -> bool:
        """
        Ensure we have a valid token, refreshing if necessary.
        
        Returns:
            bool: True if a valid token is available, False otherwise
        """
        # If no token or token is expired/expiring soon, get a new one
        if not self.token or not self.token_expiry or datetime.now() >= self.token_expiry:
            return self.authenticate()
        return True
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                     params: Optional[Dict] = None, headers: Optional[Dict] = None,
                     retry_count: int = 0) -> Tuple[int, Optional[Dict]]:
        """
        Make an HTTP request to Microsoft 365 API with retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to https://graph.microsoft.com/v1.0/)
            data: Request body for POST/PUT/PATCH
            params: URL parameters
            headers: Additional headers
            retry_count: Current retry attempt
            
        Returns:
            Tuple of (status_code, response_json)
        """
        if not self.ensure_token():
            return 401, None
            
        if not headers:
            headers = {}
            
        headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        })
        
        url = f"https://graph.microsoft.com/v1.0/{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(url, params=params, headers=headers)
            elif method == "POST":
                response = requests.post(url, params=params, json=data, headers=headers)
            elif method == "PATCH":
                response = requests.patch(url, params=params, json=data, headers=headers)
            elif method == "DELETE":
                response = requests.delete(url, params=params, headers=headers)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return 400, None
                
            # Check if we need to retry
            if response.status_code in (401, 429, 500, 502, 503, 504) and retry_count < self.max_retries:
                # If unauthorized, try to get a new token
                if response.status_code == 401:
                    self.authenticate()
                
                # Exponential backoff
                wait_time = self.retry_delay * (2 ** retry_count)
                logger.info(f"Request failed with status {response.status_code}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                
                # Retry the request
                return self._make_request(method, endpoint, data, params, headers, retry_count + 1)
                
            # If we get a successful response with JSON, return it
            if response.status_code < 400:
                try:
                    return response.status_code, response.json()
                except ValueError:
                    return response.status_code, None
                    
            # Handle error responses
            logger.error(f"Microsoft 365 API request failed: {response.status_code} - {response.text}")
            return response.status_code, None
            
        except Exception as e:
            logger.error(f"Error making request to Microsoft 365 API: {e}", exc_info=True)
            return 500, None
    
    def get_email(self, email_id: str) -> Optional[Dict[str, Any]]:
        """
        Get email details from Microsoft 365 API.
        
        Args:
            email_id: Email ID from Microsoft 365 API
            
        Returns:
            Dict with email details or None if not found
        """
        # Use users/{email} endpoint instead of me
        status_code, response = self._make_request("GET", f"users/{self.user_email}/messages/{email_id}")
        
        if status_code == 200 and response:
            return response
        
        return None
        
    def check_email_replied(self, email_id: str) -> bool:
        """
        Check if an email has been replied to.
        
        Args:
            email_id: Email ID from Microsoft 365 API
            
        Returns:
            Boolean indicating if the email has been replied to
        """
        # Get conversation ID first
        status_code, response = self._make_request("GET", f"users/{self.user_email}/messages/{email_id}?$select=conversationId,subject")
        
        if status_code != 200 or not response:
            return False
            
        conversation_id = response.get("conversationId")
        subject = response.get("subject", "")
        
        if not conversation_id:
            return False
            
        # Search for replies in this conversation
        status_code, response = self._make_request(
            "GET", 
            f"users/{self.user_email}/messages",
            params={
                "$filter": f"conversationId eq '{conversation_id}' and subject eq 'RE: {subject}'",
                "$top": 1
            }
        )
        
        if status_code == 200 and response and response.get("value"):
            return len(response.get("value", [])) > 0
            
        return False
        
    def create_subscription(self, notification_url: str, resource: str, 
                          expiration_days: int = 2) -> Optional[Dict[str, Any]]:
        """
        Create a subscription for change notifications.
        
        Args:
            notification_url: URL to receive notifications
            resource: Resource to monitor (e.g., 'me/mailFolders('Inbox')/messages')
            expiration_days: Number of days until subscription expires (max 3)
            
        Returns:
            Dict with subscription details or None if creation failed
        """
        # Calculate expiration date (max 3 days from now)
        expiration_days = min(expiration_days, 3)
        expiration = datetime.now() + timedelta(days=expiration_days)
        expiration_string = expiration.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")
        
        data = {
            "changeType": "created,updated",
            "notificationUrl": notification_url,
            "resource": resource,
            "expirationDateTime": expiration_string,
            "clientState": "Connect-Tron-2000-SecretState"
        }
        
        status_code, response = self._make_request("POST", "subscriptions", data=data)
        
        if status_code == 201 and response:
            logger.info(f"Successfully created subscription: {response.get('id')}")
            return response
        
        logger.error(f"Failed to create subscription: {status_code}")
        return None
        
    def renew_subscription(self, subscription_id: str, 
                         expiration_days: int = 2) -> bool:
        """
        Renew an existing subscription.
        
        Args:
            subscription_id: ID of the subscription to renew
            expiration_days: Number of days until subscription expires (max 3)
            
        Returns:
            Boolean indicating success
        """
        # Calculate new expiration date
        expiration_days = min(expiration_days, 3)
        expiration = datetime.now() + timedelta(days=expiration_days)
        expiration_string = expiration.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")
        
        data = {
            "expirationDateTime": expiration_string
        }
        
        status_code, response = self._make_request("PATCH", f"subscriptions/{subscription_id}", data=data)
        
        if status_code == 200 and response:
            logger.info(f"Successfully renewed subscription: {subscription_id}")
            return True
        
        logger.error(f"Failed to renew subscription: {status_code}")
        return False
        
    def delete_subscription(self, subscription_id: str) -> bool:
        """
        Delete a subscription.
        
        Args:
            subscription_id: ID of the subscription to delete
            
        Returns:
            Boolean indicating success
        """
        status_code, _ = self._make_request("DELETE", f"subscriptions/{subscription_id}")
        
        if status_code == 204:
            logger.info(f"Successfully deleted subscription: {subscription_id}")
            return True
        
        logger.error(f"Failed to delete subscription: {status_code}")
        return False
        
    def get_user_profile(self) -> Optional[Dict[str, Any]]:
        """
        Get current user profile from Microsoft 365 API.
        
        Returns:
            Dict with user profile or None if not found
        """
        status_code, response = self._make_request("GET", f"users/{self.user_email}")
        
        if status_code == 200 and response:
            return response
        
        return None

    def send_email(self, from_email: str, to_email: str, subject: str, body: str) -> Optional[str]:
        """
        Send an email using Microsoft 365 API.
        
        Args:
            from_email: Sender email address
            to_email: Recipient email address
            subject: Email subject
            body: Email body (HTML)
            
        Returns:
            Email ID if successful, None otherwise
        """
        if not self.ensure_token():
            return None
            
        data = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body
                },
                "toRecipients": [
                    {
                        "emailAddress": {
                            "address": to_email
                        }
                    }
                ]
            },
            "saveToSentItems": True
        }
        
        status_code, response = self._make_request("POST", f"users/{self.user_email}/sendMail", data=data)
        
        if status_code == 202:  # Accepted
            logger.info(f"Successfully sent email to {to_email}")
            # Instead of trying to filter by subject and recipient (which can cause filter errors),
            # just get the most recent sent emails and find the matching one
            try:
                # Get the 10 most recent sent emails
                status_code, response = self._make_request(
                    "GET", 
                    f"users/{self.user_email}/mailFolders/sentItems/messages", 
                    params={"$top": 10, "$orderby": "sentDateTime desc"}
                )
                
                # If successful, look through the emails to find our match
                if status_code == 200 and response and response.get("value"):
                    # Look for an email with the same subject and recipient
                    for email in response.get("value", []):
                        if email.get("subject") == subject and any(
                            recipient.get("emailAddress", {}).get("address") == to_email 
                            for recipient in email.get("toRecipients", [])
                        ):
                            return email.get("id")
                    
                    # If we couldn't find an exact match, return the ID of the most recent email
                    # This is a reasonable fallback since we just sent the email
                    return response["value"][0].get("id", "email_sent_but_id_not_found")
                
                return "email_sent_but_id_not_found"
                
            except Exception as e:
                logger.error(f"Error finding sent email: {e}")
                return "email_sent_but_id_not_found"
        
        logger.error(f"Failed to send email: {status_code}")
        return None
        
    def create_draft_email(self, from_email: str, to_email: str, subject: str, body: str) -> Optional[str]:
        """
        Create a draft email using Microsoft 365 API.
        
        Args:
            from_email: Sender email address
            to_email: Recipient email address
            subject: Email subject
            body: Email body (HTML)
            
        Returns:
            Email ID if successful, None otherwise
        """
        if not self.ensure_token():
            return None
            
        # MODIFIED: Ensure to_email is always a valid email address
        if not to_email or "@" not in to_email:
            logger.error(f"Invalid recipient email: {to_email}")
            to_email = "jared@gbl-data.com"  # Default to a valid email in case of errors
            
        # Use saved_to_sent_items flag to ensure drafts are visible
        data = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body
                },
                "toRecipients": [
                    {
                        "emailAddress": {
                            "address": to_email
                        }
                    }
                ],
                "from": {
                    "emailAddress": {
                        "address": from_email
                    }
                }
            },
            "saveToSentItems": False  # Set to False for drafts
        }
        
        # Always use the from_email as the sender's mailbox
        # This ensures drafts appear in the correct user's mailbox
        sending_user = from_email
        
        status_code, response = self._make_request("POST", f"users/{sending_user}/messages", data=data["message"])
        
        if status_code == 201 and response and "id" in response:  # Created
            draft_id = response["id"]
            logger.info(f"Successfully created draft email to {to_email} with ID: {draft_id}")
            
            # Log more details about the draft for debugging
            logger.info(f"Draft details: Subject: {subject}, From: {sending_user}, To: {to_email}")
            return draft_id
        
        logger.error(f"Failed to create draft email: {status_code}")
        return None