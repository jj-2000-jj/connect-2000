"""
Email engagement tracking module for the GDS Contact Management System.

This module provides functionality to track email engagement metrics:
- Email opens
- Email replies
- Link clicks
- Conversions
"""
import datetime
import time
import re
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func
import requests

from app.database.models import Contact, EmailEngagement, ContactEngagementScore
from app.email.microsoft365 import Microsoft365Client
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Constants for engagement scoring
OPEN_SCORE = 10          # Base score for opening an email
REPLY_SCORE = 30         # Base score for replying to an email
CLICK_SCORE = 20         # Base score for clicking a link
CONVERSION_SCORE = 50    # Base score for conversion (meeting, call)
RECENCY_DECAY = 0.9      # Score decay factor per day since last interaction
FREQUENCY_FACTOR = 1.2   # Multiplier for frequent interactions


class EngagementTracker:
    """Class for tracking and scoring email engagement."""
    
    def __init__(self, db_session: Session):
        """
        Initialize the engagement tracker.
        
        Args:
            db_session: Database session
        """
        self.db_session = db_session
        self.ms365_client = Microsoft365Client()
        self.is_setup = False
        logger.info("EngagementTracker initialized")
    
    def setup(self) -> bool:
        """
        Set up the engagement tracker by initializing the MS365 client.
        
        Returns:
            True if setup was successful, False otherwise
        """
        if not self.ms365_client.setup():
            logger.error("Failed to set up Microsoft365 client for engagement tracking")
            return False
            
        if not self.ms365_client.authenticate():
            logger.error("Failed to authenticate with Microsoft 365 API")
            return False
            
        self.is_setup = True
        logger.info("EngagementTracker set up successfully")
        return True
    
    def track_engagements(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Track email engagements for recent emails.
        
        Args:
            days_back: Number of days back to track (default 30)
            
        Returns:
            Dict with engagement metrics summary
        """
        if not self.is_setup and not self.setup():
            logger.error("Failed to set up engagement tracker")
            return {"error": "Setup failed"}
        
        # Find emails sent in the given time period
        start_date = datetime.datetime.utcnow() - datetime.timedelta(days=days_back)
        
        # Get the engagements to track
        engagements = self.db_session.query(EmailEngagement).filter(
            EmailEngagement.email_sent_date >= start_date
        ).all()
        
        if not engagements:
            logger.info(f"No email engagements found in the last {days_back} days")
            return {"emails_processed": 0}
        
        metrics = {
            "emails_processed": 0,
            "new_opens": 0,
            "new_replies": 0,
            "new_clicks": 0,
            "new_conversions": 0
        }
        
        for engagement in engagements:
            # Track the email open/reply status
            email_id = engagement.email_id
            if not email_id:
                continue
                
            # Check email status via Microsoft 365 API
            updates = self._check_email_status(email_id)
            
            if updates:
                metrics["emails_processed"] += 1
                
                # Apply updates
                if updates.get("opened") and not engagement.email_opened:
                    engagement.email_opened = True
                    engagement.email_opened_date = updates.get("opened_date")
                    engagement.email_opened_count += 1
                    metrics["new_opens"] += 1
                
                if updates.get("replied") and not engagement.email_replied:
                    engagement.email_replied = True
                    engagement.email_replied_date = updates.get("replied_date")
                    metrics["new_replies"] += 1
                
                if updates.get("clicked") and not engagement.clicked_link:
                    engagement.clicked_link = True
                    engagement.clicked_link_date = updates.get("clicked_date")
                    engagement.clicked_link_count += 1
                    metrics["new_clicks"] += 1
                
                engagement.last_tracked = datetime.datetime.utcnow()
                self.db_session.commit()
        
        # Now update engagement scores based on the new data
        self._calculate_engagement_scores()
        
        return metrics
    
    def record_conversion(self, contact_id: int, conversion_type: str) -> bool:
        """
        Record a conversion event (meeting scheduled, call completed, etc.)
        
        Args:
            contact_id: ID of the contact
            conversion_type: Type of conversion (meeting, call, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Find the most recent engagement for this contact
            engagement = self.db_session.query(EmailEngagement).filter(
                EmailEngagement.contact_id == contact_id
            ).order_by(EmailEngagement.email_sent_date.desc()).first()
            
            if not engagement:
                logger.error(f"No engagement found for contact ID {contact_id}")
                return False
            
            # Record the conversion
            engagement.converted = True
            engagement.conversion_date = datetime.datetime.utcnow()
            engagement.conversion_type = conversion_type
            self.db_session.commit()
            
            # Update the engagement score
            self._calculate_single_engagement_score(contact_id)
            
            logger.info(f"Recorded {conversion_type} conversion for contact ID {contact_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error recording conversion: {e}")
            return False
    
    def _check_email_status(self, email_id: str) -> Optional[Dict[str, Any]]:
        """
        Check the status of an email using Microsoft 365 API.
        
        Args:
            email_id: Microsoft 365 email ID
            
        Returns:
            Dict with status updates or None if no updates
        """
        if not self.is_setup:
            return None
        
        try:
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {self.ms365_client.token}",
                "Content-Type": "application/json"
            }
            
            # Get the message
            user_email = self.ms365_client.user_email
            response = requests.get(
                f"https://graph.microsoft.com/v1.0/users/{user_email}/messages/{email_id}",
                headers=headers
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get email status: {response.status_code}")
                return None
            
            email_data = response.json()
            updates = {}
            
            # Check if email was opened (this requires read receipts)
            # In a real implementation, you would add tracking pixels or use
            # a service like Mailchimp, SendGrid, etc.
            if email_data.get("isRead"):
                updates["opened"] = True
                updates["opened_date"] = datetime.datetime.utcnow()
            
            # Check for replies
            if self._check_for_replies(email_id, headers, user_email):
                updates["replied"] = True
                updates["replied_date"] = datetime.datetime.utcnow()
            
            # Check for link clicks (would normally use a tracking service)
            # This is a placeholder - link tracking requires URL rewriting services
            updates["clicked"] = False
            
            return updates if updates else None
            
        except Exception as e:
            logger.error(f"Error checking email status: {e}")
            return None
    
    def _check_for_replies(self, email_id: str, headers: Dict[str, str], user_email: str = None) -> bool:
        """
        Check if an email has received any replies.
        
        Args:
            email_id: Microsoft 365 email ID
            headers: Request headers
            user_email: User email address (optional, defaults to ms365_client.user_email)
            
        Returns:
            True if reply found, False otherwise
        """
        try:
            if user_email is None:
                user_email = self.ms365_client.user_email
                
            # Search for replies using the conversation ID
            response = requests.get(
                f"https://graph.microsoft.com/v1.0/users/{user_email}/messages/{email_id}?$select=conversationId",
                headers=headers
            )
            
            if response.status_code != 200:
                return False
            
            conversation_id = response.json().get("conversationId")
            if not conversation_id:
                return False
            
            # Now search for messages in this conversation
            response = requests.get(
                f"https://graph.microsoft.com/v1.0/users/{user_email}/messages?$filter=conversationId eq '{conversation_id}'&$select=id,subject",
                headers=headers
            )
            
            if response.status_code != 200:
                return False
            
            # Check if there are multiple messages in the conversation (implies replies)
            messages = response.json().get("value", [])
            return len(messages) > 1
            
        except Exception as e:
            logger.error(f"Error checking for replies: {e}")
            return False
    
    def _calculate_engagement_scores(self):
        """
        Calculate engagement scores for all contacts with recent engagement.
        """
        try:
            # Get all contacts with engagement data
            contacts = self.db_session.query(Contact).join(
                EmailEngagement, Contact.id == EmailEngagement.contact_id
            ).distinct().all()
            
            for contact in contacts:
                self._calculate_single_engagement_score(contact.id)
                
            logger.info(f"Updated engagement scores for {len(contacts)} contacts")
            
        except Exception as e:
            logger.error(f"Error calculating engagement scores: {e}")
    
    def _calculate_single_engagement_score(self, contact_id: int):
        """
        Calculate engagement score for a single contact.
        
        Args:
            contact_id: ID of the contact
        """
        try:
            # Get all engagements for this contact, ordered by date
            engagements = self.db_session.query(EmailEngagement).filter(
                EmailEngagement.contact_id == contact_id
            ).order_by(EmailEngagement.email_sent_date.desc()).all()
            
            if not engagements:
                return
            
            # Calculate base scores
            base_score = 0
            recency_score = 0
            frequency_score = 0
            depth_score = 0
            conversion_score = 0
            
            # Calculate days since most recent engagement
            most_recent = engagements[0]
            now = datetime.datetime.utcnow()
            
            # Calculate the most recent interaction date (any type)
            interaction_dates = []
            if most_recent.email_opened_date:
                interaction_dates.append(most_recent.email_opened_date)
            if most_recent.email_replied_date:
                interaction_dates.append(most_recent.email_replied_date)
            if most_recent.clicked_link_date:
                interaction_dates.append(most_recent.clicked_link_date)
            if most_recent.conversion_date:
                interaction_dates.append(most_recent.conversion_date)
            
            if interaction_dates:
                most_recent_date = max(interaction_dates)
                days_since = (now - most_recent_date).days
                recency_score = 100 * (RECENCY_DECAY ** days_since)
            
            # Calculate frequency (interactions per month)
            total_interactions = 0
            total_opens = 0
            total_replies = 0
            total_clicks = 0
            total_conversions = 0
            
            for eng in engagements:
                if eng.email_opened:
                    total_interactions += 1
                    total_opens += 1
                    base_score += OPEN_SCORE
                
                if eng.email_replied:
                    total_interactions += 1
                    total_replies += 1
                    base_score += REPLY_SCORE
                
                if eng.clicked_link:
                    total_interactions += 1
                    total_clicks += 1
                    base_score += CLICK_SCORE
                
                if eng.converted:
                    total_interactions += 1
                    total_conversions += 1
                    base_score += CONVERSION_SCORE
            
            # Calculate timespan of engagements in days
            if len(engagements) > 1:
                first_date = engagements[-1].email_sent_date
                last_date = engagements[0].email_sent_date
                
                if first_date and last_date:
                    days_span = max(1, (last_date - first_date).days)
                    monthly_interaction_rate = total_interactions * 30 / days_span
                    frequency_score = min(100, monthly_interaction_rate * 20)  # Scale to 0-100
            
            # Calculate depth score
            if total_opens > 0:
                # Depth is measured by what percentage of opens led to deeper engagement
                deeper_engagement_rate = (total_replies + total_clicks + total_conversions) / total_opens
                depth_score = min(100, deeper_engagement_rate * 100)
            
            # Calculate conversion score
            if total_interactions > 0:
                conversion_rate = total_conversions / total_interactions
                conversion_score = min(100, conversion_rate * 100)
            
            # Calculate final engagement score (weighted average)
            engagement_score = (
                base_score * 0.2 +
                recency_score * 0.3 +
                frequency_score * 0.2 +
                depth_score * 0.15 +
                conversion_score * 0.15
            )
            
            # Cap at 100
            engagement_score = min(100, engagement_score)
            
            # Update or create engagement score record
            score_record = self.db_session.query(ContactEngagementScore).filter(
                ContactEngagementScore.contact_id == contact_id
            ).first()
            
            if score_record:
                score_record.engagement_score = engagement_score
                score_record.recency_score = recency_score
                score_record.frequency_score = frequency_score
                score_record.depth_score = depth_score
                score_record.conversion_score = conversion_score
                score_record.last_calculated = now
            else:
                score_record = ContactEngagementScore(
                    contact_id=contact_id,
                    engagement_score=engagement_score,
                    recency_score=recency_score,
                    frequency_score=frequency_score,
                    depth_score=depth_score,
                    conversion_score=conversion_score,
                    last_calculated=now
                )
                self.db_session.add(score_record)
            
            self.db_session.commit()
            
        except Exception as e:
            logger.error(f"Error calculating score for contact {contact_id}: {e}")
            self.db_session.rollback()
    
    def get_engagement_statistics(self) -> Dict[str, Any]:
        """
        Get engagement statistics across all contacts.
        
        Returns:
            Dictionary with engagement statistics
        """
        try:
            stats = {
                "total_emails_sent": 0,
                "open_rate": 0,
                "reply_rate": 0,
                "click_rate": 0,
                "conversion_rate": 0,
                "average_engagement_score": 0,
                "top_engaged_contacts": [],
                "by_org_type": {}
            }
            
            # Get total emails sent
            stats["total_emails_sent"] = self.db_session.query(EmailEngagement).count()
            
            if stats["total_emails_sent"] == 0:
                return stats
                
            # Calculate rates
            opened = self.db_session.query(func.count(EmailEngagement.id)).filter(
                EmailEngagement.email_opened == True
            ).scalar()
            
            replied = self.db_session.query(func.count(EmailEngagement.id)).filter(
                EmailEngagement.email_replied == True
            ).scalar()
            
            clicked = self.db_session.query(func.count(EmailEngagement.id)).filter(
                EmailEngagement.clicked_link == True
            ).scalar()
            
            converted = self.db_session.query(func.count(EmailEngagement.id)).filter(
                EmailEngagement.converted == True
            ).scalar()
            
            stats["open_rate"] = (opened / stats["total_emails_sent"]) * 100 if stats["total_emails_sent"] > 0 else 0
            stats["reply_rate"] = (replied / stats["total_emails_sent"]) * 100 if stats["total_emails_sent"] > 0 else 0
            stats["click_rate"] = (clicked / stats["total_emails_sent"]) * 100 if stats["total_emails_sent"] > 0 else 0
            stats["conversion_rate"] = (converted / stats["total_emails_sent"]) * 100 if stats["total_emails_sent"] > 0 else 0
            
            # Get average engagement score
            avg_score = self.db_session.query(func.avg(ContactEngagementScore.engagement_score)).scalar()
            stats["average_engagement_score"] = avg_score or 0
            
            # Get top engaged contacts (top 10)
            top_contacts = self.db_session.query(
                Contact, ContactEngagementScore
            ).join(
                ContactEngagementScore, Contact.id == ContactEngagementScore.contact_id
            ).order_by(
                ContactEngagementScore.engagement_score.desc()
            ).limit(10).all()
            
            stats["top_engaged_contacts"] = [
                {
                    "id": contact.id,
                    "name": f"{contact.first_name} {contact.last_name}".strip(),
                    "email": contact.email,
                    "organization_id": contact.organization_id,
                    "engagement_score": score.engagement_score
                }
                for contact, score in top_contacts
            ]
            
            # Get stats by organization type
            from app.database.models import Organization
            org_types = self.db_session.query(Organization.org_type).distinct().all()
            
            for org_type in org_types:
                org_type = org_type[0]
                
                # Count emails sent to this org type
                sent_count = self.db_session.query(func.count(EmailEngagement.id)).join(
                    Contact, EmailEngagement.contact_id == Contact.id
                ).join(
                    Organization, Contact.organization_id == Organization.id
                ).filter(
                    Organization.org_type == org_type
                ).scalar()
                
                if sent_count == 0:
                    continue
                
                # Count opens, replies, etc. for this org type
                opened_count = self.db_session.query(func.count(EmailEngagement.id)).join(
                    Contact, EmailEngagement.contact_id == Contact.id
                ).join(
                    Organization, Contact.organization_id == Organization.id
                ).filter(
                    Organization.org_type == org_type,
                    EmailEngagement.email_opened == True
                ).scalar()
                
                replied_count = self.db_session.query(func.count(EmailEngagement.id)).join(
                    Contact, EmailEngagement.contact_id == Contact.id
                ).join(
                    Organization, Contact.organization_id == Organization.id
                ).filter(
                    Organization.org_type == org_type,
                    EmailEngagement.email_replied == True
                ).scalar()
                
                converted_count = self.db_session.query(func.count(EmailEngagement.id)).join(
                    Contact, EmailEngagement.contact_id == Contact.id
                ).join(
                    Organization, Contact.organization_id == Organization.id
                ).filter(
                    Organization.org_type == org_type,
                    EmailEngagement.converted == True
                ).scalar()
                
                # Calculate rates for this org type
                stats["by_org_type"][org_type] = {
                    "emails_sent": sent_count,
                    "open_rate": (opened_count / sent_count) * 100,
                    "reply_rate": (replied_count / sent_count) * 100,
                    "conversion_rate": (converted_count / sent_count) * 100
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting engagement statistics: {e}")
            return {"error": str(e)} 