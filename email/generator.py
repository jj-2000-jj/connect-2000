"""
Email template generator using Google Gemini 1.5 Pro API.
"""
import google.generativeai as genai
import json
from typing import Dict, Any
from app.config import GEMINI_API_KEY
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Move configuration to an explicit function instead of module level
# try:
#     genai.configure(api_key=GEMINI_API_KEY)
#     logger.info("Gemini API configured successfully for email generator")
# except Exception as e:
#     logger.error(f"Failed to configure Gemini API: {e}")


class EmailGenerator:
    """Class for generating email templates."""
    
    def __init__(self):
        """Initialize the email generator."""
        self.is_setup = True
    
    def setup(self):
        """
        Set up the email generator.
        
        Returns:
            True (always successful now with simple templates)
        """
        self.is_setup = True
        return True
    
    def generate_email(self, contact_data: Dict[str, Any], org_data: Dict[str, Any] = None, template_type: str = None) -> str:
        """
        Generate a templated email for a contact.
        
        Args:
            contact_data: Contact data including name and job title (can be Contact object or dict)
            org_data: Organization data including name, type, and location (can be Organization object or dict)
            template_type: Optional type of template to use (e.g., 'no_name' for contacts without names)
            
        Returns:
            HTML email body
        """
        return self._generate_email(contact_data, template_type)
    
    def _generate_email(self, contact_data, template_type=None):
        """
        Generate an email based on contact data.
        
        Args:
            contact_data: Contact data
            template_type: Optional type of template to use (e.g., 'no_name' for contacts without names)
            
        Returns:
            HTML email
        """
        # Convert Contact object to dictionary if needed
        if not isinstance(contact_data, dict):
            contact_dict = {}
            # Extract common attributes from Contact object
            for attr in ['id', 'first_name', 'last_name', 'job_title', 'email', 'assigned_to']:
                if hasattr(contact_data, attr):
                    contact_dict[attr] = getattr(contact_data, attr)
            contact_data = contact_dict
            
        # Get contact name
        first_name = contact_data.get('first_name', '')
        last_name = contact_data.get('last_name', '')
        full_name = f"{first_name} {last_name}".strip()
        
        # Determine if we have a verified name
        # Force no-name template if template_type is 'no_name'
        if template_type == 'no_name':
            has_valid_name = False
        else:
            has_valid_name = first_name and len(first_name) > 0
        
        # Determine sender based on assignment
        sender_email = contact_data.get("assigned_to", "marc@gbl-data.com")
        if sender_email == "marc@gbl-data.com":
            sender_name = "Marc Perkins"
            sender_title = "Sales Manager"
            phone_office = "480.461.3401"
            phone_mobile = "602.758.3374"
        elif sender_email == "tim@gbl-data.com":
            sender_name = "Tim Swietek"
            sender_title = "Project Manager"
            phone_office = "480.461.3401"
            phone_mobile = "480.250.9040"
        elif sender_email == "jared@gbl-data.com":
            sender_name = "Jared Rasmussen"
            sender_title = "General Manager"
            phone_office = "480.461.3401"
            phone_mobile = "801.910.0827"
        else:
            sender_name = "Marc Perkins"
            sender_title = "Sales Manager"
            phone_office = "480.461.3401"
            phone_mobile = "602.758.3374"
            
        # Create professional email signature with logo
        email_signature = f"""
<div style="font-family: Calibri, sans-serif; font-size: 11pt; color: #000000; margin-top: 30px;">
    <div style="margin-bottom: 5px;">
        <strong>{sender_name}</strong><br>
        Global Data Specialists | {sender_title}<br>
        T: {phone_office} | M: {phone_mobile}<br>
        <a href="https://www.gbl-data.com" style="color: #000000; text-decoration: none;">www.gbl-data.com</a>
    </div>
    <div style="margin-top: 10px;">
        <img src="https://gbl-data.com/wp-content/uploads/2025/01/GDS_Black.png" alt="Global Data Specialists" width="200" height="auto" style="display: block;">
    </div>
</div>
"""
        
        # Select the template based on whether we have a verified name
        if has_valid_name:
            email_body = f"""
Hi {first_name},<br><br>

I'm reaching out from Global Data Specialists, a SCADA integrator with 40+ years experience. We'd love to have the chance to bid on your next SCADA project or to help with any SCADA needs. Happy to jump on a call to discuss, or just please keep us in mind for the next opportunity.<br><br>

Thanks!<br><br>

{email_signature}
"""
        else:
            email_body = f"""
I'm reaching out from Global Data Specialists, a SCADA integrator with 40+ years experience. Could you please connect me with the person that works with SCADA integrators?<br><br>

Thanks!<br><br>

{email_signature}
"""
        
        # Wrap in HTML
        html_email = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Calibri, sans-serif; font-size: 11pt; line-height: 1.5; color: #000000; }}
                a {{ color: #000000; text-decoration: none; }}
                a:hover {{ text-decoration: underline; }}
            </style>
        </head>
        <body>
            {email_body}
        </body>
        </html>
        """
        
        return html_email