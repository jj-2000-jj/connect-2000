/**
 * MICROSOFT 365 MESSAGE TRACE - PERMISSIONS AND IMPLEMENTATION GUIDE
 *
 * To use Message Trace logs to capture ALL emails sent by a user (including those
 * sent to external recipients), you need the following:
 *
 * 1. ADMIN PERMISSIONS REQUIRED:
 *    - Exchange Administrator role
 *    - Compliance Administrator role
 *    - Global Administrator role also works (includes all permissions)
 *
 * 2. API PERMISSIONS REQUIRED (if using Microsoft Graph API):
 *    - MailboxSettings.Read.All
 *    - Mail.Read.All 
 *    - ReportingService.Read.All (specifically for message trace operations)
 *
 * 3. IMPLEMENTATION OPTIONS:
 *
 *    A. EXCHANGE ONLINE POWERSHELL (Easiest and Most Reliable)
 *       ```powershell
 *       # Connect to Exchange Online PowerShell
 *       Connect-ExchangeOnline -UserPrincipalName admin@yourdomain.com
 *       
 *       # Set date range
 *       $startDate = (Get-Date).AddDays(-14)
 *       $endDate = Get-Date
 *       
 *       # Get message trace for Marc's emails
 *       $messages = Get-MessageTrace -SenderAddress marc@gbl-data.com -StartDate $startDate -EndDate $endDate
 *       
 *       # Export to CSV
 *       $messages | Export-Csv -Path "C:\\MarcEmailTrace.csv" -NoTypeInformation
 *       
 *       # For detailed message content (recipients, subject, etc.)
 *       foreach ($message in $messages) {
 *         Get-MessageTraceDetail -MessageTraceId $message.MessageTraceId -RecipientAddress $message.RecipientAddress
 *       }
 *       ```
 *
 *    B. MICROSOFT GRAPH API (Limited Support for Message Trace)
 *       The standard Microsoft Graph API doesn't fully support message trace operations.
 *       You'd need to use the Office 365 Management API or Exchange Web Services.
 *
 *    C. OFFICE 365 MANAGEMENT API
 *       This API provides access to various Office 365 audit logs, including mail events.
 *       Documentation: https://learn.microsoft.com/en-us/office/office-365-management-api/office-365-management-apis-overview
 *       
 *       The specific endpoint would be:
 *       https://manage.office.com/api/v1.0/{tenant_id}/activity/feed/subscriptions/content
 *
 * 4. ALTERNATIVE APPROACH: MAIL FLOW RULE (Transport Rule)
 *    If API access is challenging, set up a mail flow rule in Exchange Admin Center:
 *    
 *    a. Go to Exchange Admin Center > Mail flow > Rules
 *    b. Create a new rule:
 *       - Apply this rule if... The sender is 'marc@gbl-data.com'
 *       - Do the following... Blind carbon copy (BCC) the message to 'archive@yourdomain.com'
 *       - (Optional) Except if... The message is sent to people inside the organization
 *    
 *    This will automatically BCC all of Marc's outgoing emails to an archive mailbox
 *    that your application can read.
 */

// Sample code for message trace implementation using Node.js and Axios
async function getMessageTraceInformation() {
  require('dotenv').config();
  const axios = require('axios');
  const { ClientSecretCredential } = require('@azure/identity');
  const { TokenCredentialAuthenticationProvider } = require('@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials');
  
  // Step 1: Get an access token for Office 365 Management API
  const credential = new ClientSecretCredential(
    process.env.MICROSOFT_TENANT_ID,
    process.env.MICROSOFT_CLIENT_ID,
    process.env.MICROSOFT_CLIENT_SECRET
  );
  
  // Get token for Office 365 Management API
  const scopes = ['https://manage.office.com/.default'];
  const token = await credential.getToken(scopes);
  
  // Step 2: Define parameters for message trace
  const tenantId = process.env.MICROSOFT_TENANT_ID;
  const startTime = new Date();
  startTime.setDate(startTime.getDate() - 14); // 14 days ago
  const endTime = new Date();
  
  const senderAddress = 'marc@gbl-data.com';
  
  // Step 3: Convert to required format
  const startTimeStr = startTime.toISOString();
  const endTimeStr = endTime.toISOString();
  
  // Step 4: Make the API request to Office 365 Management API
  // Note: This is a hypothetical endpoint as the actual implementation details
  // may vary depending on the specific Office 365 Management API version and endpoints
  try {
    const response = await axios({
      method: 'post',
      url: `https://manage.office.com/api/v1.0/${tenantId}/activity/feed/subscriptions/content`,
      headers: {
        'Authorization': `Bearer ${token.token}`,
        'Content-Type': 'application/json'
      },
      data: {
        contentType: 'Audit.Exchange',
        startTime: startTimeStr,
        endTime: endTimeStr,
        filter: `SenderAddress eq '${senderAddress}'`
      }
    });
    
    console.log('Message trace results:', response.data);
    return response.data;
  } catch (error) {
    console.error('Error accessing Message Trace via Management API:', error.response?.data || error.message);
    console.log('\nRecommendation: Use Exchange Online PowerShell instead for the most reliable access to message trace data.');
    return null;
  }
}

// Note: This code is provided as a reference implementation
// The Office 365 Management API has specific requirements for authentication and subscriptions
// that may require additional setup beyond what's shown here