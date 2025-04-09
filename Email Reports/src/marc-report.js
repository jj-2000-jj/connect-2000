require('dotenv').config();
require('isomorphic-fetch');
const { ClientSecretCredential } = require('@azure/identity');
const { Client } = require('@microsoft/microsoft-graph-client');
const { TokenCredentialAuthenticationProvider } = require('@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials');
const { summarizeEmails } = require('./summarizer');
const { formatDateString } = require('./utils');
const logger = require('./logger');

/**
 * Creates an authenticated Microsoft Graph client
 */
function createGraphClient() {
  const credential = new ClientSecretCredential(
    process.env.MICROSOFT_TENANT_ID,
    process.env.MICROSOFT_CLIENT_ID,
    process.env.MICROSOFT_CLIENT_SECRET
  );

  const authProvider = new TokenCredentialAuthenticationProvider(credential, {
    scopes: ['https://graph.microsoft.com/.default']
  });

  return Client.initWithMiddleware({
    authProvider
  });
}

/**
 * Get date range for last X days
 */
function getLastXDaysRange(days = 30) {
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  
  return {
    startDate: startDate.toISOString(),
    endDate: endDate.toISOString()
  };
}

/**
 * Fetches all emails sent by marc@gbl-data.com in the last X days
 * @param {number} days - Number of days to look back
 */
async function fetchMarcSentEmails(days = 30) {
  // Ensure valid days parameter
  days = Number(days) || 30; // Default to 30 if invalid number
  try {
    logger.info(`Fetching emails sent by marc@gbl-data.com in the last ${days} days`);
    const client = createGraphClient();
    const { startDate, endDate } = getLastXDaysRange(days);
    
    logger.info(`Fetching emails from ${startDate} to ${endDate}`);
    
    const marcEmail = 'marc@gbl-data.com';
    const recipientEmail = process.env.MICROSOFT_USER_EMAIL;
    
    // Track all emails found with a Map to avoid duplicates
    const emailMap = new Map();
    
    // METHOD 1: Get sent emails from Marc's sent items folder
    logger.info(`METHOD 1: Querying Marc's sent items folder`);
    logger.info(`Microsoft Graph API query parameters: 
      - User: ${marcEmail}
      - Filter: sentDateTime ge ${startDate} and sentDateTime le ${endDate}
      - Max items: 1000
    `);
      
    try {
      const sentResponse = await client.api(`/users/${marcEmail}/mailFolders/sentitems/messages`)
        .filter(`sentDateTime ge ${startDate} and sentDateTime le ${endDate}`)
        .select('id,subject,bodyPreview,sentDateTime,from,toRecipients,body,importance,isDraft')
        .top(1000) // Get as many as possible
        .get();
        
      // Log raw response details
      logger.info(`Microsoft Graph API response:
        - Status: Success
        - Total emails returned: ${sentResponse.value.length}
        - @odata.nextLink present: ${!!sentResponse['@odata.nextLink']}
      `);
      
      // Process Marc's sent emails
      const sentEmails = (sentResponse.value || [])
        .filter(email => !email.isDraft); // Filter out drafts
        
      logger.info(`Found ${sentEmails.length} emails in Marc's sent items folder`);
      
      // Add to our email map
      sentEmails.forEach(email => {
        emailMap.set(email.id, {
          id: email.id,
          subject: email.subject || '(No Subject)',
          from: { 
            text: email.from.emailAddress.name || email.from.emailAddress.address,
            address: email.from.emailAddress.address
          },
          to: email.toRecipients.map(recipient => ({
            address: recipient.emailAddress.address,
            name: recipient.emailAddress.name
          })),
          date: new Date(email.sentDateTime),
          text: email.bodyPreview || '',
          html: email.body && email.body.contentType === 'html' ? email.body.content : null,
          importance: email.importance || 'normal',
          direction: 'sent',
          source: 'Marc sent items'
        });
      });
    } catch (error) {
      logger.error('Error querying Marc\'s sent items folder', { 
        error: error.message,
        stack: error.stack
      });
    }
    
    // METHOD 2: Search recipient inboxes for emails from Marc
    if (recipientEmail) {
      logger.info(`METHOD 2: Searching ${recipientEmail}'s inbox for emails from ${marcEmail}`);
      
      try {
        const receivedResponse = await client.api(`/users/${recipientEmail}/messages`)
          .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${marcEmail}'`)
          .select('id,subject,bodyPreview,receivedDateTime,from,toRecipients,body,importance')
          .top(1000)
          .get();
          
        const recipientEmails = receivedResponse.value || [];
        logger.info(`Found ${recipientEmails.length} emails in ${recipientEmail}'s inbox from Marc`);
        
        // Add to our email map
        recipientEmails.forEach(email => {
          if (!emailMap.has(email.id)) {
            emailMap.set(email.id, {
              id: email.id,
              subject: email.subject || '(No Subject)',
              from: { 
                text: email.from.emailAddress.name || email.from.emailAddress.address,
                address: email.from.emailAddress.address
              },
              to: email.toRecipients.map(recipient => ({
                address: recipient.emailAddress.address,
                name: recipient.emailAddress.name
              })),
              date: new Date(email.receivedDateTime),
              text: email.bodyPreview || '',
              html: email.body && email.body.contentType === 'html' ? email.body.content : null,
              importance: email.importance || 'normal',
              direction: 'received',
              source: 'Recipient inbox'
            });
          }
        });
      } catch (error) {
        logger.error(`Error searching ${recipientEmail}'s inbox`, { 
          error: error.message,
          stack: error.stack
        });
      }
    }
    
    // Convert map to array
    const allEmails = Array.from(emailMap.values());
    
    // Sort by date
    allEmails.sort((a, b) => b.date - a.date);
    
    logger.info(`Found ${allEmails.length} total unique emails sent by marc@gbl-data.com in the last ${days} days`);
    
    // Log the distribution of email sources
    const fromSentItems = allEmails.filter(email => email.source === 'Marc sent items').length;
    const fromRecipientInbox = allEmails.filter(email => email.source === 'Recipient inbox').length;
    logger.info(`Email sources: ${fromSentItems} from Marc's sent items, ${fromRecipientInbox} from recipient inbox`);
    
    return allEmails;
  } catch (error) {
    logger.error('Error fetching Marc\'s emails', { 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

/**
 * Formats and sends the Marc report
 * @param {number} days - Number of days to look back
 * @param {string} recipient - Email address to send the report to
 */
async function sendMarcReport(days = 30, recipient) {
  try {
    // Step 1: Fetch Marc's sent emails
    const emails = await fetchMarcSentEmails(days);
    logger.info(`Fetched ${emails.length} emails from Marc`);
    
    if (emails.length === 0) {
      logger.info('No emails to report');
      return;
    }
    
    // Step 2: Organize by recipient/date
    const emailsMarcSent = { 'marc@gbl-data.com sent': emails };
    
    // Step 3: Summarize emails
    const summaries = await summarizeEmails(emailsMarcSent);
    
    // Step 4: Send the report
    const { sendCustomEmail } = require('./email-sender');
    await sendCustomEmail(
      summaries, 
      `Marc's Email Activity - Last ${days} Days`, 
      `One-time report of all emails sent by marc@gbl-data.com over the past ${days} days.`,
      recipient
    );
    
    logger.info('Marc report completed successfully');
  } catch (error) {
    logger.error('Error in Marc report generation', { error: error.message, stack: error.stack });
  }
}

// Make the function available to be called from other files
if (require.main === module) {
  // If this file is run directly, run the report with default settings
  sendMarcReport();
}

module.exports = { sendMarcReport };