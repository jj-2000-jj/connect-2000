require('dotenv').config();
require('isomorphic-fetch');
const { ClientSecretCredential } = require('@azure/identity');
const { Client } = require('@microsoft/microsoft-graph-client');
const { TokenCredentialAuthenticationProvider } = require('@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials');
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
 * Fetch ALL emails from marc in the last X days that were sent to a specific recipient
 */
async function fetchAndDisplayMarcEmails(days = 14) {
  try {
    const client = createGraphClient();
    const { startDate, endDate } = getLastXDaysRange(days);
    const marcEmail = 'marc@gbl-data.com';
    const recipientEmail = process.env.MICROSOFT_USER_EMAIL;
    
    console.log(`Searching for emails from marc@gbl-data.com in the last ${days} days`);
    console.log(`Date range: ${startDate} to ${endDate}`);
    
    // Track all emails found with a Map to avoid duplicates
    const emailMap = new Map();
    
    // METHOD 1: Get sent emails from Marc's sent items folder
    console.log('\nMETHOD 1: Querying Marc\'s sent items folder');
    
    try {
      const sentResponse = await client.api(`/users/${marcEmail}/mailFolders/sentitems/messages`)
        .filter(`sentDateTime ge ${startDate} and sentDateTime le ${endDate}`)
        .select('id,subject,sentDateTime,from,toRecipients,isDraft')
        .top(1000)
        .get();
      
      const sentEmails = (sentResponse.value || [])
        .filter(email => !email.isDraft);
        
      console.log(`Found ${sentEmails.length} emails in Marc's sent items folder`);
      
      // Add to our email map
      sentEmails.forEach(email => {
        emailMap.set(email.id, {
          id: email.id,
          subject: email.subject || '(No Subject)',
          from: email.from.emailAddress.address,
          to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
          date: new Date(email.sentDateTime).toISOString(),
          source: 'Marc sent items'
        });
      });
    } catch (error) {
      console.error('Error querying Marc\'s sent items folder:', error.message);
    }
    
    // METHOD 2: Search recipient inboxes for emails from Marc
    if (recipientEmail) {
      console.log(`\nMETHOD 2: Searching ${recipientEmail}'s inbox for emails from ${marcEmail}`);
      
      try {
        const receivedResponse = await client.api(`/users/${recipientEmail}/messages`)
          .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${marcEmail}'`)
          .select('id,subject,receivedDateTime,from,toRecipients')
          .top(1000)
          .get();
          
        const recipientEmails = receivedResponse.value || [];
        console.log(`Found ${recipientEmails.length} emails in ${recipientEmail}'s inbox from Marc`);
        
        // Add to our email map
        recipientEmails.forEach(email => {
          if (!emailMap.has(email.id)) {
            emailMap.set(email.id, {
              id: email.id,
              subject: email.subject || '(No Subject)',
              from: email.from.emailAddress.address,
              to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
              date: new Date(email.receivedDateTime).toISOString(),
              source: 'Recipient inbox'
            });
          }
        });
      } catch (error) {
        console.error(`Error searching ${recipientEmail}'s inbox:`, error.message);
      }
    }
    
    // Convert map to array
    const allEmails = Array.from(emailMap.values());
    
    // Sort by date
    allEmails.sort((a, b) => new Date(b.date) - new Date(a.date));
    
    // Log the distribution of email sources
    const fromSentItems = allEmails.filter(email => email.source === 'Marc sent items').length;
    const fromRecipientInbox = allEmails.filter(email => email.source === 'Recipient inbox').length;
    
    console.log(`\nFound ${allEmails.length} total unique emails sent by marc@gbl-data.com in the last ${days} days`);
    console.log(`Email sources: ${fromSentItems} from Marc's sent items, ${fromRecipientInbox} from recipient inbox`);
    
    console.log('\n============= EMAILS FROM MARC =============');
    console.log('Date                 | Subject                          | To');
    console.log('---------------------|----------------------------------|------------------------------');
    
    allEmails.forEach(email => {
      const date = email.date.substring(0, 10);
      const subject = email.subject.length > 30 ? email.subject.substring(0, 27) + '...' : email.subject.padEnd(30);
      const recipients = email.to.length > 28 ? email.to.substring(0, 25) + '...' : email.to.padEnd(28);
      
      console.log(`${date} | ${subject} | ${recipients} | ${email.source}`);
    });
    
    console.log('\n============================================');
    return allEmails;
  } catch (error) {
    console.error('Error in script:', error);
    return [];
  }
}

// Run the script with command-line args
const days = process.argv[2] ? parseInt(process.argv[2], 10) : 14;
fetchAndDisplayMarcEmails(days);