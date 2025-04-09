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
async function fetchAllMarcEmails(days = 30, recipientEmail = null) {
  try {
    const client = createGraphClient();
    const { startDate, endDate } = getLastXDaysRange(days);
    
    console.log(`Searching for emails from marc@gbl-data.com to ${recipientEmail || 'anyone'} from ${startDate} to ${endDate}`);
    
    // Method 1: Query Marc's sent items directly
    const marcEmail = 'marc@gbl-data.com';
    let sentFilter = `sentDateTime ge ${startDate} and sentDateTime le ${endDate}`;
    
    // Get all sent emails from Marc's account
    console.log(`Method 1: Querying Marc's sent items folder with filter: ${sentFilter}`);
    
    const sentResponse = await client.api(`/users/${marcEmail}/mailFolders/sentitems/messages`)
      .filter(sentFilter)
      .select('id,subject,bodyPreview,sentDateTime,from,toRecipients,body,importance,isDraft')
      .top(1000) // Get as many as possible
      .get();
    
    console.log(`Method 1 results: Found ${sentResponse.value.length} emails in Marc's sent items folder`);
    
    // Process sent emails and filter by recipient if specified
    let sentEmails = (sentResponse.value || [])
      .filter(email => !email.isDraft);
    
    if (recipientEmail) {
      sentEmails = sentEmails.filter(email => 
        email.toRecipients.some(recipient => 
          recipient.emailAddress.address.toLowerCase() === recipientEmail.toLowerCase()
        )
      );
      console.log(`After filtering for recipient ${recipientEmail}: ${sentEmails.length} emails`);
    }
    
    // Method 2: Search the recipient's mailbox for emails from Marc
    let recipientEmails = [];
    if (recipientEmail) {
      console.log(`Method 2: Searching ${recipientEmail}'s inbox for emails from ${marcEmail}`);
      
      try {
        const receivedResponse = await client.api(`/users/${recipientEmail}/messages`)
          .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${marcEmail}'`)
          .select('id,subject,bodyPreview,receivedDateTime,from,toRecipients,body,importance')
          .top(1000)
          .get();
          
        recipientEmails = receivedResponse.value || [];
        console.log(`Method 2 results: Found ${recipientEmails.length} emails in ${recipientEmail}'s inbox from Marc`);
      } catch (error) {
        console.error(`Error accessing ${recipientEmail}'s inbox:`, error.message);
        console.log("This could be due to permissions. Continuing with method 1 results only.");
      }
    }
    
    // Method 3: Search using Microsoft Graph search API
    console.log("Method 3: Using Microsoft Graph search API");
    let searchResults = [];
    
    try {
      // First create the search query
      const searchRequest = {
        requests: [
          {
            entityTypes: ["message"],
            query: {
              queryString: `from:"marc@gbl-data.com" AND sent>=${startDate.substring(0, 10)}`
            },
            from: 0,
            size: 100
          }
        ]
      };
      
      // Execute the search
      const searchResponse = await client.api('/search/query').post(searchRequest);
      
      // Process search results
      if (searchResponse.value && searchResponse.value[0] && searchResponse.value[0].hitsContainers) {
        const container = searchResponse.value[0].hitsContainers[0];
        
        if (container.hits) {
          searchResults = container.hits.map(hit => hit.resource);
          console.log(`Method 3 results: Found ${searchResults.length} emails using search API`);
        }
      }
    } catch (error) {
      console.error("Error using search API:", error.message);
      console.log("This could be due to API limitations or permissions. Continuing with other methods.");
    }
    
    // Combine all unique emails based on ID
    const emailMap = new Map();
    
    // Process emails from method 1
    sentEmails.forEach(email => {
      emailMap.set(email.id, {
        id: email.id,
        subject: email.subject || '(No Subject)',
        from: email.from.emailAddress.address,
        fromName: email.from.emailAddress.name,
        to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
        date: new Date(email.sentDateTime).toISOString(),
        preview: email.bodyPreview || '',
        source: 'Marc sent items'
      });
    });
    
    // Process emails from method 2
    recipientEmails.forEach(email => {
      if (!emailMap.has(email.id)) {
        emailMap.set(email.id, {
          id: email.id,
          subject: email.subject || '(No Subject)',
          from: email.from.emailAddress.address,
          fromName: email.from.emailAddress.name,
          to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
          date: new Date(email.receivedDateTime).toISOString(),
          preview: email.bodyPreview || '',
          source: 'Recipient inbox'
        });
      }
    });
    
    // Process emails from method 3
    searchResults.forEach(email => {
      if (!emailMap.has(email.id)) {
        emailMap.set(email.id, {
          id: email.id,
          subject: email.subject || '(No Subject)',
          from: email.from?.emailAddress?.address || 'marc@gbl-data.com',
          fromName: email.from?.emailAddress?.name || 'Marc',
          to: (email.toRecipients || []).map(r => r.emailAddress.address).join(', '),
          date: new Date(email.sentDateTime || email.receivedDateTime).toISOString(),
          preview: email.bodyPreview || '',
          source: 'Search API'
        });
      }
    });
    
    // Convert to array and sort by date
    const allEmails = Array.from(emailMap.values());
    allEmails.sort((a, b) => new Date(b.date) - new Date(a.date));
    
    console.log(`Total unique emails found: ${allEmails.length}`);
    return allEmails;
  } catch (error) {
    console.error('Error fetching Marc emails:', error);
    return [];
  }
}

// Run the function with command line arguments
const days = process.argv[2] ? parseInt(process.argv[2], 10) : 14;
const recipient = process.argv[3] || 'jared@gbl-data.com';

fetchAllMarcEmails(days, recipient)
  .then(emails => {
    console.log(`Found ${emails.length} emails from Marc to ${recipient} in the last ${days} days:`);
    console.log('-----------------------------------------------------------');
    
    // Print email details
    emails.forEach((email, index) => {
      console.log(`Email ${index + 1}:`);
      console.log(`Date: ${email.date}`);
      console.log(`Subject: ${email.subject}`);
      console.log(`From: ${email.fromName} <${email.from}>`);
      console.log(`To: ${email.to}`);
      console.log(`Preview: ${email.preview.substring(0, 100)}...`);
      console.log(`Source: ${email.source}`);
      console.log('-----------------------------------------------------------');
    });
  })
  .catch(error => {
    console.error('Error running script:', error);
  });