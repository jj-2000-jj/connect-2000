require('dotenv').config();
require('isomorphic-fetch');
const { ClientSecretCredential } = require('@azure/identity');
const { Client } = require('@microsoft/microsoft-graph-client');
const { TokenCredentialAuthenticationProvider } = require('@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials');

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
 * Company employees list from index.js
 */
const COMPANY_EMPLOYEES = [
  'jared@gbl-data.com',
  'marc@gbl-data.com',
  'tim@gbl-data.com',
  'clint@gbl-data.com',
  'rebeca@gbl-data.com',
  'john@gbl-data.com',
  'darren@gbl-data.com',
  'galina@gbl-data.com',
  'sales@gbl-data.com'
];

/**
 * Comprehensive approach to find ALL emails sent by Marc
 * using the permissions we have
 */
async function findAllMarcEmails(days = 30) {
  const client = createGraphClient();
  const { startDate, endDate } = getLastXDaysRange(days);
  const marcEmail = 'marc@gbl-data.com';
  
  console.log(`Looking for ALL emails sent by ${marcEmail} from ${startDate} to ${endDate}`);
  
  // Email map to track unique emails
  const emailMap = new Map();
  
  // APPROACH 1: Check Marc's sent items folder
  console.log("\n===== APPROACH 1: Checking Marc's sent items folder =====");
  try {
    const sentResponse = await client.api(`/users/${marcEmail}/mailFolders/sentitems/messages`)
      .filter(`sentDateTime ge ${startDate} and sentDateTime le ${endDate}`)
      .select('id,subject,sentDateTime,from,toRecipients,isDraft')
      .top(1000)
      .get();
    
    const sentEmails = (sentResponse.value || []).filter(email => !email.isDraft);
    console.log(`Found ${sentEmails.length} emails in Marc's sent items folder`);
    
    sentEmails.forEach(email => {
      emailMap.set(email.id, {
        id: email.id,
        subject: email.subject || '(No Subject)',
        from: email.from.emailAddress.address,
        to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
        toRecipients: email.toRecipients.map(r => r.emailAddress.address),
        date: new Date(email.sentDateTime).toISOString(),
        source: 'Marc sent items'
      });
    });
  } catch (error) {
    console.error("Error checking sent items folder:", error.message);
  }
  
  // APPROACH 2: Check all company employee inboxes for emails from Marc
  console.log("\n===== APPROACH 2: Checking company inboxes for emails from Marc =====");
  
  for (const employeeEmail of COMPANY_EMPLOYEES) {
    if (employeeEmail === marcEmail) continue; // Skip Marc's own inbox
    
    try {
      console.log(`Checking ${employeeEmail}'s inbox...`);
      
      const receivedResponse = await client.api(`/users/${employeeEmail}/messages`)
        .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${marcEmail}'`)
        .select('id,subject,receivedDateTime,from,toRecipients')
        .top(1000)
        .get();
        
      const recipientEmails = receivedResponse.value || [];
      console.log(`Found ${recipientEmails.length} emails in ${employeeEmail}'s inbox from Marc`);
      
      // Add to our email map
      recipientEmails.forEach(email => {
        if (!emailMap.has(email.id)) {
          emailMap.set(email.id, {
            id: email.id,
            subject: email.subject || '(No Subject)',
            from: email.from.emailAddress.address,
            to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
            toRecipients: email.toRecipients.map(r => r.emailAddress.address),
            date: new Date(email.receivedDateTime).toISOString(),
            source: `${employeeEmail}'s inbox`
          });
        }
      });
    } catch (error) {
      console.error(`Error checking ${employeeEmail}'s inbox:`, error.message);
    }
  }
  
  // APPROACH 3: Check Marc's mailboxes for emails he's copied himself on
  console.log("\n===== APPROACH 3: Checking if Marc BCC'd/CC'd himself =====");
  
  try {
    // Check Marc's own inbox for emails from himself (CC/BCC)
    const selfCopyResponse = await client.api(`/users/${marcEmail}/messages`)
      .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${marcEmail}'`)
      .select('id,subject,receivedDateTime,from,toRecipients,ccRecipients,bccRecipients')
      .top(1000)
      .get();
      
    const selfCopiedEmails = selfCopyResponse.value || [];
    console.log(`Found ${selfCopiedEmails.length} emails in Marc's inbox that he sent and CC'd/BCC'd himself on`);
    
    // Add to our email map
    selfCopiedEmails.forEach(email => {
      if (!emailMap.has(email.id)) {
        emailMap.set(email.id, {
          id: email.id,
          subject: email.subject || '(No Subject)',
          from: email.from.emailAddress.address,
          to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
          toRecipients: email.toRecipients.map(r => r.emailAddress.address),
          date: new Date(email.receivedDateTime).toISOString(),
          source: 'Marc CC/BCC to self'
        });
      }
    });
  } catch (error) {
    console.error("Error checking if Marc CC'd/BCC'd himself:", error.message);
  }
  
  // APPROACH 4: Check for emails to team distribution lists that Marc is on
  // This is more complex and would require knowledge of all distribution lists
  
  // Convert map to array
  const allEmails = Array.from(emailMap.values());
  allEmails.sort((a, b) => new Date(b.date) - new Date(a.date));
  
  // Count by source
  const sources = {};
  allEmails.forEach(email => {
    sources[email.source] = (sources[email.source] || 0) + 1;
  });
  
  // Count unique external recipients
  const externalRecipients = new Set();
  allEmails.forEach(email => {
    if (email.toRecipients) {
      email.toRecipients.forEach(recipient => {
        if (!COMPANY_EMPLOYEES.includes(recipient.toLowerCase())) {
          externalRecipients.add(recipient.toLowerCase());
        }
      });
    }
  });
  
  console.log("\n===== SUMMARY =====");
  console.log(`Total unique emails found: ${allEmails.length}`);
  console.log(`Unique external recipients: ${externalRecipients.size}`);
  console.log("Breakdown by source:");
  Object.keys(sources).forEach(source => {
    console.log(`- ${source}: ${sources[source]}`);
  });
  
  // Print emails
  console.log("\n===== EMAILS =====");
  console.log('Date       | Subject                          | To                              | Source');
  console.log('-----------|----------------------------------|--------------------------------|------------------');
  
  allEmails.forEach(email => {
    const date = email.date.substring(0, 10);
    const subject = email.subject.length > 30 ? email.subject.substring(0, 27) + '...' : email.subject.padEnd(30);
    const to = email.to.length > 30 ? email.to.substring(0, 27) + '...' : email.to.padEnd(30);
    
    console.log(`${date} | ${subject} | ${to} | ${email.source}`);
  });
  
  return allEmails;
}

// Run the script with command-line args
const days = process.argv[2] ? parseInt(process.argv[2], 10) : 30;
findAllMarcEmails(days)
  .catch(error => {
    console.error("Error running script:", error);
  });