require('dotenv').config();
require('isomorphic-fetch');
const { ClientSecretCredential } = require('@azure/identity');
const { Client } = require('@microsoft/microsoft-graph-client');
const { TokenCredentialAuthenticationProvider } = require('@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials');
const logger = require('./logger');
const { summarizeEmails } = require('./summarizer');
const { sendCustomEmail } = require('./email-sender');

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
 * Format a message object into our standard email format
 */
function formatEmailMessage(message, source) {
  return {
    id: message.id,
    subject: message.subject || '(No Subject)',
    from: { 
      text: message.from?.emailAddress?.name || message.from?.emailAddress?.address || 'Unknown',
      address: message.from?.emailAddress?.address || 'Unknown'
    },
    to: (message.toRecipients || []).map(recipient => ({
      address: recipient.emailAddress?.address || 'Unknown',
      name: recipient.emailAddress?.name || recipient.emailAddress?.address || 'Unknown'
    })),
    cc: (message.ccRecipients || []).map(recipient => ({
      address: recipient.emailAddress?.address || 'Unknown',
      name: recipient.emailAddress?.name || recipient.emailAddress?.address || 'Unknown'
    })),
    bcc: (message.bccRecipients || []).map(recipient => ({
      address: recipient.emailAddress?.address || 'Unknown',
      name: recipient.emailAddress?.name || recipient.emailAddress?.address || 'Unknown'
    })),
    date: new Date(message.sentDateTime || message.receivedDateTime),
    text: message.bodyPreview || '',
    html: message.body?.content || null,
    direction: 'sent',
    source: source
  };
}

/**
 * Get message trace data using Microsoft Graph API
 * 
 * Required permissions:
 * - MailboxSettings.Read
 * - Mail.Read
 * - Mail.ReadBasic.All (to search all organization mailboxes)
 */
async function getMessageTrace(days = 14, senderAddress = 'marc@gbl-data.com') {
  try {
    logger.info(`Using Microsoft Graph API to get all emails from ${senderAddress} for the last ${days} days`);
    const client = createGraphClient();
    const { startDate, endDate } = getLastXDaysRange(days);
    
    // Track all emails with a Map to avoid duplicates
    const emailMap = new Map();
    
    // Method 1: Get all users in the organization
    logger.info('Method 1: Getting all users in the organization');
    let allUsers = [];
    
    try {
      const usersResponse = await client.api('/users')
        .select('id,userPrincipalName,mail,displayName')
        .top(999)
        .get();
      
      if (usersResponse && usersResponse.value) {
        allUsers = usersResponse.value.filter(user => 
          user.mail && user.userPrincipalName && user.userPrincipalName.includes('@')
        );
        logger.info(`Found ${allUsers.length} users in the organization`);
      }
    } catch (error) {
      logger.warn(`Error getting organization users: ${error.message}`);
      // Fallback to company employees list
      allUsers = [];
    }
    
    // Method 2: Direct message access to the sender's sent items folder
    logger.info(`Method 2: Querying ${senderAddress}'s sent items folder`);
    
    try {
      const sentResponse = await client.api(`/users/${senderAddress}/mailFolders/sentitems/messages`)
        .filter(`sentDateTime ge ${startDate} and sentDateTime le ${endDate}`)
        .select('id,subject,sentDateTime,receivedDateTime,from,toRecipients,ccRecipients,bccRecipients')
        .top(999)
        .get();
      
      if (sentResponse && sentResponse.value) {
        logger.info(`Found ${sentResponse.value.length} messages in sent items folder`);
        
        sentResponse.value.forEach(message => {
          emailMap.set(message.id, formatEmailMessage(message, 'sent_items'));
        });
      } else {
        logger.info('No messages found in sent items folder');
      }
    } catch (sentError) {
      logger.warn(`Error accessing sent items: ${sentError.message}`);
    }
    
    // Method 3: Organization-wide search for emails from the sender
    // This uses Mail.ReadBasic.All permission to search ALL mailboxes
    logger.info('Method 3: Organization-wide search for emails from sender');
    
    // Using users from Method 1, or falling back to company list if that failed
    let mailboxes = allUsers.length > 0 ? 
      allUsers.map(user => user.userPrincipalName) : 
      [
        'jared@gbl-data.com',
        'tim@gbl-data.com',
        'clint@gbl-data.com',
        'rebeca@gbl-data.com',
        'john@gbl-data.com',
        'darren@gbl-data.com',
        'galina@gbl-data.com',
        'sales@gbl-data.com'
      ];
    
    // Remove the sender from the list
    mailboxes = mailboxes.filter(email => email.toLowerCase() !== senderAddress.toLowerCase());
    
    // Process in batches to avoid timeouts
    const BATCH_SIZE = 5;
    for (let i = 0; i < mailboxes.length; i += BATCH_SIZE) {
      const batch = mailboxes.slice(i, i + BATCH_SIZE);
      
      logger.info(`Processing batch ${i/BATCH_SIZE + 1}/${Math.ceil(mailboxes.length/BATCH_SIZE)} (${batch.length} mailboxes)`);
      
      const batchPromises = batch.map(async (mailbox) => {
        try {
          logger.info(`Checking ${mailbox}'s inbox for emails from ${senderAddress}`);
          
          const inboxResponse = await client.api(`/users/${mailbox}/messages`)
            .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${senderAddress}'`)
            .select('id,subject,receivedDateTime,from,toRecipients,ccRecipients,bccRecipients')
            .top(999)
            .get();
          
          if (inboxResponse && inboxResponse.value) {
            logger.info(`Found ${inboxResponse.value.length} messages in ${mailbox}'s inbox from ${senderAddress}`);
            
            inboxResponse.value.forEach(message => {
              if (!emailMap.has(message.id)) {
                emailMap.set(message.id, formatEmailMessage(message, `${mailbox}_inbox`));
              }
            });
            
            return inboxResponse.value.length;
          } else {
            logger.info(`No messages found in ${mailbox}'s inbox from ${senderAddress}`);
            return 0;
          }
        } catch (inboxError) {
          logger.warn(`Could not access ${mailbox}'s inbox: ${inboxError.message}`);
          return 0;
        }
      });
      
      // Wait for all promises in this batch to resolve
      await Promise.all(batchPromises);
    }
    
    // Method 4: Check archive mailbox for all emails from this sender
    logger.info('Method 4: Checking the archive mailbox for all emails from this sender');
    
    try {
      const archiveMailbox = 'archive@gbl-data.com'; // The archive mailbox you set up
      
      logger.info(`Checking ${archiveMailbox} for emails from ${senderAddress}`);
      
      // Get all messages in the archive mailbox from this sender
      const archiveResponse = await client.api(`/users/${archiveMailbox}/messages`)
        .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${senderAddress}'`)
        .select('id,subject,receivedDateTime,from,toRecipients,ccRecipients,bccRecipients')
        .top(999)
        .get();
      
      if (archiveResponse && archiveResponse.value) {
        logger.info(`Found ${archiveResponse.value.length} archived emails from ${senderAddress}`);
        
        archiveResponse.value.forEach(message => {
          if (!emailMap.has(message.id)) {
            emailMap.set(message.id, formatEmailMessage(message, 'archive_mailbox'));
          }
        });
      } else {
        logger.info(`No archived emails found from ${senderAddress}`);
      }
    } catch (archiveError) {
      logger.warn(`Error accessing archive mailbox: ${archiveError.message}`);
    }
    
    // Method 5: Try to use beta API to get sent messages organization-wide
    logger.info('Method 5: Attempting organization-wide message search using beta API');
    
    try {
      // This uses a beta endpoint that can search across all messages
      const betaClient = Client.initWithMiddleware({
        baseUrl: 'https://graph.microsoft.com/beta',
        authProvider: createGraphClient().authProvider
      });
      
      const searchQuery = `FROM:"${senderAddress}" AND sent>=${startDate.split('T')[0]}`;
      
      const searchResults = await betaClient.api('/communications/messages')
        .search(searchQuery)
        .get();
      
      if (searchResults && searchResults.value) {
        logger.info(`Found ${searchResults.value.length} messages using organization-wide search`);
        
        searchResults.value.forEach(message => {
          if (!emailMap.has(message.id)) {
            emailMap.set(message.id, formatEmailMessage(message, 'org_search'));
          }
        });
      }
    } catch (searchError) {
      logger.warn(`Error using organization-wide search: ${searchError.message}`);
    }
    
    // Convert map to array and sort by date
    const allEmails = Array.from(emailMap.values())
      .sort((a, b) => new Date(b.date) - new Date(a.date));
    
    logger.info(`Total unique emails found: ${allEmails.length}`);
    
    // Count sources
    const sourceCounts = {};
    allEmails.forEach(email => {
      sourceCounts[email.source] = (sourceCounts[email.source] || 0) + 1;
    });
    
    logger.info('Email sources breakdown:');
    Object.entries(sourceCounts).forEach(([source, count]) => {
      logger.info(`- ${source}: ${count}`);
    });
    
    // Count unique recipients
    const uniqueRecipients = new Set();
    allEmails.forEach(email => {
      (email.to || []).forEach(recipient => {
        if (recipient.address) {
          uniqueRecipients.add(recipient.address.toLowerCase());
        }
      });
    });
    
    logger.info(`Emails were sent to ${uniqueRecipients.size} unique recipients`);
    
    // Count domains
    const domains = {};
    uniqueRecipients.forEach(address => {
      const parts = address.split('@');
      if (parts.length === 2) {
        const domain = parts[1].toLowerCase();
        domains[domain] = (domains[domain] || 0) + 1;
      }
    });
    
    logger.info('Top recipient domains:');
    Object.entries(domains)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .forEach(([domain, count]) => {
        logger.info(`- ${domain}: ${count} recipients`);
      });
    
    return allEmails;
  } catch (error) {
    logger.error('Error getting message trace data:', {
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

/**
 * Generate analytics on emails
 */
function generateEmailAnalytics(emails) {
  // Count unique recipients
  const uniqueRecipients = new Set();
  emails.forEach(email => {
    (email.to || []).forEach(recipient => {
      if (recipient.address) {
        uniqueRecipients.add(recipient.address.toLowerCase());
      }
    });
    (email.cc || []).forEach(recipient => {
      if (recipient.address) {
        uniqueRecipients.add(recipient.address.toLowerCase());
      }
    });
    (email.bcc || []).forEach(recipient => {
      if (recipient.address) {
        uniqueRecipients.add(recipient.address.toLowerCase());
      }
    });
  });
  
  // Count by domain
  const domainCounts = {};
  uniqueRecipients.forEach(address => {
    const parts = address.split('@');
    if (parts.length === 2) {
      const domain = parts[1].toLowerCase();
      domainCounts[domain] = (domainCounts[domain] || 0) + 1;
    }
  });
  
  // Count by date
  const dateCounts = {};
  emails.forEach(email => {
    const dateStr = email.date.toISOString().split('T')[0];
    dateCounts[dateStr] = (dateCounts[dateStr] || 0) + 1;
  });
  
  // Count by source
  const sourceCounts = {};
  emails.forEach(email => {
    sourceCounts[email.source] = (sourceCounts[email.source] || 0) + 1;
  });
  
  // Count company vs external recipients
  const companyDomain = 'gbl-data.com';
  const companyRecipients = [...uniqueRecipients].filter(r => r.endsWith(companyDomain)).length;
  const externalRecipients = uniqueRecipients.size - companyRecipients;
  
  return {
    totalEmails: emails.length,
    uniqueRecipients: uniqueRecipients.size,
    companyRecipients,
    externalRecipients,
    domainCounts: Object.entries(domainCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([domain, count]) => ({ domain, count })),
    dateCounts: Object.entries(dateCounts)
      .sort((a, b) => b[0].localeCompare(a[0]))
      .map(([date, count]) => ({ date, count })),
    sourceCounts: sourceCounts
  };
}

/**
 * Send a complete report of Marc's emails using Microsoft Graph API
 */
async function sendMarcGraphReport(days = 14, recipient) {
  try {
    logger.info(`Starting Graph API-based complete Marc report for last ${days} days`);
    
    // Get message trace data
    const emails = await getMessageTrace(days, 'marc@gbl-data.com');
    
    if (emails.length === 0) {
      logger.info('No emails found to report');
      return;
    }
    
    // Generate analytics
    const analytics = generateEmailAnalytics(emails);
    logger.info(`Generated analytics for ${emails.length} emails`);
    
    // Organize emails for summarization
    const emailsMarcSent = { 'marc@gbl-data.com sent': emails };
    
    // Summarize emails if there aren't too many
    let summaries = null;
    if (emails.length <= 100) {
      try {
        summaries = await summarizeEmails(emailsMarcSent);
        logger.info('Email summarization complete');
      } catch (summaryError) {
        logger.error('Error summarizing emails:', {
          error: summaryError.message,
          stack: summaryError.stack
        });
      }
    } else {
      logger.info(`Too many emails (${emails.length}) to summarize. Skipping summarization.`);
    }
    
    // Prepare analytics HTML
    const analyticsHTML = `
    <h2>Email Analytics</h2>
    <p><strong>Total Emails Sent:</strong> ${analytics.totalEmails}</p>
    <p><strong>Unique Recipients:</strong> ${analytics.uniqueRecipients}</p>
    <p><strong>Company Recipients:</strong> ${analytics.companyRecipients}</p>
    <p><strong>External Recipients:</strong> ${analytics.externalRecipients}</p>
    
    <h3>Top 10 Recipient Domains</h3>
    <ul>
      ${analytics.domainCounts.slice(0, 10).map(d => `<li>${d.domain}: ${d.count} recipients</li>`).join('')}
    </ul>
    
    <h3>Daily Email Activity</h3>
    <ul>
      ${analytics.dateCounts.map(d => `<li>${d.date}: ${d.count} emails</li>`).join('')}
    </ul>
    
    <h3>Email Source Breakdown</h3>
    <ul>
      ${Object.entries(analytics.sourceCounts).map(([source, count]) => `<li>${source}: ${count} emails</li>`).join('')}
    </ul>
    `;
    
    // Prepare email table HTML (limited to 100 for performance)
    const emailTableHTML = `
    <h3>Recent Emails (limited to 100)</h3>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
      <tr>
        <th>Date</th>
        <th>Subject</th>
        <th>Recipients</th>
      </tr>
      ${emails.slice(0, 100).map(email => `
        <tr>
          <td>${email.date.toISOString().split('T')[0]}</td>
          <td>${email.subject}</td>
          <td>${(email.to || []).map(r => r.address).join(', ')}</td>
        </tr>
      `).join('')}
    </table>
    `;
    
    // Send the report
    if (summaries) {
      await sendCustomEmail(
        summaries,
        `Marc's Complete Email Activity (Graph API) - Last ${days} Days`,
        `<p>Comprehensive report of ALL emails sent by marc@gbl-data.com over the past ${days} days using Microsoft Graph API with Mail.ReadBasic.All permission.</p>
        ${analyticsHTML}`,
        recipient
      );
    } else {
      await sendCustomEmail(
        null,
        `Marc's Complete Email Activity (Graph API) - Last ${days} Days`,
        `<p>Comprehensive report of ALL emails sent by marc@gbl-data.com over the past ${days} days using Microsoft Graph API with Mail.ReadBasic.All permission.</p>
        ${analyticsHTML}
        ${emailTableHTML}`,
        recipient
      );
    }
    
    logger.info('Marc Graph API report sent successfully');
  } catch (error) {
    logger.error('Error in Marc Graph API report generation:', {
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

// Run the function if this script is executed directly
if (require.main === module) {
  // Parse command line arguments
  const args = process.argv.slice(2);
  const daysArg = args.find(arg => arg.startsWith('--days='));
  const recipientArg = args.find(arg => arg.startsWith('--recipient='));
  
  const days = daysArg ? parseInt(daysArg.split('=')[1], 10) : 14;
  const recipient = recipientArg ? recipientArg.split('=')[1] : process.env.SUMMARY_RECIPIENT;
  
  sendMarcGraphReport(days, recipient)
    .catch(err => {
      logger.error('Error running Marc Graph API report:', {
        error: err.message,
        stack: err.stack
      });
    });
}

module.exports = { sendMarcGraphReport, getMessageTrace };