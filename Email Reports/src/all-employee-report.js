require('dotenv').config();
require('isomorphic-fetch');
const { ClientSecretCredential } = require('@azure/identity');
const { Client } = require('@microsoft/microsoft-graph-client');
const { TokenCredentialAuthenticationProvider } = require('@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials');
const logger = require('./logger');
const { summarizeEmails } = require('./summarizer');
const { sendCustomEmail } = require('./email-sender');
const fs = require('fs');
const path = require('path');

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
 * Get all active users in the organization using Mail.ReadBasic.All permission
 */
async function getAllUsers() {
  try {
    logger.info('Getting all users in the organization');
    const client = createGraphClient();
    
    const usersResponse = await client.api('/users')
      .select('id,userPrincipalName,mail,displayName')
      .filter('accountEnabled eq true')
      .top(999)
      .get();
    
    if (usersResponse && usersResponse.value) {
      const validUsers = usersResponse.value.filter(user => 
        user.mail && user.userPrincipalName && user.userPrincipalName.includes('@')
      );
      logger.info(`Found ${validUsers.length} active users in the organization`);
      return validUsers;
    }
    
    return [];
  } catch (error) {
    logger.error('Error getting organization users:', {
      error: error.message,
      stack: error.stack
    });
    // Fallback to company employees list
    return [];
  }
}

/**
 * Try to access message trace logs (requires Exchange Administrator role)
 */
async function getMessageTraceExchangeAdmin(days, senderAddress) {
  try {
    logger.info(`Attempting to access message trace logs for ${senderAddress} (requires Exchange Admin role)`);
    const client = createGraphClient();
    const { startDate, endDate } = getLastXDaysRange(days);
    
    // Format dates for reporting API
    const formattedStartDate = startDate.split('T')[0];
    const formattedEndDate = endDate.split('T')[0];
    
    // Try to get message trace data through reporting API
    const messageTraceReport = await client.api('/reports/getEmailActivityUserDetail(period=\'D7\')')
      .get();
    
    if (messageTraceReport) {
      logger.info('Successfully accessed message trace reporting API');
      return true;
    }
    
    return false;
  } catch (error) {
    logger.warn(`Could not access message trace logs: ${error.message}`);
    logger.warn('You may need Exchange Administrator role for comprehensive message traces');
    return false;
  }
}

/**
 * Get all emails sent by a user in the organization
 */
async function getAllSentEmails(days, senderAddress) {
  try {
    logger.info(`Getting all emails sent by ${senderAddress} for the last ${days} days`);
    const client = createGraphClient();
    const { startDate, endDate } = getLastXDaysRange(days);
    
    // Track all emails with a Map to avoid duplicates
    const emailMap = new Map();
    
    // First try to use Exchange Admin features if available
    const hasExchangeAdminAccess = await getMessageTraceExchangeAdmin(days, senderAddress);
    
    // APPROACH 1: Direct access to sender's sent items folder
    logger.info(`Approach 1: Querying ${senderAddress}'s sent items folder`);
    
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
    
    // APPROACH 2: Check archive mailbox for BCC'd copies of all sent emails
    logger.info('Approach 2: Checking archive mailbox for emails from sender');
    
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
          emailMap.set(message.id, formatEmailMessage(message, 'archive_mailbox'));
        });
      } else {
        logger.info(`No archived emails found from ${senderAddress}`);
      }
    } catch (archiveError) {
      logger.warn(`Error accessing archive mailbox: ${archiveError.message}`);
    }
    
    // APPROACH 3: Organization-wide search for emails from the sender
    logger.info('Approach 3: Organization-wide search for emails from sender');
    
    // Get all users
    const allUsers = await getAllUsers();
    
    // Create a list of all mailboxes to check
    let mailboxes = allUsers.length > 0 
      ? allUsers.map(user => user.userPrincipalName)
      : [
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
    
    // Remove the sender from the list
    mailboxes = mailboxes.filter(email => email.toLowerCase() !== senderAddress.toLowerCase());
    
    // Process in batches to avoid timeouts
    const BATCH_SIZE = 5;
    for (let i = 0; i < mailboxes.length; i += BATCH_SIZE) {
      const batch = mailboxes.slice(i, i + BATCH_SIZE);
      
      logger.info(`Processing batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(mailboxes.length/BATCH_SIZE)} (${batch.length} mailboxes)`);
      
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
    
    // Convert map to array and sort by date
    const allEmails = Array.from(emailMap.values())
      .sort((a, b) => new Date(b.date) - new Date(a.date));
    
    logger.info(`Total unique emails found for ${senderAddress}: ${allEmails.length}`);
    
    // Generate some quick stats
    if (allEmails.length > 0) {
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
      
      // Count by date
      const dateCount = {};
      allEmails.forEach(email => {
        const dateStr = email.date.toISOString().split('T')[0];
        dateCount[dateStr] = (dateCount[dateStr] || 0) + 1;
      });
      
      // Log the most active days
      const topDays = Object.entries(dateCount)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3);
      
      if (topDays.length > 0) {
        logger.info(`Most active days for ${senderAddress}:`);
        topDays.forEach(([date, count]) => {
          logger.info(`- ${date}: ${count} emails`);
        });
      }
    }
    
    return allEmails;
  } catch (error) {
    logger.error(`Error getting emails for ${senderAddress}:`, {
      error: error.message,
      stack: error.stack
    });
    return [];
  }
}

/**
 * Generate analytics on emails
 */
function generateEmailAnalytics(emails, employeeEmail) {
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
  
  // Get employee domain
  const employeeDomain = employeeEmail.split('@')[1].toLowerCase();
  
  // Count company vs external recipients
  const companyRecipients = [...uniqueRecipients].filter(r => r.endsWith(employeeDomain)).length;
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
    sourceCounts: sourceCounts,
    // Calculate daily average
    dailyAverage: (emails.length / Object.keys(dateCounts).length).toFixed(1),
    // Get most active day
    mostActiveDay: Object.entries(dateCounts)
      .sort((a, b) => b[1] - a[1])
      .shift()
  };
}

/**
 * Generate a report for a single employee
 */
async function generateEmployeeReport(employeeEmail, days) {
  try {
    logger.info(`Generating email report for ${employeeEmail} over the last ${days} days`);
    
    // Get all sent emails for this employee
    const emails = await getAllSentEmails(days, employeeEmail);
    
    if (emails.length === 0) {
      logger.info(`No emails found for ${employeeEmail} in the last ${days} days`);
      return {
        employeeEmail,
        totalEmails: 0,
        analytics: null,
        summaries: null
      };
    }
    
    // Generate analytics
    const analytics = generateEmailAnalytics(emails, employeeEmail);
    logger.info(`Generated analytics for ${emails.length} emails from ${employeeEmail}`);
    
    // Organize emails for summarization
    const emailsFormatted = { [`${employeeEmail} sent`]: emails };
    
    // Summarize emails if there aren't too many
    let summaries = null;
    if (emails.length <= 100) {
      try {
        summaries = await summarizeEmails(emailsFormatted);
        logger.info(`Email summarization complete for ${employeeEmail}`);
      } catch (summaryError) {
        logger.error(`Error summarizing emails for ${employeeEmail}:`, {
          error: summaryError.message,
          stack: summaryError.stack
        });
      }
    } else {
      logger.info(`Too many emails (${emails.length}) to summarize for ${employeeEmail}. Skipping summarization.`);
    }
    
    return {
      employeeEmail,
      totalEmails: emails.length,
      analytics,
      summaries,
      emails: emails.slice(0, 50) // Include a sample of recent emails
    };
  } catch (error) {
    logger.error(`Error generating report for ${employeeEmail}:`, {
      error: error.message,
      stack: error.stack
    });
    
    return {
      employeeEmail,
      error: error.message
    };
  }
}

/**
 * Send a comprehensive report for all employees
 */
async function sendAllEmployeeReport(days = 14, recipient) {
  try {
    logger.info(`Starting comprehensive email report for all employees over the last ${days} days`);
    
    // Get all users in the organization
    const allUsers = await getAllUsers();
    
    // If we couldn't get users, use a predefined list
    const employees = allUsers.length > 0 
      ? allUsers.map(user => user.userPrincipalName)
      : [
          'jared@gbl-data.com',
          'marc@gbl-data.com',
          'tim@gbl-data.com',
          'clint@gbl-data.com',
          'rebeca@gbl-data.com',
          'john@gbl-data.com',
          'darren@gbl-data.com',
          'galina@gbl-data.com'
        ];
    
    // Track overall stats
    const overallStats = {
      totalEmployees: employees.length,
      totalEmailsSent: 0,
      employeesSendingEmails: 0,
      employeeReports: []
    };
    
    // Process each employee
    for (const employee of employees) {
      logger.info(`Processing ${employee} (${employees.indexOf(employee) + 1}/${employees.length})`);
      
      const report = await generateEmployeeReport(employee, days);
      overallStats.employeeReports.push(report);
      
      if (report.totalEmails > 0) {
        overallStats.totalEmailsSent += report.totalEmails;
        overallStats.employeesSendingEmails++;
      }
    }
    
    // Calculate team average
    overallStats.averageEmailsPerEmployee = (overallStats.totalEmailsSent / overallStats.employeesSendingEmails).toFixed(1);
    
    // Sort employees by email volume
    overallStats.employeeReports.sort((a, b) => (b.totalEmails || 0) - (a.totalEmails || 0));
    
    // Generate HTML report
    const reportHtml = generateAllEmployeeReportHtml(overallStats, days);
    
    // Send the report
    await sendCustomEmail(
      null,
      `Team Email Activity Report - Last ${days} Days`,
      reportHtml,
      recipient
    );
    
    logger.info('All employee report sent successfully');
    
    // Save a local copy
    const reportsDir = path.join(__dirname, '..', 'reports');
    if (!fs.existsSync(reportsDir)) {
      fs.mkdirSync(reportsDir);
    }
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const reportPath = path.join(reportsDir, `team_email_report_${timestamp}.html`);
    fs.writeFileSync(reportPath, reportHtml);
    
    logger.info(`Report saved to ${reportPath}`);
    
    return {
      success: true,
      totalEmployees: overallStats.totalEmployees,
      totalEmailsSent: overallStats.totalEmailsSent
    };
  } catch (error) {
    logger.error('Error sending all employee report:', {
      error: error.message,
      stack: error.stack
    });
    
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Generate HTML report for all employees
 */
function generateAllEmployeeReportHtml(overallStats, days) {
  // Generate the team overview section
  const teamOverviewHtml = `
  <div style="margin-bottom: 30px;">
    <h1>Team Email Activity Report - Last ${days} Days</h1>
    <p>This report shows email activity for all team members over the past ${days} days.</p>
    
    <h2>Team Overview</h2>
    <p><strong>Total Employees:</strong> ${overallStats.totalEmployees}</p>
    <p><strong>Employees Sending Emails:</strong> ${overallStats.employeesSendingEmails}</p>
    <p><strong>Total Emails Sent:</strong> ${overallStats.totalEmailsSent}</p>
    <p><strong>Average Emails per Active Employee:</strong> ${overallStats.averageEmailsPerEmployee}</p>
    
    <h3>Email Volume by Employee</h3>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
      <tr style="background-color: #f2f2f2;">
        <th>Employee</th>
        <th>Emails Sent</th>
        <th>Unique Recipients</th>
        <th>Company Recipients</th>
        <th>External Recipients</th>
        <th>Daily Average</th>
      </tr>
      ${overallStats.employeeReports.map(report => `
      <tr>
        <td>${report.employeeEmail}</td>
        <td>${report.totalEmails || 0}</td>
        <td>${report.analytics?.uniqueRecipients || 0}</td>
        <td>${report.analytics?.companyRecipients || 0}</td>
        <td>${report.analytics?.externalRecipients || 0}</td>
        <td>${report.analytics?.dailyAverage || 0}</td>
      </tr>
      `).join('')}
    </table>
  </div>
  `;
  
  // Generate individual employee sections
  const employeeSectionsHtml = overallStats.employeeReports
    .filter(report => report.totalEmails > 0)
    .map(report => {
      const analytics = report.analytics;
      
      // Create the employee section
      return `
      <div style="margin-bottom: 40px; border-top: 1px solid #ccc; padding-top: 20px;">
        <h2>${report.employeeEmail} Activity</h2>
        <p><strong>Total Emails Sent:</strong> ${report.totalEmails}</p>
        <p><strong>Unique Recipients:</strong> ${analytics.uniqueRecipients}</p>
        <p><strong>Daily Average:</strong> ${analytics.dailyAverage} emails</p>
        <p><strong>Most Active Day:</strong> ${analytics.mostActiveDay ? `${analytics.mostActiveDay[0]} (${analytics.mostActiveDay[1]} emails)` : 'N/A'}</p>
        
        <h3>Recipient Breakdown</h3>
        <ul>
          <li><strong>Company Recipients:</strong> ${analytics.companyRecipients}</li>
          <li><strong>External Recipients:</strong> ${analytics.externalRecipients}</li>
        </ul>
        
        <h3>Top Recipient Domains</h3>
        <ul>
          ${analytics.domainCounts.slice(0, 5).map(d => `<li>${d.domain}: ${d.count} recipients</li>`).join('')}
        </ul>
        
        <h3>Daily Email Activity</h3>
        <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
          <tr style="background-color: #f2f2f2;">
            <th>Date</th>
            <th>Emails Sent</th>
          </tr>
          ${analytics.dateCounts.slice(0, 7).map(d => `
          <tr>
            <td>${d.date}</td>
            <td>${d.count}</td>
          </tr>
          `).join('')}
        </table>
        
        ${report.summaries ? `
        <h3>Email Summaries</h3>
        <div>${report.summaries[`${report.employeeEmail} sent`]}</div>
        ` : `
        <h3>Recent Emails (sample)</h3>
        <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
          <tr style="background-color: #f2f2f2;">
            <th>Date</th>
            <th>Subject</th>
            <th>Recipients</th>
          </tr>
          ${report.emails.slice(0, 10).map(email => `
          <tr>
            <td>${email.date.toISOString().split('T')[0]}</td>
            <td>${email.subject}</td>
            <td>${(email.to || []).map(r => r.address).join(', ')}</td>
          </tr>
          `).join('')}
        </table>
        `}
      </div>
      `;
    }).join('');
  
  // Combine all sections
  return `
  <!DOCTYPE html>
  <html>
  <head>
    <title>Team Email Activity Report - Last ${days} Days</title>
    <style>
      body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 1200px; margin: 0 auto; padding: 20px; }
      h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
      h2 { color: #2980b9; margin-top: 30px; }
      h3 { color: #3498db; margin-top: 20px; }
      table { width: 100%; border-collapse: collapse; margin: 20px 0; }
      th { background-color: #f2f2f2; text-align: left; padding: 10px; }
      td { padding: 10px; border: 1px solid #ddd; }
      ul { padding-left: 20px; }
      .summary { background-color: #f9f9f9; padding: 15px; border-left: 4px solid #3498db; margin: 20px 0; }
    </style>
  </head>
  <body>
    ${teamOverviewHtml}
    ${employeeSectionsHtml}
    
    <footer style="margin-top: 50px; border-top: 1px solid #eee; padding-top: 20px; font-size: 0.8em; color: #777;">
      <p>Generated on ${new Date().toISOString().split('T')[0]} using Microsoft Graph API</p>
    </footer>
  </body>
  </html>
  `;
}

// Run the function if this script is executed directly
if (require.main === module) {
  // Parse command line arguments
  const args = process.argv.slice(2);
  const daysArg = args.find(arg => arg.startsWith('--days='));
  const recipientArg = args.find(arg => arg.startsWith('--recipient='));
  const singleEmployeeArg = args.find(arg => arg.startsWith('--employee='));
  
  const days = daysArg ? parseInt(daysArg.split('=')[1], 10) : 14;
  const recipient = recipientArg ? recipientArg.split('=')[1] : process.env.SUMMARY_RECIPIENT;
  
  if (singleEmployeeArg) {
    // Generate report for a single employee
    const employee = singleEmployeeArg.split('=')[1];
    generateEmployeeReport(employee, days)
      .then(report => {
        console.log(`Generated report for ${employee}:`);
        console.log(`- Total emails: ${report.totalEmails}`);
        if (report.analytics) {
          console.log(`- Unique recipients: ${report.analytics.uniqueRecipients}`);
          console.log(`- Daily average: ${report.analytics.dailyAverage}`);
        }
      })
      .catch(err => {
        logger.error(`Error generating report for ${employee}:`, {
          error: err.message,
          stack: err.stack
        });
      });
  } else {
    // Generate report for all employees
    sendAllEmployeeReport(days, recipient)
      .then(result => {
        if (result.success) {
          logger.info(`All employee report completed successfully: ${result.totalEmailsSent} emails from ${result.totalEmployees} employees`);
        } else {
          logger.error(`Error in all employee report: ${result.error}`);
        }
      })
      .catch(err => {
        logger.error('Error running all employee report:', {
          error: err.message,
          stack: err.stack
        });
      });
  }
}

module.exports = { sendAllEmployeeReport, generateEmployeeReport };