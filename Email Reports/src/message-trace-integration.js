require('dotenv').config();
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');
const logger = require('./logger');
const { summarizeEmails } = require('./summarizer');
const { sendCustomEmail } = require('./email-sender');

// Check if running on Windows or not
const isWindows = os.platform() === 'win32';

/**
 * Executes a PowerShell command and returns the result as a Promise
 * @param {string} psCommand - The PowerShell command to execute
 * @returns {Promise<string>} - The command output
 */
function executePowerShell(psCommand) {
  if (!isWindows) {
    return Promise.reject(new Error('PowerShell commands are only supported on Windows. This feature is not available on macOS or Linux.'));
  }
  
  return new Promise((resolve, reject) => {
    // Use PowerShell to execute the command
    const ps = exec(`powershell -Command "${psCommand}"`, { maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
      if (error) {
        reject(error);
        return;
      }
      if (stderr) {
        logger.warn(`PowerShell stderr: ${stderr}`);
      }
      resolve(stdout);
    });
  });
}

/**
 * Parse CSV data into JSON objects
 * @param {string} csvData - The CSV data as a string
 * @returns {Array} - Array of objects parsed from CSV
 */
function parseCSV(csvData) {
  // Simple CSV parser (for production, consider using a robust library like 'csv-parser')
  const lines = csvData.trim().split('\n');
  const headers = lines[0].split(',').map(header => header.replace(/^"(.*)"$/, '$1').trim());
  
  return lines.slice(1).map(line => {
    const values = line.split(',').map(value => value.replace(/^"(.*)"$/, '$1').trim());
    const obj = {};
    headers.forEach((header, index) => {
      obj[header] = values[index] || '';
    });
    return obj;
  });
}

/**
 * Runs a message trace for Marc's emails using PowerShell and returns the results
 * @param {number} days - Number of days to look back
 * @returns {Promise<Array>} - Array of email objects
 */
async function getMessageTraceForMarc(days = 14) {
  try {
    logger.info(`Running message trace for marc@gbl-data.com for the last ${days} days`);
    
    // Check if running on non-Windows platform
    if (!isWindows) {
      logger.error('Message trace using PowerShell is only supported on Windows.');
      throw new Error('This feature requires PowerShell which is only available on Windows. Please use the Graph API method instead with --marc-graph parameter.');
    }
    
    // Step 1: Create a temporary directory for our output files if it doesn't exist
    const tempDir = path.join(__dirname, '..', 'temp');
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir);
    }
    
    // Step 2: Define the output CSV file path
    const outputCSV = path.join(tempDir, `marc_message_trace_${Date.now()}.csv`);
    
    // Step 3: Prepare the PowerShell script to run message trace
    // Note: In a production environment, you should use certificate-based authentication
    // for automated scripts rather than interactive login
    const psScript = `
    # Step 1: Import the Exchange Online module
    Import-Module ExchangeOnlineManagement -ErrorAction SilentlyContinue
    
    # Step 2: Connect to Exchange Online
    # In production, use: Connect-ExchangeOnline -CertificateThumbprint "YOUR_THUMBPRINT" -AppId "YOUR_APP_ID" -Organization "gbl-data.com"
    # For this demo, we'll use interactive login (will prompt for credentials)
    # USER INPUT NEEDED HERE: The admin will need to log in interactively
    Connect-ExchangeOnline
    
    # Step 3: Set the date range
    $startDate = (Get-Date).AddDays(-${days})
    $endDate = Get-Date
    
    # Step 4: Run the message trace and export to CSV
    Get-MessageTrace -SenderAddress marc@gbl-data.com -StartDate $startDate -EndDate $endDate | 
        Select-Object MessageId, Received, SenderAddress, RecipientAddress, Subject, Status, Size, MessageTraceId, FromIP, ToIP |
        Export-Csv -Path "${outputCSV}" -NoTypeInformation
    
    # Step 5: Disconnect from Exchange Online
    Disconnect-ExchangeOnline -Confirm:$false
    `;
    
    // Step 4: Execute the PowerShell script
    logger.info('Executing PowerShell script to get message trace data...');
    await executePowerShell(psScript);
    
    // Step 5: Read and parse the CSV file
    logger.info(`Reading message trace results from ${outputCSV}`);
    if (!fs.existsSync(outputCSV)) {
      throw new Error('Message trace CSV file was not created. PowerShell script may have failed.');
    }
    
    const csvData = fs.readFileSync(outputCSV, 'utf8');
    const traceResults = parseCSV(csvData);
    
    logger.info(`Found ${traceResults.length} emails sent by marc@gbl-data.com in the message trace`);
    
    // Step 6: Clean up
    fs.unlinkSync(outputCSV);
    
    // Step 7: Format the trace results into the expected email format
    const formattedEmails = traceResults.map(trace => {
      const recipientParts = trace.RecipientAddress.split('@');
      const recipientDomain = recipientParts.length > 1 ? recipientParts[1] : '';
      
      return {
        id: trace.MessageId,
        subject: trace.Subject || '(No Subject)',
        from: {
          text: 'Marc Perkins',
          address: trace.SenderAddress
        },
        to: [
          {
            address: trace.RecipientAddress,
            name: trace.RecipientAddress
          }
        ],
        date: new Date(trace.Received),
        text: `Email sent to ${trace.RecipientAddress}`,
        html: null,
        importance: 'normal',
        direction: 'sent',
        status: trace.Status,
        size: trace.Size,
        recipientDomain: recipientDomain
      };
    });
    
    return formattedEmails;
  } catch (error) {
    logger.error('Error getting message trace:', {
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

/**
 * Generates analytics on the emails
 * @param {Array} emails - Array of email objects
 * @returns {Object} - Email analytics
 */
function generateEmailAnalytics(emails) {
  // Group emails by domain
  const domainCounts = {};
  emails.forEach(email => {
    if (email.to && email.to.length > 0) {
      const address = email.to[0].address;
      const domain = address.split('@')[1];
      
      if (domain) {
        domainCounts[domain] = (domainCounts[domain] || 0) + 1;
      }
    }
  });
  
  // Sort domains by count
  const sortedDomains = Object.entries(domainCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([domain, count]) => ({ domain, count }));
  
  // Group emails by day
  const dailyCounts = {};
  emails.forEach(email => {
    const day = email.date.toISOString().split('T')[0];
    dailyCounts[day] = (dailyCounts[day] || 0) + 1;
  });
  
  // Get email status counts
  const statusCounts = {};
  emails.forEach(email => {
    statusCounts[email.status] = (statusCounts[email.status] || 0) + 1;
  });
  
  return {
    totalEmails: emails.length,
    uniqueRecipients: new Set(emails.map(email => email.to[0].address)).size,
    uniqueDomains: Object.keys(domainCounts).length,
    topDomains: sortedDomains.slice(0, 10),
    dailyActivity: dailyCounts,
    statusCounts: statusCounts
  };
}

/**
 * Formats and sends the Marc report using message trace data
 * @param {number} days - Number of days to look back
 * @param {string} recipient - Email address to send the report to
 */
async function sendMarcCompleteReport(days = 14, recipient) {
  try {
    // Check if running on non-Windows platform
    if (!isWindows) {
      logger.error('This feature requires PowerShell which is only available on Windows.');
      
      // Send an email explaining the issue instead
      await sendCustomEmail(
        null, 
        `Marc's Complete Email Activity - Not Available`, 
        `<h2>Feature Not Available on macOS</h2>
        <p>The PowerShell-based Message Trace report is only available on Windows systems.</p>
        <p>Please use the Graph API method instead by running:</p>
        <pre>node src/index.js --marc-graph --days=${days}</pre>
        <p>The Graph API method doesn't require PowerShell and works on all platforms.</p>`,
        recipient
      );
      
      return;
    }
    
    // Step 1: Get the message trace data
    logger.info(`Starting comprehensive Marc report for last ${days} days`);
    const emails = await getMessageTraceForMarc(days);
    
    if (emails.length === 0) {
      logger.info('No emails found in message trace');
      return;
    }
    
    // Step 2: Generate analytics
    const analytics = generateEmailAnalytics(emails);
    logger.info(`Email analytics generated: ${analytics.totalEmails} total emails to ${analytics.uniqueRecipients} unique recipients`);
    
    // Step 3: Organize by recipient/date
    const emailsMarcSent = { 'marc@gbl-data.com sent': emails };
    
    // Step 4: Summarize emails if there aren't too many
    let summaries = null;
    if (emails.length <= 100) {
      // Only summarize if there's a reasonable number of emails
      summaries = await summarizeEmails(emailsMarcSent);
    }
    
    // Step 5: Prepare analytics HTML
    const analyticsHTML = `
    <h2>Email Analytics</h2>
    <p><strong>Total Emails Sent:</strong> ${analytics.totalEmails}</p>
    <p><strong>Unique Recipients:</strong> ${analytics.uniqueRecipients}</p>
    <p><strong>Unique Domains:</strong> ${analytics.uniqueDomains}</p>
    
    <h3>Top Recipient Domains</h3>
    <ul>
      ${analytics.topDomains.map(d => `<li>${d.domain}: ${d.count} emails</li>`).join('')}
    </ul>
    
    <h3>Daily Email Activity</h3>
    <ul>
      ${Object.entries(analytics.dailyActivity)
        .sort((a, b) => b[0].localeCompare(a[0]))
        .map(([day, count]) => `<li>${day}: ${count} emails</li>`)
        .join('')}
    </ul>
    
    <h3>Email Status</h3>
    <ul>
      ${Object.entries(analytics.statusCounts)
        .map(([status, count]) => `<li>${status}: ${count} emails</li>`)
        .join('')}
    </ul>
    `;
    
    // Step 6: Send the report
    if (summaries) {
      // If we have summaries, include them
      await sendCustomEmail(
        summaries, 
        `Marc's Complete Email Activity - Last ${days} Days`, 
        `Comprehensive report of ALL emails sent by marc@gbl-data.com over the past ${days} days using Microsoft 365 Message Trace.${analyticsHTML}`,
        recipient
      );
    } else {
      // If too many emails to summarize, just send analytics
      const emailList = emails.map(email => {
        return `<tr>
          <td>${new Date(email.date).toISOString().split('T')[0]}</td>
          <td>${email.subject}</td>
          <td>${email.to[0].address}</td>
          <td>${email.status}</td>
        </tr>`;
      }).join('');
      
      const emailTable = `
      <h2>Recent Emails (limited to 100)</h2>
      <table border="1" cellpadding="5" cellspacing="0">
        <tr>
          <th>Date</th>
          <th>Subject</th>
          <th>Recipient</th>
          <th>Status</th>
        </tr>
        ${emailList.slice(0, 100)}
      </table>
      `;
      
      await sendCustomEmail(
        null, 
        `Marc's Complete Email Activity - Last ${days} Days`, 
        `Comprehensive report of ALL emails sent by marc@gbl-data.com over the past ${days} days using Microsoft 365 Message Trace.
        ${analyticsHTML}
        ${emailTable}`,
        recipient
      );
    }
    
    logger.info('Marc complete report sent successfully');
  } catch (error) {
    logger.error('Error in Marc complete report generation', { 
      error: error.message, 
      stack: error.stack 
    });
  }
}

/**
 * Run the function if this file is executed directly
 */
if (require.main === module) {
  // Parse command line arguments
  const args = process.argv.slice(2);
  const daysArg = args.find(arg => arg.startsWith('--days='));
  const recipientArg = args.find(arg => arg.startsWith('--recipient='));
  
  const days = daysArg ? parseInt(daysArg.split('=')[1], 10) : 14;
  const recipient = recipientArg ? recipientArg.split('=')[1] : process.env.SUMMARY_RECIPIENT;
  
  sendMarcCompleteReport(days, recipient)
    .catch(err => {
      logger.error('Error running Marc complete report script', { error: err.message });
      process.exit(1);
    });
}

module.exports = { sendMarcCompleteReport, getMessageTraceForMarc };