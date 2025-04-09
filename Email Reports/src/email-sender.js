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
 * Formats the email summaries into a readable HTML template
 */
function formatSummaryEmailContent(summaries) {
  // Log the summaries object structure to debug
  console.log('Summaries object keys:', Object.keys(summaries));
  
  // Loop through each key and log the number of emails
  Object.keys(summaries).forEach(key => {
    console.log(`Key: ${key}, Number of emails: ${summaries[key].length}`);
  });
  
  const date = new Date().toLocaleDateString('en-US', { 
    weekday: 'long', 
    year: 'numeric', 
    month: 'long', 
    day: 'numeric' 
  });
  
  let totalEmails = 0;
  Object.values(summaries).forEach(employeeEmails => {
    totalEmails += employeeEmails.length;
  });
  
  let html = `
  <html>
    <head>
      <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 800px; margin: 0 auto; padding: 20px; }
        .header { background-color: #f5f5f5; padding: 10px; border-radius: 4px; margin-bottom: 20px; }
        .employee-section { margin-bottom: 30px; }
        .employee-section h3 { 
          background-color: #e9ecef; 
          padding: 8px; 
          border-radius: 4px;
          margin-bottom: 10px;
        }
        .email-summary { margin-bottom: 15px; padding-bottom: 15px; border-bottom: 1px solid #eee; }
        .email-subject { font-weight: bold; }
        .email-date { color: #666; font-size: 0.9em; }
        .footer { margin-top: 30px; font-size: 0.9em; color: #666; }
        .sent-section { border-left: 4px solid #28a745; padding-left: 15px; }
        .received-section { border-left: 4px solid #007bff; padding-left: 15px; }
        .empty-section { color: #6c757d; font-style: italic; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h2>Daily Email Summary</h2>
          <p>Date: ${date}</p>
          <p>Total Emails: ${totalEmails}</p>
        </div>
  `;
  
  // Get all employee names - extract first part before @ and before " sent"/" received"
  const employeeNames = new Set();
  Object.keys(summaries).forEach(key => {
    // More robust pattern matching to handle all email addresses
    const match = key.match(/^(.+?)@.*? (sent|received)$/);
    if (match) {
      employeeNames.add(match[1]);
    }
  });
  
  // Sort employee names
  const sortedEmployees = Array.from(employeeNames).sort();
  
  // For each employee, add sent and received sections
  sortedEmployees.forEach(employee => {
    // Ensure we match the exact format of keys in the summaries object
    const sentKey = Object.keys(summaries).find(key => key.includes(`${employee}@`) && key.endsWith(' sent'));
    const receivedKey = Object.keys(summaries).find(key => key.includes(`${employee}@`) && key.endsWith(' received'));
    
    const sentEmails = sentKey ? summaries[sentKey] : [];
    const receivedEmails = receivedKey ? summaries[receivedKey] : [];
    
    // Only add the employee section if they have either sent or received emails
    if (sentEmails.length > 0 || receivedEmails.length > 0) {
      html += `
        <div class="employee-section">
          <h3>${employee}@gbl-data.com</h3>
      `;
      
      // Sent emails section
      html += `
          <div class="sent-section">
            <h4>${employee}'s sent emails (${sentEmails.length})</h4>
      `;
      
      if (sentEmails.length > 0) {
        sentEmails.forEach(email => {
          try {
            const emailDate = new Date(email.date).toLocaleTimeString('en-US', {
              hour: '2-digit',
              minute: '2-digit'
            });
            
            html += `
              <div class="email-summary">
                <div class="email-subject">${email.subject || '(No Subject)'}</div>
                <div class="email-date">${emailDate}</div>
                <pre style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; white-space: pre-wrap; margin: 0; padding: 0;">${email.summary || 'No summary available'}</pre>
              </div>
            `;
          } catch (error) {
            console.error('Error formatting email:', error);
            console.log('Problem email:', email);
          }
        });
      } else {
        html += `<div class="empty-section">No sent emails today</div>`;
      }
      
      html += `
          </div>
      `;
      
      // Received emails section
      html += `
          <div class="received-section">
            <h4>${employee}'s received emails (${receivedEmails.length})</h4>
      `;
      
      if (receivedEmails.length > 0) {
        receivedEmails.forEach(email => {
          try {
            const emailDate = new Date(email.date).toLocaleTimeString('en-US', {
              hour: '2-digit',
              minute: '2-digit'
            });
            
            html += `
              <div class="email-summary">
                <div class="email-subject">${email.subject || '(No Subject)'}</div>
                <div class="email-date">${emailDate}</div>
                <pre style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; white-space: pre-wrap; margin: 0; padding: 0;">${email.summary || 'No summary available'}</pre>
              </div>
            `;
          } catch (error) {
            console.error('Error formatting email:', error);
            console.log('Problem email:', email);
          }
        });
      } else {
        html += `<div class="empty-section">No received emails today</div>`;
      }
      
      html += `
          </div>
        </div>
      `;
    }
  });
  
  // Close the HTML
  html += `
        <div class="footer">
          <p>This is an automated summary generated at ${new Date().toLocaleTimeString()}</p>
        </div>
      </div>
    </body>
  </html>
  `;
  
  return html;
}

/**
 * Format a custom email report
 */
function formatCustomEmailContent(summaries, title, description) {
  const date = new Date().toLocaleDateString('en-US', { 
    weekday: 'long', 
    year: 'numeric', 
    month: 'long', 
    day: 'numeric' 
  });
  
  let totalEmails = 0;
  Object.values(summaries).forEach(employeeEmails => {
    totalEmails += employeeEmails.length;
  });
  
  let html = `
  <html>
    <head>
      <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 800px; margin: 0 auto; padding: 20px; }
        .header { background-color: #f5f5f5; padding: 10px; border-radius: 4px; margin-bottom: 20px; }
        .employee-section { margin-bottom: 30px; }
        .employee-section h3 { 
          background-color: #e9ecef; 
          padding: 8px; 
          border-radius: 4px;
          margin-bottom: 10px;
        }
        .email-summary { margin-bottom: 15px; padding-bottom: 15px; border-bottom: 1px solid #eee; }
        .email-subject { font-weight: bold; }
        .email-date { color: #666; font-size: 0.9em; }
        .footer { margin-top: 30px; font-size: 0.9em; color: #666; }
        .sent-section { border-left: 4px solid #28a745; padding-left: 15px; }
        .received-section { border-left: 4px solid #007bff; padding-left: 15px; }
        .empty-section { color: #6c757d; font-style: italic; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h2>${title}</h2>
          <p>Generated on: ${date}</p>
          <p>${description}</p>
          <p>Total Emails: ${totalEmails}</p>
        </div>
  `;
  
  // For each category in the summaries, add a section
  Object.entries(summaries).forEach(([category, emails]) => {
    if (emails.length > 0) {
      // Determine if this is a sent or received section
      const isSent = category.includes('sent');
      const sectionClass = isSent ? 'sent-section' : 'received-section';
      
      html += `
        <div class="employee-section">
          <h3>${category}</h3>
          <div class="${sectionClass}">
      `;
      
      // Group emails by date
      const emailsByDate = {};
      emails.forEach(email => {
        const dateStr = new Date(email.date).toLocaleDateString('en-US', { 
          year: 'numeric', 
          month: 'long', 
          day: 'numeric' 
        });
        if (!emailsByDate[dateStr]) {
          emailsByDate[dateStr] = [];
        }
        emailsByDate[dateStr].push(email);
      });
      
      // Sort dates in descending order
      const sortedDates = Object.keys(emailsByDate).sort((a, b) => 
        new Date(b) - new Date(a)
      );
      
      // Add emails grouped by date
      sortedDates.forEach(dateStr => {
        html += `<h4>${dateStr} (${emailsByDate[dateStr].length} emails)</h4>`;
        
        emailsByDate[dateStr].forEach(email => {
          const emailTime = new Date(email.date).toLocaleTimeString('en-US', {
            hour: '2-digit',
            minute: '2-digit'
          });
          
          html += `
            <div class="email-summary">
              <div class="email-subject">${email.subject}</div>
              <div class="email-date">${emailTime}</div>
              <pre style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; white-space: pre-wrap; margin: 0; padding: 0;">${email.summary}</pre>
            </div>
          `;
        });
      });
      
      html += `
          </div>
        </div>
      `;
    }
  });
  
  // Close the HTML
  html += `
        <div class="footer">
          <p>This is an automated report generated at ${new Date().toLocaleTimeString()}</p>
        </div>
      </div>
    </body>
  </html>
  `;
  
  return html;
}

/**
 * Sends the summary email using Microsoft Graph API
 */
async function sendSummaryEmail(summaries) {
  try {
    const client = createGraphClient();
    const recipient = process.env.SUMMARY_RECIPIENT;
    const sender = process.env.MICROSOFT_USER_EMAIL;
    
    if (!recipient) {
      throw new Error('No recipient email configured in SUMMARY_RECIPIENT');
    }
    
    if (!sender) {
      throw new Error('No sender email configured in MICROSOFT_USER_EMAIL');
    }
    
    logger.info(`Sending email as: ${sender}`);
    
    const date = new Date().toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric' 
    });
    
    const emailContent = formatSummaryEmailContent(summaries);
    
    // Create the email message
    const message = {
      subject: `Daily Email Summary - ${date}`,
      body: {
        contentType: 'HTML',
        content: emailContent
      },
      toRecipients: [
        {
          emailAddress: {
            address: recipient
          }
        }
      ]
    };
    
    logger.info(`Sending email summary to ${recipient}`);
    
    // Use the users endpoint instead of /me
    await client.api(`/users/${sender}/sendMail`).post({
      message,
      saveToSentItems: true
    });
    
    logger.info('Email sent successfully');
    return { success: true };
  } catch (error) {
    logger.error('Error sending summary email', { 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}

/**
 * Sends a custom email report using Microsoft Graph API
 * @param {Object} summaries - Email summaries organized by category
 * @param {string} title - Email title
 * @param {string} description - Email description
 * @param {string} [customRecipient] - Optional custom recipient email
 */
async function sendCustomEmail(summaries, title, description, customRecipient) {
  try {
    const client = createGraphClient();
    const recipient = customRecipient || process.env.SUMMARY_RECIPIENT;
    const sender = process.env.MICROSOFT_USER_EMAIL;
    
    if (!recipient) {
      throw new Error('No recipient email configured in SUMMARY_RECIPIENT');
    }
    
    if (!sender) {
      throw new Error('No sender email configured in MICROSOFT_USER_EMAIL');
    }
    
    logger.info(`Sending custom email as: ${sender}`);
    
    const emailContent = formatCustomEmailContent(summaries, title, description);
    
    // Create the email message
    const message = {
      subject: title,
      body: {
        contentType: 'HTML',
        content: emailContent
      },
      toRecipients: [
        {
          emailAddress: {
            address: recipient
          }
        }
      ]
    };
    
    logger.info(`Sending custom email to ${recipient}`);
    
    // Use the users endpoint instead of /me
    await client.api(`/users/${sender}/sendMail`).post({
      message,
      saveToSentItems: true
    });
    
    logger.info('Custom email sent successfully');
    return { success: true };
  } catch (error) {
    logger.error('Error sending custom email', { 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}

module.exports = {
  sendSummaryEmail,
  sendCustomEmail
};