require('dotenv').config();
const cron = require('node-cron');
const { fetchEmails } = require('./email-fetcher');
const { summarizeEmails } = require('./summarizer');
const { sendSummaryEmail } = require('./email-sender');
const { sendMarcReport } = require('./marc-report');
const { sendMarcCompleteReport } = require('./message-trace-integration');
const { sendMarcGraphReport } = require('./message-trace-graph-api');
const { sendAllEmployeeReport, generateEmployeeReport } = require('./all-employee-report');
const logger = require('./logger');

// Parse command line arguments
const args = process.argv.slice(2);
const runNow = args.includes('--run-now');
const runMarc = args.includes('--marc-report');
const runMarcComplete = args.includes('--marc-complete');
const runMarcGraph = args.includes('--marc-graph');
const runAllEmployee = args.includes('--all-employee');
const singleEmployee = args.find(arg => arg.startsWith('--employee='));
const days = args.find(arg => arg.startsWith('--days='));
const recipient = args.find(arg => arg.startsWith('--recipient='));

// Extract days value if provided
const daysValue = days ? parseInt(days.split('=')[1], 10) : 14;

// Extract recipient if provided
const recipientValue = recipient ? recipient.split('=')[1] : null;

// Company employees list - updated with all team members including sales
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

// List of employees for which to process emails (exclude jared@gbl-data.com)
const EMPLOYEES_TO_PROCESS = COMPANY_EMPLOYEES.filter(email => email.toLowerCase() !== 'jared@gbl-data.com');

async function runDailySummary() {
  try {
    logger.info('Starting daily email summary process');
    
    // Step 1: Fetch today's emails for all users (including jared for context)
    const emails = await fetchEmails(COMPANY_EMPLOYEES);
    logger.info(`Fetched ${emails.length} emails`);
    
    if (emails.length === 0) {
      logger.info('No emails to summarize today');
      return;
    }
    
    // Step 2: Organize emails by employee and direction (sent/received)
    const emailsByEmployeeSentReceived = {};
    
    // Initialize the structure for employees to process (excluding jared)
    EMPLOYEES_TO_PROCESS.forEach(employee => {
      const username = employee.split('@')[0].toLowerCase();
      emailsByEmployeeSentReceived[`${username}@gbl-data.com sent`] = [];
      emailsByEmployeeSentReceived[`${username}@gbl-data.com received`] = [];
    });
    
    // Track processed emails by ID to avoid duplicates
    const processedEmailIds = new Set();
    
    // Debug statistics
    const stats = {
      totalEmails: emails.length,
      duplicatesSkipped: 0,
      sentEmailsProcessed: 0,
      receivedEmailsProcessed: 0,
      byEmployee: {}
    };
    
    // Initialize stats for employees we're processing
    EMPLOYEES_TO_PROCESS.forEach(employee => {
      const username = employee.split('@')[0].toLowerCase();
      stats.byEmployee[username] = { sent: 0, received: 0 };
    });
    
    // Organize emails by direction and employee
    emails.forEach(email => {
      // Skip duplicates (important when querying multiple mailboxes)
      if (processedEmailIds.has(email.id)) {
        stats.duplicatesSkipped++;
        return;
      }
      processedEmailIds.add(email.id);
      
      if (email.direction === 'sent') {
        stats.sentEmailsProcessed++;
        
        // This is a sent email - group by sender
        const from = email.from.address.toLowerCase();
        // Find the matching employee email (case insensitive)
        const matchingEmployee = EMPLOYEES_TO_PROCESS.find(emp => 
          emp.toLowerCase() === from
        );
        
        if (matchingEmployee) {
          const username = matchingEmployee.split('@')[0].toLowerCase();
          emailsByEmployeeSentReceived[`${username}@gbl-data.com sent`].push(email);
          stats.byEmployee[username].sent++;
          
          // Log details about this email for debugging
          logger.info(`Added sent email from ${username}: "${email.subject.substring(0, 30)}..." to ${email.to.map(r => r.address).join(', ')}`);
        }
        
        // Also add to recipient's "received" if it's a company employee we're processing
        email.to.forEach(recipient => {
          const recipientAddress = recipient.address?.toLowerCase();
          if (!recipientAddress) return;
          
          const matchingRecipient = EMPLOYEES_TO_PROCESS.find(emp => 
            emp.toLowerCase() === recipientAddress
          );
          
          if (matchingRecipient) {
            const username = matchingRecipient.split('@')[0].toLowerCase();
            emailsByEmployeeSentReceived[`${username}@gbl-data.com received`].push(email);
            stats.byEmployee[username].received++;
            
            // Log that we added this email to the recipient's "received" category
            logger.info(`Added to ${username}'s received: "${email.subject.substring(0, 30)}..." from ${email.from.address}`);
          }
        });
      } else {
        stats.receivedEmailsProcessed++;
        
        // This is a received email
        // For received emails from our direct mailbox query
        if (email.to && email.to.length > 0) {
          // Check each recipient to see if it's a company employee
          email.to.forEach(recipient => {
            if (recipient.address) {
              const recipientAddress = recipient.address.toLowerCase();
              const matchingRecipient = EMPLOYEES_TO_PROCESS.find(emp => 
                emp.toLowerCase() === recipientAddress
              );
            
              if (matchingRecipient) {
                const username = matchingRecipient.split('@')[0].toLowerCase();
                emailsByEmployeeSentReceived[`${username}@gbl-data.com received`].push(email);
                stats.byEmployee[username].received++;
                
                // Log that we added this email to a company employee's received list
                logger.info(`Added to ${username}'s received: "${email.subject.substring(0, 30)}..." from ${email.from.address}`);
              }
            }
          });
        }
        
        // If sender is a company employee, add to their "sent" category
        const fromAddress = email.from.address?.toLowerCase();
        if (!fromAddress) return;
        
        const matchingSender = EMPLOYEES_TO_PROCESS.find(emp => 
          emp.toLowerCase() === fromAddress
        );
        
        if (matchingSender) {
          const username = matchingSender.split('@')[0].toLowerCase();
          emailsByEmployeeSentReceived[`${username}@gbl-data.com sent`].push(email);
          stats.byEmployee[username].sent++;
          
          // Log details about this email for debugging
          logger.info(`Added to ${username}'s sent: "${email.subject.substring(0, 30)}..." to ${email.to.map(r => r.address).join(', ')}`);
        }
      }
    });
    
    // Log statistics about processed emails
    logger.info('Email processing statistics:', { stats });
    
    // Log all emails in each category for debugging
    Object.keys(emailsByEmployeeSentReceived).forEach(key => {
      const emails = emailsByEmployeeSentReceived[key];
      logger.info(`Category ${key} has ${emails.length} emails`);
      
      if (emails.length > 0) {
        // Log each email in this category
        emails.forEach((email, index) => {
          logger.info(`Email ${index + 1} in ${key}: "${email.subject}" from ${email.from.address} to ${email.to.map(r => r.address).join(', ')}`);
        });
      }
    });
    
    // Remove empty categories
    Object.keys(emailsByEmployeeSentReceived).forEach(key => {
      if (emailsByEmployeeSentReceived[key].length === 0) {
        delete emailsByEmployeeSentReceived[key];
      }
    });
    
    // Step 3: Summarize emails
    const summaries = await summarizeEmails(emailsByEmployeeSentReceived);
    
    // Remove jared@gbl-data.com from the summaries
    const excludedEmail = 'jared@gbl-data.com';
    const excludedPrefix = excludedEmail.split('@')[0].toLowerCase();
    
    // Remove both sent and received categories for jared
    delete summaries[`${excludedPrefix}@gbl-data.com sent`];
    delete summaries[`${excludedPrefix}@gbl-data.com received`];
    
    logger.info(`Excluded email summaries for ${excludedEmail} as requested`);
    
    // Step 4: Send the summary email
    await sendSummaryEmail(summaries);
    
    logger.info('Daily email summary completed successfully');
  } catch (error) {
    logger.error('Error in daily summary process', { error: error.message, stack: error.stack });
  }
}

/**
 * Run Marc's custom report
 */
async function runMarcCustomReport(days, recipient) {
  try {
    logger.info(`Starting Marc report for last ${days} days`);
    await sendMarcReport(days, recipient);
    logger.info('Marc report process completed');
  } catch (error) {
    logger.error('Error running Marc report', {
      error: error.message,
      stack: error.stack
    });
  }
}

// Schedule the job to run daily at 5 PM
const scheduleTime = process.env.SCHEDULE_TIME || '0 17 * * *';

// If --run-now flag is provided, run the summary immediately
if (runNow) {
  logger.info('Running summary immediately due to --run-now flag');
  runDailySummary();
} else if (runMarc) {
  logger.info(`Running Marc report with days=${daysValue} and recipient=${recipientValue || 'default'}`);
  runMarcCustomReport(daysValue, recipientValue);
} else if (runMarcComplete) {
  logger.info(`Running Marc COMPLETE report with days=${daysValue} and recipient=${recipientValue || 'default'}`);
  sendMarcCompleteReport(daysValue, recipientValue);
} else if (runMarcGraph) {
  logger.info(`Running Marc GRAPH API report with days=${daysValue} and recipient=${recipientValue || 'default'}`);
  sendMarcGraphReport(daysValue, recipientValue);
} else if (runAllEmployee) {
  logger.info(`Running ALL EMPLOYEE report with days=${daysValue} and recipient=${recipientValue || 'default'}`);
  sendAllEmployeeReport(daysValue, recipientValue);
} else if (singleEmployee) {
  const employeeEmail = singleEmployee.split('=')[1];
  logger.info(`Running report for single employee ${employeeEmail} with days=${daysValue} and recipient=${recipientValue || 'default'}`);
  generateEmployeeReport(employeeEmail, daysValue)
    .then(report => {
      if (report.totalEmails === 0) {
        logger.info(`No emails found for ${employeeEmail}`);
        return;
      }
      
      const { sendCustomEmail } = require('./email-sender');
      return sendCustomEmail(
        report.summaries,
        `${employeeEmail} Email Activity - Last ${daysValue} Days`,
        `<h1>${employeeEmail} Email Activity</h1>
        <p>Total emails: ${report.totalEmails}</p>
        ${report.analytics ? `
        <p>Unique recipients: ${report.analytics.uniqueRecipients}</p>
        <p>Daily average: ${report.analytics.dailyAverage}</p>
        ` : ''}`,
        recipientValue
      );
    });
} else {
  // Schedule the daily summary job
  cron.schedule(scheduleTime, () => {
    logger.info(`Running scheduled task at ${new Date().toISOString()}`);
    runDailySummary();
  });
  
  logger.info(`Email summary service started. Scheduled for: ${scheduleTime}`);
}