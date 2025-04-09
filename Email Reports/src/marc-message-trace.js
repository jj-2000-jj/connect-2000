require('dotenv').config();
require('isomorphic-fetch');
const { ClientSecretCredential } = require('@azure/identity');
const { Client } = require('@microsoft/microsoft-graph-client');
const { TokenCredentialAuthenticationProvider } = require('@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials');

/**
 * Creates an authenticated Microsoft Graph client with admin permissions
 * NOTE: This requires Global Admin or Exchange Admin permissions
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
 * Use message trace logs to find ALL emails sent by Marc
 * NOTE: This requires Exchange Admin permissions
 */
async function getMessageTraceForMarc(days = 30) {
  try {
    const client = createGraphClient();
    const { startDate, endDate } = getLastXDaysRange(days);
    const senderAddress = 'marc@gbl-data.com';
    
    console.log(`Using Message Trace to find ALL emails sent by ${senderAddress} from ${startDate} to ${endDate}`);
    
    // Attempt to use Office 365 Management API for message trace data
    // This is a specialized endpoint not typically available in standard Graph API
    // This requires proper permission configuration
    
    try {
      // This is a hypothetical endpoint - actual implementation may vary
      const response = await client.api('/security/messageTrace')
        .post({
          StartDate: startDate,
          EndDate: endDate,
          SenderAddress: senderAddress,
          // Include detailed message data
          IncludeMessageTrace: true
        });
      
      console.log(`Message Trace found ${response.length} messages`);
      return response;
    } catch (error) {
      console.error('Error accessing Message Trace API:', error.message);
      console.log('\nAlternative approach: Use Exchange Online PowerShell instead');
      console.log(`
To get complete message trace data, you need to use Exchange Online PowerShell:

1. Connect to Exchange Online PowerShell:
   Connect-ExchangeOnline -UserPrincipalName admin@yourdomain.com

2. Run a message trace for Marc's emails:
   $startDate = (Get-Date).AddDays(-${days})
   $endDate = Get-Date
   Get-MessageTrace -SenderAddress ${senderAddress} -StartDate $startDate -EndDate $endDate | Export-Csv -Path "C:\\MarcEmailTrace.csv" -NoTypeInformation

3. For detailed message information:
   $messages = Get-MessageTrace -SenderAddress ${senderAddress} -StartDate $startDate -EndDate $endDate
   foreach ($message in $messages) {
     Get-MessageTraceDetail -MessageTraceId $message.MessageTraceId -RecipientAddress $message.RecipientAddress | Export-Csv -Path "C:\\MarcEmailTraceDetails.csv" -Append -NoTypeInformation
   }
`);
      
      // Fallback to the approach we've been using
      console.log('\nFalling back to checking mailboxes...');
      return await getEmailsFromMailboxes(client, days, senderAddress);
    }
  } catch (error) {
    console.error('Error in message trace retrieval:', error);
    throw error;
  }
}

/**
 * Fallback method to check mailboxes
 */
async function getEmailsFromMailboxes(client, days, senderAddress) {
  const { startDate, endDate } = getLastXDaysRange(days);
  const emailMap = new Map();
  
  // Company employees list
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
  
  // 1. Check sent items folder
  try {
    const sentResponse = await client.api(`/users/${senderAddress}/mailFolders/sentitems/messages`)
      .filter(`sentDateTime ge ${startDate} and sentDateTime le ${endDate}`)
      .select('id,subject,sentDateTime,from,toRecipients,isDraft')
      .top(1000)
      .get();
    
    const sentEmails = (sentResponse.value || []).filter(email => !email.isDraft);
    console.log(`Found ${sentEmails.length} emails in ${senderAddress}'s sent items folder`);
    
    sentEmails.forEach(email => {
      emailMap.set(email.id, {
        id: email.id,
        subject: email.subject || '(No Subject)',
        from: email.from.emailAddress.address,
        to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
        date: new Date(email.sentDateTime).toISOString(),
        source: 'Sent items folder'
      });
    });
  } catch (error) {
    console.error("Error checking sent items folder:", error.message);
  }
  
  // 2. Check employee inboxes
  for (const employeeEmail of COMPANY_EMPLOYEES) {
    if (employeeEmail === senderAddress) continue;
    
    try {
      const receivedResponse = await client.api(`/users/${employeeEmail}/messages`)
        .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${senderAddress}'`)
        .select('id,subject,receivedDateTime,from,toRecipients')
        .top(1000)
        .get();
        
      const recipientEmails = receivedResponse.value || [];
      console.log(`Found ${recipientEmails.length} emails in ${employeeEmail}'s inbox from ${senderAddress}`);
      
      recipientEmails.forEach(email => {
        if (!emailMap.has(email.id)) {
          emailMap.set(email.id, {
            id: email.id,
            subject: email.subject || '(No Subject)',
            from: email.from.emailAddress.address,
            to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
            date: new Date(email.receivedDateTime).toISOString(),
            source: `${employeeEmail}'s inbox`
          });
        }
      });
    } catch (error) {
      console.error(`Error checking ${employeeEmail}'s inbox:`, error.message);
    }
  }
  
  // 3. Check self-BCC'd messages
  try {
    const selfCopyResponse = await client.api(`/users/${senderAddress}/messages`)
      .filter(`receivedDateTime ge ${startDate} and receivedDateTime le ${endDate} and from/emailAddress/address eq '${senderAddress}'`)
      .select('id,subject,receivedDateTime,from,toRecipients')
      .top(1000)
      .get();
      
    const selfCopiedEmails = selfCopyResponse.value || [];
    console.log(`Found ${selfCopiedEmails.length} emails in ${senderAddress}'s inbox that were self-BCC'd`);
    
    selfCopiedEmails.forEach(email => {
      if (!emailMap.has(email.id)) {
        emailMap.set(email.id, {
          id: email.id,
          subject: email.subject || '(No Subject)',
          from: email.from.emailAddress.address,
          to: email.toRecipients.map(r => r.emailAddress.address).join(', '),
          date: new Date(email.receivedDateTime).toISOString(),
          source: 'Self BCC'
        });
      }
    });
  } catch (error) {
    console.error("Error checking self-BCC'd emails:", error.message);
  }
  
  // Convert to array and sort
  const allEmails = Array.from(emailMap.values());
  allEmails.sort((a, b) => new Date(b.date) - new Date(a.date));
  
  console.log(`\nFound ${allEmails.length} unique emails using mailbox checking approach`);
  console.log(`NOTE: This approach may still miss emails sent to external recipients without BCC`);
  
  return allEmails;
}

// Run the script
const days = process.argv[2] ? parseInt(process.argv[2], 10) : 30;
getMessageTraceForMarc(days)
  .then(emails => {
    if (Array.isArray(emails)) {
      console.log("\n===== EMAILS =====");
      console.log('Date       | Subject                          | To                              | Source');
      console.log('-----------|----------------------------------|--------------------------------|------------------');
      
      emails.forEach(email => {
        const date = email.date.substring(0, 10);
        const subject = email.subject.length > 30 ? email.subject.substring(0, 27) + '...' : email.subject.padEnd(30);
        const to = email.to.length > 30 ? email.to.substring(0, 27) + '...' : email.to.padEnd(30);
        
        console.log(`${date} | ${subject} | ${to} | ${email.source}`);
      });
    }
  })
  .catch(error => {
    console.error("Script error:", error);
  });