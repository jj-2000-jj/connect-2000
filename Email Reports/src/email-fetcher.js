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
 * Gets today's date in the format required by Microsoft Graph API
 */
function getTodayDateFilter() {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return today.toISOString();
}

/**
 * Gets date 30 days ago in the format required by Microsoft Graph API
 */
function get30DaysAgoFilter() {
  const date30DaysAgo = new Date();
  date30DaysAgo.setDate(date30DaysAgo.getDate() - 30);
  date30DaysAgo.setHours(0, 0, 0, 0);
  return date30DaysAgo.toISOString();
}

/**
 * Checks if an email appears to be marketing/spam/advertisement
 */
function isLikelyUnwantedEmail(email, companyEmployees) {
  // Check for common marketing subject indicators - expanded list
  const unwantedPatterns = [
    /newsletter/i,
    /subscribe/i,
    /unsubscribe/i,
    /special offer/i,
    /discount/i,
    /sale/i,
    /promo/i,
    /off/i,
    /save/i,
    /deal/i,
    /free/i,
    /click/i,
    /buy/i,
    /limited time/i,
    /exclusive/i,
    /opportunity/i,
    /webinar/i,
    /coupon/i,
    /promotion/i,
    /update/i,
    /daily/i,
    /weekly/i,
    /notification/i,
    /receipt/i,
    /invoice/i,
    /order/i,
    /advertisement/i,
    /sponsor/i,
    /marketing/i,
    /campaign/i,
    /special pricing/i,
    /learn more/i,
    /join us/i,
    /membership/i,
    /trial/i,
    /demo/i,
    /sign up/i,
    /register/i,
    /survey/i,
    /feedback/i,
    /review/i,
    /thank you for/i,
    /thanks for/i,
    /confirm/i,
    /your account/i,
    /your order/i,
    /your subscription/i,
    /your registration/i,
    /don't miss/i,
    /don't forget/i,
    /reminder/i,
    /latest/i,
    /introducing/i,
    /new release/i,
    /now available/i,
    /waiting for you/i,
    /important news/i,
    /bulletin/i,
    /digest/i,
    /summary/i,
    /statement/i,
    /events/i,
    /invitation/i,
    /access/i,
    /welcome/i,
    /verify/i,
    /upgrade/i,
    /activate/i,
    
    /\[\s*spam\s*\]/i,
    /\[\s*bulk\s*\]/i,
    /\[\s*ad\s*\]/i,
    /\[\s*advertisement\s*\]/i,
    /undeliverable/i,
    /mail delivery failed/i,
    /delivery status notification/i,
    /mailer-daemon/i,
    /delivery failure/i,
    /virus detected/i,
    /suspended/i,
    /account.*verify/i,
    /approve.*profile/i,
    /bitcoin/i,
    /blockchain/i,
    /cryptocurrency/i,
    /crypto/i,
    /lottery/i,
    /prize/i,
    /won/i,
    /winner/i,
    /investment opportunity/i,
    /make money/i,
    /earn from home/i,
    /work from home/i,
    /job offer/i,
    /job opportunity/i,
    /job listing/i,
    /pills/i,
    /medication/i,
    /pharma/i,
    /pharmacy/i,
    /diet/i,
    /weight loss/i,
    /lose weight/i,
    /insurance quote/i,
    /mortgage/i,
    /refinance/i,
    /loan approval/i,
    /credit score/i
  ];

  // Common marketing sender domains and local parts - expanded list
  const unwantedSenders = [
    'marketing',
    'newsletter',
    'info',
    'noreply',
    'no-reply',
    'mail',
    'email',
    'news',
    'update',
    'offers',
    'promotions',
    'sales',
    'support',
    'hello',
    'contact',
    'admin',
    'team',
    'events',
    'webinar',
    'service',
    'billing',
    'account',
    'notification',
    'alert',
    'security',
    'membership',
    'customer',
    'social',
    'community',
    'care',
    'digital',
    'communications',
    'notices',
    'updates',
    'help',
    'donotreply',
    'feedback',
    'verification',
    'digest',
    'bulletin',
    'statement',
    'subscription',
    
    'spam',
    'bulk',
    'advert',
    'promotional',
    'autoresponder',
    'bounce',
    'daemon',
    'system',
    'robot',
    'auto',
    'confirm',
    'verify',
    'validation',
    'reply-to',
    'mailer',
    'broadcast',
    'campaign',
    'offer',
    'deal',
    'discount',
    'promotion',
    'sale',
    'special',
    'subscribe',
    'unsubscribe',
    'click',
    'track',
    'bounce',
    'delivery',
    'postmaster',
    'mail-daemon',
    'mailing',
    'maildelivery',
    'mail-delivery'
  ];

  // Common advertising domains
  const advertisingDomains = [
    'mailchimp',
    'sendgrid',
    'constantcontact',
    'hubspot',
    'marketo',
    'salesforce',
    'mailerlite',
    'campaign-archive',
    'campaignmonitor',
    'aweber',
    'mailgun',
    'amazonses',
    'sparkpost',
    'klaviyo',
    'cm.com',
    'dotmailer',
    'icontact',
    'verticalresponse',
    'activecampaign',
    'getresponse',
    'brevo',
    'sendinblue',
    'exacttarget',
    'mailjet',
    'zoho',
    'e-flyers',
    
    'postmaster',
    'mailer-daemon',
    'bounce',
    'click',
    'marketing',
    'tracking',
    'cmail',
    'xmail',
    'smail',
    'emarketing',
    'emarsys',
    'emailvision',
    'emsecure',
    'fishbowl',
    'listrak',
    'exacttarget',
    'madgexjb',
    'mailtrack',
    'message-business',
    'netatlantic',
    'publicaster',
    'returnpath',
    'sailthru',
    'strongview',
    'marketingcloud',
    'email-od',
    'emaillabs',
    'listrakbi',
    'robly',
    'mailmchrmp',
    'cheetahmail',
    'emailcampaigns',
    'exacttarget',
    'mailpro',
    'dmdconnects',
    'infusionmail',
    'iphmx',
    'emaildirect',
    'mimecast',
    'ccsend',
    'dotmailer',
    'listserve',
    'industrymailings',
    'deliveredby',
    'communications',
    'surveygizmo',
    'surveymonkey'
  ];

  const subject = email.subject || '';
  const from = email.from.address || '';
  const fromText = email.from.text || '';
  const body = email.text || '';
  
  // Check subject against marketing patterns
  const hasUnwantedSubject = unwantedPatterns.some(pattern => pattern.test(subject));
  
  // Check sender domain or name part against marketing indicators
  const fromParts = from.split('@');
  const localPart = fromParts[0] || '';
  const domain = fromParts[1] || '';
  
  const hasUnwantedSender = unwantedSenders.some(sender => 
    localPart.toLowerCase().includes(sender.toLowerCase())
  );
  
  const isAdvertisingDomain = advertisingDomains.some(adDomain => 
    domain && domain.toLowerCase().includes(adDomain.toLowerCase())
  );
  
  // Check if subject has emoji or special characters often used in marketing
  const hasEmoji = /[\u{1F300}-\u{1F6FF}\u{2600}-\u{26FF}]/u.test(subject);
  const hasSpecialChars = /[!$%*🔥⭐💰💯🎉✨]/g.test(subject);
  
  // Check for marketing footer patterns in body
  const hasMarketingFooter = 
    /unsubscribe|opt.out|view.in.browser|privacy.policy|email.preferences|update profile/i.test(body);
    
  // Check for excessive capitalization in subject (common in spam)
  const wordCount = subject.split(/\s+/).length;
  const capsWords = subject.split(/\s+/).filter(word => 
    word.length > 1 && word === word.toUpperCase()
  ).length;
  const hasTooManyCaps = wordCount > 3 && (capsWords / wordCount) > 0.5;
  
  // New heuristic - Check for spam trigger phrases in body
  const bodySpamScore = calculateSpamBodyScore(body);
  const hasSpamBodyContent = bodySpamScore > 3; // Threshold value
  
  // New heuristic - Check for suspicious link patterns in body
  const hasSuspiciousLinks = /https?:\/\/bit\.ly|https?:\/\/tinyurl|https?:\/\/goo\.gl|click here|click this link/i.test(body);
  
  // New heuristic - Check for frequent punctuation (common in spam)
  const excessivePunctuation = /[!?]{2,}|(?:[!?].*){3,}/i.test(subject);
  
  // Check for company domains - if it's from one of our team members, don't filter it
  const fromLower = from.toLowerCase();
  const isCompanyEmail = companyEmployees.some(employee => employee.toLowerCase() === fromLower);
  
  // If it's from our company, we always want to include it
  if (isCompanyEmail) {
    return false;
  }
  
  // Check for thread markers in subject line
  const isReplyOrForward = /^(re:|fwd:|fw:)/i.test(subject.trim());
  
  // If it's a reply and doesn't have marketing indicators, it's probably important
  if (isReplyOrForward && !hasMarketingIndicators(subject) && !hasMarketingFooter) {
    return false;
  }
  
  // Combine all spam indicators with stronger weighting for certain signals
  const isSpam = 
    hasUnwantedSubject || 
    hasUnwantedSender || 
    isAdvertisingDomain || 
    hasEmoji || 
    hasSpecialChars || 
    hasMarketingFooter || 
    hasTooManyCaps ||
    hasSpamBodyContent ||
    hasSuspiciousLinks ||
    excessivePunctuation;
  
  // Log filtering reasons for debugging
  if (isSpam) {
    logger.debug(`Filtering out email: "${subject}" from ${from}`, {
      reasons: {
        unwantedSubject: hasUnwantedSubject,
        unwantedSender: hasUnwantedSender,
        advertisingDomain: isAdvertisingDomain,
        hasEmoji: hasEmoji,
        hasSpecialChars: hasSpecialChars,
        marketingFooter: hasMarketingFooter,
        tooManyCaps: hasTooManyCaps,
        spamBodyContent: hasSpamBodyContent,
        suspiciousLinks: hasSuspiciousLinks,
        excessivePunctuation: excessivePunctuation
      }
    });
  }
  
  return isSpam;
}

/**
 * Calculates a spam score for email body content
 * Higher score means more likely to be spam
 */
function calculateSpamBodyScore(body) {
  if (!body) return 0;
  
  const bodyLower = body.toLowerCase();
  let score = 0;
  
  // High-signal spam phrases
  const highSignalPhrases = [
    'viagra', 'cialis', 'pharmacy', 'pills', 'medication',
    'lottery', 'prize', 'winner', 'won', 'casino',
    'bitcoin', 'crypto', 'investment opportunity', 'make money fast',
    'millions', 'billionaire', 'get rich', 'nigerian prince',
    'bank transfer', 'wire transfer', 'western union',
    'warranty', 'extended warranty', 'car warranty',
    'your password', 'your account has been', 'security alert',
    'tax refund', 'irs', 'government grant', 'fbi', 'cia',
    'singles in your area', 'hot girls', 'dating site',
    'weight loss', 'diet pills', 'lose weight fast'
  ];
  
  // Medium-signal spam phrases
  const mediumSignalPhrases = [
    'limited time', 'exclusive offer', 'special deal',
    'save up to', 'discount', 'clearance', 'sale',
    'increase your', 'enhance your', 'improve your',
    'one time offer', 'act now', 'don\'t wait',
    'for free', 'at no cost', 'no obligation',
    'congratulations', 'selected', 'chosen',
    'best rates', 'lowest price', 'guaranteed'
  ];
  
  // Low-signal spam phrases
  const lowSignalPhrases = [
    'click here', 'visit our website', 'check out',
    'new product', 'learn more', 'find out',
    'best', 'great', 'amazing',
    'opportunity', 'solution', 'benefits'
  ];
  
  // Add to score based on found phrases
  highSignalPhrases.forEach(phrase => {
    if (bodyLower.includes(phrase)) score += 2;
  });
  
  mediumSignalPhrases.forEach(phrase => {
    if (bodyLower.includes(phrase)) score += 1;
  });
  
  lowSignalPhrases.forEach(phrase => {
    if (bodyLower.includes(phrase)) score += 0.5;
  });
  
  // Check for excessive URLs (common in spam)
  const urlMatches = bodyLower.match(/https?:\/\//g) || [];
  if (urlMatches.length > 3) score += 1;
  if (urlMatches.length > 6) score += 1;
  
  return score;
}

/**
 * Helper for checking marketing indicators in a string
 */
function hasMarketingIndicators(text) {
  const marketingTerms = [
    'newsletter', 'subscribe', 'unsubscribe', 'offer', 'discount',
    'sale', 'promotion', 'deal', 'free', 'limited time', 'exclusive',
    'click here', 'learn more', 'buy now', 'shop now', 'act now',
    'special price', 'for a limited time', 'don\'t miss out', 'introducing',
    'latest', 'new', 'update', 'upgrade', 'announcement', 'savings',
    'discount', 'clearance', 'bargain', 'earn', 'cash back', 'gift',
    'chance', 'opportunity', 'call to action', 'open', 'important',
    'alert', 'notification', 'membership', 'subscription', 'access',
    'register', 'win', 'bonus', 'extra', 'unlock', 'invitation'
  ];
  
  const lowerText = text.toLowerCase();
  return marketingTerms.some(term => lowerText.includes(term));
}

/**
 * Less aggressive filter for company emails to only filter obvious advertisements
 */
function isObviouslyAdvertisement(email) {
  const subject = email.subject || '';
  const body = email.text || '';
  
  // Only filter out the most obvious marketing content from company employees
  const obviousMarketingSubjects = [
    /newsletter/i,
    /weekly update/i,
    /monthly update/i,
    /company announcement/i,
    /announcement:/i,
    /marketing blast/i,
    /marketing update/i,
    /advertisement/i,
    /\[marketing\]/i,
    /\[newsletter\]/i
  ];
  
  // Check for obvious marketing subject patterns
  const hasObviousMarketingSubject = obviousMarketingSubjects.some(pattern => 
    pattern.test(subject)
  );
  
  // Check for unsubscribe links in the body
  const hasUnsubscribeLink = /unsubscribe|opt.out/i.test(body);
  
  // Check if it's an obvious bulk email with many recipients
  const hasDoNotReply = /do not reply|donotreply|no-reply|noreply/i.test(body);
  
  return hasObviousMarketingSubject && (hasUnsubscribeLink || hasDoNotReply);
}

/**
 * Extracts the company domain from an employee email
 */
function getEmployeeDomain(email) {
  const parts = email.split('@');
  return parts.length > 1 ? parts[1].toLowerCase() : '';
}

/**
 * Searches for emails from specific employees regardless of recipient
 */
async function searchForEmployeeEmails(client, userEmail, employeeEmail) {
  try {
    const todayFilter = getTodayDateFilter();
    logger.info(`Searching for emails involving ${employeeEmail} since ${todayFilter}`);
    
    // First, get received emails from this employee to the main user
    logger.info(`Getting emails received from ${employeeEmail}`);
    const fromQuery = await client.api(`/users/${userEmail}/messages`)
      .filter(`receivedDateTime ge ${todayFilter} and from/emailAddress/address eq '${employeeEmail}'`)
      .select('id,subject,bodyPreview,receivedDateTime,from,toRecipients,body,importance,isDraft')
      .top(50)
      .get();
    
    logger.info(`Found ${fromQuery.value?.length || 0} emails received from ${employeeEmail}`);
      
    // Then get sent emails to this employee from the main user
    logger.info(`Getting emails sent to ${employeeEmail}`);
    const toQuery = await client.api(`/users/${userEmail}/mailFolders/sentitems/messages`)
      .filter(`sentDateTime ge ${todayFilter}`)
      .select('id,subject,bodyPreview,sentDateTime,from,toRecipients,body,importance,isDraft')
      .top(50)
      .get();
      
    // For sent emails, we need to filter for ones sent to our employee
    const sentToEmployee = (toQuery.value || []).filter(email => 
      email.toRecipients && email.toRecipients.some(recipient => 
        recipient.emailAddress && 
        recipient.emailAddress.address && 
        recipient.emailAddress.address.toLowerCase() === employeeEmail.toLowerCase()
      )
    );
    
    logger.info(`Found ${sentToEmployee.length} emails sent to ${employeeEmail}`);
    
    // Now get ALL emails sent by this employee by directly querying their mailbox
    // This requires admin/delegated permissions
    let allEmployeeEmails = [];
    try {
      logger.info(`Attempting to directly access ${employeeEmail}'s sent items folder`);
      const employeeSentQuery = await client.api(`/users/${employeeEmail}/mailFolders/sentitems/messages`)
        .filter(`sentDateTime ge ${todayFilter}`)
        .select('id,subject,bodyPreview,sentDateTime,from,toRecipients,body,importance,isDraft')
        .top(100)
        .get();
      
      allEmployeeEmails = employeeSentQuery.value || [];
      logger.info(`Successfully retrieved ${allEmployeeEmails.length} emails sent by ${employeeEmail}`);
      
      // Log each email retrieved for debugging
      if (allEmployeeEmails.length > 0) {
        allEmployeeEmails.forEach((email, index) => {
          const to = email.toRecipients.map(r => r.emailAddress.address).join(', ');
          logger.info(`Employee sent email ${index + 1}: "${email.subject}" to ${to}`);
        });
      }
    } catch (error) {
      logger.error(`Error accessing ${employeeEmail}'s mailbox. This requires admin permissions.`, { 
        error: error.message, 
        employeeEmail: employeeEmail
      });
      // Continue with limited data if we can't access their mailbox directly
    }
    
    // Try to get received emails for this employee directly
    let employeeReceivedEmails = [];
    try {
      logger.info(`Attempting to directly access ${employeeEmail}'s inbox folder`);
      const employeeReceivedQuery = await client.api(`/users/${employeeEmail}/messages`)
        .filter(`receivedDateTime ge ${todayFilter}`)
        .select('id,subject,bodyPreview,receivedDateTime,from,toRecipients,body,importance,isDraft')
        .top(100)
        .get();
      
      employeeReceivedEmails = employeeReceivedQuery.value || [];
      logger.info(`Successfully retrieved ${employeeReceivedEmails.length} emails received by ${employeeEmail}`);
      
      // Log each email retrieved for debugging
      if (employeeReceivedEmails.length > 0) {
        employeeReceivedEmails.forEach((email, index) => {
          logger.info(`Employee received email ${index + 1}: "${email.subject}" from ${email.from.emailAddress.address}`);
        });
      }
    } catch (error) {
      logger.error(`Error accessing ${employeeEmail}'s inbox. This requires admin permissions.`, { 
        error: error.message, 
        employeeEmail: employeeEmail
      });
      // Continue with limited data if we can't access their inbox directly
    }
    
    // Combine all sources of emails
    const combinedEmails = [
      ...(fromQuery.value || []), 
      ...sentToEmployee, 
      ...allEmployeeEmails,
      ...employeeReceivedEmails
    ];
    
    logger.info(`Total emails found involving ${employeeEmail}: ${combinedEmails.length}`);
    
    return combinedEmails;
  } catch (error) {
    logger.error(`Error searching for emails involving ${employeeEmail}`, { error: error.message });
    return [];
  }
}

/**
 * Fetches sent and received emails from Microsoft 365 using Graph API
 * for all company employees
 */
async function fetchEmails(companyEmployees, enableSpamFiltering = true) {
  try {
    logger.info('Fetching emails from Microsoft 365');
    const client = createGraphClient();
    const todayFilter = getTodayDateFilter();
    const userEmail = process.env.MICROSOFT_USER_EMAIL;
    
    if (!userEmail) {
      throw new Error('MICROSOFT_USER_EMAIL is not set in environment variables');
    }
    
    logger.info(`Fetching emails for user: ${userEmail}`);
    
    // Get received emails using standard approach
    const receivedResponse = await client.api(`/users/${userEmail}/messages`)
      .filter(`receivedDateTime ge ${todayFilter}`)
      .select('id,subject,bodyPreview,receivedDateTime,from,toRecipients,body,importance,isDraft')
      .top(200) // Increased for better coverage
      .get();
    
    // Get sent emails using standard approach
    const sentResponse = await client.api(`/users/${userEmail}/mailFolders/sentitems/messages`)
      .filter(`sentDateTime ge ${todayFilter}`)
      .select('id,subject,bodyPreview,sentDateTime,from,toRecipients,body,importance,isDraft')
      .top(100) // Increased for better coverage
      .get();
    
    // Track processed email IDs to avoid duplicates
    const processedIds = new Set();
    
    // Process received emails
    const receivedEmails = (receivedResponse.value || []).map(email => {
      if (!email.from || !email.from.emailAddress) {
        // Skip this email if it doesn't have proper from data
        logger.warn('Skipping email with missing from information', { subject: email.subject });
        return null;
      }
      
      processedIds.add(email.id);
      
      const fromAddress = email.from.emailAddress.address;
      
      // Check if sender is a company employee
      const isCompanyEmployee = companyEmployees.some(employee => 
        employee.toLowerCase() === fromAddress.toLowerCase()
      );
      
      const sender = isCompanyEmployee
        ? fromAddress.split('@')[0].toLowerCase() // just the name part for company employees
        : email.from.emailAddress.name || fromAddress;
      
      return {
        id: email.id,
        subject: email.subject || '(No Subject)',
        from: { 
          text: email.from.emailAddress.name || email.from.emailAddress.address,
          address: email.from.emailAddress.address,
          displayName: sender
        },
        to: email.toRecipients.map(recipient => ({
          address: recipient.emailAddress.address,
          name: recipient.emailAddress.name
        })),
        date: new Date(email.receivedDateTime),
        text: email.bodyPreview || '', // Use bodyPreview as fallback
        html: email.body && email.body.contentType === 'html' ? email.body.content : null,
        importance: email.importance || 'normal',
        direction: 'received',
        isDraft: email.isDraft || false
      };
    }).filter(email => email !== null); // Remove any null entries
    
    // Process sent emails
    const sentEmails = (sentResponse.value || []).map(email => {
      if (!email.toRecipients || !email.toRecipients.length) {
        // Skip this email if it doesn't have proper recipient data
        logger.warn('Skipping email with missing recipient information', { subject: email.subject });
        return null;
      }
      
      processedIds.add(email.id);
      
      // For sent emails, get the recipient
      const recipients = email.toRecipients.map(r => r.emailAddress.address);
      
      // Find company employees in recipients (case insensitive)
      const companyRecipients = recipients.filter(recipientAddress => 
        companyEmployees.some(employee => employee.toLowerCase() === recipientAddress.toLowerCase())
      );
      
      const primaryRecipient = companyRecipients.length > 0 
          ? companyRecipients[0].split('@')[0].toLowerCase() // Use company recipient if found
          : (email.toRecipients[0]?.emailAddress.name || email.toRecipients[0]?.emailAddress.address || 'Unknown');
      
      return {
        id: email.id,
        subject: email.subject || '(No Subject)',
        from: { 
          text: email.from.emailAddress.name || email.from.emailAddress.address,
          address: email.from.emailAddress.address
        },
        to: email.toRecipients.map(recipient => {
          const isCompanyRecipient = companyEmployees.some(employee => 
            employee.toLowerCase() === recipient.emailAddress.address.toLowerCase()
          );
          
          return {
            address: recipient.emailAddress.address,
            name: recipient.emailAddress.name,
            displayName: isCompanyRecipient
              ? recipient.emailAddress.address.split('@')[0].toLowerCase()
              : (recipient.emailAddress.name || recipient.emailAddress.address)
          };
        }),
        date: new Date(email.sentDateTime),
        text: email.bodyPreview || '',
        html: email.body && email.body.contentType === 'html' ? email.body.content : null,
        importance: email.importance || 'normal',
        direction: 'sent',
        isDraft: email.isDraft || false,
        primaryRecipient: primaryRecipient
      };
    }).filter(email => email !== null); // Remove any null entries
    
    // Always query emails for all employees to ensure we have complete data
    logger.info('Fetching emails for all company employees to ensure complete reports');
    
    // Get emails for all employees (not just "missing" ones)
    const additionalEmails = [];
    for (const employee of companyEmployees.filter(emp => emp.toLowerCase() !== userEmail.toLowerCase())) {
      logger.info(`Searching for emails involving employee: ${employee}`);
      const employeeEmails = await searchForEmployeeEmails(client, userEmail, employee);
      logger.info(`Found ${employeeEmails.length} emails involving ${employee}`);
      
      // Process these emails similar to how we processed the others
      for (const email of employeeEmails) {
        if (!email || !email.id) {
          logger.warn(`Skipping invalid email object for ${employee}`);
          continue;
        }
        
        if (processedIds.has(email.id)) {
          logger.info(`Skipping already processed email: ${email.id} - "${email.subject}"`);
          continue; // Skip already processed emails
        }
        
        processedIds.add(email.id);
        
        if (email.from?.emailAddress?.address?.toLowerCase() === userEmail.toLowerCase()) {
          // This is a sent email
          if (!email.toRecipients || !email.toRecipients.length) {
            logger.info(`Skipping email with no recipients: ${email.id} - "${email.subject}"`);
            continue;
          }
          
          const recipients = email.toRecipients.map(r => r.emailAddress.address);
          const companyRecipients = recipients.filter(recipientAddress => 
            companyEmployees.some(employee => employee.toLowerCase() === recipientAddress.toLowerCase())
          );
          
          const primaryRecipient = companyRecipients.length > 0 
              ? companyRecipients[0].split('@')[0].toLowerCase()
              : (email.toRecipients[0]?.emailAddress.name || email.toRecipients[0]?.emailAddress.address || 'Unknown');
          
          logger.info(`Adding sent email: "${email.subject}" to ${recipients.join(', ')}`);
          
          additionalEmails.push({
            id: email.id,
            subject: email.subject || '(No Subject)',
            from: { 
              text: email.from.emailAddress.name || email.from.emailAddress.address,
              address: email.from.emailAddress.address
            },
            to: email.toRecipients.map(recipient => {
              const isCompanyRecipient = companyEmployees.some(employee => 
                employee.toLowerCase() === recipient.emailAddress.address.toLowerCase()
              );
              
              return {
                address: recipient.emailAddress.address,
                name: recipient.emailAddress.name,
                displayName: isCompanyRecipient
                  ? recipient.emailAddress.address.split('@')[0].toLowerCase()
                  : (recipient.emailAddress.name || recipient.emailAddress.address)
              };
            }),
            date: new Date(email.sentDateTime),
            text: email.bodyPreview || '',
            html: email.body && email.body.contentType === 'html' ? email.body.content : null,
            importance: email.importance || 'normal',
            direction: 'sent',
            isDraft: email.isDraft || false,
            primaryRecipient: primaryRecipient
          });
        } else if (email.from?.emailAddress?.address) {
          // This is a received email
          const fromAddress = email.from.emailAddress.address;
          const isCompanyEmployee = companyEmployees.some(employee => 
            employee.toLowerCase() === fromAddress.toLowerCase()
          );
          
          const sender = isCompanyEmployee
            ? fromAddress.split('@')[0].toLowerCase()
            : email.from.emailAddress.name || fromAddress;
          
          logger.info(`Adding received email: "${email.subject}" from ${fromAddress}`);
          
          additionalEmails.push({
            id: email.id,
            subject: email.subject || '(No Subject)',
            from: { 
              text: email.from.emailAddress.name || email.from.emailAddress.address,
              address: email.from.emailAddress.address,
              displayName: sender
            },
            to: (email.toRecipients || []).map(recipient => ({
              address: recipient.emailAddress.address,
              name: recipient.emailAddress.name
            })),
            date: new Date(email.receivedDateTime || email.sentDateTime),
            text: email.bodyPreview || '',
            html: email.body && email.body.contentType === 'html' ? email.body.content : null,
            importance: email.importance || 'normal',
            direction: 'received',
            isDraft: email.isDraft || false
          });
        } else {
          logger.warn(`Skipping email with invalid structure: ${email.id}`);
        }
      }
    }
    
    // Combine all emails
    let allEmails = [...receivedEmails, ...sentEmails, ...additionalEmails];
    
    // Filter out drafts
    allEmails = allEmails.filter(email => !email.isDraft);
    
    // Store original count for logging
    const originalCount = allEmails.length;
    
    // Apply spam filtering if enabled
    let filteredEmails = allEmails;
    if (enableSpamFiltering) {
      // Apply more aggressive filtering for non-company senders
      filteredEmails = allEmails.filter(email => {
        const fromAddress = email.from.address.toLowerCase();
        const isCompanyEmail = companyEmployees.some(employee => 
          employee.toLowerCase() === fromAddress
        );
        
        // For company emails, only filter out obvious marketing/spam
        if (isCompanyEmail) {
          return !isObviouslyAdvertisement(email);
        }
        
        // For external emails, apply full filtering
        return !isLikelyUnwantedEmail(email, companyEmployees);
      });
      
      const filteredCount = originalCount - filteredEmails.length;
      logger.info(`Found ${originalCount} emails, filtered out ${filteredCount} marketing/advertisement emails`);
    } else {
      logger.info('Spam filtering disabled, using all emails');
    }
    
    logger.info(`Proceeding with ${filteredEmails.length} important emails`);
    
    // Count emails by company employee 
    companyEmployees.forEach(employee => {
      const employeeLower = employee.toLowerCase();
      const sentCount = filteredEmails.filter(email => 
        email.from.address.toLowerCase() === employeeLower
      ).length;
      
      const receivedCount = filteredEmails.filter(email => 
        email.to.some(recipient => recipient.address.toLowerCase() === employeeLower)
      ).length;
      
      logger.info(`Employee ${employee}: ${sentCount} sent, ${receivedCount} received`);
    });
    
    return filteredEmails;
  } catch (error) {
    logger.error('Error fetching emails from Microsoft 365', { 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

module.exports = {
  fetchEmails
};