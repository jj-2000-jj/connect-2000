const logger = require('./logger');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// Get Gemini API key from environment
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

// Initialize Gemini client
const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);

/**
 * Simple rule-based email summarizer as fallback when AI is unavailable
 */
function createSimpleSummary(email) {
  const text = email.text || '';
  
  // Extract first sentence or first 150 characters
  let summary = '';
  
  if (text.length > 0) {
    // Find the first sentence (ending with period, question mark, or exclamation point)
    const firstSentenceMatch = text.match(/^[^.!?]*[.!?]/);
    
    if (firstSentenceMatch && firstSentenceMatch[0]) {
      summary = firstSentenceMatch[0].trim();
    } else {
      // If no sentence found, take first 150 chars
      summary = text.substring(0, 150).trim();
      if (text.length > 150) summary += '...';
    }
  } else {
    summary = '(No content available)';
  }
  
  // Add action detection
  if (
    text.toLowerCase().includes('please') || 
    text.toLowerCase().includes('action') ||
    text.toLowerCase().includes('required') ||
    text.toLowerCase().includes('respond') ||
    text.toLowerCase().includes('review') ||
    text.toLowerCase().includes('confirm')
  ) {
    summary += ' [Possible action required]';
  }
  
  return summary;
}

/**
 * Process the summary to ensure consistent formatting
 */
function formatSummary(summaryText) {
  if (!summaryText) return summaryText;
  
  // Remove any intro text before the first section
  let formattedSummary = summaryText.replace(/^.*?KEY POINTS/i, 'KEY POINTS');
  
  // Make sure section headers are properly formatted - convert any HTML or markdown to plain text
  formattedSummary = formattedSummary
    // Remove HTML tags
    .replace(/<\/?b>/g, '')
    // Replace numbered sections like "1. Key Points" with just "KEY POINTS"
    .replace(/\d+\.\s*(KEY POINTS|ACTION ITEMS|CONTEXT)/gi, '$1')
    // Replace any **Key Points** format with KEY POINTS
    .replace(/\*\*(KEY POINTS|ACTION ITEMS|CONTEXT)\*\*/gi, '$1')
    // Replace any *Key Points* format with KEY POINTS
    .replace(/\*(KEY POINTS|ACTION ITEMS|CONTEXT)\*/gi, '$1');
  
  // Standardize section headers to uppercase
  formattedSummary = formattedSummary
    .replace(/key points/i, 'KEY POINTS')
    .replace(/action items/i, 'ACTION ITEMS')
    .replace(/context/i, 'CONTEXT');
  
  // Ensure proper spacing between sections with double line breaks
  formattedSummary = formattedSummary
    // First normalize section spacing
    .replace(/(KEY POINTS|ACTION ITEMS|CONTEXT)/g, '\n\n$1\n')
    // Clean up excessive spacing
    .replace(/\n{3,}/g, '\n\n');
  
  return formattedSummary.trim();
}

/**
 * Creates a concise summary of a single email using Google Gemini
 */
async function summarizeEmail(email) {
  try {
    const emailContent = email.text || email.html;
    if (!emailContent || emailContent.trim() === '') {
      return 'Empty email (no content)';
    }
    
    // First try using the rule-based summarizer to ensure we have a fallback
    const simpleSummary = createSimpleSummary(email);
    
    // Check if API key is configured
    if (!GEMINI_API_KEY) {
      logger.warn('No Gemini API key found, using rule-based summary', { subject: email.subject });
      return simpleSummary;
    }
    
    // Try the Gemini API
    try {
      // Initialize the Gemini-2.0 Flash model with parameters for detailed output
      const model = genAI.getGenerativeModel({ 
        model: "gemini-2.0-flash",
        generationConfig: {
          temperature: 0.2,  // Lower temperature for more focused output
          topP: 0.95,        // Slightly more creative but still focused
          maxOutputTokens: 1000 // Allow for longer, more detailed responses
        }
      });
      
      const prompt = `
      Provide a detailed and specific summary of the following email with 8-10 sentences total.
      Format the summary in 3 sections with exactly the following format:
      
      KEY POINTS
      [Write the main message with SPECIFIC details - include names, numbers, specific tasks, concrete information - avoid vague statements like "tasks that need completion" and instead list the actual tasks]
      
      ACTION ITEMS
      [List SPECIFIC actions required, with deadlines if mentioned. Include WHO needs to do WHAT and by WHEN. If no actions, state "No specific action items."]
      
      CONTEXT
      [Include relevant background with SPECIFIC details - mention project names, dates, prior conversations, and any other concrete information]
      
      Important rules:
      - Be SPECIFIC and CONCRETE - include actual details from the email, not general descriptions
      - Use names, numbers, dates, and detailed descriptions from the email
      - Instead of saying "tasks were mentioned" actually list the specific tasks
      - Instead of saying "issues were discussed" specify what those issues were
      - Use ALL CAPS for section titles exactly as shown above
      - Do NOT include any introduction text
      - Keep each section on its own paragraph with line breaks between sections
      
      Subject: ${email.subject}
      From: ${email.from.text}
      Date: ${email.date}
      
      ${emailContent.substring(0, 30000)} ${emailContent.length > 30000 ? '...(content truncated)' : ''}
      `;
      
      // Call the Gemini API
      const result = await model.generateContent(prompt);
      const response = await result.response;
      let summary = response.text();
      
      // Format the summary with proper spacing and structure
      summary = formatSummary(summary);
      
      // Log the generated summary
      logger.info('Successfully generated Gemini AI summary', { 
        subject: email.subject,
        summaryLength: summary.length
      });
      
      if (summary && summary.trim()) {
        return summary.trim();
      } else {
        logger.warn('AI generated empty summary, using rule-based summary', { subject: email.subject });
        return simpleSummary;
      }
    } catch (aiError) {
      // Check for specific API errors
      if (aiError.message.includes('SERVICE_DISABLED') || aiError.message.includes('has not been used in project')) {
        logger.error('Generative Language API not enabled in Google Cloud project', { 
          error: aiError.message,
          help: 'Enable the API at https://console.developers.google.com/apis/api/generativelanguage.googleapis.com/overview'
        });
      } else if (aiError.message.includes('API key not valid')) {
        logger.error('Invalid Google API key', { error: aiError.message });
      } else {
        logger.warn('Using rule-based summary instead of AI', { 
          error: aiError.message,
          subject: email.subject
        });
      }
      
      // Return the simple summary we created earlier
      return simpleSummary;
    }
  } catch (error) {
    logger.error('Error summarizing email', { 
      error: error.message, 
      subject: email.subject,
      from: email.from.text 
    });
    return `Failed to summarize: ${email.subject}`;
  }
}

/**
 * Summarizes a batch of emails for all employees
 */
async function summarizeEmails(emailsByEmployee) {
  logger.info('Starting email summarization process');
  
  
  const summaries = {};
  
  for (const [employee, emails] of Object.entries(emailsByEmployee)) {
    logger.info(`Summarizing ${emails.length} emails for ${employee}`);
    summaries[employee] = [];
    
    // Process emails in batches to avoid rate limiting
    const batchSize = 5;
    for (let i = 0; i < emails.length; i += batchSize) {
      const batch = emails.slice(i, i + batchSize);
      const summarizationPromises = batch.map(async (email) => {
        const summary = await summarizeEmail(email);
        return {
          subject: email.subject,
          date: email.date,
          summary
        };
      });
      
      const batchResults = await Promise.all(summarizationPromises);
      summaries[employee].push(...batchResults);
      
      // Add a small delay between batches to prevent rate limiting
      if (i + batchSize < emails.length) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }
  
  logger.info('Email summarization completed');
  
  return summaries;
}

module.exports = {
  summarizeEmails
};
