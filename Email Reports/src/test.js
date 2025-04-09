require('dotenv').config();
const { Ollama } = require('ollama');
const logger = require('./logger');

// Set up Ollama client
const OLLAMA_HOST = process.env.OLLAMA_HOST || 'http://localhost:11434';
const MODEL_NAME = process.env.OLLAMA_MODEL || 'qwq:latest';
const ollama = new Ollama({ host: OLLAMA_HOST });

// Test function to check if cleaning up thinking tags works
async function testThinkingTagCleanup() {
  try {
    logger.info('Testing thinking tag cleanup');
    
    // Test prompt that might generate thinking
    const prompt = `
    Summarize the following email in 1-2 sentences. Focus on key points and any action items.
    
    Subject: Quarterly Meeting Follow-up
    From: John Smith
    Date: March 24, 2025
    
    Hello team,
    
    Thank you all for attending our quarterly planning meeting yesterday. As discussed, we need to finalize the Q2 budget by Friday, and each department should submit their requirements by Wednesday. 
    
    Also, please remember that the new reporting system goes live next Monday, so ensure your team completes the required training by this Thursday.
    
    Best regards,
    John
    `;
    
    // Call the Ollama API
    const response = await ollama.generate({
      model: MODEL_NAME,
      prompt: prompt,
      options: { temperature: 0.3 }
    });
    
    console.log('Original response from model:');
    console.log('-----------------------------------');
    console.log(response.response);
    console.log('-----------------------------------');
    
    // Clean up thinking tags
    let cleanedSummary = response.response;
    
    // Remove <think>...</think> blocks
    const originalLength = cleanedSummary.length;
    cleanedSummary = cleanedSummary.replace(/<think>[\s\S]*?<\/think>/g, '');
    
    // Clean up any remaining tags
    cleanedSummary = cleanedSummary.replace(/<[^>]*>/g, '');
    
    // Trim whitespace and normalize spaces
    cleanedSummary = cleanedSummary.trim().replace(/\s+/g, ' ');
    
    console.log('\nCleaned response:');
    console.log('-----------------------------------');
    console.log(cleanedSummary);
    console.log('-----------------------------------');
    console.log(`Original length: ${originalLength}, Cleaned length: ${cleanedSummary.length}`);
    
    logger.info('Test completed successfully');
  } catch (error) {
    logger.error('Error in test', { error: error.message, stack: error.stack });
  }
}

// Run the test
testThinkingTagCleanup();