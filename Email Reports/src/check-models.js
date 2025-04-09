require('dotenv').config();
const { GoogleGenerativeAI } = require('@google/generative-ai');
const logger = require('./logger');

async function checkGeminiApi() {
  try {
    console.log('Checking Google Gemini API...');
    
    // Get Gemini API key
    const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
    
    if (!GEMINI_API_KEY) {
      console.error('❌ No Gemini API key found in .env file!');
      console.log('Please add your Google API key as GEMINI_API_KEY in the .env file.');
      return;
    }
    
    console.log('API key found. Testing connection to Gemini...');
    
    // Initialize the Google AI SDK
    const genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
    
    // Test with Gemini Flash model
    const model = genAI.getGenerativeModel({ model: "gemini-2.0-flash" });
    
    // Test with a simple prompt
    const prompt = "Hello, are you working?";
    console.log(`Testing with prompt: "${prompt}"`);
    
    try {
      const result = await model.generateContent(prompt);
      const response = await result.response;
      const text = response.text();
      
      console.log('\n✅ Connection to Gemini API successful!');
      console.log('\nModel test successful. Response:');
      console.log(text);
      
      console.log('\nGemini API configuration is correct and working!');
    } catch (error) {
      console.error('\n❌ Error connecting to Gemini API:', error.message);
      console.log('\nPossible issues:');
      
      // Check for API not enabled error
      if (error.message.includes('SERVICE_DISABLED') || error.message.includes('has not been used in project')) {
        console.log('ERROR: The Generative Language API is not enabled for your Google Cloud project.');
        console.log('\nTo fix this:');
        console.log('1. Go to: https://console.developers.google.com/apis/api/generativelanguage.googleapis.com/overview');
        console.log('2. Make sure you are logged in with the correct Google account');
        console.log('3. Select your project and enable the Generative Language API');
        console.log('4. Wait a few minutes for the changes to propagate');
        console.log('5. Run this check again');
      } else {
        console.log('1. Check if your API key is valid');
        console.log('2. Check your internet connection');
        console.log('3. Verify you have access to the Gemini API');
      }
    }
  } catch (error) {
    console.error('Error checking Gemini API:', error.message);
  }
}

checkGeminiApi();