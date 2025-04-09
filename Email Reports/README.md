# Email Summary Bot

A Node.js application that automatically fetches Microsoft 365 emails, summarizes them using Google Gemini 2.0 Flash AI, and sends a daily digest.

## Features

- Fetches emails from Microsoft 365 using Graph API
- Organizes emails by sender/employee
- Summarizes email content using Google Gemini 2.0 Flash AI
- Sends a formatted daily summary via Microsoft 365
- Scheduled to run automatically at 5 PM daily

## Setup

1. Install dependencies:
   ```
   npm install
   ```

2. Configure the following in your `.env` file:
   - Microsoft 365 API credentials (client ID, client secret, tenant ID)
   - Google Gemini API key (GEMINI_API_KEY)
   - Summary recipient email
   - Schedule time (default: 5 PM daily)

3. Get a Google API key for Gemini:
   - Go to https://makersuite.google.com/app/apikey
   - Create a new API key
   - Add it to your `.env` file as GEMINI_API_KEY

4. Verify the Google Gemini model is working:
   ```
   npm run check-models
   ```

## Usage

Start the application:
```
npm start
```

Run the summary process immediately (for testing):
```
node src/index.js --run-now
```

Run Marc's report (custom report):
```
node src/index.js --marc-report --days=30
```

Run Marc's COMPLETE report (with PowerShell Exchange Message Trace - requires admin permissions):
```
node src/index.js --marc-complete --days=30
```

Run Marc's report using Graph API (no PowerShell, best option - requires API permissions):
```
node src/index.js --marc-graph --days=30
```

Run report for all employees (requires Mail.ReadBasic.All permission):
```
node src/index.js --all-employee --days=14
```

Run report for a specific employee:
```
node src/index.js --employee=jared@gbl-data.com --days=14
```

## Environment Variables

- `MICROSOFT_CLIENT_ID`: Microsoft 365 application client ID
- `MICROSOFT_CLIENT_SECRET`: Microsoft 365 application client secret
- `MICROSOFT_TENANT_ID`: Microsoft 365 tenant ID
- `MICROSOFT_USER_EMAIL`: Email address of the user to authenticate as
- `GEMINI_API_KEY`: Google Gemini API key
- `SUMMARY_RECIPIENT`: Email address to receive the summary
- `SCHEDULE_TIME`: Cron expression for scheduling (default: "0 17 * * *" - 5 PM daily)