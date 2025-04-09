require('dotenv').config();
require('isomorphic-fetch');
const { ClientSecretCredential } = require('@azure/identity');
const { Client } = require('@microsoft/microsoft-graph-client');
const { TokenCredentialAuthenticationProvider } = require('@microsoft/microsoft-graph-client/authProviders/azureTokenCredentials');

/**
 * Script to verify admin permissions and test Exchange Admin features
 */
async function verifyAdminPermissions() {
  console.log('Verifying admin permissions for Exchange features...');
  
  try {
    // Create the Microsoft Graph client
    const credential = new ClientSecretCredential(
      process.env.MICROSOFT_TENANT_ID,
      process.env.MICROSOFT_CLIENT_ID,
      process.env.MICROSOFT_CLIENT_SECRET
    );

    const authProvider = new TokenCredentialAuthenticationProvider(credential, {
      scopes: ['https://graph.microsoft.com/.default']
    });

    const client = Client.initWithMiddleware({
      authProvider
    });
    
    // Test 1: Test basic admin access
    console.log('\nTest 1: Verifying basic admin access');
    try {
      const meResponse = await client.api('/me').get();
      console.log('✅ Successfully accessed /me endpoint');
      console.log(`   Logged in as: ${meResponse.displayName} (${meResponse.userPrincipalName})`);
    } catch (error) {
      console.log('❌ Failed to access /me endpoint');
      console.log(`   Error: ${error.message}`);
    }
    
    // Test 2: Check access to all users
    console.log('\nTest 2: Checking access to all users (Directory.Read.All)');
    try {
      const usersResponse = await client.api('/users').top(5).get();
      console.log('✅ Successfully accessed user directory');
      console.log(`   Retrieved ${usersResponse.value.length} users`);
    } catch (error) {
      console.log('❌ Failed to access user directory');
      console.log(`   Error: ${error.message}`);
    }
    
    // Test 3: Check access to mailboxes
    console.log('\nTest 3: Checking access to mailboxes (Mail.ReadBasic.All or Mail.Read.All)');
    try {
      const mailboxResponse = await client.api('/users/marc@gbl-data.com/mailFolders/sentitems/messages')
        .top(1)
        .get();
      console.log('✅ Successfully accessed mailbox data');
      console.log(`   Retrieved ${mailboxResponse.value.length} messages`);
    } catch (error) {
      console.log('❌ Failed to access mailbox data');
      console.log(`   Error: ${error.message}`);
    }
    
    // Test 4: Check access to reports (Exchange Admin)
    console.log('\nTest 4: Checking access to message trace reports (Reports.Read.All)');
    try {
      const reportResponse = await client.api('/reports/getEmailActivityUserDetail(period=\'D7\')')
        .get();
      console.log('✅ Successfully accessed email activity reports');
      console.log('   Reports data is available');
    } catch (error) {
      console.log('❌ Failed to access email activity reports');
      console.log(`   Error: ${error.message}`);
    }
    
    // Test 5: Try to use mailbox settings API (requires Exchange permissions)
    console.log('\nTest 5: Checking mailbox settings access (MailboxSettings.Read)');
    try {
      const settingsResponse = await client.api('/users/marc@gbl-data.com/mailboxSettings')
        .get();
      console.log('✅ Successfully accessed mailbox settings');
      console.log('   Mailbox settings are available');
    } catch (error) {
      console.log('❌ Failed to access mailbox settings');
      console.log(`   Error: ${error.message}`);
    }
    
    // Test 6: Try to create a transport rule (requires Exchange admin)
    console.log('\nTest 6: Checking ability to manage transport rules (Exchange.ManageAsApp)');
    try {
      // This is a read operation only - no changes made
      const rulesResponse = await client.api('/security/transportRules')
        .get();
      console.log('✅ Successfully accessed transport rules');
      console.log(`   Found ${rulesResponse.value ? rulesResponse.value.length : 0} transport rules`);
    } catch (error) {
      console.log('❌ Failed to access transport rules');
      console.log(`   Error: ${error.message}`);
      
      // Try beta endpoint instead
      try {
        const betaClient = Client.initWithMiddleware({
          baseUrl: 'https://graph.microsoft.com/beta',
          authProvider
        });
        
        const betaRulesResponse = await betaClient.api('/security/transportRules').get();
        console.log('✅ Successfully accessed transport rules via beta endpoint');
        console.log(`   Found ${betaRulesResponse.value ? betaRulesResponse.value.length : 0} transport rules`);
      } catch (betaError) {
        console.log('❌ Failed to access transport rules via beta endpoint');
        console.log(`   Error: ${betaError.message}`);
      }
    }
    
    // Conclusion
    console.log('\n=== Permission Summary ===');
    console.log('Based on the test results, you can:');
    console.log('1. Use the existing Marc-Graph API approach for comprehensive reporting on Marc\'s emails');
    console.log('2. Use the All-Employee report to analyze team communication patterns');
    console.log('3. For fully comprehensive email tracking, set up a mail flow rule in Exchange Admin Center');
    console.log('   to BCC all outgoing emails to an archive mailbox');
  } catch (error) {
    console.error('Script error:', error);
  }
}

// Run the verification
verifyAdminPermissions();