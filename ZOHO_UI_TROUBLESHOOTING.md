# Zoho CRM Integration UI Troubleshooting Guide

## Issue: Modules Not Loading in UI

If you click "Load Modules" but don't see the modules, follow these steps:

### Step 1: Check Browser Console
1. Open the Zoho CRM Integration page
2. Press `F12` to open Developer Tools
3. Go to the **Console** tab
4. Click "Load Modules" button
5. Look for any error messages in red
6. Check for messages like:
   - `Modules response: {...}` - This shows the API response
   - `Loaded X modules` - This confirms modules were loaded
   - Any error messages

### Step 2: Check Network Tab
1. In Developer Tools, go to the **Network** tab
2. Click "Load Modules"
3. Look for a request to `/api/zoho/list-modules`
4. Click on it to see:
   - **Status Code**: Should be 200 (green)
   - **Response**: Should show JSON with `success: true` and `modules: [...]`
   - If status is 400/401/500, check the error message

### Step 3: Verify Credentials
Make sure all fields are filled:
- ✅ API Domain (dropdown selected)
- ✅ Client ID
- ✅ Client Secret
- ✅ Refresh Token

### Step 4: Test the API Endpoint Directly

Run this test script (make sure Flask app is running):
```bash
python test_ui_modules.py
```

This will test if the backend API is working correctly.

### Step 5: Check Flask App Logs

Look at your Flask app console/terminal for error messages. You should see:
```
INFO: Attempting to get access token...
INFO: Access token obtained, API Domain: https://www.zohoapis.in
INFO: Fetching available modules...
INFO: Found 83 modules
```

### Common Issues and Solutions

#### Issue 1: "Missing required credentials"
**Solution**: Fill in all Zoho credential fields (Client ID, Client Secret, Refresh Token)

#### Issue 2: "Failed to obtain access token"
**Solution**: 
- Check your credentials are correct
- Verify the API Domain matches your Zoho region
- Check if refresh token is still valid

#### Issue 3: "No modules found" or empty response
**Solution**: 
- Check Flask app logs for errors
- Verify your Zoho account has modules enabled
- Try refreshing the page and trying again

#### Issue 4: Network Error / Connection Refused
**Solution**: 
- Make sure Flask app is running on port 5002
- Check if you're logged in (session might have expired)
- Try refreshing the page and logging in again

### Debug Helper

In the browser console, you can run:
```javascript
debugZohoModules()
```

This will show:
- Available modules array
- Container element
- Error element

### Manual Test

You can also test the endpoint manually using curl:
```bash
curl -X POST http://localhost:5002/api/zoho/list-modules \
  -H "Content-Type: application/json" \
  -H "Cookie: session=YOUR_SESSION_COOKIE" \
  -d '{
    "api_domain": "https://www.zohoapis.in",
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "refresh_token": "YOUR_REFRESH_TOKEN"
  }'
```

### Expected Behavior

When working correctly:
1. Click "Load Modules"
2. Button shows loading state
3. Spinner appears
4. After 2-5 seconds, modules appear in a grid
5. Green success message shows "Successfully loaded X modules!"
6. You can see checkboxes for each module

### Still Not Working?

1. **Restart Flask App**: Sometimes a restart fixes issues
2. **Clear Browser Cache**: Press `Ctrl+Shift+Delete` and clear cache
3. **Try Different Browser**: Test in Chrome, Firefox, or Edge
4. **Check Flask App Logs**: Look for Python errors in the terminal

### Contact for Help

If none of these work, check:
- Flask app terminal for Python errors
- Browser console for JavaScript errors
- Network tab for HTTP errors

