// In SerpScraper.gs
function initiateSerpScraper() {
    var ui = SpreadsheetApp.getUi();
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = spreadsheet.getSheetByName(SERP_CONTROL_SHEET_NAME);
  
    if (!sheet) {
      ui.alert('Error', `Sheet "${SERP_CONTROL_SHEET_NAME}" not found. Please ensure it exists and is named correctly.`, ui.ButtonSet.OK);
      return;
    }
    spreadsheet.setActiveSheet(sheet);
  
    var sourceValue = sheet.getRange(SERP_SOURCE_CELL).getValue();
    var rawSearchQuery = sheet.getRange(SERP_QUERY_CELL).getValue();
    var startPageNumInput = sheet.getRange(SERP_START_PAGE_CELL).getValue();
  
    // --- Basic Input Validation (Pop-up only) ---
    if (!sourceValue || sourceValue.toLowerCase() !== 'reddit') {
      ui.alert('Missing Info', `Please ensure the Source field (${SERP_SOURCE_CELL}) contains "reddit".`, ui.ButtonSet.OK);
      return;
    }
  
    if (!rawSearchQuery) {
      ui.alert('Missing Info', `Please enter a search phrase or keyword in the Search Query field (${SERP_QUERY_CELL}) before clicking "Get Results".`, ui.ButtonSet.OK);
      return;
    }
  
    // --- NEW: Validate startPageNumInput and calculate the actual 'start' offset ---
    var startParam = 0; // Default API offset to 0 (for page 1)
    var displayPageNum = 1; // Default page number for message display (1-indexed)
    
    if (startPageNumInput !== null && startPageNumInput !== '') { 
      var parsedPageNum = parseInt(startPageNumInput, 10);
      
      if (isNaN(parsedPageNum) || parsedPageNum < 0) {
        ui.alert('Invalid Input', `The "Start Page Number" (${SERP_START_PAGE_CELL}) must be a non-negative whole number (e.g., 0 or 1 for the first page, 2 for the second page).`, ui.ButtonSet.OK);
        return;
      }
  
      // --- CRUCIAL LOGIC CHANGE FOR FLEXIBLE PAGE NUMBER INPUT ---
      if (parsedPageNum === 0 || parsedPageNum === 1) {
        startParam = 0;      // API offset 0 for user input 0 or 1
        displayPageNum = 1;  // Display "page 1" for these inputs
      } else {
        startParam = (parsedPageNum - 1) * 10; // For page 2 (input 2) -> offset 10; for page 3 (input 3) -> offset 20
        displayPageNum = parsedPageNum;        // Display the user's input (2, 3, etc.)
      }
    }
  
  
    var cloudFunctionQuery = rawSearchQuery;
    if (sourceValue.toLowerCase() === 'reddit') {
      cloudFunctionQuery = 'site:reddit.com ' + rawSearchQuery;
    }
  
    // --- Append the 'start' parameter to the request URL ---
    var requestUrl = SERP_CLOUD_FUNCTION_URL + '?q=' + encodeURIComponent(cloudFunctionQuery);
    if (startParam > 0) { // Only add 'start' parameter if the offset is not 0
      requestUrl += '&start=' + startParam;
    }
  
    var options = {
      'method' : 'get',
      'muteHttpExceptions': true
    };
  
    try {
      var response = UrlFetchApp.fetch(requestUrl, options);
      var responseCode = response.getResponseCode();
      var responseText = response.getContentText();
  
      Logger.log('Scraper Cloud Function Response Code: ' + responseCode);
      Logger.log('Scraper Cloud Function Response Text: ' + responseText);
  
      let parsedResponse;
      try {
        parsedResponse = JSON.parse(responseText);
      } catch (e) {
        parsedResponse = { status: 'error', message: 'The tool received an unexpected response format from the server. Please try again.' };
      }
  
      if (responseCode >= 200 && responseCode < 300) { 
        const successMessage = parsedResponse.message || 'Scraper job triggered successfully.';
        const rowsCollected = parsedResponse.rows_inserted !== undefined ? parsedResponse.rows_inserted : 'an unknown number of';
        const actualQuery = parsedResponse.query || rawSearchQuery;
  
        let userFriendlyMessage = `Successfully collected search links!\n\n`;
        userFriendlyMessage += `For the search: "${rawSearchQuery}"\n`;
        if (cloudFunctionQuery !== rawSearchQuery) {
            userFriendlyMessage += `(Full query used: "${cloudFunctionQuery}")\n`;
        }
        // --- NEW: Always display the user-friendly page number based on calculated 'displayPageNum' ---
        userFriendlyMessage += `Starting from page number: ${displayPageNum}\n`; 
        userFriendlyMessage += `Links found and saved: ${rowsCollected}`;
        userFriendlyMessage += `\n\nClick "View Results" to see the collected links.`;
  
        ui.alert('Success!', userFriendlyMessage, ui.ButtonSet.OK);
      } else {
        let errorMessage = 'Something went wrong while trying to get search links. Please try again.';
        if (responseCode === 401) {
            errorMessage = 'Access Denied: The tool could not connect securely. Please ensure your permissions are correct or contact support.';
        } else if (parsedResponse.message) {
            errorMessage = `Error: ${parsedResponse.message}`;
        } else if (responseText) {
            errorMessage = `Error details: ${responseText.substring(0, 100)}...`;
        }
        errorMessage += `\n\nIf the problem continues, please contact support.`;
  
        ui.alert('Error', errorMessage, ui.ButtonSet.OK);
      }
  
    } catch (e) {
      Logger.log('Error during Scraper Cloud Function call: ' + e.message);
      ui.alert('System Error', 'An unexpected system error occurred. Please contact support and provide these details:\n' + e.message, ui.ButtonSet.OK);
    }
  }