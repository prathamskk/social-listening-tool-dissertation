/**
 * Functions related to the Social Media Scraper (Reddit, Quora, etc.).
 * Designed to be called by on-sheet buttons for specific platforms.
 */

/**
 * Public function to trigger Reddit link scraping.
 * Assign this to your "Scrape Reddit Links" button.
 */
function initiateRedditScrape() {
    _performSocialScrape('Reddit');
  }
  
  /**
   * Public function to trigger Quora link scraping.
   * Assign this to your "Scrape Quora Links" button.
   */
  function initiateQuoraScrape() {
    _performSocialScrape('Quora');
  }
  
  
  /**
   * Core private function to handle the social media scraping for a specified platform.
   * It ensures the correct sheet is active, extracts links, and sends them to the API.
   * @param {string} platform - The platform name ('Reddit' or 'Quora').
   */
  function _performSocialScrape(platform) {
    var ui = SpreadsheetApp.getUi();
    var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = spreadsheet.getSheetByName(SOCIAL_SCRAPER_CONTROL_SHEET_NAME);
  
    // --- Sheet Validation ---
    if (!sheet) {
      ui.alert('Error', `Sheet "${SOCIAL_SCRAPER_CONTROL_SHEET_NAME}" not found. Please ensure it exists and is named correctly.`, ui.ButtonSet.OK);
      return;
    }
  
    // Check if the correct sheet is currently active, or offer to switch
    if (sheet.getName() !== spreadsheet.getActiveSheet().getName()) {
      var response = ui.alert(
        'Incorrect Sheet',
        `This function is designed to run only on the "${SOCIAL_SCRAPER_CONTROL_SHEET_NAME}" sheet.\nDo you want to switch to it now?`,
        ui.ButtonSet.YES_NO
      );
      if (response === ui.Button.YES) {
        spreadsheet.setActiveSheet(sheet); // Switch to the correct sheet
      } else {
        ui.alert('Operation Cancelled', 'Please navigate to the correct sheet to run this function.', ui.ButtonSet.OK);
        return; // User chose not to switch, so exit
      }
    }
  
    // --- Determine Links and Dataset ID Based on Platform ---
    var links = [];
    var datasetId = '';
    var platformName = platform; // User-friendly name
  
    if (platform === 'Reddit') {
      links = getLinksFromColumn(REDDIT_LINK_COLUMN_INDEX);
      datasetId = REDDIT_DATASET_ID;
    } else if (platform === 'Quora') {
      links = getLinksFromColumn(QUORA_LINK_COLUMN_INDEX);
      datasetId = QUORA_DATASET_ID;
    } else {
      ui.alert('Programming Error', 'Invalid platform specified to internal function. Please contact support.', ui.ButtonSet.OK);
      return;
    }
  
    // --- Input Validation: Check for Links ---
    if (links.length === 0) {
      ui.alert('No Links Found', `No ${platformName} links found in column ${platform === 'Reddit' ? 'A' : 'D'}. Please paste links before running.`, ui.ButtonSet.OK);
      return;
    }
  
    // --- Confirmation Alert ---
    var confirmation = ui.alert(
      'Confirm Scraping',
      `You are about to send ${links.length} ${platformName} links for processing. This may take some time.\n\nContinue?`,
      ui.ButtonSet.YES_NO
    );
    if (confirmation !== ui.Button.YES) {
      ui.alert('Operation Cancelled', 'Scraping operation cancelled by user.', ui.ButtonSet.OK);
      return;
    }
  
    // --- Send Links to API & Process Response ---
    var result = sendLinks(links, datasetId, platformName); // Call helper to send and get structured result
  
    // --- Display User-Friendly Alert Based on Result ---
    if (result.status === 'success') {
      let successMessage = `${platformName} links sent successfully!\n\n`;
      successMessage += `Links submitted: ${links.length}\n`;
      if (result.snapshot_id && result.snapshot_id !== 'N/A') {
        successMessage += `Tracking ID (Snapshot ID): ${result.snapshot_id}\n`;
      }
      successMessage += `\nContent collection is now in progress.`;
  
      ui.alert('Success!', successMessage, ui.ButtonSet.OK);
    } else {
      let errorMessage = `${platformName} links failed to send. Please try again.`;
      errorMessage += `\n\nDetails: ${result.message}\n`;
      if (result.details) {
        errorMessage += `Technical Info: ${result.details}\n`; // For support/debugging if needed
      }
      errorMessage += `\nIf the problem continues, please contact support.`;
  
      ui.alert('Error', errorMessage, ui.ButtonSet.OK);
    }
  }
  
  
  /**
   * Helper function to extract links from a given column.
   * It now also removes query parameters from the links.
   * @param {number} columnIndex - The 1-indexed column number to read links from.
   * @returns {Array<string>} A 1D array of non-empty, trimmed links with query parameters removed.
   * @private
   */
  function getLinksFromColumn(columnIndex) {
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SOCIAL_SCRAPER_CONTROL_SHEET_NAME);
    var lastRow = sheet.getLastRow();
    if (lastRow < LINKS_START_ROW) { // LINKS_START_ROW from Config.gs (e.g., 2)
      return [];
    }
    var linkRange = sheet.getRange(LINKS_START_ROW, columnIndex, lastRow - (LINKS_START_ROW - 1), 1);
    var links2DArray = linkRange.getValues();
  
    var links = [];
    for (var i = 0; i < links2DArray.length; i++) {
      var link = String(links2DArray[i][0]).trim();
      if (link !== '') {
        // Remove query parameters
        var queryParamIndex = link.indexOf('?');
        if (queryParamIndex !== -1) {
          link = link.substring(0, queryParamIndex);
        }
        links.push(link);
      }
    }
    return links;
  }
  
  /**
   * Helper function to send links to the external social media scraper API.
   * @param {Array<string>} links - List of URLs to send.
   * @param {string} datasetId - The dataset ID for the platform.
   * @param {string} platformName - User-friendly name of the platform (e.g., 'Reddit', 'Quora').
   * @returns {object} A structured result object {status: 'success'|'error', message: string, details?: string, snapshot_id?: string, job_id?: string}.
   * @private
   */
  function sendLinks(links, datasetId, platformName) {
    var apiUrl = SOCIAL_API_BASE_URL + '?dataset_id=' + datasetId; // SOCIAL_API_BASE_URL from Config.gs
    var payload = JSON.stringify({ urls: links });
  
    var options = {
      'method' : 'post',
      'contentType': 'application/json',
      'payload' : payload,
      'muteHttpExceptions': true
    };
  
    try {
      var response = UrlFetchApp.fetch(apiUrl, options);
      var responseCode = response.getResponseCode();
      var responseText = response.getContentText();
  
      Logger.log(`${platformName} API Response Code: ${responseCode}`);
      Logger.log(`${platformName} API Response Text: ${responseText}`);
  
      let parsedResponse;
      try {
        parsedResponse = JSON.parse(responseText);
      } catch (e) {
        return {
          status: 'error',
          message: `API server returned an unreadable response.`,
          details: `HTTP Status: ${responseCode}, Raw Response: ${responseText.substring(0, 200)}...`
        };
      }
  
      if (responseCode >= 200 && responseCode < 300 && parsedResponse.status === 'success') {
        const snapshotId = parsedResponse.bright_data_response ? parsedResponse.bright_data_response.snapshot_id : 'N/A';
        return {
          status: 'success',
          message: `API call successful. Data upload initiated for ${links.length} links.`,
          details: parsedResponse.message, // "Bright Data API triggered and BigQuery insertion attempted"
          snapshot_id: snapshotId,
          job_id: parsedResponse.job_id || 'N/A' // If your scraper API returns a job ID
        };
      } else {
        let errorMessage = parsedResponse.message || `API error. Status: ${responseCode}.`;
        if (responseCode === 401) {
          errorMessage = `Access Denied: Please check permissions for the scraper API.`;
        } else if (responseCode >= 400 && responseCode < 500) {
          errorMessage = `Client Error (${responseCode}): ${errorMessage}`;
        } else if (responseCode >= 500) {
          errorMessage = `Server Error (${responseCode}): ${errorMessage}`;
        }
        return {
          status: 'error',
          message: errorMessage,
          details: parsedResponse.error || responseText.substring(0, 200) + '...'
        };
      }
  
    } catch (e) {
      Logger.log(`Error during ${platformName} API call: ${e.message}`);
      return {
        status: 'error',
        message: `An unexpected script error occurred during API call.`,
        details: e.message
      };
    }
  }