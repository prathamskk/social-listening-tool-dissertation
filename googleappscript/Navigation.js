/**
 * Functions for navigating between different sheets in the Social Listening Control Center.
 * These are typically assigned to on-sheet buttons.
 */

/**
 * Helper function to switch to a specified sheet.
 * Displays an alert if the sheet is not found.
 * @param {string} targetSheetName - The exact name of the sheet to activate.
 * @private
 */
function _goToSheet(targetSheetName) {
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const targetSheet = spreadsheet.getSheetByName(targetSheetName);
  
    if (targetSheet) {
      spreadsheet.setActiveSheet(targetSheet);
    } else {
      SpreadsheetApp.getUi().alert('Error', `Sheet "${targetSheetName}" not found. Please ensure it exists and is named correctly.`, SpreadsheetApp.getUi().ButtonSet.OK);
    }
  }
  
  /**
   * Navigates to the 'Search Results' sheet to view collected links from search engine queries.
   * This function should be assigned to the "View Results" button on the 'Search Links' sheet.
   */
  function viewSearchResults() {
    _goToSheet(SEARCH_RESULTS_SHEET_NAME);
  }
  
  /**
   * Navigates to the 'Scraped Content' sheet to view detailed social media posts and comments.
   * This function should be assigned to the "View Scraped Content" button on the 'Social Scraper' sheet.
   */
  function viewScrapedContent() {
    _goToSheet(SCRAPED_CONTENT_SHEET_NAME);
  }
  
  // You might add more navigation functions here later if needed
  // function viewTopicModelingResults() {
  //   _goToSheet(TOPIC_MODELING_RESULTS_SHEET_NAME);
  // }
  
  
  /**
   * Navigates to the 'Social Scraper' sheet to initiate detailed content collection from links.
   * This function should be assigned to the button on the 'Search Results' sheet.
   */
  function goToSocialScraperSheet() {
    _goToSheet(SOCIAL_SCRAPER_CONTROL_SHEET_NAME);
  }
  
  // In Navigation.gs
  function goToTopicModelerSheet() {
    _goToSheet(KMEANS_CONTROL_SHEET_NAME);
  }