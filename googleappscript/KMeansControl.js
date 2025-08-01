/**
 * Functions related to K-Means Topic Modeling control from Google Sheets.
 * Triggers a Cloud Function to perform K-Means clustering.
 */
function runKMeansFromSheet() {
    const ui = SpreadsheetApp.getUi();
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = spreadsheet.getSheetByName(KMEANS_CONTROL_SHEET_NAME);
  
    // Clear previous status messages in the sheet (assuming G6:G11 or similar range)
    // sheet.getRange('G6:G11').clearContent(); // Keep if you still want sheet cell updates
  
  
    // --- 1. Sheet Validation ---
    if (!sheet) {
      ui.alert('Error', `Sheet "${KMEANS_CONTROL_SHEET_NAME}" not found. Please ensure it exists and is named correctly.`, ui.ButtonSet.OK);
      return;
    }
  
    if (sheet.getName() !== spreadsheet.getActiveSheet().getName()) {
      const response = ui.alert(
        'Wrong Sheet', // User-friendly title
        `This tool is designed to run only on the "${KMEANS_CONTROL_SHEET_NAME}" sheet.\n\nWould you like to switch to it now?`, // Clearer question
        ui.ButtonSet.YES_NO
      );
      if (response === ui.Button.YES) {
        spreadsheet.setActiveSheet(sheet);
      } else {
        ui.alert('Cancelled', 'Please navigate to the correct sheet to use this feature.', ui.ButtonSet.OK); // Simpler message
        return;
      }
    }
  
    // --- 2. Get Unified IDs ---
    // Assuming KMEANS_HEADER_ROW_COUNT + 1 is the starting row for IDs
    const range = sheet.getRange(KMEANS_HEADER_ROW_COUNT + 1, KMEANS_ID_COLUMN_INDEX, sheet.getLastRow() - KMEANS_HEADER_ROW_COUNT, 1);
    const unifiedIds = range.getValues().map(row => String(row[0]).trim()).filter(id => id !== '');
  
    if (unifiedIds.length === 0) {
      ui.alert('No Content Selected', `Please paste the IDs of the social content you want to analyze into Column A before clicking the button.`, ui.ButtonSet.OK); // Clear action
      return;
    }
  
    // --- 3. Prompt for Number of Topics ---
    const numTopicsInput = ui.prompt(
      'Step 1: Choose Number of Topics', // Clearer step title
      `How many distinct groups or themes would you like the tool to find in your ${unifiedIds.length} pieces of content? (Minimum: 2)`, // Explain "topics" simply
      ui.ButtonSet.OK_CANCEL
    );
  
    if (numTopicsInput.getSelectedButton() !== ui.Button.OK) {
      ui.alert('Cancelled', 'Topic analysis cancelled by user.', ui.ButtonSet.OK); // Clear cancellation message
      return;
    }
  
    const numTopics = parseInt(numTopicsInput.getResponseText(), 10);
    if (isNaN(numTopics) || numTopics < 2) {
      ui.alert('Invalid Number', 'Please enter a whole number (e.g., 5, 10) that is 2 or greater.', ui.ButtonSet.OK); // Simpler error
      return;
    }
  
    // --- 4. Prompt for Run Description ---
    let description = '';
    while (true) {
      const descriptionInput = ui.prompt(
        'Step 2: Give Your Run a Title', // Clearer step title
        'Please enter a short title for this analysis (e.g., "Laptop Reviews Q2" or "Customer Service Issues May"). This helps you find your results later.', // Explain why
        ui.ButtonSet.OK_CANCEL
      );
      if (descriptionInput.getSelectedButton() !== ui.Button.OK) {
        ui.alert('Cancelled', 'Topic analysis cancelled by user.', ui.ButtonSet.OK); // Clear cancellation message
        return;
      }
      description = descriptionInput.getResponseText().trim();
      if (description) {
        break;
      } else {
        ui.alert('Title Needed', 'You must enter a title for this analysis to proceed.', ui.ButtonSet.OK); // Simpler error
      }
    }
  
    // --- 5. Confirmation Before Sending ---
    const confirmation = ui.alert(
      'Confirm Analysis Start', // Simpler title
      `You are about to start analyzing ${unifiedIds.length} pieces of content to find ${numTopics} topics.\n\n` +
      `Your analysis title: "${description}"\n\n` +
      `This will trigger a process in the cloud and may take a few minutes to complete.\n\nDo you want to continue?`, // Simpler language for cloud/cost
      ui.ButtonSet.YES_NO
    );
  
    if (confirmation !== ui.Button.YES) {
      ui.alert('Cancelled', 'Topic analysis cancelled by user.', ui.ButtonSet.OK); // Clear cancellation message
      return;
    }
  
    try {
      const payload = {
        ids: unifiedIds,
        n_clusters: numTopics,
        description: description
      };
  
      const options = {
        'method': 'post',
        'contentType': 'application/json',
        'payload': JSON.stringify(payload),
        'muteHttpExceptions': true
      };
  
      // --- Indicate Processing Started (Optional: can update a cell on sheet if desired) ---
      // sheet.getRange('G6').setValue('Starting...'); // Example of sheet update if preferred over only pop-up
      // SpreadsheetApp.flush();
  
      const response = UrlFetchApp.fetch(KMEANS_PERFORMER_ENDPOINT, options);
      const responseCode = response.getResponseCode();
      const responseText = response.getContentText();
  
      Logger.log('K-Means Cloud Function Trigger Response Code: ' + responseCode);
      Logger.log('K-Means Cloud Function Trigger Response Text: ' + responseText);
  
      let parsedResponse;
      try {
        parsedResponse = JSON.parse(responseText);
      } catch (e) {
        parsedResponse = { status: 'error', message: 'The tool received an unexpected response format from the server. Please try again.' };
      }
  
      // --- Final Success/Error Alert ---
      if (responseCode >= 200 && responseCode < 300 && parsedResponse.status === 'success') {
        const runId = parsedResponse.run_id || 'N/A';
        const workflowExecutionName = parsedResponse.workflow_execution_name || 'N/A';
        const cloudFunctionMessage = parsedResponse.message || 'Process started.';
        
        let finalSuccessMessage = `Topic analysis has started successfully!\n\n`;
        finalSuccessMessage += `Your unique Analysis ID: ${runId}\n`;
        if (workflowExecutionName !== 'N/A') {
            finalSuccessMessage += `(Cloud Process ID: ${workflowExecutionName.split('/').pop()})\n`; // Show just the execution ID part
        }
        finalSuccessMessage += `\nYour results will appear in the 'Dynamic Topic Insights Dashboard' (Looker Studio).\n`;
        finalSuccessMessage += `Please use your Analysis ID or the title "${description}" to find your results.`;
        
        ui.alert('Analysis Started!', finalSuccessMessage, ui.ButtonSet.OK);
  
        // --- Optional: Update sheet cells with run_id and job_ids for tracking ---
        // sheet.getRange('G6').setValue('Started');
        // sheet.getRange('G7').setValue(runId);
        // sheet.getRange('G8').setValue(parsedResponse.create_model_job_id || 'N/A');
        // sheet.getRange('G9').setValue(parsedResponse.predict_job_id || 'N/A');
        // sheet.getRange('G10').setValue(parsedResponse.labeling_job_id || 'N/A'); // If your CF returns this
        // sheet.getRange('G11').setValue(cloudFunctionMessage);
        // SpreadsheetApp.flush();
  
      } else {
        let errorMessage = 'Something went wrong while trying to start your topic analysis. Please try again.';
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
      Logger.log('Error during Workflow call: ' + e.message);
      ui.alert('System Error', 'An unexpected system error occurred. Please contact support and provide these details:\n' + e.message, ui.ButtonSet.OK);
    }
  }