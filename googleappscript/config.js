/**
 * Global configuration variables for the Google Apps Script project.
 * Centralizes all configurable parameters for easy management.
 */


// In Config.gs

// --- Sheet Names for Navigation ---
const SEARCH_RESULTS_SHEET_NAME = 'Search Results';       // Name of the sheet where SERP results (links) are displayed
const SCRAPED_CONTENT_SHEET_NAME = 'Scraped Content';     // Name of the sheet where detailed social content is displayed


// --- SERP Scraper Configuration ---
const SERP_CLOUD_FUNCTION_URL = 'INSERT_URL_HERE';
const SERP_CONTROL_SHEET_NAME = 'Search Links';
const SERP_SOURCE_CELL = 'A5'; // Cell for source field (e.g., "reddit")
const SERP_QUERY_CELL = 'B5';   // Cell for search query
// ... (existing SERP Scraper Configuration) ...
const SERP_START_PAGE_CELL = 'D5'; // Add this line for the 'start' parameter input cell

// --- Social Scraper Configuration ---
const SOCIAL_SCRAPER_CONTROL_SHEET_NAME = 'Social Scraper'; // <--- ADD THIS LINE
const SOCIAL_API_BASE_URL = 'INSERT_URL_HERE';
const REDDIT_DATASET_ID = 'gd_lvz8ah06191smkebj4';
const QUORA_DATASET_ID = 'gd_lvz1rbj81afv3m6n5y';
const REDDIT_LINK_COLUMN_INDEX = 1; // Column A (1-indexed)
const QUORA_LINK_COLUMN_INDEX = 4;  // Column D (1-indexed)
const LINKS_START_ROW = 5; // Links start from row 2 (skipping header)

// --- K-Means Topic Modeling Configuration ---
const KMEANS_PERFORMER_ENDPOINT = 'INSERT_URL_HERE'; // <--- UPDATED URL AND NAME
const KMEANS_CONTROL_SHEET_NAME = 'Topic Modeler';
const KMEANS_ID_COLUMN_INDEX = 1;
const KMEANS_HEADER_ROW_COUNT = 1;