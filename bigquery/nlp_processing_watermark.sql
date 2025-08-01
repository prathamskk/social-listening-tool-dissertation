CREATE TABLE `social-listening-sense.social_listening_data.nlp_processing_watermark` (
  table_name STRING NOT NULL,
  last_processed_timestamp TIMESTAMP,
  updated_at TIMESTAMP
);

-- Initialize the watermark for your unified content view (optional, but good practice)
-- If not initialized, the first run will process all historical data.
INSERT INTO `social-listening-sense.social_listening_data.nlp_processing_watermark` (table_name, last_processed_timestamp, updated_at)
VALUES ('unified_social_content_items', TIMESTAMP('1970-01-01 00:00:00 UTC'), CURRENT_TIMESTAMP());