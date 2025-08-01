CREATE TABLE `social-listening-sense.social_listening_data.social_content_enriched` (
  -- Identifier for a specific batch/run of analysis (NULL for scheduled general analysis)
  run_id STRING OPTIONS(description="Unique ID for a specific ad-hoc analysis run. NULL for scheduled general analysis."),

  -- Type of processing: 'scheduled_general' or 'ad_hoc_consultant'
  processing_type STRING NOT NULL OPTIONS(description="'scheduled_general' or 'ad_hoc_consultant'. Defines how this data was processed."),

  -- Source platform (e.g., 'Reddit', 'Quora', 'Twitter')
  source STRING NOT NULL,

  -- Type of content item (e.g., 'post', 'comment', 'reply', 'answer', 'tweet')
  content_type STRING NOT NULL,

  -- Unique ID for the content item (matches unified_social_content_items.content_item_id)
  content_item_id STRING NOT NULL,

  -- ID of the direct parent content item (e.g., post_id for a comment)
  parent_content_item_id STRING,

  -- ID of the original top-level post/question/tweet in the thread
  top_level_post_id STRING NOT NULL,

  -- Timestamp when this specific content item was processed by NLP
  analysis_timestamp TIMESTAMP NOT NULL OPTIONS(description="When this specific content item was processed by NLP."),

  -- The exact full_text_context string that was passed to the NLP model
  text_analyzed_for_nlp STRING OPTIONS(description="The exact full_text_context string that was passed to the NLP model."),

  -- Sentiment label (e.g., 'Positive', 'Negative', 'Neutral', 'Error')
  sentiment_label STRING,

  -- Sentiment score (e.g., -1.0 to 1.0)
  sentiment_score FLOAT64,

  -- Assigned topic ID (integer)
  topic_id INT64,

  -- Human-readable topic label (e.g., 'Battery Life Concerns')
  topic_label STRING,

  -- List of key keywords extracted for this content item
  keyword_list ARRAY<STRING>
)
-- Optional: Add partitioning and clustering for performance if table grows very large
-- PARTITION BY DATE(analysis_timestamp)
-- CLUSTER BY source, content_type, topic_id
;