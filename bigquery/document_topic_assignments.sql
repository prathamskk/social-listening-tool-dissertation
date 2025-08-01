-- Table to store topic assignments for documents/clusters
-- Partitioned by date for efficient querying of recent assignments
-- Clustered by run_id and topic_id for efficient filtering and grouping
CREATE TABLE IF NOT EXISTS `social-listening-sense.social_listening_data.document_topic_assignments` (
    -- Run identification
    run_id STRING,                    -- Unique identifier for this clustering run
    assigned_at TIMESTAMP,            -- When this assignment was made
    
    -- Document identification and metadata
    unified_id STRING,                -- The content item ID
    source STRING,                    -- Source platform (e.g., 'reddit', 'twitter')
    content_type STRING,              -- Type of content (e.g., 'comment', 'post', 'reply')
    content_timestamp TIMESTAMP,      -- When the content was created
    primary_text STRING,              -- The main text content
    
    -- Topic assignment details
    topic_id INT64,                   -- The assigned cluster/topic ID
    assignment_score FLOAT64,         -- Distance to cluster center (lower is better)
    
    -- Sentiment information
    sentiment_score FLOAT64,          -- Sentiment score (-1 to 1)
    sentiment_magnitude FLOAT64,      -- Sentiment magnitude (0 to infinity)
    
    -- Constraints
    PRIMARY KEY(run_id, unified_id) NOT ENFORCED
)
PARTITION BY DATE(assigned_at)
CLUSTER BY run_id, topic_id;

-- Add description to table
ALTER TABLE `social-listening-sense.social_listening_data.document_topic_assignments`
SET OPTIONS(
    description="Stores topic assignments for documents from K-means clustering runs. Each row represents a document's assignment to a topic cluster, including metadata and sentiment information."
); 