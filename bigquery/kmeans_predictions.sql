-- Table to store all K-means clustering predictions
-- Partitioned by date for efficient querying of recent predictions
-- Clustered by run_id and predicted_cluster for efficient filtering and grouping
CREATE TABLE IF NOT EXISTS `social-listening-sense.social_listening_data.kmeans_predictions` (
    run_id STRING,
    unified_id STRING,
    predicted_cluster INT64,
    distance FLOAT64,
    created_at TIMESTAMP,
    PRIMARY KEY(run_id, unified_id) NOT ENFORCED
)
PARTITION BY DATE(created_at)
CLUSTER BY run_id, predicted_cluster; 