-- Table to track K-means clustering runs and their status
CREATE TABLE IF NOT EXISTS `social-listening-sense.social_listening_data.kmeans_runs` (
    run_id STRING,
    created_at TIMESTAMP,
    num_topics INT64,
    description STRING,
    model_name STRING,      -- Name of the temp K-Means model (e.g., temp_topic_model_run_id)
    model_creation_job_id STRING, -- BigQuery job ID for ML.CREATE_MODEL
    predict_job_id STRING,        -- BigQuery job ID for ML.PREDICT
    labeling_job_id STRING,       -- BigQuery job ID for the labeling process
    status STRING,                  -- e.g., "submitted", "model_created", "prediction_completed", "labeled_completed", "failed"
    error_message STRING,           -- Any error message if the run failed
    embedding_model STRING          -- e.g., "text-embedding-004"
); 