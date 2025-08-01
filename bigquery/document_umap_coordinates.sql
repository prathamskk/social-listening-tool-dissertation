-- Table to store UMAP dimensionality reduction coordinates for documents
-- Partitioned by date for efficient querying of recent coordinates
-- Clustered by run_id for efficient filtering
CREATE TABLE IF NOT EXISTS `social-listening-sense.social_listening_data.document_umap_coordinates` (
    -- Run identification
    run_id STRING,                    -- Unique identifier for this UMAP run (same as kmeans run)
    created_at TIMESTAMP,             -- When these coordinates were created
    
    -- Document identification
    unified_id STRING,                -- The content item ID
    
    -- UMAP coordinates
    umap_x FLOAT64,                   -- X coordinate in 2D space
    umap_y FLOAT64,                   -- Y coordinate in 2D space
    
    -- Constraints
    PRIMARY KEY(run_id, unified_id) NOT ENFORCED
)
PARTITION BY DATE(created_at)
CLUSTER BY run_id;

-- Add description to table
ALTER TABLE `social-listening-sense.social_listening_data.document_umap_coordinates`
SET OPTIONS(
    description="Stores 2D UMAP coordinates for documents, created during the same run as K-means clustering. Used for visualization of document relationships in 2D space."
); 