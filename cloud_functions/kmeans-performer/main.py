import functions_framework
from flask import jsonify, request
from google.cloud import bigquery
import logging
import os
import json
import time
from datetime import datetime
from google.cloud.exceptions import NotFound
import numpy as np
import umap
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get('GCP_PROJECT', 'social-listening-sense')
BIGQUERY_LOCATION = 'eu'  # Ensure this matches your dataset location

def create_kmeans_model_job(unified_ids: list, n_clusters: int, bigquery_dataset_id: str, description: str = None):
    """
    Constructs and submits a BigQuery ML job to create a K-Means model using
    an Array Parameter for the unified_ids list.

    Args:
        unified_ids: A list of content_item_ids (strings) to cluster.
        n_clusters: The number of clusters (K) for K-Means.
        bigquery_dataset_id: The BigQuery dataset ID to use.
        description: Optional description for the run.

    Returns:
        A dictionary containing the run_id and the BigQuery job ID for model creation.
    """
    client = bigquery.Client(project=PROJECT_ID)

    # Generate a unique run_id for this K-Means operation
    run_id = f"kmeans_run_{int(time.time())}_{n_clusters}"
    model_name = f"temp_topic_model_{run_id}"

    create_model_sql = f"""
    CREATE OR REPLACE MODEL
      `{PROJECT_ID}.{bigquery_dataset_id}.temp_topic_model_{run_id}`
    OPTIONS(
      model_type='KMEANS',
      num_clusters={n_clusters},
      kmeans_init_method='KMEANS_PLUS_PLUS',
      max_iterations=50,
      distance_type='COSINE'  # Using cosine distance for embedding vectors
    )
    AS
    SELECT
      ec.unified_id,
      ec.embeddings AS feature_vector
    FROM
      `{PROJECT_ID}.{bigquery_dataset_id}.unified_social_content_items` AS v
    INNER JOIN
      `{PROJECT_ID}.{bigquery_dataset_id}.embeddings_cache` AS ec
      ON v.content_item_id = ec.unified_id
    WHERE
      ec.unified_id IN UNNEST(@unified_id_list)
      AND ec.embeddings IS NOT NULL AND ARRAY_LENGTH(ec.embeddings) > 0
    """

    logger.info(f"Submitting K-Means model creation job for run_id: {run_id}")

    # Configure the BigQuery job with the Array Parameter
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "unified_id_list",  # Parameter name matching @unified_id_list in SQL
                "STRING",           # Type of elements in the array
                unified_ids         # The Python list of IDs
            ),
        ],
        labels={
            'run_id': run_id,
            'job_type': 'kmeans_model_creation',
            'num_topics': str(n_clusters)
        },
        default_dataset=client.dataset(bigquery_dataset_id, project=PROJECT_ID)
    )

    try:
        query_job = client.query(create_model_sql, job_config=job_config, location=BIGQUERY_LOCATION)
        logger.info(f"K-Means model creation job submitted successfully: {query_job.job_id}")

        # Insert a row into kmeans_runs table
        insert_run_sql = f"""
        INSERT INTO `{PROJECT_ID}.social_listening_data.kmeans_runs`
        (run_id, created_at, num_topics, description, model_name, model_creation_job_id, status, embedding_model)
        VALUES
        (@run_id, @created_at, @num_topics, @description, @model_name, @model_creation_job_id, @status, @embedding_model)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.utcnow()),
                bigquery.ScalarQueryParameter("num_topics", "INT64", n_clusters),
                bigquery.ScalarQueryParameter("description", "STRING", description),
                bigquery.ScalarQueryParameter("model_name", "STRING", model_name),
                bigquery.ScalarQueryParameter("model_creation_job_id", "STRING", query_job.job_id),
                bigquery.ScalarQueryParameter("status", "STRING", "submitted"),
                bigquery.ScalarQueryParameter("embedding_model", "STRING", "text-embedding-004")
            ]
        )

        insert_job = client.query(insert_run_sql, job_config=job_config, location=BIGQUERY_LOCATION)
        insert_job.result()  # Wait for the insert to complete
        logger.info(f"Inserted run record into kmeans_runs table for run_id: {run_id}")

        return {
            "run_id": run_id,
            "job_id": query_job.job_id,
            "status": "submitted"
        }
    except Exception as e:
        logger.error(f"Error submitting K-Means model creation job: {e}")
        # If there's an error, try to insert a failed status record
        try:
            error_insert_sql = f"""
            INSERT INTO `{PROJECT_ID}.social_listening_data.kmeans_runs`
            (run_id, created_at, num_topics, description, model_name, status, error_message, embedding_model)
            VALUES
            (@run_id, @created_at, @num_topics, @description, @model_name, @status, @error_message, @embedding_model)
            """

            error_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", datetime.utcnow()),
                    bigquery.ScalarQueryParameter("num_topics", "INT64", n_clusters),
                    bigquery.ScalarQueryParameter("description", "STRING", description),
                    bigquery.ScalarQueryParameter("model_name", "STRING", model_name),
                    bigquery.ScalarQueryParameter("status", "STRING", "failed"),
                    bigquery.ScalarQueryParameter("error_message", "STRING", str(e)),
                    bigquery.ScalarQueryParameter("embedding_model", "STRING", "text-embedding-004")
                ]
            )

            error_insert_job = client.query(error_insert_sql, job_config=error_job_config, location=BIGQUERY_LOCATION)
            error_insert_job.result()
            logger.info(f"Inserted failed run record into kmeans_runs table for run_id: {run_id}")
        except Exception as insert_error:
            logger.error(f"Error inserting failed run record: {insert_error}")
        
        raise

def run_prediction_job(client, run_id: str, unified_ids: list, bigquery_dataset_id: str):
    """
    Runs ML.PREDICT on the created K-means model for the given unified IDs.
    Results are stored in the document_topic_assignments table with run_id to distinguish between runs.
    Includes additional metadata from content items and sentiment scores.
    
    Args:
        client: BigQuery client
        run_id: The run ID for this K-means operation
        unified_ids: List of content IDs to predict clusters for
        bigquery_dataset_id: The BigQuery dataset ID
        
    Returns:
        The BigQuery job ID for the prediction job
    """
    model_name = f"temp_topic_model_{run_id}"
    
    # Run the prediction and insert into the permanent table
    predict_sql = f"""
    INSERT INTO `{PROJECT_ID}.{bigquery_dataset_id}.document_topic_assignments` (
        run_id, unified_id, topic_id, assignment_score, assigned_at,
        source, content_type, content_timestamp, primary_text, sentiment_score, sentiment_magnitude
    )
    SELECT
        @run_id AS run_id,
        predicted_results.unified_id,
        predicted_results.CENTROID_ID AS topic_id,
        predicted_results.NEAREST_CENTROIDS_DISTANCE[OFFSET(0)].DISTANCE AS assignment_score,
        CURRENT_TIMESTAMP() AS assigned_at,
        v.source,
        v.content_type,
        v.content_timestamp,
        v.primary_text,
        ec_full.sentiment_score,
        ec_full.sentiment_magnitude
    FROM
        ML.PREDICT(
            MODEL `{PROJECT_ID}.{bigquery_dataset_id}.{model_name}`,
            (
                SELECT
                    ec.unified_id,
                    ec.embeddings AS feature_vector
                FROM
                    `{PROJECT_ID}.{bigquery_dataset_id}.unified_social_content_items` AS v
                INNER JOIN
                    `{PROJECT_ID}.{bigquery_dataset_id}.embeddings_cache` AS ec
                    ON v.content_item_id = ec.unified_id
                WHERE
                    ec.unified_id IN UNNEST(@unified_id_list)
                    AND ec.embeddings IS NOT NULL AND ARRAY_LENGTH(ec.embeddings) > 0
            )
        ) AS predicted_results
    JOIN
        `{PROJECT_ID}.{bigquery_dataset_id}.unified_social_content_items` AS v
        ON predicted_results.unified_id = v.content_item_id
    JOIN
        `{PROJECT_ID}.{bigquery_dataset_id}.embeddings_cache` AS ec_full
        ON predicted_results.unified_id = ec_full.unified_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ArrayQueryParameter(
                "unified_id_list",
                "STRING",
                unified_ids
            ),
        ],
        labels={
            'run_id': run_id,
            'job_type': 'kmeans_prediction'
        }
    )

    try:
        predict_job = client.query(predict_sql, job_config=job_config, location=BIGQUERY_LOCATION)
        logger.info(f"Prediction job submitted successfully: {predict_job.job_id}")
        
        # Update kmeans_runs table with prediction job ID
        update_sql = f"""
        UPDATE `{PROJECT_ID}.social_listening_data.kmeans_runs`
        SET predict_job_id = @predict_job_id,
            status = 'prediction_started'
        WHERE run_id = @run_id
        """
        
        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("predict_job_id", "STRING", predict_job.job_id),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
            ]
        )
        
        update_job = client.query(update_sql, job_config=update_config, location=BIGQUERY_LOCATION)
        update_job.result()
        
        return predict_job.job_id
        
    except Exception as e:
        logger.error(f"Error submitting prediction job: {e}")
        # Update status to failed
        error_update_sql = f"""
        UPDATE `{PROJECT_ID}.social_listening_data.kmeans_runs`
        SET status = 'failed',
            error_message = @error_message
        WHERE run_id = @run_id
        """
        
        error_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("error_message", "STRING", str(e)),
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
            ]
        )
        
        try:
            error_update_job = client.query(error_update_sql, job_config=error_config, location=BIGQUERY_LOCATION)
            error_update_job.result()
        except Exception as update_error:
            logger.error(f"Error updating kmeans_runs table: {update_error}")
        
        raise

def fetch_embeddings(client: bigquery.Client, unified_ids: list, bigquery_dataset_id: str) -> tuple[list[str], np.ndarray]:
    """
    Fetches embeddings for the given unified IDs from BigQuery.
    
    Args:
        client: BigQuery client
        unified_ids: List of content IDs to fetch embeddings for
        bigquery_dataset_id: The BigQuery dataset ID
        
    Returns:
        Tuple of (list of unified_ids that had valid embeddings, numpy array of embeddings)
    """
    query = f"""
    SELECT
        ec.unified_id,
        ec.embeddings
    FROM
        `{PROJECT_ID}.{bigquery_dataset_id}.embeddings_cache` AS ec
    WHERE
        ec.unified_id IN UNNEST(@unified_id_list)
        AND ec.embeddings IS NOT NULL 
        AND ARRAY_LENGTH(ec.embeddings) > 0
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("unified_id_list", "STRING", unified_ids)
        ]
    )
    
    try:
        query_job = client.query(query, job_config=job_config, location=BIGQUERY_LOCATION)
        results = query_job.result()
        
        # Convert results to lists
        valid_ids = []
        embeddings_list = []
        
        for row in results:
            valid_ids.append(row.unified_id)
            embeddings_list.append(row.embeddings)
        
        if not valid_ids:
            raise ValueError("No valid embeddings found for the provided IDs")
        
        # Convert to numpy array
        embeddings_array = np.array(embeddings_list)
        
        return valid_ids, embeddings_array
        
    except Exception as e:
        logger.error(f"Error fetching embeddings: {e}")
        raise

def store_umap_coordinates(client: bigquery.Client, run_id: str, unified_ids: list[str], 
                          coordinates: np.ndarray, bigquery_dataset_id: str) -> None:
    """
    Stores the UMAP coordinates in BigQuery.
    
    Args:
        client: BigQuery client
        run_id: The run ID for this operation
        unified_ids: List of content IDs
        coordinates: 2D numpy array of coordinates
        bigquery_dataset_id: The BigQuery dataset ID
    """
    # Prepare data for insertion
    rows_to_insert = []
    current_time = datetime.utcnow().isoformat()  # Convert to ISO format string
    
    for i, unified_id in enumerate(unified_ids):
        rows_to_insert.append({
            'run_id': run_id,
            'unified_id': unified_id,
            'umap_x': float(coordinates[i, 0]),
            'umap_y': float(coordinates[i, 1]),
            'created_at': current_time
        })
    
    # Insert into BigQuery
    table_id = f"{PROJECT_ID}.{bigquery_dataset_id}.document_umap_coordinates"
    
    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            raise Exception(f"Errors inserting rows: {errors}")
        logger.info(f"Successfully stored coordinates for {len(rows_to_insert)} documents")
        
    except Exception as e:
        logger.error(f"Error storing coordinates: {e}")
        raise

def perform_umap_reduction(embeddings: np.ndarray, n_neighbors: int = 15, 
                          min_dist: float = 0.1, metric: str = 'cosine', n_components: int = 2) -> np.ndarray:
    """
    Performs UMAP dimensionality reduction on the embeddings.
    
    Args:
        embeddings: Numpy array of embeddings
        n_neighbors: UMAP parameter for number of neighbors
        min_dist: UMAP parameter for minimum distance
        metric: Distance metric to use
        
    Returns:
        2D numpy array of coordinates
    """


    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=metric,
        n_components=n_components
    )
    
    return reducer.fit_transform(embeddings)

def get_top_documents_for_topics(client: bigquery.Client, run_id: str, bigquery_dataset_id: str, 
                               num_docs: int = 10, max_text_length: int = 500) -> dict:
    """
    Gets the top documents for each topic based on assignment score.
    
    Args:
        client: BigQuery client
        run_id: The run ID for this clustering operation
        bigquery_dataset_id: The BigQuery dataset ID
        num_docs: Number of top documents to get per topic
        max_text_length: Maximum length of text to include for each document
        
    Returns:
        Dictionary mapping topic IDs to lists of document texts
    """
    query = f"""
    WITH RankedDocs AS (
        SELECT
            topic_id,
            primary_text,
            assignment_score,
            ROW_NUMBER() OVER (PARTITION BY topic_id ORDER BY assignment_score ASC) as doc_rank
        FROM `{PROJECT_ID}.{bigquery_dataset_id}.document_topic_assignments`
        WHERE run_id = @run_id
    )
    SELECT
        topic_id,
        STRING_AGG(
            SUBSTR(primary_text, 1, @max_text_length),
            '\\n---\\n'
            ORDER BY doc_rank
            LIMIT @num_docs
        ) as topic_documents,
        AVG(assignment_score) as avg_assignment_score,
        COUNT(*) as num_documents
    FROM RankedDocs
    WHERE doc_rank <= @num_docs
    GROUP BY topic_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("num_docs", "INT64", num_docs),
            bigquery.ScalarQueryParameter("max_text_length", "INT64", max_text_length)
        ]
    )
    
    try:
        query_job = client.query(query, job_config=job_config, location=BIGQUERY_LOCATION)
        results = query_job.result()
        
        topic_docs = {}
        for row in results:
            topic_docs[row.topic_id] = {
                'documents': row.topic_documents,
                'avg_assignment_score': row.avg_assignment_score,
                'num_documents': row.num_documents
            }
        
        return topic_docs
        
    except Exception as e:
        logger.error(f"Error getting top documents: {e}")
        raise

def generate_topic_labels(client: bigquery.Client, run_id: str, topic_docs: dict, 
                         bigquery_dataset_id: str) -> None:
    """
    Generates topic labels using Gemini through BigQuery ML and stores them.
    
    Args:
        client: BigQuery client
        run_id: The run ID for this clustering operation
        topic_docs: Dictionary mapping topic IDs to document information
        bigquery_dataset_id: The BigQuery dataset ID
    """
    # Create a temporary table for the input data
    temp_table = f"temp_topic_docs_{run_id}"
    
    # Prepare rows for insertion
    rows_to_insert = []
    current_time = datetime.utcnow().isoformat()
    
    for topic_id, info in topic_docs.items():
        prompt = f"""Given the following documents that belong to the same topic cluster, 
        analyze them and provide a structured response that captures the main theme or subject.

        Documents:
        {info['documents']}

        Please provide your response in the following exact format:

        LABEL: [A concise label, maximum 5 words, that best describes the topic]
        DESCRIPTION: [A brief description, maximum 2 sentences, explaining the topic in more detail]
        CONFIDENCE: [A number between 0 and 1, indicating how well the documents fit this label]

        Example format:
        LABEL: Social Media Marketing Trends
        DESCRIPTION: Discussion of emerging social media marketing strategies and their impact on brand engagement. Focus on platform-specific tactics and ROI measurement.
        CONFIDENCE: 0.85

        Important:
        - Keep the label to maximum 5 words
        - Keep the description to maximum 2 sentences
        - Provide confidence as a number between 0 and 1
        - Use exactly the format shown above with LABEL:, DESCRIPTION:, and CONFIDENCE: prefixes
        """
        
        # Call Gemini through BigQuery ML
        gemini_sql = f"""
        SELECT
            ml_generate_text_result as result,
            ml_generate_text_status as status,
            prompt
        FROM ML.GENERATE_TEXT(
            MODEL `{PROJECT_ID}.{bigquery_dataset_id}.gemini_labeling_model`,
            (
                SELECT @prompt as prompt
            ),
            STRUCT(
                0.2 as temperature,
                1024 as max_output_tokens,
                0.95 as top_p,
                40 as top_k
            )
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("prompt", "STRING", prompt)
            ]
        )
        
        try:
            query_job = client.query(gemini_sql, job_config=job_config, location=BIGQUERY_LOCATION)
            result = next(query_job.result())
            
            # Parse the Gemini response JSON
            try:
                response_json = json.loads(result.result)
                
                # Check if we have valid candidates
                if not response_json.get('candidates'):
                    raise ValueError("No candidates in response")
                
                # Get the first candidate's content
                candidate = response_json['candidates'][0]
                if not candidate.get('content', {}).get('parts'):
                    raise ValueError("No content parts in candidate")
                
                # Get the text from the first part
                response_text = candidate['content']['parts'][0]['text'].strip()
                
                # Store the prompt for debugging
                prompt_used = result.prompt
                
                # Extract components using the explicit format
                try:
                    label_line = next(line for line in response_text.split('\n') if line.startswith('LABEL:'))
                    description_line = next(line for line in response_text.split('\n') if line.startswith('DESCRIPTION:'))
                    confidence_line = next(line for line in response_text.split('\n') if line.startswith('CONFIDENCE:'))
                    
                    # Extract the actual values
                    label = label_line.replace('LABEL:', '').strip()
                    description = description_line.replace('DESCRIPTION:', '').strip()
                    confidence = float(confidence_line.replace('CONFIDENCE:', '').strip())
                    
                    # Validate the extracted values
                    if not label:
                        raise ValueError("Label cannot be empty")
                    if len(label.split()) > 15:  # More lenient word count limit
                        raise ValueError("Label is too long (should be concise)")
                    if not description or len(description.split('.')) > 7:  # 2 sentences + potential trailing period
                        raise ValueError("Description must be 1-2 sentences")
                    if not 0 <= confidence <= 1:
                        raise ValueError("Confidence must be between 0 and 1")
                    
                    # Get additional metadata from the response
                    avg_logprobs = candidate.get('avg_logprobs', 0)
                    score = candidate.get('score', 0)
                    
                    # Log if label is longer than recommended but still accepted
                    if len(label.split()) > 5:
                        logger.info(f"Label for topic {topic_id} is longer than recommended 5 words: '{label}' ({len(label.split())} words)")
                    
                    rows_to_insert.append({
                        'run_id': run_id,
                        'topic_id': topic_id,
                        'created_at': current_time,
                        'topic_label': label,
                        'topic_description': description,
                        'confidence_score': confidence,
                        'num_documents_used': info['num_documents'],
                        'avg_assignment_score': info['avg_assignment_score'],
                        'model_metadata': json.dumps({
                            'avg_logprobs': avg_logprobs,
                            'score': score,
                            'finish_reason': candidate.get('finish_reason'),
                            'model_version': response_json.get('model_version'),
                            'response_id': response_json.get('response_id'),
                            'prompt_used': prompt_used,
                            'usage_metadata': response_json.get('usage_metadata', {})
                        })
                    })
                    
                except (ValueError, StopIteration) as e:
                    logger.error(f"Error parsing Gemini response text for topic {topic_id}: {e}")
                    logger.error(f"Raw response text: {response_text}")
                    logger.error(f"Prompt used: {prompt_used}")
                    # Insert a placeholder row with error information
                    rows_to_insert.append({
                        'run_id': run_id,
                        'topic_id': topic_id,
                        'created_at': current_time,
                        'topic_label': f"Error: Invalid response format",
                        'topic_description': f"Raw response: {response_text[:200]}...",
                        'confidence_score': 0.0,
                        'num_documents_used': info['num_documents'],
                        'avg_assignment_score': info['avg_assignment_score'],
                        'model_metadata': json.dumps({
                            'error': str(e),
                            'raw_response': response_text[:500],
                            'prompt_used': prompt_used
                        })
                    })
                
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing Gemini JSON response for topic {topic_id}: {e}")
                logger.error(f"Raw response: {result.result}")
                logger.error(f"Prompt used: {result.prompt}")
                # Insert a placeholder row with error information
                rows_to_insert.append({
                    'run_id': run_id,
                    'topic_id': topic_id,
                    'created_at': current_time,
                    'topic_label': f"Error: Invalid JSON response",
                    'topic_description': f"Raw response: {str(result.result)[:200]}...",
                    'confidence_score': 0.0,
                    'num_documents_used': info['num_documents'],
                    'avg_assignment_score': info['avg_assignment_score'],
                    'model_metadata': json.dumps({
                        'error': str(e),
                        'raw_response': str(result.result)[:500],
                        'prompt_used': result.prompt
                    })
                })
            
        except Exception as e:
            logger.error(f"Error generating label for topic {topic_id}: {e}")
            # Insert a placeholder row with error information
            rows_to_insert.append({
                'run_id': run_id,
                'topic_id': topic_id,
                'created_at': current_time,
                'topic_label': f"Error: {str(e)[:100]}",
                'topic_description': None,
                'confidence_score': 0.0,
                'num_documents_used': info['num_documents'],
                'avg_assignment_score': info['avg_assignment_score'],
                'model_metadata': json.dumps({
                    'error': str(e)
                })
            })
    
    # Insert all labels into the topic_labels table
    table_id = f"{PROJECT_ID}.{bigquery_dataset_id}.topic_labels"
    
    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            raise Exception(f"Errors inserting topic labels: {errors}")
        logger.info(f"Successfully stored labels for {len(rows_to_insert)} topics")
        
    except Exception as e:
        logger.error(f"Error storing topic labels: {e}")
        raise

@functions_framework.http
def perform_kmeans(request):
    """
    Cloud Function to perform K-means clustering, UMAP reduction, and topic labeling.
    
    Expected input format (POST request JSON body):
    {
        "ids": ["id1", "id2", "id3", ...],  # Required: List of content IDs to cluster
        "n_clusters": 5,                     # Optional: Number of clusters (default: 5, min: 2)
        "wait_for_completion": true,         # Optional: Whether to wait for completion (default: true)
        "skip_umap": false,                  # Optional: Whether to skip UMAP reduction (default: false)
        "skip_labeling": false,              # Optional: Whether to skip topic labeling (default: false)
        "umap_params": {                     # Optional: UMAP parameters
            "n_neighbors": 15,
            "min_dist": 0.1,
            "metric": "cosine",
            "n_components": 2
        },
        "labeling_params": {                 # Optional: Topic labeling parameters
            "num_docs_per_topic": 10,        # Number of top documents to use per topic
            "max_text_length": 500          # Maximum length of each document text
        },
        "description": "Optional description for the run"
    }
    """
    # Get environment variables
    bigquery_dataset_id = os.getenv('BIGQUERY_DATASET_ID')

    # Initialize clients
    client = bigquery.Client()

    # Get request parameters
    request_json = request.get_json(silent=True)
    if not request_json or 'ids' not in request_json:
        return jsonify({
            'status': 'error',
            'message': 'Request must include ids list. Example: {"ids": ["id_123", "id_456"]}'
        }), 400

    ids = request_json['ids']
    n_clusters = request_json.get('n_clusters', 5)
    wait_for_completion = request_json.get('wait_for_completion', True)
    skip_umap = request_json.get('skip_umap', False)
    skip_labeling = request_json.get('skip_labeling', False)
    umap_params = request_json.get('umap_params', {})
    labeling_params = request_json.get('labeling_params', {})
    description = request_json.get('description')

    # Validate inputs
    if not isinstance(ids, list):
        return jsonify({
            'status': 'error', 
            'message': 'ids must be a list of strings'
        }), 400

    if not isinstance(n_clusters, int) or n_clusters < 2:
        return jsonify({
            'status': 'error',
            'message': 'n_clusters must be an integer greater than 1'
        }), 400

    logger.info(f"Received {len(ids)} IDs for processing with {n_clusters} clusters")

    try:
        # Submit the K-means clustering job
        result = create_kmeans_model_job(ids, n_clusters, bigquery_dataset_id, description)
        run_id = result['run_id']
        model_creation_job_id = result['job_id']
        
        if wait_for_completion:
            # Wait for model creation job to complete
            model_creation_job = client.get_job(model_creation_job_id, location=BIGQUERY_LOCATION)
            model_creation_job.result()
            
            # Update status to model_created
            update_sql = f"""
            UPDATE `{PROJECT_ID}.social_listening_data.kmeans_runs`
            SET status = 'model_created'
            WHERE run_id = @run_id
            """
            
            update_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
                ]
            )
            
            update_job = client.query(update_sql, job_config=update_config, location=BIGQUERY_LOCATION)
            update_job.result()
            
            # Run prediction job
            predict_job_id = run_prediction_job(client, run_id, ids, bigquery_dataset_id)
            
            # Wait for prediction job to complete
            predict_job = client.get_job(predict_job_id, location=BIGQUERY_LOCATION)
            predict_job.result()
            
            # Update status to prediction completed
            update_sql = f"""
            UPDATE `{PROJECT_ID}.social_listening_data.kmeans_runs`
            SET status = 'prediction_completed'
            WHERE run_id = @run_id
            """
            
            update_job = client.query(update_sql, job_config=update_config, location=BIGQUERY_LOCATION)
            update_job.result()
            
            # Perform UMAP reduction if not skipped
            umap_response = None
            if not skip_umap:
                try:
                    # Fetch embeddings
                    valid_ids, embeddings = fetch_embeddings(client, ids, bigquery_dataset_id)
                    
                    if len(valid_ids) < len(ids):
                        logger.warning(f"Only found embeddings for {len(valid_ids)} out of {len(ids)} IDs")
                    
                    # Perform UMAP reduction
                    coordinates = perform_umap_reduction(
                        embeddings,
                        n_neighbors=umap_params.get('n_neighbors', 10),
                        min_dist=umap_params.get('min_dist', 0.0),
                        metric=umap_params.get('metric', 'cosine'),
                        n_components=umap_params.get('n_components', 2)
                    )
                    
                    # Store coordinates
                    store_umap_coordinates(client, run_id, valid_ids, coordinates, bigquery_dataset_id)
                    
                    umap_response = {
                        'status': 'success',
                        'message': 'UMAP reduction completed successfully',
                        'processed_ids': len(valid_ids)
                    }
                    
                except Exception as e:
                    logger.error(f"Error in UMAP reduction: {e}")
                    umap_response = {
                        'status': 'error',
                        'message': str(e)
                    }
            
            # Update final status
            final_update_sql = f"""
            UPDATE `{PROJECT_ID}.social_listening_data.kmeans_runs`
            SET status = 'completed'
            WHERE run_id = @run_id
            """
            
            final_update_job = client.query(final_update_sql, job_config=update_config, location=BIGQUERY_LOCATION)
            final_update_job.result()
            
            # After UMAP reduction, perform topic labeling if not skipped
            labeling_response = None
            
            if not skip_labeling and wait_for_completion:
                try:
                    # Get top documents for each topic
                    topic_docs = get_top_documents_for_topics(
                        client, 
                        run_id, 
                        bigquery_dataset_id,
                        num_docs=labeling_params.get('num_docs_per_topic', 10),
                        max_text_length=labeling_params.get('max_text_length', 500)
                    )
                    
                    # Generate and store topic labels
                    generate_topic_labels(client, run_id, topic_docs, bigquery_dataset_id)
                    
                    labeling_response = {
                        'status': 'success',
                        'message': 'Topic labeling completed successfully',
                        'num_topics_labeled': len(topic_docs)
                    }
                    
                except Exception as e:
                    logger.error(f"Error in topic labeling: {e}")
                    labeling_response = {
                        'status': 'error',
                        'message': str(e)
                    }
            
            # Update response data
            response_data = {
                'status': 'success',
                'message': 'K-means clustering and prediction completed successfully',
                'run_id': run_id,
                'model_creation_job_id': model_creation_job_id,
                'predict_job_id': predict_job_id,
                'predictions_table': f"{PROJECT_ID}.{bigquery_dataset_id}.document_topic_assignments",
                'input_summary': {
                    'num_ids': len(ids),
                    'n_clusters': n_clusters
                }
            }
            
            if not skip_umap:
                response_data['umap_reduction'] = umap_response
                if umap_response and umap_response.get('status') == 'success':
                    response_data['coordinates_table'] = f"{PROJECT_ID}.{bigquery_dataset_id}.document_umap_coordinates"
            
            if not skip_labeling:
                response_data['topic_labeling'] = labeling_response
                if labeling_response and labeling_response.get('status') == 'success':
                    response_data['labels_table'] = f"{PROJECT_ID}.{bigquery_dataset_id}.topic_labels"
            
            return jsonify(response_data), 200
        
        return jsonify({
            'status': 'success',
            'message': 'K-means clustering job submitted successfully',
            'run_id': run_id,
            'model_creation_job_id': model_creation_job_id,
            'input_summary': {
                'num_ids': len(ids),
                'n_clusters': n_clusters
            }
        }), 200

    except Exception as e:
        logger.error(f"Error in perform_kmeans: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error processing request: {str(e)}'
        }), 500

