import functions_framework
import base64
import json
import os
from google.cloud import bigquery
import datetime
from google.api_core.exceptions import GoogleAPIError # Import for BigQuery API errors

# Initialize BigQuery client outside the function for potential warm starts
# It's generally safe as long as the client is not tied to a specific request context.
try:
    bq_client = bigquery.Client()
except Exception as e:
    print(f"Error initializing BigQuery client globally: {e}")
    bq_client = None # Handle potential initialization failure

@functions_framework.cloud_event
def process_reddit_data_from_pubsub(cloud_event):
    """
    Cloud Function triggered by a Pub/Sub message containing scraped Reddit data.
    Decodes the message, inserts data into BigQuery reddit_data table,
    and updates the scrape_job table status.

    Args:
        cloud_event: The CloudEvent object containing the Pub/Sub message.
        Expected message.data: Base64 encoded JSON string of Reddit posts.
        Expected message.attributes: Contains 'job_id' (snapshot_id) and 'dataset_id'.
    """
    print("Received Pub/Sub message.")

    # Ensure BigQuery client is available
    global bq_client
    if bq_client is None:
         try:
            bq_client = bigquery.Client()
            print("BigQuery client initialized successfully within function.")
         except Exception as e:
            print(f"FATAL: Could not initialize BigQuery client: {e}")
            # Depending on error handling strategy, you might re-raise or return
            raise # Re-raise to indicate a critical failure

    # --- Extract data and attributes from Pub/Sub message ---
    try:
        pubsub_message_data = cloud_event.data["message"]["data"]
        pubsub_message_attributes = cloud_event.data["message"]["attributes"]

        # The job_id attribute from Bright Data corresponds to snapshot_id
        job_id = pubsub_message_attributes.get("job_id")
        message_dataset_id = pubsub_message_attributes.get("dataset_id")  # This is different from BigQuery dataset ID
        if not job_id:
            print("Warning: 'job_id' attribute not found in Pub/Sub message.")
            # We can still try to process the data, but won't be able to update scrape_job status
            # Decide on your error handling: skip processing, log and continue, etc.
            # For this example, we'll log and continue to process data if possible.

        print(f"Processing job_id (snapshot_id): {job_id}")
        print(f"Message dataset_id: {message_dataset_id}")
        print(f"Message attributes: {pubsub_message_attributes}")

    except KeyError as e:
        print(f"Error accessing message data or attributes: {e}")
        # This message is malformed, log and exit.
        return
    except Exception as e:
        print(f"An unexpected error occurred while accessing message components: {e}")
        return

    # --- Fetch BigQuery table IDs from environment variables ---
    bigquery_dataset_id = os.environ.get('BIGQUERY_DATASET_ID')  # Use BigQuery dataset ID from environment
    reddit_data_table_id = os.environ.get('REDDIT_DATA_TABLE_ID')
    quora_data_table_id = os.environ.get('QUORA_DATA_TABLE_ID')
    scrape_job_dataset_id = os.environ.get('BIGQUERY_DATASET_ID')  # Use BigQuery dataset ID from environment
    scrape_job_table_id = os.environ.get('SCRAPE_JOB_TABLE_ID')

    # Check if environment variables are set
    if not bigquery_dataset_id:
        print("Error: BIGQUERY_DATASET_ID environment variable not set.")
        # Log error and potentially return/exit as we can't proceed without table info
        # For CloudEvent triggers, returning doesn't send an HTTP response,
        # but exiting indicates processing failed for this message.
        return
    if not reddit_data_table_id:
        print("Error: REDDIT_DATA_TABLE_ID environment variable not set.")
        return
    if not quora_data_table_id:
        print("Error: QUORA_DATA_TABLE_ID environment variable not set.")
        return
    if not scrape_job_dataset_id:
        print("Error: BIGQUERY_DATASET_ID environment variable not set for scrape_job.")
        return
    if not scrape_job_table_id:
        print("Error: SCRAPE_JOB_TABLE_ID environment variable not set for scrape_job.")
        return

    # --- Decode and parse the message data ---
    try:
        decoded_data = base64.b64decode(pubsub_message_data)
        posts_data = json.loads(decoded_data)

        if not isinstance(posts_data, list):
            print("Error: Decoded data is not a list.")
            # Log and exit if the data format is unexpected
            return

        print(f"Successfully decoded and parsed {len(posts_data)} posts.")
        # print(f"First post data (sample): {posts_data[0] if posts_data else 'No posts'}") # Optional: print sample data

    except base64.Error as e:
        print(f"Error decoding base64 data: {e}")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON data: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred during data decoding/parsing: {e}")
        return

    # --- Prepare data for BigQuery insertion ---
    rows_to_insert = []
    
    if message_dataset_id == "gd_lvz8ah06191smkebj4":  # Reddit dataset
        for post in posts_data:
            # Helper function to convert timestamp strings to proper format
            def parse_timestamp(ts_str):
                if not ts_str:
                    return None
                try:
                    # Try parsing as ISO format first
                    dt = datetime.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    return dt.isoformat()
                except (ValueError, AttributeError):
                    return None

            # Helper function to convert to integer
            def to_int(val):
                if val is None or val == '':
                    return None
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return None

            row = {
                "post_id": post.get("post_id"),
                "url": post.get("url"),
                "user_posted": post.get("user_posted"),
                "title": post.get("title"),
                "description": post.get("description"),
                "num_comments": to_int(post.get("num_comments")),
                "date_posted": parse_timestamp(post.get("date_posted")),
                "community_name": post.get("community_name"),
                "num_upvotes": to_int(post.get("num_upvotes")),
                "photos": post.get("photos", []) if isinstance(post.get("photos"), list) else [],
                "videos": post.get("videos", []) if isinstance(post.get("videos"), list) else [],
                "tag": post.get("tag"),
                "related_posts": [
                    {
                        "num_comments": to_int(rp.get("num_comments")),
                        "num_upvotes": to_int(rp.get("num_upvotes")),
                        "thumbnail": rp.get("thumbnail"),
                        "url": rp.get("url"),
                        "title": rp.get("title"),
                        "community_url": rp.get("community_url"),
                        "community": rp.get("community")
                    }
                    for rp in (post.get("related_posts") or [])
                ],
                "comments": [
                    {
                        "replies": [
                            {
                                "num_replies": to_int(reply.get("num_replies")),
                                "num_upvotes": to_int(reply.get("num_upvotes")),
                                "date_of_reply": parse_timestamp(reply.get("date_of_reply")),
                                "user_url": reply.get("user_url"),
                                "reply": reply.get("reply"),
                                "user_replying": reply.get("user_replying")
                            }
                            for reply in (comment.get("replies") or [])
                        ],
                        "num_replies": to_int(comment.get("num_replies")),
                        "user_commenting": comment.get("user_commenting"),
                        "num_upvotes": to_int(comment.get("num_upvotes")),
                        "date_of_comment": parse_timestamp(comment.get("date_of_comment")),
                        "url": comment.get("url"),
                        "user_url": comment.get("user_url"),
                        "comment": comment.get("comment")
                    }
                    for comment in (post.get("comments") or [])
                ],
                "community_url": post.get("community_url"),
                "community_description": post.get("community_description"),
                "community_members_num": to_int(post.get("community_members_num")),
                "community_rank": {
                    "community_rank_value": post.get("community_rank", {}).get("community_rank_value"),
                    "community_rank_type": post.get("community_rank", {}).get("community_rank_type")
                } if post.get("community_rank") else None,
                "post_karma": to_int(post.get("post_karma")),
                "bio_description": post.get("bio_description"),
                "embedded_links": post.get("embedded_links", []) if isinstance(post.get("embedded_links"), list) else [],
                "timestamp": parse_timestamp(post.get("timestamp")),
                "input": {
                    "url": post.get("input", {}).get("url")
                } if post.get("input") else None,
                "error_code": post.get("error_code"),
                "error": post.get("error"),
                "warning_code": post.get("warning_code"),
                "warning": post.get("warning"),
                "snapshot_id": job_id  # This is REQUIRED in the schema
            }
            rows_to_insert.append(row)
    elif message_dataset_id == "gd_lvz1rbj81afv3m6n5y":  # Quora dataset
        for post in posts_data:
            row = {
                "timestamp": post.get("timestamp"),
                "author_education": post.get("author_education"),
                "post_id": post.get("post_id"),
                "top_comments": post.get("top_comments"),
                "views": post.get("views"),
                "shares": post.get("shares"),
                "author_content_views": post.get("author_content_views"),
                "post_date": post.get("post_date"),
                "upvotes": post.get("upvotes"),
                "extarnal_urls": post.get("extarnal_urls"),
                "pictures_urls": post.get("pictures_urls"),
                "header": post.get("header"),
                "author_joined_date": post.get("author_joined_date"),
                "input": post.get("input"),
                "post_text": post.get("post_text"),
                "videos_urls": post.get("videos_urls"),
                "over_all_answers": post.get("over_all_answers"),
                "originally_answered": post.get("originally_answered"),
                "author_name": post.get("author_name"),
                "author_about": post.get("author_about"),
                "error": post.get("error"),
                "url": post.get("url"),
                "error_code": post.get("error_code"),
                "author_active_spaces": post.get("author_active_spaces"),
                "title": post.get("title"),
                "snapshot_id": job_id
            }
            rows_to_insert.append(row)
    else:
        print(f"Error: Unknown dataset_id: {message_dataset_id}")
        return

    # --- Insert data into appropriate BigQuery table based on dataset_id ---
    target_table_id = None
    if message_dataset_id == "gd_lvz8ah06191smkebj4":  # Reddit dataset
        target_table_id = reddit_data_table_id
        print("Routing data to Reddit table")
    elif message_dataset_id == "gd_lvz1rbj81afv3m6n5y":  # Quora dataset
        target_table_id = quora_data_table_id
        print("Routing data to Quora table")
    else:
        print(f"Error: Unknown dataset_id: {message_dataset_id}")
        return

    target_table_ref = bq_client.dataset(bigquery_dataset_id).table(target_table_id)
    target_table = None
    try:
        target_table = bq_client.get_table(target_table_ref) # Get table schema
        print(f"Inserting {len(rows_to_insert)} rows into {bigquery_dataset_id}.{target_table_id}")
        errors = bq_client.insert_rows_json(target_table, rows_to_insert)

        if errors:
            print(f"BigQuery insertion into {target_table_id} had errors: {errors}")
            insertion_status = "completed_with_failures"
            # Log specific errors if needed
        else:
            print(f"BigQuery insertion into {target_table_id} successful.")
            insertion_status = "completed_success"

    except GoogleAPIError as e:
        print(f"BigQuery API error during insertion into {target_table_id}: {e}")
        insertion_status = "completed_with_failures" # Or a specific error status
        # Log the error details
    except Exception as e:
        print(f"An unexpected error occurred during BigQuery insertion into {target_table_id}: {e}")
        insertion_status = "completed_with_failures" # Or a specific error status
        # Log the error details

    # After successful insertion, run the embeddings_cache MERGE job for both Reddit and Quora
    if insertion_status == "completed_success":
        print("Running embeddings_cache MERGE job...")
        merge_sql = """
        MERGE INTO `social-listening-sense.social_listening_data.embeddings_cache` AS T
        USING (
          WITH SourceData AS (
            SELECT v.content_item_id, v.primary_text AS content
            FROM `social-listening-sense.social_listening_data.unified_social_content_items` AS v
            LEFT JOIN `social-listening-sense.social_listening_data.embeddings_cache` AS ec
            ON v.content_item_id = ec.unified_id
            WHERE
              v.record_load_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
              AND (ec.unified_id IS NULL OR ec.sentiment_score IS NULL OR ec.embeddings IS NULL OR ARRAY_LENGTH(ec.embeddings) = 0)
              AND v.primary_text IS NOT NULL AND LENGTH(TRIM(v.primary_text)) > 0
          ),
          EmbeddingResults AS (
            SELECT generated.content_item_id, generated.ml_generate_embedding_result AS embeddings_array, generated.ml_generate_embedding_status AS embedding_status
            FROM ML.GENERATE_EMBEDDING(
              MODEL `social-listening-sense.social_listening_data.social_media_embedding_model`,
              (SELECT content_item_id, content FROM SourceData),
              STRUCT(TRUE AS flatten_json_output, 'CLUSTERING' as task_type)
            ) AS generated
          ),
          SentimentResults AS (
            SELECT
              understand_results.content_item_id,
              CAST(JSON_VALUE(understand_results.ml_understand_text_result, '$.document_sentiment.score') AS FLOAT64) AS sentiment_score,
              CAST(JSON_VALUE(understand_results.ml_understand_text_result, '$.document_sentiment.magnitude') AS FLOAT64) AS sentiment_magnitude,
              understand_results.ml_understand_text_status AS sentiment_status
            FROM ML.UNDERSTAND_TEXT(
              MODEL `social-listening-sense.social_listening_data.sentiment_analysis_model`,
              (SELECT content_item_id, content AS text_content FROM SourceData),
              STRUCT('analyze_sentiment' AS nlu_option)
            ) AS understand_results
          )
          SELECT
            COALESCE(er.content_item_id, sr.content_item_id) AS unified_id,
            er.embeddings_array AS embeddings,
            CURRENT_TIMESTAMP() AS embedding_generated_at,
            'text-embedding-004' AS embedding_model_name,
            'CLUSTERING' AS embedding_task_type,
            sr.sentiment_score,
            sr.sentiment_magnitude,
            er.embedding_status,
            sr.sentiment_status
          FROM EmbeddingResults AS er
          FULL OUTER JOIN SentimentResults AS sr
          ON er.content_item_id = sr.content_item_id
          WHERE
            (LENGTH(COALESCE(er.embedding_status, '')) = 0 OR er.content_item_id IS NULL)
            AND (LENGTH(COALESCE(sr.sentiment_status, '')) = 0 OR sr.content_item_id IS NULL)
            AND (er.content_item_id IS NOT NULL OR sr.content_item_id IS NOT NULL)
        ) AS S
        ON T.unified_id = S.unified_id
        WHEN NOT MATCHED THEN
          INSERT (unified_id, embeddings, embedding_model_name, embedding_task_type, embedding_generated_at, sentiment_score, sentiment_magnitude)
          VALUES (S.unified_id, S.embeddings, S.embedding_model_name, S.embedding_task_type, S.embedding_generated_at, S.sentiment_score, S.sentiment_magnitude)
        WHEN MATCHED THEN
          UPDATE SET
            T.embeddings = COALESCE(T.embeddings, S.embeddings),
            T.embedding_model_name = COALESCE(T.embedding_model_name, S.embedding_model_name),
            T.embedding_task_type = COALESCE(T.embedding_task_type, S.embedding_task_type),
            T.embedding_generated_at = COALESCE(T.embedding_generated_at, S.embedding_generated_at),
            T.sentiment_score = COALESCE(T.sentiment_score, S.sentiment_score),
            T.sentiment_magnitude = COALESCE(T.sentiment_magnitude, S.sentiment_magnitude)
        """
        try:
            merge_job = bq_client.query(merge_sql)
            merge_job.result()  # Wait for job to complete
            print("MERGE job for embeddings_cache completed successfully.")
        except Exception as e:
            print(f"Error running embeddings_cache MERGE job: {e}")

    print("Function execution finished.")