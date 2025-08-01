import functions_framework
from flask import request, jsonify
import requests
import os
from google.cloud import bigquery # Import the BigQuery library
import datetime # Import datetime to get the current timestamp

# The base URL for the Bright Data API trigger
BRIGHT_DATA_BASE_URL = "https://api.brightdata.com/datasets/v3/trigger"

# Initialize BigQuery client (can be done outside the function for potential warm starts)
# However, initializing inside ensures it's ready when the function is invoked.
# For simplicity and clarity in this example, we'll initialize inside.

@functions_framework.http
def process_urls(request):
    """
    Cloud Function to process a list of URLs provided in the request body,
    trigger a Bright Data dataset collection, and record the job in BigQuery.

    Fetches API key and GCP credentials from environment variables.
    Receives the Bright Data dataset_id as a query parameter.

    Args:
        request (flask.Request): The request object.
        Expected query parameter: dataset_id=<your_dataset_id>
        Expected JSON body: {"urls": ["url1", "url2", ...]}

    Returns:
        A Flask response object indicating the status of the Bright Data API call
        and BigQuery insertion, or an error if environment variables are not set,
        dataset_id is missing, or the request is invalid.
    """
    # Set CORS headers for preflight requests (OPTIONS)
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    # --- Fetch credentials and BigQuery info from environment variables ---
    bright_data_api_key = os.environ.get('BRIGHT_DATA_API_KEY')
    gcp_client_email = os.environ.get('GCP_CLIENT_EMAIL')
    gcp_private_key = os.environ.get('GCP_PRIVATE_KEY')
    bigquery_dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
    scrape_job_table_id = os.environ.get('SCRAPE_JOB_TABLE_ID')

    # Check if environment variables are set
    if not bright_data_api_key:
        print("Error: BRIGHT_DATA_API_KEY environment variable not set.")
        return jsonify({
            'status': 'error',
            'message': 'BRIGHT_DATA_API_KEY environment variable not set'
        }), 500, headers

    if not gcp_client_email:
        print("Error: GCP_CLIENT_EMAIL environment variable not set.")
        return jsonify({
            'status': 'error',
            'message': 'GCP_CLIENT_EMAIL environment variable not set'
        }), 500, headers

    if not gcp_private_key:
        print("Error: GCP_PRIVATE_KEY environment variable not set.")
        return jsonify({
            'status': 'error',
            'message': 'GCP_PRIVATE_KEY environment variable not set'
        }), 500, headers

    if not bigquery_dataset_id:
        print("Error: BIGQUERY_DATASET_ID environment variable not set.")
        return jsonify({
            'status': 'error',
            'message': 'BIGQUERY_DATASET_ID environment variable not set'
        }), 500, headers

    if not scrape_job_table_id:
        print("Error: SCRAPE_JOB_TABLE_ID environment variable not set.")
        return jsonify({
            'status': 'error',
            'message': 'SCRAPE_JOB_TABLE_ID environment variable not set'
        }), 500, headers

    # --- Get dataset_id from query parameters ---
    dataset_id_from_param = request.args.get('dataset_id') # Renamed to avoid conflict with BQ dataset_id

    if not dataset_id_from_param:
        print("Error: 'dataset_id' query parameter is missing.")
        return jsonify({
            'status': 'error',
            'message': "'dataset_id' query parameter is required"
        }), 400, headers # Bad Request if dataset_id is missing

    # Construct the full Bright Data API URL with the dynamic dataset_id
    bright_data_api_url = f"{BRIGHT_DATA_BASE_URL}?dataset_id={dataset_id_from_param}&include_errors=true"
    print(f"Using Bright Data API URL: {bright_data_api_url}")


    if request.method == 'POST':
        try:
            # Get the JSON data from the request body
            request_json = request.get_json(silent=True)

            if request_json and 'urls' in request_json:
                urls_list = request_json['urls']

                # Check if 'urls' is actually a list
                if isinstance(urls_list, list):
                    print(f"Received a list of {len(urls_list)} URLs.")

                    # Prepare the input list for the Bright Data API
                    bright_data_input = [{"url": url} for url in urls_list]

                    # Construct the full payload for the Bright Data API
                    bright_data_payload = {
                        "deliver": {
                            "type": "gcs",
                            "filename": {"template": "{[snapshot_id]}", "extension": "json"},
                            "bucket":"brightdata-social-raw", # hardcoded bucket name
                            "credentials": {
                                # Use credentials from environment variables
                                "client_email": gcp_client_email,
                                "private_key": gcp_private_key
                            },
                            "directory":dataset_id_from_param
                        },
                        "input": bright_data_input # Use the list of URLs from the request
                    }

                    # --- Call the Bright Data API ---
                    bright_data_headers = {
                        "Authorization": f"Bearer {bright_data_api_key}", # Use API key from env var
                        "Content-Type": "application/json"
                    }

                    bright_data_response = None # Initialize response variable
                    bigquery_insert_status = "not attempted"
                    bigquery_insert_errors = None

                    print(f"Calling Bright Data API: {bright_data_api_url}")
                    try:
                        response = requests.post(
                            bright_data_api_url, # Use the dynamically constructed URL
                            json=bright_data_payload,
                            headers=bright_data_headers
                        )
                        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

                        # Bright Data API call was successful
                        bright_data_response = response.json()
                        print("Bright Data API call successful.")
                        print(f"Response: {bright_data_response}")

                        # --- Extract snapshot_id and insert into BigQuery ---
                        snapshot_id = bright_data_response.get("snapshot_id")

                        if snapshot_id:
                            print(f"Extracted snapshot_id: {snapshot_id}")

                            # Initialize BigQuery client
                            client = bigquery.Client()

                            # Construct the table reference
                            table_ref = client.dataset(bigquery_dataset_id).table(scrape_job_table_id)
                            table = client.get_table(table_ref) # Get table schema

                            # Prepare the row to insert
                            row_to_insert = {
                                "snapshot_id": snapshot_id,
                                "dataset_id": dataset_id_from_param, # Use the dataset_id from the query parameter
                                "urls_in_batch": urls_list, # The list of URLs from the request body
                                "total_urls_count": len(urls_list),
                                "initiated_at": datetime.datetime.now(datetime.timezone.utc).isoformat() # Current UTC timestamp
                            }

                            print(f"Inserting row into BigQuery table: {bigquery_dataset_id}.{scrape_job_table_id}")
                            print(f"Row data: {row_to_insert}")

                            try:
                                errors = client.insert_rows_json(table, [row_to_insert])

                                if errors:
                                    bigquery_insert_status = "failed"
                                    bigquery_insert_errors = errors
                                    print(f"BigQuery insertion errors: {errors}")
                                else:
                                    bigquery_insert_status = "success"
                                    print("BigQuery insertion successful.")

                            except Exception as bq_error:
                                bigquery_insert_status = "failed"
                                bigquery_insert_errors = str(bq_error)
                                print(f"An error occurred during BigQuery insertion: {bq_error}")

                        else:
                            print("Warning: 'snapshot_id' not found in Bright Data API response.")
                            # Decide how to handle this case - maybe still log to BQ with a different status?
                            # For now, we'll just note it in the response.


                        # Return a success response with API response and BigQuery status
                        return jsonify({
                            "status": "success",
                            "message": "Bright Data API triggered and BigQuery insertion attempted",
                            "bright_data_response": bright_data_response,
                            "bigquery_insert_status": bigquery_insert_status,
                            "bigquery_insert_errors": bigquery_insert_errors
                        }), 200, headers

                    except requests.exceptions.RequestException as e:
                        print(f"Error calling Bright Data API: {e}")
                        # Return an error response if the API call fails
                        return jsonify({
                            "status": "error",
                            "message": f"Failed to trigger Bright Data API: {e}",
                            "bright_data_error": str(e)
                        }), 500, headers

                else:
                    return jsonify({
                        "status": "error",
                        "message": "'urls' field must be a list"
                    }), 400, headers
            else:
                return jsonify({
                    "status": "error",
                    "message": "Invalid JSON payload. Expected {'urls': [...]}"
                }), 400, headers

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return jsonify({
                "status": "error",
                "message": f"An unexpected error occurred: {e}"
            }), 500, headers
    else:
        return jsonify({
            "status": "error",
            "message": "Only POST method is accepted"
        }), 405, headers
