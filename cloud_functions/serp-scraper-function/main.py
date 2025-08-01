import uuid
import functions_framework
import os
from flask import jsonify
import requests
from urllib.parse import quote_plus
from google.cloud import bigquery
from datetime import datetime
import json

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    bright_data_api_key = os.environ.get('BRIGHT_DATA_API_KEY')
    if not bright_data_api_key:
        return jsonify({
            'error': 'BRIGHT_DATA_API_KEY environment variable not set',
            'status': 'error'
        }), 500

    # Get search query from request args, default to 'pizza' if not provided
    search_query = request_args.get('q', 'pizza')
    start_page = request_args.get('start', '0')
    # URL encode the search query
    encoded_query = quote_plus(search_query)

    url = "https://api.brightdata.com/request"

    payload = {
        "zone": "social_listening_serp_api",
        "url": f"https://www.google.com/search?q={encoded_query}&brd_json=1&start={start_page}",
        "format": "json",
        "method": "GET",
    }
    headers = {
        "Authorization": f"Bearer {bright_data_api_key}",
        "Content-Type": "application/json"
    }

    response = requests.request("POST", url, json=payload, headers=headers)
    serp_data = response.json()
    
    # Extract the actual SERP data from the body and parse it as JSON
    serp_data = json.loads(serp_data.get('body', '{}'))
    
    # Log the entire serp_data response
    print("SERP API Response:", serp_data)

    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Get dataset and table IDs from environment variables
    dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
    serp_search_table_id = os.environ.get('SERP_SEARCH_TABLE_ID')
    serp_results_table_id = os.environ.get('SERP_RESULTS_TABLE_ID')
    
    if not dataset_id or not serp_search_table_id or not serp_results_table_id:
        return jsonify({
            'error': 'BIGQUERY_DATASET_ID, SERP_SEARCH_TABLE_ID, or SERP_RESULTS_TABLE_ID environment variables not set',
            'status': 'error'
        }), 500
        
    serp_search_table_ref = f"{client.project}.{dataset_id}.{serp_search_table_id}"
    serp_results_table_ref = f"{client.project}.{dataset_id}.{serp_results_table_id}"

    serp_search_request_id = serp_data['input']['request_id'] 
    serp_search_search_engine = serp_data['general']['search_engine']
    serp_search_results_cnt = serp_data['general']['results_cnt']
    serp_search_search_time = serp_data['general']['search_time']
    serp_search_language = serp_data['general']['language']
    serp_search_mobile = serp_data['general']['mobile']
    serp_search_timestamp = serp_data['general']['timestamp']
    
    # Insert search metadata into search table
    search_row = {
        'request_id': serp_search_request_id,
        'search_query': search_query,
        'search_engine': serp_search_search_engine,
        'results_count': serp_search_results_cnt,
        'search_time': serp_search_search_time,
        'language': serp_search_language,
        'is_mobile': str(serp_search_mobile),
        'timestamp': serp_search_timestamp,
        'pagination_start': start_page
    }

    errors = client.insert_rows_json(serp_search_table_ref, [search_row])
    if errors:
        return jsonify({
            'error': f'Failed to insert search metadata into BigQuery: {errors}',
            'status': 'error'
        }), 500

    # Extract results for processing
    # Prepare rows for BigQuery

    rows_to_insert = []
    # Process organic results
    
    if 'organic' in serp_data:
        for result in serp_data['organic']:

            # Generate row data matching the schema
            row = {
                'id': str(uuid.uuid4()),
                'query': search_query,
                'serp_request_id': serp_search_request_id,
                'link': result.get('link'),
                'title': result.get('title'),
                'description': result.get('description',''),
                'rank': result.get('rank'),
                'global_rank': result.get('global_rank'),
                'created_at': serp_search_timestamp
            }
            rows_to_insert.append(row)

    # Upload to BigQuery
    if rows_to_insert:
        errors = client.insert_rows_json(serp_results_table_ref, rows_to_insert)
        if errors:
            return jsonify({
                'error': f'Failed to insert rows into BigQuery: {errors}',
                'status': 'error'
            }), 500

    return jsonify({
        'message': 'Successfully scraped and uploaded to BigQuery',
        'query': search_query,
        'rows_inserted': len(rows_to_insert)
    })
