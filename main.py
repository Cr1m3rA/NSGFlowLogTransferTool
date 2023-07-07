import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from config import connection_string, container_name, customer_id, shared_key, log_type
from extract_fields import extract_fields
from upload_log import post_data

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get container
container_client = blob_service_client.get_container_client(container_name)

# List all blob
blobs = container_client.list_blobs(name_starts_with='')

current_year = current_time.year
current_month = current_time.month
current_day = current_time.day
current_hour = current_time.hour

# Filiter blob
json_blobs = [blob for blob in blobs if blob.name.endswith('.json') and f"y={current_year}/m={current_month}/d={current_day}/h={current_hour-1}/m=00" in blob.name]

# Sort
sorted_json_blobs = sorted(json_blobs, key=lambda blob: blob.name)

# Process log file
for blob in sorted_json_blobs:
    blob_client = container_client.get_blob_client(blob.name)

    # Download and read file content
    downloaded_blob = blob_client.download_blob()
    nsg_log_content = downloaded_blob.readall()

    # Parse JSON content
    nsg_log = json.loads(nsg_log_content)

    # extrace and generate new JSON
    file_path = blob.name
    extracted_logs = extract_fields(nsg_log, file_path)

    # Print new log
    print(json.dumps(extracted_logs, indent=4))
    # Upload to LAW
    post_data(customer_id, shared_key, json.dumps(extracted_logs, indent=4), log_type)
