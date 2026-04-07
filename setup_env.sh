#!/bin/bash

# --- CONFIGURATION ---
AUTH="admin@123.com:Admin123"
NAMESPACE="crypto"
KESTRA_URL="http://127.0.0.1:8090"

# GCP Settings
GCP_PROJECT_ID="your-google-cloud-project-id"
GCP_LOCATION="your-project-location"
BQ_DATASET="your-bigquery-dataset-name"
GCS_BUCKET="your-bucket-name"

# Folders
FLOWS_DIR="./flows"                    # Folder containing your .yaml flows
FILE_PATH="./credentials.json"          # Your GCP JSON Key

# --- FUNCTION: UPLOAD FLOWS FROM FOLDER ---
upload_all_flows() {
    if [ ! -d "$1" ]; then
        echo "Warning: Folder $1 not found. Skipping flow uploads."
        return
    fi

    echo "Scanning $1 for Kestra flows..."
    for flow_file in "$1"/*.yaml "$1"/*.yml; do
        [ -e "$flow_file" ] || continue
        
        echo "Uploading $(basename "$flow_file")..."
        
        # FIXED: Changed -F "file=@..." to -F "fileUpload=@..."
        status=$(curl -s -o /dev/null -w "%{http_code}" \
             -u "$AUTH" \
             -X POST "$KESTRA_URL/api/v1/main/flows/import" \
             -F "fileUpload=@$flow_file")

        if [ "$status" -eq 200 ] || [ "$status" -eq 204 ]; then
            echo "Success!"
        else
            echo "Failed. Status: $status"
            if [ "$status" -eq 401 ]; then
                echo "ERROR: Authentication failed."
                exit 1
            fi
        fi
    done
}

# --- FUNCTION: SET KV PAIR ---
set_kv() {
    local key=$1
    local value=$2
    [[ $value != \"* ]] && value="\"$value\""

    status=$(curl -s -o /dev/null -w "%{http_code}" \
         -u "$AUTH" \
         -X PUT "$KESTRA_URL/api/v1/main/namespaces/$NAMESPACE/kv/$key" \
         -H "Content-Type: text/plain" \
         -d "$value")
    
    if [ "$status" -eq 204 ] || [ "$status" -eq 200 ]; then
        echo "Key '$key' set successfully."
    else
        echo "Failed to set '$key'. Status: $status"
        if [ "$status" -eq 401 ]; then
            echo "AUTH ERROR: Check your 'AUTH' variable in the script."
            exit 1
        fi
    fi
}

# --- START EXECUTION ---
echo "Starting Full Automation for Namespace: $NAMESPACE"

# Step 1: Upload ALL flows in the folder
upload_all_flows "$FLOWS_DIR"

# Step 2: Set Environment Variables
echo "⚙️  Setting up Environment Variables..."
set_kv "GCP_PROJECT_ID" "$GCP_PROJECT_ID"
set_kv "GCP_LOCATION"   "$GCP_LOCATION"
set_kv "BQ_DATASET"     "$BQ_DATASET"
set_kv "GCS_BUCKET"     "$GCS_BUCKET"

# Step 3: Upload GCP Credentials
echo "Uploading GCP Credentials..."
if [ -f "$FILE_PATH" ]; then
    PAYLOAD=$(cat "$FILE_PATH" | jq -Rs .)
    set_kv "GCP_CREDS" "$PAYLOAD"
else
    echo "Warning: $FILE_PATH not found."
fi

# --- Step 4: Trigger the Infrastructure Setup Flow ---
echo "Waiting for Kestra to index flows..."
sleep 2 # Give Kestra a moment to process the import

echo "Triggering initial infrastructure setup..."
response=$(curl -s -w "%{http_code}" -u "$AUTH" \
     -X POST "$KESTRA_URL/api/v1/main/executions/$NAMESPACE/setup_bigquery")

# Extract the status code (last 3 chars)
status=${response: -3}

if [ "$status" -eq 200 ] || [ "$status" -eq 201 ]; then
    echo "Success: Execution started!"
else
    echo "Failed to start execution. Status: $status"
    echo "Response: ${response::-3}"
fi
