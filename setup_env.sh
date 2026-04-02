#!/bin/bash

# ==============================================================================
# KESTRA FULL ENVIRONMENT SETUP
# ==============================================================================

# --- CONFIGURATION (Peers edit these values) ---
AUTH="admin@123.com:Admin123"
NAMESPACE="crypto"
KESTRA_URL="http://127.0.0.1:8090"

# GCP Settings
GCP_PROJECT_ID="crypto-pipeline-491522"
GCP_LOCATION="EU"
BQ_DATASET="crypto_dataset"
GCP_BUCKET="crypto-pipeline-491522-bucket"
FILE_PATH="/workspaces/test-pipeline/credentials.json"

# --- HELPER FUNCTION ---
# Sends a KV pair to Kestra using the /main/ path and text/plain
set_kv() {
    local key=$1
    local value=$2
    
    # Wrap value in quotes if it's not already (Kestra KV expects JSON-ish strings)
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
    fi
}

echo "Starting Full Setup for Namespace: $NAMESPACE"

# 1. Upload the Project ID, Location, and Bucket
set_kv "GCP_PROJECT_ID" "$GCP_PROJECT_ID"
set_kv "GCP_LOCATION" "$GCP_LOCATION"
set_kv "GCP_BUCKET" "$GCP_BUCKET"
set_kv "BQ_DATASET" "$BQ_DATASET"

# 2. Upload the JSON Credentials File
if [ -f "$FILE_PATH" ]; then
    # We use jq to escape the whole file into a single string
    PAYLOAD=$(cat "$FILE_PATH" | jq -Rs .)
    set_kv "GCP_CREDS" "$PAYLOAD"
else
    echo "Warning: $FILE_PATH not found. GCP_CREDS not updated."
fi

echo "Setup Complete. Check the KV Store in Kestra UI."
# # --- SYSTEM LOGIC ---
# echo "Setting up Kestra KV Store for namespace: $NAMESPACE..."

# # Set KV Pairs
# curl -s -X PUT "$KESTRA_HOST/api/v1/namespaces/$NAMESPACE/kv/GCP_PROJECT_ID" \
#      -H "Content-Type: application/json" -d "\"$GCP_PROJECT_ID\""

# curl -s -X PUT "$KESTRA_HOST/api/v1/namespaces/$NAMESPACE/kv/GCP_LOCATION" \
#      -H "Content-Type: text/plain" -d "\"$GCP_LOCATION\""

# curl -s -X PUT "$KESTRA_HOST/api/v1/namespaces/$NAMESPACE/kv/GCP_DATASET" \
#      -H "Content-Type: text/plain" -d "\"$GCP_DATASET\""

# curl -X PUT "$KESTRA_URL/api/v1/namespaces/$NAMESPACE/kv/GCP_CREDS" \
#      -H "Content-Type: application/json" \
#      --data-binary "@/workspaces/test-pipeline/credentials.json"

# echo "Environment setup complete. You can now run the Kestra Flow."