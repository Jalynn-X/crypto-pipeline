"""
One-time setup script for BigQuery dataset and external table.
Run this once before running dbt for the first time.
Usage: python setup/bigquery_setup.py
"""
import os
from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
DATASET = "crypto"
LOCATION = "US"

client = bigquery.Client(project=PROJECT_ID)

# ── 1. Create dataset ─────────────────────────────────────────────────────────
print(f"Creating dataset {DATASET}...")
dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET}")
dataset.location = LOCATION
client.create_dataset(dataset, exists_ok=True)
print(f"✅ Dataset {DATASET} ready")

# ── 2. Create external table pointing at GCS bronze layer ────────────────────
print("Creating external table bronze_raw...")
table_id = f"{PROJECT_ID}.{DATASET}.bronze_raw"

external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
external_config.source_uris = [
    f"gs://{GCS_BUCKET}/bronze/crypto/*.jsonl"
]
external_config.autodetect = False

schema = [
    bigquery.SchemaField("pair", "STRING"),
    bigquery.SchemaField("data", "JSON"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
]

table = bigquery.Table(table_id, schema=schema)
table.external_data_configuration = external_config
client.create_table(table, exists_ok=True)
print(f"✅ External table bronze_raw ready")
print(f"   Reads from: gs://{GCS_BUCKET}/bronze/crypto/*.jsonl")

print("\n✅ BigQuery setup complete! You can now run: dbt run")