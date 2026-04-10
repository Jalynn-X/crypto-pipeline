# Crypto Pipeline

## Overview

Cryptocurrency markets operate 24/7 with volatility. For many investors, monitoring price action across multiple assets and determining if a price is "fair" or "expensive" relative to trading volume is a constant challenge. This pipeline automates that process by answering three core questions.

### Analytical Questions and Solutions

1. **Price Volatility**: Can price spikes or drops be detected with alerts being triggered?
    - The pipeline monitors and detects price Spikes or Drops (±1%) within a 30-minute rolling window. This filters out "noise" and identifies meaningful momentum shifts. The thresholds of Spike and Drop percentages can also be changed as needed.
2. **Relative Performance**: Do Bitcoin and Ethereum exhibit similar price trends?
   - Since BTC and ETH have vast price differences, the pipeline normalizes both prices to a base-100 index. This allows for a direct comparison to visualize whether they move together or diverge.
3. **Value Assessment**: Is the current price "too expensive" based on today's trading volume?
   - The pipeline calculates the Volume Weighted Average Price (VWAP). By analyzing the deviation between the current price and the VWAP, it identifies potential overbought (premium) or oversold (discount) conditions, providing a more sophisticated "fair value" metric than a simple moving average.

---

## Architecture

![Pipeline Architecture](https://github.com/Jalynn-X/crypto-pipeline/blob/main/images/Pipeline%20Architecture.PNG)

> Note: Kestra also handles the initial infrastructure setup, ensuring the BigQuery dataset and External tables exist before the first dbt run.
---

## Tech Stack

| Component | Technology |
|---|---|
| Cloud and Infrastructure | Google Cloud Platform + Terraform |
| Containerization | Docker + Docker Compose |
| Data ingestion (Stream Processing) | Python Kafka producer (Kafka client for Redpanda), Redpanda (Kafka-compatible), Flink |
| Data Lake | Google Cloud Storage |
| Data Warehouse | Google BigQuery |
| Transformations | dbt |
| Orchestration | Kestra |
| Visualization | Looker Studio |


---

## Data Architecture (Medallion Layers)

```
Bronze (GCS)
    Raw JSONL files partitioned by YYYY/MM/DD/HH/
    Stored exactly as received from the Kraken API
    Accessed in BigQuery via external table
    Schema: pair STRING, data JSON, ingestion_time TIMESTAMP

Silver (BigQuery — partitioned by date, clustered by pair)
    cleaned and structured data
    deduplicated records
    typed columns (price, volume, trades, etc.)

Gold (BigQuery — partitioned by date, clustered by pair)
  └── gold_ohlc_interval     OHLC candlesticks at configurable interval
  └── gold_alerts            Price alerts (DROP / SPIKE) with gap handling
  └── gold_correlation       BTC vs ETH normalized price comparison
  └── gold_vwap_analysis     VWAP deviation
```

---

## Project Structure

```
crypto-pipeline/
├── src/
│   ├── job/
│   │   └── crypto_ingest.py        Flink ingestion job (GCS sink + checkpoints)
│   └── producers/
│       └── producer.py             Fetches Kraken REST API → publishes to Redpanda via Kafka protocol
├── crypto_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_bronze_raw.sql  Flatten raw JSON into typed columns
│   │   │   └── sources.yml         BigQuery source definition
│   │   ├── silver/
│   │   │   ├── silver_prices.sql   Clean, deduplicated price records
│   │   │   └── schema.yml          dbt tests for silver layer
│   │   └── gold/
│   │       ├── gold_ohlc_interval.sql    OHLC candlesticks
│   │       ├── gold_alerts.sql           Price change alerts
│   │       ├── gold_correlation.sql      BTC vs ETH comparison
│   │       └── gold_vwap_analysis.sql    VWAP deviation
│   ├── dbt_project.yml
│   └── profiles.yml
├── terraform/
│   ├── main.tf                     Creates GCS bucket + BigQuery dataset
│   └── variables.tf                Project ID, region, dataset name
├── flows/
│   ├── start_pipeline.yml          One-time startup flow
│   └── dbt_github_pipeline.yml     Scheduled dbt run every 5 minutes
├── setup_env.sh                    Configures Kestra, uploads flows and KV pairs
├── Dockerfile.flink                Flink image with Kafka JARs + GCS plugin
├── Dockerfile.kestra               Kestra image
├── Dockerfile.producer             Python producer image
├── docker-compose.yml              All services
├── .env.example                    Template for environment variables
├── .gitignore
└── README.md
```

---

## Quick Start

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install)
- A [Google Cloud](https://cloud.google.com) account
- Docker and Docker Compose (GitHub Codespaces has Docker pre-installed, which can be used for this project)

---

### Step 1 — Google Cloud Setup

1. Create a project at [Google Cloud](https://console.cloud.google.com)

2. Create a service account with the following IAM roles:
   - `Storage Admin`
   - `BigQuery Admin`

3. Create and download a JSON key for the service account

4. Rename the downloaded key file to `credentials.json` for later use

> Make sure that `credentials.json` is listed in `.gitignore` and will never be committed
> to GitHub. Never share or commit this file.

---

### Step 2 — Terraform Setup (Local)

1. Make sure you have terraform installed locally on your computer
2. You can copy the terraform folder in the repository and go to the terraform folder
```bash
# Run following commands in bash
cd terraform

# Initialize Terraform providers
terraform init

# Authenticate Terraform with Google Cloud using OAuth
gcloud auth application-default login
# Then in the opened window, allow Google Auth Library access to your Google Account
```
3. Update variables.tf
Update `variables.tf` with your values:

```hcl
variable "project" {
    description = "Project ID"
    default = "your-gcp-project-id"          # ← paste your project id
}

variable "region" {
    description = "Project Region"
    default = "europe-west1"                 # ← change to your region
}

variable "location" {
    description = "Location"                 # ← change to your location
    default = "EU"
}

variable "bq_dataset_name" {
    description = "BigQuery Dataset Name"
    default = "crypto_dataset"                # ← keep or change
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}

variable "gcs_bucket_name" {
    description = "Storage Bucket Name"
    default = "gcs_bucket_name"              # ← change this, make sure it is unique
}
```

4. Preview the resources that will be created
```bash
terraform plan
```

5. Create the GCS bucket and BigQuery dataset
```bash
terraform apply
```

After `terraform apply` completes, verify in the GCP Console that the
following were created:

- GCS bucket: `your-project-id-bucket`
- BigQuery dataset: `crypto_dataset` (in your chosen location)

> I followed the setup in the course material. If you have questions of setting up bucket and bigquery with terraform, you can also refer to [course material](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/terraform/windows.md)
---

> For step3, it is recommended to use github codespace, where Docker is already available. The following instructions assume you are working in Codespaces. To get started, you can click the Code button and select “Create codespace on main” on this [page](https://github.com/Jalynn-X/crypto-pipeline/tree/main).  

---
### Step 3 — Environment Setup

1. Put the credentials.json file you get in the step1 in the root folder in codespace, copy the path for updating the .env file
2. Update .env file
    - Copy and edit the .env file
    ```bash
    # Copy the example environment file
    cp .env.example .env
    ```
    - Edit .env with your actual values. Your `.env` file should contain:
    ```bash
    GCP_PROJECT_ID=your-gcp-project-id
    GCS_BUCKET=your-gcs-bucket-name
    GCS_CHECKPOINT_PATH=gs://your-bucket/checkpoints
    GCS_BRONZE_PATH=gs://your-bucket/bronze/crypto
    # Change ./credentials.json to your GCP JSON Key path if different
    GOOGLE_APPLICATION_CREDENTIALS=./credentials.json 
    ```

---

### Step 4 — Kestra Setup
Update the setup_env.sh file
```python
# GCP Settings
GCP_PROJECT_ID="your-google-cloud-project-id"
GCP_LOCATION="your-project-location"
BQ_DATASET="your-bigquery-dataset-name"
GCS_BUCKET="your-bucket-name"

# Folders
FLOWS_DIR="./flows"                    # Folder containing your .yaml flows
FILE_PATH="./credentials.json"          # Your GCP JSON Key
```

---

### Step 5 — Start Infrastructure

```bash
# Start all containers
docker compose up -d

# Verify all containers are running
docker compose ps
```

---

### Step 6 — Start Kestra Orchestration 

1. Wait a few seconds for Kestra to fully start, then open the Kestra UI by clicking the port 8090 in the Ports tab

2. Register an account exactly as:
   - Email: `admin@123.com`
   - Password: `Admin123`
> **Important:** If you use different email and password, make sure to update `AUTH="admin@123.com:Admin123"` in the `setup_env.sh` correspondingly. 

3. Run the setup_env script in codespace terminal to upload all flows and configure the KV store:
   ```bash
   bash setup_env.sh
   ```
   This script automatically:
   - Uploads all flow files from the `flows/` folder to Kestra
   - Sets all required KV store values (`GCP_PROJECT_ID`, `GCS_BUCKET`,
     `BQ_DATASET`, `GCP_LOCATION`, `GCP_CREDS`)

4. Verify in the Kestra UI that:
   - Namespace `crypto` exists under **Namespaces**
     - All 5 KV store keys are set under **Namespaces → crypto → KV Store**
   - The following flows are visible under **Flows**:
     - `setup_bigquery`
     - `dbt_github_pipeline`
   
5. Verify that in the **Execution**:
   - The `setup_bigquery` ia automatically run and also trigger the `dbt_github_pipeline`
   - After successful execution, the bigquery dataset should contain the staging, silver, and gold tables tables
   - The `dbt_github_pipeline` automatically refreshes silver and gold tables
     every 5 minutes

> **Important:** Kestra uses an in-memory backend in this project. 
> you must re-run `bash setup_env.sh` to restore the KV store and flows if you restart Docker.

---

### Step 7 — Visualization in Looker Studio

![Dashboard](https://github.com/Jalynn-X/crypto-pipeline/blob/main/images/Dashboard.PNG)

1. Go to [lookerstudio](https://lookerstudio.google.com)

2. Click **Create** → **Report** → **Add data** → **BigQuery**. Select your project and connect to the gold layer tables

3. Recommended Chart for Visualization

   | BigQuery Table | Recommended Chart |
   |---|---|
   | `gold_ohlc_interval` | Candlestick / OHLC price charts |
   | `gold_alerts` | Table for Alert history, pie chart for distribution, bar chart for frequency |
   | `gold_vwap_analysis` | Bar chart or line chart for VWAP deviation |
   | `gold_correlation` | Line chart for BTC vs ETH price comparison over time |

5. Refresh Dashboard Data
   - You can click the three-dot icon on the top right corner and then click refresh data

---

### Step 8 — Shut Down and Clean Up

1. To stop the running service, run in codespace terminal:
```bash
docker compose down -v
```
> This will delete all persisted container data.
2. On your [codepsace](https://github.com/codespaces) page, you can see the codespace you just created and used for this project and delete it.
3. In the terraform folder (the **Local** directory where your main.tf is located:), run in bash:
```bash
terraform destroy
```

---

## Notes

- All timestamps are stored in **UTC**. Add 2 hours for CEST (Central European
  Summer Time).
- The Kraken API is polled every **60 seconds** per trading pair.
- dbt refreshes all BigQuery tables every **5 minutes** via Kestra schedule.
- Flink checkpoints are written to GCS every **60 seconds** to prevent data
  loss on job restart.
- The alert model uses a **30-minute rolling window** with ±1% threshold by
  default. Both values are configurable in `gold_alerts.sql`.
- The OHLC model uses a **30-minute candlestick interval** by default.
  You can change `interval_minutes` in `gold_ohlc_interval.sql` as needed.
- Kestra uses an **in-memory backend** — all flows and KV store values must be
  re-initialized after a Docker restart by running `bash setup_env.sh`.
