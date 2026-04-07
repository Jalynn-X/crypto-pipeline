# Crypto Pipeline

## Overview

A real-time crypto price data pipeline that ingests, processes, and visualizes
Bitcoin (BTC) and Ethereum (ETH) price data from the Kraken API.

The pipeline streams live ticker data every 60 seconds, stores it in a GCS data
lake, transforms it through a medallion architecture using dbt, and visualizes
insights in Looker Studio.

---

## Architecture

![Pipeline Architecture](https://github.com/Jalynn-X/crypto-pipeline/blob/main/images/Pipeline%20Architecture.PNG)
```
Kraken API
    ↓
Producer
    ↓
Redpanda (Kafka-compatible message broker)
    ↓
Apache Flink (PyFlink stream processing)
    ↓
Google Cloud Storage — Bronze Layer (raw JSONL files)
    ↓
BigQuery External Table
    ↓
dbt (Silver → Gold transformations) Kestra (Orchestration — runs dbt every 5 minutes)
    ↓
Looker Studio (Visualization dashboard)
```
---

## Pipeline Goals

### 1. Monitor Crypto Prices

Monitor real-time price changes of Bitcoin (XBTUSD) and Ethereum (ETHUSD) and
generate alerts when prices change beyond configurable thresholds over a rolling
30-minute window. Alerts are classified as DROP or SPIKE and stored in BigQuery
for historical analysis and dashboard visualization.

### 2. Analyze the Market

Beyond simple price monitoring, this pipeline enables deeper market analysis
through two analytical models:

**VWAP Deviation Analysis**

Volume Weighted Average Price (VWAP) is the average price weighted by trading
volume at each price level. Unlike a simple average, VWAP reflects where most
trading activity occurred. The pipeline calculates the deviation of the current
price from VWAP to identify whether the asset is trading at a premium or
discount relative to its fair value, which can be used to identify
potential buy and sell opportunities.

**BTC vs ETH Correlation Analysis**

Analyzes whether Ethereum price movements follow Bitcoin. Since BTC and ETH
have very different absolute prices (~67,000 vs ~2,000 USD), prices are
normalized to an index starting at 100 to enable fair comparison of relative
price movements on the same chart. This reveals whether the two assets move
together or diverge, which is useful for portfolio risk analysis.

---

## Tech Stack

| Component | Technology |
|---|---|
| Infrastructure | Terraform + Google Cloud Platform |
| Message Broker | Redpanda (Kafka-compatible) |
| Stream Processing | Apache Flink 1.17 (PyFlink) |
| Data Lake | Google Cloud Storage |
| Data Warehouse | Google BigQuery |
| Transformations | dbt (dbt-bigquery 1.7) |
| Orchestration | Kestra |
| Visualization | Looker Studio |
| Containerization | Docker + Docker Compose |

---

## Data Architecture (Medallion Layers)

```
Bronze (GCS)
  Raw JSONL files partitioned by YYYY/MM/DD/HH/
  Stored exactly as received from the Kraken API
  Accessed in BigQuery via external table (no data copy)
  Schema: pair STRING, data JSON, ingestion_time TIMESTAMP

Silver (BigQuery — partitioned by date, clustered by pair)
  Parsed, typed, and deduplicated price records
  All fields from the Kraken ticker API extracted from JSON
  Fields: price, vwap, volume, trades, high, low, open, bid, ask, spread

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

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.0
- A [Google Cloud](https://cloud.google.com) account
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
  > GitHub Codespaces has Docker pre-installed, which can be used for this project

---

### Step 1 — Google Cloud Setup

1. Create a Google Cloud project at [console.cloud.google.com](https://console.cloud.google.com)

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
cd terraform

# Initialize Terraform providers
terraform init

# Authenticate Terraform with Google Cloud using OAuth
gcloud auth application-default login
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

> If you have questions of setting up bucket and bigquery with terraform, you can refer to course material
---

### Step 3 — Environment Setup

If you want to use docker in codespace later, you can do the next steps in codespace by directly clicking "Creating codespace on main"

1. Install Dependencies
    ```bash
    # If uv it not installed, run
    pip install uv
    # If you have uv installed or after uv is installed, run:
    uv sync

    # If you don't use uv but standard pip, run directly:
    ```bash
    pip install -r requirements.txt
    ```
2. Put the credentials.json file you get in the step1 in the root folder in codespace
3. Update .env file
    - Copy and edit the .env file
    ```bash
    # Copy the example environment file
    cp .env.example .env
    # Edit .env with your actual values
    nano .env
    ```

    - Your `.env` file should contain:
    ```bash
    GCP_PROJECT_ID=your-gcp-project-id
    GCS_BUCKET=your-gcs-bucket-name
    GCS_CHECKPOINT_PATH=gs://your-bucket/checkpoints
    GCS_BRONZE_PATH=gs://your-bucket/bronze/crypto
    GOOGLE_APPLICATION_CREDENTIALS=./credentials.json
    ```

---

### Step 4 — Kestra Setup
Update the setup_env.sh file
```python
# GCP Settings
GCP_PROJECT_ID="your google project id"
GCP_LOCATION="the location of bigquery dataset"
BQ_DATASET="the name of bigquery dataset"
GCP_BUCKET="the name of storage bucket"
# Folders
FLOWS_DIR="./flows"                     # Folder containing your .yaml flows
FILE_PATH="./credentials.json"          # Your GCP JSON Key path
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

1. Wait a few seconds for Kestra to fully start, then open the Kestra UI:
   ```
   http://localhost:8090
   ```

2. Register an account exactly as:
   - Email: `admin@123.com`
   - Password: `Admin123`
> **Important:** If you use other email and password to register, make sure to update the setup_env correspondingly. 

3. Run the setup script to upload all flows and configure the KV store:
   ```bash
   bash setup_env.sh
   ```
   This script automatically:
   - Uploads all flow files from the `flows/` folder to Kestra
   - Sets all required KV store values (`GCP_PROJECT_ID`, `GCP_BUCKET`,
     `BQ_DATASET`, `GCP_LOCATION`, `GCP_CREDS`)

4. Verify in the Kestra UI that:
   - Namespace `crypto` exists under **Namespaces**
   - The following flows are visible under **Flows**:
     - `setup_bigquery`
     - `dbt_github_pipeline`
   - All 5 KV store keys are set under **Namespaces → crypto → KV Store**

5. Verify that in the **Execution**:
   - The `setup_bigquery` ia automatically run and also trigger the `dbt_github_pipeline`
   - After successful execution, the bigquery dataset should contain the staging, silver, and gold tables tables
   - The `dbt_github_pipeline` automatically refreshes silver and gold tables
     every 5 minutes

> **Important:** Kestra uses an in-memory backend in this project. 
> you must re-run `bash setup_env.sh` to restore the KV store and flows if you restart Docker.

---

### Step 8 — Visualization in Looker Studio

![Dashboard](https://github.com/Jalynn-X/crypto-pipeline/blob/main/images/Dashboard.PNG)

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)

2. Click **Create** → **Report** → **Add data** → **BigQuery**. Select your project and connect to the gold layer tables

3. Recommended Chart for Visualization

   | BigQuery Table | Recommended Chart |
   |---|---|
   | `gold_ohlc_interval` | Candlestick / OHLC price charts |
   | `gold_alerts` | Table for Alert history, pie chart for distribution, bar chart for frequency |
   | `gold_vwap_analysis` | Bar chart or line chart for VWAP deviation |
   | `gold_correlation` | Line chart for BTC vs ETH price comparison over time |

5. Refresh Dashboard Data
   - Click the three-dot icon on the top right corner and click refresh data



---

## Notes

- All timestamps are stored in **UTC**. Add 2 hours for CEST (Central European
  Summer Time).
- The Kraken API is polled every **60 seconds** per trading pair.
- dbt refreshes all BigQuery tables every **5 minutes** via Kestra schedule.
- Flink checkpoints are written to GCS every **60 seconds** to prevent data
  loss on job restart.
- The alert model uses a **30-minute rolling window** with ±2% threshold by
  default. Both values are configurable in `gold_alerts.sql`.
- The OHLC model uses a **30-minute candlestick interval** by default.
  You can change `interval_minutes` in `gold_ohlc_interval.sql` as needed.
- Kestra uses an **in-memory backend** — all flows and KV store values must be
  re-initialized after a Docker restart by running `bash setup_env.sh`.
