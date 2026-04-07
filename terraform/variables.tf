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