variable "project" {
    description = "Project ID"
    default = "crypto-pipeline-491522"
}

variable "region" {
    description = "Project Region"
    default = "europe-west1"
}

variable "location" {
    description = "Location"
    default = "EU"
}

variable "bq_dataset_name" {
    description = "BigQuery Dataset Name"
    default = "crypto_dataset"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}

variable "gcs_bucket_name" {
    description = "Storage Bucket Name"
    default = "crypto-pipeline-491522-bucket"
}