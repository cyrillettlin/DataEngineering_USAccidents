terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}


provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

locals {
  gcs_bucket_name = "us-accidents-data-lake-${var.project}"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name          = local.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_storage_bucket_iam_member" "gcs_uploader" {
  bucket = google_storage_bucket.data-lake-bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${jsondecode(file(var.credentials)).client_email}"
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

output "gcs_bucket_name" {
  value = local.gcs_bucket_name
}