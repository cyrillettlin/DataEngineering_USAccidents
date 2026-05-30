variable "credentials" {
  description = "<Insert your description here>"
  #Update the path below to your .json key. Must be changed!
  #Windows: Use normal / slash
  default     = "<Insert path to your key file> *.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "<insert project description>"
  #Update the project id below to your project id. Must be changed!
  default     = "<insert project id here>"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west6"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset"
  #Update the below to what you want your dataset to be called
  default     = "us_accidents_dataset"
}

variable "gcs_bucket_name" {
  description = "<Insert bucket name description here>"
  #Update the below to a unique bucket name. Must be changed!
  default     = "us-accidents-data-lake-bucket-name-example-1234"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}