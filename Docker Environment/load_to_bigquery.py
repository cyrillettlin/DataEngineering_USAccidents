"""
Load US Accidents data from GCS into BigQuery.

Reads CSVs exported by pipeline 1 (gs://<bucket>/exports/*.csv) via an external table,
then creates cleaned, partitioned, and partitioned+clustered native BigQuery tables.
"""

import argparse
import logging
import sys

from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="[load-to-bigquery] %(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ── SQL templates ─────────────────────────────────────────────────────────────

def sql_create_external_table(project, dataset, gcs_uri):
    return f"""
    CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.external_accidents_raw`
    OPTIONS (
      format = 'CSV',
      uris = ['{gcs_uri}'],
      skip_leading_rows = 1,
      field_delimiter = ',',
      quote = '"',
      allow_quoted_newlines = TRUE,
      allow_jagged_rows = TRUE
    )
    """


def sql_create_cleaned_table(project, dataset):
    return f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.accidents_cleaned` AS
    SELECT
      id AS accident_id,
      source,
      SAFE_CAST(severity AS INT64) AS severity,

      SAFE_CAST(start_time AS DATETIME) AS start_time,
      SAFE_CAST(start_year AS INT64) AS start_year,
      SAFE_CAST(start_month AS INT64) AS start_month,
      SAFE_CAST(start_day AS INT64) AS start_day,
      SAFE_CAST(start_hour AS INT64) AS start_hour,
      SAFE_CAST(start_minute AS INT64) AS start_minute,

      SAFE_CAST(end_time AS DATETIME) AS end_time,
      SAFE_CAST(end_year AS INT64) AS end_year,
      SAFE_CAST(end_month AS INT64) AS end_month,
      SAFE_CAST(end_day AS INT64) AS end_day,
      SAFE_CAST(end_hour AS INT64) AS end_hour,
      SAFE_CAST(end_minute AS INT64) AS end_minute,

      SAFE_CAST(weather_timestamp AS DATETIME) AS weather_timestamp,
      SAFE_CAST(weather_year AS INT64) AS weather_year,
      SAFE_CAST(weather_month AS INT64) AS weather_month,
      SAFE_CAST(weather_day AS INT64) AS weather_day,
      SAFE_CAST(weather_hour AS INT64) AS weather_hour,
      SAFE_CAST(weather_minute AS INT64) AS weather_minute,

      SAFE_CAST(start_lat AS FLOAT64) AS start_lat,
      SAFE_CAST(start_lng AS FLOAT64) AS start_lng,
      SAFE_CAST(end_lat AS FLOAT64) AS end_lat,
      SAFE_CAST(end_lng AS FLOAT64) AS end_lng,

      SAFE_CAST(distance_km AS FLOAT64) AS distance_km,
      SAFE_CAST(visibility_km AS FLOAT64) AS visibility_km,

      description, street, city, county, state, zipcode,
      country, timezone, airport_code,

      SAFE_CAST(temperature_c AS FLOAT64) AS temperature_c,
      SAFE_CAST(wind_chill_c AS FLOAT64) AS wind_chill_c,
      SAFE_CAST(humidity_pct AS FLOAT64) AS humidity_pct,
      SAFE_CAST(pressure_in AS FLOAT64) AS pressure_in,

      wind_direction,
      SAFE_CAST(wind_speed_mph AS FLOAT64) AS wind_speed_mph,
      SAFE_CAST(precipitation_in AS FLOAT64) AS precipitation_in,

      weather_condition,

      amenity,
      bump,
      crossing,
      give_way,
      junction,
      no_exit,
      railway,
      roundabout,
      station,
      stop,
      traffic_calming,
      traffic_signal,
      turning_loop,

      sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight

    FROM `{project}.{dataset}.external_accidents_raw`
    """


def sql_create_partitioned_table(project, dataset):
    return f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.accidents_partitioned`
    PARTITION BY DATE(start_time) AS
    SELECT * FROM `{project}.{dataset}.accidents_cleaned`
    """


def sql_create_partitioned_clustered_table(project, dataset):
    return f"""
    CREATE OR REPLACE TABLE `{project}.{dataset}.accidents_partitioned_clustered`
    PARTITION BY DATE(start_time)
    CLUSTER BY state, severity, city AS
    SELECT * FROM `{project}.{dataset}.accidents_cleaned`
    """


# ── Core logic ────────────────────────────────────────────────────────────────

def run_query(client, description, sql):
    log.info("Running: %s", description)
    job = client.query(sql)
    job.result()
    log.info("Done: %s", description)


def load_to_bigquery(project, dataset, bucket, gcs_prefix, credentials_path):
    gcs_uri = f"gs://{bucket}/{gcs_prefix}"
    log.info("GCS source: %s", gcs_uri)
    log.info("BigQuery target: %s.%s", project, dataset)

    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(project=project, credentials=creds)

    run_query(client, "create external table", sql_create_external_table(project, dataset, gcs_uri))
    run_query(client, "create accidents_cleaned", sql_create_cleaned_table(project, dataset))
    run_query(client, "create accidents_partitioned", sql_create_partitioned_table(project, dataset))
    run_query(client, "create accidents_partitioned_clustered", sql_create_partitioned_clustered_table(project, dataset))

    log.info("All BigQuery tables created successfully.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Load GCS accident data into BigQuery.")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset ID")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument(
        "--gcs-prefix",
        default="exports/*.csv",
        help="Path/wildcard inside the bucket (default: exports/*.csv)",
    )
    parser.add_argument(
        "--credentials",
        default="/tmp/gcp_credentials.json",
        help="Path to service account JSON key (default: /tmp/gcp_credentials.json)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        load_to_bigquery(
            project=args.project,
            dataset=args.dataset,
            bucket=args.bucket,
            gcs_prefix=args.gcs_prefix,
            credentials_path=args.credentials,
        )
    except Exception as exc:
        log.error("Pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
