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
    # Explicit schema so BigQuery ignores the original CSV headers (which contain
    # special characters like "Distance(mi)") and maps columns positionally instead.
    return f"""
    CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.external_accidents_raw` (
      id                    STRING,
      source                STRING,
      severity              STRING,
      start_time            STRING,
      end_time              STRING,
      start_lat             STRING,
      start_lng             STRING,
      end_lat               STRING,
      end_lng               STRING,
      distance_mi           STRING,
      description           STRING,
      street                STRING,
      city                  STRING,
      county                STRING,
      state                 STRING,
      zipcode               STRING,
      country               STRING,
      timezone              STRING,
      airport_code          STRING,
      weather_timestamp     STRING,
      temperature_f         STRING,
      wind_chill_f          STRING,
      humidity_pct          STRING,
      pressure_in           STRING,
      visibility_mi         STRING,
      wind_direction        STRING,
      wind_speed_mph        STRING,
      precipitation_in      STRING,
      weather_condition     STRING,
      amenity               STRING,
      bump                  STRING,
      crossing              STRING,
      give_way              STRING,
      junction              STRING,
      no_exit               STRING,
      railway               STRING,
      roundabout            STRING,
      station               STRING,
      stop                  STRING,
      traffic_calming       STRING,
      traffic_signal        STRING,
      turning_loop          STRING,
      sunrise_sunset        STRING,
      civil_twilight        STRING,
      nautical_twilight     STRING,
      astronomical_twilight STRING
    )
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
    WITH base AS (
      SELECT
        id,
        source,
        SAFE_CAST(severity          AS INT64)   AS severity,
        SAFE_CAST(start_time        AS DATETIME) AS start_time,
        SAFE_CAST(end_time          AS DATETIME) AS end_time,
        SAFE_CAST(weather_timestamp AS DATETIME) AS weather_timestamp,
        SAFE_CAST(start_lat         AS FLOAT64)  AS start_lat,
        SAFE_CAST(start_lng         AS FLOAT64)  AS start_lng,
        SAFE_CAST(end_lat           AS FLOAT64)  AS end_lat,
        SAFE_CAST(end_lng           AS FLOAT64)  AS end_lng,
        SAFE_CAST(distance_mi       AS FLOAT64)  AS distance_mi,
        SAFE_CAST(visibility_mi     AS FLOAT64)  AS visibility_mi,
        description, street, city, county, state, zipcode, country, timezone, airport_code,
        SAFE_CAST(temperature_f     AS FLOAT64)  AS temperature_f,
        SAFE_CAST(wind_chill_f      AS FLOAT64)  AS wind_chill_f,
        SAFE_CAST(humidity_pct      AS FLOAT64)  AS humidity_pct,
        SAFE_CAST(pressure_in       AS FLOAT64)  AS pressure_in,
        wind_direction,
        SAFE_CAST(wind_speed_mph    AS FLOAT64)  AS wind_speed_mph,
        SAFE_CAST(precipitation_in  AS FLOAT64)  AS precipitation_in,
        weather_condition,
        SAFE_CAST(amenity           AS BOOL)     AS amenity,
        SAFE_CAST(bump              AS BOOL)     AS bump,
        SAFE_CAST(crossing          AS BOOL)     AS crossing,
        SAFE_CAST(give_way          AS BOOL)     AS give_way,
        SAFE_CAST(junction          AS BOOL)     AS junction,
        SAFE_CAST(no_exit           AS BOOL)     AS no_exit,
        SAFE_CAST(railway           AS BOOL)     AS railway,
        SAFE_CAST(roundabout        AS BOOL)     AS roundabout,
        SAFE_CAST(station           AS BOOL)     AS station,
        SAFE_CAST(stop              AS BOOL)     AS stop,
        SAFE_CAST(traffic_calming   AS BOOL)     AS traffic_calming,
        SAFE_CAST(traffic_signal    AS BOOL)     AS traffic_signal,
        SAFE_CAST(turning_loop      AS BOOL)     AS turning_loop,
        sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight
      FROM `{project}.{dataset}.external_accidents_raw`
    )
    SELECT
      id                                          AS accident_id,
      source,
      severity,

      start_time,
      EXTRACT(YEAR   FROM start_time)             AS start_year,
      EXTRACT(MONTH  FROM start_time)             AS start_month,
      EXTRACT(DAY    FROM start_time)             AS start_day,
      EXTRACT(HOUR   FROM start_time)             AS start_hour,
      EXTRACT(MINUTE FROM start_time)             AS start_minute,

      end_time,
      EXTRACT(YEAR   FROM end_time)               AS end_year,
      EXTRACT(MONTH  FROM end_time)               AS end_month,
      EXTRACT(DAY    FROM end_time)               AS end_day,
      EXTRACT(HOUR   FROM end_time)               AS end_hour,
      EXTRACT(MINUTE FROM end_time)               AS end_minute,

      weather_timestamp,
      EXTRACT(YEAR   FROM weather_timestamp)      AS weather_year,
      EXTRACT(MONTH  FROM weather_timestamp)      AS weather_month,
      EXTRACT(DAY    FROM weather_timestamp)      AS weather_day,
      EXTRACT(HOUR   FROM weather_timestamp)      AS weather_hour,
      EXTRACT(MINUTE FROM weather_timestamp)      AS weather_minute,

      start_lat, start_lng, end_lat, end_lng,

      ROUND(distance_mi   * 1.60934, 3)           AS distance_km,
      ROUND(visibility_mi * 1.60934, 3)           AS visibility_km,

      description, street, city, county, state, zipcode, country, timezone, airport_code,

      ROUND((temperature_f - 32) * 5.0 / 9.0, 2) AS temperature_c,
      ROUND((wind_chill_f  - 32) * 5.0 / 9.0, 2) AS wind_chill_c,
      humidity_pct,
      pressure_in,
      wind_direction,
      wind_speed_mph,
      precipitation_in,
      weather_condition,

      amenity, bump, crossing, give_way, junction, no_exit,
      railway, roundabout, station, stop, traffic_calming, traffic_signal, turning_loop,

      sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight

    FROM base
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
        default="/data/service_account.json",  # ← was /tmp/gcp_credentials.json
        help="Path to service account JSON key (default: /data/service_account.json)",
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