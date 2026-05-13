"""
US Accidents BigQuery pipeline DAG.

Waits for pipeline 1 (us_accidents_pipeline) to finish uploading to GCS, then
loads the exported CSV data into BigQuery: external table → cleaned → partitioned
→ partitioned+clustered.

Scheduled daily at 06:00 UTC (3 h after pipeline 1 at 03:00 UTC).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from docker.types import Mount


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_airflow_variable(name, default=""):
    return Variable.get(name, default_var=default).strip()


# ── Config ────────────────────────────────────────────────────────────────────

BQ_PROJECT = read_airflow_variable("bq_project", "solar-solution-******-**")
BQ_DATASET = read_airflow_variable("bq_dataset", "us_accidents_dataset")
GCS_BUCKET = read_airflow_variable("gcs_bucket", "your-bucket-name")
GCS_PREFIX = read_airflow_variable("gcs_prefix", "exports/*.csv")
GCP_CREDENTIALS_PATH = read_airflow_variable(
    "gcp_credentials_path", "<Insert path to your key file> *.json"
)
SKIP_UPSTREAM_SENSOR = read_airflow_variable("bq_skip_upstream_sensor", "false").lower() == "true"


# ── Mounts ────────────────────────────────────────────────────────────────────

DATA_MOUNT = Mount(
    target="/data",
    source="dockerenvironment_accidents_data",
    type="volume",
    read_only=False,
)

GCP_CREDENTIALS_MOUNT = Mount(
    target="/tmp/gcp_credentials.json",
    source=GCP_CREDENTIALS_PATH,
    type="bind",
    read_only=True,
)


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
        dag_id="us_accidents_bq_pipeline",
        description="Pipeline 2: load GCS accident exports → BigQuery (cleaned, partitioned, clustered).",
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule_interval="0 6 * * *",
        catchup=True,
        max_active_runs=1,
        tags=["accidents", "batch", "bigquery"],
) as dag:

    if SKIP_UPSTREAM_SENSOR:
        wait_for_gcs_upload = EmptyOperator(task_id="wait_for_gcs_upload")
    else:
        wait_for_gcs_upload = ExternalTaskSensor(
            task_id="wait_for_gcs_upload",
            external_dag_id="us_accidents_pipeline",
            external_task_id="upload_to_gcs",
            allowed_states=["success"],
            execution_delta=timedelta(hours=3),  # pipeline 1 runs at 03:00, this at 06:00
            timeout=3600,
            poke_interval=60,
            mode="poke",
        )

    load_to_bigquery = DockerOperator(
        task_id="load_to_bigquery",
        image="python:3.12-slim",
        command=(
            f"sh -c 'pip install --no-cache-dir -r /data/requirements.txt -q && "
            f"python /data/load_to_bigquery.py "
            f"--project {BQ_PROJECT} "
            f"--dataset {BQ_DATASET} "
            f"--bucket {GCS_BUCKET} "
            f"--gcs-prefix \"{GCS_PREFIX}\"'"
        ),
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/tmp/gcp_credentials.json",
        },
        mounts=[DATA_MOUNT, GCP_CREDENTIALS_MOUNT],
        extra_hosts={"host.docker.internal": "host-gateway"},
        network_mode="accidents_net",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        tty=False,
    )

    wait_for_gcs_upload >> load_to_bigquery
