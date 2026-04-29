"""
US Accidents batch pipeline DAG.

Runs ingest → transform → upload_to_gcs sequentially using DockerOperator.
Scheduled daily at 03:00 UTC. Supports backfills from 2024-01-01.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_airflow_variable(name, default=""):
    return Variable.get(name, default_var=default).strip()


# ── Postgres host ─────────────────────────────────────────────────────────────

PGHOST = os.environ.get("PGHOST", "host.docker.internal")

PG_ENV = {
    "PGHOST": PGHOST,
    "PGPORT": "5432",
    "PGDATABASE": "us_accidents",
    "PGUSER": "root",
    "PGPASSWORD": "root",
}


# ── Ingest config ─────────────────────────────────────────────────────────────

_limit_raw = read_airflow_variable("ingest_limit", "")
INGEST_LIMIT = int(_limit_raw) if _limit_raw else None
_limit_flag = f"--limit {INGEST_LIMIT}" if INGEST_LIMIT else ""


# ── GCS upload config ─────────────────────────────────────────────────────────
# Example:
# cd ../../terraform
# terraform apply
# BUCKET_NAME=$(terraform output -raw gcs_bucket_name)
# docker compose exec airflow_scheduler airflow variables set gcs_bucket "$BUCKET_NAME"

GCS_BUCKET = read_airflow_variable("gcs_bucket", "your-bucket-name")
GCS_TABLE = read_airflow_variable("gcs_table", "accidents")
GCS_OBJECT_NAME = read_airflow_variable("gcs_object_name", "")

_upload_limit_raw = read_airflow_variable("upload_limit", "")
UPLOAD_LIMIT = int(_upload_limit_raw) if _upload_limit_raw else None
_upload_limit_flag = f"--limit {UPLOAD_LIMIT}" if UPLOAD_LIMIT else ""

_object_name_flag = f"--object-name {GCS_OBJECT_NAME}" if GCS_OBJECT_NAME else ""


# ── Shared volume mount ───────────────────────────────────────────────────────

DATA_MOUNT = Mount(
    target="/data",
    source="dockerenvironment_accidents_data",
    type="volume",
    read_only=False,
)

GCP_CREDENTIALS_PATH = read_airflow_variable(
    "gcp_credentials_path", "<Insert path to your key file> *.json"
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
        dag_id="us_accidents_pipeline",
        description="Batch pipeline: ingest US Accidents CSV -> Postgres -> transform -> GCS.",
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule_interval="0 3 * * *",
        catchup=True,
        max_active_runs=1,
        tags=["accidents", "batch", "gcs"],
) as dag:

    ingest = DockerOperator(
        task_id="ingest",
        image="python:3.12-slim",
        command=(
            f"sh -c 'pip install --no-cache-dir -r /data/requirements.txt -q && "
            f"python /data/ingest.py --csv-file /data/us_accidents.csv {_limit_flag}'"
        ),
        environment=PG_ENV,
        mounts=[DATA_MOUNT],
        extra_hosts={"host.docker.internal": "host-gateway"},
        network_mode="accidents_net",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        tty=False,
    )

    transform = DockerOperator(
        task_id="transform",
        image="python:3.12-slim",
        command=(
            "sh -c 'pip install --no-cache-dir -r /data/requirements.txt -q && "
            "python /data/transform.py'"
        ),
        environment=PG_ENV,
        mounts=[DATA_MOUNT],
        extra_hosts={"host.docker.internal": "host-gateway"},
        network_mode="accidents_net",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        tty=False,
    )

    upload_to_gcs = DockerOperator(
        task_id="upload_to_gcs",
        image="python:3.12-slim",
        command=(
            f"sh -c 'pip install --no-cache-dir -r /data/requirements.txt -q && "
            f"python /data/upload_to_gcs.py "
            f"--bucket {GCS_BUCKET} "
            f"--table {GCS_TABLE} "
            f"{_upload_limit_flag} "
            f"{_object_name_flag}'"
        ),
        environment={
            **PG_ENV,
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

    ingest >> transform >> upload_to_gcs