"""
US Accidents batch pipeline DAG.

Uploads the raw CSV directly to GCS. All transformation runs in BigQuery
(see us_accidents_bq_dag). Scheduled daily at 03:00 UTC.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


def read_airflow_variable(name, default=""):
    return Variable.get(name, default_var=default).strip()


# ── Config ────────────────────────────────────────────────────────────────────

GCS_BUCKET = read_airflow_variable("gcs_bucket", "your-bucket-name")
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
        description="Batch pipeline: upload US Accidents CSV directly to GCS (transform runs in BigQuery).",
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule="0 3 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["accidents", "batch", "gcs"],
) as dag:

    upload_to_gcs = DockerOperator(
        task_id="upload_to_gcs",
        image="python:3.12-slim",
        command=(
            f"sh -c 'pip install --no-cache-dir -r /data/requirements.txt -q && "
            f"python /data/upload_to_gcs.py "
            f"--csv-file /data/us_accidents.csv "
            f"--bucket {GCS_BUCKET} "
            f"{_upload_limit_flag} "
            f"{_object_name_flag}'"
        ),
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/data/service_account.json",
        },
        mounts=[DATA_MOUNT],
        extra_hosts={"host.docker.internal": "host-gateway"},
        network_mode="accidents_net",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        tty=False,
    )
