"""
US Accidents batch pipeline DAG.

Runs ingest → transform sequentially using DockerOperator.
Scheduled daily at 03:00 UTC. Supports backfills from 2024-01-01.

Row limit (for testing):
  Set the Airflow Variable 'ingest_limit' to control how many rows are ingested.
  - Not set or empty  →  full file (~7M rows, production mode)
  - "1000"            →  1k rows, fast smoke test (~10 seconds)
  - "100000"          →  100k rows, representative sample (~1 minute)

  Set via Airflow UI:  Admin → Variables → ingest_limit
  Set via CLI:
    docker compose exec airflow_scheduler \
      airflow variables set ingest_limit 1000
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ── Row limit ─────────────────────────────────────────────────────────────────
# Read from Airflow Variable so it can be changed without touching the DAG file.
# Returns None (= full file) if the variable is not set or is blank.
_limit_raw = Variable.get("ingest_limit", default_var="").strip()
INGEST_LIMIT = int(_limit_raw) if _limit_raw else None

_limit_flag = f"--limit {INGEST_LIMIT}" if INGEST_LIMIT else ""

# ── Postgres connection (host.docker.internal for Docker Desktop on Windows/Mac)
PG_ENV = {
    "PGHOST": "host.docker.internal",
    "PGPORT": "5432",
    "PGDATABASE": "us_accidents",
    "PGUSER": "root",
    "PGPASSWORD": "root",
}

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
    description="Batch pipeline: ingest US Accidents CSV -> Postgres, then transform.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["accidents", "batch"],
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
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        tty=False,
    )

    ingest >> transform