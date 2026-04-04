"""
US Accidents batch pipeline DAG.

Runs ingest → transform sequentially using DockerOperator.
Scheduled daily at 03:00 UTC. Supports backfills from 2024-01-01.

Platform notes:
  PGHOST is read from the environment variable set by setup_env.sh:
    - Linux / WSL:   pgdatabase          (task containers join accidents_net)
    - Windows / Mac: host.docker.internal (Docker Desktop proxy)

Row limit (for testing):
  Set Airflow Variable 'ingest_limit' to restrict how many rows are ingested.
    Not set  →  full file (~7M rows)
    "1000"   →  quick smoke test (~10 seconds)
  Via CLI:  docker compose exec airflow_scheduler airflow variables set ingest_limit 1000
  Via UI:   Admin → Variables → ingest_limit
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ── Postgres host (platform-aware) ────────────────────────────────────────────
# Injected into the DAG container via the .env file written by setup_env.sh.
# Falls back to host.docker.internal so the DAG works on Windows/Mac even
# without running setup_env.sh first.
PGHOST = os.environ.get("PGHOST", "host.docker.internal")

PG_ENV = {
    "PGHOST": PGHOST,
    "PGPORT": "5432",
    "PGDATABASE": "us_accidents",
    "PGUSER": "root",
    "PGPASSWORD": "root",
}

# ── Row limit (optional, for testing) ────────────────────────────────────────
_limit_raw = Variable.get("ingest_limit", default_var="").strip()
INGEST_LIMIT = int(_limit_raw) if _limit_raw else None
_limit_flag = f"--limit {INGEST_LIMIT}" if INGEST_LIMIT else ""

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
        # extra_hosts ensures host.docker.internal resolves inside Linux
        # containers on Docker Desktop. On native Linux with network_mode
        # accidents_net this entry is harmless.
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

    ingest >> transform