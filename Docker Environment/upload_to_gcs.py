import argparse
import os
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from google.cloud import storage


DB_CONFIG = {
    "host": os.getenv("PGHOST", "pgdatabase"),
    "port": int(os.getenv("PGPORT", 5432)),
    "dbname": os.getenv("PGDATABASE", "us_accidents"),
    "user": os.getenv("PGUSER", "root"),
    "password": os.getenv("PGPASSWORD", "root"),
}

DEFAULT_BUCKET_NAME = os.getenv("GCS_BUCKET", "your-bucket-name")
DEFAULT_TABLE_NAME = os.getenv("EXPORT_TABLE", "accidents")
DEFAULT_EXPORT_DIR = "/tmp"
DEFAULT_CHUNK_SIZE = 1024 * 1024 * 8  # 8 MB


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export a PostgreSQL table to CSV and upload it to a Google Cloud Storage bucket."
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET_NAME,
        help=f"Target Google Cloud Storage bucket name, without gs://. Default: {DEFAULT_BUCKET_NAME}",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE_NAME,
        help=f"Postgres table to export. Default: {DEFAULT_TABLE_NAME}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of rows to export. Default: export all rows.",
    )
    parser.add_argument(
        "--object-name",
        default=None,
        help="Target object name in the bucket. Default: exports/<table>_<timestamp>.csv",
    )
    parser.add_argument(
        "--export-dir",
        default=DEFAULT_EXPORT_DIR,
        help=f"Local directory for the temporary CSV export. Default: {DEFAULT_EXPORT_DIR}",
    )
    parser.add_argument(
        "--keep-local-file",
        action="store_true",
        help="Keep the exported CSV locally after upload.",
    )
    return parser.parse_args()


def get_connection(retries=20, delay=3):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"[gcs-upload] Connected to Postgres at {DB_CONFIG['host']}.")
            return conn
        except Exception as e:
            print(f"[gcs-upload] Postgres not ready yet ({attempt}/{retries}): {e}")
            time.sleep(delay)

    raise RuntimeError("Could not connect to Postgres after all retries.")


def validate_table_name(table_name):
    if not table_name.replace("_", "").isalnum():
        raise ValueError("Invalid table name. Use only letters, numbers and underscores.")


def ensure_table_has_rows(conn, table_name):
    validate_table_name(table_name)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);",
            (table_name,),
        )

        if not cur.fetchone()[0]:
            raise RuntimeError(f"Table '{table_name}' does not exist.")

        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cur.fetchone()[0]

        if row_count == 0:
            raise RuntimeError(f"Table '{table_name}' is empty. Nothing to export.")

    print(f"[gcs-upload] Table '{table_name}' is ready with {row_count:,} rows.")


def export_table_to_csv(conn, table_name, export_dir, limit=None):
    validate_table_name(table_name)

    if limit is not None and limit <= 0:
        raise ValueError("--limit must be greater than 0.")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    export_path = Path(export_dir) / f"{table_name}_{timestamp}.csv"
    export_path.parent.mkdir(parents=True, exist_ok=True)

    if limit is None:
        copy_sql = f"COPY {table_name} TO STDOUT WITH CSV HEADER"
        print(f"[gcs-upload] Exporting all rows from '{table_name}' to '{export_path}'...")
    else:
        copy_sql = f"COPY (SELECT * FROM {table_name} LIMIT {limit}) TO STDOUT WITH CSV HEADER"
        print(f"[gcs-upload] Exporting {limit:,} rows from '{table_name}' to '{export_path}'...")

    with conn.cursor() as cur, open(export_path, "w", encoding="utf-8", newline="") as f:
        cur.copy_expert(copy_sql, f)

    size_mb = export_path.stat().st_size / (1024 * 1024)
    print(f"[gcs-upload] Export complete: {size_mb:.2f} MB")

    return export_path


def upload_file_to_bucket(local_path, bucket_name, object_name):
    print(f"[gcs-upload] Uploading to gs://{bucket_name}/{object_name}...")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    blob.chunk_size = DEFAULT_CHUNK_SIZE
    blob.upload_from_filename(str(local_path), content_type="text/csv")

    print(f"[gcs-upload] Upload complete: gs://{bucket_name}/{object_name}")


def main():
    args = parse_args()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    object_name = args.object_name or f"exports/{args.table}_{timestamp}.csv"

    conn = get_connection()
    local_path = None

    try:
        ensure_table_has_rows(conn, args.table)
        local_path = export_table_to_csv(
            conn=conn,
            table_name=args.table,
            export_dir=args.export_dir,
            limit=args.limit,
        )
        upload_file_to_bucket(
            local_path=local_path,
            bucket_name=args.bucket,
            object_name=object_name,
        )

    finally:
        conn.close()
        print("[gcs-upload] Postgres connection closed.")

        if local_path and local_path.exists() and not args.keep_local_file:
            local_path.unlink()
            print(f"[gcs-upload] Deleted local temp file: {local_path}")


if __name__ == "__main__":
    main()