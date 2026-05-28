"""
Upload US Accidents CSV directly to Google Cloud Storage.

Uploads the raw CSV file as-is (full file) or a row-limited subset for testing.
BigQuery reads the file via an external table with an explicit schema, so the
original CSV headers with special characters are skipped and columns are mapped
positionally — no header renaming is needed here.
"""

import argparse
import csv
import os
from datetime import datetime
from pathlib import Path

from google.cloud import storage

DEFAULT_BUCKET_NAME = os.getenv("GCS_BUCKET", "your-bucket-name")
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB resumable upload chunks


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload US Accidents CSV to a GCS bucket."
    )
    parser.add_argument("--csv-file", required=True, help="Path to the source CSV file")
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET_NAME,
        help=f"GCS bucket name (default: {DEFAULT_BUCKET_NAME})",
    )
    parser.add_argument(
        "--object-name",
        default=None,
        help="Target object path in the bucket (default: exports/us_accidents_<timestamp>.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Upload only the first N data rows — useful for testing",
    )
    return parser.parse_args()


def upload_csv(csv_path: Path, bucket_name: str, object_name: str, limit=None):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.chunk_size = CHUNK_SIZE

    if limit is None:
        # Stream the file directly to GCS — no memory overhead regardless of size
        print(f"[gcs-upload] Uploading '{csv_path}' → gs://{bucket_name}/{object_name}...")
        blob.upload_from_filename(str(csv_path), content_type="text/csv")
        size_mb = csv_path.stat().st_size / (1024 * 1024)
        print(f"[gcs-upload] Done. {size_mb:.1f} MB uploaded.")
    else:
        # Write a row-limited subset to /tmp, upload it, then clean up
        tmp_path = Path(f"/tmp/_upload_{datetime.utcnow():%Y%m%d_%H%M%S}.csv")
        print(f"[gcs-upload] Writing {limit:,}-row subset to '{tmp_path}'...")
        with open(csv_path, "r", encoding="utf-8") as f_in, \
             open(tmp_path, "w", encoding="utf-8", newline="") as f_out:
            reader = csv.reader(f_in)
            writer = csv.writer(f_out)
            writer.writerow(next(reader))  # preserve original header
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                writer.writerow(row)
        print(f"[gcs-upload] Uploading subset → gs://{bucket_name}/{object_name}...")
        blob.upload_from_filename(str(tmp_path), content_type="text/csv")
        tmp_path.unlink()
        print(f"[gcs-upload] Done. {limit:,} rows uploaded.")


def main():
    args = parse_args()
    csv_path = Path(args.csv_file)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    object_name = args.object_name or f"exports/us_accidents_{timestamp}.csv"

    upload_csv(
        csv_path=csv_path,
        bucket_name=args.bucket,
        object_name=object_name,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
