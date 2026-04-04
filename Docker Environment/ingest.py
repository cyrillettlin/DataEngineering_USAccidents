import csv
import io
import os
import time
import argparse
from datetime import datetime

import psycopg2
import psycopg2.extras

# Database connection configuration
DB_CONFIG = {
    "host": os.getenv("PGHOST", "pgdatabase"),
    "port": int(os.getenv("PGPORT", 5432)),
    "dbname": os.getenv("PGDATABASE", "us_accidents"),
    "user": os.getenv("PGUSER", "root"),
    "password": os.getenv("PGPASSWORD", "root"),
}

TABLE_NAME = "import_accidents"

# Rows accumulated in memory before flushing to Postgres.
# execute_batch sends each batch in a single network roundtrip,
# so larger batches = fewer roundtrips = faster ingestion.
# 50k is a good balance between speed and memory usage.
BATCH_SIZE = 50_000


def parse_args():
    """Parse command line arguments (CSV path + optional row limit)."""
    parser = argparse.ArgumentParser(description="Ingest CSV into Postgres")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N rows — useful for quick tests (e.g. --limit 1000)",
    )
    parser.add_argument(
        "--csv-file",
        type=str,
        required=True,
        help="Path to the CSV file",
    )
    return parser.parse_args()


# ── Type helpers ──────────────────────────────────────────────────────────────

def parse_float(value):
    return float(value) if value and value.strip() else None

def parse_int(value):
    return int(value) if value and value.strip() else None

def parse_bool(value):
    if not value or not value.strip():
        return None
    return value.strip().lower() == "true"

def parse_timestamp(value):
    if not value or not value.strip():
        return None
    return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection(retries=20, delay=3):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"[ingest] Connected to Postgres at {DB_CONFIG['host']}.")
            return conn
        except Exception as e:
            print(f"[ingest] Postgres not ready yet ({attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after all retries.")


def create_table(conn):
    """Create the raw staging table if it does not already exist."""
    query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id                    TEXT PRIMARY KEY,
        source                TEXT,
        severity              INTEGER,
        start_time            TIMESTAMP,
        end_time              TIMESTAMP,
        start_lat             DOUBLE PRECISION,
        start_lng             DOUBLE PRECISION,
        end_lat               DOUBLE PRECISION,
        end_lng               DOUBLE PRECISION,
        distance_mi           DOUBLE PRECISION,
        description           TEXT,
        street                TEXT,
        city                  TEXT,
        county                TEXT,
        state                 TEXT,
        zipcode               TEXT,
        country               TEXT,
        timezone              TEXT,
        airport_code          TEXT,
        weather_timestamp     TIMESTAMP,
        temperature_f         DOUBLE PRECISION,
        wind_chill_f          DOUBLE PRECISION,
        humidity_pct          DOUBLE PRECISION,
        pressure_in           DOUBLE PRECISION,
        visibility_mi         DOUBLE PRECISION,
        wind_direction        TEXT,
        wind_speed_mph        DOUBLE PRECISION,
        precipitation_in      DOUBLE PRECISION,
        weather_condition     TEXT,
        amenity               BOOLEAN,
        bump                  BOOLEAN,
        crossing              BOOLEAN,
        give_way              BOOLEAN,
        junction              BOOLEAN,
        no_exit               BOOLEAN,
        railway               BOOLEAN,
        roundabout            BOOLEAN,
        station               BOOLEAN,
        stop                  BOOLEAN,
        traffic_calming       BOOLEAN,
        traffic_signal        BOOLEAN,
        turning_loop          BOOLEAN,
        sunrise_sunset        TEXT,
        civil_twilight        TEXT,
        nautical_twilight     TEXT,
        astronomical_twilight TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    print(f"[ingest] Table '{TABLE_NAME}' is ready.")


# ── Core ingestion ────────────────────────────────────────────────────────────

INSERT_QUERY = f"""
INSERT INTO {TABLE_NAME} VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s
)
ON CONFLICT (id) DO NOTHING;
"""


def build_row(row):
    """Parse one CSV dict into a tuple of typed values."""
    return (
        row["ID"],
        row["Source"],
        parse_int(row["Severity"]),
        parse_timestamp(row["Start_Time"]),
        parse_timestamp(row["End_Time"]),
        parse_float(row["Start_Lat"]),
        parse_float(row["Start_Lng"]),
        parse_float(row["End_Lat"]),
        parse_float(row["End_Lng"]),
        parse_float(row["Distance(mi)"]),
        row["Description"],
        row["Street"],
        row["City"],
        row["County"],
        row["State"],
        row["Zipcode"],
        row["Country"],
        row["Timezone"],
        row["Airport_Code"],
        parse_timestamp(row["Weather_Timestamp"]),
        parse_float(row["Temperature(F)"]),
        parse_float(row["Wind_Chill(F)"]),
        parse_float(row["Humidity(%)"]),
        parse_float(row["Pressure(in)"]),
        parse_float(row["Visibility(mi)"]),
        row["Wind_Direction"],
        parse_float(row["Wind_Speed(mph)"]),
        parse_float(row["Precipitation(in)"]),
        row["Weather_Condition"],
        parse_bool(row["Amenity"]),
        parse_bool(row["Bump"]),
        parse_bool(row["Crossing"]),
        parse_bool(row["Give_Way"]),
        parse_bool(row["Junction"]),
        parse_bool(row["No_Exit"]),
        parse_bool(row["Railway"]),
        parse_bool(row["Roundabout"]),
        parse_bool(row["Station"]),
        parse_bool(row["Stop"]),
        parse_bool(row["Traffic_Calming"]),
        parse_bool(row["Traffic_Signal"]),
        parse_bool(row["Turning_Loop"]),
        row["Sunrise_Sunset"],
        row["Civil_Twilight"],
        row["Nautical_Twilight"],
        row["Astronomical_Twilight"],
    )


def flush_batch(cur, batch):
    """
    Send a batch of rows to Postgres in a single roundtrip.

    execute_batch() is significantly faster than calling execute() once per row
    because it packs all rows into one network request instead of N requests.
    page_size controls how many rows are sent per individual SQL statement;
    the default (100) is fine — we control total batch size via BATCH_SIZE.
    """
    psycopg2.extras.execute_batch(cur, INSERT_QUERY, batch, page_size=BATCH_SIZE)


def ingest_csv(conn, csv_file, limit=None):
    """
    Load CSV data into the staging table using batched inserts.

    Key performance changes vs. the original:
      - execute_batch() instead of execute(): sends BATCH_SIZE rows per
        network roundtrip instead of one row at a time. On a local Docker
        network this alone cuts ingestion time by ~5–10x.
      - Batch size raised from 10k to 50k: fewer commits, less overhead.
      - Single cursor kept open for the entire file: no repeated open/close.
    """
    count = 0
    skipped = 0
    batch = []

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        with conn.cursor() as cur:
            for row in reader:
                try:
                    batch.append(build_row(row))
                    count += 1
                except Exception as e:
                    skipped += 1
                    print(f"[ingest] Skipping row (ID={row.get('ID', '?')}): {e}")

                if len(batch) >= BATCH_SIZE:
                    flush_batch(cur, batch)
                    conn.commit()
                    batch.clear()
                    print(f"[ingest] Progress: {count:,} rows committed...")

                if limit is not None and count >= limit:
                    print(f"[ingest] Row limit reached: {limit:,}")
                    break

            # Flush any remaining rows
            if batch:
                flush_batch(cur, batch)

    conn.commit()
    print(f"[ingest] Done. Inserted: {count:,} rows | Skipped: {skipped:,} rows.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    if not os.path.exists(args.csv_file):
        raise FileNotFoundError(f"CSV file not found: {args.csv_file}")

    mode = f"limit: {args.limit:,}" if args.limit else "full file"
    print(f"[ingest] Starting ingestion of '{args.csv_file}' ({mode})...")

    conn = get_connection()
    try:
        create_table(conn)
        ingest_csv(conn, args.csv_file, args.limit)
    finally:
        conn.close()
        print("[ingest] Connection closed.")


if __name__ == "__main__":
    main()