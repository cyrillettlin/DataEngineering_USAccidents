import csv
import os
import time
import argparse
from datetime import datetime

import psycopg2

# Database connection configuration
DB_CONFIG = {
    "host": "pgdatabase",
    "port": 5432,
    "dbname": "us_accidents",
    "user": "root",
    "password": "root",
}

# Target table for raw ingestion (staging table)
TABLE_NAME = "import_accidents"


def parse_args():
    """Parse command line arguments (CSV path + optional row limit)."""
    parser = argparse.ArgumentParser(description="Ingest CSV into Postgres")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Number of rows to ingest (default: all)",
    )
    parser.add_argument(
        "--csv-file",
        type=str,
        required=True,
        help="Path to the CSV file",
    )
    return parser.parse_args()


# Helper functions to safely convert values from CSV
def parse_float(value):
    return float(value) if value else None


def parse_int(value):
    return int(value) if value else None


def parse_bool(value):
    return value.strip().lower() == "true" if value else None


def parse_timestamp(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value else None


def get_connection(retries=20, delay=3):
    """Retry connection to Postgres (useful in container environments)."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("Connected to Postgres.")
            return conn
        except Exception as e:
            print(f"Postgres not ready yet ({attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres.")


def create_table(conn):
    """Create staging table for raw data if it does not exist."""
    query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id TEXT PRIMARY KEY,
        source TEXT,
        severity INTEGER,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        start_lat DOUBLE PRECISION,
        start_lng DOUBLE PRECISION,
        end_lat DOUBLE PRECISION,
        end_lng DOUBLE PRECISION,
        distance_mi DOUBLE PRECISION,
        description TEXT,
        street TEXT,
        city TEXT,
        county TEXT,
        state TEXT,
        zipcode TEXT,
        country TEXT,
        timezone TEXT,
        airport_code TEXT,
        weather_timestamp TIMESTAMP,
        temperature_f DOUBLE PRECISION,
        wind_chill_f DOUBLE PRECISION,
        humidity_pct DOUBLE PRECISION,
        pressure_in DOUBLE PRECISION,
        visibility_mi DOUBLE PRECISION,
        wind_direction TEXT,
        wind_speed_mph DOUBLE PRECISION,
        precipitation_in DOUBLE PRECISION,
        weather_condition TEXT,
        amenity BOOLEAN,
        bump BOOLEAN,
        crossing BOOLEAN,
        give_way BOOLEAN,
        junction BOOLEAN,
        no_exit BOOLEAN,
        railway BOOLEAN,
        roundabout BOOLEAN,
        station BOOLEAN,
        stop BOOLEAN,
        traffic_calming BOOLEAN,
        traffic_signal BOOLEAN,
        turning_loop BOOLEAN,
        sunrise_sunset TEXT,
        civil_twilight TEXT,
        nautical_twilight TEXT,
        astronomical_twilight TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


def ingest_csv(conn, csv_file, limit=None):
    """
    Load CSV data into Postgres.

    Reads the file row by row and inserts into the staging table.
    ON CONFLICT ensures no duplicate IDs are inserted.
    """
    insert_query = f"""
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

    count = 0

    # Open CSV file and read rows as dictionaries
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        with conn.cursor() as cur:
            for row in reader:
                # Convert and map CSV fields to database schema
                values = (
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

                # Insert row into database
                cur.execute(insert_query, values)
                count += 1

                # Optional limit for testing / partial ingestion
                if limit is not None and count >= limit:
                    print(f"Limit reached: {limit} rows")
                    break

    conn.commit()
    print(f"Inserted {count} rows.")


def main():
    """Main entry point for ingestion pipeline."""
    args = parse_args()

    # Validate input file
    if not os.path.exists(args.csv_file):
        raise FileNotFoundError(f"CSV not found: {args.csv_file}")

    conn = get_connection()
    try:
        create_table(conn)
        ingest_csv(conn, args.csv_file, args.limit)
    finally:
        conn.close()


if __name__ == "__main__":
    main()