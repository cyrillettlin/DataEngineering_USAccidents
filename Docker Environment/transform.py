import os
import time

import psycopg2

DB_CONFIG = {
    "host": os.getenv("PGHOST", "pgdatabase"),
    "port": int(os.getenv("PGPORT", 5432)),
    "dbname": os.getenv("PGDATABASE", "us_accidents"),
    "user": os.getenv("PGUSER", "root"),
    "password": os.getenv("PGPASSWORD", "root"),
}

SOURCE_TABLE = "import_accidents"
TARGET_TABLE = "accidents"
MI_TO_KM = 1.60934


# ── Database helpers ──────────────────────────────────────────────────────────

def get_connection(retries=20, delay=3):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"[transform] Connected to Postgres at {DB_CONFIG['host']}.")
            return conn
        except Exception as e:
            print(f"[transform] Postgres not ready yet ({attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres after all retries.")


def wait_for_source_table(conn, retries=60, delay=5):
    """Wait until the staging table exists and contains at least one row."""
    for attempt in range(1, retries + 1):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);",
                    (SOURCE_TABLE,),
                )
                if cur.fetchone()[0]:
                    cur.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE};")
                    row_count = cur.fetchone()[0]
                    if row_count > 0:
                        print(f"[transform] Source table ready with {row_count:,} rows.")
                        return
                    print(f"[transform] Table empty ({attempt}/{retries}), waiting...")
                else:
                    print(f"[transform] Waiting for '{SOURCE_TABLE}' ({attempt}/{retries})...")
            time.sleep(delay)
        except Exception as e:
            print(f"[transform] Error while waiting ({attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError(f"Source table '{SOURCE_TABLE}' was not ready after all retries.")


# ── Schema ────────────────────────────────────────────────────────────────────

def create_target_table(conn):
    """Create the clean analytics table and its indexes if they don't exist."""
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id                    TEXT PRIMARY KEY,
        source                TEXT,
        severity              INTEGER,

        start_time            TIMESTAMP,
        start_year            INTEGER,
        start_month           INTEGER,
        start_day             INTEGER,
        start_hour            INTEGER,
        start_minute          INTEGER,

        end_time              TIMESTAMP,
        end_year              INTEGER,
        end_month             INTEGER,
        end_day               INTEGER,
        end_hour              INTEGER,
        end_minute            INTEGER,

        weather_timestamp     TIMESTAMP,
        weather_year          INTEGER,
        weather_month         INTEGER,
        weather_day           INTEGER,
        weather_hour          INTEGER,
        weather_minute        INTEGER,

        start_lat             DOUBLE PRECISION,
        start_lng             DOUBLE PRECISION,
        end_lat               DOUBLE PRECISION,
        end_lng               DOUBLE PRECISION,

        distance_km           DOUBLE PRECISION,
        visibility_km         DOUBLE PRECISION,

        description           TEXT,
        street                TEXT,
        city                  TEXT,
        county                TEXT,
        state                 TEXT,
        zipcode               TEXT,
        country               TEXT,
        timezone              TEXT,
        airport_code          TEXT,

        temperature_c         DOUBLE PRECISION,
        wind_chill_c          DOUBLE PRECISION,
        humidity_pct          DOUBLE PRECISION,
        pressure_in           DOUBLE PRECISION,
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

    """
    index_queries is a list of CREATE INDEX SQL statements that get executed after the target table is created. Each one adds a database index on a column that's commonly used as a filter in analytical       
    queries — start_time, start_year, start_month, start_hour, state, and severity.
                                                                                                                                                                                                              
    The purpose is query performance: without indexes, filtering a large accidents dataset (e.g. "accidents in California in 2022") requires a full table scan. With the indexes, the database can jump directly
    to matching rows.
    """
    index_queries = [
        f"CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_start_time  ON {TARGET_TABLE} (start_time);",
        f"CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_start_year  ON {TARGET_TABLE} (start_year);",
        f"CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_start_month ON {TARGET_TABLE} (start_month);",
        f"CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_start_hour  ON {TARGET_TABLE} (start_hour);",
        f"CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_state       ON {TARGET_TABLE} (state);",
        f"CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_severity    ON {TARGET_TABLE} (severity);",
    ]

    with conn.cursor() as cur:
        cur.execute(create_query)
        for q in index_queries:
            cur.execute(q)
    conn.commit()
    print(f"[transform] Table '{TARGET_TABLE}' and indexes are ready.")


# ── Transformation ────────────────────────────────────────────────────────────

def transform_data(conn):
    """
    Populate the analytics table from the staging table.

    Performance improvements vs. original:
      - SET work_mem: gives Postgres more RAM for the sort/hash operations
        inside the INSERT…SELECT, avoiding slow disk spills on large datasets.
      - ANALYZE after insert: updates table statistics so the query planner
        picks efficient plans for subsequent analytical queries.
    """
    truncate_query = f"TRUNCATE TABLE {TARGET_TABLE};"

    # Bump work_mem for this session only — does not affect other connections.
    # 256MB is enough for the sort pass over ~7M rows without disk spill.
    work_mem_query = "SET work_mem = '256MB';"

    insert_query = f"""
    INSERT INTO {TARGET_TABLE} (
        id, source, severity,

        start_time, start_year, start_month, start_day, start_hour, start_minute,
        end_time,   end_year,   end_month,   end_day,   end_hour,   end_minute,

        weather_timestamp,
        weather_year, weather_month, weather_day, weather_hour, weather_minute,

        start_lat, start_lng, end_lat, end_lng,

        distance_km, visibility_km,

        description, street, city, county, state, zipcode,
        country, timezone, airport_code,

        temperature_c, wind_chill_c,
        humidity_pct, pressure_in,
        wind_direction, wind_speed_mph, precipitation_in, weather_condition,

        amenity, bump, crossing, give_way, junction, no_exit,
        railway, roundabout, station, stop, traffic_calming,
        traffic_signal, turning_loop,

        sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight
    )
    SELECT
        id, source, severity,

        start_time,
        EXTRACT(YEAR   FROM start_time)::INTEGER,
        EXTRACT(MONTH  FROM start_time)::INTEGER,
        EXTRACT(DAY    FROM start_time)::INTEGER,
        EXTRACT(HOUR   FROM start_time)::INTEGER,
        EXTRACT(MINUTE FROM start_time)::INTEGER,

        end_time,
        EXTRACT(YEAR   FROM end_time)::INTEGER,
        EXTRACT(MONTH  FROM end_time)::INTEGER,
        EXTRACT(DAY    FROM end_time)::INTEGER,
        EXTRACT(HOUR   FROM end_time)::INTEGER,
        EXTRACT(MINUTE FROM end_time)::INTEGER,

        weather_timestamp,
        EXTRACT(YEAR   FROM weather_timestamp)::INTEGER,
        EXTRACT(MONTH  FROM weather_timestamp)::INTEGER,
        EXTRACT(DAY    FROM weather_timestamp)::INTEGER,
        EXTRACT(HOUR   FROM weather_timestamp)::INTEGER,
        EXTRACT(MINUTE FROM weather_timestamp)::INTEGER,

        start_lat, start_lng, end_lat, end_lng,

        CASE WHEN distance_mi   IS NOT NULL THEN ROUND((distance_mi   * {MI_TO_KM})::numeric, 3)::DOUBLE PRECISION END,
        CASE WHEN visibility_mi IS NOT NULL THEN ROUND((visibility_mi * {MI_TO_KM})::numeric, 3)::DOUBLE PRECISION END,

        description, street, city, county, state, zipcode,
        country, timezone, airport_code,

        CASE WHEN temperature_f IS NOT NULL THEN ROUND(((temperature_f - 32) * 5.0 / 9.0)::numeric, 2)::DOUBLE PRECISION END,
        CASE WHEN wind_chill_f  IS NOT NULL THEN ROUND(((wind_chill_f  - 32) * 5.0 / 9.0)::numeric, 2)::DOUBLE PRECISION END,

        humidity_pct, pressure_in,
        wind_direction, wind_speed_mph, precipitation_in, weather_condition,

        amenity, bump, crossing, give_way, junction, no_exit,
        railway, roundabout, station, stop, traffic_calming,
        traffic_signal, turning_loop,

        sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight

    FROM {SOURCE_TABLE};
    """

    analyze_query = f"ANALYZE {TARGET_TABLE};"
    count_query   = f"SELECT COUNT(*) FROM {TARGET_TABLE};"

    with conn.cursor() as cur:
        cur.execute(work_mem_query)

        print(f"[transform] Truncating '{TARGET_TABLE}'...")
        cur.execute(truncate_query)

        print(f"[transform] Inserting transformed rows...")
        cur.execute(insert_query)

        print(f"[transform] Running ANALYZE...")
        cur.execute(analyze_query)

        cur.execute(count_query)
        count = cur.fetchone()[0]

    conn.commit()
    print(f"[transform] Done. {count:,} rows written to '{TARGET_TABLE}'.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("[transform] Starting transformation...")
    conn = get_connection()
    try:
        wait_for_source_table(conn)
        create_target_table(conn)
        transform_data(conn)
    finally:
        conn.close()
        print("[transform] Connection closed.")


if __name__ == "__main__":
    main()