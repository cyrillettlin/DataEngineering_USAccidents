import time
import psycopg2

DB_CONFIG = {
    "host": "pgdatabase",
    "port": 5432,
    "dbname": "us_accidents",
    "user": "root",
    "password": "root",
}

SOURCE_TABLE = "import_accidents"
TARGET_TABLE = "accidents"

MI_TO_KM = 1.60934


def get_connection(retries=20, delay=3):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("Connected to Postgres.")
            return conn
        except Exception as e:
            print(f"Postgres not ready yet ({attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres.")


def create_target_table(conn):
    query = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id TEXT PRIMARY KEY,
        source TEXT,
        severity INTEGER,

        start_time TIMESTAMP,
        start_year INTEGER,
        start_month INTEGER,
        start_day INTEGER,
        start_hour INTEGER,
        start_minute INTEGER,

        end_time TIMESTAMP,
        end_year INTEGER,
        end_month INTEGER,
        end_day INTEGER,
        end_hour INTEGER,
        end_minute INTEGER,

        weather_timestamp TIMESTAMP,
        weather_year INTEGER,
        weather_month INTEGER,
        weather_day INTEGER,
        weather_hour INTEGER,
        weather_minute INTEGER,

        start_lat DOUBLE PRECISION,
        start_lng DOUBLE PRECISION,
        end_lat DOUBLE PRECISION,
        end_lng DOUBLE PRECISION,

        distance_km DOUBLE PRECISION,
        visibility_km DOUBLE PRECISION,

        description TEXT,
        street TEXT,
        city TEXT,
        county TEXT,
        state TEXT,
        zipcode TEXT,
        country TEXT,
        timezone TEXT,
        airport_code TEXT,

        temperature_f DOUBLE PRECISION,
        wind_chill_f DOUBLE PRECISION,
        humidity_pct DOUBLE PRECISION,
        pressure_in DOUBLE PRECISION,
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


def transform_data(conn):
    truncate_query = f"TRUNCATE TABLE {TARGET_TABLE};"

    insert_query = f"""
    INSERT INTO {TARGET_TABLE} (
        id,
        source,
        severity,

        start_time,
        start_year,
        start_month,
        start_day,
        start_hour,
        start_minute,

        end_time,
        end_year,
        end_month,
        end_day,
        end_hour,
        end_minute,

        weather_timestamp,
        weather_year,
        weather_month,
        weather_day,
        weather_hour,
        weather_minute,

        start_lat,
        start_lng,
        end_lat,
        end_lng,

        distance_km,
        visibility_km,

        description,
        street,
        city,
        county,
        state,
        zipcode,
        country,
        timezone,
        airport_code,

        temperature_f,
        wind_chill_f,
        humidity_pct,
        pressure_in,
        wind_direction,
        wind_speed_mph,
        precipitation_in,
        weather_condition,

        amenity,
        bump,
        crossing,
        give_way,
        junction,
        no_exit,
        railway,
        roundabout,
        station,
        stop,
        traffic_calming,
        traffic_signal,
        turning_loop,

        sunrise_sunset,
        civil_twilight,
        nautical_twilight,
        astronomical_twilight
    )
    SELECT
        id,
        source,
        severity,

        start_time,
        EXTRACT(YEAR FROM start_time)::INTEGER,
        EXTRACT(MONTH FROM start_time)::INTEGER,
        EXTRACT(DAY FROM start_time)::INTEGER,
        EXTRACT(HOUR FROM start_time)::INTEGER,
        EXTRACT(MINUTE FROM start_time)::INTEGER,

        end_time,
        EXTRACT(YEAR FROM end_time)::INTEGER,
        EXTRACT(MONTH FROM end_time)::INTEGER,
        EXTRACT(DAY FROM end_time)::INTEGER,
        EXTRACT(HOUR FROM end_time)::INTEGER,
        EXTRACT(MINUTE FROM end_time)::INTEGER,

        weather_timestamp,
        EXTRACT(YEAR FROM weather_timestamp)::INTEGER,
        EXTRACT(MONTH FROM weather_timestamp)::INTEGER,
        EXTRACT(DAY FROM weather_timestamp)::INTEGER,
        EXTRACT(HOUR FROM weather_timestamp)::INTEGER,
        EXTRACT(MINUTE FROM weather_timestamp)::INTEGER,

        start_lat,
        start_lng,
        end_lat,
        end_lng,

        CASE
            WHEN distance_mi IS NOT NULL THEN ROUND((distance_mi * {MI_TO_KM})::numeric, 3)::double precision
            ELSE NULL
        END,

        CASE
            WHEN visibility_mi IS NOT NULL THEN ROUND((visibility_mi * {MI_TO_KM})::numeric, 3)::double precision
            ELSE NULL
        END,

        description,
        street,
        city,
        county,
        state,
        zipcode,
        country,
        timezone,
        airport_code,

        temperature_f,
        wind_chill_f,
        humidity_pct,
        pressure_in,
        wind_direction,
        wind_speed_mph,
        precipitation_in,
        weather_condition,

        amenity,
        bump,
        crossing,
        give_way,
        junction,
        no_exit,
        railway,
        roundabout,
        station,
        stop,
        traffic_calming,
        traffic_signal,
        turning_loop,

        sunrise_sunset,
        civil_twilight,
        nautical_twilight,
        astronomical_twilight
    FROM {SOURCE_TABLE};
    """

    count_query = f"SELECT COUNT(*) FROM {TARGET_TABLE};"

    with conn.cursor() as cur:
        cur.execute(truncate_query)
        cur.execute(insert_query)
        cur.execute(count_query)
        count = cur.fetchone()[0]

    conn.commit()
    print(f"Transformed {count} rows into {TARGET_TABLE}.")

def wait_for_source_table(conn, retries=60, delay=5):
    for attempt in range(1, retries + 1):
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = %s
                    );
                """, (SOURCE_TABLE,))
                table_exists = cur.fetchone()[0]

                if table_exists:
                    cur.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE};")
                    row_count = cur.fetchone()[0]
                    print(f"Source table {SOURCE_TABLE} exists with {row_count} rows.")
                    return

            print(f"Waiting for source table {SOURCE_TABLE} ({attempt}/{retries})...")
            time.sleep(delay)
        except Exception as e:
            print(f"Error while waiting for source table ({attempt}/{retries}): {e}")
            time.sleep(delay)

    raise RuntimeError(f"Source table {SOURCE_TABLE} was not ready in time.")

def main():
    conn = get_connection()
    try:
        wait_for_source_table(conn)
        create_target_table(conn)
        transform_data(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()