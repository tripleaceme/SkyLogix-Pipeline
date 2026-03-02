import duckdb
import os
from config import DUCKDB_PATH, AIRBYTE_RAW_PATH

# Airbyte wraps all synced data inside _airbyte_data as a JSON string.
# Table: _airbyte_raw_weather_raw
# Columns: _airbyte_ab_id, _airbyte_emitted_at, _airbyte_data (JSON string)
RAW_TABLE = "_airbyte_raw_weather_raw"


def run_transform():
    """Transform Airbyte raw weather data into a star schema in the analytics warehouse."""

    if not os.path.exists(AIRBYTE_RAW_PATH):
        print(f"[WARN] Airbyte raw file not found at {AIRBYTE_RAW_PATH}")
        print("       Run an Airbyte sync (MongoDB -> DuckDB) first.")
        return

    con = duckdb.connect(DUCKDB_PATH)

    try:
        con.execute(f"ATTACH '{AIRBYTE_RAW_PATH}' AS raw (READ_ONLY)")

        raw_count = con.execute(f'SELECT COUNT(*) FROM raw."{RAW_TABLE}"').fetchone()[0]
        print(f"[OK] Raw source: {raw_count} records\n")

        # --- Dimension: Cities ---
        con.execute("""
            CREATE TABLE IF NOT EXISTS dim_city (
                city_id BIGINT PRIMARY KEY,
                city_name TEXT,
                latitude DOUBLE,
                longitude DOUBLE,
                country TEXT,
                timezone_offset INTEGER
            )
        """)
        con.execute(f"""
            INSERT OR REPLACE INTO dim_city
            SELECT DISTINCT
                (json_extract_string(_airbyte_data, '$.id'))::BIGINT           AS city_id,
                json_extract_string(_airbyte_data, '$.name')                   AS city_name,
                (json_extract_string(_airbyte_data, '$.coord.lat'))::DOUBLE    AS latitude,
                (json_extract_string(_airbyte_data, '$.coord.lon'))::DOUBLE    AS longitude,
                json_extract_string(_airbyte_data, '$.sys.country')            AS country,
                (json_extract_string(_airbyte_data, '$.timezone'))::INTEGER    AS timezone_offset
            FROM raw."{RAW_TABLE}"
        """)
        print(f"[OK] dim_city: {con.execute('SELECT COUNT(*) FROM dim_city').fetchone()[0]} rows")

        # --- Dimension: Weather Conditions ---
        con.execute("""
            CREATE TABLE IF NOT EXISTS dim_weather_condition (
                condition_id INTEGER PRIMARY KEY,
                main TEXT,
                description TEXT,
                icon TEXT
            )
        """)
        con.execute(f"""
            INSERT OR REPLACE INTO dim_weather_condition
            SELECT DISTINCT
                (json_extract_string(_airbyte_data, '$.weather[0].id'))::INTEGER          AS condition_id,
                json_extract_string(_airbyte_data, '$.weather[0].main')                   AS main,
                json_extract_string(_airbyte_data, '$.weather[0].description')            AS description,
                json_extract_string(_airbyte_data, '$.weather[0].icon')                   AS icon
            FROM raw."{RAW_TABLE}"
        """)
        print(f"[OK] dim_weather_condition: {con.execute('SELECT COUNT(*) FROM dim_weather_condition').fetchone()[0]} rows")

        # --- Fact: Weather Readings ---
        con.execute("""
            CREATE TABLE IF NOT EXISTS fact_weather_readings (
                reading_id UUID DEFAULT uuid() PRIMARY KEY,
                reading_time TIMESTAMP,
                updated_at TIMESTAMP,
                city_id BIGINT,
                condition_id INTEGER,
                temperature DOUBLE,
                feels_like DOUBLE,
                temp_min DOUBLE,
                temp_max DOUBLE,
                humidity INTEGER,
                pressure INTEGER,
                sea_level INTEGER,
                ground_level INTEGER,
                wind_speed DOUBLE,
                wind_deg INTEGER,
                wind_gust DOUBLE,
                visibility INTEGER,
                cloud_coverage INTEGER
            )
        """)
        con.execute(f"""
            INSERT INTO fact_weather_readings (
                reading_time, updated_at, city_id, condition_id,
                temperature, feels_like, temp_min, temp_max,
                humidity, pressure, sea_level, ground_level,
                wind_speed, wind_deg, wind_gust, visibility, cloud_coverage
            )
            SELECT
                TO_TIMESTAMP((json_extract_string(_airbyte_data, '$.dt'))::BIGINT)       AS reading_time,
                (json_extract_string(_airbyte_data, '$.updatedAt'))::TIMESTAMP           AS updated_at,
                (json_extract_string(_airbyte_data, '$.id'))::BIGINT                     AS city_id,
                (json_extract_string(_airbyte_data, '$.weather[0].id'))::INTEGER         AS condition_id,
                (json_extract_string(_airbyte_data, '$.main.temp'))::DOUBLE              AS temperature,
                (json_extract_string(_airbyte_data, '$.main.feels_like'))::DOUBLE        AS feels_like,
                (json_extract_string(_airbyte_data, '$.main.temp_min'))::DOUBLE          AS temp_min,
                (json_extract_string(_airbyte_data, '$.main.temp_max'))::DOUBLE          AS temp_max,
                (json_extract_string(_airbyte_data, '$.main.humidity'))::INTEGER         AS humidity,
                (json_extract_string(_airbyte_data, '$.main.pressure'))::INTEGER         AS pressure,
                (json_extract_string(_airbyte_data, '$.main.sea_level'))::INTEGER        AS sea_level,
                (json_extract_string(_airbyte_data, '$.main.grnd_level'))::INTEGER       AS ground_level,
                (json_extract_string(_airbyte_data, '$.wind.speed'))::DOUBLE             AS wind_speed,
                (json_extract_string(_airbyte_data, '$.wind.deg'))::INTEGER              AS wind_deg,
                (json_extract_string(_airbyte_data, '$.wind.gust'))::DOUBLE              AS wind_gust,
                (json_extract_string(_airbyte_data, '$.visibility'))::INTEGER            AS visibility,
                (json_extract_string(_airbyte_data, '$.clouds.all'))::INTEGER            AS cloud_coverage
            FROM raw."{RAW_TABLE}" r
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_weather_readings f
                WHERE f.city_id = (json_extract_string(r._airbyte_data, '$.id'))::BIGINT
                  AND f.updated_at = (json_extract_string(r._airbyte_data, '$.updatedAt'))::TIMESTAMP
            )
        """)
        new_count = con.execute("SELECT COUNT(*) FROM fact_weather_readings").fetchone()[0]
        print(f"[OK] fact_weather_readings: {new_count} total rows")

        print("\n[DONE] Transform complete.")

    finally:
        con.close()


if __name__ == "__main__":
    run_transform()
