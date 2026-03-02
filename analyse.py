import duckdb
import pandas as pd
from config import DUCKDB_PATH


def load_dataframes(con):
    """Load all warehouse tables into pandas DataFrames."""
    cities = con.execute("SELECT * FROM dim_city").fetchdf()
    conditions = con.execute("SELECT * FROM dim_weather_condition").fetchdf()
    readings = con.execute("""
        SELECT
            f.*,
            c.city_name,
            c.country,
            w.main AS weather_main,
            w.description AS weather_desc
        FROM fact_weather_readings f
        JOIN dim_city c ON f.city_id = c.city_id
        JOIN dim_weather_condition w ON f.condition_id = w.condition_id
    """).fetchdf()
    return cities, conditions, readings


def city_overview(cities):
    """Display city dimension summary."""
    print("=" * 60)
    print("CITY OVERVIEW")
    print("=" * 60)
    print(cities[["city_name", "country", "latitude", "longitude"]].to_string(index=False))
    print()


def temperature_summary(readings):
    """Temperature stats per city."""
    print("=" * 60)
    print("TEMPERATURE SUMMARY (Celsius)")
    print("=" * 60)
    stats = readings.groupby("city_name")["temperature"].agg(
        ["count", "mean", "min", "max", "std"]
    ).round(2)
    stats.columns = ["readings", "avg_temp", "min_temp", "max_temp", "std_dev"]
    print(stats.to_string())
    print()


def weather_conditions_breakdown(readings):
    """Frequency of weather conditions per city."""
    print("=" * 60)
    print("WEATHER CONDITIONS BREAKDOWN")
    print("=" * 60)
    breakdown = readings.groupby(["city_name", "weather_desc"]).size().reset_index(name="count")
    breakdown = breakdown.sort_values(["city_name", "count"], ascending=[True, False])
    print(breakdown.to_string(index=False))
    print()


def wind_analysis(readings):
    """Wind speed stats per city."""
    print("=" * 60)
    print("WIND ANALYSIS")
    print("=" * 60)
    wind = readings.groupby("city_name").agg(
        avg_wind=("wind_speed", "mean"),
        max_wind=("wind_speed", "max"),
        avg_gust=("wind_gust", "mean"),
    ).round(2)
    print(wind.to_string())
    print()


def humidity_pressure(readings):
    """Humidity and pressure averages per city."""
    print("=" * 60)
    print("HUMIDITY & PRESSURE")
    print("=" * 60)
    hp = readings.groupby("city_name").agg(
        avg_humidity=("humidity", "mean"),
        avg_pressure=("pressure", "mean"),
        avg_visibility=("visibility", "mean"),
    ).round(1)
    print(hp.to_string())
    print()


def latest_readings(readings):
    """Most recent reading per city."""
    print("=" * 60)
    print("LATEST READINGS")
    print("=" * 60)
    latest = readings.sort_values("reading_time", ascending=False).groupby("city_name").first()
    cols = ["temperature", "feels_like", "humidity", "weather_desc", "wind_speed", "reading_time"]
    print(latest[cols].to_string())
    print()


def extreme_conditions(readings):
    """Flag readings with extreme weather values."""
    print("=" * 60)
    print("EXTREME CONDITIONS FLAGS")
    print("=" * 60)

    high_temp = readings[readings["temperature"] > 35]
    high_wind = readings[readings["wind_speed"] > 10]
    low_visibility = readings[readings["visibility"] < 3000]

    print(f"  High temperature (>35C):   {len(high_temp)} readings")
    print(f"  High wind speed (>10 m/s): {len(high_wind)} readings")
    print(f"  Low visibility (<3km):     {len(low_visibility)} readings")

    if not high_temp.empty:
        print("\n  High temp events:")
        print(high_temp[["city_name", "temperature", "reading_time"]].to_string(index=False))
    if not high_wind.empty:
        print("\n  High wind events:")
        print(high_wind[["city_name", "wind_speed", "reading_time"]].to_string(index=False))
    if not low_visibility.empty:
        print("\n  Low visibility events:")
        print(low_visibility[["city_name", "visibility", "reading_time"]].to_string(index=False))
    print()


def run_analysis():
    """Run all analytics on the warehouse."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    try:
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        required = {"dim_city", "dim_weather_condition", "fact_weather_readings"}
        if not required.issubset(set(tables)):
            print(f"[WARN] Missing tables. Found: {tables}")
            print("       Run transform.py first to populate the warehouse.")
            return

        cities, conditions, readings = load_dataframes(con)

        if readings.empty:
            print("[WARN] No weather readings found. Run the pipeline first.")
            return

        print(f"\nAnalyzing {len(readings)} weather readings across {len(cities)} cities\n")

        city_overview(cities)
        temperature_summary(readings)
        weather_conditions_breakdown(readings)
        wind_analysis(readings)
        humidity_pressure(readings)
        latest_readings(readings)
        extreme_conditions(readings)

        print("=" * 60)
        print("ANALYSIS COMPLETE")
        print("=" * 60)

    finally:
        con.close()


if __name__ == "__main__":
    run_analysis()
