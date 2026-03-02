import os
from dotenv import load_dotenv


load_dotenv(override=True)


OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
CITY_LIST = [c.strip() for c in os.getenv("CITY_LIST", "Lagos").split(",")]
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./analytics.duckdb")
AIRBYTE_RAW_PATH = os.getenv("AIRBYTE_RAW_PATH", "/tmp/airbyte_local/weather_raw.duckdb")
