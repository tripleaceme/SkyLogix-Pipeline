import requests
import pymongo
import datetime
import psycopg2
from config import (
    OPENWEATHER_API_KEY,
    MONGO_URI,
    CITY_LIST,
    PG_HOST,
    PG_PORT,
    PG_USER,
    PG_PASSWORD,
    PG_DATABASE
)

# MongoDB Client
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["weather_db"]
collection = db["weather_raw"]

# PostgreSQL Connection
pg_conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    user=PG_USER,
    password=PG_PASSWORD,
    database=PG_DATABASE
)
pg_conn.autocommit = True
pg_cursor = pg_conn.cursor()

def log_insert_history(city, mongo_id, status, error_message=None):
    query = """
        INSERT INTO insert_history (city, inserted_at, mongo_id, status, error_message)
        VALUES (%s, NOW(), %s, %s, %s)
    """
    pg_cursor.execute(query, (city, mongo_id, status, error_message))


def fetch_weather(city):
    url = (
        f"https://api.openweathermap.org/data/2.5/weather?q={city}"
        f"&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    data["city"] = city
    data["updatedAt"] = datetime.datetime.utcnow()
    return data


def upsert_weather_record(data):
    result = collection.update_one(
        {"id": data["id"]},
        {"$set": data},
        upsert=True
    )
    
    # Determine unique ID used
    mongo_id = str(data["id"])

    return mongo_id


def run_ingestor():
    for city in CITY_LIST:
        try:
            weather = fetch_weather(city)
            mongo_id = upsert_weather_record(weather)
            print(f"[✔] Upserted weather for {city}")

            # Log success to Postgres
            log_insert_history(
                city=city,
                mongo_id=mongo_id,
                status="success",
                error_message=None
            )

        except Exception as e:
            print(f"[✖] Failed for {city} → {e}")

            # Log failure to Postgres
            log_insert_history(
                city=city,
                mongo_id=None,
                status="failed",
                error_message=str(e)
            )


if __name__ == "__main__":
    run_ingestor()
