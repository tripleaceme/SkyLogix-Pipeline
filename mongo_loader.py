import pymongo
from config import MONGO_URI, MONGO_DB
from fetch_weather import fetch_all_cities


def get_collection():
    """Connect to MongoDB and return the weather_raw collection."""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return client, db["weather_raw"]


def upsert_weather(collection, data):
    """Upsert a single weather record using the city id as the key."""
    collection.update_one(
        {"id": data["id"]},
        {"$set": data},
        upsert=True
    )
    return str(data["id"])


def load_to_mongo():
    """Fetch weather for all cities and load into MongoDB."""
    client, collection = get_collection()

    try:
        results = fetch_all_cities()
        for city, data in results:
            try:
                mongo_id = upsert_weather(collection, data)
                print(f"[OK] Upserted {city} to MongoDB (id: {mongo_id})")
            except Exception as e:
                print(f"[FAIL] MongoDB upsert for {city} -> {e}")
    finally:
        client.close()


if __name__ == "__main__":
    load_to_mongo()
