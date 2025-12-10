import os
from dotenv import load_dotenv


load_dotenv()


OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
CITY_LIST = [c.strip() for c in os.getenv("CITY_LIST", "Lagos").split(",")]
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT"))
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")

