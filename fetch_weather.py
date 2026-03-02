import requests
import datetime
from config import OPENWEATHER_API_KEY, CITY_LIST


def fetch_current_weather(city_name):
    """Fetch current weather from OpenWeather API for a given city. Returns raw JSON."""

    # Geocode city name to coordinates
    geo_url = (
        f"https://api.openweathermap.org/geo/1.0/direct"
        f"?q={city_name}&limit=1&appid={OPENWEATHER_API_KEY}"
    )
    response = requests.get(geo_url)

    if response.status_code != 200 or len(response.json()) == 0:
        raise Exception(f"Geocoding failed for {city_name}: HTTP {response.status_code}")

    city_data = response.json()[0]
    lat = city_data["lat"]
    lon = city_data["lon"]

    # Fetch weather using coordinates
    weather_url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    weather_response = requests.get(weather_url)

    if weather_response.status_code != 200:
        raise Exception(f"Weather fetch failed for {city_name}: HTTP {weather_response.status_code}")

    data = weather_response.json()
    data["updatedAt"] = datetime.datetime.utcnow()
    return data


def fetch_all_cities():
    """Fetch weather for all configured cities. Returns list of (city, data) tuples."""
    results = []
    for city in CITY_LIST:
        try:
            data = fetch_current_weather(city)
            results.append((city, data))
            print(f"[OK] Fetched weather for {city}")
        except Exception as e:
            print(f"[FAIL] {city} -> {e}")
    return results


if __name__ == "__main__":
    results = fetch_all_cities()
    for city, data in results:
        print(f"  {city}: {data['main']['temp']}°C, {data['weather'][0]['description']}")
