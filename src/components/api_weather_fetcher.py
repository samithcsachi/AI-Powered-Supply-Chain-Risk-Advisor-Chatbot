import os
import requests
import json
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

WEATHERBIT_API_KEY = os.getenv("WEATHERBIT_API_KEY")

if not WEATHERBIT_API_KEY:
    print("ERROR: WEATHERBIT_API_KEY not set. Add it to your .env file or environment variables.")
    exit(1)

CITIES = [
    {"city": "Shanghai", "country": "China", "lat": 31.2304, "lon": 121.4737},
    {"city": "Rotterdam", "country": "Netherlands", "lat": 51.9225, "lon": 4.47917},
    {"city": "Los Angeles", "country": "USA", "lat": 34.0522, "lon": -118.2437},
    {"city": "Mumbai", "country": "India", "lat": 19.0760, "lon": 72.8777},
    {"city": "Singapore", "country": "Singapore", "lat": 1.3521, "lon": 103.8198},
    {"city": "Hamburg", "country": "Germany", "lat": 53.5511, "lon": 9.9937},
    {"city": "Dubai", "country": "UAE", "lat": 25.276987, "lon": 55.296249},
    {"city": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060},
]

ENDPOINT = "https://api.weatherbit.io/v2.0/current"

def fetch_weather(lat, lon):
    params = {
        "lat": lat,
        "lon": lon,
        "key": WEATHERBIT_API_KEY
    }
    response = requests.get(ENDPOINT, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def extract_weather(data, loc):
 
    if data and "data" in data and len(data["data"]) > 0:
        entry = data["data"][0]
        return {
            "city": loc["city"],
            "country": loc["country"],
            "lat": loc["lat"],
            "lon": loc["lon"],
            "timestamp": entry.get("ts"),
            "datetime": entry.get("datetime"),
            "temp": entry.get("temp"),
            "weather_main": entry["weather"].get("description"),
            "weather_code": entry["weather"].get("code"),
            "precip": entry.get("precip"),
            "wind_spd": entry.get("wind_spd"),
            "wind_dir": entry.get("wind_cdir_full"),
            "clouds": entry.get("clouds"),
            "aqi": entry.get("aqi", None),
            "visibility": entry.get("vis"),
            "alert": "Yes" if entry["weather"].get("code", 800) >= 700 else "No"
        }
    else:
        return None

def main():
    weather_reports = []
    for loc in CITIES:
        print(f"Fetching weather for {loc['city']}, {loc['country']}")
        data = fetch_weather(loc['lat'], loc['lon'])
        report = extract_weather(data, loc)
        if report:
            weather_reports.append(report)
    # Save output
    output_dir = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "weather_alerts.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(weather_reports, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(weather_reports)} weather records to {output_path}")

if __name__ == "__main__":
    main()
