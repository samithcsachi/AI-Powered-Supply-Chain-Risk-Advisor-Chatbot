import os
import requests
from dotenv import load_dotenv

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

import logging
logger = logging.getLogger(__name__)

load_dotenv()
WEATHERBIT_API_KEY = os.getenv("WEATHERBIT_API_KEY")
ENDPOINT = "https://api.weatherbit.io/v2.0/current"

class WeatherFetcher:
    def __init__(self, api_key=WEATHERBIT_API_KEY, endpoint=ENDPOINT):
        self.api_key = api_key
        self.endpoint = endpoint
        if not self.api_key:
            logger.error("WEATHERBIT_API_KEY environment variable not set.")

    def fetch_weather(self, lat, lon):
        params = {
            "lat": lat,
            "lon": lon,
            "key": self.api_key
        }
        try:
            logger.info(f"Fetching weather for lat/lon: {lat},{lon}")
            response = requests.get(self.endpoint, params=params)
            response.raise_for_status()
            logger.info(f"Weather fetch success for {lat},{lon}")
            return response.json()
        except Exception as e:
            logger.error(f"WeatherBit fetch error for {lat},{lon}: {e}")
            return None

    @staticmethod
    def extract_weather(data, loc):
        if data and "data" in data and len(data["data"]) > 0:
            entry = data["data"][0]
            logger.info(f"Extracting weather for {loc['city']}, {loc['country']}")
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
            logger.warning("No valid weather data structure to extract.")
            return None
