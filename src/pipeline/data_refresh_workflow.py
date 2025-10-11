import os
from datetime import datetime

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

from components.api_gnews_fetcher import GNewsFetcher
from components.api_weather_fetcher import WeatherFetcher
from config.config import API_CONFIG
from utils.logger import *

import logging
logger = logging.getLogger(__name__)

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
        logger.info(f"Directory created: {path}")

def save_snapshot(data, folder, prefix, region):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{prefix}_{region.replace(' ', '_')}_{timestamp}.json"
    fpath = os.path.join(folder, fname)
    with open(fpath, "w", encoding="utf-8") as f:
        import json
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Snapshot saved: {fpath}")

def refresh_gnews(regions, out_dir):
    fetcher = GNewsFetcher()
    ensure_dir(out_dir)
    for region in regions:
        try:
            news_data = fetcher.fetch_news(region)
            save_snapshot(news_data, out_dir, "gnews", region)
            logger.info(f"GNews data for {region} saved.")
        except Exception as e:
            logger.error(f"Error fetching GNews for {region}: {e}")

def refresh_weather(weather_regions, out_dir):
    fetcher = WeatherFetcher()
    ensure_dir(out_dir)
    for loc in weather_regions:
        try:
            weather_data = fetcher.fetch_weather(loc["lat"], loc["lon"])
            save_snapshot(weather_data, out_dir, "weather", loc["city"])
            logger.info(f"Weather data for {loc['city']} saved.")
        except Exception as e:
            logger.error(f"Error fetching Weather for {loc['city']}: {e}")

def run_all():
    logger.info("Starting data refresh workflow...")
    regions = API_CONFIG['regions']              # For GNews
    weather_regions = API_CONFIG['weather_regions'] # For WeatherBit
    news_dir = API_CONFIG['news_output_dir']
    weather_dir = API_CONFIG['weather_output_dir']

    refresh_gnews(regions, news_dir)
    refresh_weather(weather_regions, weather_dir)
    logger.info("All data refreshes complete.")


if __name__ == "__main__":
    run_all()
