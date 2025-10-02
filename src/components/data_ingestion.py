import json
import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.logger import *

import logging
logger = logging.getLogger(__name__)

def load_news_events(path=None):
    if path is None:
        path = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "raw" / "news_events.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = pd.DataFrame(json.load(f))
        logger.info(f"News events loaded successfully: {data.shape}")
        return data
    except Exception as e:
        logger.error(f"Failed to load news events: {e}")
        raise

def load_weather_alerts(path=None):
    if path is None:
        path = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "raw" / "weather_alerts.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = pd.DataFrame(json.load(f))
        logger.info(f"Weather alerts loaded successfully: {data.shape}")
        return data
    except Exception as e:
        logger.error(f"Failed to load weather alerts: {e}")
        raise

def load_supply_chain_disruptions(csv_path=None):
    if csv_path is None:
        csv_path = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "raw" / "DataCoSupplyChainDataset.csv"
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
        logger.info(f"Historic incidents loaded successfully: {df.shape}")
        return df
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="ISO-8859-1")
        logger.info(f"Historic incidents loaded successfully (ISO-8859-1): {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Failed to load historic supply chain CSV: {e}")
        raise

if __name__ == "__main__":
    try:
        news_df = load_news_events()
        weather_df = load_weather_alerts()
        try:
            incidents_df = load_supply_chain_disruptions()
        except Exception as e:
            logger.error(f"No historic CSV loaded: {e}")
    except Exception as e:
        logger.error(f"Major error in data ingestion: {e}")
