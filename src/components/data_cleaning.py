import pandas as pd
import numpy as np
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.logger import *

import logging
logger = logging.getLogger(__name__)



# === NEWS DATA CLEANING ===

def clean_news_events(df):
    required_columns = ['title', 'publishedAt', 'description', 'source', 'url']
    df = df[[col for col in required_columns if col in df.columns]]
    df = df.drop_duplicates(subset=['title', 'publishedAt'])  # remove repeated news
    df['title'] = df['title'].str.strip().str.lower()
    df['description'] = df['description'].str.strip().str.lower()
    df['publishedAt'] = pd.to_datetime(df['publishedAt'], errors='coerce')
    df = df.dropna(subset=['title', 'publishedAt'])  # drop nulls in critical cols
    logger.info(f"Cleaned news events: {df.shape}")
    return df

# === WEATHER DATA CLEANING ===

def clean_weather_alerts(df):
    keep_cols = ['city', 'country', 'lat', 'lon', 'weather_main', 'timestamp']
    df = df[[col for col in keep_cols if col in df.columns]].copy()
    df['city'] = df['city'].str.strip().str.title()
    df['country'] = df['country'].str.strip().str.upper()
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
    df = df.dropna(subset=['city', 'timestamp'])
    logger.info(f"Cleaned weather alerts: {df.shape}")
    return df

# === SUPPLY CHAIN CSV CLEANING ===

def clean_supply_chain_disruptions(df):
    df = df.drop_duplicates()
   
    df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'], errors='coerce')
    df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'], errors='coerce')
    
    if 'Late_delivery_risk' in df.columns:
        df['Late_delivery_risk'] = df['Late_delivery_risk'].fillna(0).astype(int)

    if 'Order Status' in df.columns:
        df['Order Status'] = df['Order Status'].str.strip().str.title()
    logger.info(f"Cleaned supply chain CSV: {df.shape}")
    return df

# === ENTRY POINT FOR PIPELINE/TESTING ===

if __name__ == "__main__":
 
    artifacts = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "raw"
    
    # Load raw news
    try:
        news_df = pd.read_json(artifacts / "news_events.json")
        cleaned_news = clean_news_events(news_df)
        logger.info(f"WeNews ather Alerts cleaned successfully: shape {cleaned_news.shape}")
    except Exception as e:
        logger.error(f"Error cleaning news: {e}")


    try:
        weather_df = pd.read_json(artifacts / "weather_alerts.json")
        cleaned_weather = clean_weather_alerts(weather_df)
        logger.info(f"Weather Alerts cleaned successfully: shape {cleaned_weather.shape}")
    except Exception as e:
        logger.error(f"Error cleaning weather: {e}")

  
    try:
        try:
            sc_df = pd.read_csv(artifacts / "DataCoSupplyChainDataset.csv", encoding="utf-8")
        except UnicodeDecodeError:
            sc_df = pd.read_csv(artifacts / "DataCoSupplyChainDataset.csv", encoding="ISO-8859-1")
        cleaned_sc = clean_supply_chain_disruptions(sc_df)
        logger.info(f"Supply chain CSV cleaned successfully: shape {cleaned_sc.shape}")
    except Exception as e:
        logger.error(f"Error cleaning supply chain CSV: {e}")



processed_dir = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "processed"
processed_dir.mkdir(parents=True, exist_ok=True)

# Save cleaned datasets
cleaned_news.to_csv(processed_dir / "news_events_clean.csv", index=False)
cleaned_weather.to_csv(processed_dir / "weather_alerts_clean.csv", index=False)
cleaned_sc.to_csv(processed_dir / "supply_chain_disruptions_clean.csv", index=False)
