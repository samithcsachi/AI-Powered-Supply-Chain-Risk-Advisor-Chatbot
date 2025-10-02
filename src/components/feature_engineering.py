import pandas as pd
import numpy as np
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.logger import *

import logging
logger = logging.getLogger(__name__)

# === FEATURE ENGINEERING FOR NEWS EVENTS ===

def engineer_news_features(df):
    """
    Adds event type flags and temporal fields for supply chain risk news articles.
    """
    KEYWORDS = {
        "strike": ["strike", "walkout", "protest"],
        "disaster": ["earthquake", "flood", "hurricane", "typhoon", "fire", "storm"],
        "accident": ["collision", "accident", "spill", "blockage"],
    }
    for key, words in KEYWORDS.items():
        df[f"is_{key}"] = (
            df['title'].str.contains('|'.join(words), case=False, na=False) |
            df['description'].str.contains('|'.join(words), case=False, na=False)
        )
    # Temporal features
    if "publishedAt" in df.columns:
        df["event_weekday"] = pd.to_datetime(df["publishedAt"], errors='coerce').dt.weekday
        df["event_hour"] = pd.to_datetime(df["publishedAt"], errors='coerce').dt.hour
    logger.info(f"Engineered news event features: {df.shape}")
    return df

# === FEATURE ENGINEERING FOR WEATHER ALERTS ===

def engineer_weather_features(df):
    """
    Adds flags and one-hot encodings for severe weather risk indicators.
    """
    severe_words = ["Storm", "Thunderstorm", "Rain", "Snow", "Hurricane", "Extreme"]
    df["severe_weather"] = df["weather_main"].str.contains('|'.join(severe_words), case=False, na=False)
    # One-hot encoding for weather type
    if "weather_main" in df.columns:
        df = pd.get_dummies(df, columns=["weather_main"], prefix="weather")
    # Add month/season features
    if "timestamp" in df.columns:
        df["month"] = pd.to_datetime(df["timestamp"], errors='coerce').dt.month
        df["season"] = pd.to_datetime(df["timestamp"], errors='coerce').dt.month % 12 // 3 + 1
    logger.info(f"Engineered weather features: {df.shape}")
    return df

# === FEATURE ENGINEERING FOR SUPPLY CHAIN CSV ===

def engineer_supply_chain_features(df):
    """
    Adds lead time, one-hot encodings, and risk flags for supply chain records.
    """
    # Lead time
    if "order date (DateOrders)" in df.columns and "shipping date (DateOrders)" in df.columns:
        df["lead_time_days"] = (
            pd.to_datetime(df["shipping date (DateOrders)"], errors='coerce') - 
            pd.to_datetime(df["order date (DateOrders)"], errors='coerce')
        ).dt.days
    # One-hot encode key categorical columns
    for col in ["Order Status", "Product Status", "Shipping Mode", "Order Region", "Order Country"]:
        if col in df.columns:
            df = pd.get_dummies(df, columns=[col], prefix=col.replace(' ', '_'))
    # Late delivery flag
    if "Late_delivery_risk" in df.columns:
        df["is_late"] = df["Late_delivery_risk"] > 0
    logger.info(f"Engineered supply chain features: {df.shape}")
    return df

# ==== MAIN PIPELINE ====

if __name__ == "__main__":
    processed_dir = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "processed"
    # Feature engineer news
    try:
        news_df = pd.read_csv(processed_dir / "news_events_clean.csv")
        news_feats = engineer_news_features(news_df)
        news_feats.to_csv(processed_dir / "news_events_features.csv", index=False)
        logger.info("Saved engineered news features.")
    except Exception as e:
        logger.error(f"Error engineering news features: {e}")
    # Feature engineer weather
    try:
        weather_df = pd.read_csv(processed_dir / "weather_alerts_clean.csv")
        weather_feats = engineer_weather_features(weather_df)
        weather_feats.to_csv(processed_dir / "weather_alerts_features.csv", index=False)
        logger.info("Saved engineered weather features.")
    except Exception as e:
        logger.error(f"Error engineering weather features: {e}")
    # Feature engineer supply chain
    try:
        sc_df = pd.read_csv(processed_dir / "supply_chain_disruptions_clean.csv", encoding="utf-8")
        sc_feats = engineer_supply_chain_features(sc_df)
        sc_feats.to_csv(processed_dir / "supply_chain_disruptions_features.csv", index=False)
        logger.info("Saved engineered supply chain features.")
    except Exception as e:
        logger.error(f"Error engineering supply chain features: {e}")
