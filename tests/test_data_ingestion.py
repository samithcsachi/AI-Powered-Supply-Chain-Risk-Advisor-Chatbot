import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from components.data_ingestion import load_news_events, load_weather_alerts, load_supply_chain_disruptions


def test_load_news_events():
    df = load_news_events()
    assert df.shape[0] > 0
    assert 'title' in df.columns

def test_load_weather_alerts():
    df = load_weather_alerts()
    assert df.shape[0] > 0
    assert 'city' in df.columns

def test_load_supply_chain_disruptions():
    df = load_supply_chain_disruptions()
    assert df.shape[0] > 0
    
    required_columns = [
        'Type', 'Delivery Status', 'Late_delivery_risk', 'Order Status', 
        'Customer Country', 'Order Country', 'Shipping Mode'
    ]
    for col in required_columns:
        assert col in df.columns, f"Missing column: {col}"
