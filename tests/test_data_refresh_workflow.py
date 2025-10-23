import os
import pytest
from pathlib import Path

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from components.api_gnews_fetcher import GNewsFetcher
from components.api_weather_fetcher import WeatherFetcher
from pipeline.data_refresh_workflow import save_snapshot, ensure_dir

# ----------- GNews Tests ------------

def test_gnews_fetcher_live():
    fetcher = GNewsFetcher()
    articles = fetcher.fetch_news("Mumbai", max_results=2)
    assert isinstance(articles, list)
    
    for article in articles:
        assert "title" in article

def test_gnews_fetcher_error(monkeypatch):
    # Simulate an API error
    def fake_get(*args, **kwargs):
        class FakeResponse:
            status_code = 401
            text = "Unauthorized"
            def raise_for_status(self): raise Exception("Unauthorized")
            def json(self): return {}
        return FakeResponse()
    monkeypatch.setattr("requests.get", fake_get)
    fetcher = GNewsFetcher(api_key="BAD_KEY")
    result = fetcher.fetch_news("Mumbai")
    assert result == []

# ----------- Weather Tests ------------

def test_weather_fetcher_live():
    fetcher = WeatherFetcher()
    # Use public Mumbai coordinates
    result = fetcher.fetch_weather(19.0760, 72.8777)
    assert result is not None
    assert "data" in result

def test_weather_fetcher_error(monkeypatch):
    def fake_get(*args, **kwargs):
        class FakeResponse:
            status_code = 401
            text = "Unauthorized"
            def raise_for_status(self): raise Exception("Unauthorized")
            def json(self): return {}
        return FakeResponse()
    monkeypatch.setattr("requests.get", fake_get)
    fetcher = WeatherFetcher(api_key="BAD_KEY")
    result = fetcher.fetch_weather(19.0760, 72.8777)
    assert result is None

# ----------- Snapshot Save ------------

def test_snapshot_creation(tmp_path):
    sample_data = {"foo": "bar"}
    save_snapshot(sample_data, str(tmp_path), "testprefix", "testregion")
    found = list(tmp_path.glob("*.json"))
    assert len(found) == 1
    # Content check
    import json
    with open(found[0], "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data == sample_data

# ----------- Directory Creation ------------

def test_ensure_dir(tmp_path):
    from src.pipeline.data_refresh_workflow import ensure_dir
    test_dir = tmp_path / "test_subfolder"
    ensure_dir(str(test_dir))
    assert os.path.exists(test_dir)

# ----------- Config usage ------------

def test_config_import():
    from src.config.config import API_CONFIG
    assert "regions" in API_CONFIG
    assert isinstance(API_CONFIG["regions"], list)
