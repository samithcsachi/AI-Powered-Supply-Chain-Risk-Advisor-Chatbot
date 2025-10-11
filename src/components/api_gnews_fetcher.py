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
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
GNEWS_API_ENDPOINT = "https://gnews.io/api/v4/search"

class GNewsFetcher:
    def __init__(self, api_key=GNEWS_API_KEY, endpoint=GNEWS_API_ENDPOINT):
        self.api_key = api_key
        self.endpoint = endpoint
        if not self.api_key:
            logger.error("GNEWS_API_KEY environment variable not set.")

    def fetch_news(self, keyword, max_results=100):
        params = {
            'q': keyword,
            'token': self.api_key,
            'lang': 'en',
            'max': max_results,
        }
        try:
            logger.info(f"Fetching GNews for keyword: {keyword}")
            response = requests.get(self.endpoint, params=params)
            response.raise_for_status()
            articles = response.json().get('articles', [])
            logger.info(f"Fetched {len(articles)} articles for '{keyword}'")
            return articles
        except Exception as e:
            logger.error(f"GNews fetch error for '{keyword}': {e}")
            return []
