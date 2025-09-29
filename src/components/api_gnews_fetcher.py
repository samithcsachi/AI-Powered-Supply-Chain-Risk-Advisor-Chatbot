import os
import requests
import json
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")

KEYWORDS = [
    "supply chain disruption",
    "port strike",
    "rail strike",
    "logistics delay",
    "weather disruption"
]

GNEWS_API_ENDPOINT = "https://gnews.io/api/v4/search"

def fetch_news(keyword):
    params = {
        'q': keyword,
        'token': GNEWS_API_KEY,
        'lang': 'en',
        'max': 100,
    }
    response = requests.get(GNEWS_API_ENDPOINT, params=params)
    if response.status_code == 200:
        return response.json().get('articles', [])
    else:
        print(f"Error fetching {keyword}: {response.status_code} - {response.text}")
        return []

def main():
    all_articles = []
    for kw in KEYWORDS:
        print(f"Fetching news for keyword: '{kw}'")
        articles = fetch_news(kw)
        for article in articles:
            all_articles.append({
                'title': article.get('title'),
                'description': article.get('description'),
                'publishedAt': article.get('publishedAt'),
                'source': article.get('source', {}).get('name'),
                'url': article.get('url'),
                'keyword': kw
            })
   
    base_path = Path(__file__).resolve().parent.parent.parent / "artifacts" / "data" / "raw"
    base_path.mkdir(parents=True, exist_ok=True)
    output_path = base_path / "news_events.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(all_articles)} articles to {output_path}")

if __name__ == "__main__":
    if not GNEWS_API_KEY:
        print("ERROR: GNEWS_API_KEY environment variable not set.")
        exit(1)
    main()
