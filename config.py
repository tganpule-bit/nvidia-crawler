import os

# Database
DB_PATH = os.getenv("NVIDIA_CRAWLER_DB", "nvidia_chatter.db")

# Search terms
SEARCH_TERMS = ["NVDA", "nvidia", "nvidia stock"]
TWITTER_SEARCH_TERMS = ["$NVDA", "nvidia stock", "NVDA"]

# Reddit
SUBREDDITS = ["wallstreetbets", "stocks", "investing", "nvidia", "stockmarket"]
REDDIT_USER_AGENT = "NvidiaCrawler/1.0"

# Crawl interval in minutes
CRAWL_INTERVAL_MINUTES = int(os.getenv("CRAWL_INTERVAL", "720"))

# Optional API keys (for future upgrades)
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")

# Request settings
REQUEST_TIMEOUT = 15
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
