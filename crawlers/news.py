import hashlib
import logging
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

import config
from crawlers.base import BaseCrawler

logger = logging.getLogger(__name__)


class NewsCrawler(BaseCrawler):
    name = "news"

    def crawl(self) -> list[dict]:
        posts = []
        posts.extend(self._google_news_rss())
        time.sleep(1)
        posts.extend(self._yahoo_finance())
        time.sleep(1)
        posts.extend(self._marketwatch())
        logger.info("News: fetched %d articles", len(posts))
        return posts

    def _google_news_rss(self) -> list[dict]:
        results = []
        for term in config.SEARCH_TERMS:
            url = f"https://news.google.com/rss/search?q={quote_plus(term)}&hl=en-US&gl=US&ceid=US:en"
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:20]:
                    pub_date = None
                    if hasattr(entry, "published"):
                        try:
                            pub_date = dateparser.parse(entry.published).isoformat()
                        except Exception:
                            pass

                    ext_id = hashlib.md5(entry.get("link", "").encode()).hexdigest()
                    results.append(
                        {
                            "source": "news",
                            "external_id": f"gnews_{ext_id}",
                            "title": entry.get("title", ""),
                            "content": entry.get("summary", "")[:2000],
                            "author": entry.get("source", {}).get("title", ""),
                            "url": entry.get("link", ""),
                            "subreddit": None,
                            "score": None,
                            "num_comments": None,
                            "sentiment": None,
                            "published_at": pub_date,
                        }
                    )
            except Exception as e:
                logger.warning("Google News RSS failed for %s: %s", term, e)
            time.sleep(0.5)
        return results

    def _yahoo_finance(self) -> list[dict]:
        url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=NVDA&region=US&lang=en-US"
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            logger.warning("Yahoo Finance RSS failed: %s", e)
            return []

        results = []
        for entry in feed.entries[:20]:
            pub_date = None
            if hasattr(entry, "published"):
                try:
                    pub_date = dateparser.parse(entry.published).isoformat()
                except Exception:
                    pass

            ext_id = entry.get("id", "") or entry.get("link", "")
            ext_id = hashlib.md5(ext_id.encode()).hexdigest()
            results.append(
                {
                    "source": "news",
                    "external_id": f"yahoo_{ext_id}",
                    "title": entry.get("title", ""),
                    "content": entry.get("summary", "")[:2000],
                    "author": "Yahoo Finance",
                    "url": entry.get("link", ""),
                    "subreddit": None,
                    "score": None,
                    "num_comments": None,
                    "sentiment": None,
                    "published_at": pub_date,
                }
            )
        return results

    def _marketwatch(self) -> list[dict]:
        """Fetch MarketWatch headlines via Dow Jones RSS, filtered for NVIDIA."""
        url = "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            logger.warning("MarketWatch RSS failed: %s", e)
            return []

        keywords = {"nvda", "nvidia", "geforce", "jensen"}
        results = []
        for entry in feed.entries:
            title = entry.get("title", "")
            summary = entry.get("summary", "")
            text = f"{title} {summary}".lower()
            if not any(kw in text for kw in keywords):
                continue

            pub_date = None
            if hasattr(entry, "published"):
                try:
                    pub_date = dateparser.parse(entry.published).isoformat()
                except Exception:
                    pass

            ext_id = entry.get("id", "") or entry.get("link", "")
            ext_id = hashlib.md5(ext_id.encode()).hexdigest()
            results.append(
                {
                    "source": "news",
                    "external_id": f"mw_{ext_id}",
                    "title": title,
                    "content": summary[:2000],
                    "author": "MarketWatch",
                    "url": entry.get("link", ""),
                    "subreddit": None,
                    "score": None,
                    "num_comments": None,
                    "sentiment": None,
                    "published_at": pub_date,
                }
            )
        return results
