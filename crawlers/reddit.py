import logging
import time
from datetime import datetime, timezone

import requests

import config
from crawlers.base import BaseCrawler

logger = logging.getLogger(__name__)


class RedditCrawler(BaseCrawler):
    name = "reddit"

    def crawl(self) -> list[dict]:
        posts = []
        for subreddit in config.SUBREDDITS:
            for term in config.SEARCH_TERMS:
                posts.extend(self._search_subreddit(subreddit, term))
                time.sleep(1)  # rate-limit courtesy
        logger.info("Reddit: fetched %d posts", len(posts))
        return posts

    def _search_subreddit(self, subreddit: str, query: str) -> list[dict]:
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {
            "q": query,
            "sort": "new",
            "restrict_sr": "on",
            "limit": 25,
            "t": "day",
        }
        headers = {"User-Agent": config.REDDIT_USER_AGENT}

        try:
            resp = requests.get(
                url, params=params, headers=headers, timeout=config.REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("Reddit search failed for r/%s q=%s: %s", subreddit, query, e)
            return []

        results = []
        for child in data.get("data", {}).get("children", []):
            p = child.get("data", {})
            results.append(
                {
                    "source": "reddit",
                    "external_id": p.get("id", ""),
                    "title": p.get("title", ""),
                    "content": p.get("selftext", "")[:2000],
                    "author": p.get("author", ""),
                    "url": f"https://reddit.com{p.get('permalink', '')}",
                    "subreddit": subreddit,
                    "score": p.get("score", 0),
                    "num_comments": p.get("num_comments", 0),
                    "sentiment": None,
                    "published_at": datetime.fromtimestamp(
                        p.get("created_utc", 0), tz=timezone.utc
                    ).isoformat(),
                }
            )
        return results
