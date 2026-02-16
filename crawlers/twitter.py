import logging

import config
from crawlers.base import BaseCrawler

logger = logging.getLogger(__name__)


class TwitterCrawler(BaseCrawler):
    name = "twitter"

    def crawl(self) -> list[dict]:
        """Attempt to scrape Twitter using snscrape.

        snscrape may break if Twitter/X changes their site structure.
        Falls back gracefully with a warning if unavailable.
        """
        try:
            return self._scrape_with_snscrape()
        except ImportError:
            logger.warning(
                "snscrape not installed or incompatible. "
                "Install with: pip install snscrape. "
                "Twitter crawling is disabled."
            )
            return []
        except Exception as e:
            logger.warning("Twitter crawl failed: %s", e)
            return []

    def _scrape_with_snscrape(self) -> list[dict]:
        import snscrape.modules.twitter as sntwitter

        results = []
        for term in config.TWITTER_SEARCH_TERMS:
            query = f"{term} lang:en"
            try:
                scraper = sntwitter.TwitterSearchScraper(query)
                for i, tweet in enumerate(scraper.get_items()):
                    if i >= 50:
                        break
                    results.append(
                        {
                            "source": "twitter",
                            "external_id": str(tweet.id),
                            "title": "",
                            "content": tweet.rawContent[:2000],
                            "author": tweet.user.username if tweet.user else "",
                            "url": tweet.url,
                            "subreddit": None,
                            "score": tweet.likeCount,
                            "num_comments": tweet.replyCount,
                            "sentiment": None,
                            "published_at": tweet.date.isoformat()
                            if tweet.date
                            else None,
                        }
                    )
            except Exception as e:
                logger.warning("snscrape search failed for '%s': %s", term, e)

        logger.info("Twitter: fetched %d tweets", len(results))
        return results
