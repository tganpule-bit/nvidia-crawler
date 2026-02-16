from abc import ABC, abstractmethod


class BaseCrawler(ABC):
    """Abstract base class for all crawlers."""

    name: str = "base"

    @abstractmethod
    def crawl(self) -> list[dict]:
        """Fetch posts and return a list of post dicts.

        Each dict should have keys matching the posts table columns:
        source, external_id, title, content, author, url,
        subreddit, score, num_comments, sentiment, published_at
        """
        ...
