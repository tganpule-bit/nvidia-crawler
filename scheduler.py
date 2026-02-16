import logging

from apscheduler.schedulers.blocking import BlockingScheduler

import config
from crawlers import ALL_CRAWLERS
from db import init_db, insert_posts
from sentiment import backfill_sentiment

logger = logging.getLogger(__name__)


def run_all_crawlers():
    """Execute all crawlers and store results."""
    total = 0
    for crawler_cls in ALL_CRAWLERS:
        crawler = crawler_cls()
        try:
            posts = crawler.crawl()
            new_count = insert_posts(posts)
            total += new_count
            logger.info(
                "%s: %d fetched, %d new", crawler.name, len(posts), new_count
            )
        except Exception as e:
            logger.error("Crawler %s failed: %s", crawler.name, e)
    logger.info("Crawl complete. %d new posts total.", total)

    # Auto-score new posts
    if total > 0:
        try:
            scored = backfill_sentiment()
            logger.info("Auto-scored %d new posts.", scored)
        except Exception as e:
            logger.error("Sentiment scoring failed: %s", e)

    return total


def start_scheduler():
    """Start the blocking scheduler that runs crawlers on an interval."""
    init_db()
    logger.info(
        "Starting scheduler. Crawl interval: %d minutes.",
        config.CRAWL_INTERVAL_MINUTES,
    )

    # Run immediately on start
    run_all_crawlers()

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_all_crawlers,
        "interval",
        minutes=config.CRAWL_INTERVAL_MINUTES,
        id="nvidia_crawl",
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
