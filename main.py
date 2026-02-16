import argparse
import logging

import os

from db import init_db, get_post_counts, get_connection
from scheduler import run_all_crawlers, start_scheduler
from sentiment import backfill_sentiment, predict_trend
from charts import generate_sentiment_chart, generate_volume_chart


def run_analysis():
    """Run the full sentiment analysis pipeline."""
    # 1. Backfill sentiment on any unscored posts
    scored = backfill_sentiment()
    print(f"Sentiment scoring: {scored} posts scored.\n")

    # 2. Generate prediction
    prediction = predict_trend()

    # 3. Print prediction summary
    print("=" * 60)
    print(f"  NVDA PREDICTION: {prediction['direction']}")
    print(f"  Confidence: {prediction['confidence']}/10")
    print("=" * 60)
    print(f"\n{prediction['summary']}\n")

    # 4. Generate charts
    daily = prediction["daily_scores"]
    if daily:
        project_dir = os.path.dirname(os.path.abspath(__file__))
        sentiment_path = os.path.join(project_dir, "sentiment_chart.png")
        volume_path = os.path.join(project_dir, "volume_chart.png")

        generate_sentiment_chart(daily, sentiment_path)
        generate_volume_chart(daily, volume_path)

        print(f"Charts saved:")
        print(f"  Sentiment: {sentiment_path}")
        print(f"  Volume:    {volume_path}")
    else:
        print("No data available to generate charts.")


def show_posts(source=None, limit=20):
    """Display recent posts from the database."""
    conn = get_connection()
    query = "SELECT source, title, url, author, score, published_at FROM posts"
    params = []
    if source:
        query += " WHERE source = ?"
        params.append(source)
    query += " ORDER BY published_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        print("No posts found.")
        return

    counts = get_post_counts()
    print(f"Total posts: {sum(counts.values())} ({', '.join(f'{s}: {c}' for s, c in sorted(counts.items()))})\n")

    for r in rows:
        score = f" [{r['score']} pts]" if r["score"] is not None else ""
        date = r["published_at"][:16] if r["published_at"] else "N/A"
        print(f"[{r['source']:8s}] {date}  {r['title'][:80]}{score}")
        print(f"           {r['url']}")
        print()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="NVIDIA Stock Chatter Crawler")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run all crawlers once and exit (default: run on schedule)",
    )
    parser.add_argument(
        "--show",
        nargs="?",
        const="all",
        metavar="SOURCE",
        help="Show recent posts. Optionally filter by source: reddit, news, twitter",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of posts to show with --show (default: 20)",
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Run sentiment analysis, generate charts, and print prediction",
    )
    args = parser.parse_args()

    init_db()

    if args.analyze:
        run_analysis()
    elif args.show:
        source = None if args.show == "all" else args.show
        show_posts(source=source, limit=args.limit)
    elif args.once:
        run_all_crawlers()
        counts = get_post_counts()
        print("\nPost counts by source:")
        for source, count in sorted(counts.items()):
            print(f"  {source}: {count}")
        if not counts:
            print("  (no posts collected)")
    else:
        print("Starting scheduled crawler. Press Ctrl+C to stop.")
        start_scheduler()


if __name__ == "__main__":
    main()
