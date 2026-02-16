import logging
from datetime import datetime, timedelta
from collections import defaultdict

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from db import get_connection

logger = logging.getLogger(__name__)

# Download VADER lexicon on first import
try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError:
    nltk.download("vader_lexicon", quiet=True)

_sia = SentimentIntensityAnalyzer()


def score_post(title, content):
    """Run VADER on title+content, return compound score (-1 to +1)."""
    text = ""
    if title:
        text += title
    if content:
        text += " " + content
    text = text.strip()
    if not text:
        return 0.0
    return _sia.polarity_scores(text)["compound"]


def backfill_sentiment():
    """Score all posts where sentiment IS NULL, update the DB."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, title, content FROM posts WHERE sentiment IS NULL"
        ).fetchall()
        if not rows:
            logger.info("No posts need sentiment scoring.")
            return 0

        count = 0
        for row in rows:
            score = score_post(row["title"], row["content"])
            conn.execute(
                "UPDATE posts SET sentiment = ? WHERE id = ?",
                (score, row["id"]),
            )
            count += 1

        conn.commit()
        logger.info("Backfilled sentiment for %d posts.", count)
        return count
    finally:
        conn.close()


def get_daily_sentiment(days=14):
    """Query posts grouped by date, return avg sentiment per day per source,
    plus overall weighted avg (weight Reddit by upvotes, news equally).

    Returns list of dicts with keys: date, reddit_avg, news_avg, combined_avg,
    reddit_count, news_count, total_count.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT date(published_at) as day, source,
                      CAST(sentiment AS REAL) as sentiment,
                      COALESCE(score, 1) as weight
               FROM posts
               WHERE sentiment IS NOT NULL
                 AND published_at >= ?
               ORDER BY day""",
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()

    # Group by day
    days_data = defaultdict(lambda: {
        "reddit_scores": [], "reddit_weights": [],
        "news_scores": [], "news_weights": [],
    })

    for row in rows:
        day = row["day"]
        sentiment = row["sentiment"]
        weight = max(row["weight"], 1)
        source = row["source"]

        if source == "reddit":
            days_data[day]["reddit_scores"].append(sentiment)
            days_data[day]["reddit_weights"].append(weight)
        else:
            days_data[day]["news_scores"].append(sentiment)
            days_data[day]["news_weights"].append(1)

    result = []
    for day in sorted(days_data.keys()):
        d = days_data[day]
        reddit_avg = _weighted_avg(d["reddit_scores"], d["reddit_weights"])
        news_avg = _weighted_avg(d["news_scores"], d["news_weights"])

        # Combined: weighted average of all sources
        all_scores = d["reddit_scores"] + d["news_scores"]
        all_weights = d["reddit_weights"] + d["news_weights"]
        combined_avg = _weighted_avg(all_scores, all_weights)

        result.append({
            "date": day,
            "reddit_avg": reddit_avg,
            "news_avg": news_avg,
            "combined_avg": combined_avg,
            "reddit_count": len(d["reddit_scores"]),
            "news_count": len(d["news_scores"]),
            "total_count": len(all_scores),
        })

    return result


def _weighted_avg(values, weights):
    """Compute weighted average. Returns 0.0 if no values."""
    if not values:
        return 0.0
    total_weight = sum(weights)
    if total_weight == 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total_weight


def predict_trend():
    """Analyze recent sentiment data and produce a prediction.

    Returns dict with: direction, confidence, summary, daily_scores.
    """
    daily = get_daily_sentiment(days=14)

    if len(daily) < 2:
        return {
            "direction": "Neutral",
            "confidence": 1,
            "summary": "Not enough data to make a prediction.",
            "daily_scores": daily,
        }

    # Split into this week (last 7 days) and prior week
    today = datetime.utcnow().strftime("%Y-%m-%d")
    week_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")

    this_week = [d for d in daily if d["date"] >= week_ago]
    last_week = [d for d in daily if d["date"] < week_ago]

    # Averages
    this_week_avg = _avg([d["combined_avg"] for d in this_week]) if this_week else 0.0
    last_week_avg = _avg([d["combined_avg"] for d in last_week]) if last_week else 0.0

    # Momentum
    momentum = this_week_avg - last_week_avg

    # Volume trend
    this_week_count = sum(d["total_count"] for d in this_week)
    last_week_count = sum(d["total_count"] for d in last_week)
    volume_ratio = this_week_count / max(last_week_count, 1)

    # Engagement-weighted score (this week)
    engagement = this_week_avg  # already weighted by upvotes in get_daily_sentiment

    # Direction
    if momentum > 0.05 and engagement > 0.05:
        direction = "Bullish"
    elif momentum < -0.05 and engagement < -0.05:
        direction = "Bearish"
    else:
        direction = "Neutral"

    # Confidence (1-10)
    abs_momentum = abs(momentum)
    # Source agreement: do reddit and news agree on direction?
    reddit_avgs = [d["reddit_avg"] for d in this_week if d["reddit_count"] > 0]
    news_avgs = [d["news_avg"] for d in this_week if d["news_count"] > 0]
    reddit_dir = _avg(reddit_avgs) if reddit_avgs else 0.0
    news_dir = _avg(news_avgs) if news_avgs else 0.0
    agreement = 1.0 if (reddit_dir >= 0) == (news_dir >= 0) else 0.5

    confidence = min(10, max(1, int(
        abs_momentum * 20 +
        min(volume_ratio, 3) * 1.5 +
        agreement * 2 +
        min(this_week_count, 50) * 0.05
    )))

    # Summary
    vol_desc = "rising" if volume_ratio > 1.2 else "falling" if volume_ratio < 0.8 else "steady"
    summary = (
        f"{direction} outlook (confidence: {confidence}/10). "
        f"Sentiment momentum: {momentum:+.3f} (this week: {this_week_avg:.3f}, "
        f"last week: {last_week_avg:.3f}). "
        f"Post volume is {vol_desc} ({this_week_count} vs {last_week_count} posts). "
        f"Reddit sentiment: {reddit_dir:.3f}, News sentiment: {news_dir:.3f}."
    )

    return {
        "direction": direction,
        "confidence": confidence,
        "summary": summary,
        "daily_scores": daily,
    }


def _avg(values):
    """Simple average."""
    if not values:
        return 0.0
    return sum(values) / len(values)
