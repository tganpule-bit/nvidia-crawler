import sqlite3
from config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    external_id TEXT,
    title TEXT,
    content TEXT,
    author TEXT,
    url TEXT,
    subreddit TEXT,
    score INTEGER,
    num_comments INTEGER,
    sentiment TEXT,
    published_at DATETIME,
    crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, external_id)
);
"""


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    conn.executescript(SCHEMA)
    conn.close()


def insert_post(post: dict) -> bool:
    """Insert a post, ignoring duplicates. Returns True if inserted."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO posts
               (source, external_id, title, content, author, url,
                subreddit, score, num_comments, sentiment, published_at)
               VALUES (:source, :external_id, :title, :content, :author, :url,
                       :subreddit, :score, :num_comments, :sentiment, :published_at)""",
            {
                "source": post.get("source"),
                "external_id": post.get("external_id"),
                "title": post.get("title"),
                "content": post.get("content"),
                "author": post.get("author"),
                "url": post.get("url"),
                "subreddit": post.get("subreddit"),
                "score": post.get("score"),
                "num_comments": post.get("num_comments"),
                "sentiment": post.get("sentiment"),
                "published_at": post.get("published_at"),
            },
        )
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def insert_posts(posts: list[dict]) -> int:
    """Insert multiple posts, skipping duplicates. Returns count of new posts."""
    count = 0
    conn = get_connection()
    try:
        for post in posts:
            cur = conn.execute(
                """INSERT OR IGNORE INTO posts
                   (source, external_id, title, content, author, url,
                    subreddit, score, num_comments, sentiment, published_at)
                   VALUES (:source, :external_id, :title, :content, :author, :url,
                           :subreddit, :score, :num_comments, :sentiment, :published_at)""",
                {
                    "source": post.get("source"),
                    "external_id": post.get("external_id"),
                    "title": post.get("title"),
                    "content": post.get("content"),
                    "author": post.get("author"),
                    "url": post.get("url"),
                    "subreddit": post.get("subreddit"),
                    "score": post.get("score"),
                    "num_comments": post.get("num_comments"),
                    "sentiment": post.get("sentiment"),
                    "published_at": post.get("published_at"),
                },
            )
            if cur.rowcount > 0:
                count += 1
        conn.commit()
    finally:
        conn.close()
    return count


def get_post_counts():
    """Returns dict of source -> count."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM posts GROUP BY source"
    ).fetchall()
    conn.close()
    return {row["source"]: row["cnt"] for row in rows}


def update_sentiment(post_id, sentiment_score):
    """Update the sentiment score for a single post."""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE posts SET sentiment = ? WHERE id = ?",
            (sentiment_score, post_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_posts_without_sentiment():
    """Return all posts where sentiment IS NULL."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, title, content FROM posts WHERE sentiment IS NULL"
    ).fetchall()
    conn.close()
    return rows


def get_posts_by_date_range(days=14):
    """Return posts with sentiment scores from the last N days."""
    from datetime import datetime, timedelta

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, source, title, content, score, num_comments,
                  CAST(sentiment AS REAL) as sentiment, published_at
           FROM posts
           WHERE sentiment IS NOT NULL AND published_at >= ?
           ORDER BY published_at DESC""",
        (cutoff,),
    ).fetchall()
    conn.close()
    return rows
