import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


def generate_sentiment_chart(daily_data, output_path="sentiment_chart.png"):
    """Line chart of daily sentiment over time.

    X-axis: dates, Y-axis: sentiment score (-1 to +1).
    Separate lines for reddit vs news, plus combined.
    Green zone above 0, red zone below 0.
    """
    if not daily_data:
        return

    dates = [datetime.strptime(d["date"], "%Y-%m-%d") for d in daily_data]
    reddit = [d["reddit_avg"] for d in daily_data]
    news = [d["news_avg"] for d in daily_data]
    combined = [d["combined_avg"] for d in daily_data]

    fig, ax = plt.subplots(figsize=(12, 6))

    # Color zones
    ax.axhspan(0, 1, alpha=0.05, color="green")
    ax.axhspan(-1, 0, alpha=0.05, color="red")
    ax.axhline(y=0, color="gray", linestyle="--", linewidth=0.8)

    # Plot lines
    ax.plot(dates, combined, "b-o", linewidth=2, markersize=4, label="Combined", zorder=3)
    ax.plot(dates, reddit, "s--", color="orange", linewidth=1.5, markersize=3, label="Reddit", alpha=0.8)
    ax.plot(dates, news, "^--", color="purple", linewidth=1.5, markersize=3, label="News", alpha=0.8)

    ax.set_xlabel("Date")
    ax.set_ylabel("Sentiment Score")
    ax.set_title("NVDA Sentiment Over Time")
    ax.set_ylim(-1, 1)
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
    fig.autofmt_xdate()

    plt.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path


def generate_volume_chart(daily_data, output_path="volume_chart.png"):
    """Stacked bar chart of post volume per day by source."""
    if not daily_data:
        return

    dates = [datetime.strptime(d["date"], "%Y-%m-%d") for d in daily_data]
    reddit_counts = [d["reddit_count"] for d in daily_data]
    news_counts = [d["news_count"] for d in daily_data]

    fig, ax = plt.subplots(figsize=(12, 5))

    bar_width = 0.8
    ax.bar(dates, reddit_counts, bar_width, label="Reddit", color="orange", alpha=0.8)
    ax.bar(dates, news_counts, bar_width, bottom=reddit_counts, label="News", color="purple", alpha=0.8)

    ax.set_xlabel("Date")
    ax.set_ylabel("Post Count")
    ax.set_title("NVDA Post Volume by Source")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3, axis="y")

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
    fig.autofmt_xdate()

    plt.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path
