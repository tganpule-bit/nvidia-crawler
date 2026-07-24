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

# --- Email subscription audit agent ---
# Finds which services an email address is subscribed to by scanning a
# mailbox (accurate) and, optionally, public web/breach sources (best-effort).
# Only run this against a mailbox/email address you own or are authorized
# to audit.
EMAIL_AUDIT_DB_PATH = os.getenv("EMAIL_AUDIT_DB_PATH", "email_subscriptions.db")

# IMAP settings for the inbox scanner. Credentials are never hardcoded here;
# they must be supplied via environment variables or an interactive prompt.
IMAP_HOST = os.getenv("IMAP_HOST", "imap.gmail.com")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))
IMAP_USER = os.getenv("IMAP_USER", "")
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD", "")
IMAP_FOLDER = os.getenv("IMAP_FOLDER", "INBOX")
EMAIL_SCAN_LOOKBACK_DAYS = int(os.getenv("EMAIL_SCAN_LOOKBACK_DAYS", "730"))
EMAIL_SCAN_MAX_MESSAGES = int(os.getenv("EMAIL_SCAN_MAX_MESSAGES", "3000"))

# Optional: Have I Been Pwned breach lookup (requires a paid API key).
HIBP_API_KEY = os.getenv("HIBP_API_KEY", "")

# Optional: Anthropic API key to let the agent use an LLM to double-check
# ambiguous inbox matches and clean up service names. Falls back to
# rule-based heuristics when unset.
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
EMAIL_AUDIT_LLM_MODEL = os.getenv("EMAIL_AUDIT_LLM_MODEL", "claude-sonnet-5")
