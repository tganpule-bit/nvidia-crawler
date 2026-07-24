"""Best-effort public OSINT signals for an email address:

- A single web search for the literal address, to surface any public pages
  that mention it (this does NOT reveal private newsletter subscriptions --
  those live in each service's own database, not on crawlable web pages).
- An optional Have I Been Pwned breach lookup, which lists services whose
  user databases (including this address) were exposed in a known breach.

Use only against an email address you own or are authorized to investigate.
"""

import logging

import requests
from bs4 import BeautifulSoup

import config

logger = logging.getLogger(__name__)

DUCKDUCKGO_URL = "https://html.duckduckgo.com/html/"
HIBP_URL = "https://haveibeenpwned.com/api/v3/breachedaccount/{email}"


def web_search(email: str, limit: int = 10) -> list[dict]:
    """Run a single web search for the exact email address.

    Returns findings with method="web_search". Results are pages that
    merely mention the address publicly, not confirmed subscriptions.
    """
    query = f'"{email}"'
    try:
        resp = requests.post(
            DUCKDUCKGO_URL,
            data={"q": query},
            headers=config.REQUEST_HEADERS,
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Web search failed: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    for result in soup.select("div.result")[:limit]:
        link = result.select_one("a.result__a")
        snippet = result.select_one(".result__snippet")
        if not link or not link.get("href"):
            continue
        results.append(
            {
                "method": "web_search",
                "service": link.get_text(strip=True)[:80] or "Unknown page",
                "sender": None,
                "subject": None,
                "confidence": "low",
                "unsubscribe_url": None,
                "evidence": link["href"],
                "detected_at": None,
                "snippet": snippet.get_text(strip=True) if snippet else "",
            }
        )

    if not results:
        logger.info(
            "Web search: no public mentions found for this address "
            "(expected -- most subscriptions aren't publicly indexed)."
        )
    return results


def breach_check(email: str) -> list[dict]:
    """Check Have I Been Pwned for breaches involving this address.

    Requires HIBP_API_KEY to be configured. Returns an empty list (with a
    log message) if no key is set.
    """
    if not config.HIBP_API_KEY:
        logger.info("HIBP_API_KEY not set; skipping breach check.")
        return []

    headers = {
        **config.REQUEST_HEADERS,
        "hibp-api-key": config.HIBP_API_KEY,
    }
    try:
        resp = requests.get(
            HIBP_URL.format(email=email),
            headers=headers,
            timeout=config.REQUEST_TIMEOUT,
        )
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        breaches = resp.json()
    except Exception as e:
        logger.warning("HIBP breach check failed: %s", e)
        return []

    return [
        {
            "method": "breach_check",
            "service": b.get("Title") or b.get("Name"),
            "sender": None,
            "subject": None,
            "confidence": "high",
            "unsubscribe_url": None,
            "evidence": f"Data breach at {b.get('Domain', b.get('Name'))}",
            "detected_at": b.get("BreachDate"),
        }
        for b in breaches
    ]
