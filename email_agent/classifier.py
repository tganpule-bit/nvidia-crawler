"""Heuristics (and optional LLM refinement) for deciding whether an email
message represents an active subscription, and for naming the service.
"""

import json
import logging
import re

import config

logger = logging.getLogger(__name__)

_SUBJECT_KEYWORDS = (
    "newsletter", "unsubscribe", "subscription", "subscribed", "digest",
    "weekly update", "you're in", "welcome to", "confirm your email",
    "verify your email", "confirm your subscription", "you have been added",
    "your weekly", "your daily", "recap", "here's what's new",
)
_SENDER_KEYWORDS = (
    "noreply", "no-reply", "newsletter", "notification", "notifications",
    "updates", "marketing", "news", "hello", "digest", "mailer", "info",
)
_GENERIC_DOMAIN_PREFIXES = ("mail", "email", "e", "notifications", "notify", "news", "mailer")


def score_message(sender: str, subject: str, has_list_unsubscribe: bool) -> tuple[str, str]:
    """Return (confidence, service_name) for a message.

    confidence is one of "high", "medium", "low".
    """
    sender = sender or ""
    subject = subject or ""
    subject_l = subject.lower()
    local_part = sender.split("@")[0].lower() if "@" in sender else sender.lower()

    if has_list_unsubscribe:
        confidence = "high"
    elif any(kw in subject_l for kw in _SUBJECT_KEYWORDS):
        confidence = "medium"
    elif any(kw in local_part for kw in _SENDER_KEYWORDS):
        confidence = "medium"
    else:
        confidence = "low"

    return confidence, _service_name_from_sender(sender)


_GENERIC_DISPLAY_NAMES = {
    "newsletter", "newsletters", "notification", "notifications",
    "no reply", "noreply", "no-reply", "updates", "digest", "team",
    "support", "info", "hello", "news", "mailer", "alerts",
}


def _service_name_from_sender(sender: str) -> str:
    """Derive a human-friendly service name from a From: header value."""
    display_match = re.match(r'^"?([^"<]+?)"?\s*<', sender)
    if display_match:
        name = display_match.group(1).strip()
        if name and "@" not in name and name.lower() not in _GENERIC_DISPLAY_NAMES:
            return name

    addr_match = re.search(r"<?([\w.+-]+@[\w.-]+)>?", sender)
    addr = addr_match.group(1) if addr_match else sender
    domain = addr.split("@")[-1].lower()

    parts = domain.split(".")
    if len(parts) > 2 and parts[0] in _GENERIC_DOMAIN_PREFIXES:
        parts = parts[1:]
    base = parts[0] if parts else domain
    return base.capitalize()


def refine_with_llm(findings: list[dict]) -> list[dict]:
    """Optionally use an LLM to clean up service names / drop false positives
    among low/medium confidence findings. No-op if ANTHROPIC_API_KEY is unset
    or the anthropic package / API call is unavailable.
    """
    if not config.ANTHROPIC_API_KEY:
        return findings

    candidates = [f for f in findings if f.get("confidence") != "high"]
    if not candidates:
        return findings

    try:
        import anthropic
    except ImportError:
        logger.info("anthropic package not installed; skipping LLM refinement.")
        return findings

    try:
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        items = [
            {"index": i, "sender": f.get("sender", ""), "subject": f.get("subject", "")}
            for i, f in enumerate(candidates)
        ]
        prompt = (
            "For each email below, decide if it is a newsletter/subscription/"
            "marketing email (is_subscription: true/false) and give a clean "
            "company/service name (service). Respond with ONLY a JSON array "
            "of {index, is_subscription, service}, one entry per input item.\n\n"
            f"{json.dumps(items)}"
        )
        response = client.messages.create(
            model=config.EMAIL_AUDIT_LLM_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(
            block.text for block in response.content if getattr(block, "type", "") == "text"
        )
        results = json.loads(_extract_json_array(text))
        by_index = {r["index"]: r for r in results if "index" in r}

        refined = list(findings)
        candidate_indices = [i for i, f in enumerate(findings) if f.get("confidence") != "high"]
        for pos, idx in enumerate(candidate_indices):
            r = by_index.get(pos)
            if not r:
                continue
            if r.get("is_subscription") is False:
                refined[idx] = None
            elif r.get("service"):
                refined[idx]["service"] = r["service"]
        return [f for f in refined if f is not None]
    except Exception as e:
        logger.warning("LLM refinement failed, keeping heuristic results: %s", e)
        return findings


def _extract_json_array(text: str) -> str:
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        raise ValueError("No JSON array found in LLM response")
    return text[start : end + 1]
