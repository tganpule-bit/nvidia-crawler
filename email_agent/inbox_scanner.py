"""Scans a mailbox (via IMAP) for subscription/newsletter emails addressed
to a given account, and reports which services that address is subscribed to.

This is the accurate detection method: it only works against a mailbox you
control (or are explicitly authorized to audit), using your own credentials.
"""

import email
import imaplib
import logging
import re
from email.header import decode_header
from email.utils import parsedate_to_datetime

import config
from email_agent.classifier import score_message

logger = logging.getLogger(__name__)


def scan_inbox(imap_password: str, max_messages: int = None) -> list[dict]:
    """Connect to IMAP and scan for subscription-like messages.

    Returns a list of finding dicts with keys: method, sender, subject,
    confidence, service, unsubscribe_url, evidence, detected_at.
    """
    if not config.IMAP_USER:
        raise ValueError("IMAP_USER is not configured (set the IMAP_USER env var).")
    if not imap_password:
        raise ValueError("No IMAP password supplied.")

    max_messages = max_messages or config.EMAIL_SCAN_MAX_MESSAGES
    findings_by_service = {}

    conn = imaplib.IMAP4_SSL(config.IMAP_HOST, config.IMAP_PORT)
    try:
        conn.login(config.IMAP_USER, imap_password)
        status, _ = conn.select(config.IMAP_FOLDER, readonly=True)
        if status != "OK":
            raise RuntimeError(f"Could not open folder {config.IMAP_FOLDER!r}")

        since = _lookback_date(config.EMAIL_SCAN_LOOKBACK_DAYS)
        status, data = conn.search(None, "SINCE", since)
        if status != "OK":
            raise RuntimeError("IMAP SEARCH failed")

        message_ids = data[0].split()
        logger.info("Inbox scan: %d messages since %s", len(message_ids), since)
        message_ids = message_ids[-max_messages:]

        for msg_id in message_ids:
            try:
                finding = _fetch_and_score(conn, msg_id)
            except Exception as e:
                logger.debug("Skipping message %s: %s", msg_id, e)
                continue
            if not finding:
                continue

            key = finding["service"].lower()
            existing = findings_by_service.get(key)
            if existing is None or _rank(finding["confidence"]) > _rank(existing["confidence"]):
                findings_by_service[key] = finding
    finally:
        try:
            conn.logout()
        except Exception:
            pass

    results = list(findings_by_service.values())
    logger.info("Inbox scan: %d distinct services found.", len(results))
    return results


def _fetch_and_score(conn, msg_id) -> dict | None:
    status, msg_data = conn.fetch(
        msg_id, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE LIST-UNSUBSCRIBE)])"
    )
    if status != "OK" or not msg_data or not msg_data[0]:
        return None

    raw_headers = msg_data[0][1]
    msg = email.message_from_bytes(raw_headers)

    sender = _decode(msg.get("From", ""))
    subject = _decode(msg.get("Subject", ""))
    list_unsubscribe = msg.get("List-Unsubscribe", "")
    date_hdr = msg.get("Date", "")

    confidence, service = score_message(sender, subject, bool(list_unsubscribe))
    if confidence == "low":
        return None

    detected_at = None
    if date_hdr:
        try:
            detected_at = parsedate_to_datetime(date_hdr).isoformat()
        except Exception:
            detected_at = None

    return {
        "method": "inbox_scan",
        "sender": sender,
        "subject": subject,
        "confidence": confidence,
        "service": service,
        "unsubscribe_url": _extract_unsubscribe_url(list_unsubscribe),
        "evidence": f"List-Unsubscribe header present" if list_unsubscribe else subject[:200],
        "detected_at": detected_at,
    }


def _extract_unsubscribe_url(header_value: str) -> str | None:
    if not header_value:
        return None
    urls = re.findall(r"<(https?://[^>]+)>", header_value)
    return urls[0] if urls else None


def _decode(value: str) -> str:
    if not value:
        return ""
    parts = decode_header(value)
    decoded = ""
    for text, charset in parts:
        if isinstance(text, bytes):
            decoded += text.decode(charset or "utf-8", errors="replace")
        else:
            decoded += text
    return decoded


def _lookback_date(days: int) -> str:
    from datetime import datetime, timedelta

    dt = datetime.utcnow() - timedelta(days=days)
    return dt.strftime("%d-%b-%Y")


def _rank(confidence: str) -> int:
    return {"high": 2, "medium": 1, "low": 0}.get(confidence, 0)
