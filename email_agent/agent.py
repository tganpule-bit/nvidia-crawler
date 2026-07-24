"""Orchestrates the email subscription audit agent.

Combines an inbox scan (accurate, requires mailbox credentials), an optional
public web search, and an optional breach-database lookup, then reconciles
the results through the heuristic/LLM classifier before persisting them.
"""

import logging
from datetime import datetime, timezone

from email_agent import db
from email_agent.classifier import refine_with_llm
from email_agent.inbox_scanner import scan_inbox
from email_agent.web_search import breach_check, web_search

logger = logging.getLogger(__name__)


class EmailSubscriptionAgent:
    def __init__(
        self,
        email: str,
        use_inbox: bool = True,
        use_web: bool = True,
        use_breach: bool = True,
        use_llm: bool = True,
    ):
        self.email = email
        self.use_inbox = use_inbox
        self.use_web = use_web
        self.use_breach = use_breach
        self.use_llm = use_llm

    def run(self, imap_password: str = None) -> dict:
        """Run the configured scans and return a results dict."""
        inbox_findings = []
        web_findings = []
        breach_findings = []

        if self.use_inbox:
            try:
                inbox_findings = scan_inbox(imap_password)
                if self.use_llm:
                    inbox_findings = refine_with_llm(inbox_findings)
            except Exception as e:
                logger.error("Inbox scan failed: %s", e)

        if self.use_web:
            web_findings = web_search(self.email)

        if self.use_breach:
            breach_findings = breach_check(self.email)

        all_findings = inbox_findings + web_findings + breach_findings
        saved = db.save_findings(self.email, all_findings)
        logger.info("Saved %d findings for %s.", saved, self.email)

        return {
            "email": self.email,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "inbox_findings": inbox_findings,
            "web_findings": web_findings,
            "breach_findings": breach_findings,
        }
