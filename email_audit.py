import argparse
import getpass
import json
import logging

import config
from email_agent import db
from email_agent.agent import EmailSubscriptionAgent

logger = logging.getLogger(__name__)


def run_scan(args):
    agent = EmailSubscriptionAgent(
        email=args.email,
        use_inbox=not args.skip_inbox,
        use_web=not args.skip_web,
        use_breach=not args.skip_breach,
        use_llm=not args.skip_llm,
    )

    imap_password = None
    if agent.use_inbox:
        imap_password = (
            args.imap_password
            or config.IMAP_PASSWORD
            or getpass.getpass(
                f"IMAP password for {config.IMAP_USER or '(set IMAP_USER)'}: "
            )
        )

    results = agent.run(imap_password=imap_password)

    if args.output == "json":
        print(json.dumps(results, indent=2, default=str))
        return

    print_report(results)


def print_report(results):
    email = results["email"]
    print("=" * 70)
    print(f"  Subscription audit for {email}")
    print(f"  Generated: {results['generated_at']}")
    print("=" * 70)

    _print_section(
        "Inbox scan (mailbox evidence)",
        results["inbox_findings"],
        empty_msg="No subscription emails found in the scanned mailbox/folder.",
    )
    _print_section(
        "Public web mentions (best-effort, not confirmed subscriptions)",
        results["web_findings"],
        empty_msg="No public mentions found (expected for most private inboxes).",
    )
    _print_section(
        "Known data breaches",
        results["breach_findings"],
        empty_msg="No known breaches (or HIBP_API_KEY not configured).",
    )


def _print_section(title, findings, empty_msg):
    print(f"\n{title}:")
    if not findings:
        print(f"  {empty_msg}")
        return
    for f in findings:
        conf = f.get("confidence", "?")
        line = f"  [{conf:6s}] {f.get('service', 'Unknown')}"
        print(line)
        if f.get("evidence"):
            print(f"           {f['evidence']}")
        if f.get("unsubscribe_url"):
            print(f"           unsubscribe: {f['unsubscribe_url']}")


def show_stored(args):
    rows = db.get_findings(args.email)
    if not rows:
        print("No stored findings. Run a scan first with --email <address>.")
        return
    for r in rows:
        print(f"[{r['method']:12s}] {r['email']:30s} {r['service']} ({r['confidence']})")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Email subscription audit agent: finds which services an "
        "email address is subscribed to. Only run this against an address "
        "you own or are authorized to audit."
    )
    parser.add_argument("--email", help="Email address to audit")
    parser.add_argument(
        "--imap-password",
        help="IMAP password/app-password (prefer IMAP_PASSWORD env var or the prompt instead)",
    )
    parser.add_argument("--skip-inbox", action="store_true", help="Skip the IMAP inbox scan")
    parser.add_argument("--skip-web", action="store_true", help="Skip the public web search")
    parser.add_argument("--skip-breach", action="store_true", help="Skip the HIBP breach check")
    parser.add_argument("--skip-llm", action="store_true", help="Skip LLM refinement of results")
    parser.add_argument(
        "--output", choices=["text", "json"], default="text", help="Output format"
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Show previously stored findings instead of running a new scan",
    )
    args = parser.parse_args()

    db.init_db()

    if args.show:
        show_stored(args)
        return

    if not args.email:
        parser.error("--email is required unless --show is used")

    run_scan(args)


if __name__ == "__main__":
    main()
