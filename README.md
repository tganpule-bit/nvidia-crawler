nvidia crawler will crawl the web to get latest chatter on NVIDIA stock. This will happen twice a day and can be configured to set up a desired interval.

## Email subscription audit agent

`email_audit.py` is a separate agent that finds which services a given email address is subscribed to. **Only run it against an email address/mailbox you own or are explicitly authorized to audit.**

There's no public web index of private newsletter subscriptions, so the agent combines three signals:

1. **Inbox scan (accurate).** Logs into the mailbox over IMAP with your own credentials, looks for messages with a `List-Unsubscribe` header or newsletter-style subject/sender patterns, and lists the distinct services detected along with an unsubscribe link where available.
2. **Web search (best-effort).** A single search for the exact address to surface any public pages that mention it. This rarely finds active subscriptions (that data isn't public) but can catch public leaks/mentions.
3. **Breach check (optional).** Looks up the address against [Have I Been Pwned](https://haveibeenpwned.com/) if `HIBP_API_KEY` is set, listing services whose breached user databases included this address.

Ambiguous inbox matches are optionally refined by an LLM (set `ANTHROPIC_API_KEY`) to clean up service names and drop false positives; without a key, the agent falls back to rule-based heuristics.

### Configuration (environment variables)

| Variable | Purpose | Default |
|---|---|---|
| `IMAP_HOST` | IMAP server | `imap.gmail.com` |
| `IMAP_PORT` | IMAP port | `993` |
| `IMAP_USER` | Mailbox username/address | (required for inbox scan) |
| `IMAP_PASSWORD` | IMAP password / app password | prompted if unset |
| `IMAP_FOLDER` | Folder to scan | `INBOX` |
| `EMAIL_SCAN_LOOKBACK_DAYS` | How far back to scan | `730` |
| `EMAIL_SCAN_MAX_MESSAGES` | Max messages per scan | `3000` |
| `HIBP_API_KEY` | Enables the breach check | unset (skipped) |
| `ANTHROPIC_API_KEY` | Enables LLM refinement | unset (heuristics only) |

For Gmail, use an [app password](https://myaccount.google.com/apppasswords) rather than your account password.

### Usage

```bash
# Run a full scan and print a report
python email_audit.py --email you@example.com

# JSON output, skip the web search and breach check
python email_audit.py --email you@example.com --skip-web --skip-breach --output json

# Show previously stored findings without rescanning
python email_audit.py --show --email you@example.com
```
