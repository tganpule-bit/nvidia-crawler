import sqlite3

import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    service TEXT NOT NULL,
    method TEXT NOT NULL,
    confidence TEXT,
    sender TEXT,
    subject TEXT,
    evidence TEXT,
    unsubscribe_url TEXT,
    detected_at DATETIME,
    crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(email, service, method)
);
"""


def get_connection():
    conn = sqlite3.connect(config.EMAIL_AUDIT_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    conn.executescript(SCHEMA)
    conn.close()


def save_findings(email: str, findings: list[dict]) -> int:
    """Upsert findings, keyed on (email, service, method). Returns count saved."""
    conn = get_connection()
    count = 0
    try:
        for f in findings:
            cur = conn.execute(
                """INSERT INTO subscriptions
                   (email, service, method, confidence, sender, subject,
                    evidence, unsubscribe_url, detected_at)
                   VALUES (:email, :service, :method, :confidence, :sender,
                           :subject, :evidence, :unsubscribe_url, :detected_at)
                   ON CONFLICT(email, service, method) DO UPDATE SET
                       confidence=excluded.confidence,
                       sender=excluded.sender,
                       subject=excluded.subject,
                       evidence=excluded.evidence,
                       unsubscribe_url=excluded.unsubscribe_url,
                       detected_at=excluded.detected_at""",
                {
                    "email": email,
                    "service": f.get("service"),
                    "method": f.get("method"),
                    "confidence": f.get("confidence"),
                    "sender": f.get("sender"),
                    "subject": f.get("subject"),
                    "evidence": f.get("evidence"),
                    "unsubscribe_url": f.get("unsubscribe_url"),
                    "detected_at": f.get("detected_at"),
                },
            )
            if cur.rowcount > 0:
                count += 1
        conn.commit()
    finally:
        conn.close()
    return count


def get_findings(email: str = None) -> list[sqlite3.Row]:
    conn = get_connection()
    try:
        if email:
            rows = conn.execute(
                """SELECT * FROM subscriptions WHERE email = ?
                   ORDER BY method, service""",
                (email,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM subscriptions ORDER BY email, method, service"
            ).fetchall()
        return rows
    finally:
        conn.close()
