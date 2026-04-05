#!/usr/bin/env python3
"""
SuperNews — Daily Digest Email Sender (Brevo API) v3
=====================================================
Reads directly from SQLite, composes an HTML email grouped by topic
with article summaries under each heading, and sends via Brevo.

Only includes articles collected since the last successful send.

Run: python3 send_digest.py
     python3 send_digest.py --dry-run   (preview without sending)
     python3 send_digest.py --force      (ignore last_sent, send last 7 days)

Requires: pip install requests
Config:   config.json in the same directory
"""

import json
import sys
import sqlite3
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict, Counter

try:
    import requests
except ImportError:
    print("Missing 'requests'. Run: pip install requests")
    sys.exit(1)

# ─── Paths & Constants ────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
DB_PATH = SCRIPT_DIR / "super_news.db"
BREVO_API_URL = "https://api.brevo.com/v3/smtp/email"
DEFAULT_LOOKBACK_DAYS = 7

TIER_META = {
    1: {"name": "Media Articles",            "icon": "&#x1F4F0;", "color": "#1a56db", "bg": "#eff6ff"},
    2: {"name": "Regulatory & Government",   "icon": "&#x1F3DB;", "color": "#057a55", "bg": "#f0fdf4"},
    3: {"name": "Industry Bodies & Research", "icon": "&#x1F4CA;", "color": "#9f580a", "bg": "#fffbeb"},
    4: {"name": "Social Media & Blogs",      "icon": "&#x1F4AC;", "color": "#6c2bd9", "bg": "#faf5ff"},
}

# Topic display order
TOPIC_ORDER = [
    "Regulation & Policy",
    "Complaints & Enforcement",
    "Fund Operations & Product",
    "Investment & Returns",
    "ESG & Responsible Investment",
    "Mergers & Corporate Actions",
    "People & Governance",
    "Insurance & Claims",
    "Retirement Income",
    "Employer & Contributions",
    "SMSF",
    "Technology & Cyber",
    "General",
]


# ─── Config & DB ──────────────────────────────────────────────────────────────

def load_config(dry_run=False):
    if not CONFIG_PATH.exists():
        print(f"Error: Config not found at {CONFIG_PATH}")
        print("  Copy config.json.example to config.json and add your Brevo API key.")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    if not dry_run and config.get("brevo_api_key", "").startswith("YOUR_"):
        print("Error: Brevo API key not configured.")
        sys.exit(1)
    return config


def get_db():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        print("  Run collect.py first.")
        sys.exit(1)
    return sqlite3.connect(str(DB_PATH))


def get_last_sent(conn):
    """Get the timestamp of the last successful digest send."""
    try:
        row = conn.execute(
            "SELECT value FROM digest_state WHERE key = 'last_sent'"
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def set_last_sent(conn):
    """Record that a digest was just sent."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO digest_state (key, value) VALUES ('last_sent', ?)",
        (now,)
    )
    conn.commit()
    return now


# ─── Fetch Articles ───────────────────────────────────────────────────────────

def fetch_articles_since(conn, since_dt):
    """Fetch all articles collected after since_dt, with fund tags."""
    articles = conn.execute("""
        SELECT a.article_id, a.url, a.title, a.source, a.tier,
               a.topic_category, a.summary, a.published_at, a.collected_at,
               GROUP_CONCAT(aft.fund_id) AS fund_ids
        FROM articles a
        LEFT JOIN article_fund_tags aft ON a.article_id = aft.article_id
        WHERE a.collected_at >= ?
        GROUP BY a.article_id
        ORDER BY a.tier ASC, a.published_at DESC NULLS LAST
    """, (since_dt,)).fetchall()

    funds = conn.execute(
        "SELECT fund_id, display_name FROM funds"
    ).fetchall()
    funds_lookup = {r[0]: r[1] for r in funds}

    result = []
    for row in articles:
        result.append({
            "id": row[0], "url": row[1], "title": row[2],
            "source": row[3], "tier": row[4], "topic": row[5] or "General",
            "summary": row[6] or "", "published_at": row[7] or "",
            "collected_at": row[8],
            "fund_ids": row[9].split(",") if row[9] else [],
            "fund_names": [funds_lookup.get(fid, fid) for fid in (row[9].split(",") if row[9] else [])],
        })
    return result


# ─── Group by Topic ───────────────────────────────────────────────────────────

def group_by_topic(articles):
    """Group articles by topic category, ordered by TOPIC_ORDER."""
    groups = defaultdict(list)
    for a in articles:
        groups[a["topic"]].append(a)
    # Return in defined order, then any extras alphabetically
    ordered = []
    for topic in TOPIC_ORDER:
        if topic in groups:
            ordered.append((topic, groups.pop(topic)))
    for topic in sorted(groups.keys()):
        ordered.append((topic, groups[topic]))
    return ordered


# ─── Build Subject ────────────────────────────────────────────────────────────

def build_subject(config, articles, since_dt):
    """Build email subject with date range and article count."""
    prefix = config.get("subject_prefix", "SuperNews Digest")
    try:
        start = datetime.fromisoformat(since_dt)
        start_str = start.strftime("%-d %b")
    except (ValueError, TypeError):
        start_str = "Recent"
    end_str = datetime.now().strftime("%-d %b %Y")
    n = len(articles)
    return f"{prefix} — {start_str} to {end_str} ({n} article{'s' if n != 1 else ''})"


# ─── Build HTML Email ─────────────────────────────────────────────────────────

def fmt_date(iso_str):
    """Format an ISO date string to '2 Apr' style."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%-d %b")
    except (ValueError, TypeError):
        return ""


def build_html(articles, since_dt, config):
    """Compose HTML email grouped by topic with summaries under each heading."""
    now = datetime.now()
    try:
        start = datetime.fromisoformat(since_dt)
        date_range = f"{start.strftime('%-d %B')} — {now.strftime('%-d %B %Y')}"
    except (ValueError, TypeError):
        date_range = f"to {now.strftime('%-d %B %Y')}"

    topic_groups = group_by_topic(articles)
    all_sources = set(a["source"] for a in articles)
    source_counter = Counter(a["source"] for a in articles)
    top_sources = ", ".join(f"{s} ({c})" for s, c in source_counter.most_common(6))

    # Count by tier
    tier_counts = Counter(a["tier"] for a in articles)
    tier_summary_parts = []
    for t in [1, 2, 3, 4]:
        if tier_counts[t]:
            tier_summary_parts.append(f"{TIER_META[t]['icon']} {tier_counts[t]} {TIER_META[t]['name']}")

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SuperNews Digest — {date_range}</title>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:24px 0;">
<tr><td align="center">
<table width="620" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08);">

<!-- Header -->
<tr><td style="background:#0f172a;padding:28px 32px;">
  <h1 style="margin:0;color:white;font-size:22px;font-weight:700;">&#x1F998; Super<span style="color:#60a5fa;">News</span></h1>
  <p style="margin:6px 0 0;color:#e2e8f0;font-size:14px;font-weight:600;">{date_range}</p>
  <p style="margin:4px 0 0;color:#94a3b8;font-size:12px;">
    {len(articles)} articles from {len(all_sources)} sources
  </p>
</td></tr>

<!-- Source Summary -->
<tr><td style="padding:12px 32px;background:#f8fafc;border-bottom:1px solid #e2e8f0;">
  <p style="margin:0;font-size:11px;color:#64748b;line-height:1.5;">
    <strong style="color:#475569;">Sources:</strong> {top_sources}
  </p>
</td></tr>

<!-- Tier Breakdown Bar -->
<tr><td style="padding:10px 32px;border-bottom:1px solid #e2e8f0;">
  <p style="margin:0;font-size:11px;color:#64748b;">
    {"&nbsp;&nbsp;|&nbsp;&nbsp;".join(tier_summary_parts)}
  </p>
</td></tr>
"""


    # ── Topic Sections ──
    for topic_name, topic_articles in topic_groups:
        # Determine the dominant tier color for this topic
        tier_counts_topic = Counter(a["tier"] for a in topic_articles)
        dominant_tier = tier_counts_topic.most_common(1)[0][0]
        meta = TIER_META.get(dominant_tier, TIER_META[1])

        html += f"""
<!-- Topic: {topic_name} -->
<tr><td style="padding:20px 32px 6px;">
  <table width="100%" cellpadding="0" cellspacing="0">
  <tr><td style="background:{meta['bg']};border-left:4px solid {meta['color']};padding:10px 14px;border-radius:6px;">
    <span style="font-size:14px;font-weight:700;color:{meta['color']};">{topic_name}</span>
    <span style="float:right;font-size:12px;color:#64748b;">{len(topic_articles)} article{'s' if len(topic_articles)!=1 else ''}</span>
  </td></tr>
  </table>
</td></tr>
"""
        for a in topic_articles:
            title = a["title"]
            url = a["url"]
            source = a["source"]
            summary = a["summary"][:250] if a["summary"] else ""
            date_str = fmt_date(a["published_at"])
            tier = a["tier"]
            tier_info = TIER_META.get(tier, TIER_META[1])

            # Fund tags
            fund_html = "".join(
                f'<span style="display:inline-block;background:#dbeafe;color:#1d4ed8;'
                f'font-size:10px;font-weight:600;padding:2px 7px;border-radius:12px;'
                f'margin-right:4px;margin-top:4px;">{fn}</span>'
                for fn in a["fund_names"]
            )

            # Tier dot
            tier_dot = (
                f'<span style="display:inline-block;width:8px;height:8px;'
                f'border-radius:50%;background:{tier_info["color"]};margin-right:4px;'
                f'vertical-align:middle;"></span>'
            )

            meta_parts = [source]
            if date_str:
                meta_parts.append(date_str)
            meta_line = " &middot; ".join(meta_parts)

            html += f"""
<tr><td style="padding:4px 32px;">
  <table width="100%" cellpadding="0" cellspacing="0" style="border-bottom:1px solid #f1f5f9;padding-bottom:10px;margin-bottom:2px;">
  <tr><td>
    <p style="margin:0 0 2px;font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.3px;">
      {tier_dot}{meta_line}
    </p>
    <p style="margin:0 0 3px;font-size:14px;font-weight:600;line-height:1.4;">
      <a href="{url}" style="color:#1e293b;text-decoration:none;">{title}</a>
    </p>
    {f'<p style="margin:0 0 4px;font-size:12px;color:#64748b;line-height:1.4;">{summary}</p>' if summary else ''}
    {fund_html}
  </td></tr>
  </table>
</td></tr>
"""


    # ── Footer ──
    html += f"""
<!-- Footer -->
<tr><td style="padding:24px 32px;background:#f8fafc;border-top:1px solid #e2e8f0;">
  <p style="margin:0;font-size:11px;color:#94a3b8;text-align:center;line-height:1.6;">
    SuperNews — Australian Superannuation Intelligence<br>
    {len(articles)} articles from {len(all_sources)} sources &middot; {date_range}<br>
    Generated {now.strftime('%d %b %Y %H:%M')}<br><br>
    <a href="#" style="color:#3b82f6;text-decoration:none;">Unsubscribe</a> &middot;
    <a href="#" style="color:#3b82f6;text-decoration:none;">Manage preferences</a>
  </p>
</td></tr>

</table>
</td></tr>
</table>
</body></html>"""

    return html


# ─── Send via Brevo API ───────────────────────────────────────────────────────

def send_email(config, subject, html_body, dry_run=False):
    """Send the digest email via Brevo's transactional API."""
    recipients = [{"email": r["email"], "name": r.get("name", "")} for r in config["recipients"]]

    payload = {
        "sender": {
            "name": config.get("sender_name", "SuperNews Digest"),
            "email": config["sender_email"],
        },
        "to": recipients,
        "subject": subject,
        "htmlContent": html_body,
    }

    if dry_run:
        print(f"\n  DRY RUN — would send to {len(recipients)} recipient(s):")
        for r in recipients:
            print(f"    -> {r['name']} <{r['email']}>")
        print(f"    Subject: {subject}")
        print(f"    HTML length: {len(html_body):,} chars")
        preview_path = SCRIPT_DIR / "email_preview.html"
        with open(preview_path, "w") as f:
            f.write(html_body)
        print(f"    Preview saved to: {preview_path}")
        return True

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": config["brevo_api_key"],
    }

    try:
        resp = requests.post(BREVO_API_URL, json=payload, headers=headers, timeout=30)
        if resp.status_code in (200, 201):
            msg_id = resp.json().get("messageId", "unknown")
            print(f"  Email sent successfully!")
            print(f"    Message ID: {msg_id}")
            print(f"    Recipients: {len(recipients)}")
            return True
        else:
            print(f"  Brevo API error ({resp.status_code}):")
            print(f"    {resp.text}")
            return False
    except Exception as e:
        print(f"  Failed to send: {e}")
        return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Send SuperNews digest via Brevo")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview email without sending (saves email_preview.html)")
    parser.add_argument("--force", action="store_true",
                        help="Ignore last_sent marker, send last 7 days of articles")
    parser.add_argument("--days", type=int, default=None,
                        help="Override: send articles from the last N days")
    args = parser.parse_args()

    print("=" * 54)
    print("  SuperNews — Digest Sender v3")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M AEST')}")
    print("=" * 54)

    config = load_config(dry_run=args.dry_run)
    conn = get_db()

    # Determine the "since" cutoff
    if args.days:
        since_dt = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
        print(f"\n  Mode: last {args.days} days (--days override)")
    elif args.force:
        since_dt = (datetime.now(timezone.utc) - timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()
        print(f"\n  Mode: last {DEFAULT_LOOKBACK_DAYS} days (--force)")
    else:
        last_sent = get_last_sent(conn)
        if last_sent:
            since_dt = last_sent
            print(f"\n  Mode: since last send ({fmt_date(last_sent) or last_sent[:16]})")
        else:
            since_dt = (datetime.now(timezone.utc) - timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()
            print(f"\n  Mode: last {DEFAULT_LOOKBACK_DAYS} days (no previous send recorded)")

    articles = fetch_articles_since(conn, since_dt)

    if not articles:
        print("\n  No new articles since last send. Nothing to do.")
        conn.close()
        return

    # Show summary
    topic_groups = group_by_topic(articles)
    print(f"\n  {len(articles)} articles across {len(topic_groups)} topics:")
    for topic_name, topic_articles in topic_groups:
        print(f"    {topic_name}: {len(topic_articles)}")

    subject = build_subject(config, articles, since_dt)
    html = build_html(articles, since_dt, config)
    print(f"\n  Email composed ({len(html):,} chars)")
    print(f"  Subject: {subject}")

    success = send_email(config, subject, html, dry_run=args.dry_run)

    if success and not args.dry_run:
        set_last_sent(conn)
        print("\n  Digest delivered. last_sent marker updated.")
    elif success and args.dry_run:
        print("\n  Dry run complete. Open email_preview.html to review.")
    else:
        print("\n  Delivery failed. Check config.json and try again.")
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
