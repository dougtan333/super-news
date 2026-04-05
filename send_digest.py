#!/usr/bin/env python3
"""
SuperNews — Weekly Digest Email Sender (Brevo API) v2
=====================================================
Reads data.json, composes an HTML email grouped by tier with source context,
and sends via Brevo's transactional email API.

Run: python3 send_digest.py
     python3 send_digest.py --dry-run   (preview without sending)

Requires: pip install requests
Config:   config.json in the same directory
"""

import json
import sys
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter

try:
    import requests
except ImportError:
    print("Missing 'requests'. Run: pip install requests")
    sys.exit(1)

# ─── Paths ────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
DATA_PATH = SCRIPT_DIR / "data.json"

BREVO_API_URL = "https://api.brevo.com/v3/smtp/email"

# ─── Tier Styling ─────────────────────────────────────────────────────────────

TIER_META = {
    1: {"name": "Media Articles",            "icon": "&#x1F4F0;", "color": "#1a56db", "bg": "#eff6ff"},
    2: {"name": "Regulatory & Government",   "icon": "&#x1F3DB;", "color": "#057a55", "bg": "#f0fdf4"},
    3: {"name": "Industry Bodies & Research", "icon": "&#x1F4CA;", "color": "#9f580a", "bg": "#fffbeb"},
    4: {"name": "Social Media & Blogs",      "icon": "&#x1F4AC;", "color": "#6c2bd9", "bg": "#faf5ff"},
}


# ─── Load Config & Data ───────────────────────────────────────────────────────

def load_config(dry_run=False):
    if not CONFIG_PATH.exists():
        print(f"Error: Config not found at {CONFIG_PATH}")
        print("  Copy config.json.example to config.json and add your Brevo API key.")
        sys.exit(1)
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    if not dry_run and config.get("brevo_api_key", "").startswith("YOUR_"):
        print("Error: Brevo API key not configured.")
        print("  Edit config.json and replace YOUR_BREVO_API_KEY_HERE with your actual key.")
        print("  Get one free at: https://app.brevo.com/settings/keys/api")
        sys.exit(1)
    return config


def load_data():
    if not DATA_PATH.exists():
        print(f"Error: No data.json found at {DATA_PATH}")
        print("  Run collect.py first, or wait for the Sunday collection task.")
        sys.exit(1)
    with open(DATA_PATH) as f:
        data = json.load(f)
    articles = data.get("articles", [])
    if not articles:
        print("Error: data.json contains no articles. Nothing to send.")
        sys.exit(1)
    return data


# ─── Select Top Articles ──────────────────────────────────────────────────────

def select_articles(data, config):
    """Pick the top articles per tier based on config limits."""
    articles = data["articles"]
    limits = config.get("max_articles_per_tier", {"1": 8, "2": 5, "3": 4, "4": 4})
    selected = {1: [], 2: [], 3: [], 4: []}
    for a in articles:
        tier = a["tier"]
        limit = int(limits.get(str(tier), 5))
        if len(selected[tier]) < limit:
            selected[tier].append(a)
    return selected


# ─── Build Subject Line with Week Range ───────────────────────────────────────

def build_subject(config, data, selected):
    """Build email subject with week range and article count."""
    total = sum(len(arts) for arts in selected.values())
    prefix = config.get("subject_prefix", "SuperNews Weekly Digest")

    # Determine the week range from the articles
    now = datetime.now()
    week_start = now - timedelta(days=7)
    week_start_str = week_start.strftime("%-d %b")
    week_end_str = now.strftime("%-d %b %Y")

    return f"{prefix} — {week_start_str} to {week_end_str} ({total} articles)"


# ─── Build Source Summary ─────────────────────────────────────────────────────

def build_source_summary(selected):
    """Create a brief source breakdown for the email header."""
    all_sources = []
    for tier_articles in selected.values():
        for a in tier_articles:
            all_sources.append(a.get("source", "Unknown"))
    counter = Counter(all_sources)
    top_sources = counter.most_common(6)
    parts = [f"{name} ({count})" for name, count in top_sources]
    return ", ".join(parts)


# ─── Build HTML Email ─────────────────────────────────────────────────────────

def build_html(selected, data, config):
    """Compose the HTML email body with source context."""
    now = datetime.now()
    week_start = now - timedelta(days=7)
    week_range = f"{week_start.strftime('%-d %B')} — {now.strftime('%-d %B %Y')}"
    total = sum(len(arts) for arts in selected.values())
    tier_count = sum(1 for t in selected.values() if t)
    funds_lookup = {f["fund_id"]: f["display_name"] for f in data.get("funds", [])}
    source_summary = build_source_summary(selected)

    # Count unique sources
    all_sources = set()
    for tier_articles in selected.values():
        for a in tier_articles:
            all_sources.add(a.get("source", "Unknown"))

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SuperNews Digest — {week_range}</title>
</head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:24px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08);">

<!-- Header -->
<tr><td style="background:#0f172a;padding:28px 32px;">
  <h1 style="margin:0;color:white;font-size:22px;font-weight:700;">&#x1F998; Super<span style="color:#60a5fa;">News</span></h1>
  <p style="margin:6px 0 0;color:#e2e8f0;font-size:14px;font-weight:600;">Week of {week_range}</p>
  <p style="margin:4px 0 0;color:#94a3b8;font-size:12px;">
    {total} articles from {len(all_sources)} sources across {tier_count} tiers
  </p>
</td></tr>

<!-- Source Summary Bar -->
<tr><td style="padding:12px 32px;background:#f8fafc;border-bottom:1px solid #e2e8f0;">
  <p style="margin:0;font-size:11px;color:#64748b;line-height:1.5;">
    <strong style="color:#475569;">Top sources:</strong> {source_summary}
  </p>
</td></tr>
"""

    # Build each tier section
    for tier in [1, 2, 3, 4]:
        arts = selected[tier]
        if not arts:
            continue
        meta = TIER_META[tier]

        # Count unique sources in this tier
        tier_sources = set(a.get("source", "") for a in arts)
        tier_source_str = f" from {len(tier_sources)} source{'s' if len(tier_sources) != 1 else ''}" if len(tier_sources) > 1 else ""

        html += f"""
<!-- Tier {tier}: {meta['name']} -->
<tr><td style="padding:24px 32px 8px;">
  <table width="100%" cellpadding="0" cellspacing="0">
  <tr><td style="background:{meta['bg']};border-left:4px solid {meta['color']};padding:10px 14px;border-radius:6px;">
    <span style="font-size:14px;font-weight:700;color:{meta['color']};">{meta['icon']} {meta['name']}</span>
    <span style="float:right;font-size:12px;color:#64748b;">{len(arts)} article{'s' if len(arts)!=1 else ''}{tier_source_str}</span>
  </td></tr>
  </table>
</td></tr>
"""
        for a in arts:
            title = a.get("title", "Untitled")
            url = a.get("url", "#")
            source = a.get("source", "")
            summary = (a.get("summary") or "")[:200]
            topic = a.get("topic", "")
            published = a.get("published_at", "")
            fund_ids = a.get("fund_ids", [])
            fund_names = [funds_lookup.get(fid, fid) for fid in fund_ids]

            # Format published date if available
            date_str = ""
            if published:
                try:
                    dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                    date_str = dt.strftime("%-d %b")
                except (ValueError, TypeError):
                    date_str = ""

            fund_tags_html = "".join(
                f'<span style="display:inline-block;background:#dbeafe;color:#1d4ed8;'
                f'font-size:10px;font-weight:600;padding:2px 7px;border-radius:12px;margin-right:4px;">'
                f'{fn}</span>' for fn in fund_names
            )

            meta_parts = [source]
            if topic:
                meta_parts.append(topic)
            if date_str:
                meta_parts.append(date_str)
            meta_line = " &middot; ".join(meta_parts)

            html += f"""
<tr><td style="padding:6px 32px;">
  <table width="100%" cellpadding="0" cellspacing="0" style="border-bottom:1px solid #f1f5f9;padding-bottom:12px;margin-bottom:4px;">
  <tr><td>
    <p style="margin:0 0 2px;font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.4px;">
      {meta_line}
    </p>
    <p style="margin:0 0 4px;font-size:14px;font-weight:600;line-height:1.4;">
      <a href="{url}" style="color:#1e293b;text-decoration:none;">{title}</a>
    </p>
    {f'<p style="margin:0 0 6px;font-size:12px;color:#64748b;line-height:1.4;">{summary}</p>' if summary else ''}
    {fund_tags_html}
  </td></tr>
  </table>
</td></tr>
"""

    # Footer
    html += f"""
<!-- Footer -->
<tr><td style="padding:24px 32px;background:#f8fafc;border-top:1px solid #e2e8f0;">
  <p style="margin:0;font-size:11px;color:#94a3b8;text-align:center;line-height:1.6;">
    SuperNews — Australian Superannuation Intelligence<br>
    {total} articles from {len(all_sources)} sources &middot; Week of {week_range}<br>
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
        # Save preview
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
    parser = argparse.ArgumentParser(description="Send SuperNews weekly digest via Brevo")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview email without sending (saves email_preview.html)")
    args = parser.parse_args()

    print("=" * 54)
    print("  SuperNews — Weekly Digest Sender v2")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M AEST')}")
    print("=" * 54)

    config = load_config(dry_run=args.dry_run)
    data = load_data()
    selected = select_articles(data, config)

    total = sum(len(arts) for arts in selected.values())
    print(f"\n  Selected {total} articles:")
    for tier in [1, 2, 3, 4]:
        count = len(selected[tier])
        if count:
            print(f"    Tier {tier} ({TIER_META[tier]['name']}): {count}")

    subject = build_subject(config, data, selected)
    html = build_html(selected, data, config)
    print(f"\n  Email composed ({len(html):,} chars)")
    print(f"  Subject: {subject}")

    success = send_email(config, subject, html, dry_run=args.dry_run)

    if success and not args.dry_run:
        print("\n  Digest delivered. Have a great week!")
    elif success and args.dry_run:
        print("\n  Dry run complete. Open email_preview.html to review.")
    else:
        print("\n  Delivery failed. Check config.json and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
