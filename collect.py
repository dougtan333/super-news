#!/usr/bin/env python3
"""
Australian Superannuation News Collector — v2
==============================================
Collects articles across four content tiers and stores them in SQLite.
Exports to data.json for the web interface and email digest.

Tiers:
  1 — Media Articles (trade press RSS + Google News RSS)
  2 — Regulatory & Government (APRA, ASIC, ATO, Treasury, Fair Work, AFCA, OAIC, Parliament)
  3 — Industry Bodies & Research (ASFA, SMC, Conexus, FSC, Super Consumers, ratings agencies)
  4 — Fund Newsrooms & Social (fund media centres, blogs, social discovery RSS)

Run:  python3 collect.py              # full collection
      python3 collect.py --export     # re-export data.json only (no scraping)

Requires: pip install feedparser requests beautifulsoup4 lxml
"""

import sqlite3
import hashlib
import re
import json
import time
import sys
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode, urljoin, quote

try:
    import feedparser
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run:")
    print("  pip install feedparser requests beautifulsoup4 lxml")
    sys.exit(1)

# ─── Configuration ────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "super_news.db"
DATA_JSON_PATH = Path(__file__).parent / "data.json"
TAXONOMY_PATH = Path(__file__).parent / "taxonomy.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

REQUEST_DELAY = 1.5          # seconds between requests (polite crawling)
COLLECTION_WINDOW_DAYS = 2   # daily run with 1-day overlap for safety
EXPORT_WINDOW_DAYS = 30      # articles retained in data.json (rolling month)
DIGEST_DEFAULT_DAYS = 7      # default window for email digest if no last_sent marker


# ═══════════════════════════════════════════════════════════════════════════════
#  TAXONOMY LOADER — loads funds, keywords, queries from taxonomy.json
# ═══════════════════════════════════════════════════════════════════════════════

def load_taxonomy():
    """Load taxonomy.json and return (funds, topic_keywords, queries, relevance_kw, negative_filters, media_markers)."""
    if not TAXONOMY_PATH.exists():
        print(f"WARNING: taxonomy.json not found at {TAXONOMY_PATH}")
        print("  Using built-in fallback data. Create taxonomy.json for full coverage.")
        return _fallback_taxonomy()

    with open(TAXONOMY_PATH) as f:
        tax = json.load(f)

    # Flatten fund groups into single list
    funds_data = tax.get("funds", {})
    funds = []
    for group in ["public_offer", "non_public_offer", "pooled_super_trusts", "smsf"]:
        funds.extend(funds_data.get(group, []))

    topic_keywords = tax.get("topic_keywords", {})
    google_news_queries = [q for q in tax.get("google_news_queries", []) if not q.startswith("_")]
    relevance_keywords = tax.get("relevance_keywords", [])
    negative_filters = tax.get("negative_filters", [])
    media_markers = tax.get("media_markers", [])

    return funds, topic_keywords, google_news_queries, relevance_keywords, negative_filters, media_markers


def _fallback_taxonomy():
    """Minimal built-in data if taxonomy.json is missing."""
    funds = [
        {"fund_id": "australiansuper", "display_name": "AustralianSuper",
         "aliases": ["AustralianSuper", "Australian Super"], "fund_type": "Industry"},
        {"fund_id": "art", "display_name": "Australian Retirement Trust",
         "aliases": ["Australian Retirement Trust", "ART"], "fund_type": "Industry"},
        {"fund_id": "smsf", "display_name": "SMSF",
         "aliases": ["SMSF", "self managed super"], "fund_type": "Self-Managed"},
    ]
    topic_keywords = {"General": ["superannuation", "super fund", "retirement"]}
    queries = ["Australian superannuation news"]
    relevance = ["superannuation", "super fund", "retirement", "APRA", "ASIC"]
    negatives = ["rugby", "supercars", "super mario", "supermarket"]
    markers = ["media release", "newsroom", "news"]
    return funds, topic_keywords, queries, relevance, negatives, markers


# Load taxonomy at module level
FUNDS, TOPIC_KEYWORDS, GOOGLE_NEWS_QUERIES, RELEVANCE_KEYWORDS, NEGATIVE_FILTERS, MEDIA_MARKERS = load_taxonomy()

# Pre-compile lowercase sets for fast matching
_RELEVANCE_LOWER = [kw.lower() for kw in RELEVANCE_KEYWORDS]
_NEGATIVE_LOWER = [nf.lower() for nf in NEGATIVE_FILTERS]

print(f"  Taxonomy loaded: {len(FUNDS)} funds, {len(TOPIC_KEYWORDS)} topics, "
      f"{len(GOOGLE_NEWS_QUERIES)} queries, {len(NEGATIVE_FILTERS)} negative filters")


# ═══════════════════════════════════════════════════════════════════════════════
#  TIER 1 — MEDIA ARTICLES
#  Trade press RSS feeds + Google News discovery queries
# ═══════════════════════════════════════════════════════════════════════════════

TIER1_RSS_FEEDS = [
    # ── Specialist super / investment trade media ──
    {"name": "InvestorDaily",
     "url": "https://www.investordaily.com.au/feed",
     "fallback_url": "https://news.google.com/rss/search?q=site:investordaily.com.au+superannuation&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "Super Review",
     "url": "https://www.superreview.com.au/feed",
     "fallback_url": "https://news.google.com/rss/search?q=site:superreview.com.au&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "Investment Magazine",
     "url": "https://www.investmentmagazine.com.au/feed/",
     "fallback_url": "https://news.google.com/rss/search?q=site:investmentmagazine.com.au&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "Financial Standard",
     "url": "https://www.financialstandard.com.au/rss",
     "fallback_url": "https://news.google.com/rss/search?q=site:financialstandard.com.au+superannuation&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "Professional Planner",
     "url": "https://www.professionalplanner.com.au/feed/",
     "fallback_url": "https://news.google.com/rss/search?q=site:professionalplanner.com.au+super&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "Money Management",
     "url": "https://www.moneymanagement.com.au/feed",
     "fallback_url": "https://news.google.com/rss/search?q=site:moneymanagement.com.au+superannuation&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "SMSF Adviser",
     "url": "https://www.smsfadviser.com/feed",
     "fallback_url": "https://news.google.com/rss/search?q=site:smsfadviser.com+smsf&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "ifa (Independent Financial Adviser)",
     "url": "https://www.ifa.com.au/feed",
     "fallback_url": None},
    {"name": "Accountants Daily",
     "url": "https://www.accountantsdaily.com.au/feed",
     "fallback_url": None},
    # ── Mainstream business desks (RSS where available) ──
    {"name": "ABC Business",
     "url": "https://www.abc.net.au/news/feed/2942460/rss.xml",
     "fallback_url": None},
    {"name": "The Guardian Australia — Business",
     "url": "https://www.theguardian.com/au/business/rss",
     "fallback_url": None},
    {"name": "Reuters — Australia",
     "url": "https://news.google.com/rss/search?q=site:reuters.com+australia+superannuation&hl=en-AU&gl=AU&ceid=AU:en",
     "fallback_url": None},
]


# ═══════════════════════════════════════════════════════════════════════════════
#  TIER 2 — REGULATORY & GOVERNMENT
#  APRA, ASIC, ATO, Treasury, Fair Work, AFCA, OAIC, Parliament
# ═══════════════════════════════════════════════════════════════════════════════

TIER2_SCRAPE_SOURCES = [
    {"name": "APRA — News & Updates",
     "url": "https://www.apra.gov.au/news-and-publications",
     "base_url": "https://www.apra.gov.au",
     "filter_keywords": ["super", "fund", "trustee", "rse", "sps", "retirement", "prudential"],
     "body": "APRA has released new prudential information regarding superannuation funds."},
    {"name": "APRA — Superannuation Statistics",
     "url": "https://www.apra.gov.au/quarterly-superannuation-statistics",
     "base_url": "https://www.apra.gov.au",
     "filter_keywords": ["statistic", "quarterly", "data", "fund", "super", "asset"],
     "body": "APRA has published updated quarterly superannuation fund statistics."},
    {"name": "ASIC — News Centre",
     "url": "https://asic.gov.au/about-asic/news-centre/",
     "base_url": "https://asic.gov.au",
     "filter_keywords": ["super", "fund", "retirement", "trustee", "insurance", "disclosure"],
     "body": "ASIC has published regulatory guidance or enforcement action on superannuation."},
    {"name": "ASIC — Superannuation",
     "url": "https://asic.gov.au/regulatory-resources/superannuation-funds/",
     "base_url": "https://asic.gov.au",
     "filter_keywords": ["super", "fund", "retirement", "trustee", "design", "distribution"],
     "body": "ASIC regulatory resources for superannuation trustees and consumers."},
    {"name": "ATO — Super for Individuals",
     "url": "https://www.ato.gov.au/individuals-and-families/super-for-individuals-and-families/super",
     "base_url": "https://www.ato.gov.au",
     "filter_keywords": ["super", "contribution", "fund", "retirement", "concessional", "cap"],
     "body": "ATO has released new information on superannuation obligations and tax rules."},
    {"name": "ATO — Super for Employers (Payday Super)",
     "url": "https://www.ato.gov.au/businesses-and-organisations/super-for-employers",
     "base_url": "https://www.ato.gov.au",
     "filter_keywords": ["super", "employer", "payday", "guarantee", "contribution", "stp"],
     "body": "ATO guidance on employer superannuation obligations including payday super."},
    {"name": "Treasury — Superannuation",
     "url": "https://treasury.gov.au/superannuation",
     "base_url": "https://treasury.gov.au",
     "filter_keywords": ["super", "retirement", "fund", "contribution", "tax", "consultation"],
     "body": "Treasury has published policy documents relating to superannuation reform."},
    {"name": "Treasury — Consultations",
     "url": "https://treasury.gov.au/consultation",
     "base_url": "https://treasury.gov.au",
     "filter_keywords": ["super", "retirement", "fund", "objective", "tax", "member"],
     "body": "Treasury consultation on superannuation policy or retirement income."},
    {"name": "Fair Work — Superannuation",
     "url": "https://www.fairwork.gov.au/pay-and-wages/paying-super",
     "base_url": "https://www.fairwork.gov.au",
     "filter_keywords": ["super", "employer", "payday", "entitlement", "unpaid"],
     "body": "Fair Work information about employer superannuation obligations."},
    {"name": "AFCA — Publications & Decisions",
     "url": "https://www.afca.org.au/news",
     "base_url": "https://www.afca.org.au",
     "filter_keywords": ["super", "complaint", "death benefit", "insurance", "fund", "member"],
     "body": "AFCA has published complaint data or decisions relating to superannuation."},
    {"name": "OAIC — Notifiable Data Breaches",
     "url": "https://www.oaic.gov.au/privacy/notifiable-data-breaches",
     "base_url": "https://www.oaic.gov.au",
     "filter_keywords": ["super", "fund", "breach", "data", "financial", "privacy"],
     "body": "OAIC data breach notification relevant to superannuation fund members."},
    {"name": "Parliament — Super Inquiries",
     "url": "https://www.aph.gov.au/Parliamentary_Business/Committees/Senate/Economics",
     "base_url": "https://www.aph.gov.au",
     "filter_keywords": ["super", "retirement", "fund", "inquiry", "pension", "objective"],
     "body": "Parliamentary committee inquiry or hearing relating to superannuation."},
]


# ═══════════════════════════════════════════════════════════════════════════════
#  TIER 3 — INDUSTRY BODIES & RESEARCH
#  Peak bodies, research institutes, ratings agencies, consumer advocates
# ═══════════════════════════════════════════════════════════════════════════════

TIER3_SCRAPE_SOURCES = [
    {"name": "ASFA — Media Releases",
     "url": "https://www.superannuation.asn.au/media-and-publications/media-releases/",
     "base_url": "https://www.superannuation.asn.au",
     "filter_keywords": ["super", "fund", "retirement", "policy", "research", "member"],
     "body": "ASFA has published new research or policy analysis on superannuation."},
    {"name": "Super Members Council",
     "url": "https://www.supermemberscouncil.com.au/news/",
     "base_url": "https://www.supermemberscouncil.com.au",
     "filter_keywords": ["super", "member", "fund", "return", "retirement", "profit"],
     "body": "The Super Members Council has released analysis on fund performance and member outcomes."},
    {"name": "The Conexus Institute",
     "url": "https://theconexusinstitute.org.au/recent-thinking/",
     "base_url": "https://theconexusinstitute.org.au",
     "filter_keywords": ["super", "retirement", "investment", "fund", "decumulation", "state of super"],
     "body": "The Conexus Institute has published retirement income and investment research."},
    {"name": "Financial Services Council",
     "url": "https://www.fsc.org.au/news",
     "base_url": "https://www.fsc.org.au",
     "filter_keywords": ["super", "fund", "retirement", "policy", "investment", "advice"],
     "body": "The Financial Services Council has published a policy position on superannuation."},
    {"name": "Super Consumers Australia",
     "url": "https://www.superconsumers.com.au/news",
     "base_url": "https://www.superconsumers.com.au",
     "filter_keywords": ["super", "consumer", "fee", "fund", "member", "complaint", "insurance"],
     "body": "Super Consumers Australia has published consumer advocacy research on superannuation."},
    {"name": "SMSF Association",
     "url": "https://www.smsfa.org.au/news",
     "base_url": "https://www.smsfa.org.au",
     "filter_keywords": ["smsf", "self-managed", "trustee", "compliance", "audit", "fund"],
     "body": "The SMSF Association has published guidance or advocacy on self-managed super."},
    {"name": "Morningstar Australia — Super",
     "url": "https://www.morningstar.com.au/insights/superannuation",
     "base_url": "https://www.morningstar.com.au",
     "filter_keywords": ["super", "fund", "return", "rating", "award", "analysis"],
     "body": "Morningstar has released superannuation ratings or fund performance analysis."},
    {"name": "Chant West",
     "url": "https://www.chantwest.com.au/resources/media-releases",
     "base_url": "https://www.chantwest.com.au",
     "filter_keywords": ["super", "fund", "return", "performance", "award", "rating"],
     "body": "Chant West has published fund ratings, performance data or awards."},
    {"name": "SuperRatings",
     "url": "https://www.superratings.com.au/media/",
     "base_url": "https://www.superratings.com.au",
     "filter_keywords": ["super", "fund", "return", "performance", "rating", "award"],
     "body": "SuperRatings has published fund performance data or ratings."},
    {"name": "Rainmaker Information",
     "url": "https://www.rainmaker.com.au/media",
     "base_url": "https://www.rainmaker.com.au",
     "filter_keywords": ["super", "fund", "fee", "performance", "research"],
     "body": "Rainmaker has published superannuation research or fee benchmarking data."},
]


# ═══════════════════════════════════════════════════════════════════════════════
#  TIER 4 — FUND NEWSROOMS, BLOGS & SOCIAL DISCOVERY
#  Direct fund media centres + social/blog backstop RSS
# ═══════════════════════════════════════════════════════════════════════════════

TIER4_FUND_NEWSROOMS = [
    # ── Major industry funds ──
    {"name": "AustralianSuper Newsroom",
     "url": "https://www.australiansuper.com/about-us/media-centre",
     "fund_id": "australiansuper",
     "base_url": "https://www.australiansuper.com",
     "filter_keywords": ["media", "news", "announcement", "release", "invest", "member", "performance"]},
    {"name": "Australian Retirement Trust Media",
     "url": "https://www.australianretirementtrust.com.au/about/media-centre",
     "fund_id": "art",
     "base_url": "https://www.australianretirementtrust.com.au",
     "filter_keywords": ["news", "media", "member", "invest", "award", "release"]},
    {"name": "Aware Super Newsroom",
     "url": "https://aware.com.au/about/media-centre",
     "fund_id": "aware",
     "base_url": "https://aware.com.au",
     "filter_keywords": ["news", "media", "award", "invest", "member", "release"]},
    {"name": "UniSuper Media Centre",
     "url": "https://www.unisuper.com.au/about-us/media-centre",
     "fund_id": "unisuper",
     "base_url": "https://www.unisuper.com.au",
     "filter_keywords": ["media", "news", "investment", "member", "return", "release"]},
    {"name": "HESTA Media",
     "url": "https://www.hesta.com.au/about/media",
     "fund_id": "hesta",
     "base_url": "https://www.hesta.com.au",
     "filter_keywords": ["media", "news", "release", "member", "invest", "climate"]},
    {"name": "Cbus Media Releases",
     "url": "https://www.cbussuper.com.au/about-cbus/media-centre",
     "fund_id": "cbus",
     "base_url": "https://www.cbussuper.com.au",
     "filter_keywords": ["media", "news", "release", "member", "invest", "construction"]},
    {"name": "Hostplus Media",
     "url": "https://hostplus.com.au/about-us/media",
     "fund_id": "hostplus",
     "base_url": "https://hostplus.com.au",
     "filter_keywords": ["media", "news", "release", "member", "hospitality"]},
    {"name": "Rest News",
     "url": "https://rest.com.au/about-rest/news",
     "fund_id": "rest",
     "base_url": "https://rest.com.au",
     "filter_keywords": ["news", "media", "member", "invest", "retail"]},
    {"name": "Brighter Super News",
     "url": "https://www.brightersuper.com.au/about/news-and-media",
     "fund_id": "brightersuper",
     "base_url": "https://www.brightersuper.com.au",
     "filter_keywords": ["news", "media", "member", "merge", "release"]},
    {"name": "TelstraSuper News",
     "url": "https://www.telstrasuper.com.au/about/news",
     "fund_id": "telstrasuper",
     "base_url": "https://www.telstrasuper.com.au",
     "filter_keywords": ["news", "media", "member", "update", "release"]},
    # ── Retail / provider newsrooms ──
    {"name": "AMP News",
     "url": "https://www.amp.com.au/news",
     "fund_id": "amp",
     "base_url": "https://www.amp.com.au",
     "filter_keywords": ["news", "media", "super", "wealth", "investor", "release"]},
    {"name": "Insignia Financial Media",
     "url": "https://www.insigniafinancial.com.au/about-us/media-centre",
     "fund_id": "insignia",
     "base_url": "https://www.insigniafinancial.com.au",
     "filter_keywords": ["media", "news", "release", "mlc", "ioof", "super", "wealth"]},
    {"name": "CFS News & Market Updates",
     "url": "https://www.cfs.com.au/about-us/news.html",
     "fund_id": "cfs",
     "base_url": "https://www.cfs.com.au",
     "filter_keywords": ["news", "update", "super", "market", "performance"]},
    # ── Public sector ──
    {"name": "CSC Member Updates",
     "url": "https://www.csc.gov.au/members/news-and-updates",
     "fund_id": "csc",
     "base_url": "https://www.csc.gov.au",
     "filter_keywords": ["news", "update", "member", "super", "investment"]},
]

TIER4_SOCIAL_RSS = [
    # Discovery backstop — Google News RSS for blog/social content
    {"name": "Super Blog & Social Discovery",
     "url": "https://news.google.com/rss/search?q=superannuation+blog+OR+opinion+Australia&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "Super LinkedIn/Social Discovery",
     "url": "https://news.google.com/rss/search?q=superannuation+Australia+site:linkedin.com+OR+site:medium.com&hl=en-AU&gl=AU&ceid=AU:en"},
    {"name": "Super Fund Scam & Outage Alerts",
     "url": "https://news.google.com/rss/search?q=superannuation+scam+OR+outage+OR+breach+Australia&hl=en-AU&gl=AU&ceid=AU:en"},
]


# ═══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS funds (
            fund_id      TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            fund_type    TEXT,
            aliases      TEXT   -- JSON array
        );

        CREATE TABLE IF NOT EXISTS articles (
            article_id     TEXT PRIMARY KEY,   -- SHA-256 prefix of URL
            url            TEXT UNIQUE NOT NULL,
            title          TEXT NOT NULL,
            source         TEXT NOT NULL,
            tier           INTEGER NOT NULL,   -- 1, 2, 3, or 4
            topic_category TEXT,
            summary        TEXT,
            published_at   TEXT,                -- RFC 3339 ISO 8601
            collected_at   TEXT NOT NULL        -- RFC 3339 ISO 8601
        );

        CREATE TABLE IF NOT EXISTS article_fund_tags (
            article_id TEXT NOT NULL,
            fund_id    TEXT NOT NULL,
            PRIMARY KEY (article_id, fund_id),
            FOREIGN KEY (article_id) REFERENCES articles(article_id) ON DELETE CASCADE,
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id)
        );

        CREATE TABLE IF NOT EXISTS digest_state (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_articles_collected_at ON articles(collected_at DESC);
        CREATE INDEX IF NOT EXISTS idx_articles_tier ON articles(tier);
        CREATE INDEX IF NOT EXISTS idx_articles_topic ON articles(topic_category);
        CREATE INDEX IF NOT EXISTS idx_article_fund_tags_fund ON article_fund_tags(fund_id);
    """)
    conn.commit()


def seed_funds(conn):
    """Populate funds table from FUNDS taxonomy."""
    existing = set(row[0] for row in conn.execute("SELECT fund_id FROM funds"))
    for fund in FUNDS:
        if fund["fund_id"] not in existing:
            aliases = json.dumps(fund.get("aliases", []))
            conn.execute(
                "INSERT INTO funds (fund_id, display_name, fund_type, aliases) VALUES (?, ?, ?, ?)",
                (fund["fund_id"], fund["display_name"], fund.get("fund_type"), aliases),
            )
    conn.commit()


def make_article_id(url: str) -> str:
    """Generate stable article ID from URL hash."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def strip_html(text: str) -> str:
    """Remove HTML tags and decode HTML entities from text."""
    if not text:
        return ""
    import html
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return text


def normalise_text(text: str) -> str:
    """Strip HTML tags, decode entities, collapse whitespace, and strip."""
    if not text:
        return ""
    cleaned = strip_html(text)
    return " ".join(cleaned.split())


def extract_date_near_element(element) -> str | None:
    """
    Try to find a publication date near an <a> element in the DOM.
    Checks: sibling text, parent text, <time> tags, common date classes.
    Returns ISO date string or None.
    """
    from dateutil import parser as dateparser

    # 1. Look for <time> tag nearby (sibling or parent's children)
    parent = element.parent
    if parent:
        time_tag = parent.find("time")
        if time_tag:
            dt_attr = time_tag.get("datetime", "") or time_tag.get_text(strip=True)
            if dt_attr:
                try:
                    return dateparser.parse(dt_attr, dayfirst=True).replace(tzinfo=timezone.utc).isoformat()
                except (ValueError, TypeError):
                    pass

    # 2. Look for date-like classes in siblings or parent
    if parent:
        for cls_pattern in ["date", "time", "published", "posted", "meta"]:
            date_el = parent.find(class_=lambda c: c and cls_pattern in c.lower() if c else False)
            if date_el:
                text = date_el.get_text(strip=True)
                try:
                    return dateparser.parse(text, dayfirst=True).replace(tzinfo=timezone.utc).isoformat()
                except (ValueError, TypeError):
                    pass

    # 3. Regex for common date patterns in surrounding text
    search_text = parent.get_text(" ", strip=True) if parent else ""
    # Australian format: "5 Apr 2026", "05/04/2026", "5 April 2026", "2026-04-05"
    date_patterns = [
        r'\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}',
        r'\d{4}-\d{2}-\d{2}',
        r'\d{1,2}/\d{1,2}/\d{4}',
    ]
    for pat in date_patterns:
        match = re.search(pat, search_text, re.IGNORECASE)
        if match:
            try:
                return dateparser.parse(match.group(), dayfirst=True).replace(tzinfo=timezone.utc).isoformat()
            except (ValueError, TypeError):
                continue

    return None


def is_super_relevant(text: str) -> bool:
    """
    Filter relevance using taxonomy.
    - Accept if any RELEVANCE_KEYWORD found
    - Reject if any NEGATIVE_FILTER found AND no relevance keyword found
    """
    if not text:
        return False
    lowered = text.lower()

    # Check for any relevance keyword
    has_relevance = any(kw in lowered for kw in _RELEVANCE_LOWER)

    # Check for negative filters
    has_negative = any(nf in lowered for nf in _NEGATIVE_LOWER)

    # Accept if has relevance keyword, reject if has negative AND no relevance
    if has_relevance:
        return True
    if has_negative and not has_relevance:
        return False

    # Default: accept if no negative filters (and relevance check above)
    return has_relevance


def classify_topic(title: str, body: str = "") -> str:
    """Rule-based topic classification using keyword match scoring."""
    combined = (title + " " + body).lower()
    scores = {}
    for topic, keywords in TOPIC_KEYWORDS.items():
        scores[topic] = sum(1 for kw in keywords if kw in combined)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "General"


def tag_funds(title: str, body: str = "") -> list:
    """Return list of fund_ids mentioned in title + body text using word-boundary matching."""
    combined = (title + " " + body)
    matches = []
    for fund in FUNDS:
        aliases = fund.get("aliases", [fund["display_name"]])
        for alias in aliases:
            # Always use word-boundary matching to avoid false positives
            # e.g. "ART" must not match "start", "ING Super" must not match "investing super"
            pattern = r'\b' + re.escape(alias) + r'\b'
            if re.search(pattern, combined, re.IGNORECASE):
                matches.append(fund["fund_id"])
                break
    return matches


def save_article(conn, url, title, source, tier, topic, summary, published_at, fund_tags):
    """Insert article if not already present (dedup on URL hash). Returns True if new."""
    article_id = make_article_id(url)
    title = normalise_text(title)
    if not title or len(title) < 10:
        return False
    # Reject junk titles — archive/pagination pages, index listings
    _junk_patterns = [
        r"archives?\s*-?\s*page\s+\d+",
        r"page\s+\d+\s+of\s+\d+",
        r"^\s*index\s*$",
        r"^\s*home\s*$",
        r"^\s*search results",
    ]
    title_lower = title.lower()
    if any(re.search(pat, title_lower) for pat in _junk_patterns):
        return False
    # Reject articles with extracted dates older than the export window
    try:
        from dateutil import parser as dateparser
        pub_dt = dateparser.parse(published_at)
        if pub_dt:
            cutoff = datetime.now(timezone.utc) - timedelta(days=EXPORT_WINDOW_DAYS)
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            if pub_dt < cutoff:
                return False
    except (ValueError, TypeError):
        pass
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(
            "INSERT INTO articles "
            "(article_id, url, title, source, tier, topic_category, summary, published_at, collected_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (article_id, url, title, source, tier, topic, normalise_text(summary)[:500], published_at, now),
        )
        for fund_id in fund_tags:
            conn.execute(
                "INSERT OR IGNORE INTO article_fund_tags (article_id, fund_id) VALUES (?, ?)",
                (article_id, fund_id),
            )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def fetch_url(url: str, timeout: int = 15):
    """Fetch URL and return response or None on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp
    except (requests.RequestException, Exception):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  COLLECTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def collect_tier1_rss(conn) -> int:
    """Collect Tier 1 RSS feed articles."""
    count = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=COLLECTION_WINDOW_DAYS)

    for feed_def in TIER1_RSS_FEEDS:
        feed_url = feed_def["url"]
        feed_name = feed_def["name"]
        fallback_url = feed_def.get("fallback_url")

        # Try primary feed
        parsed = feedparser.parse(feed_url)
        if parsed.bozo and fallback_url:
            print(f"  ⚠ {feed_name}: primary feed failed, trying fallback...")
            parsed = feedparser.parse(fallback_url)
        elif parsed.bozo:
            print(f"  ✗ {feed_name}: parse error")
            continue

        for entry in parsed.entries:
            try:
                pub_time_tuple = entry.get("published_parsed", entry.get("updated_parsed"))
                pub_time = (
                    datetime(*pub_time_tuple[:6], tzinfo=timezone.utc)
                    if pub_time_tuple
                    else datetime.now(timezone.utc)
                )
                if pub_time < cutoff:
                    continue

                url = entry.link
                title = entry.title or "Untitled"
                summary = entry.get("summary", "")

                if not is_super_relevant(title + " " + summary):
                    continue

                topic = classify_topic(title, summary)
                fund_tags = tag_funds(title, summary)

                if save_article(conn, url, title, feed_name, 1, topic, summary, pub_time.isoformat(), fund_tags):
                    count += 1
            except Exception:
                pass

        time.sleep(REQUEST_DELAY)

    print(f"  → Tier 1 RSS: {count} new articles")
    return count


def collect_tier1_google_news(conn) -> int:
    """Collect Tier 1 Google News RSS queries."""
    count = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=COLLECTION_WINDOW_DAYS)

    for query in GOOGLE_NEWS_QUERIES:
        feed_url = (
            f"https://news.google.com/rss/search?q={quote(query)}&hl=en-AU&gl=AU&ceid=AU:en"
        )
        parsed = feedparser.parse(feed_url)
        if parsed.bozo:
            print(f"  ⚠ Google News query '{query[:30]}...': parse error")
            continue

        for entry in parsed.entries:
            try:
                pub_time_tuple = entry.get("published_parsed", entry.get("updated_parsed"))
                pub_time = (
                    datetime(*pub_time_tuple[:6], tzinfo=timezone.utc)
                    if pub_time_tuple
                    else datetime.now(timezone.utc)
                )
                if pub_time < cutoff:
                    continue

                url = entry.link
                title = entry.title or "Untitled"
                summary = entry.get("summary", "")

                if not is_super_relevant(title + " " + summary):
                    continue

                topic = classify_topic(title, summary)
                fund_tags = tag_funds(title, summary)

                # Extract source name from Google News title format "Headline - Source"
                gnews_source = "Google News"
                if " - " in (entry.title or ""):
                    gnews_source = entry.title.rsplit(" - ", 1)[-1].strip()
                    title = entry.title.rsplit(" - ", 1)[0].strip()

                if save_article(conn, url, title, gnews_source, 1, topic, summary, pub_time.isoformat(), fund_tags):
                    count += 1
            except Exception:
                pass

        time.sleep(REQUEST_DELAY)

    print(f"  → Tier 1 Google News: {count} new articles")
    return count


def collect_tier2_regulatory(conn) -> int:
    """Scrape Tier 2 regulatory and government sources."""
    count = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=COLLECTION_WINDOW_DAYS)

    for source in TIER2_SCRAPE_SOURCES:
        source_name = source["name"]
        source_url = source["url"]
        base_url = source.get("base_url")
        filter_kws = source.get("filter_keywords", [])

        resp = fetch_url(source_url)
        if not resp:
            print(f"  ✗ {source_name}: fetch failed")
            continue

        try:
            soup = BeautifulSoup(resp.text, "lxml")
            links = soup.find_all("a", href=True)

            for link in links:
                href = link.get("href", "")
                link_text = link.get_text(strip=True)

                # Construct absolute URL
                if href.startswith("http"):
                    url = href
                elif href.startswith("/"):
                    url = urljoin(base_url, href)
                else:
                    continue

                # Filter by keywords
                if filter_kws:
                    combined = (link_text + " " + url).lower()
                    if not any(kw in combined for kw in filter_kws):
                        continue

                title = link_text or url
                summary = source.get("body", "")

                if not is_super_relevant(title + " " + summary):
                    continue

                topic = classify_topic(title, summary)
                fund_tags = tag_funds(title, summary)
                extracted_date = extract_date_near_element(link)
                pub_time = extracted_date or datetime.now(timezone.utc).isoformat()

                if save_article(conn, url, title, source_name, 2, topic, summary, pub_time, fund_tags):
                    count += 1
        except Exception:
            print(f"  ⚠ {source_name}: parse error")

        time.sleep(REQUEST_DELAY)

    print(f"  → Tier 2 Regulatory: {count} new articles")
    return count


def collect_tier3_industry(conn) -> int:
    """Scrape Tier 3 industry body and research sources."""
    count = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=COLLECTION_WINDOW_DAYS)

    for source in TIER3_SCRAPE_SOURCES:
        source_name = source["name"]
        source_url = source["url"]
        base_url = source.get("base_url")
        filter_kws = source.get("filter_keywords", [])

        resp = fetch_url(source_url)
        if not resp:
            print(f"  ✗ {source_name}: fetch failed")
            continue

        try:
            soup = BeautifulSoup(resp.text, "lxml")
            links = soup.find_all("a", href=True)

            for link in links:
                href = link.get("href", "")
                link_text = link.get_text(strip=True)

                # Construct absolute URL
                if href.startswith("http"):
                    url = href
                elif href.startswith("/"):
                    url = urljoin(base_url, href)
                else:
                    continue

                # Filter by keywords
                if filter_kws:
                    combined = (link_text + " " + url).lower()
                    if not any(kw in combined for kw in filter_kws):
                        continue

                title = link_text or url
                summary = source.get("body", "")

                if not is_super_relevant(title + " " + summary):
                    continue

                topic = classify_topic(title, summary)
                fund_tags = tag_funds(title, summary)
                extracted_date = extract_date_near_element(link)
                pub_time = extracted_date or datetime.now(timezone.utc).isoformat()

                if save_article(conn, url, title, source_name, 3, topic, summary, pub_time, fund_tags):
                    count += 1
        except Exception:
            print(f"  ⚠ {source_name}: parse error")

        time.sleep(REQUEST_DELAY)

    print(f"  → Tier 3 Industry: {count} new articles")
    return count


def collect_tier4_newsrooms(conn) -> int:
    """Scrape Tier 4 fund newsroom pages."""
    count = 0

    for newsroom in TIER4_FUND_NEWSROOMS:
        newsroom_name = newsroom["name"]
        newsroom_url = newsroom["url"]
        fund_id = newsroom.get("fund_id")
        base_url = newsroom.get("base_url")
        filter_kws = newsroom.get("filter_keywords", [])

        resp = fetch_url(newsroom_url)
        if not resp:
            print(f"  ✗ {newsroom_name}: fetch failed")
            continue

        try:
            soup = BeautifulSoup(resp.text, "lxml")
            links = soup.find_all("a", href=True)

            for link in links:
                href = link.get("href", "")
                link_text = link.get_text(strip=True)

                # Construct absolute URL
                if href.startswith("http"):
                    url = href
                elif href.startswith("/"):
                    url = urljoin(base_url, href)
                else:
                    continue

                # Filter by keywords
                if filter_kws:
                    combined = (link_text + " " + url).lower()
                    if not any(kw in combined for kw in filter_kws):
                        continue

                title = link_text or url
                summary = ""

                if not is_super_relevant(title + " " + summary):
                    continue

                topic = classify_topic(title, summary)
                fund_tags = tag_funds(title, summary)
                if fund_id and fund_id not in fund_tags:
                    fund_tags.append(fund_id)
                extracted_date = extract_date_near_element(link)
                pub_time = extracted_date or datetime.now(timezone.utc).isoformat()

                if save_article(conn, url, title, newsroom_name, 4, topic, summary, pub_time, fund_tags):
                    count += 1
        except Exception:
            print(f"  ⚠ {newsroom_name}: parse error")

        time.sleep(REQUEST_DELAY)

    print(f"  → Tier 4 Newsrooms: {count} new articles")
    return count


def collect_tier4_social_rss(conn) -> int:
    """Collect Tier 4 social/blog discovery RSS feeds."""
    count = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=COLLECTION_WINDOW_DAYS)

    for feed_def in TIER4_SOCIAL_RSS:
        feed_url = feed_def["url"]
        feed_name = feed_def["name"]

        parsed = feedparser.parse(feed_url)
        if parsed.bozo:
            print(f"  ⚠ {feed_name}: parse error")
            continue

        for entry in parsed.entries:
            try:
                pub_time_tuple = entry.get("published_parsed", entry.get("updated_parsed"))
                pub_time = (
                    datetime(*pub_time_tuple[:6], tzinfo=timezone.utc)
                    if pub_time_tuple
                    else datetime.now(timezone.utc)
                )
                if pub_time < cutoff:
                    continue

                url = entry.link
                title = entry.title or "Untitled"
                summary = entry.get("summary", "")

                if not is_super_relevant(title + " " + summary):
                    continue

                topic = classify_topic(title, summary)
                fund_tags = tag_funds(title, summary)

                if save_article(conn, url, title, feed_name, 4, topic, summary, pub_time.isoformat(), fund_tags):
                    count += 1
            except Exception:
                pass

        time.sleep(REQUEST_DELAY)

    print(f"  → Tier 4 Social/Blog: {count} new articles")
    return count


TIER_NAMES = {
    1: "Media Articles",
    2: "Regulatory & Government",
    3: "Industry Bodies & Research",
    4: "Social Media & Blogs",
}


def export_to_json(conn):
    """Export recent articles to data.json for the web interface."""
    print("\n→ Exporting to data.json...")

    articles = conn.execute("""
        SELECT a.article_id, a.url, a.title, a.source, a.tier,
               a.topic_category, a.summary, a.published_at, a.collected_at,
               GROUP_CONCAT(aft.fund_id) AS fund_ids
        FROM articles a
        LEFT JOIN article_fund_tags aft ON a.article_id = aft.article_id
        WHERE a.collected_at >= datetime('now', ?)
        GROUP BY a.article_id
        ORDER BY a.published_at DESC NULLS LAST, a.collected_at DESC
    """, (f"-{EXPORT_WINDOW_DAYS} days",)).fetchall()

    funds = conn.execute(
        "SELECT fund_id, display_name, fund_type, aliases FROM funds ORDER BY fund_type, display_name"
    ).fetchall()

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "collection_window": f"Last {EXPORT_WINDOW_DAYS} days",
        "article_count": len(articles),
        "articles": [
            {
                "id": row[0],
                "url": row[1],
                "title": row[2],
                "source": row[3],
                "tier": row[4],
                "tier_name": TIER_NAMES.get(row[4], "Unknown"),
                "topic": row[5],
                "summary": row[6],
                "published_at": row[7],
                "collected_at": row[8],
                "fund_ids": row[9].split(",") if row[9] else [],
            }
            for row in articles
        ],
        "funds": [
            {
                "fund_id": row[0],
                "display_name": row[1],
                "fund_type": row[2],
                "aliases": json.loads(row[3]) if row[3] else [],
            }
            for row in funds
        ],
    }

    with open(DATA_JSON_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"  → Exported {len(articles)} articles to {DATA_JSON_PATH}")
    return output


def main():
    parser = argparse.ArgumentParser(description="Australian Superannuation News Collector v2")
    parser.add_argument("--export", action="store_true",
                        help="Re-export data.json without collecting new articles")
    args = parser.parse_args()

    print("=" * 64)
    print("  Australian Superannuation News Collector v2")
    print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M AEST')}")
    print(f"  Window: last {COLLECTION_WINDOW_DAYS} days | Export: last {EXPORT_WINDOW_DAYS} days")
    print("=" * 64)

    conn = sqlite3.connect(str(DB_PATH))
    init_db(conn)
    seed_funds(conn)

    if args.export:
        export_to_json(conn)
        conn.close()
        print("\nDone (export only). Open index.html to view.")
        return

    totals = {}

    print("\n  TIER 1 — Media Articles")
    print("  " + "-" * 40)
    t1_rss = collect_tier1_rss(conn)
    t1_gnews = collect_tier1_google_news(conn)
    totals[1] = t1_rss + t1_gnews

    print("\n  TIER 2 — Regulatory & Government")
    print("  " + "-" * 40)
    totals[2] = collect_tier2_regulatory(conn)

    print("\n  TIER 3 — Industry Bodies & Research")
    print("  " + "-" * 40)
    totals[3] = collect_tier3_industry(conn)

    print("\n  TIER 4 — Fund Newsrooms & Social")
    print("  " + "-" * 40)
    t4_news = collect_tier4_newsrooms(conn)
    t4_social = collect_tier4_social_rss(conn)
    totals[4] = t4_news + t4_social

    total_new = sum(totals.values())
    print("\n" + "=" * 64)
    print(f"  Collection complete — {total_new} new articles added")
    for tier, count in totals.items():
        print(f"    Tier {tier} ({TIER_NAMES[tier]}): {count}")
    print("=" * 64)

    # Show total articles in database
    total_db = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"\n  Database total: {total_db} articles")

    export_to_json(conn)
    conn.close()
    print("\nDone. Open index.html to view the digest.")


if __name__ == "__main__":
    main()
