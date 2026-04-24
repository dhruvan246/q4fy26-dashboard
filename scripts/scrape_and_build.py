#!/usr/bin/env python3
"""
Daily cloud refresh of the Q4 FY26 dashboard — Yahoo Finance edition.

Why Yahoo Finance?
------------------
screener.in blocks the IP ranges used by GitHub-hosted runners, so every
request from Actions to www.screener.in times out with "Network is
unreachable". Yahoo Finance's v8 chart API is publicly reachable from
everywhere (GitHub, Cloudflare, etc.) and returns the exact daily-close
history we need.

Flow
----
1. Parse the existing index.html for COMPANIES and the current STOCK_MOVES
   cache (for each slug we may have previously stored `_sym`, the suffix
   that works on Yahoo: `.BO` for BSE or `.NS` for NSE).
2. For each slug, try `{slug}.BO` first, then `{slug}.NS`. Cache the
   working suffix in STOCK_MOVES[slug]._sym so subsequent refreshes skip
   the failed probe.
3. From the chart response pick the Apr 1 close (first trading day >=
   2026-04-01) and the most recent close.
4. Rewrite STOCK_MOVES, STOCK_MOVE_ASOF, and the refresh banner in
   index.html. GitHub Actions commits + pushes.

This keeps the dashboard's COMPANIES list untouched; new filings need a
manual re-run of the local scraper that can authenticate to screener.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

YF_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

REPO_ROOT = Path(__file__).resolve().parent.parent
INDEX_HTML = REPO_ROOT / "index.html"

IST = timezone(timedelta(hours=5, minutes=30))
TODAY_IST = datetime.now(IST).date()
APR1 = "2026-04-01"
APR1_TS = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp())


# --------------------------------------------------------------------- #
#  Parse existing index.html
# --------------------------------------------------------------------- #
def load_existing(html):
    companies = []
    m = re.search(r'const COMPANIES = (\[.*?\]);', html, re.DOTALL)
    if m:
        try:
            companies = json.loads(m.group(1))
        except Exception as e:
            print(f'[warn] failed to parse COMPANIES: {e}', flush=True)
    stock_moves = {}
    m = re.search(r'const STOCK_MOVES = (\{.*?\n\});', html, re.DOTALL)
    if m:
        try:
            stock_moves = json.loads(m.group(1))
        except Exception as e:
            print(f'[warn] failed to parse STOCK_MOVES: {e}', flush=True)
    return companies, stock_moves


def slug_from_link(link):
    m = re.search(r'/company/([^/]+)/', link or '')
    return m.group(1) if m else None


# --------------------------------------------------------------------- #
#  Yahoo Finance
# --------------------------------------------------------------------- #
def _pick_apr1_and_latest(timestamps, closes):
    pairs = []
    for ts, c in zip(timestamps or [], closes or []):
        if ts is None or c is None:
            continue
        try:
            pairs.append((int(ts), float(c)))
        except (ValueError, TypeError):
            continue
    if not pairs:
        return None, None, None, None
    pairs.sort(key=lambda t: t[0])
    apr1_p, apr1_ts = None, None
    for ts, c in pairs:
        if ts >= APR1_TS:
            apr1_p, apr1_ts = c, ts
            break
    if apr1_p is None:
        apr1_ts, apr1_p = pairs[0]
    latest_ts, latest_p = pairs[-1]
    apr1_d = datetime.fromtimestamp(apr1_ts, tz=timezone.utc).date().isoformat()
    latest_d = datetime.fromtimestamp(latest_ts, tz=timezone.utc).date().isoformat()
    return apr1_p, apr1_d, latest_p, latest_d


def fetch_chart(session, symbol):
    url = f"{YF_BASE}/{symbol}"
    params = {"range": "2mo", "interval": "1d"}
    try:
        r = session.get(url, params=params, timeout=20)
    except requests.RequestException as e:
        print(f'[err] yf {symbol}: {e}', flush=True)
        return None
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    res = (data.get('chart') or {}).get('result') or []
    if not res:
        return None
    res = res[0]
    ts = res.get('timestamp') or []
    quote = ((res.get('indicators') or {}).get('quote') or [{}])[0]
    closes = quote.get('close') or []
    apr1_p, apr1_d, latest_p, latest_d = _pick_apr1_and_latest(ts, closes)
    if apr1_p and latest_p:
        return {
            'a': round(apr1_p, 2),
            'ad': apr1_d,
            'l': round(latest_p, 2),
            'ld': latest_d,
            'p': round((latest_p / apr1_p - 1.0) * 100, 2),
            '_sym': symbol,
        }
    return None


def fetch_stock_move(session, slug, cached_sym=None):
    tried = []
    candidates = []
    if cached_sym:
        candidates.append(cached_sym)
    for suffix in ('.BO', '.NS'):
        sym = f"{slug}{suffix}"
        if sym not in candidates:
            candidates.append(sym)
    for sym in candidates:
        tried.append(sym)
        sm = fetch_chart(session, sym)
        if sm:
            return sm
        time.sleep(0.15)
    print(f'[skip] {slug}: tried {tried}', flush=True)
    return None


# --------------------------------------------------------------------- #
#  HTML rewrite
# --------------------------------------------------------------------- #
def build_stock_moves_block(new_sm):
    lines = []
    for slug, sm in new_sm.items():
        if slug.startswith('__') or not sm:
            continue
        entry = {
            'apr1': round(float(sm['a']), 2),
            'latest': round(float(sm['l']), 2),
            'latestDate': sm.get('ld', ''),
            'pct': round(float(sm['p']), 2) if sm.get('p') is not None else None,
        }
        if sm.get('ad') and sm['ad'] != APR1:
            entry['apr1Date'] = sm['ad']
        if sm.get('_sym'):
            entry['_sym'] = sm['_sym']
        lines.append(f'  "{slug}":{json.dumps(entry, separators=(",", ":"))}')
    return ',\n'.join(lines)


def _fmt_date(iso):
    plat = sys.platform
    fmt = '%#d %b %Y' if plat == 'win32' else '%-d %b %Y'
    return datetime.fromisoformat(iso).strftime(fmt)


def rewrite_html(html, new_sm, companies_count, latest_close_iso):
    sm_block = build_stock_moves_block(new_sm)
    html = re.sub(
        r'const STOCK_MOVES = \{.*?\n\};',
        f'const STOCK_MOVES = {{\n{sm_block}\n}};',
        html, count=1, flags=re.DOTALL,
    )
    html = re.sub(
        r'const STOCK_MOVE_ASOF = "[^"]*";',
        f'const STOCK_MOVE_ASOF = "{latest_close_iso}";',
        html, count=1,
    )

    refresh_label = _fmt_date(TODAY_IST.isoformat())
    close_label = _fmt_date(latest_close_iso)
    html = re.sub(
        r'Refreshed \d+ \w+ 2026[^<]*</span>',
        f'Refreshed {refresh_label} · last trading day {close_label} · Jan–Mar 2026</span>',
        html, count=1,
    )
    html = re.sub(
        r'Q4 FY26 results fetched \d+ \w+ 2026 \(\d+ companies\) · stock moves refreshed \d+ \w+ 2026 \(last close [^)]*\)',
        f'Q4 FY26 results fetched {refresh_label} ({companies_count} companies) · '
        f'stock moves refreshed {refresh_label} (last close {close_label})',
        html, count=1,
    )
    return html


# --------------------------------------------------------------------- #
#  Main
# --------------------------------------------------------------------- #
def main():
    session = requests.Session()
    session.headers.update(HEADERS)
    print(f'[run] today_ist={TODAY_IST.isoformat()} source=yahoo-finance', flush=True)

    if not INDEX_HTML.exists():
        print(f'[fatal] template missing: {INDEX_HTML}', flush=True)
        sys.exit(3)

    html = INDEX_HTML.read_text(encoding='utf-8')
    companies, prev_sm = load_existing(html)
    print(f'[existing] companies={len(companies)} prev_stock_moves={len(prev_sm)}',
          flush=True)

    if not companies:
        print('[fatal] no companies found in index.html', flush=True)
        sys.exit(4)

    new_sm = {}
    latest_close = None
    ok = 0
    for i, c in enumerate(companies, 1):
        slug = slug_from_link(c.get('link', ''))
        if not slug:
            continue
        prev = prev_sm.get(slug) or {}
        cached_sym = prev.get('_sym')
        sm = fetch_stock_move(session, slug, cached_sym)
        if sm:
            new_sm[slug] = sm
            ok += 1
            if latest_close is None or sm['ld'] > latest_close:
                latest_close = sm['ld']
        if i % 10 == 0:
            print(f'[progress] {i}/{len(companies)} ok={ok}', flush=True)
        time.sleep(0.1)

    if not new_sm:
        print('[fatal] no stock moves refreshed', flush=True)
        sys.exit(5)

    latest_close = latest_close or TODAY_IST.isoformat()
    print(f'[summary] refreshed {len(new_sm)} stock moves · last close {latest_close}',
          flush=True)

    new_html = rewrite_html(html, new_sm, len(companies), latest_close)
    INDEX_HTML.write_text(new_html, encoding='utf-8')
    print(f'[done] wrote {INDEX_HTML} size={len(new_html)} '
          f'companies={len(companies)} stock_moves={len(new_sm)}',
          flush=True)


if __name__ == '__main__':
    main()
