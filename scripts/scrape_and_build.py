#!/usr/bin/env python3
"""
Daily cloud refresh of the Q4 FY26 dashboard.

Architecture
------------
The public screener.in /results/latest/ listing requires a logged-in
session — which GitHub Actions does not have. So this script does an
auth-optional refresh:

  Fast path (no auth, runs every day):
    * Parse the existing index.html for the current COMPANIES list.
    * For each company slug, look up the screener internal id
      (data-company-id) from the public /company/{slug}/ page. The id is
      cached in STOCK_MOVES[slug]._iid after the first run so subsequent
      refreshes skip that step.
    * Hit /api/company/{iid}/chart/?q=Price&days=30 (public) to get the
      Apr 1 and most recent close prices.
    * Rewrite STOCK_MOVES, STOCK_MOVE_ASOF, and the refresh banner in
      index.html and commit.

  Full path (requires SCREENER_SESSIONID secret, not used by default):
    * Not needed for steady-state refreshes — we already have every
      Q4 FY26 filer. When new filings drop, re-run the full local
      scraper and push a refreshed index.html manually.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

BASE = "https://www.screener.in"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}

REPO_ROOT = Path(__file__).resolve().parent.parent
INDEX_HTML = REPO_ROOT / "index.html"

IST = timezone(timedelta(hours=5, minutes=30))
TODAY_IST = datetime.now(IST).date()
APR1 = "2026-04-01"


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
        raw = m.group(1)
        try:
            stock_moves = json.loads(raw)
        except Exception as e:
            print(f'[warn] failed to parse STOCK_MOVES: {e}', flush=True)
    return companies, stock_moves


def slug_from_link(link):
    m = re.search(r'/company/([^/]+)/', link or '')
    return m.group(1) if m else None


# --------------------------------------------------------------------- #
#  Chart / iid fetch (no auth needed)
# --------------------------------------------------------------------- #
def resolve_iid(session, slug):
    try:
        r = session.get(f"{BASE}/company/{slug}/", timeout=25)
    except requests.RequestException as e:
        print(f'[err] resolve_iid({slug}): {e}', flush=True)
        return None
    if r.status_code != 200:
        return None
    m = re.search(r'data-company-id=["\'](\d+)', r.text)
    return m.group(1) if m else None


def _pick_apr1_and_latest(values):
    parsed_vals = []
    for entry in values or []:
        if not entry or len(entry) < 2:
            continue
        try:
            parsed_vals.append((entry[0], float(entry[1])))
        except (ValueError, TypeError):
            continue
    if not parsed_vals:
        return None, None, None, None
    parsed_vals.sort(key=lambda t: t[0])
    apr1_p, apr1_d = None, APR1
    for d, v in parsed_vals:
        if d >= APR1:
            apr1_p, apr1_d = v, d
            break
    if apr1_p is None:
        apr1_d, apr1_p = parsed_vals[0]
    latest_d, latest_p = parsed_vals[-1]
    return apr1_p, apr1_d, latest_p, latest_d


def fetch_stock_move(session, iid):
    if not iid:
        return None
    for suffix in ('&consolidated=true', ''):
        url = f"{BASE}/api/company/{iid}/chart/?q=Price&days=30{suffix}"
        try:
            r = session.get(url, timeout=25)
        except requests.RequestException as e:
            print(f'[err] chart iid={iid}: {e}', flush=True)
            continue
        if r.status_code != 200:
            continue
        try:
            data = r.json()
        except Exception:
            continue
        values = data.get('datasets', [{}])[0].get('values', [])
        apr1_p, apr1_d, latest_p, latest_d = _pick_apr1_and_latest(values)
        if apr1_p and latest_p:
            return {
                'a': round(apr1_p, 2),
                'ad': apr1_d,
                'l': round(latest_p, 2),
                'ld': latest_d,
                'p': round((latest_p / apr1_p - 1.0) * 100, 2),
                'i': iid,
            }
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
        if sm.get('i'):
            entry['_iid'] = sm['i']
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
    sid = os.environ.get('SCREENER_SESSIONID')
    if sid:
        session.cookies.set('sessionid', sid, domain='.screener.in')
    print(f'[run] today_ist={TODAY_IST.isoformat()} sessionid={"set" if sid else "missing"}',
          flush=True)

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
        iid = prev.get('_iid') or prev.get('i')
        if not iid:
            iid = resolve_iid(session, slug)
            time.sleep(0.15)
        if not iid:
            print(f'[skip] {slug}: unable to resolve iid', flush=True)
            continue
        sm = fetch_stock_move(session, iid)
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
