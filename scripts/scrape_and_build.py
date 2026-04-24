#!/usr/bin/env python3
"""
Daily cloud refresh of the Q4 FY26 dashboard — v4 (parallelized).

Same data flow as v3 but with concurrent HTTP to fit inside the GH Actions
20-minute timeout:

  * BSE ticker lookups: 12-way ThreadPool (~216 calls → ~20s).
  * Per-slug fetch (screener + Yahoo): 10-way ThreadPool (~300 slugs → ~3 min).
"""
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from threading import Lock

import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept": "text/html, application/json, */*",
           "Accept-Language": "en-US,en;q=0.9"}
BSE_H = {**HEADERS,
         "Referer": "https://www.bseindia.com/",
         "Origin": "https://www.bseindia.com",
         "Accept": "application/json, text/plain, */*"}

YF_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
APR1 = "2026-04-01"
APR1_TS = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp())

IST = timezone(timedelta(hours=5, minutes=30))
TODAY_IST = datetime.now(IST).date()

REPO_ROOT = Path(__file__).resolve().parent.parent
INDEX_HTML = REPO_ROOT / "index.html"

PRINT_LOCK = Lock()


def log(msg):
    with PRINT_LOCK:
        print(msg, flush=True)


# ===================================================================== #
# BSE
# ===================================================================== #
def bse_pagination(session, from_date='20260301', to_date=None, max_pages=20):
    to_date = to_date or TODAY_IST.strftime('%Y%m%d')
    rows = []
    for p in range(1, max_pages + 1):
        url = (
            'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w'
            f'?pageno={p}&strCat=Result&strPrevDate={from_date}'
            f'&strScrip=&strSearch=P&strToDate={to_date}'
            '&strType=C&subcategory=Financial%20Results'
        )
        try:
            r = session.get(url, headers=BSE_H, timeout=20)
        except requests.RequestException as e:
            log(f'[bse] page{p} error: {e}')
            break
        if r.status_code != 200:
            log(f'[bse] page{p} status={r.status_code}')
            break
        try:
            j = r.json()
        except Exception as e:
            log(f'[bse] page{p} json_err={e}')
            break
        tbl = j.get('Table') or []
        rows.extend(tbl)
        if len(tbl) < 50:
            break
        time.sleep(0.2)
    by_cd = {}
    for r in rows:
        cd = str(r.get('SCRIP_CD') or '').strip()
        if not cd:
            continue
        prev = by_cd.get(cd)
        if not prev or (r.get('NEWS_DT') or '') > (prev.get('NEWS_DT') or ''):
            by_cd[cd] = r
    log(f'[bse] pages pulled rows={len(rows)} unique_scrip_cd={len(by_cd)}')
    return by_cd


def bse_ticker_one(scrip_cd):
    """Single-shot ticker lookup used inside a ThreadPool."""
    sess = requests.Session()
    sess.headers.update(BSE_H)
    for endpoint in ('ComHeaderNew/w', 'ComHeader/w'):
        url = (f'https://api.bseindia.com/BseIndiaAPI/api/{endpoint}'
               f'?quotetype=EQ&scripcode={scrip_cd}&seriesid=')
        try:
            r = sess.get(url, timeout=12)
        except requests.RequestException:
            continue
        if r.status_code != 200:
            continue
        try:
            j = r.json()
        except Exception:
            continue
        if not isinstance(j, dict):
            continue
        tkr = j.get('SecurityId') or j.get('scrip_id') or j.get('SC_ID')
        nm = j.get('Scripname') or j.get('Comp_Name') or j.get('COMPANY_NAME')
        if tkr:
            return (scrip_cd, str(tkr).strip(),
                    (str(nm).strip() if nm else None))
    return (scrip_cd, None, None)


def bse_tickers_parallel(scrip_cds, workers=12):
    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(bse_ticker_one, cd) for cd in scrip_cds]
        for i, f in enumerate(as_completed(futs), 1):
            cd, tkr, nm = f.result()
            out[cd] = (tkr, nm)
            if i % 50 == 0:
                log(f'[bse_ticker] {i}/{len(scrip_cds)}')
    return out


# ===================================================================== #
# Screener parser
# ===================================================================== #
QUARTER_RE = re.compile(r'\b([A-Z][a-z]{2} \d{4})\b')


def _clean(s):
    return re.sub(r'<[^>]+>', '', s or '').replace('&nbsp;', ' ').replace('&amp;', '&').strip()


def _to_num(s):
    s = (s or '').strip().replace(',', '').replace('₹', '').replace('%', '')
    if s in ('', '-', '—'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _pct(cur, yoy):
    if cur is None or yoy is None or yoy == 0:
        return None
    return round((cur - yoy) / abs(yoy) * 100)


def parse_screener_quarters(html):
    m_h1 = re.search(r'<h1[^>]*>\s*([^<]+?)\s*</h1>', html)
    name = _clean(m_h1.group(1)) if m_h1 else None

    qm = re.search(r'id="quarters"[\s\S]*?</section>', html)
    if not qm:
        return None
    q = qm.group(0)

    thead = re.search(r'<thead[\s\S]*?</thead>', q)
    if not thead:
        return None
    labels = [_clean(x) for x in re.findall(r'<th[^>]*>([\s\S]*?)</th>', thead.group(0))]
    quarter_labels = [l for l in labels if QUARTER_RE.match(l)]
    if len(quarter_labels) < 2:
        return None

    tbody = re.search(r'<tbody[\s\S]*?</tbody>', q)
    if not tbody:
        return None
    rows = re.findall(r'<tr[\s\S]*?</tr>', tbody.group(0))
    row_map = {}
    for row in rows:
        cells = [_clean(c) for c in re.findall(r'<t[dh][^>]*>([\s\S]*?)</t[dh]>', row)]
        if not cells:
            continue
        label = cells[0].rstrip('+ ').strip()
        row_map[label] = cells[1:]

    def row(*keys):
        for k in keys:
            for label in row_map:
                if label.lower() == k.lower():
                    return row_map[label]
            for label in row_map:
                if label.lower().startswith(k.lower()):
                    return row_map[label]
        return None

    sales_row = row('Sales', 'Revenue')
    op_row = row('Operating Profit', 'Financing Profit')
    np_row = row('Net Profit')
    eps_row = row('EPS in Rs', 'EPS')

    if not any([sales_row, op_row, np_row, eps_row]):
        return None

    def aligned(vals):
        if not vals:
            return {}
        n = min(len(vals), len(quarter_labels))
        return {quarter_labels[-n + i]: vals[-n + i] for i in range(n)}

    sales_vals = aligned(sales_row) if sales_row else {}
    op_vals = aligned(op_row) if op_row else {}
    np_vals = aligned(np_row) if np_row else {}
    eps_vals = aligned(eps_row) if eps_row else {}

    cq = quarter_labels[-1]
    pq = quarter_labels[-2] if len(quarter_labels) >= 2 else None
    cq_month = cq.split(' ')[0]
    cq_year = int(cq.split(' ')[1])
    yoy_label = f'{cq_month} {cq_year - 1}'
    yq = yoy_label if yoy_label in quarter_labels else (
        quarter_labels[-5] if len(quarter_labels) >= 5 else None)

    def block(vals):
        if not vals:
            return None
        cur = _to_num(vals.get(cq))
        prev = _to_num(vals.get(pq)) if pq else None
        yoy = _to_num(vals.get(yq)) if yq else None
        pct = _pct(cur, yoy)
        return {'pct': pct, 'cur': cur, 'prev': prev, 'yoy': yoy}

    def top_num(*patterns):
        for pat in patterns:
            m = re.search(pat, html)
            if m:
                n = _to_num(m.group(1))
                if n is not None:
                    return n
        return None

    price = top_num(r'Current Price[\s\S]{0,250}?<span[^>]*class="number"[^>]*>([^<]+)</span>')
    mcap = top_num(r'Market Cap[\s\S]{0,250}?<span[^>]*class="number"[^>]*>([^<]+)</span>')
    pe = top_num(r'Stock P/E[\s\S]{0,250}?<span[^>]*class="number"[^>]*>([^<]+)</span>')

    return {
        'name': name, 'price': price, 'mcap': mcap, 'pe': pe,
        'cq': cq, 'pq': pq, 'yq': yq,
        'sa': block(sales_vals), 'eb': block(op_vals),
        'np': block(np_vals), 'ep': block(eps_vals),
    }


def fetch_screener(slug):
    sess = requests.Session()
    sess.headers.update(HEADERS)
    for path in (f'/company/{slug}/consolidated/', f'/company/{slug}/'):
        url = f'https://www.screener.in{path}'
        try:
            r = sess.get(url, timeout=25)
        except requests.RequestException:
            continue
        if r.status_code != 200 or len(r.text) < 30000:
            continue
        parsed = parse_screener_quarters(r.text)
        if parsed:
            return parsed
    return None


# ===================================================================== #
# Yahoo
# ===================================================================== #
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


def yahoo_chart(session, symbol):
    url = f"{YF_BASE}/{symbol}"
    try:
        r = session.get(url, params={"range": "2mo", "interval": "1d"}, timeout=20)
    except requests.RequestException:
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
        return {'a': round(apr1_p, 2), 'ad': apr1_d,
                'l': round(latest_p, 2), 'ld': latest_d,
                'p': round((latest_p / apr1_p - 1.0) * 100, 2),
                '_sym': symbol}
    return None


def yahoo_for_slug(slug, cached_sym=None):
    sess = requests.Session()
    sess.headers.update(HEADERS)
    cands = []
    if cached_sym:
        cands.append(cached_sym)
    for suf in ('.BO', '.NS'):
        s = f'{slug}{suf}'
        if s not in cands:
            cands.append(s)
    for sym in cands:
        sm = yahoo_chart(sess, sym)
        if sm:
            return sm
    return None


# ===================================================================== #
# Per-slug worker
# ===================================================================== #
def process_slug(slug, cached_sym, bse_name, is_existing, existing_card):
    parsed = fetch_screener(slug)
    if parsed:
        card = parsed_to_card(slug, parsed, fallback_name=bse_name)
    elif is_existing and existing_card:
        card = existing_card
    else:
        card = bse_only_card(slug, bse_name)

    sm = yahoo_for_slug(slug, cached_sym)
    return (slug, card, sm, parsed is not None)


# ===================================================================== #
# Load existing
# ===================================================================== #
def load_existing(html):
    companies = []
    m = re.search(r'const COMPANIES = (\[.*?\]);', html, re.DOTALL)
    if m:
        try:
            companies = json.loads(m.group(1))
        except Exception as e:
            log(f'[warn] COMPANIES parse failed: {e}')
    stock_moves = {}
    m = re.search(r'const STOCK_MOVES = (\{.*?\n\});', html, re.DOTALL)
    if m:
        try:
            stock_moves = json.loads(m.group(1))
        except Exception as e:
            log(f'[warn] STOCK_MOVES parse failed: {e}')
    return companies, stock_moves


def slug_from_link(link):
    m = re.search(r'/company/([^/]+)/', link or '')
    return m.group(1) if m else None


# ===================================================================== #
# Card formatting
# ===================================================================== #
def fmt_pct(p):
    if p is None:
        return ''
    p = int(round(float(p)))
    if p > 0:
        return f'⇡ {p}%'
    if p < 0:
        return f'⇣ {-p}%'
    return '⇡ 0%'


def fmt_cell(b):
    if not b:
        return {'pct': '', 'latest': '0.00', 'yearAgo': '0.00'}
    return {
        'pct': fmt_pct(b.get('pct')),
        'latest': str(b.get('cur', b.get('yoy', '0.00'))) if b.get('cur') is not None else '0.00',
        'yearAgo': str(b.get('yoy', '0.00')) if b.get('yoy') is not None else '0.00',
    }


def fmt_eps(b):
    if not b:
        return {'pct': '', 'latest': '₹ None', 'yearAgo': '₹ None'}
    cur = b.get('cur')
    yoy = b.get('yoy')
    return {
        'pct': fmt_pct(b.get('pct')),
        'latest': f'₹ {cur}' if cur is not None else '₹ None',
        'yearAgo': f'₹ {yoy}' if yoy is not None else '₹ None',
    }


def fmt_price(p):
    if p is None:
        return '-'
    try:
        p = float(p)
        if p >= 1000:
            return f'{p:,.0f}'
        if p >= 100:
            return f'{p:.0f}'
        if p >= 10:
            return f'{p:.1f}'
        return f'{p:.2f}'
    except Exception:
        return str(p)


def fmt_mcap(m):
    if m is None:
        return '-'
    try:
        return f'{float(m):,.0f}'
    except Exception:
        return str(m)


def build_link(slug):
    base = f'https://www.screener.in/company/{slug}/'
    if not slug.isdigit():
        base = f'https://www.screener.in/company/{slug}/consolidated/'
    return base + '#quarters'


def parsed_to_card(slug, parsed, fallback_name=None):
    name = parsed.get('name') or fallback_name or slug
    sa = fmt_cell(parsed.get('sa'))
    eb = fmt_cell(parsed.get('eb'))
    npb = fmt_cell(parsed.get('np'))
    ep = fmt_eps(parsed.get('ep'))
    pat_pct = parsed.get('np', {}).get('pct') if parsed.get('np') else None
    return {
        'name': name,
        'link': build_link(slug),
        'price': fmt_price(parsed.get('price')),
        'mcap': fmt_mcap(parsed.get('mcap')),
        'quarter': parsed.get('cq', 'Mar 2026'),
        'yearAgoQ': parsed.get('yq', 'Mar 2025'),
        'sales': sa, 'ebidt': eb, 'np': npb, 'eps': ep,
        'patPct': pat_pct if pat_pct is not None else 0,
    }


def bse_only_card(slug, name):
    return {
        'name': name or slug,
        'link': build_link(slug),
        'price': '-', 'mcap': '-',
        'quarter': 'Mar 2026', 'yearAgoQ': 'Mar 2025',
        'sales': {'pct': '', 'latest': '-', 'yearAgo': '-'},
        'ebidt': {'pct': '', 'latest': '-', 'yearAgo': '-'},
        'np': {'pct': '', 'latest': '-', 'yearAgo': '-'},
        'eps': {'pct': '', 'latest': '₹ -', 'yearAgo': '₹ -'},
        'patPct': 0,
    }


# ===================================================================== #
# Rewrite HTML
# ===================================================================== #
def build_stock_moves_block(sm_map):
    lines = []
    for slug, sm in sm_map.items():
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


def rewrite_html(html, companies, stock_moves, latest_close_iso):
    companies_sorted = sorted(
        companies,
        key=lambda c: c['patPct'] if c['patPct'] is not None else -1e9,
        reverse=True,
    )
    companies_js = ','.join(
        json.dumps(c, ensure_ascii=False, separators=(',', ':')) for c in companies_sorted
    )
    html = re.sub(r'const COMPANIES = \[.*?\];',
                  f'const COMPANIES = [{companies_js}];',
                  html, count=1, flags=re.DOTALL)

    sm_block = build_stock_moves_block(stock_moves)
    html = re.sub(r'const STOCK_MOVES = \{.*?\n\};',
                  f'const STOCK_MOVES = {{\n{sm_block}\n}};',
                  html, count=1, flags=re.DOTALL)
    html = re.sub(r'const STOCK_MOVE_ASOF = "[^"]*";',
                  f'const STOCK_MOVE_ASOF = "{latest_close_iso}";',
                  html, count=1)

    refresh_label = _fmt_date(TODAY_IST.isoformat())
    close_label = _fmt_date(latest_close_iso)
    html = re.sub(r'Refreshed \d+ \w+ 2026[^<]*</span>',
                  f'Refreshed {refresh_label} · last trading day {close_label} · Jan–Mar 2026</span>',
                  html, count=1)
    html = re.sub(
        r'Q4 FY26 results fetched \d+ \w+ 2026 \(\d+ companies\) · stock moves refreshed \d+ \w+ 2026 \(last close [^)]*\)',
        f'Q4 FY26 results fetched {refresh_label} ({len(companies)} companies) · '
        f'stock moves refreshed {refresh_label} (last close {close_label})',
        html, count=1)
    return html


# ===================================================================== #
# Main
# ===================================================================== #
def main():
    session = requests.Session()
    session.headers.update(HEADERS)
    log(f'[run] today_ist={TODAY_IST.isoformat()} source=BSE+Screener+Yahoo v4')

    if not INDEX_HTML.exists():
        log(f'[fatal] no index.html at {INDEX_HTML}')
        sys.exit(3)

    html = INDEX_HTML.read_text(encoding='utf-8')
    existing, prev_sm = load_existing(html)
    log(f'[existing] companies={len(existing)} stock_moves={len(prev_sm)}')

    existing_by_slug = {}
    for c in existing:
        s = slug_from_link(c.get('link', ''))
        if s:
            existing_by_slug[s] = c

    # Phase 1: BSE pagination
    t0 = time.time()
    bse_map = bse_pagination(session)
    log(f'[phase1] bse_pagination done in {time.time()-t0:.1f}s discovered={len(bse_map)}')

    # Phase 2: BSE ticker parallel lookup
    t0 = time.time()
    cd_list = list(bse_map.keys())
    ticker_map = bse_tickers_parallel(cd_list, workers=12)
    log(f'[phase2] bse_tickers done in {time.time()-t0:.1f}s')

    slug_to_bse = {}
    for cd, (tkr, nm) in ticker_map.items():
        slug = tkr or cd
        if slug in existing_by_slug:
            slug_to_bse[slug] = {'bse_name': nm, 'scrip_cd': cd}
        elif cd in existing_by_slug:
            slug_to_bse[cd] = {'bse_name': nm, 'scrip_cd': cd}
        else:
            slug_to_bse[slug] = {'bse_name': nm, 'scrip_cd': cd}

    all_slugs = set(existing_by_slug.keys()) | set(slug_to_bse.keys())
    log(f'[union] total_slugs={len(all_slugs)} existing={len(existing_by_slug)} '
        f'new={len(all_slugs) - len(existing_by_slug)}')

    # Phase 3: parallel per-slug screener + Yahoo
    t0 = time.time()
    companies_new = []
    stock_moves_new = {}
    done = 0
    screener_ok = 0
    yahoo_ok = 0
    slugs_list = sorted(all_slugs)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {
            ex.submit(
                process_slug,
                slug,
                (prev_sm.get(slug) or {}).get('_sym'),
                slug_to_bse.get(slug, {}).get('bse_name'),
                slug in existing_by_slug,
                existing_by_slug.get(slug),
            ): slug
            for slug in slugs_list
        }
        for f in as_completed(futs):
            slug, card, sm, scr_ok = f.result()
            companies_new.append(card)
            if sm:
                stock_moves_new[slug] = sm
                yahoo_ok += 1
            if scr_ok:
                screener_ok += 1
            done += 1
            if done % 30 == 0:
                log(f'[phase3] {done}/{len(slugs_list)} scr_ok={screener_ok} yh_ok={yahoo_ok} '
                    f'elapsed={time.time()-t0:.0f}s')

    log(f'[phase3] done in {time.time()-t0:.1f}s scr_ok={screener_ok} yh_ok={yahoo_ok}')

    if not stock_moves_new:
        log('[fatal] no stock moves refreshed')
        sys.exit(5)

    latest_close = max(
        (sm['ld'] for sm in stock_moves_new.values() if sm.get('ld')),
        default=TODAY_IST.isoformat(),
    )
    log(f'[summary] companies={len(companies_new)} stock_moves={len(stock_moves_new)} '
        f'last_close={latest_close}')

    new_html = rewrite_html(html, companies_new, stock_moves_new, latest_close)
    INDEX_HTML.write_text(new_html, encoding='utf-8')
    log(f'[done] wrote {INDEX_HTML} size={len(new_html)}')


if __name__ == '__main__':
    main()
