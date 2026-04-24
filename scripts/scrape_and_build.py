#!/usr/bin/env python3
"""
Daily cloud refresh of the Q4 FY26 dashboard.

Flow:
  1. Paginate https://www.screener.in/results/latest/?all=&page=N
     (25 cards per page) and parse each company card.
  2. For each company hit /api/company/{internal_id}/chart/?q=Price&days=30
     to capture the Apr-1 price and most recent close.
  3. Rebuild index.html by replacing specific JS constants
     (COMPANIES, COMPANY_SECTOR, STOCK_MOVES, ASOF, refresh banner, footer).

Commits produced by GitHub Actions push the new index.html back to the repo
so GitHub Pages automatically redeploys the dashboard.
"""

import json
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ----------------------------------------------------------------------------- #
#  Config
# ----------------------------------------------------------------------------- #
BASE = "https://www.screener.in"
LISTING_URL = f"{BASE}/results/latest/?all=&page={{page}}"
CHART_URL = (
    f"{BASE}/api/company/{{iid}}/chart/?q=Price&days=30{{suffix}}"
)
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

# ----------------------------------------------------------------------------- #
#  Sector mapping (same dictionary as rebuild_145.py)
# ----------------------------------------------------------------------------- #
SECTOR_MAP = {
    # Banking & Finance
    'HDFC Bank': 'Banking & Finance', 'ICICI Bank': 'Banking & Finance',
    'Yes Bank': 'Banking & Finance', 'Jio Financial': 'Banking & Finance',
    'Nikki Glob.Fin.': 'Banking & Finance', 'Ganesh Holdings': 'Banking & Finance',
    'Jindal Leasefin': 'Banking & Finance', 'SG Finserve': 'Banking & Finance',
    'HDB FINANC SER': 'Banking & Finance', 'CRISIL': 'Banking & Finance',
    'Ind Bank Housing': 'Banking & Finance', 'Roselabs Finance': 'Banking & Finance',
    'Kapil Raj Financ': 'Banking & Finance', 'Bridge Securitie': 'Banking & Finance',
    # Asset Mgmt & Broking
    'Anand Rathi Shar': 'Asset Mgmt & Broking', 'Angel One': 'Asset Mgmt & Broking',
    'Anand Rathi Wea.': 'Asset Mgmt & Broking', 'ICICI AMC': 'Asset Mgmt & Broking',
    'HDFC AMC': 'Asset Mgmt & Broking', 'Adit.Birla Money': 'Asset Mgmt & Broking',
    # Insurance
    'ICICI Pru Life': 'Insurance', 'ICICI Lombard': 'Insurance',
    'HDFC Life Insur.': 'Insurance',
    # IT & Software
    'TCS': 'IT & Software', 'Wipro': 'IT & Software', 'Mastek': 'IT & Software',
    'Virgo Global': 'IT & Software', 'Wherrelz IT': 'IT & Software',
    'Infollion Resea.': 'IT & Software', 'B2B Soft.Tech.': 'IT & Software',
    'Just Dial': 'IT & Software',
    # Telecom
    'Tejas Networks': 'Telecom', 'GTPL Hathway': 'Telecom',
    'Hathway Cable': 'Telecom',
    # Media & Entertainment
    'Media Matrix': 'Media & Entertainment', 'Den Networks': 'Media & Entertainment',
    'Vashu Bhagnani': 'Media & Entertainment', 'Netwrk.18 Media': 'Media & Entertainment',
    'Infomedia Press': 'Media & Entertainment',
    # Food & Agri
    'Kesar India': 'Food & Agri', 'M B Agro Prod.': 'Food & Agri',
    'Agri-Tech India': 'Food & Agri', 'Mangalam Global': 'Food & Agri',
    # FMCG & Consumer
    'VST Industries': 'FMCG & Consumer', 'Bajaj Consumer': 'FMCG & Consumer',
    'G M Breweries': 'FMCG & Consumer', 'Lotus Chocolate': 'FMCG & Consumer',
    # Retail & Durables
    'Lloyds Luxuries': 'Retail & Durables',
    # Capital Goods & Engineering
    'Bombay Wire': 'Capital Goods & Engineering',
    'Elecon Engg.Co': 'Capital Goods & Engineering',
    'Eimco Elecon(I)': 'Capital Goods & Engineering',
    'Hathway Bhawani': 'Capital Goods & Engineering',
    'Cont. Controls': 'Capital Goods & Engineering',
    'Nilachal Refract': 'Capital Goods & Engineering',
    'GSM Foils': 'Capital Goods & Engineering',
    'National Standar': 'Capital Goods & Engineering',
    'Sanathnagar Ent.': 'Capital Goods & Engineering',
    # Auto & Engines
    'Swaraj Engines': 'Auto & Engines',
    # Chemicals & Fertilizers
    'Krishana Phosch.': 'Chemicals & Fertilizers',
    # Textiles
    'Alka India': 'Textiles', 'Alok Industries': 'Textiles',
    # Power & Renewables
    'Waaree Renewab.': 'Power & Renewables',
    # Cement
    'Nuvoco Vistas': 'Cement',
    # Infrastructure & Real Estate
    'Rel. Indl. Infra': 'Infrastructure & Real Estate',
    'PropsharePlatina': 'Infrastructure & Real Estate',
    'PropshareTitania': 'Infrastructure & Real Estate',
    'Yuranus Infrast.': 'Infrastructure & Real Estate',
    # Hospitality
    'Eco Hotels': 'Hospitality',
}


def sector_for(name: str) -> str:
    return SECTOR_MAP.get(name, 'Other')


# ----------------------------------------------------------------------------- #
#  Screener listing scrape
# ----------------------------------------------------------------------------- #
def _to_float(text):
    if text is None:
        return None
    t = text.replace('₹', '').replace(',', '').replace('\u00a0', ' ').strip()
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _parse_block(row_cells):
    """row_cells is ['Sales', '⇡ 202%', '70.3', '16.1', '23.3']."""
    if not row_cells or len(row_cells) < 5:
        return None
    pct_text = row_cells[1]
    pct_num = None
    m = re.search(r'(-?\d+)%', pct_text)
    if m:
        pct_num = int(m.group(1))
        if '⇣' in pct_text and pct_num > 0:
            pct_num = -pct_num
    return {
        'pct': pct_num,
        'cur': _to_float(row_cells[2]),
        'prev': _to_float(row_cells[3]),
        'yoy': _to_float(row_cells[4]),
    }


def _parse_card(card):
    link = card.select_one('a[href*="/company/"][href*="#quarters"]')
    if link is None:
        return None
    name_span = link.select_one('span.ink-900') or link
    name = name_span.get_text(strip=True)
    m = re.search(r'/company/([^/]+)/', link.get('href', ''))
    slug = m.group(1) if m else None

    pdf = card.select_one('a[href*="/company/source/quarter/"]')
    iid = None
    if pdf:
        mm = re.search(r'/company/source/quarter/(\d+)/', pdf.get('href', ''))
        if mm:
            iid = mm.group(1)

    table = card.select_one('table.data-table')
    cur_q, prev_q, yoy_q = 'Mar 2026', 'Dec 2025', 'Mar 2025'
    rows = {}
    if table:
        trs = table.select('tr')
        if trs:
            header_cells = [c.get_text(strip=True) for c in trs[0].select('th,td')]
            if len(header_cells) >= 5:
                cur_q = header_cells[2] or cur_q
                prev_q = header_cells[3] or prev_q
                yoy_q = header_cells[4] or yoy_q
        for tr in trs[1:]:
            cells = [c.get_text(strip=True) for c in tr.select('th,td')]
            if cells and len(cells) >= 5:
                rows[cells[0]] = cells

    sa = _parse_block(rows.get('Sales'))
    eb = _parse_block(rows.get('EBIDT'))
    np_ = _parse_block(rows.get('Net profit'))
    ep = _parse_block(rows.get('EPS'))

    txt = card.get_text(' ', strip=True)
    price = mcap = pe = None
    pm = re.search(r'Price\s*₹\s*([\d,\.]+)', txt)
    if pm:
        price = _to_float(pm.group(1))
    mm_ = re.search(r'M\.Cap\s*₹\s*([\d,\.]+)\s*Cr', txt)
    if mm_:
        mcap = _to_float(mm_.group(1))
    pem = re.search(r'PE\s*([\d\.]+)', txt)
    if pem:
        pe = _to_float(pem.group(1))

    return {
        'n': name, 's': slug, 'iid': iid,
        'p': price, 'm': mcap, 'pe': pe,
        'cq': cur_q, 'pq': prev_q, 'yq': yoy_q,
        'sa': sa, 'eb': eb, 'np': np_, 'ep': ep,
    }


def _find_cards(soup):
    """Walk up from each quarter link to the first ancestor that contains a data-table."""
    links = soup.select('a[href*="/company/"][href*="#quarters"]')
    cards = []
    seen = set()
    for link in links:
        parent = link
        for _ in range(10):
            parent = parent.parent
            if parent is None:
                break
            if parent.select_one('table.data-table'):
                if id(parent) not in seen:
                    seen.add(id(parent))
                    cards.append(parent)
                break
    return cards


def scrape_listings(session: requests.Session):
    all_rows = []
    seen_slugs = set()
    for page in range(1, 12):
        url = LISTING_URL.format(page=page)
        r = session.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'lxml')
        cards = _find_cards(soup)
        if not cards:
            break
        new_in_page = 0
        for card in cards:
            data = _parse_card(card)
            if data and data['s'] and data['s'] not in seen_slugs:
                seen_slugs.add(data['s'])
                all_rows.append(data)
                new_in_page += 1
        print(f'[listing] page={page} cards={len(cards)} new={new_in_page} total={len(all_rows)}',
              flush=True)
        if len(cards) < 20:
            break
        time.sleep(0.3)
    return all_rows


# ----------------------------------------------------------------------------- #
#  Chart / stock-move fetch
# ----------------------------------------------------------------------------- #
def _pick_apr1_and_latest(values):
    """values is list of [date, price] strings. Return (apr1_price, apr1_date, latest_price, latest_date)."""
    if not values:
        return None, None, None, None
    parsed_vals = []
    for entry in values:
        if not entry or len(entry) < 2:
            continue
        d, v = entry[0], entry[1]
        try:
            fv = float(v)
        except (ValueError, TypeError):
            continue
        parsed_vals.append((d, fv))
    if not parsed_vals:
        return None, None, None, None
    parsed_vals.sort(key=lambda t: t[0])
    # Apr 1 or earliest after
    apr1_price = None
    apr1_date = APR1
    for d, v in parsed_vals:
        if d >= APR1:
            apr1_price = v
            apr1_date = d
            break
    if apr1_price is None:  # fallback to earliest
        apr1_date, apr1_price = parsed_vals[0]
    latest_date, latest_price = parsed_vals[-1]
    return apr1_price, apr1_date, latest_price, latest_date


def fetch_stock_move(session, iid):
    if not iid:
        return None
    for suffix in ('&consolidated=true', ''):
        url = CHART_URL.format(iid=iid, suffix=suffix)
        try:
            r = session.get(url, timeout=25)
            if r.status_code != 200:
                continue
            data = r.json()
            values = data.get('datasets', [{}])[0].get('values', [])
            apr1_p, apr1_d, latest_p, latest_d = _pick_apr1_and_latest(values)
            if apr1_p and latest_p:
                pct = (latest_p / apr1_p - 1.0) * 100
                return {
                    'a': round(apr1_p, 2),
                    'ad': apr1_d,
                    'l': round(latest_p, 2),
                    'ld': latest_d,
                    'p': round(pct, 2),
                }
        except Exception as e:
            print(f'[chart] iid={iid} err={e}', flush=True)
    return None


# ----------------------------------------------------------------------------- #
#  Dashboard rebuild (same formatting as rebuild_145.py)
# ----------------------------------------------------------------------------- #
def fmt_pct(p):
    if p is None:
        return ''
    p = int(round(float(p)))
    if p > 0: return f'⇡ {p}%'
    if p < 0: return f'⇣ {-p}%'
    return '⇡ 0%'


def fmt_cell(block):
    if not block:
        return {'pct': '', 'latest': '0.00', 'yearAgo': '0.00'}
    cur = block.get('cur')
    yoy = block.get('yoy')
    return {
        'pct': fmt_pct(block.get('pct')),
        'latest': str(cur) if cur is not None else '0.00',
        'yearAgo': str(yoy) if yoy is not None else '0.00',
    }


def fmt_eps(block):
    if not block:
        return {'pct': '', 'latest': '₹ None', 'yearAgo': '₹ None'}
    cur = block.get('cur')
    yoy = block.get('yoy')
    return {
        'pct': fmt_pct(block.get('pct')),
        'latest': f'₹ {cur}' if cur is not None else '₹ None',
        'yearAgo': f'₹ {yoy}' if yoy is not None else '₹ None',
    }


def fmt_price(p):
    if p is None: return '-'
    try:
        p = float(p)
        if p >= 1000: return f'{p:,.0f}'
        if p >= 100: return f'{p:.0f}'
        if p >= 10: return f'{p:.1f}'
        return f'{p:.2f}'
    except (ValueError, TypeError):
        return str(p)


def fmt_mcap(m):
    if m is None: return '-'
    try:
        return f'{float(m):,.0f}'
    except (ValueError, TypeError):
        return str(m)


def build_link(slug):
    if slug and not slug.isdigit():
        return f'https://www.screener.in/company/{slug}/consolidated/#quarters'
    return f'https://www.screener.in/company/{slug}/#quarters'


def rebuild_html(parsed_rows, stock_moves, html, today_ist, latest_close_date):
    """Apply regex replacements to index.html in-place."""
    companies = []
    for p in parsed_rows:
        pat_pct = p.get('np', {}).get('pct') if p.get('np') else None
        companies.append({
            'name': p['n'],
            'link': build_link(p['s']),
            'price': fmt_price(p.get('p')),
            'mcap': fmt_mcap(p.get('m')),
            'quarter': p.get('cq', 'Mar 2026'),
            'yearAgoQ': p.get('yq', 'Mar 2025'),
            'sales': fmt_cell(p.get('sa')),
            'ebidt': fmt_cell(p.get('eb')),
            'np': fmt_cell(p.get('np')),
            'eps': fmt_eps(p.get('ep')),
            'patPct': pat_pct if pat_pct is not None else 0,
        })
    companies.sort(
        key=lambda c: c['patPct'] if c['patPct'] is not None else -999999,
        reverse=True,
    )

    sm_entries = []
    for slug, sm in stock_moves.items():
        if slug.startswith('__') or not sm:
            continue
        if sm.get('a') is None or sm.get('l') is None:
            continue
        entry = {
            'apr1': round(float(sm['a']), 2),
            'latest': round(float(sm['l']), 2),
            'latestDate': sm.get('ld', ''),
            'pct': round(float(sm['p']), 2) if sm.get('p') is not None else None,
        }
        ad = sm.get('ad', '')
        if ad and ad != APR1:
            entry['apr1Date'] = ad
        sm_entries.append(f'  "{slug}":{json.dumps(entry, separators=(",", ":"))}')
    sm_block = ',\n'.join(sm_entries)

    sector_lines = []
    for c in companies:
        sec = sector_for(c['name'])
        name_esc = c['name'].replace('"', '\\"')
        sector_lines.append(f'  "{name_esc}": "{sec}"')
    sector_block = ',\n'.join(sector_lines)

    companies_js = ','.join(
        json.dumps(c, ensure_ascii=False, separators=(',', ':')) for c in companies
    )
    html = re.sub(
        r'const COMPANIES = \[.*?\];',
        f'const COMPANIES = [{companies_js}];',
        html, count=1, flags=re.DOTALL,
    )
    html = re.sub(
        r'const COMPANY_SECTOR = \{.*?\n\};',
        f'const COMPANY_SECTOR = {{\n{sector_block}\n}};',
        html, count=1, flags=re.DOTALL,
    )
    html = re.sub(
        r'const STOCK_MOVE_BASIS = "[^"]*";',
        f'const STOCK_MOVE_BASIS = "{APR1}";',
        html, count=1,
    )
    asof_iso = latest_close_date or today_ist.isoformat()
    html = re.sub(
        r'const STOCK_MOVE_ASOF = "[^"]*";',
        f'const STOCK_MOVE_ASOF = "{asof_iso}";',
        html, count=1,
    )
    html = re.sub(
        r'const STOCK_MOVES = \{.*?\n\};',
        f'const STOCK_MOVES = {{\n{sm_block}\n}};',
        html, count=1, flags=re.DOTALL,
    )

    refresh_label = today_ist.strftime('%-d %b %Y') if sys.platform != 'win32' \
        else today_ist.strftime('%#d %b %Y')
    close_label = (
        datetime.fromisoformat(asof_iso).strftime('%-d %b %Y')
        if sys.platform != 'win32'
        else datetime.fromisoformat(asof_iso).strftime('%#d %b %Y')
    )
    html = re.sub(
        r'Refreshed \d+ \w+ 2026[^<]*</span>',
        f'Refreshed {refresh_label} · last trading day {close_label} · Jan–Mar 2026</span>',
        html, count=1,
    )
    html = re.sub(
        r'Q4 FY26 results fetched \d+ \w+ 2026 \(\d+ companies\) · stock moves refreshed \d+ \w+ 2026 \(last close [^)]*\)',
        f'Q4 FY26 results fetched {refresh_label} ({len(companies)} companies) · '
        f'stock moves refreshed {refresh_label} (last close {close_label})',
        html, count=1,
    )
    return html, len(companies), len(sm_entries)


# ----------------------------------------------------------------------------- #
#  Main
# ----------------------------------------------------------------------------- #
def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    print(f'[run] today_ist={TODAY_IST.isoformat()}', flush=True)

    parsed = scrape_listings(session)
    if not parsed:
        print('[fatal] no companies scraped', flush=True)
        sys.exit(2)

    stock_moves = {}
    latest_close_date = None
    for i, p in enumerate(parsed, 1):
        iid = p.get('iid')
        sm = fetch_stock_move(session, iid)
        if sm:
            sm['n'] = p['n']
            stock_moves[p['s']] = sm
            if latest_close_date is None or sm['ld'] > latest_close_date:
                latest_close_date = sm['ld']
        if i % 10 == 0:
            print(f'[chart] progress {i}/{len(parsed)}', flush=True)
        time.sleep(0.1)

    print(f'[run] parsed={len(parsed)} stock_moves={len(stock_moves)} '
          f'latest_close={latest_close_date}', flush=True)

    if not INDEX_HTML.exists():
        print(f'[fatal] template missing: {INDEX_HTML}', flush=True)
        sys.exit(3)
    html = INDEX_HTML.read_text(encoding='utf-8')
    new_html, n_comp, n_sm = rebuild_html(parsed, stock_moves, html, TODAY_IST,
                                          latest_close_date)
    INDEX_HTML.write_text(new_html, encoding='utf-8')
    print(f'[done] wrote {INDEX_HTML} size={len(new_html)} '
          f'companies={n_comp} stock_moves={n_sm}', flush=True)


if __name__ == '__main__':
    main()
