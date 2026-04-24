#!/usr/bin/env python3
"""
Probe v2: confirm BSE pagination, SCRIP_CD->ticker lookup, and screener page reachability.
Drives the architecture choice for the daily cloud refresh.
"""
import json
import sys
import time
import requests

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
BSE_H = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}


def get(url, headers=None, timeout=20):
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    t = time.time()
    try:
        r = requests.get(url, headers=h, timeout=timeout)
        dt = time.time() - t
        return r, dt, None
    except Exception as e:
        return None, time.time() - t, e


def section(title):
    print(f'\n=== {title} ===', flush=True)


def main():
    # 1) BSE AnnSubCategory pagination: walk pages until empty, count unique SCRIP_CDs
    section('BSE pagination (all Q4 result filings since 2026-03-01)')
    all_rows = []
    for p in range(1, 20):
        u = (
            'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w'
            f'?pageno={p}&strCat=Result&strPrevDate=20260301'
            f'&strScrip=&strSearch=P&strToDate=20260424'
            '&strType=C&subcategory=Financial%20Results'
        )
        r, dt, err = get(u, BSE_H)
        if err:
            print(f'  page{p} ERROR {err!r}', flush=True)
            break
        try:
            j = r.json()
        except Exception as e:
            print(f'  page{p} json_err={e}', flush=True)
            break
        tbl = j.get('Table') or []
        print(f'  page{p} rows={len(tbl)} dt={dt:.2f}s', flush=True)
        all_rows.extend(tbl)
        if len(tbl) < 50:  # last page
            break
        time.sleep(0.3)
    codes = {str(r.get('SCRIP_CD')): r.get('HEADLINE', '')[:60] for r in all_rows}
    print(f'  TOTAL_rows={len(all_rows)} unique_scrip_cd={len(codes)}', flush=True)
    sample = list(codes.items())[:5]
    for cd, hl in sample:
        print(f'  {cd}: {hl}', flush=True)
    pick = list(codes.keys())[:3]

    # 2) BSE ComHeader/ComHead: SCRIP_CD -> SCRIP_ID (ticker)
    section('BSE SCRIP_CD -> SCRIP_ID')
    for cd in pick:
        for endpoint in ('ComHeaderNew/w', 'ComHeadernew/w', 'ComHeader/w', 'ComHead/w'):
            u = f'https://api.bseindia.com/BseIndiaAPI/api/{endpoint}?quotetype=EQ&scripcode={cd}&seriesid='
            r, dt, err = get(u, BSE_H, timeout=15)
            if err:
                print(f'  {cd} {endpoint} ERROR {err!r}', flush=True)
                continue
            try:
                j = r.json()
                keys = list(j.keys())[:10] if isinstance(j, dict) else '(list)'
                # common shapes
                sid = None
                if isinstance(j, dict):
                    sid = j.get('scrip_id') or j.get('ScripID') or j.get('SCRIP_ID') or j.get('SC_ID')
                    if not sid:
                        cd2 = j.get('CompanyMaster') or j.get('Data') or []
                        if isinstance(cd2, list) and cd2:
                            sid = cd2[0].get('scrip_id') or cd2[0].get('SC_ID')
                print(f'  {cd} {endpoint} status={r.status_code} sid={sid!r} keys={keys}', flush=True)
                if sid:
                    break
            except Exception as e:
                print(f'  {cd} {endpoint} json_err={e} status={r.status_code}', flush=True)

    # 3) Alternate: BSE scrip master list (cached once)
    section('BSE scrip master (all BSE equities)')
    u = 'https://api.bseindia.com/BseIndiaAPI/api/ListOfScripCodeNewFormat/w?Group=&Scripcode=&industry=&segment=Equity&status=Active'
    r, dt, err = get(u, BSE_H, timeout=30)
    if err:
        print(f'  ERROR {err!r}', flush=True)
    else:
        try:
            j = r.json()
            if isinstance(j, list):
                print(f'  status={r.status_code} len={len(j)} keys[0]={list(j[0].keys())[:10] if j else None}', flush=True)
            else:
                print(f'  status={r.status_code} not_list type={type(j).__name__}', flush=True)
        except Exception as e:
            print(f'  json_err={e} status={r.status_code} text_len={len(r.text)}', flush=True)

    # 4) Screener per-company page (pick a known existing slug)
    section('Screener per-company page (HDFCBANK)')
    u = 'https://www.screener.in/company/HDFCBANK/'
    r, dt, err = get(u, {'User-Agent': UA, 'Accept': 'text/html'}, timeout=25)
    if err:
        print(f'  ERROR {err!r}', flush=True)
    else:
        body = r.text
        print(f'  status={r.status_code} len={len(body)}', flush=True)
        markers = {
            'Quarterly_Results': 'Quarterly Results' in body,
            'Compounded_Sales': 'Compounded Sales' in body,
            'Market_Cap': 'Market Cap' in body,
            'Cloudflare': 'cloudflare' in body.lower() or 'cf-ray' in body.lower(),
        }
        print(f'  markers={markers}', flush=True)

    # 5) Screener /results/latest/ pagination check
    section('Screener results latest — count company links')
    u = 'https://www.screener.in/results/latest/?page=1'
    r, dt, err = get(u, {'User-Agent': UA, 'Accept': 'text/html'}, timeout=25)
    if err:
        print(f'  ERROR {err!r}', flush=True)
    else:
        body = r.text
        # count unique /company/SLUG/ occurrences
        import re
        slugs = set(re.findall(r'/company/([A-Z0-9\-&]+)/', body))
        print(f'  status={r.status_code} len={len(body)} unique_company_slugs={len(slugs)}', flush=True)
        print(f'  sample_slugs={list(slugs)[:8]}', flush=True)


if __name__ == '__main__':
    main()
