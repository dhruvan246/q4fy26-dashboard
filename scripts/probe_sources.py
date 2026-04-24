#!/usr/bin/env python3
"""
One-shot probe: which Q4-results data sources are reachable from GitHub
Actions runners? Used to pick the discovery source for new filings.
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


def probe(name, url, headers=None, timeout=15):
    h = {"User-Agent": UA, "Accept": "application/json, text/html, */*"}
    if headers:
        h.update(headers)
    print(f'\n=== {name} ===', flush=True)
    print(f'GET {url}', flush=True)
    t = time.time()
    try:
        r = requests.get(url, headers=h, timeout=timeout)
        dt = time.time() - t
        body = r.text
        print(f'  status={r.status_code} dt={dt:.2f}s len={len(body)}', flush=True)
        print(f'  ct={r.headers.get("Content-Type")}', flush=True)
        print(f'  preview={body[:400]!r}', flush=True)
        # try to parse as JSON
        try:
            j = r.json()
            if isinstance(j, dict):
                print(f'  json_keys={list(j.keys())[:10]}', flush=True)
                if 'Table' in j:
                    tbl = j['Table']
                    print(f'  Table_len={len(tbl)}', flush=True)
                    if tbl:
                        print(f'  Table[0]_keys={list(tbl[0].keys())[:15]}', flush=True)
                        print(f'  Table[0]={json.dumps(tbl[0])[:600]}', flush=True)
        except Exception as e:
            print(f'  json_parse_error={e}', flush=True)
    except Exception as e:
        dt = time.time() - t
        print(f'  ERROR after {dt:.2f}s: {e!r}', flush=True)


def main():
    today = '20260424'
    from_date = '20260301'

    # BSE: all Q4 financial results announcements
    probe(
        'BSE Announcements (Financial Results)',
        'https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w'
        f'?pageno=1&strCat=Result&strPrevDate={from_date}'
        f'&strScrip=&strSearch=P&strToDate={today}'
        '&strType=C&subcategory=Financial%20Results',
        headers={
            'Referer': 'https://www.bseindia.com/',
            'Origin': 'https://www.bseindia.com',
        },
    )

    # BSE alternative: CorpannResultsGrid
    probe(
        'BSE CorpannResults (quarterly)',
        'https://api.bseindia.com/BseIndiaAPI/api/CorpannResultsGrid/w'
        f'?pageno=1&strCat=-1&strPrevDate={from_date}&strScrip=&strSearch=P'
        f'&strToDate={today}&strType=C',
        headers={
            'Referer': 'https://www.bseindia.com/',
            'Origin': 'https://www.bseindia.com',
        },
    )

    # NSE: corporate financial results
    probe(
        'NSE corporates-financial-results',
        'https://www.nseindia.com/api/corporates-financial-results'
        '?index=equities&period=Quarterly'
        f'&from_date=01-04-2026&to_date=24-04-2026',
        headers={
            'Referer': 'https://www.nseindia.com/',
            'Accept': 'application/json',
        },
    )

    # screener.in (known blocked — baseline check)
    probe(
        'screener.in /results/latest',
        'https://www.screener.in/results/latest/',
    )

    # Yahoo chart (known working — baseline check)
    probe(
        'Yahoo Finance chart HDFCBANK.BO',
        'https://query1.finance.yahoo.com/v8/finance/chart/HDFCBANK.BO?range=5d&interval=1d',
    )


if __name__ == '__main__':
    main()
