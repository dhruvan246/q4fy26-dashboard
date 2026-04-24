#!/usr/bin/env python3
"""
Probe screener page structure from GH Actions to finalize the parser.
Output: row labels found in the #quarters table, sample cells,
and a minimal hand-parsed extraction for sanity check.
"""
import json
import re
import sys
import time
import requests

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
H = {"User-Agent": UA, "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"}


def section(title):
    print(f'\n=== {title} ===', flush=True)


def strip_tags(s):
    return re.sub(r'<[^>]+>', '', s).strip()


def parse_quarters(html, company_label):
    # Look for the quarters section
    m = re.search(r'id="quarters"[\s\S]*?</section>', html)
    if not m:
        print(f'  {company_label}: NO quarters section', flush=True)
        return
    q = m.group(0)
    # headers
    thead = re.search(r'<thead[\s\S]*?</thead>', q)
    if thead:
        hdrs = [strip_tags(x) for x in re.findall(r'<th[^>]*>([\s\S]*?)</th>', thead.group(0))]
        print(f'  {company_label}: headers={hdrs}', flush=True)
    # rows
    tbody = re.search(r'<tbody[\s\S]*?</tbody>', q)
    if not tbody:
        print(f'  {company_label}: NO tbody', flush=True)
        return
    rows = re.findall(r'<tr[^>]*>([\s\S]*?)</tr>', tbody.group(0))
    print(f'  {company_label}: row_count={len(rows)}', flush=True)
    for ri, row in enumerate(rows):
        cells = re.findall(r'<t[dh][^>]*>([\s\S]*?)</t[dh]>', row)
        cells = [strip_tags(c) for c in cells]
        if cells:
            label = cells[0]
            vals = cells[1:]
            if ri < 12:  # print first 12 rows
                print(f'    r{ri}: {label!r} -> {vals[-5:]}', flush=True)


def page_meta(html, company_label):
    # market cap
    mcap = re.search(r'Market Cap[\s\S]{0,200}?<span[^>]*class="number"[^>]*>([^<]+)</span>', html)
    # current price (first "Current Price" number)
    price = re.search(r'Current Price[\s\S]{0,200}?<span[^>]*class="number"[^>]*>([^<]+)</span>', html)
    pe = re.search(r'Stock P/E[\s\S]{0,200}?<span[^>]*class="number"[^>]*>([^<]+)</span>', html)
    name = re.search(r'<h1[^>]*>\s*([^<]+)\s*</h1>', html)
    print(f'  {company_label}: name={name.group(1).strip() if name else None!r}', flush=True)
    print(f'  {company_label}: price={price.group(1).strip() if price else None!r} '
          f'mcap={mcap.group(1).strip() if mcap else None!r} '
          f'pe={pe.group(1).strip() if pe else None!r}', flush=True)


def probe_company(slug):
    section(f'Screener /company/{slug}/')
    for path in (f'/company/{slug}/consolidated/', f'/company/{slug}/'):
        url = f'https://www.screener.in{path}'
        try:
            r = requests.get(url, headers=H, timeout=25)
        except Exception as e:
            print(f'  {slug} {path} ERR {e!r}', flush=True)
            continue
        if r.status_code != 200:
            print(f'  {slug} {path} status={r.status_code}', flush=True)
            continue
        html = r.text
        print(f'  {slug} {path} status=200 len={len(html)}', flush=True)
        page_meta(html, slug)
        parse_quarters(html, slug)
        break


def main():
    # Three companies: well-known (HDFCBANK), mid-cap (EMAPARTNER),
    # numeric-slug (513119 = Onix Solar).
    probe_company('HDFCBANK')
    time.sleep(0.5)
    probe_company('EMAPARTNER')
    time.sleep(0.5)
    probe_company('513119')


if __name__ == '__main__':
    main()
