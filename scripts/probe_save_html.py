#!/usr/bin/env python3
"""Download HDFCBANK screener page from GH Actions runner and save to /tmp/hdfc.html
   so we can inspect and write a parser against real content.
   The workflow step after this commits the file back to the repo."""
import os
import sys
import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

for label, url in [
    ("hdfcbank", "https://www.screener.in/company/HDFCBANK/consolidated/"),
    ("emapartner", "https://www.screener.in/company/EMAPARTNER/"),
    ("onix_513119", "https://www.screener.in/company/513119/"),
]:
    r = requests.get(url, headers={"User-Agent": UA, "Accept": "text/html"}, timeout=30)
    print(f'{label}: status={r.status_code} len={len(r.text)}', flush=True)
    out = f'samples/{label}.html'
    os.makedirs('samples', exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        f.write(r.text)
    print(f'  wrote {out}', flush=True)
