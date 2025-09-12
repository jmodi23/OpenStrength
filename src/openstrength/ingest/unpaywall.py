#!/usr/bin/env python3
# -- coding: utf-8 --
"""
unpaywall.py
------------
Harvest DOIs from Crossref for a given topic/time window,
query Unpaywall for OA locations, and save metadata + PDFs.

Designed to be usable both:
  1) as a module: call harvest_unpaywall(...)
  2) as a CLI:   python -m unpaywall --query "creatine" --email you@example.com --out data/raw/unpaywall

Zero external project deps; only stdlib + requests.

Output layout:
  <out_dir>/<slug(query)>/
      crossref.jsonl                (raw Crossref items; one JSON per line)
      unpaywall.jsonl               (raw Unpaywall responses; one JSON per line)
      pdf/<doi_slug>.pdf            (downloaded PDFs when available)
      meta/<doi_slug>.json          (compact summary metadata we use later)
"""

from __future__ import annotations
import os
import re
import time
import json
import math
import html
import queue
import errno
import typing as T
from dataclasses import dataclass, asdict
from urllib.parse import quote, urlencode
import logging

try:
    import requests
except Exception as e:
    raise RuntimeError("This script requires the 'requests' package") from e


# -------------------------
# small, local utilities
# -------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_json(path: str, obj: T.Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def append_jsonl(path: str, obj: T.Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(obj, ensure_ascii=False))
        f.write("\n")

def write_bytes(path: str, data: bytes) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, 'wb') as f:
        f.write(data)

def slugify(s: str, maxlen: int = 80) -> str:
    s = s.lower()
    s = re.sub(r'[^\w\s-]+', '', s)
    s = re.sub(r'[\s_-]+', '', s).strip('')
    if len(s) > maxlen:
        s = s[:maxlen].rstrip('_')
    return s or "item"

def make_session(timeout: int = 30) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "OpenStrength-Ingest/1.0 (+https://github.com/)",
        "Accept": "application/json, /;q=0.8",
    })
    s.timeout = timeout
    return s

def backoff_sleep(i: int, base: float = 1.0, cap: float = 30.0):
    # i: attempt index starting at 0
    t = min(cap, base * (2 ** i) + (0.1 * i))
    time.sleep(t)


# -------------------------
# datatypes
# -------------------------

@dataclass
class HarvestResult:
    total_crossref: int = 0
    total_unpaywall: int = 0
    total_pdf_saved: int = 0
    errors: int = 0


# -------------------------
# Crossref search
# -------------------------

def crossref_iter(query: str, years: tuple[int,int]|None, rows: int, sess: requests.Session):
    """
    Yields raw Crossref 'items' dicts for query within year range.
    """
    base = "https://api.crossref.org/works"
    params = {
        "query": query,
        "rows": rows,
        "sort": "relevance",
        "select": "DOI,title,issued,type,URL,container-title,author,license,subject",
    }
    if years:
        y0, y1 = years
        params["filter"] = f"from-pub-date:{y0}-01-01,until-pub-date:{y1}-12-31"

    cursor = "*"
    seen = 0
    while True:
        url = f"{base}?{urlencode(params)}&cursor={quote(cursor)}"
        r = sess.get(url, timeout=60)
        if r.status_code >= 500:
            # retry on 5xx
            for i in range(4):
                backoff_sleep(i)
                r = sess.get(url, timeout=60)
                if r.ok: break
        r.raise_for_status()
        data = r.json()
        items = data.get("message", {}).get("items", [])
        if not items:
            break
        for it in items:
            yield it
            seen += 1
        cursor = data.get("message", {}).get("next-cursor")
        if not cursor:
            break
        # polite pacing
        time.sleep(0.1)
        # guardrail: do not exceed requested rows if rows < default page size
        if rows and seen >= rows:
            break


# -------------------------
# Unpaywall lookup
# -------------------------

def unpaywall_lookup(doi: str, email: str, sess: requests.Session) -> dict|None:
    """
    Return full Unpaywall JSON for DOI or None on 404.
    """
    if not doi: return None
    url = f"https://api.unpaywall.org/v2/{quote(doi)}?email={quote(email)}"
    r = sess.get(url, timeout=60)
    if r.status_code == 404:
        return None
    if r.status_code >= 500:
        for i in range(4):
            backoff_sleep(i)
            r = sess.get(url, timeout=60)
            if r.ok: break
    r.raise_for_status()
    return r.json()

def pick_pdf_url(upw: dict) -> str|None:
    # Prefer best_oa_location url_for_pdf; fall back to first oa_location with url_for_pdf
    loc = upw.get("best_oa_location") or {}
    url = loc.get("url_for_pdf") or loc.get("url")
    if url and url.lower().endswith(".pdf"):
        return url
    # try alternate locations
    for loc in upw.get("oa_locations", []):
        u = loc.get("url_for_pdf") or loc.get("url")
        if u and u.lower().endswith(".pdf"):
            return u
    return None

def download_pdf(url: str, sess: requests.Session) -> bytes|None:
    # some URLs are behind 302 redirect
    r = sess.get(url, timeout=90, allow_redirects=True, stream=True)
    if r.status_code >= 500:
        for i in range(4):
            backoff_sleep(i)
            r = sess.get(url, timeout=90, allow_redirects=True, stream=True)
            if r.ok: break
    if not r.ok:
        return None
    content_type = r.headers.get("Content-Type","").lower()
    if "pdf" not in content_type and not url.lower().endswith(".pdf"):
        return None
    return r.content


# -------------------------
# public API
# -------------------------

def harvest_unpaywall(
    topic: str,
    out_dir: str,
    *,
    email: str,
    years: tuple[int,int]|None=(2010, 2025),
    max_rows: int=1000,
    sleep_between: float=0.1,
    allow_non_oa: bool=False,
    save_pdfs: bool=True,
) -> HarvestResult:
    """
    Find DOIs from Crossref for a topic, query Unpaywall, and save results.
    Returns a HarvestResult summary. Never raises for individual record failures.
    """
    result = HarvestResult()
    sess = make_session()
    qslug = slugify(f"{topic}")
    base_dir = os.path.join(out_dir, qslug)
    pdf_dir  = os.path.join(base_dir, "pdf")
    meta_dir = os.path.join(base_dir, "meta")
    ensure_dir(pdf_dir); ensure_dir(meta_dir)

    crossref_log = os.path.join(base_dir, "crossref.jsonl")
    unpaywall_log = os.path.join(base_dir, "unpaywall.jsonl")

    seen_dois: set[str] = set()

    # 1) Crossref search
    for item in crossref_iter(topic, years, rows=max_rows, sess=sess):
        doi = item.get("DOI")
        if not doi or doi in seen_dois:
            continue
        seen_dois.add(doi)
        append_jsonl(crossref_log, item)
        result.total_crossref += 1

        # 2) Unpaywall
        try:
            upw = unpaywall_lookup(doi, email, sess)
        except Exception as e:
            logging.warning("Unpaywall lookup failed for DOI=%s: %s", doi, e)
            result.errors += 1
            continue
        if upw is None:
            # no entry in Unpaywall
            result.total_unpaywall += 1
            append_jsonl(unpaywall_log, {"doi": doi, "unpaywall": None})
            continue

        result.total_unpaywall += 1
        append_jsonl(unpaywall_log, upw)

        # 3) Save a compact metadata summary (always)
        summary = {
            "doi": doi,
            "title": (upw.get("title") or (item.get("title") or [""])[0]),
            "is_oa": upw.get("is_oa", False),
            "oa_status": upw.get("oa_status"),
            "journal_name": upw.get("journal_name") or (item.get("container-title") or [""])[0],
            "year": (item.get("issued",{}).get("date-parts") or [[None]])[0][0],
            "best_oa_location": upw.get("best_oa_location"),
        }
        write_json(os.path.join(meta_dir, f"{slugify(doi)}.json"), summary)

        # 4) Optionally attempt to fetch the PDF
        if save_pdfs:
            url_pdf = pick_pdf_url(upw)
            if (not allow_non_oa) and not upw.get("is_oa", False):
                url_pdf = None
            if url_pdf:
                try:
                    data = download_pdf(url_pdf, sess)
                    if data:
                        write_bytes(os.path.join(pdf_dir, f"{slugify(doi)}.pdf"), data)
                        result.total_pdf_saved += 1
                except Exception as e:
                    logging.warning("PDF download failed DOI=%s url=%s : %s", doi, url_pdf, e)
                    result.errors += 1

        # politeness
        if sleep_between:
            time.sleep(sleep_between)

    return result


# -------------------------
# CLI
# -------------------------

def _parse_cli(argv=None):
    import argparse
    p = argparse.ArgumentParser(description="Unpaywall harvester")
    p.add_argument("--query", required=True, help="topic query string (e.g., 'creatine')")
    p.add_argument("--out", required=True, help="output directory (e.g., data/raw/unpaywall)")
    p.add_argument("--email", required=True, help="contact email for Unpaywall")
    p.add_argument("--years", default="2010:2025", help="year range 'YYYY:YYYY' (inclusive)")
    p.add_argument("--rows", type=int, default=1000, help="max Crossref rows to retrieve")
    p.add_argument("--no-pdf", action="store_true", help="do not try to save PDFs")
    p.add_argument("--allow-non-oa", action="store_true", help="attempt PDFs even when not OA (not recommended)")
    return p.parse_args(argv)

def main(argv=None):
    args = _parse_cli(argv)
    y0, y1 = None, None
    if args.years and ":" in args.years:
        parts = args.years.split(":")
        y0 = int(parts[0]); y1 = int(parts[1])
    res = harvest_unpaywall(
        topic=args.query,
        out_dir=args.out,
        email=args.email,
        years=(y0,y1) if y0 and y1 else None,
        max_rows=args.rows,
        save_pdfs=not args.no_pdf,
        allow_non_oa=args.allow_non_oa,
    )
    print(json.dumps(asdict(res), indent=2))

if __name__ == "_main_":
    main()