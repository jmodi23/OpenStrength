# biorxiv.py
# Standalone bioRxiv/medRxiv ingester:
# - Harvest by date range (required; API design).
# - Optional client-side keyword filter on title/abstract.
# - Saves normalized metadata per item and (if license allows) downloads PDF.
# - No repo-internal imports; uses only requests + tqdm + stdlib.

from __future__ import annotations

import os
import re
import io
import sys
import json
import time
import math
import hashlib
import argparse
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError  # type: ignore
from tqdm import tqdm  # type: ignore

DEFAULT_RATE = 1.0       # requests per second
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
CHUNK = 1 << 14

VALID_SERVERS = {"biorxiv", "medrxiv"}

def slugify(text: str, keep: str = "-._") -> str:
    text = text.strip().lower()
    text = text.replace(" ", "-").replace("/", "_")
    out = []
    for ch in text:
        if ch.isalnum() or ch in keep:
            out.append(ch)
        else:
            out.append("-")
    s = re.sub(r"-{2,}", "-", "".join(out)).strip("-")
    return s or "item"

def sha1_of_bytes(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def safe_write_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)

def safe_write_bytes(path: str, content: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(content)
    os.replace(tmp, path)

def is_probably_pdf_response(resp: requests.Response) -> bool:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "pdf" in ctype:
        return True
    try:
        path = urlparse(resp.url).path
    except Exception:
        path = ""
    return path.endswith(".pdf")

def license_allows_download(lic: Optional[str]) -> bool:
    if not lic:
        return False
    l = lic.strip().lower()
    if "creativecommons" in l or "creativecommons.org" in l:
        return True
    if re.search(r"\bcc[-_ ]?(by|by[-]nc|by[-]sa|by[-]nc[-]sa|by[_-]nd|0)\b", l):
        return True
    if "cc0" in l or "public domain" in l:
        return True
    return False

class RateLimiter:
    def _init_(self, rps: float):
        self.delay = 0.0 if rps <= 0 else 1.0 / float(rps)
        self._last = 0.0

    def wait(self):
        if self.delay <= 0:
            return
        now = time.time()
        delta = now - self._last
        if delta < self.delay:
            time.sleep(self.delay - delta)
        self._last = time.time()

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "OpenStrength/ingest (bioRxiv/medRxiv harvester)"
    })
    return s

# ---------------------------
# API helpers
# ---------------------------

def _api_base(server: str) -> str:
    return f"https://api.{server}.org/details"

def fetch_batch(
    server: str,
    start_date: str,  # YYYY-MM-DD
    end_date: str,    # YYYY-MM-DD
    cursor: int,
    sess: requests.Session,
    rate: RateLimiter,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    # Endpoint doc: /details/{server}/{from}/{to}/{cursor}
    url = f"{_api_base(server)}/{server}/{start_date}/{end_date}/{cursor}"
    for attempt in range(1, retries + 1):
        try:
            rate.wait()
            r = sess.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (Timeout, ConnectionError):
            if attempt == retries:
                raise
            time.sleep(min(2**attempt, 10))
        except HTTPError:
            # 4xx: don't hammer
            if 400 <= r.status_code < 500:
                return {"collection": [], "messages": []}
            if attempt == retries:
                raise
            time.sleep(min(2**attempt, 10))
        except RequestException:
            if attempt == retries:
                raise
            time.sleep(min(2**attempt, 10))
    return {"collection": [], "messages": []}

def normalize_record(rec: Dict[str, Any], server: str) -> Dict[str, Any]:
    # Example fields (per API docs): 'doi', 'title', 'authors', 'date', 'license', 'category', 'abstract', 'version', 'type', 'jatsxml', 'published', 'server', 'rel_title'
    doi = (rec.get("doi") or "").strip()
    title = (rec.get("title") or "").strip()
    abstract = (rec.get("abstract") or "").strip()
    authors = [a.strip() for a in (rec.get("authors") or "").split(";") if a.strip()]
    version = rec.get("version")
    date = rec.get("date")
    category = rec.get("category")
    lic = rec.get("license")

    pdf_url = pdf_url_from_rec(rec, server)

    normalized = {
        "source": server,
        "doi": doi,
        "title": title,
        "abstract": abstract,
        "authors": authors,
        "version": version,
        "date": date,
        "category": category,
        "license": lic,
        "pdf_url": pdf_url,
    }
    return {
        "_normalized": normalized,
        "_raw_biorxiv": rec,
    }

def pdf_url_from_rec(rec: Dict[str, Any], server: str) -> Optional[str]:
    # Typical pattern: https://www.biorxiv.org/content/10.1101/2024.01.01.123456v2.full.pdf
    doi = (rec.get("doi") or "").strip()
    v = rec.get("version")
    if not doi or not v:
        return None
    host = "www.biorxiv.org" if server == "biorxiv" else "www.medrxiv.org"
    return f"https://{host}/content/{doi}v{v}.full.pdf"

def fits_keywords(rec: Dict[str, Any], keywords: List[str]) -> bool:
    if not keywords:
        return True
    hay = " ".join([
        rec.get("title") or "",
        rec.get("abstract") or "",
        rec.get("category") or "",
    ]).lower()
    return all(k.lower() in hay for k in keywords)

def target_paths(raw_dir: str, server: str, doi: str) -> Dict[str, str]:
    base = os.path.join(raw_dir, server)
    ensure_dir(base)
    slug = slugify(doi) if doi else "item"
    item_dir = os.path.join(base, slug)
    ensure_dir(item_dir)
    return {
        "dir": item_dir,
        "meta": os.path.join(item_dir, "metadata.json"),
        "pdf": os.path.join(item_dir, "paper.pdf"),
        "pdf_info": os.path.join(item_dir, "paper.pdf.info.json"),
    }

def try_download_pdf(url: str, sess: requests.Session, rate: RateLimiter, retries: int = DEFAULT_RETRIES, timeout: int = DEFAULT_TIMEOUT) -> Optional[bytes]:
    for attempt in range(1, retries + 1):
        try:
            rate.wait()
            r = sess.get(url, timeout=timeout, stream=True, allow_redirects=True)
            if not r.ok:
                if r.status_code == 404:
                    return None
                r.raise_for_status()

            head = r.raw.read(8, decode_content=True) if hasattr(r.raw, "read") else r.content[:8]
            rest = r.content if head == r.content[:8] else r.raw.read()
            content = head + rest

            if not (is_probably_pdf_response(r) or content.startswith(b"%PDF")):
                return None
            return content
        except (Timeout, ConnectionError):
            if attempt == retries:
                return None
            time.sleep(min(2**attempt, 10))
        except RequestException:
            if attempt == retries:
                return None
            time.sleep(min(2**attempt, 10))
    return None

# ---------------------------
# High-level harvesting
# ---------------------------

def harvest_biorxiv(
    server: str,
    start_date: str,
    end_date: str,
    raw_dir: str,
    keywords: Optional[List[str]] = None,
    rate: float = DEFAULT_RATE,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    limit: Optional[int] = None,
    download_pdfs: bool = True,
) -> None:
    """
    Harvest bioRxiv/medRxiv by date range (YYYY-MM-DD).
    Optionally filter by keywords (title/abstract/category, AND across terms).
    Saves per-item folder with metadata (+ PDF when license allows).
    """
    server = server.lower().strip()
    if server not in VALID_SERVERS:
        raise ValueError(f"server must be one of {sorted(VALID_SERVERS)}")

    sess = make_session()
    limiter = RateLimiter(rate)
    ensure_dir(raw_dir)

    keywords = keywords or []
    cursor = 0
    fetched = 0
    total = None

    pbar = tqdm(desc=f"{server} {start_date}→{end_date}", unit="rec")
    while True:
        data = fetch_batch(server, start_date, end_date, cursor, sess, limiter, retries, timeout)
        coll = data.get("collection") or []
        # The API returns summary total in messages[0].total
        if total is None:
            try:
                msgs = data.get("messages") or []
                if msgs and isinstance(msgs, list) and "total" in msgs[0]:
                    total = int(msgs[0]["total"])
            except Exception:
                total = None

        if not coll:
            break

        for rec in coll:
            if limit is not None and fetched >= limit:
                break
            if not fits_keywords(rec, keywords):
                continue

            doi = (rec.get("doi") or "").strip()
            if not doi:
                continue

            paths = target_paths(raw_dir, server, doi)

            # If metadata exists, we may skip fetch unless we still want to try the PDF.
            meta_exists = os.path.exists(paths["meta"])
            if meta_exists:
                try:
                    with open(paths["meta"], "r", encoding="utf-8") as f:
                        existing = json.load(f)
                except Exception:
                    existing = None
            else:
                existing = None

            # Save/refresh metadata
            meta = normalize_record(rec, server)
            safe_write_json(paths["meta"], meta)

            # PDF?
            if download_pdfs and not os.path.exists(paths["pdf"]):
                lic = rec.get("license")
                pdf_url = meta["_normalized"].get("pdf_url")

                if pdf_url and license_allows_download(lic):
                    pdf_bytes = try_download_pdf(pdf_url, sess, limiter, retries, timeout)
                    if pdf_bytes:
                        safe_write_bytes(paths["pdf"], pdf_bytes)
                        info = {
                            "reason": "downloaded",
                            "license": lic,
                            "pdf_url": pdf_url,
                            "bytes": len(pdf_bytes),
                            "sha1": sha1_of_bytes(pdf_bytes),
                            "timestamp": time.time(),
                        }
                        safe_write_json(paths["pdf_info"], info)
                    else:
                        info = {
                            "reason": "download_failed_or_not_pdf",
                            "license": lic,
                            "pdf_url": pdf_url,
                            "timestamp": time.time(),
                        }
                        safe_write_json(paths["pdf_info"], info)
                elif pdf_url:
                    info = {
                        "reason": "license_not_allowed_or_unknown",
                        "license": lic,
                        "pdf_url": pdf_url,
                        "timestamp": time.time(),
                    }
                    safe_write_json(paths["pdf_info"], info)

            fetched += 1
            pbar.update(1)

        if limit is not None and fetched >= limit:
            break

        cursor += 100  # API page size is 100
    pbar.close()

# ---------------------------
# CLI
# ---------------------------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="bioRxiv/medRxiv harvester (metadata + legal PDFs)")
    ap.add_argument("--server", choices=sorted(VALID_SERVERS), required=True, help="Choose biorxiv or medrxiv.")
    ap.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    ap.add_argument("--raw-dir", required=True, help="Path to your data/raw directory (creates biorxiv/ or medrxiv/ inside).")
    ap.add_argument("--keyword", action="append", default=[], help="Keyword to AND-filter on title/abstract/category. Can be repeated.")
    ap.add_argument("--rate", type=float, default=DEFAULT_RATE, help=f"Requests per second (default {DEFAULT_RATE}).")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help=f"Per-request timeout seconds (default {DEFAULT_TIMEOUT}).")
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help=f"Retries per request (default {DEFAULT_RETRIES}).")
    ap.add_argument("--limit", type=int, default=None, help="Stop after N matching records.")
    ap.add_argument("--no-pdf", action="store_true", help="Do not attempt to download PDFs; metadata only.")
    args = ap.parse_args(argv)

    harvest_biorxiv(
        server=args.server,
        start_date=args.start,
        end_date=args.end,
        raw_dir=args.raw_dir,
        keywords=args.keyword,
        rate=args.rate,
        retries=args.retries,
        timeout=args.timeout,
        limit=args.limit,
        download_pdfs=not args.no_pdf,
    )
    return 0

if __name__ == "_main_":
    raise SystemExit(main())