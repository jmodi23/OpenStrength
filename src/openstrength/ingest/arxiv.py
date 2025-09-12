# src/openstrength/ingest/arxiv.py
from __future__ import annotations

import json
import math
import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests

try:
    import feedparser  # nicer Atom parsing if available
    HAS_FEEDPARSER = True
except Exception:
    import xml.etree.ElementTree as ET
    HAS_FEEDPARSER = False

# ----------------------------
# Config dataclass (internal)
# ----------------------------
@dataclass
class ArxivConfig:
    enabled: bool
    queries: List[str]
    categories: List[str]
    max_results_per_query: int
    rate_per_sec: float  # requests/second to API and to PDFs

# ----------------------------
# Utilities
# ----------------------------
ARXIV_API = "https://export.arxiv.org/api/query"
DEFAULT_TIMEOUT = 30
RETRY_STATUS = {500, 502, 503, 504, 520, 522, 524}

def slugify(text: str, maxlen: int = 80) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("/", "-")
    text = re.sub(r"[^A-Za-z0-9 _.\-]", "", text)
    text = text.replace(" ", "_")
    if len(text) > maxlen:
        text = text[:maxlen]
    return text or "untitled"

def mk_session(rate_per_sec: float) -> requests.Session:
    sess = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.packages.urllib3.util.retry.Retry(
            total=3,
            read=3,
            connect=3,
            backoff_factor=0.5,
            status_forcelist=list(RETRY_STATUS),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
    )
    sess.headers.update({"User-Agent": "OpenStrength/ingest (arxiv) +https://arxiv.org"})
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    # simple throttle controller
    sess._os_last_call = 0.0  # type: ignore[attr-defined]
    sess._os_min_interval = 1.0 / max(0.01, rate_per_sec)  # type: ignore[attr-defined]
    return sess

def throttle(sess: requests.Session):
    now = time.time()
    elapsed = now - getattr(sess, "_os_last_call", 0.0)
    wait = getattr(sess, "_os_min_interval", 0.0) - elapsed
    if wait > 0:
        time.sleep(wait)
    sess._os_last_call = time.time()

def safe_write_json(path: Path, obj: Dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def safe_write_bytes(path: Path, data_iter: Iterable[bytes]):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        for chunk in data_iter:
            if chunk:
                f.write(chunk)
    os.replace(tmp, path)

# ----------------------------
# Atom parsing
# ----------------------------
def parse_entries_atom(atom_text: str) -> List[Dict]:
    """
    Returns a list of dict entries with keys:
    id, title, summary, authors (list[str]), categories (list[str]), links (list[dict]),
    published, updated, pdf_url (if available)
    """
    out: List[Dict] = []

    if HAS_FEEDPARSER:
        feed = feedparser.parse(atom_text)
        for e in feed.entries:
            links = []
            pdf_url = None
            for l in getattr(e, "links", []):
                links.append({"href": l.get("href"), "type": l.get("type"), "rel": l.get("rel")})
                # arXiv marks the PDF link as rel=related type=application/pdf OR rel=alternate with .pdf
                href = l.get("href", "")
                if (l.get("type") == "application/pdf") or href.endswith(".pdf"):
                    pdf_url = href
            cats = [c.term for c in getattr(e, "tags", []) if hasattr(c, "term")]
            authors = [a.name for a in getattr(e, "authors", []) if hasattr(a, "name")]
            out.append(
                {
                    "id": getattr(e, "id", ""),
                    "title": getattr(e, "title", ""),
                    "summary": getattr(e, "summary", ""),
                    "authors": authors,
                    "categories": cats,
                    "links": links,
                    "published": getattr(e, "published", ""),
                    "updated": getattr(e, "updated", ""),
                    "pdf_url": pdf_url,
                }
            )
        return out

    # Fallback: minimal ElementTree parsing
    ns = {"a": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(atom_text)
    for e in root.findall("a:entry", ns):
        _id = e.findtext("a:id", default="", namespaces=ns)
        title = e.findtext("a:title", default="", namespaces=ns)
        summary = e.findtext("a:summary", default="", namespaces=ns)
        published = e.findtext("a:published", default="", namespaces=ns)
        updated = e.findtext("a:updated", default="", namespaces=ns)

        authors = []
        for a_el in e.findall("a:author", ns):
            name = a_el.findtext("a:name", default="", namespaces=ns)
            if name:
                authors.append(name)

        cats = []
        for c in e.findall("a:category", ns):
            term = c.attrib.get("term")
            if term:
                cats.append(term)

        links = []
        pdf_url = None
        for l in e.findall("a:link", ns):
            href = l.attrib.get("href")
            typ = l.attrib.get("type")
            rel = l.attrib.get("rel")
            if href:
                links.append({"href": href, "type": typ, "rel": rel})
                if (typ == "application/pdf") or href.endswith(".pdf"):
                    pdf_url = href

        out.append(
            {
                "id": _id,
                "title": title,
                "summary": summary,
                "authors": authors,
                "categories": cats,
                "links": links,
                "published": published,
                "updated": updated,
                "pdf_url": pdf_url,
            }
        )
    return out

# ----------------------------
# API querying with pagination
# ----------------------------
def build_query_term(user_query: str, categories: List[str]) -> str:
    """
    Combine the user's query with category filters, if any.
    Example: (ti:"creatine" OR abs:"creatine") AND (cat:q-bio OR cat:cs.LG)
    """
    q = user_query.strip()
    if categories:
        cat_clause = " OR ".join([f"cat:{c}" for c in categories])
        return f"({q}) AND ({cat_clause})"
    return q

def fetch_arxiv_batch(sess: requests.Session, query: str, start: int, max_results: int) -> Tuple[List[Dict], int]:
    params = {
        "search_query": query,
        "start": start,
        "max_results": max_results,
        # Tip: add sortBy=lastUpdatedDate if you want recency bias. We keep default relevance.
    }
    url = f"{ARXIV_API}?{urlencode(params)}"
    throttle(sess)
    r = sess.get(url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    entries = parse_entries_atom(r.text)

    # Determine totalResults if feedparser exposed it, else approximate
    total = None
    if HAS_FEEDPARSER:
        try:
            total = int(getattr(feedparser.parse(r.text), "feed", {}).get("opensearch_totalresults", 0))
        except Exception:
            total = None
    if total is None:
        # Fallback: infer via smart guess (we only know length of this page)
        total = start + len(entries)

    return entries, total

# ----------------------------
# PDF fetching (robust)
# ----------------------------
def fetch_pdf_with_retries(arxiv_pdf_url: str, out_path: Path, sess: requests.Session, max_retries: int = 6) -> bool:
    """
    Download PDF from arXiv with retries/backoff.
    Returns True if saved successfully, False otherwise.
    """
    if out_path.exists() and out_path.stat().st_size > 1024:
        return True

    for attempt in range(1, max_retries + 1):
        try:
            throttle(sess)
            r = sess.get(arxiv_pdf_url, timeout=DEFAULT_TIMEOUT, stream=True)
            ctype = r.headers.get("Content-Type", "")
            # Handle transient statuses first
            if r.status_code in RETRY_STATUS:
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = int(ra)
                    except Exception:
                        sleep_s = min(60, 2 ** attempt)
                else:
                    sleep_s = min(60, 2 ** attempt) + random.uniform(0, 1.0)
                print(f"[arxiv] pdf-get retry {attempt}/{max_retries} url={arxiv_pdf_url} status={r.status_code}")
                time.sleep(sleep_s)
                continue

            if r.status_code == 200:
                if "application/pdf" not in ctype.lower():
                    # Not a PDF (often text/plain for error page) -> backoff + retry
                    print(f"[arxiv] pdf-get skipped url={arxiv_pdf_url} status={r.status_code} ctype={ctype}")
                    time.sleep(min(30, 2 ** attempt) + random.uniform(0, 0.5))
                    continue

                safe_write_bytes(out_path, r.iter_content(chunk_size=1 << 15))
                return True

            if r.status_code == 404:
                print(f"[arxiv] pdf-get 404 not found url={arxiv_pdf_url}")
                return False

            # Other non-OK
            print(f"[arxiv] pdf-get status={r.status_code} url={arxiv_pdf_url}")
            time.sleep(min(30, 2 ** attempt) + random.uniform(0, 0.5))

        except requests.RequestException as e:
            print(f"[arxiv] pdf-get error url={arxiv_pdf_url} err={e.__class__.__name__}: {e}")
            time.sleep(min(30, 2 ** attempt) + random.uniform(0, 0.5))

    # Optional final fallback: try e-print endpoint (comment out to disable)
    try:
        ep_url = arxiv_pdf_url.replace("/pdf/", "/e-print/").rsplit(".pdf", 1)[0]
        throttle(sess)
        ep = sess.get(ep_url, params={"format": "pdf"}, timeout=DEFAULT_TIMEOUT, stream=True)
        if ep.status_code == 200 and "application/pdf" in ep.headers.get("Content-Type", "").lower():
            safe_write_bytes(out_path, ep.iter_content(chunk_size=1 << 15))
            return True
        else:
            print(f"[arxiv] e-print fallback failed url={ep_url} status={ep.status_code} "
                  f"ctype={ep.headers.get('Content-Type')}")
    except requests.RequestException as e:
        print(f"[arxiv] e-print fallback error url={arxiv_pdf_url} err={e}")

    return False

# ----------------------------
# Main per-query harvest
# ----------------------------
def harvest_query(
    sess: requests.Session,
    out_root: Path,
    user_query: str,
    categories: List[str],
    max_results_per_query: int,
) -> None:
    query = build_query_term(user_query, categories)
    qslug = slugify(user_query)
    base_dir = out_root / "arxiv" / qslug
    base_dir.mkdir(parents=True, exist_ok=True)

    per_page = 100  # the API supports up to 2000, but smaller pages -> more resilient + nicer progress
    total_target = max_results_per_query
    total_seen = 0
    start = 0

    print(f"[arxiv] query='{user_query}' cats={categories} => '{query}'")

    while total_seen < total_target:
        to_fetch = min(per_page, total_target - total_seen)
        try:
            entries, total_reported = fetch_arxiv_batch(sess, query, start, to_fetch)
        except requests.HTTPError as e:
            print(f"[arxiv] API error start={start} err={e}")
            # gentle backoff then continue
            time.sleep(3.0)
            continue

        if not entries:
            break

        for e in entries:
            # arXiv ID is the trailing part of entry id, like 'http://arxiv.org/abs/2411.01004v1'
            raw_id = e.get("id", "")
            arxiv_id = raw_id.rsplit("/", 1)[-1] if raw_id else None
            if not arxiv_id:
                # Skip malformed entries
                continue

            # per-paper dir
            paper_dir = base_dir / slugify(arxiv_id)
            paper_dir.mkdir(parents=True, exist_ok=True)

            # Prepare metadata
            meta = {
                "source": "arxiv",
                "query": user_query,
                "categories_filter": categories,
                "arxiv_id": arxiv_id,
                "title": e.get("title"),
                "summary": e.get("summary"),
                "authors": e.get("authors", []),
                "categories": e.get("categories", []),
                "links": e.get("links", []),
                "published": e.get("published"),
                "updated": e.get("updated"),
                "pdf_url": e.get("pdf_url"),
            }

            # Write metadata first
            safe_write_json(paper_dir / "metadata.json", meta)

            # Try to fetch PDF (if URL known)
            pdf_url = e.get("pdf_url")
            if pdf_url:
                pdf_path = paper_dir / "paper.pdf"
                ok = fetch_pdf_with_retries(pdf_url, pdf_path, sess)
                if ok:
                    meta["pdf_status"] = "ok"
                    meta["pdf_path"] = str(pdf_path.as_posix())
                else:
                    meta["pdf_status"] = "pending"
                    meta["pdf_path"] = None
                # Update metadata with final status
                safe_write_json(paper_dir / "metadata.json", meta)

        got = len(entries)
        total_seen += got
        start += got

        # If the API reports fewer total results than we want, stop when we’ve reached it
        if total_reported is not None and start >= total_reported:
            break

# ----------------------------
# Public entrypoint for run.py
# ----------------------------
def run_from_config(cfg: Dict, out_root: str | Path) -> None:
    """
    Expected by src.openstrength.ingest.run
    cfg: the full YAML-loaded dict; this function reads cfg["arxiv"]
    out_root: base directory for outputs (e.g., 'data/raw')
    """
    arx = cfg.get("arxiv") or {}
    enabled = bool(arx.get("enabled", False))
    if not enabled:
        print("[arxiv] disabled in config; skipping")
        return

    queries = list(arx.get("queries") or [])
    categories = list(arx.get("categories") or [])
    max_results = int(arx.get("max_results_per_query", 1000))
    rate_per_sec = float(arx.get("rate_per_sec", 1.0))

    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    if not queries:
        print("[arxiv] no queries found; nothing to do")
        return

    sess = mk_session(rate_per_sec=rate_per_sec)

    for q in queries:
        try:
            harvest_query(
                sess=sess,
                out_root=out_root,
                user_query=q,
                categories=categories,
                max_results_per_query=max_results,
            )
        except Exception as e:
            print(f"[arxiv] ERROR query='{q}': {e.__class__.__name__}: {e}")

if __name__ == "__main__":
    # minimal manual test (reads environment variables or defaults)
    import argparse, yaml

    p = argparse.ArgumentParser()
    p.add_argument("--sources", type=str, required=True, help="Path to sources.yaml")
    args = p.parse_args()

    with open(args.sources, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # out_root from the config (paths.raw_dir), or default to ./data/raw
    out_root = (cfg.get("paths") or {}).get("raw_dir", "data/raw")
    run_from_config(cfg, out_root)
