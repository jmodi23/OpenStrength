# src/openstrength/ingest/arxiv.py
# Standalone arXiv harvester: saves metadata JSON for every hit and downloads PDFs when available.

from __future__ import annotations
import os
import re
import json
import time
import math
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import requests

# feedparser is convenient; we fall back to ElementTree if it's not installed.
try:
    import feedparser  # type: ignore
    HAS_FEEDPARSER = True
except Exception:
    HAS_FEEDPARSER = False
    import xml.etree.ElementTree as ET

ARXIV_API = "http://export.arxiv.org/api/query"
ARXIV_PDF = "https://arxiv.org/pdf/{id}.pdf"

# ---------- small, local utilities (no external project deps) ----------

def _mk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "OpenStrength/ingest (arxiv) +https://arxiv.org",
        "Accept": "application/xml, text/xml;q=0.9, */*;q=0.1",
    })
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def _safe_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)

def _safe_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _sanitize_query(q: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", q.lower()).strip("_")

def _log_setup():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s"
    )

# ---------- arXiv parsing ----------

def _parse_feed_text(xml_text: str) -> List[Dict[str, Any]]:
    """Parse arXiv Atom feed into a list of dicts (works with or without feedparser)."""
    if HAS_FEEDPARSER:
        feed = feedparser.parse(xml_text)
        out: List[Dict[str, Any]] = []
        for e in feed.entries:
            arxiv_id = e.get("id", "").split("/")[-1]
            authors = [a.get("name") for a in e.get("authors", [])] if e.get("authors") else []
            links = []
            for l in e.get("links", []):
                links.append({
                    "href": l.get("href"),
                    "type": l.get("type"),
                    "rel": l.get("rel"),
                })
            out.append({
                "id": arxiv_id,
                "title": e.get("title", "").strip(),
                "summary": e.get("summary", "").strip(),
                "published": e.get("published"),
                "updated": e.get("updated"),
                "authors": authors,
                "primary_category": getattr(e, "arxiv_primary_category", {}).get("term") if hasattr(e, "arxiv_primary_category") else None,
                "categories": [t.get("term") for t in e.get("tags", [])] if e.get("tags") else [],
                "doi": getattr(e, "arxiv_doi", None),
                "link": e.get("link"),
                "links": links,
            })
        return out

    # Fallback simple XML parsing (ElementTree). Handles the common fields.
    ns = {
        "a": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }
    root = ET.fromstring(xml_text)
    out: List[Dict[str, Any]] = []
    for entry in root.findall("a:entry", ns):
        id_text = (entry.findtext("a:id", default="", namespaces=ns) or "").split("/")[-1]
        title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
        summary = (entry.findtext("a:summary", default="", namespaces=ns) or "").strip()
        published = entry.findtext("a:published", default=None, namespaces=ns)
        updated = entry.findtext("a:updated", default=None, namespaces=ns)
        authors = [a.findtext("a:name", default="", namespaces=ns) for a in entry.findall("a:author", ns)]
        links = []
        for l in entry.findall("a:link", ns):
            links.append({
                "href": l.attrib.get("href"),
                "type": l.attrib.get("type"),
                "rel": l.attrib.get("rel"),
            })
        primary_cat_el = entry.find("arxiv:primary_category", ns)
        primary_category = primary_cat_el.attrib.get("term") if primary_cat_el is not None else None
        cats = [t.attrib.get("term") for t in entry.findall("a:category", ns)]
        doi_el = entry.find("arxiv:doi", ns)
        doi = doi_el.text if doi_el is not None else None

        out.append({
            "id": id_text,
            "title": title,
            "summary": summary,
            "published": published,
            "updated": updated,
            "authors": authors,
            "primary_category": primary_category,
            "categories": cats,
            "doi": doi,
            "link": next((l["href"] for l in links if l.get("rel") == "alternate"), None),
            "links": links,
        })
    return out

def _fetch_batch(sess: requests.Session, query: str, start: int, max_results: int) -> List[Dict[str, Any]]:
    url = f"{ARXIV_API}?search_query={quote_plus(query)}&start={start}&max_results={max_results}"
    r = sess.get(url, timeout=60)
    r.raise_for_status()
    return _parse_feed_text(r.text)

def _download_pdf(sess: requests.Session, arxiv_id: str, out_path: Path) -> bool:
    pdf_url = ARXIV_PDF.format(id=arxiv_id)
    try:
        r = sess.get(pdf_url, timeout=90)
        if r.status_code == 200 and "application/pdf" in r.headers.get("content-type", "").lower():
            _safe_write_bytes(out_path, r.content)
            return True
        logging.info(f"[arxiv] pdf-get skipped id={arxiv_id} status={r.status_code} ctype={r.headers.get('content-type')}")
    except Exception as e:
        logging.info(f"[arxiv] pdf-get failed id={arxiv_id} → {e}")
    return False

# ---------- public entrypoint ----------

def harvest_arxiv(cfg: Dict[str, Any]) -> None:
    """
    Harvest arXiv results according to cfg:
      cfg["paths"]["raw_dir"] -> base data directory
      cfg["arxiv"]["queries"] -> list[str] arXiv API search queries
      cfg["arxiv"]["max_results_per_query"] -> int (default 1000)
      cfg["arxiv"]["sleep_between_requests_sec"] -> float (default 3.0)
      cfg["arxiv"]["pdf"] -> bool (download PDFs, default True)
    """
    _log_setup()

    raw_dir = Path(cfg["paths"]["raw_dir"])
    out_root = raw_dir / "arxiv"
    out_root.mkdir(parents=True, exist_ok=True)

    arxiv_cfg = cfg.get("arxiv", {})
    queries: List[str] = arxiv_cfg.get("queries", [])
    max_per_query: int = int(arxiv_cfg.get("max_results_per_query", 1000))
    per_request: int = 100  # API soft max
    sleep_s: float = float(arxiv_cfg.get("sleep_between_requests_sec", 3.0))
    want_pdf: bool = bool(arxiv_cfg.get("pdf", True))

    if not queries:
        logging.info("[arxiv] No queries configured; nothing to do.")
        return

    sess = _mk_session()

    for q in queries:
        safe_folder = _sanitize_query(q)
        folder = out_root / safe_folder
        folder.mkdir(parents=True, exist_ok=True)

        total_saved = 0
        start = 0
        while start < max_per_query:
            try:
                batch = _fetch_batch(sess, q, start, per_request)
            except requests.HTTPError as e:
                logging.info(f"[arxiv] HTTPError for start={start}: {e}")
                break
            except Exception as e:
                logging.info(f"[arxiv] fetch error start={start}: {e}")
                break

            if not batch:
                break

            for item in batch:
                arxiv_id = item["id"]  # may include version (e.g., 2101.00001v2)
                if not arxiv_id:
                    continue

                meta_path = folder / f"{arxiv_id}.json"
                pdf_path = folder / f"{arxiv_id}.pdf"

                # Always write metadata JSON
                _safe_write_json(meta_path, item)

                # Try PDF download (optional)
                if want_pdf and not pdf_path.exists():
                    ok = _download_pdf(sess, arxiv_id, pdf_path)
                    if not ok and pdf_path.exists() and pdf_path.stat().st_size == 0:
                        pdf_path.unlink(missing_ok=True)

                total_saved += 1

            logging.info(f"[arxiv] '{q}': saved {total_saved} records (start={start})")
            start += per_request
            time.sleep(sleep_s)

        logging.info(f"[arxiv] '{q}': done. total records saved: {total_saved}")

# ---------- CLI helper ----------

if __name__ == "__main__":
    # Minimal ad-hoc runner for testing without the orchestrator.
    import argparse
    parser = argparse.ArgumentParser(description="Harvest arXiv results.")
    parser.add_argument("--raw-dir", required=True, help="Path to data/raw")
    parser.add_argument("--query", action="append", required=True, help="arXiv search_query (repeatable)")
    parser.add_argument("--max", type=int, default=500, help="Max results per query")
    parser.add_argument("--no-pdf", action="store_true", help="Do not attempt to download PDFs")
    parser.add_argument("--sleep", type=float, default=3.0, help="Seconds between API requests")
    args = parser.parse_args()

    cfg = {
        "paths": {"raw_dir": args.raw_dir},
        "arxiv": {
            "queries": args.query,
            "max_results_per_query": args.max,
            "pdf": not args.no_pdf,
            "sleep_between_requests_sec": args.sleep,
        },
    }
    harvest_arxiv(cfg)
