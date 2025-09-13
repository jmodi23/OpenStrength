# doaj.py
from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

log = logging.getLogger("doaj")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[DOAJ] %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

# ----------------- utils -----------------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def safe_write_json(path: Path, obj: dict) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def safe_write_bytes(path: Path, data: bytes) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
    tmp.replace(path)

def rate_sleep(rate_per_sec: float) -> None:
    if rate_per_sec and rate_per_sec > 0:
        time.sleep(max(0.0, 1.0 / rate_per_sec))

def norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def has_pdf_ext(url: str) -> bool:
    return bool(re.search(r"\.pdf(\?|$)", url, flags=re.IGNORECASE))

# ----------------- DOAJ client -----------------

API_PATTERNS = [
    "https://doaj.org/api/v3/search/articles/{q}?page={page}&pageSize={ps}",
    "https://doaj.org/api/v2/search/articles/{q}?page={page}&pageSize={ps}",
    "https://doaj.org/api/search/articles/{q}?page={page}&pageSize={ps}",
]

def try_fetch_page(session: requests.Session, q: str, page: int, page_size: int, rate_per_sec: float) -> Optional[dict]:
    """
    Try multiple API versions until one responds. Returns parsed JSON or None.
    """
    for pat in API_PATTERNS:
        url = pat.format(q=q, page=page, ps=page_size)
        rate_sleep(rate_per_sec)
        try:
            r = session.get(url, timeout=60)
            r.raise_for_status()
            js = r.json()
            # quick sanity check: must contain results-like list
            if isinstance(js, dict) and any(k in js for k in ("results", "data")):
                return js
        except Exception as e:
            log.debug(f"endpoint miss: {url} :: {e}")
    return None

def extract_results(payload: dict) -> List[dict]:
    """
    Normalize result list across DOAJ API shapes.
    Typically payload['results'] is a list of objects with key 'bibjson'.
    """
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("results"), list):
        return payload["results"]
    if isinstance(payload.get("data"), list):
        return payload["data"]
    # Sometimes payload itself may be a list
    if isinstance(payload, list):
        return payload
    return []

def get_bibjson(rec: dict) -> dict:
    # v2/v3: item.get('bibjson'), sometimes under 'bibjson' directly
    if isinstance(rec, dict):
        bj = rec.get("bibjson")
        return bj if isinstance(bj, dict) else rec
    return {}

def pick_id(rec: dict) -> str:
    # prefer DOAJ id; fallback to DOI; otherwise a hash-like from title/year
    rid = str(rec.get("id") or rec.get("_id") or "").strip()
    if rid:
        return rid.replace("/", "_")
    bj = get_bibjson(rec)
    doi = ""
    for idobj in bj.get("identifier", []) or []:
        if str(idobj.get("type")).lower() == "doi":
            doi = idobj.get("id") or idobj.get("canonical") or ""
            break
    if doi:
        return doi.replace("/", "_")
    title = (bj.get("title") or "untitled").strip().replace("/", "_")[:60]
    year = str((bj.get("year") or bj.get("journal", {}).get("publisher") or "na"))
    return f"{title}_{year}"

def article_license(bj: dict) -> Optional[str]:
    """
    DOAJ licenses: bj['license'] is often a list of dicts with 'type' and/or 'url'.
    Common 'type' examples: 'cc-by', 'cc-by-sa', 'cc0'.
    """
    lic = bj.get("license")
    if isinstance(lic, list) and lic:
        # prefer an explicit 'type'
        for l in lic:
            t = norm(l.get("type"))
            if t:
                return t
        # fallback: derive from URL
        for l in lic:
            url = norm(l.get("url"))
            if "creativecommons.org/licenses/by/" in url:
                return "cc-by"
            if "creativecommons.org/publicdomain/zero" in url:
                return "cc0"
            if "creativecommons.org/licenses/by-sa/" in url:
                return "cc-by-sa"
    if isinstance(lic, dict):
        t = norm(lic.get("type"))
        if t:
            return t
    return None

def allowed_by_license(bj: dict, whitelist: List[str]) -> bool:
    if not whitelist:
        return True
    lic = article_license(bj) or ""
    wl = [norm(x) for x in whitelist]
    if lic in wl:
        return True
    # minor normalization
    lic2 = lic.replace("_", "-")
    return lic2 in wl

def extract_links_for_pdf(bj: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (pdf_url, html_url). DOAJ 'bibjson.link' entries contain URLs with 'type' and 'content_type'.
    """
    links = bj.get("link") or []
    pdf_url = None
    html_url = None
    for ln in links:
        url = ln.get("url") or ""
        ctype = norm(ln.get("content_type"))
        ltype = norm(ln.get("type"))
        # Prefer explicit PDF
        if ctype in ("application/pdf", "pdf") or has_pdf_ext(url) or ltype == "fulltext_pdf":
            pdf_url = url
        # Keep an HTML fulltext if present
        if ltype in ("fulltext", "landing page", "homepage") or ctype in ("text/html", "html"):
            html_url = url
    return pdf_url, html_url

def download(session: requests.Session, url: str, rate_per_sec: float) -> Optional[bytes]:
    if not url:
        return None
    rate_sleep(rate_per_sec)
    try:
        r = session.get(url, timeout=180, stream=True)
        r.raise_for_status()
        return r.content
    except Exception as e:
        log.info(f"download failed: {url} :: {e}")
        return None

# ----------------- main -----------------

def run_from_config(cfg: Dict, paths: Dict, *_args, **_kwargs) -> None:
    if not cfg.get("enabled", False):
        log.info("disabled in config; skipping")
        return

    queries: List[str] = cfg.get("queries", [])
    page_size: int = int(cfg.get("page_size", 100))
    pages: int = int(cfg.get("pages", 30))
    rate_per_sec: float = float(cfg.get("rate_per_sec", 2))
    license_whitelist: List[str] = cfg.get("license_whitelist", [])

    raw_root = Path(paths.get("raw_dir", "data/raw")) / "doaj"
    ensure_dir(raw_root)

    s = requests.Session()
    s.headers.update({"User-Agent": "OpenStrength-DOAJHarvester/1.0"})

    total_seen = 0
    total_saved_pdf = 0

    for q in queries:
        log.info(f"query: {q}")
        for page in range(1, pages + 1):
            payload = try_fetch_page(s, q, page, page_size, rate_per_sec)
            if not payload:
                log.info(f"no payload returned (q={q}, page={page}); stopping this query")
                break

            results = extract_results(payload)
            if not results:
                log.info(f"no results (q={q}, page={page}); stopping this query")
                break

            for rec in results:
                bj = get_bibjson(rec)
                aid = pick_id(rec)

                if not allowed_by_license(bj, license_whitelist):
                    log.info(f"skip license id={aid} lic={article_license(bj)}")
                    continue

                art_dir = raw_root / aid
                ensure_dir(art_dir)

                # Save raw record once
                meta_path = art_dir / "article.metadata.json"
                if not meta_path.exists():
                    safe_write_json(meta_path, rec)

                # Try to fetch a PDF (if any)
                pdf_url, html_url = extract_links_for_pdf(bj)
                saved_this = 0

                if pdf_url:
                    pdf_path = art_dir / "fulltext.pdf"
                    if not pdf_path.exists():
                        blob = download(s, pdf_url, rate_per_sec)
                        if blob:
                            safe_write_bytes(pdf_path, blob)
                            saved_this += 1
                            total_saved_pdf += 1
                            safe_write_json(pdf_path.with_suffix(".pdf.metadata.json"), {
                                "download_url": pdf_url,
                                "source": "DOAJ",
                                "id": aid,
                            })

                # Save a pointer to an HTML fulltext if useful
                if html_url:
                    safe_write_json(art_dir / "fulltext.link.json", {
                        "url": html_url,
                        "note": "Likely landing page or HTML full text"
                    })

                total_seen += 1
                log.info(f"id={aid}: pdf_saved={saved_this} lic={article_license(bj)}")

            # Heuristic stop if fewer than a full page returned
            res_len = len(results)
            if res_len < page_size:
                log.info(f"short page ({res_len} < {page_size}); end of results for this query")
                break

    log.info(f"done. articles_seen={total_seen} pdfs_saved={total_saved_pdf}")

def harvest_doaj(cfg: dict) -> None:
    return run_from_config(cfg, cfg.get("paths") or {}) if "run_from_config" in globals() else harvest_doaj(cfg)
