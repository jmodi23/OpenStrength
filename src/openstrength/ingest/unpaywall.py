# unpaywall.py
from __future__ import annotations

import json
import logging
import os
import re
import string
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import urllib.parse as urlparse

import requests

log = logging.getLogger("unpaywall")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[Unpaywall] %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

SAFE_CHARS = f"-_.() {string.ascii_letters}{string.digits}"

def sanitize(s: str, maxlen: int = 120) -> str:
    if not s:
        return "na"
    s = "".join(c for c in s if c in SAFE_CHARS).strip()
    s = re.sub(r"\s+", "_", s)
    return s[:maxlen] or "na"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_json(path: Path, obj: dict) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def write_bytes(path: Path, data: bytes) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
    tmp.replace(path)

def rate_sleep(rate_per_sec: float) -> None:
    if rate_per_sec and rate_per_sec > 0:
        time.sleep(max(0.0, 1.0 / rate_per_sec))

# ---- license normalization & checks ----
LICENSE_PATTERNS = [
    (r"cc[-\s]?by[-\s]?(\d\.\d)?", "cc-by"),
    (r"cc[-\s]?by[-\s]?sa[-\s]?(\d\.\d)?", "cc-by-sa"),
    (r"cc0|publicdomainzero|pdm", "cc0"),
    (r"public\s*domain|us-gov|pd", "public-domain"),
]

def norm_license(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    t = text.strip().lower()
    for pat, tag in LICENSE_PATTERNS:
        if re.search(pat, t):
            return tag
    if "creativecommons.org/licenses/by" in t:
        return "cc-by"
    if "creativecommons.org/licenses/by-sa" in t:
        return "cc-by-sa"
    if "creativecommons.org/publicdomain/zero" in t:
        return "cc0"
    if t in {"cc-by", "cc-by-sa", "cc0", "public-domain"}:
        return t
    return None

def license_allowed(tags: List[str], whitelist: Optional[List[str]], global_allow: Optional[Iterable[str]]) -> bool:
    if whitelist is None and global_allow is None:
        return True
    allow = set(x.lower() for x in (whitelist or []))
    allow |= set(x.lower() for x in (global_allow or []))
    if not allow:
        return True
    for t in tags:
        lt = norm_license(t)
        if lt and lt in allow:
            return True
    return False

# ---- Crossref search → DOI list ----
def crossref_iter_dois(
    query: str,
    date_from: Optional[str],
    date_until: Optional[str],
    rows_per_page: int,
    rate_per_sec: float,
    session: requests.Session,
):
    """
    Yield DOIs for a Crossref query using query.bibliographic,
    filtered by from-pub-date / until-pub-date. Paginates with 'cursor'.
    """
    base = "https://api.crossref.org/works"
    cursor = "*"
    params_common = {
        "query.bibliographic": query,
        "filter": ",".join(
            f for f in [
                f"from-pub-date:{date_from}" if date_from else None,
                f"until-pub-date:{date_until}" if date_until else None,
                "type:journal-article",
            ] if f
        ),
        "rows": rows_per_page,
        "cursor": cursor,
        "select": "DOI,title,license,issued,type",
        "mailto": "openstrength@example.org",
    }

    total = 0
    while True:
        try:
            rate_sleep(rate_per_sec)
            r = session.get(base, params=params_common, timeout=60)
            r.raise_for_status()
            js = r.json()
        except Exception as e:
            log.error(f"Crossref query failed for '{query}': {e}")
            return

        items = js.get("message", {}).get("items", []) or []
        if not items:
            if total == 0:
                log.info(f"Crossref returned no items for query='{query}'")
            return

        for it in items:
            doi = (it.get("DOI") or "").strip()
            if doi:
                total += 1
                yield doi

        nxt = js.get("message", {}).get("next-cursor")
        if not nxt or nxt == params_common["cursor"]:
            break
        params_common["cursor"] = nxt

# ---- Unpaywall lookup ----
def unpaywall_lookup(
    doi: str,
    email: str,
    session: requests.Session,
    rate_per_sec: float,
) -> Optional[dict]:
    try:
        rate_sleep(rate_per_sec)
        url = f"https://api.unpaywall.org/v2/{urlparse.quote(doi)}"
        r = session.get(url, params={"email": email}, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Unpaywall lookup failed doi={doi}: {e}")
        return None

def best_pdf_url(u: dict) -> Tuple[Optional[str], List[str]]:
    """
    Return (pdf_url, license_tags) from unpaywall record.
    """
    if not u:
        return None, []
    # prefer best_oa_location then first oa_location with pdf
    def collect_tags(loc: Optional[dict]) -> List[str]:
        tags = []
        if not loc:
            return tags
        lic = loc.get("license") or ""
        if lic:
            tags.append(lic)
        # also try host_type-specific normalized guesses
        if loc.get("url_for_pdf", ""):
            if lic:
                tags.append(lic)
        return tags

    loc = u.get("best_oa_location") or {}
    pdf = loc.get("url_for_pdf") or None
    tags = collect_tags(loc)

    if not pdf:
        for loc in (u.get("oa_locations") or []):
            if loc.get("url_for_pdf"):
                pdf = loc.get("url_for_pdf")
                tags = collect_tags(loc)
                break

    # If still nothing, consider url (might be HTML; we don't HTML-scrape here)
    return pdf, tags

# ---- main entry ----
def run_from_config(
    cfg: dict,
    paths: dict,
    global_license_allow: Optional[Iterable[str]] = None,
) -> None:
    if not cfg.get("enabled", False):
        log.info("disabled in config; skipping")
        return

    email = (cfg.get("email") or "").strip()
    if not email or email == "you@example.com":
        log.error("Unpaywall requires a real contact email in sources.yaml under unpaywall.email")
        return

    queries: List[str] = cfg.get("queries", [])
    date_from: Optional[str] = cfg.get("from")
    date_until: Optional[str] = cfg.get("to")
    rows: int = int(cfg.get("rows_per_page", 100))
    rate_per_sec: float = float(cfg.get("rate_per_sec", 3))
    whitelist: Optional[List[str]] = cfg.get("license_whitelist")

    out_root = Path(paths.get("raw_dir", "data/raw")) / "unpaywall"
    ensure_dir(out_root)

    s = requests.Session()
    s.headers.update({"User-Agent": "OpenStrength-UnpaywallHarvester/1.0"})

    total_dois = 0
    total_saved = 0

    for q in queries:
        qslug = sanitize(q or "all")
        qdir = out_root / qslug
        ensure_dir(qdir)
        log.info(f"Crossref → Unpaywall for query='{q}' from={date_from} to={date_until}")

        seen: set[str] = set()
        for doi in crossref_iter_dois(q, date_from, date_until, rows, rate_per_sec, s):
            if doi in seen:
                continue
            seen.add(doi)
            total_dois += 1

            urec = unpaywall_lookup(doi, email, s, rate_per_sec)
            if not urec:
                continue

            pdf_url, lic_tags = best_pdf_url(urec)
            # merge with any top-level license fields if present
            all_licenses = list(lic_tags)
            # license_allowed will normalize; just include raw strings
            if not license_allowed(all_licenses, whitelist, global_license_allow):
                continue

            # write metadata
            slug = sanitize(doi.replace("/", "_"))
            out_dir = qdir / slug
            ensure_dir(out_dir)

            meta = {
                "query": q,
                "doi": doi,
                "unpaywall": urec,
                "chosen_pdf_url": pdf_url,
                "licenses_detected": all_licenses,
            }
            write_json(out_dir / "metadata.json", meta)

            # download PDF if available
            if pdf_url:
                try:
                    rate_sleep(rate_per_sec)
                    r = s.get(pdf_url, timeout=90, allow_redirects=True)
                    r.raise_for_status()
                    ctype = r.headers.get("Content-Type", "").lower()
                    if "pdf" in ctype or pdf_url.lower().endswith(".pdf"):
                        write_bytes(out_dir / "paper.pdf", r.content)
                except Exception as e:
                    log.warning(f"PDF fetch failed doi={doi} url={pdf_url}: {e}")

            total_saved += 1

        log.info(f"Query done '{q}': DOIs={len(seen)} saved={total_saved}")

    log.info(f"Unpaywall complete. Total DOIs processed={total_dois}, saved={total_saved}")