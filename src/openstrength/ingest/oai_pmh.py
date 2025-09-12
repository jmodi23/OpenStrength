# oai_pmh.py
from __future__ import annotations

import json
import html
import logging
import os
import re
import string
import time
import urllib.parse as urlparse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

# ---------------- logging ----------------
log = logging.getLogger("oai_pmh")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[OAI-PMH] %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

# ---------------- helpers ----------------
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

def same_host(url: str) -> str:
    try:
        return urlparse.urlparse(url).netloc or "unknown-host"
    except Exception:
        return "unknown-host"

# ---------------- license logic ----------------
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
    return None

def is_license_allowed(dc_rights_texts: List[str], whitelist: Optional[List[str]], global_allow: Optional[Iterable[str]]) -> bool:
    if whitelist is None and global_allow is None:
        return True
    allow = set(x.lower() for x in (whitelist or []))
    allow |= set(x.lower() for x in (global_allow or []))
    if not allow:
        return True
    for t in dc_rights_texts:
        tag = norm_license(t)
        if tag and tag in allow:
            return True
    return False

# ---------------- OAI core ----------------
NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/",
}

def oai_list_records(
    endpoint: str,
    metadata_prefix: str,
    date_from: Optional[str] = None,
    date_until: Optional[str] = None,
    session: Optional[requests.Session] = None,
    rate_per_sec: float = 1.0,
):
    """
    Yield (header_elem, metadata_elem) for ListRecords (handles resumptionToken).
    """
    s = session or requests.Session()
    params: Dict[str, str] = {"verb": "ListRecords", "metadataPrefix": metadata_prefix}
    if date_from:
        params["from"] = date_from
    if date_until:
        params["until"] = date_until

    token = None
    while True:
        try:
            q = {"verb": "ListRecords", "resumptionToken": token} if token else dict(params)
            rate_sleep(rate_per_sec)
            r = s.get(endpoint, params=q, timeout=60)
            r.raise_for_status()
            text = r.text
            try:
                root = ET.fromstring(text)
            except ET.ParseError:
                root = ET.fromstring(html.unescape(text))

            # OAI-level errors
            err = root.find(".//oai:error", NS)
            if err is not None:
                code = err.attrib.get("code", "unknown")
                msg = (err.text or "").strip()
                log.warning(f"{endpoint}: OAI error code={code} msg={msg}")
                return

            for rec in root.findall(".//oai:record", NS):
                header = rec.find("./oai:header", NS)
                metadata = rec.find("./oai:metadata", NS)
                if header is None or header.attrib.get("status") == "deleted":
                    continue
                yield header, metadata

            rt = root.find(".//oai:resumptionToken", NS)
            token = (rt.text or "").strip() if rt is not None else None
            if not token:
                break
        except requests.HTTPError as e:
            log.error(f"{endpoint}: HTTP {e}")
            break
        except Exception as e:
            log.error(f"{endpoint}: {e}")
            break

def extract_dc(metadata_elem: Optional[ET.Element]) -> Dict[str, List[str]]:
    fields = ["title","creator","subject","description","publisher","contributor","date","type","format","identifier","source","language","relation","coverage","rights"]
    out: Dict[str, List[str]] = {k: [] for k in fields}
    if metadata_elem is None:
        return out
    for tag in fields:
        for el in metadata_elem.findall(f".//dc:{tag}", NS):
            txt = (el.text or "").strip()
            if txt:
                out[tag].append(txt)
    return out

def any_query_match(queries: List[str], haystack_fields: List[str]) -> bool:
    if not queries:
        return True
    hay = " \n ".join(haystack_fields).lower()
    for q in queries:
        q = q.strip().lower()
        if not q:
            continue
        if q.startswith('"') and q.endswith('"') and len(q) > 1:
            if q[1:-1].strip() in hay:
                return True
        else:
            if q in hay:
                return True
    return False

# ---------------- PDF retrieval helpers ----------------
HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', flags=re.IGNORECASE)

def find_pdf_links_in_html(html_text: str, base_url: str) -> List[str]:
    urls = []
    for href in HREF_RE.findall(html_text or ""):
        u = urlparse.urljoin(base_url, href)
        if u.lower().endswith(".pdf") or "bitstream" in u.lower() or "download" in u.lower():
            urls.append(u)
    # keep order, drop dups
    seen, dedup = set(), []
    for u in urls:
        if u not in seen:
            dedup.append(u); seen.add(u)
    return dedup

def try_download_pdf(url: str, session: requests.Session, rate_per_sec: float) -> Optional[bytes]:
    try:
        rate_sleep(rate_per_sec)
        r = session.get(url, timeout=60, allow_redirects=True)
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "").split(";")[0].strip().lower()
        # Direct PDF?
        if ctype == "application/pdf" or url.lower().endswith(".pdf"):
            return r.content
        # Landing page → try discover links (DSpace/Harvard DASH style)
        if "text/html" in ctype or ctype == "text/html":
            for pdf in find_pdf_links_in_html(r.text, r.url):
                try:
                    rate_sleep(rate_per_sec)
                    r2 = session.get(pdf, timeout=60, allow_redirects=True)
                    r2.raise_for_status()
                    c2 = r2.headers.get("Content-Type", "").split(";")[0].strip().lower()
                    if c2 == "application/pdf" or pdf.lower().endswith(".pdf"):
                        return r2.content
                except Exception:
                    continue
    except Exception:
        return None
    return None

def choose_pdf_identifiers(identifiers: List[str]) -> List[str]:
    pdfs = [i for i in identifiers if i.lower().endswith(".pdf")]
    bitstreams = [i for i in identifiers if ("bitstream" in i.lower() or "download" in i.lower()) and i.startswith("http")]
    http_ids = [i for i in identifiers if i.startswith("http")]
    seen, out = set(), []
    for lst in (pdfs, bitstreams, http_ids):
        for u in lst:
            if u not in seen:
                out.append(u); seen.add(u)
    return out

# ---------------- main entry ----------------
def run_from_config(
    cfg: dict,
    paths: dict,
    global_license_allow: Optional[Iterable[str]] = None,
) -> None:
    """
    Called by run.py

    cfg: sources['oai_pmh']
    paths: sources['paths']
    """
    if not cfg.get("enabled", False):
        log.info("disabled in config; skipping")
        return

    endpoints: List[str] = cfg.get("endpoints", [])
    if not endpoints:
        log.warning("No OAI-PMH endpoints configured; skipping")
        return

    metadata_prefix: str = cfg.get("metadata_prefix", "oai_dc")
    queries: List[str] = cfg.get("queries", [])
    date_from: Optional[str] = cfg.get("from")
    date_until: Optional[str] = cfg.get("to")
    rate_per_sec: float = float(cfg.get("rate_per_sec", 1))
    whitelist: Optional[List[str]] = cfg.get("license_whitelist")

    out_root = Path(paths.get("raw_dir", "data/raw")) / "oai_pmh"
    ensure_dir(out_root)

    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "OpenStrength-OAIHarvester/1.0 (+https://example.org)",
        "Accept": "application/xml, text/xml;q=0.9, /;q=0.8",
    })

    total_seen = 0
    total_saved = 0
    qlist = [q.strip() for q in queries]

    for ep in endpoints:
        host = same_host(ep)
        log.info(f"Harvesting endpoint={ep} (host={host}) from={date_from} until={date_until} prefix={metadata_prefix}")

        for header, metadata in oai_list_records(
            endpoint=ep,
            metadata_prefix=metadata_prefix,
            date_from=date_from,
            date_until=date_until,
            session=sess,
            rate_per_sec=rate_per_sec,
        ):
            total_seen += 1

            oai_identifier = header.findtext("./oai:identifier", default="", namespaces=NS).strip()
            datestamp = header.findtext("./oai:datestamp", default="", namespaces=NS).strip()

            dc = extract_dc(metadata)
            fields_for_match: List[str] = []
            for k in ("title", "description", "subject", "identifier"):
                fields_for_match.extend(dc.get(k, []))

            # license check
            rights_texts = dc.get("rights", [])
            if not is_license_allowed(rights_texts, whitelist, global_license_allow):
                continue

            # match per query (write to each matching query bucket)
            match_targets = fields_for_match or [""]  # avoid false negatives
            matched = False
            for q in (qlist or [""]):  # if no queries, accept all
                if q and not any_query_match([q], match_targets):
                    continue
                matched = True

                rec_slug = sanitize(oai_identifier or datestamp or "record")
                out_dir = out_root / sanitize(q or "all") / host / rec_slug
                ensure_dir(out_dir)

                meta = {
                    "endpoint": ep,
                    "host": host,
                    "oai_identifier": oai_identifier,
                    "datestamp": datestamp,
                    "license_normalized": [norm_license(t) for t in rights_texts if t],
                    "dc": dc,
                }
                write_json(out_dir / "metadata.json", meta)

                # attempt to fetch a PDF
                identifiers = dc.get("identifier", [])
                for url in choose_pdf_identifiers(identifiers):
                    pdf_bytes = try_download_pdf(url, sess, rate_per_sec)
                    if pdf_bytes:
                        name = sanitize(Path(urlparse.urlparse(url).path).name) or "document.pdf"
                        if not name.lower().endswith(".pdf"):
                            name += ".pdf"
                        write_bytes(out_dir / name, pdf_bytes)
                        break  # one PDF is enough

                total_saved += 1

            if total_seen % 500 == 0:
                log.info(f"{host}: seen={total_seen} saved={total_saved}")

        log.info(f"Done endpoint={host}: seen={total_seen} saved={total_saved}")

    log.info(f"All endpoints complete. Total seen={total_seen}, saved={total_saved}")