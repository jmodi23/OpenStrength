# src/openstrength/ingest/pmc.py
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
HEADERS = {"User-Agent": "OpenStrength/0.1 (contact: mjainil1201@gmail.com)"}


# ---------------------------
# Helpers
# ---------------------------

def _sleep(rate_per_sec: float) -> None:
    if rate_per_sec and rate_per_sec > 0:
        time.sleep(1.0 / rate_per_sec)


def _session() -> requests.Session:
    """HTTP session with retries/backoff for resilience."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s


def _sanitize_dirname(name: str, maxlen: int = 48) -> str:
    """Windows-safe, relatively short dirname (avoid MAX_PATH issues)."""
    name = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return name[:maxlen] if len(name) > maxlen else name


def safe_write(path: Path, content: bytes | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        path.write_text(content, encoding="utf-8", errors="ignore")
    else:
        path.write_bytes(content)


# ---------------------------
# E-utilities
# ---------------------------

def esearch_pmc(
    term: str,
    start: str,
    end: str,
    email: str,
    api_key: Optional[str],
    rate: float,
    retmax: int = 100000,
    sess: Optional[requests.Session] = None,
) -> List[str]:
    """Search PMC and return a list of PMCID (numeric) strings."""
    s = sess or _session()
    params = {
        "db": "pmc",
        "term": term,
        "retmax": retmax,
        "retmode": "json",
        "email": email,
    }
    if api_key:
        params["api_key"] = api_key
    url = EUTILS + "esearch.fcgi?" + urlencode(params)
    _sleep(rate)
    r = s.get(url, timeout=30)
    r.raise_for_status()
    js = r.json()
    ids = js.get("esearchresult", {}).get("idlist", [])
    print(f"[PMC] term='{term}' -> {len(ids)} ids")
    return ids


def efetch_pmc_xml(
    pmcid: str,
    email: str,
    api_key: Optional[str],
    rate: float,
    sess: Optional[requests.Session] = None,
) -> str:
    """Fetch PMC JATS/XML for a PMCID."""
    s = sess or _session()
    params = {"db": "pmc", "id": pmcid, "retmode": "xml", "email": email}
    if api_key:
        params["api_key"] = api_key
    url = EUTILS + "efetch.fcgi?" + urlencode(params)
    _sleep(rate)
    r = s.get(url, timeout=60)
    r.raise_for_status()
    return r.text


def pmcid_to_pdf_url(pmcid: str) -> str:
    """The common 'pdf' landing path (may return HTML)."""
    return f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmcid}/pdf"


def resolve_pdf_url_from_html(html: str, base: str) -> Optional[str]:
    """When /pdf returns an HTML page, scrape for the real PDF link(s)."""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf") or "/pdf/" in href.lower():
            return requests.compat.urljoin(base, href)
    return None


# ---------------------------
# License detection
# ---------------------------

_CC_RE = re.compile(
    r"(https?://)?creativecommons\.org/"
    r"(?:(?:licenses/(?P<lic>by(?:-sa)?))/|publicdomain/(?P<pd>zero))/"
    r"(?P<ver>\d(?:\.\d)?)",
    re.IGNORECASE,
)


def parse_license_from_xml(xml_text: str) -> Tuple[Optional[str], Optional[str]]:
    """Detect permissive licenses in PMC JATS/XML."""
    soup = BeautifulSoup(xml_text, "lxml-xml")

    cand_nodes = []
    cand_nodes += soup.find_all(["license", "ali:license_ref", "license-ref", "license-p"])
    cand_nodes += soup.find_all("ext-link", attrs={"ext-link-type": "license"})
    perms = soup.find("permissions")
    if perms:
        cand_nodes.append(perms)

    texts: list[str] = []
    for node in cand_nodes:
        if not node:
            continue
        href = getattr(node, "get", lambda *_: None)("xlink:href")
        if href:
            texts.append(href)
        texts.append(node.get_text(" ", strip=True))

    blob = " ".join(texts)
    m = _CC_RE.search(blob) or _CC_RE.search(xml_text)

    norm: Optional[str] = None
    url: Optional[str] = None
    if m:
        url = f"https://creativecommons.org/{'licenses/' + m.group('lic') if m.group('lic') else 'publicdomain/zero'}/{m.group('ver')}/"
        lic = m.group("lic") or "zero"
        ver = m.group("ver") or "1.0"
        if lic == "by":
            norm = f"CC-BY-{ver}"
        elif lic == "by-sa":
            norm = f"CC-BY-SA-{ver}"
        elif lic == "zero":
            norm = f"CC0-{ver}"
    else:
        lower_xml = xml_text.lower()
        if "public domain" in lower_xml and ("us" in lower_xml or "government" in lower_xml):
            norm = "US-Gov-PD"

    return norm, (url or (texts[0] if texts else None))


# ---------------------------
# Main harvest
# ---------------------------

def harvest_pmc(cfg: dict) -> None:
    """Harvest PMC XML + (when licensed) PDFs."""
    pmc = cfg["pmc"]
    allowed = {a.lower() for a in cfg["licenses"]["allow"]}

    raw_dir = Path(cfg["paths"]["raw_dir"]).resolve() / "pmc"
    email = pmc["email"]
    api_key = pmc.get("api_key") or None
    rate = float(pmc.get("rate_per_sec", 3))

    sess = _session()

    for q in pmc["queries"]:
        ids = esearch_pmc(q, cfg["time_window"]["start"], cfg["time_window"]["end"],
                          email, api_key, rate, retmax=pmc["max_results_per_query"], sess=sess)
        out_dir = raw_dir / _sanitize_dirname(q)
        out_dir.mkdir(parents=True, exist_ok=True)

        pbar = tqdm(ids, desc=f"PMC fetch: {q}", unit="doc")
        for pmcid in pbar:
            meta_path = out_dir / f"PMC{pmcid}.meta.json"
            xml_path = out_dir / f"PMC{pmcid}.xml"
            pdf_path = out_dir / f"PMC{pmcid}.pdf"
            try:
                # Fetch XML
                xml = efetch_pmc_xml(pmcid, email, api_key, rate, sess=sess)
                norm_tag, raw_license = parse_license_from_xml(xml)
                meta = {
                    "pmcid": pmcid,
                    "license": norm_tag or "UNKNOWN",
                    "license_raw": raw_license,
                    "query": q,
                }

                safe_write(xml_path, xml)
                safe_write(meta_path, json.dumps(meta, ensure_ascii=False, indent=2))

                norm_lower = (norm_tag or "").lower()
                is_allowed_tag = norm_lower in allowed
                is_cc_url = bool(raw_license and "creativecommons.org" in str(raw_license).lower())

                if is_allowed_tag or is_cc_url:
                    # Try standard /pdf endpoint
                    pdf_url = pmcid_to_pdf_url(pmcid)
                    _sleep(rate)
                    r = sess.get(pdf_url, timeout=60, allow_redirects=True)
                    ctype = (r.headers.get("Content-Type") or "").lower()

                    if r.ok and "pdf" in ctype:
                        safe_write(pdf_path, r.content)
                    elif r.ok and "html" in ctype:
                        # Look for <self-uri> in the XML
                        soup = BeautifulSoup(xml, "lxml-xml")
                        self_uri = soup.find("self-uri", attrs={"content-type": "pmc-pdf"})
                        if self_uri and self_uri.has_attr("xlink:href"):
                            real = requests.compat.urljoin(pdf_url, self_uri["xlink:href"])
                            rr = sess.get(real, timeout=60, allow_redirects=True)
                            ctype2 = (rr.headers.get("Content-Type") or "").lower()
                            if rr.ok and "pdf" in ctype2:
                                safe_write(pdf_path, rr.content)
                            else:
                                print(f"[PMC] self-uri failed pmcid=PMC{pmcid} url={real} ctype={ctype2}")
                        else:
                            # Try scraping landing HTML for PDF links
                            real = resolve_pdf_url_from_html(r.text, pdf_url)
                            if real:
                                rr = sess.get(real, timeout=60, allow_redirects=True)
                                ctype2 = (rr.headers.get("Content-Type") or "").lower()
                                if rr.ok and "pdf" in ctype2:
                                    safe_write(pdf_path, rr.content)
                                else:
                                    print(f"[PMC] html-follow failed pmcid=PMC{pmcid} url={real} ctype={ctype2}")
                            else:
                                print(f"[PMC] no-pdf-link pmcid=PMC{pmcid} page={pdf_url}")
                    else:
                        print(f"[PMC] pdf-get failed pmcid=PMC{pmcid} status={r.status_code}")
                else:
                    print(f"[PMC] skip license='{norm_tag}' pmcid=PMC{pmcid}")

                pbar.set_postfix_str(meta["license"] or "UNKNOWN")

            except Exception as e:
                try:
                    safe_write(out_dir / f"PMC{pmcid}.fail.txt", f"{type(e).__name__}: {e}")
                except Exception:
                    pass
                import traceback
                traceback.print_exc()
                print(f"[PMC] ERROR pmcid=PMC{pmcid} → {e}")
                continue
