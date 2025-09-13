# src/openstrength/ingest/pmc.py
from __future__ import annotations

import os
import re
import json
import time
import errno
import hashlib
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm
except Exception:
    # very small shim if tqdm isn't available
    def tqdm(it, **kw):
        return it


# ---------------------------
# Helpers
# ---------------------------

PMC_BASE = "https://www.ncbi.nlm.nih.gov/pmc/articles"
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

def _mkdir_p(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def _rate_sleep(rate_per_sec: float) -> None:
    if rate_per_sec and rate_per_sec > 0:
        time.sleep(max(0.0, 1.0 / float(rate_per_sec)))

def _mk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "OpenStrength/ingest (PMCID harvester)",
        "Accept": "*/*",
    })
    s.timeout = 30
    return s

def _choose_parser() -> str:
    # Try robust fallbacks; works even without lxml installed.
    for p in ("lxml-xml", "xml", "lxml", "html.parser"):
        try:
            BeautifulSoup("<x/>", p)  # quick sanity check
            return p
        except Exception:
            continue
    return "html.parser"

XML_PARSER = _choose_parser()

# ---------------------------
# E-utilities
# ---------------------------

def esearch_pmc_ids(term: str, email: str, api_key: str, retmax: int, rate: float, sess: requests.Session) -> List[str]:
    """
    Return a list of PMCID strings (like 'PMC1234567') for a search term.
    """
    params = {
        "db": "pmc",
        "term": term,
        "retmax": retmax,
        "retmode": "json",
        "email": email,
    }
    if api_key:
        params["api_key"] = api_key

    url = f"{EUTILS}/esearch.fcgi"
    _rate_sleep(rate)
    r = sess.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    ids = data.get("esearchresult", {}).get("idlist", []) or []
    # Convert numeric ids to PMCID format for convenience
    pmcids = [f"PMC{_id}" if not str(_id).startswith("PMC") else str(_id) for _id in ids]
    return pmcids


def efetch_pmc_xml(pmcid: str, email: str, api_key: str, rate: float, sess: requests.Session) -> str:
    """
    Fetch the JATS XML for a PMCID. Returns text (XML).
    """
    numeric = pmcid.replace("PMC", "")
    params = {
        "db": "pmc",
        "id": numeric,
        "retmode": "xml",
        "email": email,
    }
    if api_key:
        params["api_key"] = api_key

    url = f"{EUTILS}/efetch.fcgi"
    _rate_sleep(rate)
    r = sess.get(url, params=params)
    r.raise_for_status()
    return r.text


# ---------------------------
# License parsing
# ---------------------------

_CC_PAT = re.compile(r"creativecommons\.org/licenses/([a-z\-]+)/([0-9.]+)/?", re.I)
_CC_ZERO_PAT = re.compile(r"creativecommons\.org/publicdomain/zero/([0-9.]+)/?", re.I)
_PD_PAT = re.compile(r"public\s*domain|pd\b|us-?gov|work\s*of\s*the\s*us\s*government", re.I)

def _norm_cc_tag(kind: str, ver: str) -> str:
    kind = kind.lower()
    ver = ver.strip()
    mapping = {
        "by": "CC-BY",
        "by-sa": "CC-BY-SA",
        "by-nd": "CC-BY-ND",
        "by-nc": "CC-BY-NC",
        "by-nc-sa": "CC-BY-NC-SA",
        "by-nc-nd": "CC-BY-NC-ND",
    }
    base = mapping.get(kind, f"CC-{kind.upper()}")
    return f"{base}-{ver}"

def normalize_license(raw: str) -> Optional[str]:
    """
    Map raw license strings/URLs to normalized tags consistent with sources.yaml.
    """
    if not raw:
        return None
    txt = raw.strip()

    # CC BY URLs
    m = _CC_PAT.search(txt)
    if m:
        return _norm_cc_tag(m.group(1), m.group(2))

    # CC0 URL
    m = _CC_ZERO_PAT.search(txt)
    if m:
        return f"CC0-{m.group(1)}"

    # Plain text variants
    t = txt.lower()
    # Common variants
    if "cc by 4.0" in t or "cc-by 4.0" in t or "cc-by-4.0" in t or "attribution 4.0" in t:
        return "CC-BY-4.0"
    if "cc by 3.0" in t or "cc-by-3.0" in t or "attribution 3.0" in t:
        return "CC-BY-3.0"
    if "cc by-sa 4.0" in t or "cc-by-sa-4.0" in t or "attribution-sharealike 4.0" in t:
        return "CC-BY-SA-4.0"
    if "cc0" in t or "public domain dedication" in t:
        return "CC0-1.0"

    # US Gov / Public Domain
    if _PD_PAT.search(t):
        # Try to be specific when obvious
        if "us" in t and "gov" in t:
            return "US-Gov-PD"
        return "Public-Domain"

    return None


def parse_license_from_xml(xml_text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract a normalized license tag and the raw textual hint from JATS XML.
    Returns (normalized_tag, raw_text_or_url)
    """
    soup = BeautifulSoup(xml_text, XML_PARSER)

    # 1) <license> or ali:license or <license-p>
    lic_nodes = []
    lic_nodes += soup.find_all(["license", "license-p"])
    lic_nodes += soup.find_all(attrs={"license-type": True})
    # any element with rel="license" or xlink:href to cc
    lic_nodes += soup.find_all(lambda tag: tag.name == "ext-link" and (tag.get("ext-link-type") == "uri" or tag.get("rel") == "license"))
    lic_nodes += soup.find_all(lambda tag: any(k.endswith("href") and "creativecommons.org" in (tag.get(k) or "") for k in tag.attrs.keys()))

    # 2) Check common attributes/urls
    candidates: List[str] = []
    for node in lic_nodes:
        # href/xlink:href
        for k, v in node.attrs.items():
            if k.endswith("href") and isinstance(v, str):
                candidates.append(v)
        # text
        if node.string and isinstance(node.string, str):
            candidates.append(node.string)
        txt = node.get_text(" ", strip=True)
        if txt:
            candidates.append(txt)

    # 3) Extra: permissions block sometimes used
    for node in soup.find_all(["permissions", "copyright-statement", "copyright-year"]):
        txt = node.get_text(" ", strip=True)
        if txt:
            candidates.append(txt)

    # 4) As a last resort look for any link to CC in the whole XML
    for a in soup.find_all(["a", "ext-link"]):
        href = a.get("href") or a.get("xlink:href") or a.get("href")
        if href and "creativecommons.org" in href:
            candidates.append(href)

    # Decide
    for c in candidates:
        tag = normalize_license(c)
        if tag:
            return tag, c

    # If nothing found, return None + best raw hint if present
    return None, (candidates[0] if candidates else None)


# ---------------------------
# PDF URL discovery + download
# ---------------------------

def pdf_url_candidates(pmcid: str, xml_text: str) -> List[str]:
    """
    Build a list of candidate PDF URLs for a PMCID.
    """
    pmc_url = f"{PMC_BASE}/{pmcid}/"
    base_pdf = f"{PMC_BASE}/{pmcid}/pdf/"
    direct_pdf_named = f"{PMC_BASE}/{pmcid}/pdf/{pmcid}.pdf"
    query_pdf = f"{PMC_BASE}/{pmcid}/?pdf=1"

    cands = [direct_pdf_named, base_pdf, query_pdf]

    # Find self-uri in XML (sometimes points at the PDF)
    soup = BeautifulSoup(xml_text, XML_PARSER)
    for su in soup.find_all("self-uri"):
        href = su.get("href") or su.get("xlink:href")
        if href:
            if href.startswith("http"):
                cands.append(href)
            else:
                # relative
                cands.append(pmc_url + href.lstrip("/"))

    # Unique preserve order
    seen = set()
    out = []
    for u in cands:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def download_pdf(urls: Iterable[str], sess: requests.Session, rate: float, pmcid: str) -> Optional[bytes]:
    """
    Try each URL until a PDF is returned.
    """
    for url in urls:
        try:
            _rate_sleep(rate)
            r = sess.get(url, allow_redirects=True, stream=True)
            if r.status_code == 404:
                print(f"[PMC] pdf-get failed pmcid={pmcid} status=404")
                continue
            r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "pdf" not in ctype:
                # Some PMC links respond with HTML; surface that for debugging
                print(f"[PMC] self-uri failed pmcid={pmcid} url={url} ctype={ctype or 'UNKNOWN'}")
                continue
            return r.content
        except requests.HTTPError as e:
            print(f"[PMC] pdf-get failed pmcid={pmcid} status={getattr(e.response, 'status_code', '??')}")
        except Exception as e:
            print(f"[PMC] pdf-get failed pmcid={pmcid} err={e!r}")
    return None


# ---------------------------
# Main harvester
# ---------------------------

def harvest_pmc(
    out_root: str,
    queries: List[str],
    email: str,
    api_key: str,
    retmax: int,
    rate_per_sec: float,
    allowed_licenses: List[str],
    permit_unlicensed_readonly: bool = False,
) -> Dict[str, int]:
    """
    Run PMC harvest across queries. Returns summary counters.
    """
    sess = _mk_session()
    total_ids = 0
    saved = 0
    skipped_license = 0
    pdf_fail = 0
    xml_fail = 0

    for term in queries:
        try:
            ids = esearch_pmc_ids(term, email=email, api_key=api_key, retmax=retmax, rate=rate_per_sec, sess=sess)
        except Exception as e:
            print(f"[PMC] esearch failed term={term!r} err={e}")
            continue

        print(f"[PMC] term={term!r} -> {len(ids)} ids")
        total_ids += len(ids)

        # Per-query output dir
        safe_term = re.sub(r"[^a-zA-Z0-9._-]+", "_", term)[:100].strip("_")
        q_dir = os.path.join(out_root, "pmc", safe_term)
        _mkdir_p(q_dir)

        # Iterate ids
        for pmcid in tqdm(ids, desc=f"PMC fetch: {term}", unit="doc"):
            try:
                xml = efetch_pmc_xml(pmcid, email=email, api_key=api_key, rate=rate_per_sec, sess=sess)
            except requests.HTTPError as e:
                print(f"[PMC] ERROR pmcid={pmcid} -> {e}")
                xml_fail += 1
                continue
            except Exception as e:
                print(f"[PMC] ERROR pmcid={pmcid} -> {e}")
                xml_fail += 1
                continue

            # License parsing
            norm_tag, raw_license = parse_license_from_xml(xml)

            # Decide keep or skip
            keep = False
            reason = ""
            if norm_tag and norm_tag in set(allowed_licenses):
                keep = True
            elif permit_unlicensed_readonly and (norm_tag is None):
                keep = True
                reason = " (permit_unlicensed_readonly)"
            else:
                print(f"[PMC] skip license={norm_tag!r} pmcid={pmcid}")
                skipped_license += 1
                continue

            # PDF discovery + download
            urls = pdf_url_candidates(pmcid, xml)
            pdf_bytes = download_pdf(urls, sess=sess, rate=rate_per_sec, pmcid=pmcid)
            if not pdf_bytes:
                pdf_fail += 1
                continue

            # Write files
            meta = build_metadata_record(pmcid, xml, norm_tag, raw_license, term)
            base = os.path.join(q_dir, pmcid)
            with open(base + ".json", "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            with open(base + ".pdf", "wb") as f:
                f.write(pdf_bytes)

            saved += 1

    return {
        "total_ids": total_ids,
        "saved": saved,
        "skipped_license": skipped_license,
        "xml_fail": xml_fail,
        "pdf_fail": pdf_fail,
    }


def build_metadata_record(pmcid: str, xml_text: str, norm_license: Optional[str], raw_license: Optional[str], term: str) -> Dict:
    """
    A small, resilient metadata extraction using JATS. We keep it minimal to
    avoid strict schema dependencies.
    """
    soup = BeautifulSoup(xml_text, XML_PARSER)

    def _first_text(names: List[str]) -> Optional[str]:
        for n in names:
            el = soup.find(n)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt:
                    return txt
        return None

    title = _first_text(["article-title", "title"])
    journal = _first_text(["journal-title"]) or _first_text(["journal-title-group"])
    year = _first_text(["year"])
    abstract = _first_text(["abstract"])

    return {
        "source": "pmc",
        "pmcid": pmcid,
        "search_term": term,
        "title": title,
        "journal": journal,
        "year": year,
        "abstract": abstract,
        "license": norm_license,
        "license_raw": raw_license,
        "record_hash": hashlib.sha256(xml_text.encode("utf-8")).hexdigest(),
    }


# ---------------------------
# Entry point for run.py
# ---------------------------

def run_from_config(*args, **kwargs):
    """
    Flexible entrypoint so different runners can call:
      - run_from_config()
      - run_from_config(cfg)
      - run_from_config(cfg, out_root)
      - run_from_config(cfg=..., out_root=...)
    """
    # Parse inputs robustly
    cfg = None
    out_root = None

    if args:
        if len(args) == 1 and isinstance(args[0], dict):
            cfg = args[0]
        elif len(args) >= 2 and isinstance(args[0], dict):
            cfg, out_root = args[0], args[1]

    # keyword fallbacks
    if cfg is None:
        # allow callers to pass just a dict in kwargs too
        if "cfg" in kwargs and isinstance(kwargs["cfg"], dict):
            cfg = kwargs["cfg"]
        else:
            # if someone accidentally passed the whole config as kwargs,
            # accept that (last resort)
            cfg = kwargs if kwargs else {}

    if out_root is None:
        out_root = kwargs.get("out_root")

    # default out_root from config if still missing
    if not out_root:
        out_root = (cfg.get("paths", {}) or {}).get("raw_dir", "data/raw")

    section = (cfg or {}).get("pmc", {}) or {}
    if not section.get("enabled", False):
        print("[PMC] disabled in config; skipping")
        return {"status": "skipped"}

    queries = section.get("queries", [])
    email = section.get("email", "")
    api_key = section.get("api_key", "")
    rate = float(section.get("rate_per_sec", 1))
    retmax = int(section.get("max_results_per_query", 2000))

    allowed_licenses = (cfg.get("licenses", {}) or {}).get("allow", []) or []
    permit_unlicensed_readonly = bool(section.get("permit_unlicensed_readonly", False))

    out = harvest_pmc(
        out_root=out_root,
        queries=queries,
        email=email,
        api_key=api_key,
        retmax=retmax,
        rate_per_sec=rate,
        allowed_licenses=allowed_licenses,
        permit_unlicensed_readonly=permit_unlicensed_readonly,
    )
    print(f"[ok]   pmc -> {json.dumps(out)}")
    return out

def harvest_pmc(cfg: dict) -> None:
    return run_from_config(cfg.get("pmc") or {}, cfg.get("paths") or {})

