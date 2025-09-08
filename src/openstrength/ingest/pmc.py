from __future__ import annotations
import json, re, time
from pathlib import Path
from typing import Iterable, List, Dict, Optional
from urllib.parse import urlencode
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
HEADERS = {"User-Agent": "OpenStrength/0.1 (contact: youremail@example.com)"}

def _sleep(rate_per_sec: float):
    if rate_per_sec > 0:
        time.sleep(1.0 / rate_per_sec)

def esearch_pmc(term: str, start: str, end: str, email: str, api_key: str | None, rate: float, retmax=100000) -> List[str]:
    params = {
        "db": "pmc",
        "term": f"({term}) AND ({start}:{end}[pdat])",
        "retmax": retmax,
        "retmode": "json",
        "email": email,
    }
    if api_key:
        params["api_key"] = api_key
    url = EUTILS + "esearch.fcgi?" + urlencode(params)
    _sleep(rate)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    js = r.json()
    return js.get("esearchresult", {}).get("idlist", [])

def efetch_pmc_xml(pmcid: str, email: str, api_key: str | None, rate: float) -> str:
    params = {"db": "pmc", "id": pmcid, "retmode": "xml", "email": email}
    if api_key:
        params["api_key"] = api_key
    url = EUTILS + "efetch.fcgi?" + urlencode(params)
    _sleep(rate)
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text

def pmcid_to_pdf_url(pmcid: str) -> Optional[str]:
    # Standard PMC PDF URL pattern
    return f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmcid}/pdf"

def parse_license_from_xml(xml_text: str) -> tuple[Optional[str], Optional[str]]:
    # Extract <license> or xlink:href with Creative Commons URL
    soup = BeautifulSoup(xml_text, "lxml-xml")
    lic = soup.find(["license", "ali:license_ref"])
    if lic:
        txt = lic.get_text(strip=True)
        href = getattr(lic, "get", lambda *_: None)("xlink:href")
        full = href or txt
        norm = None
        if full:
            low = full.lower()
            if "cc-by-sa" in low: norm = "CC-BY-SA-4.0"
            elif "cc-by" in low:  norm = "CC-BY-4.0"
            elif "cc0" in low:    norm = "CC0-1.0"
        return norm, full
    return None, None

def safe_write(path: Path, content: bytes | str):
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        path.write_text(content, encoding="utf-8", errors="ignore")
    else:
        path.write_bytes(content)

def harvest_pmc(cfg: dict) -> None:
    pmc = cfg["pmc"]
    allowed = set(cfg["licenses"]["allow"])
    raw_dir = Path(cfg["paths"]["raw_dir"]) / "pmc"
    start, end = cfg["time_window"]["start"], cfg["time_window"]["end"]
    email, api_key, rate = pmc["email"], pmc.get("api_key") or None, float(pmc.get("rate_per_sec", 3))

    for q in pmc["queries"]:
        ids = esearch_pmc(q, start, end, email, api_key, rate, retmax=pmc["max_results_per_query"])
        out_dir = raw_dir / re.sub(r"[^a-z0-9]+", "_", q.lower())[:60]
        out_dir.mkdir(parents=True, exist_ok=True)

        pbar = tqdm(ids, desc=f"PMC fetch: {q}", unit="doc")
        for pmcid in pbar:
            try:
                xml = efetch_pmc_xml(pmcid, email, api_key, rate)
                norm_tag, raw_license = parse_license_from_xml(xml)
                meta = {"pmcid": pmcid, "license": norm_tag or "UNKNOWN", "license_raw": raw_license, "query": q}
                meta_path = out_dir / f"PMC{pmcid}.meta.json"
                xml_path = out_dir / f"PMC{pmcid}.xml"
                pdf_path = out_dir / f"PMC{pmcid}.pdf"

                # Save XML and meta regardless; enforce license later in pipeline if you prefer.
                safe_write(xml_path, xml)
                safe_write(meta_path, json.dumps(meta, ensure_ascii=False, indent=2))

                if norm_tag in allowed:
                    pdf_url = pmcid_to_pdf_url(pmcid)
                    _sleep(rate)
                    r = requests.get(pdf_url, headers=HEADERS, timeout=60)
                    if r.ok and r.headers.get("Content-Type", "").lower().startswith("application/pdf"):
                        safe_write(pdf_path, r.content)
                pbar.set_postfix_str(meta["license"])
            except Exception:
                continue
