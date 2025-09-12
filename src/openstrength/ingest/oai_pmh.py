from __future__ import annotations
import xml.etree.ElementTree as ET
from urllib.parse import urlencode
from pathlib import Path
import concurrent.futures as fut
from tqdm import tqdm
import requests
from .utils_net import mk_session, sleep_rate, is_pdf_response, safe_write_bytes, safe_write_json, slugify

def _list_records(session: requests.Session, base_url: str, metadata_prefix: str, frm: str, to: str):
    # simple ListRecords + resumptionToken loop
    params = {"verb": "ListRecords", "metadataPrefix": metadata_prefix, "from": frm, "until": to}
    while True:
        r = session.get(base_url, params=params, timeout=30)
        if r.status_code != 200:
            break
        root = ET.fromstring(r.content)
        ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        for rec in root.findall(".//oai:record", ns):
            yield rec
        token_el = root.find(".//oai:resumptionToken", ns)
        token = token_el.text if token_el is not None else None
        if not token:
            break
        params = {"verb": "ListRecords", "resumptionToken": token}

def _extract_pdf_links(record: ET.Element, q_terms: list[str]) -> tuple[str, list[str], dict]:
    # works for oai_dc; for others extend as needed
    md = record.find(".//{http://www.openarchives.org/OAI/2.0/oai_dc/}dc")
    if md is None:
        return "", [], {}
    ns = "{http://purl.org/dc/elements/1.1/}"
    title = "".join([(e.text or "") for e in md.findall(f"{ns}title")])
    descr = " ".join([(e.text or "") for e in md.findall(f"{ns}description")])
    text = f"{title} {descr}".lower()
    if q_terms and not any(t in text for t in q_terms):
        return "", [], {}
    identifiers = [e.text or "" for e in md.findall(f"{ns}identifier")]
    links = [u for u in identifiers if u.lower().endswith(".pdf")]
    # capture license if present
    rights = " ".join([(e.text or "") for e in md.findall(f"{ns}rights")]).lower()
    return title, links, {"rights": rights}

def harvest_oai_pmh(cfg: dict) -> None:
    ocfg = cfg["oai_pmh"]
    if not ocfg.get("enabled"):
        return
    allowed = {x.lower() for x in ocfg["license_whitelist"]}
    out_root = Path(cfg["paths"]["raw_dir"]) / "oai_pmh"
    out_root.mkdir(parents=True, exist_ok=True)
    max_workers = cfg.get("parallelism", {}).get("max_workers", 8)
    q_terms = [t.lower() for t in ocfg["queries"]]

    with mk_session() as s:
        for base in ocfg["endpoints"]:
            dslug = slugify(base)
            out_dir = out_root / dslug
            out_dir.mkdir(parents=True, exist_ok=True)
            candidates = []
            for rec in _list_records(s, base, ocfg["metadata_prefix"], ocfg["from"], ocfg["to"]):
                title, pdfs, extra = _extract_pdf_links(rec, q_terms)
                if not pdfs:
                    continue
                rights = extra.get("rights","")
                if allowed and rights and all(a not in rights for a in allowed):
                    continue
                for url in pdfs:
                    candidates.append((title, rights, url))
            def fetch_one(t):
                title, rights, url = t
                try:
                    r = s.get(url, timeout=60, allow_redirects=True)
                    if not r.ok or not is_pdf_response(r):
                        return 0
                    fnbase = slugify((title or url))
                    safe_write_bytes(out_dir / f"{fnbase}.pdf", r.content)
                    safe_write_json(out_dir / f"{fnbase}.meta.json", {"title": title, "rights": rights, "url": url, "endpoint": base, "source": "oai_pmh"})
                    return 1
                except Exception:
                    return 0
            with fut.ThreadPoolExecutor(max_workers=max_workers) as ex:
                for _ in tqdm(ex.map(fetch_one, candidates), total=len(candidates), desc=f"OAI-PMH: {base}", unit="file"):
                    pass
