from __future__ import annotations
import time, re
from pathlib import Path
from typing import List
import requests
from tqdm import tqdm
from xml.etree import ElementTree as ET

ARXIV_API = "http://export.arxiv.org/api/query"
HEADERS = {"User-Agent": "OpenStrength/0.1"}

def _sleep(rate: float): 
    if rate > 0: time.sleep(1.0 / rate)

def search_arxiv(query: str, categories: List[str], max_results: int, rate: float):
    start = 0
    step = 200
    while start < max_results:
        q = f"({query}) AND cat:{' OR cat:'.join(categories)}"
        params = {"search_query": q, "start": start, "max_results": min(step, max_results - start), "sortBy": "submittedDate", "sortOrder": "descending"}
        _sleep(rate)
        r = requests.get(ARXIV_API, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        feed = ET.fromstring(r.text.encode("utf-8"))
        entries = feed.findall("{http://www.w3.org/2005/Atom}entry")
        if not entries: break
        yield entries
        start += step

def get_pdf_url(entry) -> str | None:
    for link in entry.findall("{http://www.w3.org/2005/Atom}link"):
        if link.attrib.get("title") == "pdf":
            return link.attrib["href"]
    return None

def harvest_arxiv(cfg: dict) -> None:
    a = cfg["arxiv"]
    raw_dir = Path(cfg["paths"]["raw_dir"]) / "arxiv"
    raw_dir.mkdir(parents=True, exist_ok=True)
    for q in a["queries"]:
        out_dir = raw_dir / re.sub(r"[^a-z0-9]+", "_", q.lower())[:60]
        out_dir.mkdir(parents=True, exist_ok=True)
        total = 0
        for page in search_arxiv(q, a["categories"], a["max_results_per_query"], float(a["rate_per_sec"])):
            for entry in tqdm(page, desc=f"arXiv: {q}", unit="doc"):
                id_tag = entry.find("{http://www.w3.org/2005/Atom}id")
                title = entry.find("{http://www.w3.org/2005/Atom}title").text.strip() if entry.find("{http://www.w3.org/2005/Atom}title") is not None else ""
                pdf = get_pdf_url(entry)
                if not pdf or not id_tag: continue
                aid = id_tag.text.split("/")[-1]
                pdf_path = out_dir / f"{aid}.pdf"
                meta_path = out_dir / f"{aid}.meta.json"
                try:
                    r = requests.get(pdf, headers=HEADERS, timeout=60)
                    if r.ok and r.headers.get("Content-Type", "").lower().startswith("application/pdf"):
                        pdf_path.write_bytes(r.content)
                        meta_path.write_text(json_dump({"arxiv_id": aid, "title": title, "license": "CC-BY-4.0", "query": q}), encoding="utf-8")
                        total += 1
                except Exception:
                    continue

def json_dump(d: dict) -> str:
    import json
    return json.dumps(d, ensure_ascii=False, indent=2)
