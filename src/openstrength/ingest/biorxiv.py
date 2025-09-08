from __future__ import annotations
import time, re, json
from pathlib import Path
from typing import List
import requests
from tqdm import tqdm

API = "https://api.biorxiv.org/details/"
HEADERS = {"User-Agent": "OpenStrength/0.1"}

def _sleep(rate: float): 
    if rate > 0: time.sleep(1.0 / rate)

def fetch_range(server: str, query: str, start_date: str, end_date: str, rate: float):
    cursor = 0
    while True:
        url = f"{API}{server}/{start_date}/{end_date}/{cursor}"
        _sleep(rate)
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200: break
        js = r.json()
        collection = js.get("collection", [])
        if not collection: break
        for item in collection:
            # naive query filter
            title_abs = (item.get("title","") + " " + item.get("abstract","")).lower()
            if any(tok in title_abs for tok in query.lower().split()):
                yield item
        cursor += len(collection)
        if cursor >= js.get("messages", [{}])[-1].get("count", cursor): break

def harvest_biorxiv(cfg: dict) -> None:
    bx = cfg["biorxiv"]
    raw_dir = Path(cfg["paths"]["raw_dir"]) / "biorxiv"
    raw_dir.mkdir(parents=True, exist_ok=True)
    start, end = cfg["time_window"]["start"], cfg["time_window"]["end"]
    allowed = set(bx.get("license_whitelist", []))
    for q in bx["queries"]:
        out_dir = raw_dir / re.sub(r"[^a-z0-9]+", "_", q.lower())[:60]
        out_dir.mkdir(parents=True, exist_ok=True)
        for server in ["biorxiv", "medrxiv"]:
            items = list(fetch_range(server, q, start, end, float(bx["rate_per_sec"])))
            for it in tqdm(items, desc=f"{server}: {q}", unit="doc"):
                lic = (it.get("license") or "").lower()
                if allowed and lic not in allowed: 
                    continue
                # PDF link sometimes in 'links' or construct from DOI
                pdf_url = it.get("pdf_url") or it.get("link_pdf") or None
                meta_path = out_dir / f"{server}_{it['doi'].replace('/','_')}.meta.json"
                safe_write(meta_path, json.dumps({"server": server, "doi": it["doi"], "title": it.get("title"), "license": lic, "query": q}, ensure_ascii=False, indent=2))
                if pdf_url:
                    try:
                        r = requests.get(pdf_url, headers=HEADERS, timeout=60)
                        if r.ok and r.headers.get("Content-Type","").lower().startswith("application/pdf"):
                            (out_dir / f"{server}_{it['doi'].replace('/','_')}.pdf").write_bytes(r.content)
                    except Exception:
                        continue

def safe_write(path: Path, txt: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(txt, encoding="utf-8")
