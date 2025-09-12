from __future__ import annotations
import concurrent.futures as fut
from pathlib import Path
from typing import List
import requests
from tqdm import tqdm
from .utils_net import mk_session, sleep_rate, is_pdf_response, safe_write_bytes, safe_write_json, slugify

DOAJ = "https://doaj.org/api/v2/search/articles/"

def _page(session: requests.Session, q: str, page: int, page_size: int) -> dict:
    url = DOAJ + q
    params = {"page": page, "pageSize": page_size}
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def harvest_doaj(cfg: dict) -> None:
    dj = cfg["doaj"]
    if not dj.get("enabled"):
        return
    allowed = {x.lower() for x in dj["license_whitelist"]}
    out_root = Path(cfg["paths"]["raw_dir"]) / "doaj"
    out_root.mkdir(parents=True, exist_ok=True)
    max_workers = cfg.get("parallelism", {}).get("max_workers", 8)

    with mk_session() as s:
        for q in dj["queries"]:
            qslug = slugify(q)
            out_dir = out_root / qslug
            out_dir.mkdir(parents=True, exist_ok=True)
            total = 0
            for page in range(1, dj["pages"] + 1):
                js = _page(s, q, page, dj["page_size"])
                results = js.get("results", [])
                if not results:
                    break
                # collect candidate fulltext links
                candidates = []
                for res in results:
                    bib = res.get("bibjson", {})
                    license_arr = (bib.get("license") or [])
                    lic = (license_arr[0].get("type") if license_arr else "" ).lower()
                    if allowed and all(x not in lic for x in allowed):
                        continue
                    links = bib.get("link") or []
                    for L in links:
                        if (L.get("type") or "").lower() == "fulltext":
                            candidates.append((res.get("id"), bib.get("title",""), lic, L.get("url")))
                if not candidates:
                    continue

                def fetch_one(tup):
                    id_, title, lic, url = tup
                    try:
                        r = s.get(url, timeout=60, allow_redirects=True)
                        if not r.ok or not is_pdf_response(r):
                            return 0
                        fnbase = slugify((id_ or title) or url)
                        safe_write_bytes(out_dir / f"{fnbase}.pdf", r.content)
                        safe_write_json(out_dir / f"{fnbase}.meta.json", {
                            "id": id_, "title": title, "license": lic, "query": q, "url": url, "source": "doaj"
                        })
                        return 1
                    except Exception:
                        return 0

                with fut.ThreadPoolExecutor(max_workers=max_workers) as ex:
                    for got in tqdm(ex.map(fetch_one, candidates), total=len(candidates), desc=f"DOAJ: {q} p{page}", unit="file"):
                        total += got
