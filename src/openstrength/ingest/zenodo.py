from __future__ import annotations
from pathlib import Path
import concurrent.futures as fut
import requests
from tqdm import tqdm
from .utils_net import mk_session, is_pdf_response, safe_write_bytes, safe_write_json, slugify

Z = "https://zenodo.org/api/records"

def harvest_zenodo(cfg: dict) -> None:
    zc = cfg["zenodo"]
    if not zc.get("enabled"):
        return
    out_root = Path(cfg["paths"]["raw_dir"]) / "zenodo"
    out_root.mkdir(parents=True, exist_ok=True)
    allowed = {x.lower() for x in zc["license_whitelist"]}
    max_workers = cfg.get("parallelism", {}).get("max_workers", 8)

    with mk_session() as s:
        for q in zc["queries"]:
            qslug = slugify(q)
            out_dir = out_root / qslug
            out_dir.mkdir(parents=True, exist_ok=True)
            candidates = []
            for page in range(1, zc["pages"] + 1):
                params = {"q": q, "page": page, "size": zc["page_size"], "all_versions": "true", "type": "publication", "access_right": "open"}
                r = s.get(Z, params=params, timeout=30)
                if r.status_code != 200:
                    break
                js = r.json()
                hits = js.get("hits", {}).get("hits", [])
                if not hits:
                    break
                for h in hits:
                    lic = (h.get("metadata", {}).get("license", {}).get("id") or "").lower()
                    if allowed and lic and lic not in allowed:
                        continue
                    files = h.get("files") or []
                    for f in files:
                        if (f.get("type") or "").lower() == "pdf" or f.get("key","").lower().endswith(".pdf"):
                            candidates.append((h.get("doi") or h.get("conceptdoi") or "", h.get("metadata", {}).get("title",""), lic, f.get("links",{}).get("self")))
            def fetch_one(t):
                doi, title, lic, url = t
                try:
                    r = s.get(url, timeout=60, allow_redirects=True)
                    if not r.ok or not is_pdf_response(r):
                        return 0
                    fnbase = slugify((doi or title) or url)
                    safe_write_bytes(out_dir / f"{fnbase}.pdf", r.content)
                    safe_write_json(out_dir / f"{fnbase}.meta.json", {"doi": doi, "title": title, "license": lic, "url": url, "query": q, "source": "zenodo"})
                    return 1
                except Exception:
                    return 0
            with fut.ThreadPoolExecutor(max_workers=max_workers) as ex:
                for _ in tqdm(ex.map(fetch_one, candidates), total=len(candidates), desc=f"Zenodo: {q}", unit="file"):
                    pass
