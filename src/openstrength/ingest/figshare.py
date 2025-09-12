from __future__ import annotations
from pathlib import Path
import concurrent.futures as fut
import requests
from tqdm import tqdm
from .utils_net import mk_session, is_pdf_response, safe_write_bytes, safe_write_json, slugify

FG = "https://api.figshare.com/v2/articles/search"
FGA = "https://api.figshare.com/v2/articles/{}"

def harvest_figshare(cfg: dict) -> None:
    fc = cfg["figshare"]
    if not fc.get("enabled"):
        return
    out_root = Path(cfg["paths"]["raw_dir"]) / "figshare"
    out_root.mkdir(parents=True, exist_ok=True)
    allowed = {x.lower() for x in fc["license_whitelist"]}
    max_workers = cfg.get("parallelism", {}).get("max_workers", 8)

    with mk_session() as s:
        for q in fc["queries"]:
            qslug = slugify(q)
            out_dir = out_root / qslug
            out_dir.mkdir(parents=True, exist_ok=True)
            candidates = []
            for page in range(1, fc["pages"] + 1):
                params = {"search_for": q, "page_size": fc["page_size"], "page": page}
                r = s.get(FG, params=params, timeout=30)
                if r.status_code != 200:
                    break
                items = r.json()
                if not items:
                    break
                for it in items:
                    art_id = it.get("id")
                    if not art_id:
                        continue
                    # details
                    d = s.get(FGA.format(art_id), timeout=30)
                    if d.status_code != 200:
                        continue
                    det = d.json()
                    lic = (det.get("license", {}).get("name") or "").lower()
                    if allowed and all(x not in lic for x in allowed):
                        continue
                    files = det.get("files") or []
                    for f in files:
                        if f.get("name","").lower().endswith(".pdf"):
                            candidates.append((det.get("title",""), lic, f.get("download_url")))
            def fetch_one(t):
                title, lic, url = t
                try:
                    r = s.get(url, timeout=60, allow_redirects=True)
                    if not r.ok or not is_pdf_response(r):
                        return 0
                    fnbase = slugify(title or url)
                    safe_write_bytes(out_dir / f"{fnbase}.pdf", r.content)
                    safe_write_json(out_dir / f"{fnbase}.meta.json", {"title": title, "license": lic, "url": url, "query": q, "source": "figshare"})
                    return 1
                except Exception:
                    return 0
            with fut.ThreadPoolExecutor(max_workers=max_workers) as ex:
                for _ in tqdm(ex.map(fetch_one, candidates), total=len(candidates), desc=f"Figshare: {q}", unit="file"):
                    pass
