from __future__ import annotations
from pathlib import Path
import concurrent.futures as fut
import requests
from tqdm import tqdm
from .utils_net import mk_session, is_pdf_response, safe_write_bytes, safe_write_json, slugify

CORE_API = "https://core.ac.uk/api-v2/search/"

def harvest_core(cfg: dict) -> None:
    cc = cfg["core"]
    if not cc.get("enabled") or not cc.get("api_key"):
        return
    out_root = Path(cfg["paths"]["raw_dir"]) / "core"
    out_root.mkdir(parents=True, exist_ok=True)
    allowed = {x.lower() for x in cc["license_whitelist"]}
    max_workers = cfg.get("parallelism", {}).get("max_workers", 8)

    with mk_session() as s:
        for q in cc["queries"]:
            qslug = slugify(q)
            out_dir = out_root / qslug
            out_dir.mkdir(parents=True, exist_ok=True)
            candidates = []
            for page in range(1, cc["pages"] + 1):
                params = {"page": page, "pageSize": cc["page_size"], "apiKey": cc["api_key"]}
                r = s.get(CORE_API + q, params=params, timeout=30)
                if r.status_code != 200:
                    break
                js = r.json()
                hits = js.get("results", [])
                if not hits:
                    break
                for h in hits:
                    lic = (h.get("license") or "").lower()
                    if allowed and all(x not in lic for x in allowed):
                        continue
                    pdf = h.get("downloadUrl") or h.get("fullTextLink")
                    if pdf:
                        candidates.append((h.get("id"), h.get("title"), lic, pdf))
            def fetch_one(t):
                id_, title, lic, url = t
                try:
                    r = s.get(url, timeout=60, allow_redirects=True)
                    if not r.ok or not is_pdf_response(r):
                        return 0
                    fnbase = slugify((str(id_) or title) or url)
                    safe_write_bytes(out_dir / f"{fnbase}.pdf", r.content)
                    safe_write_json(out_dir / f"{fnbase}.meta.json", {"id": id_, "title": title, "license": lic, "url": url, "query": q, "source": "core"})
                    return 1
                except Exception:
                    return 0
            with fut.ThreadPoolExecutor(max_workers=max_workers) as ex:
                for _ in tqdm(ex.map(fetch_one, candidates), total=len(candidates), desc=f"CORE: {q}", unit="file"):
                    pass
