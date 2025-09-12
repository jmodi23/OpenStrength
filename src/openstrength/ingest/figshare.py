# figshare.py
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import List, Optional

import requests

log = logging.getLogger("figshare")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[FIGSHARE] %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

API_BASE = "https://api.figshare.com/v2/articles"

# ---------- utils ----------

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def safe_write_bytes(path: Path, data: bytes) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
    tmp.replace(path)

def safe_write_json(path: Path, obj: dict) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def rate_sleep(rate_per_sec: float) -> None:
    if rate_per_sec and rate_per_sec > 0:
        time.sleep(max(0.0, 1.0 / rate_per_sec))

def norm_license(article: dict) -> Optional[str]:
    """
    Normalize Figshare license info.
    Typical: article['license']['value'] = 'cc-by', 'cc0', etc.
    """
    try:
        lic = article.get("license")
        if isinstance(lic, dict):
            return (lic.get("value") or "").lower()
        if isinstance(lic, str):
            return lic.lower()
    except Exception:
        pass
    return None

def allowed_by_license(article: dict, whitelist: List[str]) -> bool:
    if not whitelist:
        return True
    lic = norm_license(article) or ""
    wl = [x.lower() for x in whitelist]
    if lic in wl:
        return True
    lic_norm = lic.replace("_", "-")
    return lic_norm in wl

def article_id(article: dict) -> str:
    return str(article.get("id") or article.get("doi") or "unknown").replace("/", "_")

def iter_articles(session: requests.Session, query: str, page_size: int, pages: int, rate_per_sec: float):
    for page in range(1, pages + 1):
        params = {"search_for": query, "page": page, "page_size": page_size}
        rate_sleep(rate_per_sec)
        try:
            r = session.get(API_BASE, params=params, timeout=60)
            r.raise_for_status()
            articles = r.json()
        except Exception as e:
            log.warning(f"query failed page={page}: {e}")
            break

        if not articles:
            break

        for art in articles:
            yield art

        if len(articles) < page_size:
            break

def dl_file(session: requests.Session, url: str, rate_per_sec: float) -> Optional[bytes]:
    rate_sleep(rate_per_sec)
    try:
        r = session.get(url, timeout=180, stream=True)
        r.raise_for_status()
        return r.content
    except Exception as e:
        log.warning(f"download failed: {url} :: {e}")
        return None

# ---------- main runner ----------

def run_from_config(cfg: dict, paths: dict, *_args, **_kwargs) -> None:
    if not cfg.get("enabled", False):
        log.info("disabled in config; skipping")
        return

    queries: List[str] = cfg.get("queries", [])
    page_size: int = int(cfg.get("page_size", 100))
    pages: int = int(cfg.get("pages", 20))
    rate_per_sec: float = float(cfg.get("rate_per_sec", 2))
    license_whitelist: List[str] = cfg.get("license_whitelist", [])

    out_root = Path(paths.get("raw_dir", "data/raw")) / "figshare"
    ensure_dir(out_root)

    s = requests.Session()
    s.headers.update({"User-Agent": "OpenStrength-FigshareHarvester/1.0"})

    total_articles = 0
    total_saved_files = 0

    for q in queries:
        log.info(f"query: {q}")
        for art in iter_articles(s, q, page_size, pages, rate_per_sec):
            aid = article_id(art)

            if not allowed_by_license(art, license_whitelist):
                log.info(f"skip license article id={aid} lic={norm_license(art)}")
                continue

            art_dir = out_root / aid
            ensure_dir(art_dir)
            meta_path = art_dir / "article.metadata.json"
            if not meta_path.exists():
                safe_write_json(meta_path, art)

            # detail fetch to get files
            rate_sleep(rate_per_sec)
            try:
                r = s.get(f"{API_BASE}/{aid}", timeout=60)
                r.raise_for_status()
                detail = r.json()
            except Exception as e:
                log.warning(f"detail fetch failed id={aid}: {e}")
                continue

            files = detail.get("files") or []
            saved_this = 0
            for f in files:
                fname = f.get("name") or f.get("id") or "file"
                url = f.get("download_url")
                if not url:
                    continue

                out_file = art_dir / fname
                if out_file.exists() and out_file.stat().st_size > 0:
                    saved_this += 1
                    continue

                blob = dl_file(s, url, rate_per_sec)
                if not blob:
                    continue

                safe_write_bytes(out_file, blob)
                safe_write_json(out_file.with_suffix(out_file.suffix + ".metadata.json"), {
                    "article_id": aid,
                    "filename": fname,
                    "size": f.get("size"),
                    "download": url,
                })
                saved_this += 1
                total_saved_files += 1

            total_articles += 1
            log.info(f"article {aid}: files_saved={saved_this}")

    log.info(f"done. articles_seen={total_articles} files_saved={total_saved_files}")