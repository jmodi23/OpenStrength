# zenodo.py
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

log = logging.getLogger("zenodo")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[ZENODO] %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

API_BASE = "https://zenodo.org/api/records"

# ---------- small utils ----------

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

def norm_license(rec: dict) -> Optional[str]:
    """
    Try to normalize a Zenodo license identifier to lowercase (e.g., 'cc-by', 'cc0').
    Zenodo records commonly store license info at:
      rec['metadata']['license']['id']  (preferred)
    Fall back to rec['metadata']['license'] if it's a string.
    """
    try:
        lic = rec.get("metadata", {}).get("license", None)
        if isinstance(lic, dict):
            lid = lic.get("id") or lic.get("identifier") or ""
            return lid.strip().lower() or None
        if isinstance(lic, str):
            return lic.strip().lower() or None
    except Exception:
        pass
    return None

def allowed_by_license(rec: dict, whitelist: List[str]) -> bool:
    if not whitelist:
        return True
    lic = norm_license(rec) or ""
    lic = lic.lower()
    wl = [x.lower() for x in whitelist]
    # accept exact matches or common variations with hyphen/underscore
    if lic in wl:
        return True
    lic_norm = lic.replace("_", "-")
    return lic_norm in wl

def record_id(rec: dict) -> str:
    # Prefer persistent conceptrecid? We’ll store by 'id' (numeric string)
    rid = rec.get("id")
    return str(rid) if rid is not None else rec.get("doi", "unknown").replace("/", "_")

def iter_records(session: requests.Session, query: str, size: int, pages: int, rate_per_sec: float) -> Iterable[dict]:
    """
    Yield raw record JSONs for a query across pages.
    """
    for page in range(1, pages + 1):
        params = {
            "q": query,
            "size": max(1, min(size, 1000)),  # zenodo caps at 1000
            "page": page,
            # Consider restricting to bestversion or latest? Leaving default for completeness.
        }
        rate_sleep(rate_per_sec)
        try:
            r = session.get(API_BASE, params=params, timeout=60)
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            log.warning(f"query page failed (page={page}): {e}")
            break

        hits = payload if isinstance(payload, list) else payload.get("hits", {}).get("hits", [])
        if not hits:
            break
        for rec in hits:
            # Some responses are wrapped under 'metadata', others are already flat under 'metadata' key.
            # Keep the full record as-is; caller extracts fields as needed.
            yield rec

        # If Zenodo returns fewer than requested, we likely exhausted results
        if len(hits) < params["size"]:
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

    out_root = Path(paths.get("raw_dir", "data/raw")) / "zenodo"
    ensure_dir(out_root)

    s = requests.Session()
    s.headers.update({"User-Agent": "OpenStrength-ZenodoHarvester/1.0"})

    total_records = 0
    total_saved_files = 0

    for q in queries:
        log.info(f"query: {q}")
        for rec in iter_records(s, q, page_size, pages, rate_per_sec):
            rid = record_id(rec)

            # license screen
            if not allowed_by_license(rec, license_whitelist):
                log.info(f"skip license record id={rid} lic={norm_license(rec)}")
                continue

            # record dir & metadata
            rec_dir = out_root / rid
            ensure_dir(rec_dir)
            meta_path = rec_dir / "record.metadata.json"
            # Write record JSON once (idempotent)
            if not meta_path.exists():
                safe_write_json(meta_path, rec)

            # files array lives at top-level 'files' (modern API) or under 'files' inside record
            files = rec.get("files") or []
            saved_this_rec = 0

            for f in files:
                # file metadata fields: key, size, links.download, checksum, id
                fname = f.get("key") or f.get("filename") or f.get("id") or "file"
                dl = None
                links = f.get("links") or {}
                dl = links.get("download") or links.get("self")

                if not dl:
                    # try legacy: sometimes file URLs are in 'links' of the record->files entry
                    continue

                out_file = rec_dir / fname
                if out_file.exists() and out_file.stat().st_size > 0:
                    # already downloaded
                    saved_this_rec += 1
                    continue

                blob = dl_file(s, dl, rate_per_sec)
                if not blob:
                    continue

                safe_write_bytes(out_file, blob)
                # per-file sidecar metadata
                safe_write_json(out_file.with_suffix(out_file.suffix + ".metadata.json"), {
                    "record_id": rid,
                    "filename": fname,
                    "size": f.get("size"),
                    "checksum": f.get("checksum"),
                    "download": dl,
                })
                saved_this_rec += 1
                total_saved_files += 1

            total_records += 1
            log.info(f"record {rid}: files_saved={saved_this_rec}")

    log.info(f"done. records_seen={total_records} files_saved={total_saved_files}")