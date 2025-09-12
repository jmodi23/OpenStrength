from __future__ import annotations
import time, json, re
from pathlib import Path
from typing import Any, Optional
import requests
from requests.adapters import HTTPAdapter, Retry

PDF_CT = ("application/pdf", "application/x-pdf", "binary/octet-stream")
UA = {"User-Agent": "OpenStrength/0.1 (+github.com/jmodi23/OpenStrength)"}

def mk_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5,
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=frozenset(["GET", "HEAD"]))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update(UA)
    return s

def sleep_rate(rate: float):
    if rate and rate > 0:
        time.sleep(1.0 / rate)

def is_pdf_response(resp: requests.Response) -> bool:
    ct = (resp.headers.get("Content-Type") or "").lower()
    return any(x in ct for x in PDF_CT)

def safe_write_bytes(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)

def safe_write_json(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def slugify(s: str, maxlen: int = 80) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s[:maxlen]
