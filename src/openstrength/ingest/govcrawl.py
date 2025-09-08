from __future__ import annotations
import time, re
from urllib.parse import urljoin, urlparse
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

HEADERS = {"User-Agent": "OpenStrength/0.1"}

def _sleep(rate: float): 
    if rate > 0: time.sleep(1.0 / rate)

def harvest_gov(cfg: dict) -> None:
    g = cfg["gov"]
    raw_dir = Path(cfg["paths"]["raw_dir"]) / "gov"
    raw_dir.mkdir(parents=True, exist_ok=True)
    allow = set(cfg["gov"]["allow_domains"])
    exts = set(cfg["gov"]["filetypes"])
    rate = float(cfg["gov"]["rate_per_sec"])

    visited = set()
    for seed in g["seeds"]:
        domain = urlparse(seed).netloc
        if domain not in allow: 
            continue
        out_dir = raw_dir / re.sub(r"[^a-z0-9]+", "_", domain.lower())
        out_dir.mkdir(parents=True, exist_ok=True)
        to_visit = [seed]
        with tqdm(total=0, desc=f"crawl {domain}", unit="file") as pbar:
            while to_visit:
                url = to_visit.pop()
                if url in visited: 
                    continue
                visited.add(url)
                try:
                    _sleep(rate)
                    r = requests.get(url, headers=HEADERS, timeout=20)
                    if r.status_code != 200 or "text/html" not in r.headers.get("Content-Type","").lower():
                        continue
                    soup = BeautifulSoup(r.text, "html.parser")
                    for a in soup.find_all("a", href=True):
                        href = urljoin(url, a["href"])
                        d = urlparse(href).netloc
                        if d != domain: 
                            continue
                        if any(href.lower().endswith(ext) for ext in exts):
                            # fetch file
                            _sleep(rate)
                            fr = requests.get(href, headers=HEADERS, timeout=60)
                            if fr.ok:
                                fn = re.sub(r"[^a-z0-9]+", "_", href.split("/")[-1].lower())
                                path = out_dir / fn
                                path.write_bytes(fr.content)
                                pbar.total += 1; pbar.update(1)
                        elif href.startswith("http"):
                            to_visit.append(href)
                except Exception:
                    continue
