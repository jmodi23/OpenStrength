# govcrawl.py
from __future__ import annotations

import collections
import json
import logging
import os
import re
import string
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, urldefrag

import requests

log = logging.getLogger("govcrawl")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[GOV] %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

SAFE_CHARS = f"-_.() {string.ascii_letters}{string.digits}"

def sanitize(s: str, maxlen: int = 120) -> str:
    if not s:
        return "na"
    s = "".join(c for c in s if c in SAFE_CHARS).strip()
    s = re.sub(r"\s+", "_", s)
    return s[:maxlen] or "na"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_json(path: Path, obj: dict) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def write_bytes(path: Path, data: bytes) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
    tmp.replace(path)

def rate_sleep(rate_per_sec: float) -> None:
    if rate_per_sec and rate_per_sec > 0:
        time.sleep(max(0.0, 1.0 / rate_per_sec))

# --- URL helpers -------------------------------------------------------------

HTML_LINK_RE = re.compile(
    r"""(?isx)
    <a[^>]+href\s*=\s*      # anchor with href=
    (?P<q>["'])             # opening quote
    (?P<href>[^"'>#\s]+)    # the URL itself (stop at quote, >, #)
    (?P=q)                  # matching closing quote
    """
)

def strip_fragment(u: str) -> str:
    # Remove fragment and return URL
    return urldefrag(u)[0]

def norm_url(u: str) -> str:
    # Normalize to help dedupe:
    #  - strip fragment
    #  - lower-case scheme/host
    #  - remove default ports
    u = strip_fragment(u)
    p = urlparse(u)
    netloc = p.hostname or ""
    if p.port:
        # drop default ports
        if (p.scheme == "http" and p.port == 80) or (p.scheme == "https" and p.port == 443):
            netloc = p.hostname or ""
        else:
            netloc = f"{p.hostname}:{p.port}" if p.hostname else (p.netloc or "")
    scheme = (p.scheme or "https").lower()
    netloc = netloc.lower()
    return urlunparse((scheme, netloc, p.path or "/", p.params, p.query, ""))

def url_domain(u: str) -> str:
    return (urlparse(u).hostname or "").lower()

def is_allowed(u: str, allow_domains: Set[str]) -> bool:
    host = url_domain(u)
    if not host:
        return False
    return any(host == d or host.endswith("." + d) for d in allow_domains)

def should_fetch(u: str, allow_exts: Set[str]) -> bool:
    path = urlparse(u).path or "/"
    ext = Path(path).suffix.lower()
    # If extension is empty (e.g., section page), treat as HTML if .html is allowed
    if not ext:
        return ".html" in allow_exts
    return ext in allow_exts

def guess_ext_from_ctype(ctype: str) -> str:
    ctype = (ctype or "").lower()
    if "pdf" in ctype:
        return ".pdf"
    if "html" in ctype or "xml" in ctype:
        return ".html"
    return ""

# --- crawling ---------------------------------------------------------------

@dataclass
class CrawlItem:
    url: str
    depth: int

def extract_links(html: str, base_url: str) -> List[str]:
    # Lightweight href extractor; resolves relative URLs and strips fragments
    out: List[str] = []
    for m in HTML_LINK_RE.finditer(html or ""):
        href = m.group("href").strip()
        if not href:
            continue
        if href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        absu = urljoin(base_url, href)
        out.append(strip_fragment(absu))
    return out

def fetch(session: requests.Session, url: str, rate_per_sec: float, timeout: int = 60) -> Tuple[int, Dict[str, str], bytes]:
    rate_sleep(rate_per_sec)
    r = session.get(url, timeout=timeout, allow_redirects=True)
    status = r.status_code
    headers = {k.lower(): v for k, v in r.headers.items()}
    content = r.content if status == 200 else b""
    return status, headers, content

def save_payload(root: Path, url: str, content: bytes, content_type: str, filetypes: Set[str]) -> Optional[Path]:
    # Determine filename
    p = urlparse(url)
    ext = Path(p.path).suffix.lower()
    if not ext:
        # fall back to content-type
        ext = guess_ext_from_ctype(content_type)
        if not ext:
            # if unknown ctype, only persist if .html allowed and looks like HTML
            if b"<html" in content.lower() and ".html" in filetypes:
                ext = ".html"
            else:
                return None

    # only keep allowed types
    if ext not in filetypes:
        return None

    domain = (p.hostname or "unknown").lower()
    clean_path = sanitize(p.path.lstrip("/")).rstrip("_")
    if not clean_path:
        clean_path = "index"

    # Ensure a unique-ish path: /gov/<domain>/<path>/file.ext
    outdir = root / domain / sanitize(Path(p.path).parent.as_posix().lstrip("/"))
    ensure_dir(outdir)
    fname = sanitize(Path(p.path).name or "index")
    outpath = outdir / f"{fname}{ext}"

    write_bytes(outpath, content)
    return outpath

def run_from_config(cfg: dict, paths: dict, *_args, **_kwargs) -> None:
    if not cfg.get("enabled", False):
        log.info("disabled in config; skipping")
        return

    seeds: List[str] = cfg.get("seeds", [])
    allow_domains = {d.lower() for d in cfg.get("allow_domains", [])}
    if not allow_domains:
        # If not explicitly set, infer from seeds
        allow_domains = {url_domain(s) for s in seeds if url_domain(s)}
        log.info(f"inferred allow_domains={sorted(allow_domains)}")

    filetypes = {e.lower() for e in cfg.get("filetypes", [".pdf", ".html"])}
    rate_per_sec: float = float(cfg.get("rate_per_sec", 1))
    # Sensible bounds to avoid explosions
    max_per_domain: int = int(cfg.get("max_per_domain", 800))
    max_depth: int = int(cfg.get("max_depth", 3))

    out_root = Path(paths.get("raw_dir", "data/raw")) / "gov"
    ensure_dir(out_root)

    s = requests.Session()
    s.headers.update({"User-Agent": "OpenStrength-GovCrawler/1.0 (+https://example.org)"})

    seen: Set[str] = set()
    saved_per_domain: Dict[str, int] = collections.defaultdict(int)

    # Seed queue
    q: Deque[CrawlItem] = collections.deque()
    for seed in seeds:
        if not seed:
            continue
        seed = norm_url(seed)
        if is_allowed(seed, allow_domains):
            q.append(CrawlItem(seed, 0))
            seen.add(seed)
        else:
            log.info(f"Seed outside allow_domains, skipping: {seed}")

    while q:
        item = q.popleft()
        url = item.url
        dom = url_domain(url)

        # Stop if domain quota reached
        if saved_per_domain[dom] >= max_per_domain:
            continue

        try:
            status, headers, content = fetch(s, url, rate_per_sec)
        except Exception as e:
            log.warning(f"fetch failed: {url} :: {e}")
            continue

        ctype = headers.get("content-type", "")
        final_url = headers.get("content-location", "") or url  # requests follows redirects; still record original

        # Decide whether we save this URL based on ext/ctype
        save_path: Optional[Path] = None
        if status == 200:
            # We save only if type matches requested filetypes
            # First, check extension allow-list
            if should_fetch(final_url, filetypes):
                p = save_payload(out_root, final_url, content, ctype, filetypes)
                if p:
                    save_path = p
                    saved_per_domain[dom] += 1

        # If HTML, extract links (even if not saved due to filetypes mismatch)
        do_extract = False
        if status == 200:
            if "html" in ctype or should_fetch(final_url, {".html"}):
                do_extract = True

        links: List[str] = []
        if do_extract:
            try:
                html_text = content.decode("utf-8", errors="ignore")
                links = extract_links(html_text, final_url)
            except Exception as e:
                log.debug(f"link-extract failed {final_url}: {e}")

        # Enqueue new links (stay in-domain, obey limits)
        if item.depth < max_depth and links:
            for lk in links:
                lk = norm_url(lk)
                if lk in seen:
                    continue
                if not is_allowed(lk, allow_domains):
                    continue
                # Only consider URLs that might be interesting according to allowed filetypes or HTML pages
                if not should_fetch(lk, filetypes | {".html"}):
                    continue
                # Domain quota check (rough guard)
                if saved_per_domain[url_domain(lk)] >= max_per_domain:
                    continue
                seen.add(lk)
                q.append(CrawlItem(lk, item.depth + 1))

        # Write metadata for saved items
        if save_path:
            meta = {
                "source": "govcrawl",
                "requested_url": url,
                "final_url": final_url,
                "status": status,
                "content_type": ctype,
                "saved_path": str(save_path.as_posix()),
                "domain": dom,
                "depth": item.depth,
            }
            write_json(save_path.with_suffix(save_path.suffix + ".metadata.json"), meta)

    # Summary
    total_saved = sum(saved_per_domain.values())
    by_dom = ", ".join(f"{k}:{v}" for k, v in sorted(saved_per_domain.items()))
    log.info(f"crawl complete. saved={total_saved} by_domain=[{by_dom}]")