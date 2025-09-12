# src/openstrength/ingest/run.py
from __future__ import annotations

import sys
import time
import traceback
from pathlib import Path
import yaml

from .pmc import harvest_pmc
from .arxiv import harvest_arxiv
from .biorxiv import harvest_biorxiv
from .govcrawl import harvest_gov
from .unpaywall import harvest_unpaywall
from .doaj import harvest_doaj
from .core_api import harvest_core
from .oai_pmh import harvest_oai_pmh
from .zenodo import harvest_zenodo
from .figshare import harvest_figshare

CFG = "configs/ingest/sources.yaml"

# Map human-readable names to YAML keys
KEYS = {
    "PMC": "pmc",
    "arXiv": "arxiv",
    "bioRxiv/medRxiv": "biorxiv",
    "Gov": "govcrawl",
    "Unpaywall": "unpaywall",
    "DOAJ": "doaj",
    "CORE": "core",
    "OAI-PMH": "oai_pmh",
    "Zenodo": "zenodo",
    "Figshare": "figshare",
}

PIPELINE = [
    ("PMC", harvest_pmc),
    ("arXiv", harvest_arxiv),
    ("bioRxiv/medRxiv", harvest_biorxiv),
    ("Gov", harvest_gov),
    ("Unpaywall", harvest_unpaywall),
    ("DOAJ", harvest_doaj),
    ("CORE", harvest_core),
    ("OAI-PMH", harvest_oai_pmh),
    ("Zenodo", harvest_zenodo),
    ("Figshare", harvest_figshare),
]


def main() -> int:
    cfg_path = Path(CFG)
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))

    # Ensure raw_dir exists (use absolute path to avoid CWD surprises)
    raw_dir = Path(cfg["paths"]["raw_dir"]).resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)
    print(f"[ingest] raw_dir = {raw_dir}")

    for name, fn in PIPELINE:
        key = KEYS[name]
        if not cfg.get(key, {}).get("enabled", False):
            continue  # <-- only run when enabled

        try:
            print(f"==> {name} start")
            fn(cfg)
            print(f"==> {name} done")
        except Exception as e:
            traceback.print_exc()
            print(f"[WARN] {name} failed: {e}")
        finally:
            time.sleep(0.2)

    return 0


if __name__ == "__main__":
    sys.exit(main())
