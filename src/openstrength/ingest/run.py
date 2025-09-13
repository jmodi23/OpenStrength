from __future__ import annotations
import sys, time, yaml
from pathlib import Path
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

from openstrength.ingest.pmc import harvest_pmc
from openstrength.ingest.arxiv import harvest_arxiv
from openstrength.ingest.biorxiv import harvest_biorxiv
from openstrength.ingest.core_api import harvest_core


CFG = "configs/ingest/sources.yaml"

PIPELINE = [
    ("pmc", "PMC", harvest_pmc),
    ("arxiv", "arXiv", harvest_arxiv),
    ("biorxiv", "bioRxiv/medRxiv", harvest_biorxiv),
    ("gov", "Gov", harvest_gov),
    ("unpaywall", "Unpaywall", harvest_unpaywall),
    ("doaj", "DOAJ", harvest_doaj),
    ("core", "CORE", harvest_core),
    ("oai_pmh", "OAI-PMH", harvest_oai_pmh),
    ("zenodo", "Zenodo", harvest_zenodo),
    ("figshare", "Figshare", harvest_figshare),
]

def main():
    cfg = yaml.safe_load(Path(CFG).read_text(encoding="utf-8"))
    Path(cfg["paths"]["raw_dir"]).mkdir(parents=True, exist_ok=True)
    summary = {"total": 0, "ok": 0, "error": 0, "missing": 0}

    for key, label, fn in PIPELINE:
        summary["total"] += 1
        if not cfg.get(key, {}).get("enabled", False):
            print(f"[{label}] disabled in config; skipping")
            summary["missing"] += 1
            continue
        print(f"==> {label} start")
        try:
            fn(cfg)
            print(f"==> {label} done")
            summary["ok"] += 1
        except Exception as e:
            print(f"[error] {label}: {e}")
            summary["error"] += 1
        time.sleep(0.1)

    print("\n==> summary")
    print(summary)

if __name__ == "__main__":
    sys.exit(main())