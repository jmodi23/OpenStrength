from __future__ import annotations
import time, yaml, sys
from pathlib import Path
from tqdm import tqdm
from .pmc import harvest_pmc
from .arxiv import harvest_arxiv
from .biorxiv import harvest_biorxiv
from .govcrawl import harvest_gov

CFG = "configs/ingest/sources.yaml"

def main():
    cfg = yaml.safe_load(Path(CFG).read_text(encoding="utf-8"))
    raw_dir = Path(cfg["paths"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    tasks = []
    if cfg.get("pmc", {}).get("enabled"):
        tasks.append(("PMC", harvest_pmc, cfg))
    if cfg.get("arxiv", {}).get("enabled"):
        tasks.append(("arXiv", harvest_arxiv, cfg))
    if cfg.get("biorxiv", {}).get("enabled"):
        tasks.append(("bioRxiv/medRxiv", harvest_biorxiv, cfg))
    if cfg.get("gov", {}).get("enabled"):
        tasks.append(("Gov", harvest_gov, cfg))

    for name, fn, c in tasks:
        print(f"==> {name} start")
        fn(c)
        print(f"==> {name} done")
        time.sleep(0.5)

if __name__ == "__main__":
    sys.exit(main())
