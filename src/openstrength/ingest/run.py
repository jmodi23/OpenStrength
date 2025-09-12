#!/usr/bin/env python3
# -- coding: utf-8 --

"""
Central runner for OpenStrength ingestion.

- Reads sources.yaml
- Dispatches to per-source run_from_config(name, cfg, out_root) functions
- Handles --only / --skip filters
- OAI-PMH logic is intentionally commented out (see the stub below)

Usage:
  python run.py --sources sources.yaml --out data/raw
  python run.py --sources sources.yaml --out data/raw --only pmc,arxiv
  python run.py --sources sources.yaml --out data/raw --skip oai_pmh
"""

from __future__ import annotations
import os
import sys
import json
import time
import argparse
from typing import Any, Dict

# --- lightweight YAML loader ---
try:
    import yaml
except Exception as e:
    raise SystemExit("ERROR: You need PyYAML installed: pip install pyyaml") from e


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


# --- lazy imports & dispatch ---

def _import_runner(modname: str):
    """
    Import a module and fetch its run_from_config. Returns None if missing.
    """
    try:
        mod = __import__(modname, fromlist=["run_from_config"])
        return getattr(mod, "run_from_config", None)
    except Exception:
        return None


DISPATCH = {
    # working & updated sources
    "pmc": _import_runner("pmc"),
    "arxiv": _import_runner("arxiv"),
    "unpaywall": _import_runner("unpaywall"),
    "zenodo": _import_runner("zenodo"),

    # comment out oai_pmh (see stub below)
    # "oai_pmh": _import_runner("oai_pmh"),

    # keep placeholders for others you might add later
    "biorxiv": _import_runner("biorxiv"),
    "gov": _import_runner("govcrawl"),   # if your module is govcrawl.py
    "doaj": _import_runner("doaj"),
}


def available_sources():
    return [k for k, v in DISPATCH.items() if v is not None]


def run_source(name: str, cfg: Dict[str, Any], out_root: str):
    fn = DISPATCH.get(name)
    if fn is None:
        print(f"[skip] {name}: no runner found (module missing or no run_from_config).")
        return {"source": name, "status": "missing"}
    print(f"[run] {name} ...")
    t0 = time.time()
    try:
        stats = fn(name, cfg or {}, out_root)
        dt = time.time() - t0
        # Best-effort pretty print of dataclass/dict
        if hasattr(stats, "_dict_"):
            stats_obj = stats._dict_
        elif isinstance(stats, dict):
            stats_obj = stats
        else:
            try:
                from dataclasses import asdict
                stats_obj = asdict(stats)
            except Exception:
                stats_obj = {"_repr": repr(stats)}
        print(json.dumps({"source": name, "elapsed_sec": round(dt, 2), "stats": stats_obj}, indent=2))
        return {"source": name, "elapsed_sec": dt, "stats": stats_obj, "status": "ok"}
    except KeyboardInterrupt:
        raise
    except Exception as e:
        dt = time.time() - t0
        print(json.dumps({"source": name, "elapsed_sec": round(dt, 2), "error": str(e)}, indent=2))
        return {"source": name, "elapsed_sec": dt, "error": str(e), "status": "error"}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="OpenStrength central ingestion runner")
    p.add_argument("--sources", default="sources.yaml", help="Path to sources.yaml")
    p.add_argument("--out", default="data/raw", help="Root output directory")
    p.add_argument("--only", default="", help="Comma-separated subset of sources to run")
    p.add_argument("--skip", default="", help="Comma-separated sources to skip")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg_all = load_yaml(args.sources)

    ensure_dir(args.out)

    only = set([s.strip() for s in args.only.split(",") if s.strip()]) if args.only else set()
    skip = set([s.strip() for s in args.skip.split(",") if s.strip()]) if args.skip else set()

    # Determine run order: YAML key order if possible, else alphabetical.
    # PyYAML preserves mapping order since Python 3.7.
    declared_order = list(cfg_all.keys()) if isinstance(cfg_all, dict) else []
    known = set(DISPATCH.keys())
    run_list = [s for s in declared_order if s in known] or sorted(available_sources())

    results = []
    print(f"==> starting run: out_root='{args.out}'")
    print(f"==> sources file: '{args.sources}'")
    if only:
        print(f"==> ONLY: {sorted(only)}")
    if skip:
        print(f"==> SKIP: {sorted(skip)}")

    for name in run_list:
        if only and name not in only:
            print(f"[skip] {name}: not in --only")
            continue
        if name in skip:
            print(f"[skip] {name}: in --skip")
            continue

        cfg = cfg_all.get(name, {}) if isinstance(cfg_all, dict) else {}
        enabled = bool(cfg.get("enabled", True))
        if not enabled:
            print(f"[skip] {name}: enabled=false")
            continue

        # --- OAI-PMH: intentionally disabled/commented out -------------------
        if name in ("oai_pmh", "pmh", "oai"):
            print(f"[skip] {name}: OAI-PMH logic is commented out in runner (see stub).")
            # If you want to re-enable later, implement oai_pmh.run_from_config
            # and then remove the skip above. For reference, your OLD logic likely looked like:
            #
            #   from oai_pmh import harvest_oai
            #   stats = harvest_oai(topic=..., out_dir=..., years=..., ...)
            #
            # We’ve standardized around per-module run_from_config(name, cfg, out_root).
            # -------------------------------------------------------------------
            continue

        results.append(run_source(name, cfg, args.out))

    # Short summary
    ok = sum(1 for r in results if r.get("status") == "ok")
    err = sum(1 for r in results if r.get("status") == "error")
    miss = sum(1 for r in results if r.get("status") == "missing")
    print("\n==> summary")
    print(json.dumps({
        "total": len(results),
        "ok": ok,
        "error": err,
        "missing": miss,
    }, indent=2))


if __name__ == "__main__":
    main()