# src/openstrength/ingest/run.py
from __future__ import annotations
import argparse, importlib, inspect, json, sys, traceback
from pathlib import Path

try:
    import yaml  # pyyaml
except Exception as e:
    print("FATAL: PyYAML not installed. pip install pyyaml", file=sys.stderr)
    raise

DEFAULT_SOURCE_ORDER = [
    "pmc",
    "arxiv",
    "biorxiv",
    "gov",
    "unpaywall",
    "doaj",
    "oai_pmh",
    "zenodo",
    "figshare",
]

# For each source, try these candidate entrypoint names in order.
CANDIDATE_FUNCS = {
    "pmc":       ["run_from_config", "harvest_pmc", "run", "main", "entrypoint"],
    "arxiv":     ["run_from_config", "harvest_arxiv", "run", "main", "entrypoint"],
    "biorxiv":   ["run_from_config", "harvest_biorxiv", "run", "main", "entrypoint"],
    "gov":       ["run_from_config", "crawl", "run", "main", "entrypoint", "harvest_gov"],
    "unpaywall": ["run_from_config", "harvest_unpaywall", "run", "main", "entrypoint"],
    "doaj":      ["run_from_config", "harvest_doaj", "run", "main", "entrypoint"],
    "oai_pmh":   ["run_from_config", "harvest_oai_pmh", "run", "main", "entrypoint"],
    "zenodo":    ["run_from_config", "harvest_zenodo", "run", "main", "entrypoint"],
    "figshare":  ["run_from_config", "harvest_figshare", "run", "main", "entrypoint"],
}

# Map source name -> import path (module file name under src/openstrength/ingest/)
MODULE_PATHS = {
    "pmc":       "src.openstrength.ingest.pmc",
    "arxiv":     "src.openstrength.ingest.arxiv",
    "biorxiv":   "src.openstrength.ingest.biorxiv",
    "gov":       "src.openstrength.ingest.govcrawl",
    "unpaywall": "src.openstrength.ingest.unpaywall",
    "doaj":      "src.openstrength.ingest.doaj",
    "oai_pmh":   "src.openstrength.ingest.oai_pmh",
    "zenodo":    "src.openstrength.ingest.zenodo",
    "figshare":  "src.openstrength.ingest.figshare",
}

def load_yaml(p: Path) -> dict:
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def call_runner(mod, func_name: str, cfg: dict) -> None:
    fn = getattr(mod, func_name, None)
    if not callable(fn):
        raise AttributeError(f"function {func_name} not found")

    # Try (cfg) first; if the function doesn’t accept args, fall back to no-arg call.
    try:
        sig = inspect.signature(fn)
        if len([p for p in sig.parameters.values() if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]) >= 1:
            return fn(cfg)  # try passing the config
        else:
            return fn()     # no-arg function
    except TypeError:
        # Try the opposite calling convention
        try:
            return fn()
        except TypeError:
            return fn(cfg)

def run_source(name: str, cfg: dict) -> tuple[str, str]:
    # Disabled?
    scfg = cfg.get(name, {})
    enabled = bool(scfg.get("enabled", False))
    if not enabled:
        # keep your existing “disabled” messaging style
        tag = name.upper() if name in ("gov","unpaywall","doaj","zenodo") else name
        return "ok", f"[{tag.capitalize()}] disabled in config; skipping"

    # Import the module
    mod_path = MODULE_PATHS.get(name)
    if not mod_path:
        return "skip", f"[skip] {name}: no module path mapping."

    try:
        mod = importlib.import_module(mod_path)
    except Exception as e:
        return "skip", f"[skip] {name}: cannot import module ({e})."

    # Try candidates
    for cand in CANDIDATE_FUNCS.get(name, []):
        try:
            call_runner(mod, cand, cfg)
            return "ok", f"[ok]   {name}"
        except AttributeError:
            continue
        except Exception as e:
            tb = traceback.format_exc(limit=1)
            return "error", f"[error] {name}: {e}\n{tb}"

    return "skip", f"[skip] {name}: no runner found (module missing or no supported entrypoint)."

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", type=str, required=True, help="Path to sources.yaml")
    args = ap.parse_args()

    src_path = Path(args.sources)
    cfg = load_yaml(src_path)

    out_root = (cfg.get("paths") or {}).get("raw_dir", "data/raw")
    print(f"==> starting run: out_root='{out_root}'")
    print(f"==> sources file: '{src_path}'")

    results = []
    for name in DEFAULT_SOURCE_ORDER:
        status, msg = run_source(name, cfg)
        print(msg)
        results.append((name, status))

    summary = {
        "total": len(results),
        "ok": sum(1 for _, s in results if s == "ok"),
        "error": sum(1 for _, s in results if s == "error"),
        "missing": sum(1 for _, s in results if s == "skip"),
    }
    print("\n==> summary")
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()