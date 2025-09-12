# OpenStrength

OpenStrength is a lightweight, configurable pipeline for harvesting **open-access** literature and datasets around strength training, nutrition, and related topics. It pulls from multiple sources (PMC, arXiv, bioRxiv, DOAJ, Unpaywall, institutional OAI-PMH endpoints, Zenodo, Gov sites, etc.), normalizes per-item metadata, saves PDFs (when allowed), and writes everything to a consistent on-disk layout.

> One command. One config file. Multiple sources.

---

## Quick start

```bash
# Python 3.10+ recommended
python -m venv .venv
. .venv/Scripts/activate    # Windows
# or: source .venv/bin/activate

pip install -U pip
pip install -r requirements.txt

# Run the centralized harvester
python -m src.openstrength.ingest.run --sources "configs/ingest/sources.yaml"
```
By default, data lands under `data/raw/<source>/....` You can change this in the config.

## **Features** 

- Single entry point: `src/openstrength/ingest/run.py` loads one `sources.yaml` and orchestrates all enabled sources.
- **Per-source parallelization** (respecting rate limits).
- Robust fetching:
    - Retry/backoff for transient errors (e.g., arXiv `504`, PMC `5xx`).
    - MIME/type checks so we don’t save HTML error pages as PDFs.

- License filtering: only keep items whose license matches your allowlist.
- Traceable storage: every item has a folder with ``metadata.json`` and (when allowed) `paper.pdf`.

## Repository Layout
```bash
OpenStrength/
├─ configs/
│  └─ ingest/
│     └─ sources.yaml         # the central config you edit
├─ data/
│  ├─ raw/                    # harvested outputs (default)
│  └─ snapshots/              # optional archives/checkpoints
├─ src/
│  └─ openstrength/
│     └─ ingest/
│        ├─ run.py            # central runner (loads sources.yaml)
│        ├─ pmc.py            # PubMed Central harvester
│        ├─ arxiv.py          # arXiv harvester
│        ├─ biorxiv.py        # bioRxiv/medRxiv harvester
│        ├─ doaj.py           # Directory of Open Access Journals
│        ├─ govcrawl.py       # selected gov/WHO/CDC/etc. seeds
│        ├─ oai_pmh.py        # OAI-PMH (e.g., Harvard DASH, MIT DSpace)
│        ├─ unpaywall.py      # Unpaywall lookup (email required)
│        └─ zenodo.py         # Zenodo search
├─ requirements.txt
└─ README.md
```

## Configuration
All behavior is controlled by `configs/ingest/sources.yaml`. Example (abbreviated):

```yaml
paths:
  raw_dir: "data/raw"
  snapshots_dir: "data/snapshots"

licenses:
  allow:
    - CC-BY-4.0
    - CC-BY-SA-4.0
    - CC0-1.0
    - US-Gov-PD
    - Public-Domain

pmc:
  enabled: true
  queries:
    - 'creatine[tiab] AND 2010:2025[PDAT]'
  max_results_per_query: 5000
  email: "you@example.com"
  api_key: ""        # optional
  rate_per_sec: 3

arxiv:
  enabled: true
  queries:
    - 'ti:"creatine" OR abs:"creatine"'
  categories: [q-bio, cs.LG, stat.ML]
  max_results_per_query: 2000
  rate_per_sec: 1
```

## Common knobs:
    *enabled: turn a source on/off.
    * queries: per-source search strings.
    * rate_per_sec: throttle to respect rate limits.
    * max_results_per_query / pages: cap volume.
    * license_whitelist (per source) and global licenses.allow: enforce permissive reuse.


## Output Layout 
Each source writes under `data/raw/<source>/<query-slug>/...`

Example (arXiv):

```bash
data/raw/arxiv/ti_creatine_or_abs_creatine_/
└─ 2411.01004v1/
   ├─ `metadata.json`
   └─ paper.pdf            # present when fetched AND license permitted
```

Example (PMC):

```bash
data/raw/pmc/creatine_tiab_2010_2025_pdat/
└─ PMC12345678/
   ├─ `metadata.json`
   └─ paper.pdf            # if PDF link permitted and retrievable
```

``metadata.json`` always includes what was attempted (including PDF URL if known) and the final `pdf_status` (`ok` or `pending`).

## **Installation**
Create a virtual environment and install:

```bash
pip install -r requirements.txt
```

## **Minimal Requirements**:
    * `requests`
    * `beautifulsoup4`
    * `lxml` ← required for PMC XML parsing
    * `feedparser` ← improves arXiv Atom parsing (fallback included)
    * `PyYAML`
    * `tqdm` ← progress bars
    * `urllib3` (via requests)

## **Running**
```bash
python -m src.openstrength.ingest.run --sources "configs/ingest/sources.yaml"
```

The runner will:
    1. Load the YAML.
    2. For each enabled source, call its `run_from_config(cfg, out_root)`.
    3. Print per-source status and a summary.

You can rerun safely; already-written `metadata.json` is overwritten atomically, PDFs are skipped if present and non-trivial in size.

## **Source-Specific Notes**

**PMC**
- Uses E-utilities: search → efetch XML → parse license → try “Self-URI” PDF links.
- If a record has `license=None` or a non-allowed license, it’s skipped early with a clear log line.
- Needs `lxml` installed for `BeautifulSoup(..., "lxml-xml")`.

**arXiv**
- Atom API with pagination.
- Robust PDF downloader with backoff; `504`/`5xx` are retried. Non-PDF responses (e.g., `text/plain` error pages) are logged and marked `pending`.

**bioRxiv / medRxiv**
- JSON API where available; license filtering on `cc-by`, `cc0`, `cc-by-sa` (configurable).

**DOAJ**
- Works via DOAJ API; filters by CC licenses in the config.

**Unpaywall**
- Requires a valid email in the config.
- Honors license allowlist; writes ````metadata.json`, fetches OA link if clearly allowed and stable.

**OAI-PMH**
- Targets institutional repositories (e.g., Harvard DASH, MIT DSpace).
- Filters by date range + query terms; pulls license from Dublin Core fields where present.

**Gov crawl**
- Seed URLs + allowlist of domains + filetype filters (e.g., ```.pdf```, ```.html```).
- Polite rate-limit; won’t stray outside approved domains.

**Zenodo**
- REST search; respects ```license_whitelist```.


## **Troubleshooting**
```python
bs4.exceptions.FeatureNotFound: lxml-xml
```

Install ```lxml```:
```python
pip install lxml
```

**arXiv logs** ```pdf-get skipped ... ctype=text/plain``` or ```status=504```
This is arXiv throttling/transient errors. The harvester retries with exponential backoff and marks the item pdf_status: "pending" if it still can’t fetch. Re-run later.

**PMC:** ```skip license='None'```
The record does not carry a machine-readable license (or it’s outside your allowlist). That’s expected behavior if you require permissive reuse.

**Nothing runs / “no runner found”**
Every module exposes ```run_from_config(cfg, out_root)```. Ensure your package import path is correct (running from repo root helps) and the module files live under ```src/openstrength/ingest/```.

**Windows paths**
Use quoted absolute paths for `--sources` as you’ve been doing.

## **Development**
- Keep each source self-contained and idempotent.
- All modules must expose:

```python
def run_from_config(cfg: dict, out_root: str | Path) -> None: ...
```

- Write files atomically:
    - JSON via temp file + `os.replace`
    - PDFs via streamed writes to temp + `os.replace`

**Adding a new source** 
1. Create `src/openstrength/ingest/<newsource>.py` with `run_from_config`.
2. Add a section to `sources.yaml`.
3. Re-run the centralized runner.

## **Data & Licensing**
OpenStrength is designed for **open-access**. You control reuse via `licenses.allow` (global) plus per-source `license_whitelist`. If an item doesn’t match, it’s skipped. You are responsible for verifying downstream uses comply with each work’s license.

## **Roadmap (short)**
- Optional SQLite index/manifest of harvested items.
- Smarter PDF re-try queue for pending items.
- Deduplication across sources via DOI/arXiv ID/PMCID.

## **Contributing**
- PRs welcome! Please:
    - Keep network calls polite (respect rate_per_sec).
    - Add informative logs.
    - Don’t relax license filters by default.

## **Citation**
If this pipeline helps your work, a quick mention (and a star ⭐) is appreciated.

## **License**

This repository’s code is under an OSI-approved license (see `LICENSE`). **Harvested content** remains under the license of the original source.