from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from .schema import Document, ProtocolExercise, Citation

IN_JSONL = Path("data/staged/extracted.jsonl")
OUT_PARQUET = Path("data/curated/docs.parquet")
OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

def normalize_record(rec: dict) -> dict:
    # Minimal heuristic normalization. Extend with real parsing of tables later.
    goal = []
    if "hypertrophy" in rec["fulltext"].lower(): goal.append("hypertrophy")
    if "strength" in rec["fulltext"].lower(): goal.append("strength")

    citations = [Citation(doi=doi, title=None, chunk_id=None, source=rec.get("source_path")) for doi in rec.get("dois", [])]
    doc = Document(
        doc_id=rec["doc_id"],
        license=rec.get("license"),
        type="trial" if "random" in rec["fulltext"].lower() else "review",
        population={},
        goal=goal,
        protocol={"exercises":[]},
        nutrition={},
        evidence={},
        citations=citations,
        text=rec["fulltext"],
    )
    return doc.model_dump()

def main():
    rows = []
    with IN_JSONL.open("r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            rows.append(normalize_record(rec))
    df = pd.DataFrame(rows)
    df.to_parquet(OUT_PARQUET, index=False)
    print(f"Wrote {len(df)} normalized docs → {OUT_PARQUET}")

if __name__ == "__main__":
    main()
