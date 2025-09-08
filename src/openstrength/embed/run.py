from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--model", default="sentence-transformers/bge-base-en-v1.5")
parser.add_argument("--inp", default="data/curated/chunks_science.parquet")
parser.add_argument("--out", default="artifacts/embeddings")
args = parser.parse_args()

Path(args.out).mkdir(parents=True, exist_ok=True)

def main():
    df = pd.read_parquet(args.inp)
    model = SentenceTransformer(args.model)
    embs = model.encode(df["text"].tolist(), batch_size=64, convert_to_numpy=True, show_progress_bar=True, normalize_embeddings=True)
    np.save(Path(args.out) / "vectors.npy", embs)
    df[["chunk_id","doc_id","license"]].to_parquet(Path(args.out) / "meta.parquet", index=False)
    print(f"Saved {embs.shape} embeddings to {args.out}")

if __name__ == "__main__":
    main()
