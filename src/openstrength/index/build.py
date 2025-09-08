from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
import faiss
import yaml
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--emb", default="artifacts/embeddings")
parser.add_argument("--out", default="artifacts/indices")
parser.add_argument("--cfg", default="configs/index/faiss.yaml")
args = parser.parse_args()

Path(args.out).mkdir(parents=True, exist_ok=True)

def main():
    X = np.load(Path(args.emb) / "vectors.npy").astype("float32")
    meta = pd.read_parquet(Path(args.emb) / "meta.parquet")
    cfg = yaml.safe_load(Path(args.cfg).read_text())
    if cfg["index_type"] == "hnsw":
        index = faiss.IndexHNSWFlat(X.shape[1], cfg["hnsw"]["m"])
        index.hnsw.efConstruction = cfg["hnsw"]["ef_construction"]
    else:
        index = faiss.IndexFlatIP(X.shape[1])
    faiss.normalize_L2(X)
    index.add(X)
    faiss.write_index(index, str(Path(args.out) / "science.faiss"))
    meta.to_parquet(Path(args.out) / "meta.parquet", index=False)
    print(f"Built index with {index.ntotal} vectors")

if __name__ == "__main__":
    main()
