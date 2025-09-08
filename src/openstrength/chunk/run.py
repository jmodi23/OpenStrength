from __future__ import annotations
import hashlib, math
from pathlib import Path
import pandas as pd
import nltk

nltk.download('punkt', quiet=True)

IN_PARQUET = Path("data/curated/docs.parquet")
OUT_PARQUET = Path("data/curated/chunks_science.parquet")
OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

def chunk_text(text: str, target_tokens=400) -> list[str]:
    # crude token proxy = words/ ~0.75; good enough for MVP
    sents = nltk.sent_tokenize(text)
    chunks, cur = [], []
    cur_len = 0
    for s in sents:
        n = max(1, len(s.split()))
        if cur_len + n > target_tokens and cur:
            chunks.append(" ".join(cur))
            cur, cur_len = [], 0
        cur.append(s); cur_len += n
    if cur: chunks.append(" ".join(cur))
    return chunks

def main():
    df = pd.read_parquet(IN_PARQUET)
    out_rows = []
    for _, row in df.iterrows():
        chunks = chunk_text(row["text"], target_tokens=400)
        for i, ch in enumerate(chunks):
            cid = hashlib.md5((row["doc_id"] + str(i)).encode()).hexdigest()[:12]
            out_rows.append({
                "chunk_id": f"{row['doc_id']}:{cid}",
                "doc_id": row["doc_id"],
                "text": ch,
                "license": row.get("license"),
            })
    pd.DataFrame(out_rows).to_parquet(OUT_PARQUET, index=False)
    print(f"Wrote {len(out_rows)} chunks → {OUT_PARQUET}")

if __name__ == "__main__":
    main()
