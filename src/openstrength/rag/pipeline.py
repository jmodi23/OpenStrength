from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
import json, yaml

class Retriever:
    def __init__(self, emb_model: str, idx_path: str, meta_path: str):
        self.emb = SentenceTransformer(emb_model)
        self.index = faiss.read_index(idx_path)
        self.meta = pd.read_parquet(meta_path)
        self.texts = {row["chunk_id"]: None for _, row in self.meta.iterrows()}
        # we need chunk texts to return—load from curated chunks
        chunks = pd.read_parquet("data/curated/chunks_science.parquet")[["chunk_id","text"]]
        self.chunks = dict(zip(chunks["chunk_id"], chunks["text"]))

    def search(self, query: str, k=10):
        q = self.emb.encode([query], normalize_embeddings=True)
        D, I = self.index.search(q.astype("float32"), k)
        rows = []
        for idx in I[0]:
            cid = self.meta.iloc[idx]["chunk_id"]
            rows.append({"chunk_id": cid, "text": self.chunks.get(cid, ""), "license": self.meta.iloc[idx]["license"]})
        return rows

class Generator:
    def __init__(self, model_name="mistralai/Mistral-7B-Instruct-v0.2"):
        self.tok = AutoTokenizer.from_pretrained(model_name)
        self.lm = AutoModelForCausalLM.from_pretrained(model_name, torch_dtype=torch.float16, device_map="auto")

    def generate_json(self, system: str, user: str):
        prompt = f"<s>[INST] <<SYS>>\n{system}\n<</SYS>>\n{user} [/INST]"
        ids = self.tok(prompt, return_tensors="pt").to(self.lm.device)
        out = self.lm.generate(**ids, max_new_tokens=800, temperature=0.2)
        text = self.tok.decode(out[0], skip_special_tokens=True)
        # Extract last JSON object heuristically
        js = text[text.find("{"): text.rfind("}")+1]
        return json.loads(js)

def make_user_prompt(template_path: str, variables: dict, contexts: list[dict]) -> str:
    tmpl = yaml.safe_load(Path(template_path).read_text())["user_template"]
    desc = tmpl.format(**variables)
    ctx = "\n\n".join([f"[{c['chunk_id']}]\n{c['text']}" for c in contexts])
    return f"{desc}\n\nContext:\n{ctx}"

def plan(goal, training_age, frequency, equipment, bodymass_kg, constraints=None):
    sys_cfg = yaml.safe_load(Path("configs/rag/prompt.yaml").read_text())
    r_cfg = yaml.safe_load(Path("configs/rag/retrieval.yaml").read_text())
    retriever = Retriever(r_cfg["embedding_model"], "artifacts/indices/science.faiss", "artifacts/indices/meta.parquet")
    contexts = retriever.search(f"{goal} {training_age} {equipment}", k=r_cfg["k"])
    user = make_user_prompt("configs/rag/prompt.yaml",
                            {"goal": goal, "training_age": training_age, "frequency": frequency, "equipment": equipment,
                             "bodymass_kg": bodymass_kg, "constraints": constraints or "none"},
                            contexts)
    gen = Generator()  # swap to your chosen local model
    return gen.generate_json(sys_cfg["system"], user)
