# Statement of Work (SOW)

## 1. Purpose

Design, implement, and evaluate a zero-cost, open-source system that ingests permissively licensed fitness/nutrition literature and publicly licensed training plans, retrieves and synthesizes guidance with citations, and outputs: (a) professional, evidence-grounded text plans and (b) an Excel schedule. The system compares three base LLMs and multiple embedding models under a controlled benchmark to select the best configuration.

## 2. Background

Evidence-based exercise and nutrition guidance is scattered across open-access journals, government guidelines, and community plans with permissive licenses. Retrieval-Augmented Generation (RAG) combined with lightweight fine-tuning (LoRA) on curated instruction data can produce grounded, auditable plans without paid APIs. Selecting the right base LLM and embedding model is critical to performance, cost, and latency.

## 3. Objectives

* Build a legally clean corpus and indices for science sources and plan sources.
* Implement a RAG pipeline that generates structured plans with citations and exports Excel.
* Benchmark three base LLMs and multiple embedding models; select a winner by predefined metrics.
* Apply optional LoRA SFT on permissibly derived supervision; retain RAG for grounding.
* Deliver production-ready CLI/API, documentation, and evaluation harness.

## 4. Success Criteria (Acceptance)

* **Legal**: 100% of included sources carry CC-BY/CC-BY-SA/CC0/Public Domain or equivalent; each chunk has license metadata.
* **Grounding**: ≥95% of generated claims that assert training/nutrition prescriptions include at least one retrievable open citation present in the index.
* **Retrieval Quality**: On a held-out QA set, **Recall\@10 ≥ 0.85** and **nDCG\@10 ≥ 0.70** using the chosen embedding+r reranker.
* **Plan Validity**: Plan validator passes ≥98% of test personas for dosage bounds, weekly volume, progression, rest days, and nutrition macros ranges.
* **Export**: Excel and CSV generated deterministically from the schema with no manual edits; files open in Excel, LibreOffice, Google Sheets.
* **Latency**: P95 end-to-end generation ≤ 12s on specified local hardware for the winning configuration.
* **Reproducibility**: Single `make`/CLI command reproduces indices and evaluation with consistent scores (±1% tolerance).
* **Security**: No PII ingested; no outbound calls during generation; all models served locally.

## 5. Scope

### In Scope

* Corpus acquisition from open-access sources; parsing, normalization, chunking, deduplication.
* Dual-index RAG (ScienceIndex, PlanIndex) with metadata and per-chunk licenses.
* Candidate base LLMs: **Phi-3 Mini/Medium (MIT)**, **Mistral 7B Instruct (Apache-2.0)**, **Qwen2-7B Instruct (Apache-2.0)**.
* Candidate embeddings: **bge-large-en-v1.5**, **intfloat/e5-large-v2**, **gte-large**, **hkunlp/instructor-large** (+ corresponding base variants for ablations).
* Optional reranker: **bge-reranker-base** or **cross-encoder/ms-marco-MiniLM-L-6-v2**.
* LoRA SFT on curated instruction pairs derived from the legal corpus.
* API (FastAPI) + minimal UI (Gradio) + CLI.
* Excel/CSV export and schema validation.
* Automated evaluation harness and reports.

### Out of Scope

* Medical diagnosis or individualized clinical advice beyond general healthy-adult guidance.
* Ingestion of copyrighted/commercial plans without explicit permissive license.
* Model training on non-permissive content.

## 6. Requirements

### 6.1 Functional

* Ingest documents; retain metadata: source URL, DOI, license, publication date, section.
* Normalize to canonical JSON schema for protocols and nutrition.
* Build FAISS indices for science and plans; embed with selected models; optional rerank.
* Generate outputs with explicit structure: text summary, assumptions, lift plan, nutrition, progression rules, contraindications, citations, and export flags.
* Produce `.xlsx` and `.csv` files aligned with the schema.
* Provide endpoints:

  * `POST /plan`: input user profile + goal → structured JSON + downloadable files.
  * `POST /evaluate`: run full benchmark suite; return metrics.
  * `POST /admin/ingest`: add/update sources with license validation.
* Log provenance: chunk IDs and citations embedded in responses.

### 6.2 Non-Functional

* **License hygiene**: enforced at ingest; pipeline fails closed on unknown licenses.
* **Determinism**: seed control; temperature ≤ 0.3 for production.
* **Performance**: meet latency target; memory within single-GPU 12–24GB or CPU-only path.
* **Portability**: Linux/macOS; no paid services; offline operation after initial fetch.
* **Observability**: structured logs for retrieval hits, reranker scores, token counts, latency.

## 7. Architecture

* **Acquisition**: PMC OA subset, WHO/NIH/CDC/USDA, bioRxiv/medRxiv (permissive only), institutional repositories, CC-licensed GitHub plans.
* **Parsing**: GROBID → TEI/XML; trafilatura/html5lib for HTML; pdfminer.six for PDFs.
* **Normalization**: canonical JSON with domains: population, goals, protocol, nutrition, evidence, citations.
* **Embedding**: BGE/E5/GTE/INSTRUCTOR variants; store vectors + metadata.
* **Index**: FAISS (HNSW or IVF+PQ); persist `index/`.
* **Rerank** (optional): cross-encoder on top-50 → top-10.
* **LLM Serving**: Ollama or vLLM (local); models: Phi-3, Mistral 7B, Qwen2-7B.
* **Generation**: RAG → schema-constrained generation → JSON validation → export.
* **Fine-tuning**: PEFT/LoRA via TRL or LLaMA-Factory; 4/8-bit with bitsandbytes; datasets in JSONL.
* **API/UI**: FastAPI + Gradio; Docker optional.

## 8. Data Governance and Licensing

* Accept only sources with CC-BY/CC-BY-SA/CC0/PD or equivalent; store SPDX-like license tags.
* Keep source text; never redistribute closed text.
* Maintain `LICENSES.md` with obligations (attribution for CC-BY; share-alike mirrored in outputs where required).
* Include auto-generated bibliography with DOIs/links in outputs.

## 9. Output Specifications

### 9.1 JSON Contract

```
{
  "summary_text": "...",
  "assumptions": {"training_age":"2y","equipment":"gym"},
  "lift_plan": [
    {"day":1, "blocks":[{"exercise":"Back Squat","sets":4,"reps":"5","intensity":"77%1RM","rest":"2-3m"}]}
  ],
  "nutrition": {"kcal":2300, "protein_g":180, "carb_g":220, "fat_g":70},
  "progression_rules": "...",
  "contraindications": ["..."],
  "citations": [{"title":"...","doi":"...","chunk_id":"..."}],
  "export": {"csv": true, "xlsx": true}
}
```

### 9.2 Excel/CSV

* **Sheets**: `Training`, `Nutrition`, `Progression`, `Citations`.
* **Training columns**: Day | Block | Exercise | Sets | Reps | Intensity | Rest | Notes.
* **Nutrition columns**: kcal | protein\_g | carb\_g | fat\_g | timing | supplements.
* **Citations columns**: title | doi | source | license | chunk\_id.

## 10. Evaluation and Benchmarking

### 10.1 Model Candidates

* Base LLMs: Phi-3 Mini/Medium, Mistral 7B Instruct, Qwen2-7B Instruct.
* Embeddings: bge-large-en-v1.5, e5-large-v2, gte-large, instructor-large (+ base variants).
* Rerankers: bge-reranker-base, ms-marco-MiniLM-L-6-v2.

### 10.2 Datasets

* **Retrieval QA**: 1,000–2,000 item set built from corpus: question + gold passages + DOI labels.
* **Plan Tasks**: 200+ personas (novice/intermediate, hypertrophy/strength/fat loss, equipment constraints).
* **Citation Set**: hand-labeled claim→citation pairs for precision/recall.

### 10.3 Metrics

* Retrieval: Recall\@k, nDCG\@k, MRR.
* Generation: citation coverage (% claims with at least one correct citation), citation precision (correct/total), schema validity rate, constraint violations per plan, word-level groundedness via overlap with retrieved chunks.
* Efficiency: tokens generated, wall time P50/P95, peak memory.

### 10.4 Protocol

* Grid search over {LLM} × {embedding} × {reranker on/off}. Temperature=0.2. k={10,20}. Chunk size={300,500}.
* Report table with best configuration per metric; final winner selected by weighted score: 0.4 grounding, 0.3 retrieval, 0.2 validity, 0.1 latency.

## 11. Plan Validator

* Volume ranges per muscle group vs. goal (evidence-backed bounds; configurable).
* Intensity constraints by experience (novice ≤85% 1RM default).
* Weekly frequency, rest day spacing, progression monotonicity.
* Nutrition defaults: protein 1.6–2.2 g/kg; fat 0.6–1.0 g/kg; carb 3–6 g/kg adjustable.
* Contraindication rules for common conditions; trigger safe fallbacks.

## 12. Work Breakdown Structure (WBS)

1. **Project Setup**: repo, code style, CI, Makefile.
2. **Ingest**: crawlers, license filters, storage.
3. **Parse/Normalize**: GROBID, JSON schema, dedup.
4. **Index**: embeddings, FAISS build, reranker.
5. **RAG Service**: retrieval, synthesis, schema validator.
6. **Exporters**: CSV/XLSX.
7. **Evaluation Harness**: datasets, metrics, reporting.
8. **Fine-tuning**: SFT LoRA, eval.
9. **API/UI**: FastAPI, Gradio.
10. **Docs**: architecture, ops, licenses.

## 13. Timeline (estimated, sequential weeks)

* W1: Setup, ingest MVP (500 docs), schema v1.
* W2: Parsing/dedup, initial indices, RAG stub.
* W3: RAG v1, exporter v1, 50 QA items.
* W4: Benchmark framework, 500 QA items, first grid run.
* W5: Validator v1, dataset expansion, second grid run.
* W6: LoRA SFT on 500–1,000 pairs, eval; API/UI beta.
* W7: Hardening, docs, release v1.0.

## 14. Deliverables

* `data/` Parquet corpus with license metadata.
* `index/` FAISS indices for ScienceIndex and PlanIndex.
* `eval/` datasets and scripts; HTML/markdown report with metrics.
* `train/` JSONL instruction pairs and training configs.
* `serve/` FastAPI service, Gradio UI, Dockerfile (optional).
* `export/` Excel/CSV templates and tests.
* `docs/` Architecture.md, LICENSES.md, CONTRIBUTING.md, README.md.

## 15. Tools and Stack (free)

* Models: Phi-3, Mistral 7B, Qwen2-7B; embeddings BGE/E5/GTE/INSTRUCTOR; rerankers as above.
* Ingest/Parse: GROBID, trafilatura, pdfminer.six, PyPDF2.
* Index: FAISS; orchestration in LlamaIndex or LangChain.
* Fine-tune: PEFT, TRL, bitsandbytes, LLaMA-Factory or Axolotl.
* Serve: vLLM or Ollama; FastAPI; Gradio; Docker (optional).
* Eval: pytest, scikit-learn, pandas, plotnine/matplotlib; textstat for readability.

## 16. Assumptions and Dependencies

* Local machine with ≥16GB RAM; optional single GPU 12–24GB for faster embedding/fine-tune; CPU-only path supported with longer runtimes.
* Internet required only for initial acquisition; offline thereafter.
* Jurisdiction: US; treat US government works as public domain; still cite.

## 17. Risks and Mitigations

* **License drift**: sources change or retract permissions → pin versions, store snapshots, continuous license checks.
* **Hallucination**: enforce low temperature, tight RAG, schema validator, citation requirement, refusal policy when evidence absent.
* **Data quality**: poor PDFs → GROBID failure → retry with alternate parsers; manual review queue.
* **Latency**: fallback to smaller models; enable reranker only when beneficial.
* **Scope creep**: lock MVP; change control before expanding to non-English or specialized populations.

## 18. Change Control

* GitHub Issues + PRs, semantic versioning, CODEOWNERS for protected files, ADRs for architectural decisions.

## 19. Documentation and Handoff

* Comprehensive README, quickstart, eval reproduction guide, and deployment steps.
* Usage examples with sample personas and expected outputs.

## 20. Maintenance

* Weekly crawl job; incremental re-embedding for changed docs only.
* Quarterly model retest on benchmark; update winner if metrics improve.

## 21. Intellectual Property and Licensing

* Project under Apache-2.0 or MIT.
* Respect third-party licenses; attach ATTRIBUTION file for CC-BY sources.
* Disallow redistribution of raw third-party texts where prohibited; distribute embeddings/indices only when licenses allow.

## 22. Acceptance Testing

* Run `make evaluate` to generate the benchmark report.
* Verify acceptance thresholds in Section 4.
* Manual spot-check: 10 generated plans must include valid citations resolvable to sources; Excel opens and matches JSON contract.

## 23. Go/No-Go Criteria

* All acceptance metrics met.
* No critical license violations in audit.
* Validator passes ≥98% of personas without hard failures.

## 24. Appendix — Canonical JSON Schema (Draft)

```
Document {
  doc_id: string,
  license: enum[CC-BY, CC-BY-SA, CC0, PD, ...],
  type: enum[trial, review, guideline, plan],
  population: {age: [min,max], sex: enum[M,F,All], trained_status: enum[novice,intermediate,trained]},
  goal: array[hypertrophy|strength|fat_loss|endurance],
  protocol: { exercises: array[ {name, muscles[], sets, reps, intensity, rest, tempo, notes} ], frequency, duration_weeks, progression },
  nutrition: { kcal: number|range, protein: string, carb: string, fat: string, timing: string, supplements[] },
  evidence: { design, n, outcomes[], effect_direction },
  citations: array[{ doi, title, url, section }]
}
```
