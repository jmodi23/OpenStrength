# OpenStrength — Project Task Checklist

This checklist is derived from the SOW and broken down into actionable steps. Use it to track project progress by crossing off tasks as they’re completed.

---

## 0. Repository and Environment

* [ ] Initialize repo with license, CODEOWNERS, CONTRIBUTING.md, SECURITY.md
* [ ] Add .editorconfig, .gitattributes, pyproject.toml, pre-commit hooks
* [ ] Create Makefile targets (install, ingest, parse, normalize, chunk, embed, index, serve, export, evaluate, train, bench, lint, test, docs)
* [ ] Create Python environment, pin dependencies
* [ ] Optional Docker setup (CPU + GPU)
* [ ] Define project structure (data/, artifacts/, src/, configs/, docs/, tests/)

## 1. Corpus Acquisition & Licensing

* [ ] Define allowed licenses (SPDX mapping)
* [ ] Implement license gate (fail-closed)
* [ ] Fetch PMC OA subset, filter by license
* [ ] Crawl WHO/NIH/CDC/USDA docs
* [ ] Fetch bioRxiv/medRxiv (permissive only)
* [ ] Harvest institutional repositories via OAI-PMH
* [ ] Collect CC-licensed GitHub plans
* [ ] Implement snapshotting and hashing
* [ ] Enforce robots.txt compliance

## 2. Parsing & Extraction

* [ ] Deploy GROBID service
* [ ] Convert PDFs to TEI
* [ ] Extract HTML with trafilatura
* [ ] Extract sections and tables
* [ ] Parse plan repo files (YAML/Markdown)
* [ ] Normalize DOIs and PubMed IDs

## 3. Normalization

* [ ] Define Pydantic schema for documents
* [ ] Normalize units (kg/lb, kcal, macros)
* [ ] Standardize taxonomies (exercises, goals)
* [ ] Populate schema fields (population, protocol, nutrition, citations)
* [ ] Save curated docs as Parquet

## 4. Deduplication & Chunking

* [ ] Language filtering (English)
* [ ] Near-duplicate detection (MinHash/LSH)
* [ ] Apply quality gates (drop low-value chunks)
* [ ] Chunk into 200–600 tokens with metadata
* [ ] Assign deterministic chunk IDs

## 5. Embeddings & Index

* [ ] Implement embedding backends (BGE, E5, GTE, INSTRUCTOR)
* [ ] Batch embedding pipeline
* [ ] Save embeddings + metadata
* [ ] Build FAISS indices (ScienceIndex, PlanIndex)
* [ ] Integrate rerankers (cross-encoders)

## 6. RAG Pipeline

* [ ] Build query generator
* [ ] Retrieve from ScienceIndex & PlanIndex
* [ ] Optional reranking
* [ ] Prompt templates with schema + citation rules
* [ ] JSON validation and repair
* [ ] Constraint validator integration

## 7. Exporters

* [ ] Convert plans to DataFrames
* [ ] Export nutrition, progression, citations
* [ ] Generate Excel with multiple sheets
* [ ] CSV fallback export
* [ ] Unit tests for exports

## 8. Base LLM Serving

* [ ] Integrate Phi-3, Mistral 7B, Qwen2-7B with Ollama/vLLM
* [ ] Unified generation client (timeouts, retries)
* [ ] Model registry config

## 9. Benchmarking

* [ ] Build Retrieval QA dataset (1–2k items)
* [ ] Build persona Plan Tasks (200+)
* [ ] Build Citation Set
* [ ] Implement metrics (Recall, nDCG, MRR, citation precision)
* [ ] Run grid search (LLM × Embedding × Reranker)
* [ ] Capture latency, tokens, memory
* [ ] Select best configuration

## 10. Plan Validator

* [ ] Encode weekly volume bounds
* [ ] Intensity caps by status
* [ ] Frequency and rest rules
* [ ] Progression monotonicity
* [ ] Nutrition guardrails
* [ ] Contraindication checks
* [ ] Pytest coverage

## 11. LoRA Fine-Tuning (Optional)

* [ ] Generate instruction pairs from RAG
* [ ] Human curation tool
* [ ] Assemble JSONL dataset
* [ ] Train LoRA (PEFT, TRL, bitsandbytes)
* [ ] Evaluate fine-tuned model

## 12. API & UI

* [ ] Define FastAPI request/response models
* [ ] Implement /plan endpoint
* [ ] Implement /evaluate endpoint
* [ ] Build Gradio UI
* [ ] Add input sanitization

## 13. Observability & Security

* [ ] Structured logging (retrieval, latencies)
* [ ] Unit & integration tests
* [ ] Static analysis and audits
* [ ] PII handling and redaction
* [ ] Reproducibility (seeds, model hashes)

## 14. Documentation

* [ ] Write README.md
* [ ] Add Architecture.md
* [ ] Add Evaluation.md
* [ ] Add LICENSES.md & ATTRIBUTION.md
* [ ] Create ADRs for design choices

## 15. CI/CD

* [ ] GitHub Actions for lint/tests
* [ ] Docker build & push workflow
* [ ] Release workflow with eval report

## 16. Maintenance

* [ ] Weekly crawl job
* [ ] Incremental re-embedding
* [ ] Quarterly benchmark rerun
* [ ] Deprecation policy + migration scripts

## 17. Go/No-Go Criteria

* [ ] Run full evaluation suite
* [ ] Manual audit of 10 generated plans
* [ ] License audit (sample 100 docs)
* [ ] Confirm all Success Criteria met

---
