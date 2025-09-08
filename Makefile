# Makefile — OpenStrength

PY ?= python
PIP ?= $(PY) -m pip
PACKAGE := openstrength

.PHONY: help venv install pre-commit format lint typecheck test ingest parse normalize \
        chunk embed index serve export evaluate train bench docs clean

.DEFAULT_GOAL := help

## —— Env & Tooling ——
venv: ## Create virtual environment
	$(PY) -m venv .venv
	@echo "Activate: source .venv/bin/activate  (Windows: .venv\\Scripts\\activate)"

install: ## Install project + dev deps
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

pre-commit: ## Install pre-commit hooks
	pre-commit install

format: ## Run formatters (Black + isort)
	black src tests
	isort src tests

lint: ## Ruff linter
	ruff check src tests

typecheck: ## mypy type checks
	mypy src

test: ## Run tests
	pytest

## —— Pipelines (stubs print TODO until modules exist) ——
ingest: ## Ingest open-access sources into data/raw
	$(PY) -m src.ingest.run --config configs/ingest/pmc.yaml || echo "TODO: implement src/ingest/run.py"

parse: ## Parse PDFs/HTML to structured TEI/JSON
	$(PY) -m src.parse.run --input data/raw --out data/staged || echo "TODO: implement src/parse/run.py"

normalize: ## Normalize to canonical schema (Parquet)
	$(PY) -m src.normalize.run --in data/staged --out data/curated/docs.parquet || echo "TODO: implement src/normalize/run.py"

chunk: ## Chunk documents into retrieval units
	$(PY) -m src.chunk.run --in data/curated/docs.parquet --out data/curated/chunks_science.parquet || echo "TODO: implement src/chunk/run.py"

embed: ## Compute embeddings and save vectors
	$(PY) -m src.embed.run --model bge-large-en-v1.5 --in data/curated/chunks_science.parquet --out artifacts/embeddings || echo "TODO: implement src/embed/run.py"

index: ## Build FAISS indices
	$(PY) -m src.index.build --emb artifacts/embeddings --out artifacts/indices || echo "TODO: implement src/index/build.py"

serve: ## Run FastAPI server (RAG endpoint)
	uvicorn src.serve.api:app --host 0.0.0.0 --port 8000 --reload || echo "TODO: implement src/serve/api.py"

export: ## Convert last plan JSON to Excel/CSV
	$(PY) -m src.export.excel --in artifacts/last_plan.json --out artifacts/exports/plan.xlsx || echo "TODO: implement src/export/excel.py"

evaluate: ## Run benchmark/evaluation suite
	$(PY) -m src.eval.run --config configs/eval/grid.yaml || echo "TODO: implement src/eval/run.py"

train: ## Optional: LoRA SFT training
	$(PY) -m src.train.lora --config configs/train/lora.yaml || echo "TODO: implement src/train/lora.py"

bench: ## Quick smoke benchmark
	$(PY) -m src.eval.smoke || echo "TODO: implement src/eval/smoke.py"

docs: ## Build docs (placeholder)
	@echo "TODO: add docs builder (e.g., mkdocs or mdBook)"

clean: ## Remove caches and artifacts
	find . -name "__pycache__" -type d -prune -exec rm -rf {} + || true
	rm -rf .mypy_cache .pytest_cache .ruff_cache build dist || true

## —— Help ——
help: ## Show this help
	@printf "\033[1mTargets:\033[0m\n"; \
	awk -F ':|##' '/^[a-zA-Z0-9._-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$NF }' $(MAKEFILE_LIST)
