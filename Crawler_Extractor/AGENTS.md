# Repository Guidelines

## Project Structure & Module Organization
Keep executable crawler modules in `src/`, grouped by responsibility: `src/ingest/` for downloaders, `src/parsers/` for PDF/text parsing, and `src/pipelines/` for orchestration (entry point `src/main.py`). Shared DTOs and Supabase helpers belong in `src/common/`. Configuration YAML files live in `configs/` and are loaded via `python src/main.py --config configs/default.yml`. Persisted artifacts (sample PDFs, cached JSON, prompt templates) stay in `data/` or `assets/prompts/`, never in `src/`. Tests mirror the tree under `tests/`, e.g., `tests/parsers/test_pdf_splitter.py`.

## Build, Test, and Development Commands
Install tools with `uv pip install -r requirements.txt` (preferred over pip for reproducible, cached resolution) after creating a virtual env via `python -m venv .venv && source .venv/bin/activate`. Use `python src/main.py crawl --config configs/dev.yml` to run the crawler locally, and `python scripts/replay_job.py --job-id <id>` for smoke replays. Run quality gates via `ruff check src tests`, `black src tests`, and `pytest -q`. `make ci` (or `./scripts/ci.sh`) chains lint, type-check, and tests for pre-push validation.

## Coding Style & Naming Conventions
Use 4-space indentation, type hints on public functions, and descriptive snake_case names for modules, functions, and Supabase columns (`processed_at`, `source_url`). Prefer dataclasses for structured payloads. Keep logging structured (`logger.info("parsed", extra={"doc": doc_id})`) and guard network calls behind adapters. Do not commit secrets; reference environment keys as `os.environ["SUPABASE_KEY"]`. Import all required dependencies directly at module load—do not wrap imports in `try/except ImportError`; missing libraries should crash early so we notice misconfigured environments and avoid running with half-configured tooling. When calling LLMs, follow the LangChain structured-output pattern:
`client = ChatOpenAI(...); client.with_structured_output(MySchema).invoke([SystemMessage(...), HumanMessage(...)])` so downstream logic never parses free-form strings.

## Testing Guidelines
All new logic needs pytest coverage with fixtures under `tests/fixtures/`. Name test modules `test_<module>.py` and use scenario-focused test names. Hit ≥85% coverage by running `pytest --cov=src --cov-report=term-missing`. Mock Supabase or HTTP calls with `responses` to keep suites offline. Integration runs belong in `tests/integration/` and can be toggled via `PYTEST_ADDOPTS="--integration"`.

## Commit & Pull Request Guidelines
Follow Conventional Commits (`feat: add pdf table extractor`, `fix: guard empty page tokens`). Keep commits focused and update `spec.md` whenever the pipeline surface area changes (new ingestion sources, schema fields, or config flags). Pull requests must include: scope summary, test evidence (`pytest` output or screenshots for UI helpers), linked Linear/GitHub issues, and a confirmation that `spec.md` reflects the new behavior. Request review from at least one maintainer familiar with the touched pipeline, and ensure CI passes before marking ready for merge.

## Planning Expectations
Any major change (new modules, schema shifts, dependency upgrades) must begin with a plan: share a short step-by-step outline in your PR description or discussion thread, get buy-in, and then execute. Use the plan mode workflow in Codex to track progress, updating steps as you complete them so reviewers can follow along.

## Security & Configuration Tips
Keep `.env.local` out of version control and document required keys in `configs/README.md`. When sharing sample data, redact PHI/PII and store sanitized copies under `data/samples/`. Rotate Supabase service keys whenever onboarding new agents and revoke unused personal tokens immediately.
