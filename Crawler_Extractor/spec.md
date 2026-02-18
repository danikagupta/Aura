# Paper Discovery & Scoring Product Specification

## Goal & Scope
Rebuild the crawler as a clean-room Streamlit application that curates a topic-specific paper graph. Users operate everything through the UI, yet every action is backed by callable Python functions so the same workflows can run headless or via tests. The Supabase schema (tables + storage buckets) remains as-is; all other code is rewritten. The system must:

- Accept seed PDFs (state `PDF Available`) and store binaries/text in Supabase Storage with references in Supabase tables.
- Execute a deterministic multi-stage pipeline: text extraction → scoring → citation expansion → PDF acquisition for references.
- Recursively ingest new citations by inserting rows marked `PDF Not Available` until their binaries are fetched.
- Provide a Streamlit dashboard to inspect pipeline counts/health and trigger “process N” batches while remaining decoupled from business logic.
- Support a Gemini-specific PGX flow in `gpapers` where `gstatus` is managed independently from `papers.status`.

## Canonical States & Lifecycle
We operate on rows stored in the existing Supabase schema (see `supabase_schema.sql`). Every paper moves through the following states:

1. `PDF Available`: Raw PDF uploaded, awaiting text extraction.
2. `Text Available`: Extraction completed and text stored; ready for scoring.
3. `Scored`: Scoring + reasoning persisted for papers that met or exceeded the citation threshold; eligible for citation expansion.
4. `Processed (Low Score)`: Scoring completed but result fell below the threshold; terminal state with no citation expansion.
5. `PDF Not Available`: Citation record inserted but binary missing; acquisition service searches for the PDF.
6. `Processed`: Citation expansion completed for a high-scoring paper; terminal success state.
7. `Failed`: Terminal state for unrecoverable errors (corrupt PDF, missing text, etc.).
8. `P1`: PGX extraction queue state. Rows in this state are eligible for sample-level PGX extraction.
9. `P1Success`: PGX extraction completed and rows persisted to `public.pgx_extractions`.
10. `P1Failure`: PGX extraction failed (parse/model/storage error) and reason captured on the paper row.
11. `gpapers.gstatus` mirrors the PGX queue lifecycle (`P1` → `P1WIP` → `P1Success`/`P1Failure`) without mutating `gpapers.status`.

State transitions are single-responsibility functions so each stage can be tested in isolation and re-run idempotently. Every paper row also carries a `seed_number` supplied during seed upload so downstream placeholders inherit the same identifier as their parent.

## Pipeline Stages
1. **Seed Upload (Streamlit → Storage Gateway)**
   - User uploads PDF. Title is auto-derived as `SEED: <filename>`. `upload_seed_pdf()` writes to Supabase storage, records a `PDF Available` row, and stores `pdf_md5`.
2. **Text Extraction (`src/parsers/text_extractor.py`, `src/pipelines/text_stage.py`)**
   - `TextExtractor.extract()` downloads the PDF, extracts normalized text, stores it, and updates the row to `Text Available` with a text pointer. Batch runners call `process_text_extraction_batch()`.
3. **Scoring (`src/parsers/scoring_engine.py`, `src/pipelines/scoring_stage.py`)**
   - `score_text()` loads the text, invokes the rubric/LLM, stores score + reasoning, and marks `Scored` for above-threshold papers and `Processed (Low Score)` for those that fell short. Batch runners call `process_scoring_batch()`.
4. **Citation Expansion (`src/parsers/citation_extractor.py`, `src/pipelines/citation_stage.py`)**
   - `extract_citations()` runs the LangChain structured-output agent on high-scoring papers. Each citation becomes a `PDF Not Available` row with parent linkage and raw text stored in metadata. The parent paper is moved to `Processed`. Batch runners call `process_citation_batch()`.
5. **PDF Acquisition (`src/ingest/pdf_acquisition.py`, `src/pipelines/pdf_acquisition_stage.py`)**
   - `fetch_pdf()` (only for `PDF Not Available`) uses the Google Search API + downloader to locate a PDF, uploads it, and flips the row to `PDF Available`. If no PDF is found, the row remains pending and the stage returns a “not found” outcome (no DB metadata change). Batch runners call `process_pdf_acquisition_batch()`.
6. **PGX Extraction (`src/parsers/pgx_extractor.py`, `src/pipelines/pgx_extraction_stage.py`)**
   - `process_pgx_extraction()` (only for `P1`) downloads the PDF from `source_uri`, extracts text, runs structured LLM extraction for sample-level pharmacogenomic rows, writes rows into `public.pgx_extractions`, and flips status to `P1Success`. Errors or empty extraction results move rows to `P1Failure`.
7. **Gemini PGX Extraction (`src/pipelines/gpapers_sync.py`, `pages/13_gemini_pgx_extraction.py`)**
   - `sync_gpapers_from_papers()` copies `papers` rows where `status ∈ {P1, P1Success, P1Failure, P1WIP}` into `public.gpapers` and resets `gstatus='P1'`.
   - The Gemini PGX page reuses the PGX batch stage against `PaperRepository(table="gpapers", state_column="gstatus")` and `StructuredGeminiPgxExtractor`, so transitions apply to `gstatus` and extraction rows are inserted into `public.pgx_gextractions` (separate from OpenAI `public.pgx_extractions`).

Additional extraction behavior:
- During text extraction, hyperlinks embedded in PDFs are captured and inserted as `PDF Not Available` placeholders with `url:`-prefixed titles so operators can process linked sources alongside cited papers.

Core stage logic lives in `src/pipelines/processing_steps.py` and returns `StageOutcome` objects so UI pages can orchestrate batches without re-implementing persistence rules.

## Architecture & Module Map
- `src/common/`: DTOs (PaperRecord, CitationRecord, StageOutcome), config loading, logging helpers, Supabase client bootstrap, shared errors.
- `src/ingest/`: PDF acquisition adapters (Google Search client, HTTP downloader) plus the storage gateway that uploads/reads blobs from Supabase Storage.
- `src/parsers/`: PDF text extractor and citation LLM agent, both exposing pure functions with injectable dependencies for testability.
- `src/parsers/`: PDF text extractor, citation agent, and PGX extractors for both OpenAI and Gemini backends.
- `src/pipelines/`: Orchestrators (`process_pipeline_batch`), batch runners (`process_*_batch`), and deterministic single-row processors (`PaperProcessor`, `ProcessingHarness`). Pipelines never import Streamlit.
- `streamlit_app.py`: Thin overview page that introduces the pipeline; Streamlit feature pages live under `pages/`.
- `pages/`: Seed upload, status dashboard, targeted/parallel processing, export tools, and diagnostics.
- `pages/`: Seed upload, status dashboard, targeted/parallel processing, PGX extraction pages (OpenAI + Gemini), export tools, and diagnostics.
- `tests/`: Mirrors module tree with coverage across core pipeline and helper modules, plus focused UI helper tests (e.g., export helpers).
- Persistent metadata in `public.papers` includes `pdf_md5`, the MD5 checksum of the stored PDF bytes. Populate it whenever a PDF is written so downstream tooling can detect duplicate binaries without downloading them again.

Patterns applied:
- Service-wired pipeline orchestrator governs batch execution (`process_pipeline_batch`).
- Adapter-Based Ingest Layer powers storage + Google Search integrations.
- Document Parsing Micro-Kits encapsulate text extraction and citation agents.
- Observability Control Plane standardizes structured logs/metrics emitted from each module.

## UI Requirements (Decoupled from Logic)
- Display pipeline summaries (counts by status/level/seed and score buckets) and surface row-level identifiers when executing stages.
- Provide upload widget calling `upload_seed_pdf()` and surfacing its return payload.
- Offer controls to process `N` papers and to fetch PDFs for citations; pages orchestrate using `ProcessingHarness`, `PaperProcessor`, and the batch runners in `src/pipelines/`.
- Streamlit session state only stores UI hints (selected threshold, batch size). All business state lives in Supabase and callable modules so headless runs can invoke the same functions in tests.
- The Seed Upload workflow lives on its own Streamlit page and prompts for a `seed_number` alongside the PDF so we can persist that number on the parent row and propagate it to derived citations.
- The Targeted Processing page accepts a leading `seed_number` filter so operators can scope both the summary counts and stage execution to a single lineage.
- The Parallel Processing page launches PDF acquisition, text extraction, scoring, and citation expansion simultaneously for a specific seed/level, letting operators process a batch across every stage with one click.
- The pipeline status dashboard exposes counts grouped by `seed_number` in addition to the level/status grid.
- Exporting PDFs writes to `output/<seed-number>/<level>/<score>/...` so copies are organized per seed lineage instead of just level/score.
- A dedicated **Process PGX Extraction** page lets operators enter a file count and process that many `P1` rows in one run.
- A dedicated **Gemini PGX Extraction** page provides two controls: sync `gpapers` from `papers` P1 statuses, then process `gpapers.gstatus = P1` rows using Gemini.

## Testing & Quality Gates
- `pytest --cov=src --cov-report=term-missing` must stay ≥85 %.
- `ruff check src tests` and `black src tests` run clean.
- Test helpers live in `tests/conftest.py` and `tests/pipelines/fakes.py`, with coverage across `tests/common`, `tests/ingest`, `tests/parsers`, `tests/pipelines`, and `tests/ui`.
- Unit tests cover the core pipeline steps and helpers (seed upload, scoring/citation extraction, storage, and orchestration). UI pages rely on these underlying modules; `src/ui/export_helpers.py` has direct tests.

## Implementation Guardrails
- Core stage logic lives in small, testable helpers; UI pages can be more verbose to support interactive workflows.
- Fail fast: do not wrap broad try/except blocks; only catch exceptions when we can continue processing other items safely and emit diagnostics.
- Streamlit pages orchestrate via `src/pipelines` and `src/parsers` modules so core logic remains usable headlessly.
- Supabase schema is authoritative; no migrations are introduced unless the DB team updates `supabase_schema.sql`.
- All secrets (Supabase, Google Search, LLM) are read from environment variables or Streamlit secrets; never hard-code credentials.
- Gemini extraction uses `GOOGLE_API_KEY`/`GOOGLE_SEARCH_API_KEY` and optional `GEMINI_MODEL` (defaults to `gemini-2.5-flash`).
- Processing attempts are tracked per row (`attempts` column). Start at zero, increment before each stage run, reset on state transitions, and skip rows when attempts are greater than two (so the third attempt is the last one executed).

## Roadmap / Next Steps (Optional)
1. Expand prompt mappings for additional `seed_number` values under `prompts/` and `src/common/prompt_config.py`.
2. Add integration-style smoke tests that exercise the full pipeline with mocked Supabase services.
3. Polish the status and targeted processing pages to surface richer diagnostics (e.g., per-stage error summaries).
