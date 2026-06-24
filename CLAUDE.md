# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
pytest tests/ -v

# Run a single test file
pytest tests/test_pipeline_v2.py -v

# Run a single test
pytest tests/test_store.py::TestCompareRuns::test_detects_null_count_increase -v

# Lint
flake8 pipelinehub/

# Format
black pipelinehub/ tests/

# Build distribution
python -m build

# Bump patch version (CI does this automatically on merge to main)
bump-my-version bump patch
```

## Architecture

Four modules in `pipelinehub/`, each with one responsibility:

**`pipeline.py` — DataPipeline (public API)**
The only class users interact with. `execute()` has two paths: `debug=False` runs steps bare (v0.1 behaviour, raises `RuntimeError`); `debug=True` (default) runs through the snapshot engine, raising `PipelineStepError` on failure. The engine calls `DataProfiler`, writes to `RunStore`, and runs anomaly detection after each step.

**`profiler.py` — DataProfiler (data fingerprinting)**
Captures typed snapshots of any Python object. Imports pandas, polars, and numpy lazily inside methods using `contextlib.suppress(ImportError)` — never fails if they're not installed. For Polars dtype detection, uses string-based `_is_polars_numeric()` (not `isinstance`) for cross-version compatibility.

**`store.py` — RunStore (SQLite persistence)**
Writes run history to `.pipelinehub/runs.db`. For `:memory:` paths (used in tests), keeps a single persistent connection via `self._persist_conn`; for file paths, opens/closes per operation via `_get_conn()` context manager. The `row_factory` is saved and restored around every query to avoid mutating the shared connection.

**`errors.py` — PipelineStepError**
Inherits from `RuntimeError` (not just `Exception`) so v0.1 callers using `except RuntimeError` still catch it. Contains the full snapshot dict of data entering the failing step.

## Key Constraints

- **Zero external dependencies.** stdlib only. pandas/polars/numpy are optional — all imports are lazy and guarded.
- **Python >=3.7** — no walrus operators, no `match` statements, no 3.8+ syntax.
- **SQLite schema** lives in `store.py::_SCHEMA` (3 tables: `runs`, `step_snapshots`, `failures`). Changes require a migration strategy.
- **Version** is tracked in both `pyproject.toml` and `pipelinehub/__init__.py`. Both are updated by `bump-my-version` — never edit manually.
- **Tests use `db_path=":memory:"`** — never write to disk in tests. Legacy tests in `test_pipeline.py` also pass `db_path=":memory:"`.

## CI / Publishing

- **CI workflow** (`.github/workflows/ci.yml`): runs `pytest tests/ -v` on every PR to `main`.
- **Publish workflow** (`.github/workflows/publish.yml`): triggers on merge to `main`, bumps patch version, commits back with `[skip ci]` in the message (prevents re-trigger), builds wheel + sdist, publishes to PyPI via OIDC Trusted Publisher. Uses `PAT_TOKEN` secret for the push step to bypass branch protection.
- **Branch protection**: main is protected via GitHub Ruleset — all changes require a PR. Repo admin role bypasses for the CI bot.
