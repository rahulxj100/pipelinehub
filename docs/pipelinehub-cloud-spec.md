# pipelinehub-cloud — Product Spec

## Context

**pipelinehub** is an open-source Python library (github.com/rahulxj100/pipelinehub, PyPI: 1,000+ installs) that adds automatic debugging to data pipelines. It captures snapshots of data at every pipeline step — shape, nulls, dtypes, statistics — stores run history in local SQLite (`.pipelinehub/runs.db`), detects anomalies, and surfaces rich failure context.

**pipelinehub-cloud** (github.com/rahulxj100/pipelinehub-cloud, private) is the commercial product built on top. It replaces local SQLite with a cloud backend, adds a web dashboard, team sharing, alerts, and billing. This is an open-core model: the library stays free and open source forever; the cloud product is what teams pay for.

---

## Business Model

- **Free tier** — OSS library, local SQLite, single user (today's v0.2)
- **Team tier** — $49/seat/month — cloud dashboard, shared run history, anomaly alerts, 90-day retention
- **Business tier** — $99/seat/month — unlimited retention, SSO, audit logs, priority support

Primary buyer: data team lead / engineering manager at a mid-size company.

Primary value proposition: faster pipeline debugging — quantifiable as "hours saved per engineer per week."

---

## How the Library Connects to Cloud

When a user has a cloud API key set, pipelinehub sends run data to the cloud instead of (or in addition to) writing to local SQLite. The library already has `RunStore` as the persistence layer — cloud sync is a drop-in replacement or parallel write.

Configuration (user sets one env var or passes to constructor):

```python
# Option A: env var
export PIPELINEHUB_API_KEY=ph_live_xxxx

# Option B: constructor
pipeline = DataPipeline(name="my-pipeline", api_key="ph_live_xxxx")
```

When `api_key` is set, `RunStore` sends snapshots to `https://api.pipelinehub.cloud` via HTTPS POST. Local SQLite still writes as a cache/fallback.

---

## Four Subsystems (build in this order)

### 1. Library → Cloud Sync (build first)

**What:** Modify `RunStore` in the OSS library to optionally POST run data to a cloud endpoint alongside the local SQLite write.

**Interface:**
- `RunStore(db_path=".pipelinehub/runs.db", api_key=None, api_url="https://api.pipelinehub.cloud")`
- When `api_key` is set, every `start_run`, `save_step`, `save_failure`, `finish_run` call also fires an async HTTP POST to the cloud
- Failures are silent — cloud sync errors must never crash the user's pipeline
- Use `urllib.request` (stdlib) — no new dependencies

**Endpoints the library calls:**
```
POST /v1/runs              — start_run
POST /v1/runs/{id}/steps   — save_step
POST /v1/runs/{id}/failure — save_failure
PATCH /v1/runs/{id}        — finish_run
```

**Auth:** `Authorization: Bearer ph_live_xxxx` header on every request.

---

### 2. Backend API

**What:** HTTP API that receives run data from the library, stores it, and serves the dashboard.

**Tech stack recommendation:** FastAPI + PostgreSQL + Redis (for rate limiting). Deploy on Railway or Render for MVP speed — migrate to AWS later.

**Key endpoints:**
```
POST   /v1/runs
POST   /v1/runs/{id}/steps
POST   /v1/runs/{id}/failure
PATCH  /v1/runs/{id}
GET    /v1/runs?pipeline_name=&limit=&status=
GET    /v1/runs/{id}
GET    /v1/pipelines
GET    /v1/anomalies?since=
```

**Auth:** API key lookup table. Keys prefixed `ph_live_` for production, `ph_test_` for test mode.

**Multi-tenancy:** Every row tagged with `org_id`. API key resolves to `org_id` on every request.

---

### 3. Web Dashboard

**What:** Single-page app showing pipeline run history, step snapshots, anomaly timeline, and run diffs.

**Tech stack recommendation:** Next.js + Tailwind + shadcn/ui. Deploy on Vercel.

**Core screens:**
1. **Runs list** — table of recent runs per pipeline, status, duration, anomaly count
2. **Run detail** — step-by-step breakdown, before/after snapshots, anomaly flags
3. **Run diff** — side-by-side comparison of two runs, null/dtype/mean changes highlighted
4. **Anomaly feed** — chronological list of anomalies across all pipelines
5. **Settings** — API key management, team members, billing

---

### 4. Auth + Billing

**Auth:** Clerk (simplest integration with Next.js) or Auth0. Email + Google SSO. Org-based — one org per company, multiple users per org.

**Billing:** Stripe. Seat-based. Free tier = 1 seat, no credit card. Team tier = per-seat metered. Stripe Checkout for self-serve, Stripe invoicing for enterprise.

**API key management:** User generates keys in the dashboard. Keys are hashed before storage (show once on creation, never again).

---

## Current OSS Library State (v0.2)

Repo: `github.com/rahulxj100/pipelinehub`

Key files:
- `pipelinehub/pipeline.py` — `DataPipeline` class, public API
- `pipelinehub/profiler.py` — `DataProfiler`, captures typed snapshots
- `pipelinehub/store.py` — `RunStore`, SQLite persistence (this is what gets extended for cloud sync)
- `pipelinehub/errors.py` — `PipelineStepError(RuntimeError)`

`RunStore` schema (SQLite):
```sql
runs (run_id, pipeline_name, started_at, finished_at, status, total_steps)
step_snapshots (id, run_id, step_name, step_index, snapshot_before, snapshot_after, duration_seconds)
failures (id, run_id, step_name, step_index, snapshot_before, exception_type, exception_message)
```

Snapshots are JSON-serialised dicts with shape:
```json
{
  "step_name": "normalize",
  "stage": "before|after",
  "dtype": "dataframe|sequence|array|dict|generic",
  "timestamp": "2025-09-16T10:23:45.123456",
  "profile": { ... type-specific keys ... }
}
```

---

## Where to Start

**Start with Subsystem 1 (Library → Cloud Sync).**

Reason: without the library sending data to the cloud, Subsystems 2, 3, and 4 have nothing to work with. The sync layer is also the simplest to build and test in isolation — mock the cloud endpoint, run the existing test suite, confirm data flows correctly.

The first PR should be: `RunStore` accepts `api_key` param, fires async HTTP POSTs to configurable endpoint, all failures suppressed silently, existing tests unchanged, new tests use `responses` or `unittest.mock` to mock the HTTP calls.

---

## Constraints

- Library must remain zero external dependencies (stdlib only) — cloud sync uses `urllib.request` and `threading`, not `httpx` or `requests`
- Cloud sync failures must never raise or slow down the user's pipeline — fire-and-forget with a background thread
- Python >=3.7 compatibility required in the library
- Cloud backend has no such constraint — use whatever stack is fastest to ship
