# PipelineHub

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/pipelinehub.svg)](https://pypi.org/project/pipelinehub/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/pipelinehub?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/pipelinehub)
[![CI](https://github.com/rahulxj100/pipelinehub/actions/workflows/ci.yml/badge.svg)](https://github.com/rahulxj100/pipelinehub/actions/workflows/ci.yml)

**Python pipelines with automatic debugging built in.**

```bash
pip install pipelinehub
```

---

## The problem

Your pipeline fails at 2am. The stack trace points to step 9. The actual problem happened in step 4 — a column silently changed dtype and nobody caught it until three steps later.

You spend the next four hours adding print statements, re-running on stale data, and guessing. I built this after doing exactly that, more times than I want to count.

---

## What it does

pipelinehub captures your data at every step — shape, nulls, dtypes, statistics — without any configuration. When something breaks, you see what the data looked like going *into* the failing step, not just which line exploded.

```python
from pipelinehub import DataPipeline

pipeline = DataPipeline(name="ml-pipeline")
pipeline.add_step(clean_data, "clean")
pipeline.add_step(feature_engineer, "features")
pipeline.add_step(normalize, "normalize")

result = pipeline.execute(df)
```

When a step fails:

```
PipelineStepError: Step "normalize" (step 3 of 3) failed

Data entering this step:
  type:    dataframe
  shape:   (10420, 8)
  nulls:   col_price: 142
  dtypes:  col_id: object  col_price: float64

Original error: TypeError: unsupported operand type(s) for +: 'float' and 'str'

To replay from this step:
  pipeline.replay_from("normalize", your_data)
```

---

## Why not an AI assistant?

An AI assistant needs you to notice something is wrong, copy the data, describe the problem, and ask. pipelinehub runs at 3am on a schedule — no human in the loop. It catches silent failures that never throw an exception: null counts that creep up, dtypes that silently change, row counts that drop 60% in one step.

---

## Why not Kedro or Airflow?

Kedro requires a new project structure, a CLI, a data catalog, and YAML configs. Reasonable for a large team running production ML. Overkill if you're preprocessing data in a notebook or a script and just want visibility into what's happening at each step.

Airflow is an orchestrator. It schedules and monitors jobs — it doesn't help you understand what's wrong inside one.

pipelinehub is a library, not a framework. It adds one import and one method call to code you already have.

---

## Quick start

```python
from pipelinehub import DataPipeline

pipeline = (DataPipeline(name="my-pipeline")
    .add_step(lambda x: [i for i in x if i > 0], "filter_positive")
    .add_step(lambda x: [i**2 for i in x], "square")
    .add_step(lambda x: [i/max(x) for i in x], "normalize"))

result = pipeline.execute([-2, -1, 0, 1, 2, 3, 4, 5])
```

Pass `debug=False` to skip snapshotting entirely — same behaviour as v0.1, no overhead:

```python
result = pipeline.execute(data, debug=False)
```

---

## Features

- **Automatic snapshots** — shape, dtypes, null counts, and numeric stats captured at every step, zero configuration
- **Rich failure context** — `PipelineStepError` shows exactly what the data looked like entering the failing step
- **Anomaly detection** — warns when null counts spike, dtypes change, rows drop more than 50%, or value distributions shift compared to the last run
- **Run history** — every run stored locally in `.pipelinehub/runs.db`, no setup required
- **Run comparison** — `pipeline.compare_runs()` diffs any two runs step by step
- **Replay from any step** — re-run forward from any named step without re-running everything before it
- **`ph` CLI** — inspect run history, diff runs, and watch live execution from the terminal
- **Fluent chaining** — method chaining works if you prefer that style
- **No external dependencies** — stdlib only; pandas, polars, and numpy are detected and profiled if installed
- **Full type hints** — works with IDE autocomplete

---

## Snapshot output

When anomalies are detected, pipelinehub prints a summary after `execute()`:

```
Pipeline completed  ✓  (2.3s)

  Step              Rows              Nulls          Duration
  ──────────────────────────────────────────────────────────
  clean             10500→10420       0→0            0.4s
  feature_engineer  10420→10420       0→142 ⚠        1.1s
  normalize         10420→10420       142→0          0.8s

⚠  col_price nulls introduced in step "feature_engineer" (+142)
```

### Inspect run history

```python
# Last run with all step snapshots
last = pipeline.last_run()

# Recent runs
pipeline.list_runs(limit=5)

# Compare last two runs
pipeline.compare_runs()

# Compare specific runs by ID
pipeline.compare_runs(run_id_a, run_id_b)
```

### Replay from a step

```python
# Fix normalize(), replay forward — skips clean and feature_engineer
result = pipeline.replay_from("normalize", your_data)
```

---

## `ph` CLI

`pip install pipelinehub` also installs the `ph` command. It reads from `.pipelinehub/runs.db` in your project — no auth, no setup.

```bash
# List recent runs
ph runs list

# Show last completed run
ph runs last

# Step-by-step detail for a specific run
ph runs show <run_id>

# Diff two runs
ph runs diff <run_id_a> <run_id_b>

# Watch a pipeline execute in real time (polls every 1s)
ph runs watch

# Health summary across all pipelines
ph status

# Aggregate stats — success rate, failure count
ph stats

# Check setup
ph doctor
```

Example output for `ph runs list`:

```
  ID         Pipeline               Started                Status       Steps
  ──────────────────────────────────────────────────────────────────────────
  a3f2c1b0   ml-pipeline            2026-06-24 09:12:03    ✅ success    4
  7d91e4a2   ml-pipeline            2026-06-23 22:47:11    ❌ failed     3
  c58b0f31   etl-daily              2026-06-23 06:00:02    ✅ success    6
```

---

## Examples

### Data cleaning

```python
from pipelinehub import DataPipeline

pipeline = (DataPipeline(name="cleaning")
    .add_step(lambda x: [float(i) for i in x if i is not None], "convert")
    .add_step(lambda x: [i for i in x if abs(i - sum(x)/len(x)) < 2.5], "remove_outliers")
    .add_step(lambda x: [(i - min(x)) / (max(x) - min(x)) for i in x], "normalize"))

result = pipeline.execute([1, 2, 3, None, 100, 4, 5, 6, 7, 8, 9])
```

### Text processing

```python
import re
from pipelinehub import DataPipeline

pipeline = (DataPipeline(name="text")
    .add_step(str.lower, "lowercase")
    .add_step(lambda t: re.sub(r'[^a-zA-Z0-9\s]', '', t), "clean")
    .add_step(str.split, "tokenize")
    .add_step(lambda words: sorted(set(w for w in words if len(w) >= 4)), "keywords"))

result = pipeline.execute("Hello World! This is a Sample Text for Processing...")
```

### Pipeline management

```python
pipeline = DataPipeline(name="example")
pipeline.add_step(lambda x: [i*2 for i in x], "double")
pipeline.add_step(lambda x: [i+1 for i in x], "add_one")

print(pipeline.get_steps())   # ['double', 'add_one']
print(len(pipeline))          # 2

pipeline.remove_step(0)
pipeline.clear_steps()
```

---

## LangChain / AI agent observability

```bash
pip install pipelinehub[langchain]
```

`AgentPipeline` + `PipelineHubCallbackHandler` instrument any LangChain chain or agent with zero code changes beyond adding the callback. Every LLM call, tool call, and chain step is recorded to local SQLite automatically.

```python
from pipelinehub import AgentPipeline
from pipelinehub.langchain import PipelineHubCallbackHandler

pipeline = AgentPipeline(name="research-agent")
handler = PipelineHubCallbackHandler(pipeline)

result = chain.invoke(
    {"input": "Summarise AI trends"},
    config={"callbacks": [handler]}
)
```

After each run:

```
[PipelineHub] Run complete — 2,847 tokens used across 4 steps.
              💰 Track cost trends over time → pipelinehub.cloud
```

**What gets tracked per run:**
- Every LLM call — model name, prompt tokens, completion tokens, latency, output preview
- Every tool call — tool name, input, output, latency
- Every chain step — chain type, latency, output preview

**Anomaly detection across runs** (automatic, no config):
- `token_spike` — total tokens >2× last run
- `latency_regression` — total duration >3× last run
- `tool_call_drift` — different tools called vs last run
- `tool_call_order_change` — same tools, different sequence
- `error_rate_spike` — >20% of steps errored

Works with OpenAI, Anthropic (Claude), and Google — token usage normalised automatically across all three providers.

---

## Airflow integration

```bash
pip install pipelinehub[airflow]
```

Attach `PipelinehubCallback` to any Airflow 2.x task. On success, it profiles the task's XCom output and records a snapshot. On failure, it records the exception.

```python
from pipelinehub.airflow_integration import PipelinehubCallback

ph = PipelinehubCallback(pipeline_name="my_dag")

@task(
    on_success_callback=ph.on_success,
    on_failure_callback=ph.on_failure,
)
def extract():
    return df
```

`pipeline_name` is optional — falls back to the DAG id. Each task callback creates its own run record in `.pipelinehub/runs.db`, so you get per-task history and XCom snapshot diffing across DAG runs.

---

## Roadmap

**v0.1** ✅ — Fluent pipeline chaining, zero dependencies, verbose mode  
**v0.2** ✅ — Automatic snapshot engine, rich failure context, run comparison, anomaly detection, replay, `ph` CLI  
**v0.3** ✅ — LangChain agent observability, Airflow integration, cloud sync  
**v0.4** — Web dashboard for run history and team visibility  

---

## Contributing

```bash
git checkout -b feature/your-feature
pytest tests/
git commit -m 'Add your feature'
# open a pull request against main
```

Branch protection is on `main` — all changes go through a PR.

---

## License

MIT — see LICENSE for details.

---

*Built by [Rahul Paul](https://github.com/rahulxj100)*
