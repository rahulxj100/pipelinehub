# pipelinehub TODO

## Profiler enhancements
- [ ] Add percentiles (p25, p50, p75) to numeric stats for dataframes and arrays
- [ ] Add cardinality (unique value count) per column for dataframes
- [ ] Add correlation matrix between numeric columns for dataframes

## Cloud (v0.3)
- [ ] Subsystem 1: Library → Cloud sync (RunStore posts to api.pipelinehub.cloud)
- [ ] Subsystem 2: Backend API (FastAPI + PostgreSQL)
- [ ] Subsystem 3: Web dashboard (Next.js + Tailwind + shadcn/ui)
- [ ] Subsystem 4: Auth + billing (Clerk + Stripe)

## Cloud — Monetization Products

### Tier Deepeners (justify seat price)
- [ ] Alerting: Slack/email/PagerDuty on pipeline failure or anomaly spike (Team tier)
- [ ] Run comparison UI: visual diff of two runs — schema changes, row drift, null spikes
- [ ] Data quality scoring: per-pipeline health score over time (Team tier)
- [ ] SLA tracking: define expected runtime, track breach history (Business tier)
- [ ] Incident timeline / post-mortems: auto-generate shareable markdown timeline from run history

### New Product Surfaces
- [ ] GitHub Actions integration: snapshot diff posted as PR comment (low priority — narrow market, dbt-style teams only)
- [ ] VS Code / Cursor extension: inline last run status next to pipeline definition
- [ ] CLI (`ph`): `ph runs list`, `ph runs diff`, `ph alerts` — free local, API key for cloud
- [ ] Scheduled pipeline monitor: alert if pipeline doesn't run on expected cron schedule
- [ ] Embeddable status badge: `![status](pipelinehub.io/badge/id)` for READMEs — viral distribution

### Ecosystem Integrations (Business tier / add-ons)
- [ ] dbt integration: sync post-run snapshots from dbt model tests
- [ ] Airflow / Prefect / Dagster connector: DAG-level run metadata into pipelinehub
- [ ] Webhook outbound: fire POST on run events for custom integrations

### AI / Intelligence Layer (premium)
- [ ] Root cause suggestion: LLM explains failure from snapshot diff (Business tier)
- [ ] Anomaly explanation: natural language summary of good-run vs bad-run diff
- [ ] Predictive failure: train on run history, flag likely-to-fail runs early

### Compliance / Enterprise
- [ ] Audit logs: who viewed what run, when — required for SOC 2
- [ ] Data masking in snapshots: redact PII columns before leaving client (GDPR)
- [ ] On-premise / self-hosted: deploy backend in customer VPC, annual license
- [ ] Role-based access: read-only viewers, pipeline owners, admins

## Error output
- [ ] Surface element_type and mixed-type flag in PipelineStepError for sequences
