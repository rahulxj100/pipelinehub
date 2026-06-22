# Changelog

## [0.2.0] - unreleased

### Added
- Automatic snapshot engine: captures data shape, dtypes, null counts,
  and numeric statistics at every pipeline step with zero configuration
- SQLite-backed run history stored in `.pipelinehub/runs.db`
- Rich failure context: `PipelineStepError` surfaces exactly what the
  data looked like when a step failed
- Run comparison: `pipeline.compare_runs()` diffs any two runs step by step
- Anomaly detection: automatic warnings when null counts spike, dtypes
  change, or value distributions shift significantly vs last run
- `pipeline.replay_from(step_name, data)`: re-run pipeline from any step
- `pipeline.last_run()`: inspect the most recent run
- `pipeline.list_runs()`: list run history
- `debug=False` flag on `execute()` to opt out of snapshotting entirely

### Changed
- `DataPipeline.__init__` now accepts optional `name` and `db_path` params
- `execute()` now prints anomaly warnings automatically

### Backward compatibility
- All v0.1 public methods unchanged
- Existing code requires no modifications to benefit from snapshotting

## [0.1.0] - initial release

- Initial `DataPipeline` with `add_step`, `remove_step`, `clear_steps`,
  `get_steps`, `set_data`, and `execute`
