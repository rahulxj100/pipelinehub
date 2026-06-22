"""
Core DataPipeline class — v0.2 with transparent snapshot engine.
"""

import datetime
import time
from typing import Any, Callable, Dict, List, Optional

from pipelinehub.errors import PipelineStepError
from pipelinehub.profiler import DataProfiler
from pipelinehub.store import RunStore


class DataPipeline:
    """
    A flexible data pipeline with automatic snapshot engine.

    All v0.1 public methods are unchanged. New behaviour is added via
    the debug=True flag on execute() and new inspection methods.
    """

    def __init__(
        self,
        name: str = "pipeline",
        db_path: str = ".pipelinehub/runs.db",
    ) -> None:
        """
        Initialize a new DataPipeline.

        Args:
            name: Pipeline name, used to group runs in history.
            db_path: Path to SQLite database. Use ":memory:" for testing.
        """
        self.name = name
        self.data: Any = None
        self.steps: List[Callable] = []
        self.step_names: List[str] = []
        self._profiler = DataProfiler()
        self._store = RunStore(db_path=db_path)

    def add_step(self, func: Callable, name: Optional[str] = None) -> "DataPipeline":
        """
        Add a processing step to the pipeline.

        Args:
            func: A callable that takes data and returns transformed data.
            name: Optional step name for display and replay.

        Returns:
            self (for method chaining)

        Raises:
            ValueError: If func is not callable.
        """
        if not callable(func):
            raise ValueError("Step must be a callable function")
        self.steps.append(func)
        self.step_names.append(name or getattr(func, "__name__", f"step_{len(self.steps)}"))
        return self

    def set_data(self, data: Any) -> "DataPipeline":
        """
        Set the initial data for the pipeline.

        Args:
            data: The data to process.

        Returns:
            self (for method chaining)
        """
        self.data = data
        return self

    def execute(self, data: Any = None, verbose: bool = False, debug: bool = True) -> Any:
        """
        Execute all steps in the pipeline.

        Args:
            data: Data to process (overrides instance data set by set_data()).
            verbose: Print step-by-step summary when True or when anomalies detected.
            debug: When True (default), snapshot engine runs automatically.
                   When False, runs identically to v0.1 with no overhead.

        Returns:
            Transformed data after all steps.

        Raises:
            ValueError: If no data is provided.
            PipelineStepError: (debug=True) If a step raises, wraps it with snapshot context.
            RuntimeError: (debug=False) If a step raises, wraps it with step index info.
        """
        current_data = data if data is not None else self.data
        if current_data is None:
            raise ValueError("No data provided. Use set_data() or pass data to execute()")

        if not debug:
            return self._execute_v1(current_data, verbose)

        return self._execute_with_snapshots(current_data, verbose)

    def _execute_v1(self, current_data: Any, verbose: bool) -> Any:
        """v0.1 execution path — no overhead."""
        if verbose:
            print(f"Starting pipeline with {len(self.steps)} steps")
            print(f"Initial data: {self._get_data_info(current_data)}")

        for i, (step, step_name) in enumerate(zip(self.steps, self.step_names)):
            try:
                if verbose:
                    print(f"\nStep {i + 1}: {step_name}")
                current_data = step(current_data)
                if verbose:
                    print(f"  Output: {self._get_data_info(current_data)}")
            except Exception as e:
                raise RuntimeError(f"Error in step {i + 1} ({step_name}): {e}")
        return current_data

    def _execute_with_snapshots(self, current_data: Any, verbose: bool) -> Any:
        """Snapshot engine execution path."""
        last_run = self._store.get_last_run(self.name)
        run_id = self._store.start_run(self.name, len(self.steps))
        last_steps_by_name: Dict[str, Any] = {}
        if last_run:
            for s in last_run.get("steps", []):
                last_steps_by_name[s["step_name"]] = s

        all_anomalies: List[str] = []
        step_summaries: List[Dict[str, Any]] = []
        prev_snap_after: Optional[Dict[str, Any]] = None
        run_start = time.time()

        for i, (step, step_name) in enumerate(zip(self.steps, self.step_names)):
            snap_before = self._profiler.capture(current_data, step_name, "before")
            step_start = time.time()

            try:
                result = step(current_data)
            except Exception as e:
                self._store.save_failure(run_id, step_name, i, snap_before, e)
                self._store.finish_run(run_id, "failed", datetime.datetime.utcnow().isoformat())
                self._store.prune_old_runs()
                raise PipelineStepError(step_name, i, snap_before, e)

            duration = time.time() - step_start
            snap_after = self._profiler.capture(result, step_name, "after")
            self._store.save_step(run_id, step_name, i, snap_before, snap_after, duration)

            anomalies = self._detect_anomalies(
                step_name, snap_before, snap_after, prev_snap_after,
                last_steps_by_name.get(step_name),
            )
            all_anomalies.extend(anomalies)
            step_summaries.append(
                self._build_step_summary(step_name, snap_before, snap_after, duration, anomalies)
            )

            prev_snap_after = snap_after
            current_data = result

        finished_at = datetime.datetime.utcnow().isoformat()
        self._store.finish_run(run_id, "success", finished_at)
        self._store.prune_old_runs()

        if verbose or all_anomalies:
            self._print_summary(step_summaries, time.time() - run_start, all_anomalies)

        return current_data

    def _detect_anomalies(
        self,
        step_name: str,
        snap_before: Dict[str, Any],
        snap_after: Dict[str, Any],
        prev_snap_after: Optional[Dict[str, Any]],
        last_run_step: Optional[Dict[str, Any]],
    ) -> List[str]:
        anomalies: List[str] = []
        dtype = snap_after.get("dtype")
        profile_after = snap_after.get("profile", {})

        if prev_snap_after is not None:
            prev_profile = prev_snap_after.get("profile", {})

            if dtype == "dataframe":
                rows_prev = prev_profile.get("rows", 0)
                if rows_prev == 0:
                    pass  # can't compute percentage drop from 0-row step
                else:
                    rows_now = profile_after.get("rows", 0)
                    if rows_now < rows_prev * 0.5:
                        anomalies.append(
                            f'⚠  Row count dropped {rows_prev}→{rows_now} in step "{step_name}" (>50%)'
                        )

                nulls_now = profile_after.get("null_counts", {})
                nulls_prev = prev_profile.get("null_counts", {})
                for col, count_now in nulls_now.items():
                    increase = count_now - nulls_prev.get(col, 0)
                    if increase > 100:
                        anomalies.append(
                            f'⚠  {col} nulls introduced in step "{step_name}" (+{increase})'
                        )

                dtypes_now = profile_after.get("dtypes", {})
                dtypes_prev = prev_profile.get("dtypes", {})
                for col, t_now in dtypes_now.items():
                    t_prev = dtypes_prev.get(col)
                    if t_prev is not None and t_now != t_prev:
                        anomalies.append(
                            f'⚠  {col} dtype changed in step "{step_name}": {t_prev}→{t_now}'
                        )

                cols_now = set(profile_after.get("columns", []))
                cols_prev = set(prev_profile.get("columns", []))
                for col in sorted(cols_prev - cols_now):
                    anomalies.append(f'⚠  Column "{col}" dropped in step "{step_name}"')

            elif dtype == "sequence":
                len_prev = prev_profile.get("length", 0)
                if len_prev > 0:
                    len_now = profile_after.get("length", 0)
                    if len_now < len_prev * 0.5:
                        anomalies.append(
                            f'⚠  Sequence length dropped {len_prev}→{len_now} in step "{step_name}" (>50%)'
                        )

        if last_run_step is not None and dtype == "dataframe":
            last_profile = (last_run_step.get("snapshot_after") or {}).get("profile", {})
            stats_now = profile_after.get("numeric_stats", {})
            stats_last = last_profile.get("numeric_stats", {})
            for col, stat_now in stats_now.items():
                stat_last = stats_last.get(col)
                if stat_last is None:
                    continue
                mean_now = (stat_now or {}).get("mean")
                mean_last = (stat_last or {}).get("mean")
                if mean_now is not None and mean_last is not None and mean_last != 0:
                    shift = abs(mean_now - mean_last) / abs(mean_last)
                    if shift > 0.20:
                        anomalies.append(
                            f'⚠  {col} mean shifted {shift * 100:.1f}% vs last run in step "{step_name}"'
                        )

        if last_run_step is not None and dtype == "sequence":
            last_profile = (last_run_step.get("snapshot_after") or {}).get("profile", {})
            len_now = profile_after.get("length", 0)
            len_last = last_profile.get("length", 0)
            if len_last > 0 and len_now < len_last * 0.5:
                anomalies.append(
                    f'⚠  Sequence length dropped {len_last}→{len_now} vs last run in step "{step_name}" (>50%)'
                )

        return anomalies

    def _build_step_summary(
        self,
        step_name: str,
        snap_before: Dict[str, Any],
        snap_after: Dict[str, Any],
        duration: float,
        anomalies: List[str],
    ) -> Dict[str, Any]:
        dtype = snap_after.get("dtype")
        pb = snap_before.get("profile", {})
        pa = snap_after.get("profile", {})

        rows_str = ""
        nulls_str = ""

        if dtype == "dataframe":
            rows_before = pb.get("rows", "?")
            rows_after = pa.get("rows", "?")
            rows_str = f"{rows_before}→{rows_after}"
            nulls_b = sum(pb.get("null_counts", {}).values())
            nulls_a = sum(pa.get("null_counts", {}).values())
            flag = " ⚠" if anomalies else ""
            nulls_str = f"{nulls_b}→{nulls_a}{flag}"
        elif dtype == "sequence":
            rows_str = f"{pb.get('length', '?')}→{pa.get('length', '?')}"
            nulls_str = "-"
        else:
            rows_str = "-"
            nulls_str = "-"

        return {
            "name": step_name,
            "rows": rows_str,
            "nulls": nulls_str,
            "duration": duration,
        }

    def _print_summary(
        self,
        step_summaries: List[Dict[str, Any]],
        total_duration: float,
        all_anomalies: List[str],
    ) -> None:
        print(f"\nPipeline completed  ✓  ({total_duration:.1f}s)\n")
        if step_summaries:
            print(f"  {'Step':<20} {'Rows':<18} {'Nulls':<15} {'Duration'}")
            print(f"  {'─' * 62}")
            for s in step_summaries:
                print(f"  {s['name']:<20} {s['rows']:<18} {s['nulls']:<15} {s['duration']:.1f}s")
        if all_anomalies:
            print()
            for a in all_anomalies:
                print(a)

    def last_run(self) -> Optional[Dict[str, Any]]:
        """Return the last run with all snapshots."""
        return self._store.get_last_run(self.name)

    def compare_runs(
        self,
        run_id_a: Optional[str] = None,
        run_id_b: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compare two runs. If no run_ids given, compare last two runs.
        Prints a human-readable diff and returns the raw diff dict.
        """
        if run_id_a is None and run_id_b is None:
            runs = self._store.list_runs(pipeline_name=self.name, limit=2)
            if len(runs) < 2:
                print("Not enough runs to compare (need at least 2)")
                return {}
            run_id_b = runs[0]["run_id"]
            run_id_a = runs[1]["run_id"]

        diff = self._store.compare_runs(run_id_a, run_id_b)
        self._print_diff(diff, run_id_a, run_id_b)
        return diff

    def _print_diff(
        self, diff: Dict[str, Any], run_id_a: Optional[str], run_id_b: Optional[str]
    ) -> None:
        a_label = run_id_a[:8] + "…" if run_id_a else "unknown"
        b_label = run_id_b[:8] + "…" if run_id_b else "unknown"
        print(f"\nRun comparison: {a_label} vs {b_label}\n")
        for step_name, step_diff in diff.get("steps", {}).items():
            status = step_diff.get("status", "")
            print(f"  Step: {step_name}  [{status}]")
            if "rows" in step_diff:
                r = step_diff["rows"]
                print(f"    rows: {r['a']} → {r['b']}")
            for col, change in step_diff.get("null_count_changes", {}).items():
                print(f"    ⚠  {col} nulls: {change['a']} → {change['b']}")
            for col, shift in step_diff.get("mean_shifts", {}).items():
                print(f"    ⚠  {col} mean shifted {shift['shift_pct']}%: {shift['a']:.3f} → {shift['b']:.3f}")
            sc = step_diff.get("schema_changes", {})
            for col in sc.get("added", []):
                print(f"    +  column added: {col}")
            for col in sc.get("removed", []):
                print(f"    -  column removed: {col}")

    def replay_from(self, step_name: str, data: Any) -> Any:
        """
        Re-execute the pipeline starting from step_name with given data.
        Skips all steps before step_name.

        Args:
            step_name: Name of the step to start from.
            data: Input data for that step.

        Returns:
            Result after executing from step_name to end.

        Raises:
            ValueError: If step_name is not found in the pipeline.
        """
        if step_name not in self.step_names:
            raise ValueError(f"Step '{step_name}' not found in pipeline. Available: {self.step_names}")
        start_idx = self.step_names.index(step_name)
        current_data = data
        for step in self.steps[start_idx:]:
            current_data = step(current_data)
        return current_data

    def list_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent runs for this pipeline."""
        return self._store.list_runs(pipeline_name=self.name, limit=limit)

    def clear_steps(self) -> "DataPipeline":
        """Remove all steps from the pipeline."""
        self.steps.clear()
        self.step_names.clear()
        return self

    def remove_step(self, index: int) -> "DataPipeline":
        """Remove a step by index."""
        if 0 <= index < len(self.steps):
            self.steps.pop(index)
            self.step_names.pop(index)
        return self

    def get_steps(self) -> List[str]:
        """Get list of step names."""
        return self.step_names.copy()

    def _get_data_info(self, data: Any) -> str:
        data_type = type(data).__name__
        if hasattr(data, "__len__"):
            return f"{data_type} with {len(data)} elements"
        return data_type

    def __len__(self) -> int:
        """Return number of steps in the pipeline."""
        return len(self.steps)

    def __repr__(self) -> str:
        steps_str = ", ".join(self.step_names) if self.step_names else "no steps"
        return f"DataPipeline(name={self.name!r}, {len(self.steps)} steps: {steps_str})"
