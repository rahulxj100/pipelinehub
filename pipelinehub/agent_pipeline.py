"""AgentPipeline: Observability pipeline for LangChain agents and chains."""

import datetime
from contextlib import suppress
from typing import Any, Dict, List, Optional

from pipelinehub.store import RunStore


class AgentPipeline:
    """
    Observability pipeline for LangChain agents and chains.

    Records LLM calls, tool calls, and chain steps to local SQLite.
    Use with PipelineHubCallbackHandler for zero-code-change instrumentation.
    """

    def __init__(
        self,
        name: str = "agent",
        db_path: str = ".pipelinehub/runs.db",
        api_key: Optional[str] = None,
        api_url: str = "https://api.pipelinehub.cloud",
    ) -> None:
        self.name = name
        self._store = RunStore(db_path=db_path, api_key=api_key, api_url=api_url)
        self._current_run_id: Optional[str] = None
        self._current_run_steps: List[Dict[str, Any]] = []
        self._last_run_steps: List[Dict[str, Any]] = []
        self._step_index: int = 0
        self._error_count: int = 0

    def start(self) -> str:
        """Begin a new agent run. Returns the run_id."""
        last_run = self._store.get_last_run(self.name)
        self._last_run_steps = last_run.get("steps", []) if last_run else []
        self._current_run_id = self._store.start_run(self.name, total_steps=0)
        self._current_run_steps = []
        self._step_index = 0
        self._error_count = 0
        return self._current_run_id

    def record_step(self, step_type: str, **metrics: Any) -> None:
        """Record a completed step (llm_call, tool_call, chain, retrieval)."""
        if self._current_run_id is None:
            return
        snap: Dict[str, Any] = {"step_type": step_type, **metrics}
        with suppress(Exception):
            self._store.save_step(
                self._current_run_id,
                step_name="{}_{}".format(step_type, self._step_index),
                step_index=self._step_index,
                snapshot_before={},
                snapshot_after=snap,
                duration_seconds=float(metrics.get("duration", 0.0)),
            )
        self._current_run_steps.append(snap)
        self._step_index += 1

    def record_step_error(self, step_type: str, error: str, **metrics: Any) -> None:
        """Record a failed step."""
        if self._current_run_id is None:
            return
        snap: Dict[str, Any] = {"step_type": step_type, "error": error, **metrics}
        with suppress(Exception):
            self._store.save_failure(
                self._current_run_id,
                step_name="{}_{}".format(step_type, self._step_index),
                step_index=self._step_index,
                snapshot_before=snap,
                exception=Exception(error),
            )
        self._error_count += 1
        self._step_index += 1

    def end(self, status: str = "success") -> None:
        """Finalise the run, detect anomalies, print completion hint."""
        if self._current_run_id is None:
            return
        finished_at = datetime.datetime.utcnow().isoformat()
        with suppress(Exception):
            self._store.finish_run(self._current_run_id, status, finished_at)
        with suppress(Exception):
            self._store.prune_old_runs()
        anomalies = self._detect_run_anomalies(
            self._current_run_steps, self._last_run_steps
        )
        self._print_completion(self._current_run_steps, anomalies)
        self._current_run_id = None

    def _detect_run_anomalies(
        self,
        current_steps: List[Dict[str, Any]],
        last_steps: List[Dict[str, Any]],
    ) -> List[str]:
        anomalies: List[str] = []

        def _last_snap(step: Dict[str, Any]) -> Dict[str, Any]:
            return step.get("snapshot_after") or {}

        # token_spike, latency_regression, tool_call_drift, tool_call_order_change
        # — only compare when there is prior history
        if last_steps:
            current_tokens = sum(
                s.get("prompt_tokens", 0) + s.get("completion_tokens", 0)
                for s in current_steps
                if s.get("step_type") == "llm_call"
            )
            last_tokens = sum(
                _last_snap(s).get("prompt_tokens", 0) + _last_snap(s).get("completion_tokens", 0)
                for s in last_steps
                if _last_snap(s).get("step_type") == "llm_call"
            )
            if last_tokens > 0 and current_tokens > 2 * last_tokens:
                anomalies.append(
                    "⚠  token_spike: {} tokens vs"
                    " {} last run (>2x)".format(current_tokens, last_tokens)
                )

            # latency_regression
            current_dur = sum(s.get("duration", 0.0) for s in current_steps)
            last_dur = sum(_last_snap(s).get("duration", 0.0) for s in last_steps)
            if last_dur > 0 and current_dur > 3 * last_dur:
                anomalies.append(
                    "⚠  latency_regression: {:.1f}s vs"
                    " {:.1f}s last run (>3x)".format(current_dur, last_dur)
                )

            # tool_call_drift / tool_call_order_change
            current_tools = [
                s["tool_name"]
                for s in current_steps
                if s.get("step_type") == "tool_call" and "tool_name" in s
            ]
            last_tools = [
                _last_snap(s)["tool_name"]
                for s in last_steps
                if _last_snap(s).get("step_type") == "tool_call"
                and "tool_name" in _last_snap(s)
            ]
            if last_tools:
                if set(current_tools) != set(last_tools):
                    added = sorted(set(current_tools) - set(last_tools))
                    removed = sorted(set(last_tools) - set(current_tools))
                    anomalies.append(
                        "⚠  tool_call_drift: added={}"
                        " removed={} vs last run".format(added, removed)
                    )
                elif current_tools != last_tools:
                    anomalies.append(
                        "⚠  tool_call_order_change: same tools,"
                        " different sequence vs last run"
                    )

        # error_rate_spike — always checked, no prior run needed
        total = len(current_steps) + self._error_count
        if total > 0 and self._error_count / total > 0.20:
            anomalies.append(
                "⚠  error_rate_spike: {}/{}"
                " steps errored (>20%)".format(self._error_count, total)
            )

        return anomalies

    def _print_completion(
        self, steps: List[Dict[str, Any]], anomalies: List[str]
    ) -> None:
        total_tokens = sum(
            s.get("prompt_tokens", 0) + s.get("completion_tokens", 0)
            for s in steps
            if s.get("step_type") == "llm_call"
        )
        n_steps = len(steps) + self._error_count
        if total_tokens > 0:
            print(
                "[PipelineHub] Run complete — {:,} tokens used"
                " across {} steps.".format(total_tokens, n_steps)
            )
            print(
                "              \U0001f4b0 Track cost trends over time"
                " → pipelinehub.cloud"
            )
        else:
            print("[PipelineHub] Run complete — {} steps.".format(n_steps))
        for a in anomalies:
            print(a)

    def __repr__(self) -> str:
        return "AgentPipeline(name={!r})".format(self.name)
