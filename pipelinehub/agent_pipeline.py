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
    ) -> None:
        self.name = name
        self._store = RunStore(db_path=db_path)
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
        return []  # anomaly logic added in Task 2

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
