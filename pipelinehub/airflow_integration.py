"""Airflow 2.x callback integration for pipelinehub."""

import datetime
from contextlib import suppress
from typing import Any, Dict, Optional

from pipelinehub.profiler import DataProfiler
from pipelinehub.store import RunStore


class PipelinehubCallback:
    """
    Airflow 2.x callback handler. Attach on_success / on_failure to any task.

    Example::

        from pipelinehub.airflow_integration import (
            PipelinehubCallback,
        )

        ph = PipelinehubCallback(
            pipeline_name="my_dag", api_key="sk-..."
        )

        @task(
            on_success_callback=ph.on_success,
            on_failure_callback=ph.on_failure,
        )
        def extract():
            return df
    """

    def __init__(
        self,
        pipeline_name: Optional[str] = None,
        db_path: str = ".pipelinehub/runs.db",
        api_key: Optional[str] = None,
        api_url: str = "https://api.pipelinehub.cloud",  # noqa: E501
    ) -> None:
        self._pipeline_name = pipeline_name
        self._profiler = DataProfiler()
        self._store = RunStore(
            db_path=db_path, api_key=api_key, api_url=api_url
        )

    def on_success(self, context: Dict[str, Any]) -> None:
        """Airflow callback to profile XCom and record success."""
        ti = context.get("task_instance") or context.get("ti")
        if ti is None:
            return
        pipeline_name = self._pipeline_name or getattr(
            ti, "dag_id", "unknown"
        )
        step_name = getattr(ti, "task_id", "unknown")

        xcom_value = None
        with suppress(Exception):
            xcom_value = ti.xcom_pull(task_ids=step_name)

        if xcom_value is not None:
            snapshot = self._profiler.capture(xcom_value, step_name, "after")
        else:
            snapshot = {}

        duration = getattr(ti, "duration", None) or 0.0

        with suppress(Exception):
            run_id = self._store.start_run(pipeline_name, 1)
            self._store.save_step(run_id, step_name, 0, {}, snapshot, duration)
            self._store.finish_run(
                run_id, "success", datetime.datetime.utcnow().isoformat()
            )

    def on_failure(self, context: Dict[str, Any]) -> None:
        """Airflow callback to record failure and exception details."""
        ti = context.get("task_instance") or context.get("ti")
        if ti is None:
            return
        pipeline_name = self._pipeline_name or getattr(
            ti, "dag_id", "unknown"
        )
        step_name = getattr(ti, "task_id", "unknown")

        exc = context.get("exception")
        if not isinstance(exc, Exception):
            msg = str(exc) if exc is not None else "unknown error"
            exc = RuntimeError(msg)

        with suppress(Exception):
            run_id = self._store.start_run(pipeline_name, 1)
            self._store.save_failure(run_id, step_name, 0, {}, exc)
            self._store.finish_run(
                run_id, "failed", datetime.datetime.utcnow().isoformat()
            )
