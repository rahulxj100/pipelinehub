# tests/test_agent_pipeline.py
import pytest
from pipelinehub.agent_pipeline import AgentPipeline


@pytest.fixture
def pipeline():
    return AgentPipeline(name="test-agent", db_path=":memory:")


class TestAgentPipelineLifecycle:

    def test_start_returns_run_id(self, pipeline):
        run_id = pipeline.start()
        assert isinstance(run_id, str)
        assert len(run_id) > 0

    def test_start_run_ids_unique(self, pipeline):
        id1 = pipeline.start()
        pipeline.end()
        id2 = pipeline.start()
        pipeline.end()
        assert id1 != id2

    def test_record_step_stored(self, pipeline):
        run_id = pipeline.start()
        pipeline.record_step("llm_call", model="gpt-4o", prompt_tokens=100,
                             completion_tokens=50, duration=1.2)
        pipeline.end()
        run = pipeline._store.get_run(run_id)
        assert run is not None
        assert len(run["steps"]) == 1
        snap = run["steps"][0]["snapshot_after"]
        assert snap["step_type"] == "llm_call"
        assert snap["prompt_tokens"] == 100
        assert snap["completion_tokens"] == 50

    def test_record_step_error_stored_in_failures(self, pipeline):
        run_id = pipeline.start()
        pipeline.record_step_error("tool_call", error="timeout", tool_name="search")
        pipeline.end()
        run = pipeline._store.get_run(run_id)
        # failure table — check via direct store query
        with pipeline._store._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM failures WHERE run_id = ?", (run_id,)
            ).fetchall()
        assert len(rows) == 1

    def test_end_marks_run_success(self, pipeline):
        run_id = pipeline.start()
        pipeline.record_step("llm_call", model="gpt-4o", duration=0.5)
        pipeline.end(status="success")
        run = pipeline._store.get_run(run_id)
        assert run["status"] == "success"

    def test_end_marks_run_failed(self, pipeline):
        run_id = pipeline.start()
        pipeline.end(status="failed")
        run = pipeline._store.get_run(run_id)
        assert run["status"] == "failed"

    def test_record_step_noop_before_start(self, pipeline):
        # Must not raise even if start() never called
        pipeline.record_step("llm_call", model="gpt-4o", duration=0.1)

    def test_multiple_steps_stored_in_order(self, pipeline):
        run_id = pipeline.start()
        pipeline.record_step("llm_call", model="gpt-4o", duration=1.0)
        pipeline.record_step("tool_call", tool_name="search", duration=0.3)
        pipeline.record_step("llm_call", model="gpt-4o", duration=0.8)
        pipeline.end()
        run = pipeline._store.get_run(run_id)
        assert len(run["steps"]) == 3
        assert run["steps"][0]["snapshot_after"]["step_type"] == "llm_call"
        assert run["steps"][1]["snapshot_after"]["step_type"] == "tool_call"
        assert run["steps"][2]["snapshot_after"]["step_type"] == "llm_call"


class TestAnomalyDetection:

    def _make_llm_step(self, prompt=100, completion=50, duration=1.0, tool_name=None):
        return {"step_type": "llm_call", "prompt_tokens": prompt,
                "completion_tokens": completion, "duration": duration}

    def _make_tool_step(self, name="search", duration=0.5):
        return {"step_type": "tool_call", "tool_name": name, "duration": duration}

    def _last_step_row(self, snap: dict) -> dict:
        """Wrap a snap dict in the shape RunStore._hydrate_run produces."""
        return {"snapshot_after": snap, "snapshot_before": {}}

    def test_no_anomaly_with_no_history(self, pipeline):
        current = [self._make_llm_step(100, 50, 1.0)]
        result = pipeline._detect_run_anomalies(current, [])
        assert result == []

    def test_token_spike_triggers(self, pipeline):
        last = [self._last_step_row(self._make_llm_step(100, 50))]
        current = [self._make_llm_step(500, 200)]  # 700 tokens vs 150 last — >2x
        anomalies = pipeline._detect_run_anomalies(current, last)
        assert any("token_spike" in a for a in anomalies)

    def test_token_spike_not_triggered_below_threshold(self, pipeline):
        last = [self._last_step_row(self._make_llm_step(100, 50))]
        current = [self._make_llm_step(150, 75)]  # 225 vs 150 — <2x
        anomalies = pipeline._detect_run_anomalies(current, last)
        assert not any("token_spike" in a for a in anomalies)

    def test_latency_regression_triggers(self, pipeline):
        last = [self._last_step_row({"step_type": "llm_call", "duration": 1.0})]
        current = [{"step_type": "llm_call", "duration": 4.0}]  # 4x — >3x threshold
        anomalies = pipeline._detect_run_anomalies(current, last)
        assert any("latency_regression" in a for a in anomalies)

    def test_latency_regression_not_triggered_below_threshold(self, pipeline):
        last = [self._last_step_row({"step_type": "llm_call", "duration": 1.0})]
        current = [{"step_type": "llm_call", "duration": 2.5}]  # 2.5x — <3x
        anomalies = pipeline._detect_run_anomalies(current, last)
        assert not any("latency_regression" in a for a in anomalies)

    def test_tool_call_drift_triggers_new_tool(self, pipeline):
        last = [self._last_step_row(self._make_tool_step("search"))]
        current = [self._make_tool_step("calculator")]  # different tool
        anomalies = pipeline._detect_run_anomalies(current, last)
        assert any("tool_call_drift" in a for a in anomalies)

    def test_tool_call_drift_triggers_missing_tool(self, pipeline):
        last = [
            self._last_step_row(self._make_tool_step("search")),
            self._last_step_row(self._make_tool_step("calculator")),
        ]
        current = [self._make_tool_step("search")]  # calculator missing
        anomalies = pipeline._detect_run_anomalies(current, last)
        assert any("tool_call_drift" in a for a in anomalies)

    def test_tool_call_order_change_triggers(self, pipeline):
        last = [
            self._last_step_row(self._make_tool_step("search")),
            self._last_step_row(self._make_tool_step("calculator")),
        ]
        current = [self._make_tool_step("calculator"), self._make_tool_step("search")]
        anomalies = pipeline._detect_run_anomalies(current, last)
        assert any("tool_call_order_change" in a for a in anomalies)

    def test_error_rate_spike_triggers(self, pipeline):
        pipeline.start()
        pipeline.record_step("llm_call", model="gpt-4o", duration=0.5)
        pipeline._error_count = 4  # simulate 4 errors (1 step + 4 errors = 80% > 20%)
        anomalies = pipeline._detect_run_anomalies(pipeline._current_run_steps, [])
        assert any("error_rate_spike" in a for a in anomalies)
        pipeline.end()

    def test_no_anomaly_same_run(self, pipeline):
        last = [
            self._last_step_row(self._make_llm_step(100, 50, 1.0)),
            self._last_step_row(self._make_tool_step("search", 0.5)),
        ]
        current = [self._make_llm_step(105, 52, 1.1), self._make_tool_step("search", 0.6)]
        anomalies = pipeline._detect_run_anomalies(current, last)
        assert anomalies == []
