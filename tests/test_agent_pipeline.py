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
