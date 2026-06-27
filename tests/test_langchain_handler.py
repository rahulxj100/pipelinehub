import uuid
from unittest.mock import MagicMock

import pytest


def _skip_if_no_langchain():
    pytest.importorskip("langchain_core", reason="langchain-core not installed")


class MockLLMResult:
    def __init__(self, text="hello", prompt_tokens=10, completion_tokens=5):
        self.llm_output = {
            "token_usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
            }
        }
        gen = MagicMock()
        gen.text = text
        self.generations = [[gen]]


@pytest.fixture
def handler():
    _skip_if_no_langchain()
    from pipelinehub.agent_pipeline import AgentPipeline
    from pipelinehub.langchain.handler import PipelineHubCallbackHandler
    pipeline = AgentPipeline(name="test-agent", db_path=":memory:")
    return PipelineHubCallbackHandler(pipeline), pipeline


class TestHandlerLLM:

    def test_llm_start_end_records_step(self, handler):
        cb, pipeline = handler
        run_id = uuid.uuid4()
        cb.on_chain_start({"name": "RunnableSequence"}, {}, run_id=run_id)
        llm_run_id = uuid.uuid4()
        cb.on_llm_start({"name": "ChatOpenAI"}, ["Hello"], run_id=llm_run_id)
        result = MockLLMResult(text="world", prompt_tokens=20, completion_tokens=10)
        cb.on_llm_end(result, run_id=llm_run_id)
        cb.on_chain_end({"output": "world"}, run_id=run_id)
        run = pipeline._store.get_last_run("test-agent")
        assert run is not None
        llm_steps = [
            s for s in run["steps"]
            if (s.get("snapshot_after") or {}).get("step_type") == "llm_call"
        ]
        assert len(llm_steps) == 1
        snap = llm_steps[0]["snapshot_after"]
        assert snap["prompt_tokens"] == 20
        assert snap["completion_tokens"] == 10
        assert snap["model"] == "ChatOpenAI"

    def test_llm_error_records_step_error(self, handler):
        cb, pipeline = handler
        run_id = uuid.uuid4()
        cb.on_chain_start({"name": "Chain"}, {}, run_id=run_id)
        llm_run_id = uuid.uuid4()
        cb.on_llm_start({"name": "ChatOpenAI"}, ["hello"], run_id=llm_run_id)
        cb.on_llm_error(ValueError("rate limit"), run_id=llm_run_id)
        cb.on_chain_error(ValueError("rate limit"), run_id=run_id)
        with pipeline._store._get_conn() as conn:
            rows = conn.execute("SELECT * FROM failures").fetchall()
        assert len(rows) >= 1

    def test_llm_end_without_token_usage(self, handler):
        cb, pipeline = handler
        run_id = uuid.uuid4()
        cb.on_chain_start({"name": "Chain"}, {}, run_id=run_id)
        llm_run_id = uuid.uuid4()
        cb.on_llm_start({"name": "ChatOpenAI"}, ["hello"], run_id=llm_run_id)
        result = MagicMock()
        result.llm_output = None  # some providers return None
        gen = MagicMock()
        gen.text = "answer"
        gen.generation_info = None  # prevent auto-mock from producing truthy usage values
        result.generations = [[gen]]
        cb.on_llm_end(result, run_id=llm_run_id)  # must not raise
        cb.on_chain_end({}, run_id=run_id)


class TestHandlerTool:

    def test_tool_start_end_records_step(self, handler):
        cb, pipeline = handler
        run_id = uuid.uuid4()
        cb.on_chain_start({"name": "Chain"}, {}, run_id=run_id)
        tool_run_id = uuid.uuid4()
        cb.on_tool_start({"name": "search"}, "AI trends", run_id=tool_run_id)
        cb.on_tool_end("results here", run_id=tool_run_id)
        cb.on_chain_end({}, run_id=run_id)
        run = pipeline._store.get_last_run("test-agent")
        tool_steps = [
            s for s in run["steps"]
            if (s.get("snapshot_after") or {}).get("step_type") == "tool_call"
        ]
        assert len(tool_steps) == 1
        snap = tool_steps[0]["snapshot_after"]
        assert snap["tool_name"] == "search"
        assert "AI trends" in (snap.get("input_preview") or "")

    def test_tool_error_records_step_error(self, handler):
        cb, pipeline = handler
        run_id = uuid.uuid4()
        cb.on_chain_start({"name": "Chain"}, {}, run_id=run_id)
        tool_run_id = uuid.uuid4()
        cb.on_tool_start({"name": "calculator"}, "1+1", run_id=tool_run_id)
        cb.on_tool_error(RuntimeError("timeout"), run_id=tool_run_id)
        cb.on_chain_error(RuntimeError("timeout"), run_id=run_id)
        with pipeline._store._get_conn() as conn:
            rows = conn.execute("SELECT * FROM failures").fetchall()
        assert len(rows) >= 1


class TestHandlerChain:

    def test_root_chain_starts_and_ends_run(self, handler):
        cb, pipeline = handler
        run_id = uuid.uuid4()
        cb.on_chain_start({"name": "RunnableSequence"}, {}, run_id=run_id)
        cb.on_chain_end({"output": "done"}, run_id=run_id)
        run = pipeline._store.get_last_run("test-agent")
        assert run is not None
        assert run["status"] == "success"

    def test_nested_chain_does_not_start_new_run(self, handler):
        cb, pipeline = handler
        root_id = uuid.uuid4()
        nested_id = uuid.uuid4()
        cb.on_chain_start({"name": "Outer"}, {}, run_id=root_id)
        cb.on_chain_start({"name": "Inner"}, {}, run_id=nested_id)
        cb.on_chain_end({"output": "inner"}, run_id=nested_id)
        cb.on_chain_end({"output": "outer"}, run_id=root_id)
        # only one run should exist
        runs = pipeline._store.list_runs(pipeline_name="test-agent")
        assert len(runs) == 1

    def test_root_chain_error_marks_run_failed(self, handler):
        cb, pipeline = handler
        run_id = uuid.uuid4()
        cb.on_chain_start({"name": "Chain"}, {}, run_id=run_id)
        cb.on_chain_error(RuntimeError("boom"), run_id=run_id)
        runs = pipeline._store.list_runs(pipeline_name="test-agent")
        assert len(runs) == 1
        assert runs[0]["status"] == "failed"

    def test_handler_reusable_across_invocations(self, handler):
        cb, pipeline = handler
        for i in range(3):
            run_id = uuid.uuid4()
            cb.on_chain_start({"name": "Chain"}, {}, run_id=run_id)
            cb.on_chain_end({"output": str(i)}, run_id=run_id)
        runs = pipeline._store.list_runs(pipeline_name="test-agent")
        assert len(runs) == 3


class TestTruncate:

    def test_short_string_unchanged(self):
        from pipelinehub.langchain.utils import truncate
        assert truncate("hello", 500) == "hello"

    def test_exact_length_unchanged(self):
        from pipelinehub.langchain.utils import truncate
        s = "a" * 500
        assert truncate(s, 500) == s

    def test_long_string_truncated(self):
        from pipelinehub.langchain.utils import truncate
        s = "a" * 600
        result = truncate(s, 500)
        assert len(result) == 503  # 500 + "..."
        assert result.endswith("...")

    def test_empty_string(self):
        from pipelinehub.langchain.utils import truncate
        assert truncate("", 500) == ""

    def test_none_returns_empty(self):
        from pipelinehub.langchain.utils import truncate
        assert truncate(None, 500) == ""

    def test_non_string_coerced(self):
        from pipelinehub.langchain.utils import truncate
        assert truncate(12345, 10) == "12345"
