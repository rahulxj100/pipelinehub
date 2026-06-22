import datetime
import pytest
from pipelinehub.store import RunStore


@pytest.fixture
def store():
    return RunStore(db_path=":memory:")


def _snap(step_name: str, dtype: str = "sequence", length: int = 5) -> dict:
    return {
        "step_name": step_name,
        "stage": "after",
        "dtype": dtype,
        "timestamp": "2025-01-01T00:00:00",
        "profile": {"length": length, "element_type": "int", "sample_head": [], "sample_tail": [], "numeric_stats": None},
    }


def _df_snap(step_name: str, rows: int = 100, nulls: dict = None, means: dict = None, cols=None) -> dict:
    columns = cols or ["a", "b"]
    return {
        "step_name": step_name,
        "stage": "after",
        "dtype": "dataframe",
        "timestamp": "2025-01-01T00:00:00",
        "profile": {
            "rows": rows,
            "cols": len(columns),
            "columns": columns,
            "dtypes": {c: "float64" for c in columns},
            "null_counts": nulls or {c: 0 for c in columns},
            "numeric_stats": {c: {"mean": (means or {}).get(c, 1.0), "std": 0.1, "min": 0.0, "max": 2.0} for c in columns},
            "schema_hash": "abc123",
        },
    }


class TestStartRun:

    def test_returns_run_id(self, store):
        run_id = store.start_run("my_pipeline", 3)
        assert isinstance(run_id, str)
        assert len(run_id) > 0

    def test_run_id_is_unique(self, store):
        id1 = store.start_run("p", 1)
        id2 = store.start_run("p", 1)
        assert id1 != id2

    def test_run_record_created(self, store):
        run_id = store.start_run("my_pipeline", 3)
        run = store.get_run(run_id)
        assert run is not None
        assert run["pipeline_name"] == "my_pipeline"
        assert run["total_steps"] == 3
        assert run["status"] == "running"


class TestSaveStep:

    def test_step_persisted(self, store):
        run_id = store.start_run("p", 1)
        before = _snap("s1")
        after = _snap("s1", length=3)
        store.save_step(run_id, "s1", 0, before, after, 0.25)
        run = store.get_run(run_id)
        assert len(run["steps"]) == 1
        assert run["steps"][0]["step_name"] == "s1"
        assert run["steps"][0]["duration_seconds"] == pytest.approx(0.25)

    def test_snapshot_deserialised(self, store):
        run_id = store.start_run("p", 1)
        before = _snap("s1")
        after = _snap("s1", length=99)
        store.save_step(run_id, "s1", 0, before, after, 0.1)
        run = store.get_run(run_id)
        assert run["steps"][0]["snapshot_after"]["profile"]["length"] == 99

    def test_multiple_steps_ordered(self, store):
        run_id = store.start_run("p", 2)
        store.save_step(run_id, "step_a", 0, _snap("step_a"), _snap("step_a"), 0.1)
        store.save_step(run_id, "step_b", 1, _snap("step_b"), _snap("step_b"), 0.2)
        run = store.get_run(run_id)
        assert run["steps"][0]["step_name"] == "step_a"
        assert run["steps"][1]["step_name"] == "step_b"


class TestSaveFailure:

    def test_failure_persisted(self, store):
        run_id = store.start_run("p", 1)
        before = _snap("s1")
        exc = ValueError("something went wrong")
        store.save_failure(run_id, "s1", 0, before, exc)
        store.finish_run(run_id, "failed", datetime.datetime.utcnow().isoformat())
        run = store.get_run(run_id)
        assert run["status"] == "failed"
        # Verify failure record stored correctly
        with store._get_conn() as conn:
            row = conn.execute(
                "SELECT exception_type, exception_message FROM failures WHERE run_id = ?",
                (run_id,)
            ).fetchone()
        assert row is not None
        assert row[0] == "ValueError"
        assert row[1] == "something went wrong"


class TestFinishRun:

    def test_status_updated(self, store):
        run_id = store.start_run("p", 1)
        ts = "2025-06-01T12:00:00"
        store.finish_run(run_id, "success", ts)
        run = store.get_run(run_id)
        assert run["status"] == "success"
        assert run["finished_at"] == ts


class TestGetLastRun:

    def test_returns_none_when_empty(self, store):
        assert store.get_last_run() is None

    def test_returns_most_recent(self, store):
        id1 = store.start_run("p", 1)
        store.finish_run(id1, "success", "2025-01-01T00:00:00")
        id2 = store.start_run("p", 1)
        store.finish_run(id2, "success", "2025-01-02T00:00:00")
        last = store.get_last_run("p")
        assert last["run_id"] == id2

    def test_filters_by_pipeline_name(self, store):
        id1 = store.start_run("pipeline_a", 1)
        store.finish_run(id1, "success", "2025-01-01T00:00:00")
        id2 = store.start_run("pipeline_b", 1)
        store.finish_run(id2, "success", "2025-01-02T00:00:00")
        last_a = store.get_last_run("pipeline_a")
        assert last_a["run_id"] == id1


class TestCompareRuns:

    def test_detects_null_count_increase(self, store):
        id_a = store.start_run("p", 1)
        store.save_step(id_a, "s1", 0, _snap("s1"), _df_snap("s1", nulls={"a": 0, "b": 0}), 0.1)
        store.finish_run(id_a, "success", "2025-01-01T00:00:00")

        id_b = store.start_run("p", 1)
        store.save_step(id_b, "s1", 0, _snap("s1"), _df_snap("s1", nulls={"a": 0, "b": 50}), 0.1)
        store.finish_run(id_b, "success", "2025-01-02T00:00:00")

        diff = store.compare_runs(id_a, id_b)
        assert diff["steps"]["s1"]["null_count_changes"]["b"]["b"] == 50

    def test_detects_mean_shift_over_10_percent(self, store):
        id_a = store.start_run("p", 1)
        store.save_step(id_a, "s1", 0, _snap("s1"), _df_snap("s1", means={"a": 1.0, "b": 1.0}), 0.1)
        store.finish_run(id_a, "success", "2025-01-01T00:00:00")

        id_b = store.start_run("p", 1)
        store.save_step(id_b, "s1", 0, _snap("s1"), _df_snap("s1", means={"a": 1.5, "b": 1.0}), 0.1)
        store.finish_run(id_b, "success", "2025-01-02T00:00:00")

        diff = store.compare_runs(id_a, id_b)
        assert "a" in diff["steps"]["s1"]["mean_shifts"]

    def test_detects_schema_change(self, store):
        id_a = store.start_run("p", 1)
        store.save_step(id_a, "s1", 0, _snap("s1"), _df_snap("s1", cols=["a", "b"]), 0.1)
        store.finish_run(id_a, "success", "2025-01-01T00:00:00")

        id_b = store.start_run("p", 1)
        store.save_step(id_b, "s1", 0, _snap("s1"), _df_snap("s1", cols=["a", "c"]), 0.1)
        store.finish_run(id_b, "success", "2025-01-02T00:00:00")

        diff = store.compare_runs(id_a, id_b)
        assert "b" in diff["steps"]["s1"]["schema_changes"]["removed"]
        assert "c" in diff["steps"]["s1"]["schema_changes"]["added"]

    def test_nonexistent_run_returns_error(self, store):
        diff = store.compare_runs("nonexistent-a", "nonexistent-b")
        assert "error" in diff


class TestPruneOldRuns:

    def test_deletes_runs_older_than_days(self, store):
        run_id = store.start_run("p", 1)
        old_time = (datetime.datetime.utcnow() - datetime.timedelta(days=31)).isoformat()
        # Backdate via internal connection
        with store._get_conn() as conn:
            conn.execute("UPDATE runs SET started_at = ? WHERE run_id = ?", (old_time, run_id))
        store.prune_old_runs(days=30)
        assert store.get_run(run_id) is None

    def test_keeps_recent_runs(self, store):
        run_id = store.start_run("p", 1)
        store.finish_run(run_id, "success", datetime.datetime.utcnow().isoformat())
        store.prune_old_runs(days=30)
        assert store.get_run(run_id) is not None


class TestListRuns:

    def test_returns_list(self, store):
        store.start_run("p", 1)
        store.start_run("p", 1)
        runs = store.list_runs()
        assert len(runs) == 2

    def test_limit_respected(self, store):
        for _ in range(5):
            store.start_run("p", 1)
        runs = store.list_runs(limit=3)
        assert len(runs) == 3

    def test_filters_by_name(self, store):
        store.start_run("pipeline_a", 1)
        store.start_run("pipeline_b", 1)
        runs = store.list_runs(pipeline_name="pipeline_a")
        assert len(runs) == 1
        assert runs[0]["pipeline_name"] == "pipeline_a"
