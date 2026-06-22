import pytest
from pipelinehub import DataPipeline, PipelineStepError


def double(data):
    return [x * 2 for x in data]


def add_one(data):
    return [x + 1 for x in data]


def failing_step(data):
    raise ValueError("intentional failure")


def drop_half(data):
    return data[: len(data) // 2]


class TestExecuteDebugTrue:

    def test_basic_execute_returns_correct_result(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.add_step(add_one, "add_one")
        result = p.execute([1, 2, 3], debug=True)
        assert result == [3, 5, 7]

    def test_run_is_stored(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.execute([1, 2, 3], debug=True)
        last = p.last_run()
        assert last is not None
        assert last["status"] == "success"
        assert len(last["steps"]) == 1

    def test_step_snapshots_stored(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.execute([1, 2, 3], debug=True)
        last = p.last_run()
        step = last["steps"][0]
        assert step["snapshot_before"]["profile"]["length"] == 3
        assert step["snapshot_after"]["profile"]["length"] == 3


class TestExecuteDebugFalse:

    def test_returns_correct_result(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.add_step(add_one, "add_one")
        result = p.execute([1, 2, 3], debug=False)
        assert result == [3, 5, 7]

    def test_no_run_stored(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.execute([1, 2, 3], debug=False)
        assert p.last_run() is None

    def test_raises_runtime_error_on_failure(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(failing_step, "failing_step")
        with pytest.raises(RuntimeError):
            p.execute([1, 2, 3], debug=False)


class TestPipelineStepErrorRaised:

    def test_raises_pipeline_step_error_on_failure(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.add_step(failing_step, "failing_step")
        with pytest.raises(PipelineStepError) as exc_info:
            p.execute([1, 2, 3], debug=True)
        err = exc_info.value
        assert err.step_name == "failing_step"
        assert err.step_index == 1
        assert isinstance(err.original_exception, ValueError)

    def test_snapshot_before_captured_on_failure(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.add_step(failing_step, "failing_step")
        with pytest.raises(PipelineStepError) as exc_info:
            p.execute([1, 2, 3], debug=True)
        snap = exc_info.value.snapshot_before
        assert snap["profile"]["length"] == 3

    def test_run_marked_failed_in_store(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(failing_step, "failing_step")
        with pytest.raises(PipelineStepError):
            p.execute([1, 2, 3], debug=True)
        runs = p.list_runs()
        assert runs[0]["status"] == "failed"


class TestReplayFrom:

    def test_skips_earlier_steps(self, tmp_path):
        called = []

        def track_a(data):
            called.append("a")
            return data

        def track_b(data):
            called.append("b")
            return data

        def track_c(data):
            called.append("c")
            return data

        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(track_a, "a")
        p.add_step(track_b, "b")
        p.add_step(track_c, "c")
        p.replay_from("b", [1, 2, 3])
        assert called == ["b", "c"]

    def test_returns_correct_result(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.add_step(add_one, "add_one")
        result = p.replay_from("add_one", [10, 20])
        assert result == [11, 21]

    def test_raises_for_unknown_step(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        with pytest.raises(ValueError, match="not found"):
            p.replay_from("nonexistent", [1, 2, 3])


class TestCompareRuns:

    def test_compare_last_two_runs(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.execute([1, 2, 3], debug=True)
        p.execute([1, 2, 3], debug=True)
        diff = p.compare_runs()
        assert diff is not None
        assert "steps" in diff

    def test_compare_with_explicit_ids(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.execute([1, 2, 3], debug=True)
        p.execute([1, 2, 3], debug=True)
        runs = p.list_runs()
        diff = p.compare_runs(runs[1]["run_id"], runs[0]["run_id"])
        assert "steps" in diff

    def test_returns_empty_with_fewer_than_two_runs(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.execute([1, 2, 3], debug=True)
        diff = p.compare_runs()
        assert diff == {}

    def test_compare_runs_one_id_none_does_not_crash(self, tmp_path):
        """compare_runs with run_id_a=None should not raise TypeError."""
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.execute([1, 2, 3], debug=True)
        runs = p.list_runs()
        # Only run_id_b provided; run_id_a=None causes _print_diff to be called with None
        try:
            p.compare_runs(run_id_b=runs[0]["run_id"])
        except TypeError:
            pytest.fail("compare_runs raised TypeError when run_id_a was None")


class TestAnomalyDetection:

    def test_anomaly_warning_printed_for_null_spike(self, tmp_path, capsys):
        pytest.importorskip("pandas")
        import pandas as pd

        def make_df(data):
            return pd.DataFrame({"a": list(range(200)), "b": list(range(200))})

        def spike_nulls(df):
            df = df.copy()
            df["b"] = None  # introduces 200 nulls (> 100 threshold)
            return df

        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(make_df, "make_df")
        p.add_step(spike_nulls, "spike_nulls")
        p.execute([1], debug=True)
        captured = capsys.readouterr()
        assert "⚠" in captured.out

    def test_anomaly_warning_printed_for_large_row_drop(self, tmp_path, capsys):
        def keep_one(data):
            return data[:1]  # 100 → 1: 1 < 100*0.5, triggers >50% drop anomaly

        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")       # 100 → 100 items, no anomaly
        p.add_step(keep_one, "keep_one")   # 100 → 1 item, triggers anomaly
        p.execute(list(range(100)), debug=True)
        captured = capsys.readouterr()
        assert "⚠" in captured.out


class TestListRuns:

    def test_returns_run_metadata(self, tmp_path):
        p = DataPipeline(name="test", db_path=str(tmp_path / "runs.db"))
        p.add_step(double, "double")
        p.execute([1, 2, 3], debug=True)
        runs = p.list_runs()
        assert len(runs) == 1
        assert "run_id" in runs[0]
        assert runs[0]["status"] == "success"
