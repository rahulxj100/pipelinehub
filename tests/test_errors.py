import pytest
from pipelinehub.errors import PipelineStepError


class TestPipelineStepError:

    def _make_sequence_snapshot(self):
        return {
            "step_name": "normalize",
            "stage": "before",
            "dtype": "sequence",
            "timestamp": "2025-09-16T10:23:45.123456",
            "profile": {"length": 10, "element_type": "int", "sample_head": [1, 2, 3, 4, 5], "sample_tail": [6, 7, 8, 9, 10], "numeric_stats": {"mean": 5.5, "min": 1, "max": 10}},
        }

    def _make_dataframe_snapshot(self):
        return {
            "step_name": "normalize",
            "stage": "before",
            "dtype": "dataframe",
            "timestamp": "2025-09-16T10:23:45.123456",
            "profile": {
                "rows": 10420,
                "cols": 8,
                "columns": ["col_id", "col_price"],
                "dtypes": {"col_id": "object", "col_price": "float64"},
                "null_counts": {"col_id": 0, "col_price": 142},
                "numeric_stats": {},
                "schema_hash": "abc123",
            },
        }

    def test_attributes_stored(self):
        snap = self._make_sequence_snapshot()
        original = ValueError("test error")
        err = PipelineStepError("normalize", 2, snap, original)
        assert err.step_name == "normalize"
        assert err.step_index == 2
        assert err.snapshot_before is snap
        assert err.original_exception is original

    def test_str_contains_step_name(self):
        snap = self._make_sequence_snapshot()
        err = PipelineStepError("normalize", 2, snap, ValueError("bad"))
        assert "normalize" in str(err)

    def test_str_contains_original_error(self):
        snap = self._make_sequence_snapshot()
        err = PipelineStepError("normalize", 2, snap, TypeError("unsupported operand"))
        assert "TypeError" in str(err)
        assert "unsupported operand" in str(err)

    def test_str_contains_replay_hint(self):
        snap = self._make_sequence_snapshot()
        err = PipelineStepError("normalize", 2, snap, ValueError("x"))
        assert "replay_from" in str(err)
        assert "normalize" in str(err)

    def test_str_dataframe_shows_shape(self):
        snap = self._make_dataframe_snapshot()
        err = PipelineStepError("normalize", 2, snap, ValueError("x"))
        s = str(err)
        assert "10420" in s
        assert "8" in s

    def test_str_dataframe_shows_nulls(self):
        snap = self._make_dataframe_snapshot()
        err = PipelineStepError("normalize", 2, snap, ValueError("x"))
        s = str(err)
        assert "col_price" in s
        assert "142" in s

    def test_is_exception_subclass(self):
        snap = self._make_sequence_snapshot()
        err = PipelineStepError("s", 0, snap, ValueError("x"))
        assert isinstance(err, Exception)

    def test_can_be_raised_and_caught(self):
        snap = self._make_sequence_snapshot()
        with pytest.raises(PipelineStepError) as exc_info:
            raise PipelineStepError("s", 0, snap, ValueError("original"))
        assert exc_info.value.step_name == "s"
