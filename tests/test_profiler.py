import pytest
from pipelinehub.profiler import DataProfiler


class TestDetectType:

    def test_list(self):
        p = DataProfiler()
        assert p._detect_type([1, 2, 3]) == "sequence"

    def test_tuple(self):
        p = DataProfiler()
        assert p._detect_type((1, 2, 3)) == "sequence"

    def test_dict(self):
        p = DataProfiler()
        assert p._detect_type({"a": 1}) == "dict"

    def test_generic_int(self):
        p = DataProfiler()
        assert p._detect_type(42) == "generic"

    def test_generic_custom_class(self):
        class Foo:
            pass
        p = DataProfiler()
        assert p._detect_type(Foo()) == "generic"

    def test_generic_none(self):
        p = DataProfiler()
        assert p._detect_type(None) == "generic"

    def test_pandas_dataframe(self):
        pytest.importorskip("pandas")
        import pandas as pd
        p = DataProfiler()
        df = pd.DataFrame({"a": [1, 2]})
        assert p._detect_type(df) == "dataframe"

    def test_numpy_array(self):
        pytest.importorskip("numpy")
        import numpy as np
        p = DataProfiler()
        arr = np.array([1, 2, 3])
        assert p._detect_type(arr) == "array"


class TestProfileSequence:

    def test_int_list(self):
        p = DataProfiler()
        result = p._profile_sequence([1, 2, 3, 4, 5])
        assert result["length"] == 5
        assert result["element_type"] == "int"
        assert result["numeric_stats"]["mean"] == 3.0
        assert result["numeric_stats"]["min"] == 1
        assert result["numeric_stats"]["max"] == 5
        assert result["sample_head"] == [1, 2, 3, 4, 5]

    def test_string_list(self):
        p = DataProfiler()
        result = p._profile_sequence(["a", "b", "c"])
        assert result["element_type"] == "str"
        assert result["numeric_stats"] is None

    def test_mixed_list(self):
        p = DataProfiler()
        result = p._profile_sequence([1, "a", 2.0])
        assert result["element_type"] == "mixed"

    def test_empty_list(self):
        p = DataProfiler()
        result = p._profile_sequence([])
        assert result["length"] == 0
        assert result["element_type"] == "empty"

    def test_sample_head_tail_long_list(self):
        p = DataProfiler()
        data = list(range(20))
        result = p._profile_sequence(data)
        assert result["sample_head"] == [0, 1, 2, 3, 4]
        assert result["sample_tail"] == [15, 16, 17, 18, 19]

    def test_tuple_works(self):
        p = DataProfiler()
        result = p._profile_sequence((10, 20, 30))
        assert result["length"] == 3


class TestProfileDict:

    def test_basic(self):
        p = DataProfiler()
        result = p._profile_dict({"x": 1, "y": "hello"})
        assert result["num_keys"] == 2
        assert set(result["keys"]) == {"x", "y"}
        assert result["value_types"]["x"] == "int"
        assert result["value_types"]["y"] == "str"

    def test_empty_dict(self):
        p = DataProfiler()
        result = p._profile_dict({})
        assert result["num_keys"] == 0
        assert result["keys"] == []


class TestProfileGeneric:

    def test_custom_class(self):
        class MyObj:
            def __str__(self):
                return "MyObj(value=42)"
        p = DataProfiler()
        result = p._profile_generic(MyObj())
        assert result["type_name"] == "MyObj"
        assert "MyObj(value=42)" in result["str_repr"]

    def test_str_repr_truncated_to_200(self):
        p = DataProfiler()
        long_str = "x" * 300
        result = p._profile_generic(long_str)
        assert len(result["str_repr"]) <= 200

    def test_unprintable_object_doesnt_raise(self):
        class Unprintable:
            def __str__(self):
                raise RuntimeError("cannot print")
        p = DataProfiler()
        result = p._profile_generic(Unprintable())
        assert "type_name" in result


class TestCapture:

    def test_returns_correct_structure(self):
        p = DataProfiler()
        result = p.capture([1, 2, 3], "my_step", "before")
        assert result["step_name"] == "my_step"
        assert result["stage"] == "before"
        assert result["dtype"] == "sequence"
        assert "timestamp" in result
        assert "profile" in result

    def test_stage_after(self):
        p = DataProfiler()
        result = p.capture([1, 2, 3], "s", "after")
        assert result["stage"] == "after"

    def test_never_raises_on_any_input(self):
        p = DataProfiler()
        problematic_inputs = [
            None,
            42,
            object(),
            lambda x: x,
            [None, None],
            {"nested": {"a": 1}},
        ]
        for data in problematic_inputs:
            try:
                p.capture(data, "test_step", "before")
            except Exception as e:
                pytest.fail(f"Profiler raised {type(e).__name__}: {e} for input {data!r}")


class TestProfileDataframePandas:

    def test_basic_dataframe(self):
        pytest.importorskip("pandas")
        import pandas as pd
        p = DataProfiler()
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, None, 6.0]})
        result = p._profile_dataframe(df)
        assert result["rows"] == 3
        assert result["cols"] == 2
        assert result["null_counts"]["b"] == 1
        assert result["null_counts"]["a"] == 0
        assert "schema_hash" in result
        assert isinstance(result["schema_hash"], str)

    def test_numeric_stats_computed(self):
        pytest.importorskip("pandas")
        import pandas as pd
        p = DataProfiler()
        df = pd.DataFrame({"val": [1.0, 2.0, 3.0, 4.0, 5.0]})
        result = p._profile_dataframe(df)
        stats = result["numeric_stats"]["val"]
        assert abs(stats["mean"] - 3.0) < 0.01
        assert stats["min"] == 1.0
        assert stats["max"] == 5.0

    def test_sampling_large_dataframe(self):
        pytest.importorskip("pandas")
        import pandas as pd
        p = DataProfiler()
        df = pd.DataFrame({"a": range(15000), "b": range(15000)})
        result = p._profile_dataframe(df)
        # Actual row count reported, not sample size
        assert result["rows"] == 15000
        # Stats still computed (via sample)
        assert "a" in result["numeric_stats"]

    def test_schema_hash_changes_with_schema(self):
        pytest.importorskip("pandas")
        import pandas as pd
        p = DataProfiler()
        df1 = pd.DataFrame({"a": [1], "b": [2]})
        df2 = pd.DataFrame({"a": [1], "c": [2]})
        h1 = p._profile_dataframe(df1)["schema_hash"]
        h2 = p._profile_dataframe(df2)["schema_hash"]
        assert h1 != h2
