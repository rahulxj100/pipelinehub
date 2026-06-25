"""Tests for Polars DataFrame and LazyFrame profiling."""
import pytest

polars = pytest.importorskip("polars")

import polars as pl
from pipelinehub.profiler import DataProfiler
from pipelinehub import DataPipeline


@pytest.fixture
def profiler():
    return DataProfiler()


@pytest.fixture
def df():
    return pl.DataFrame({
        "id": [1, 2, 3, 4],
        "value": [10.0, None, 30.0, 40.0],
        "label": ["a", "b", None, "d"],
    })


class TestPolarsDetection:

    def test_dataframe_detected_as_dataframe(self, profiler, df):
        assert profiler._detect_type(df) == "dataframe"

    def test_lazyframe_detected_as_dataframe(self, profiler, df):
        assert profiler._detect_type(df.lazy()) == "dataframe"


class TestPolarsDataFrameProfile:

    def test_rows(self, profiler, df):
        snap = profiler.capture(df, "s", "after")
        assert snap["profile"]["rows"] == 4

    def test_cols(self, profiler, df):
        snap = profiler.capture(df, "s", "after")
        assert snap["profile"]["cols"] == 3

    def test_columns(self, profiler, df):
        snap = profiler.capture(df, "s", "after")
        assert snap["profile"]["columns"] == ["id", "value", "label"]

    def test_null_counts(self, profiler, df):
        snap = profiler.capture(df, "s", "after")
        nulls = snap["profile"]["null_counts"]
        assert nulls["id"] == 0
        assert nulls["value"] == 1
        assert nulls["label"] == 1

    def test_numeric_stats(self, profiler, df):
        snap = profiler.capture(df, "s", "after")
        stats = snap["profile"]["numeric_stats"]
        assert "value" in stats
        assert abs(stats["value"]["mean"] - (10 + 30 + 40) / 3) < 0.01
        assert stats["value"]["min"] == 10.0
        assert stats["value"]["max"] == 40.0

    def test_non_numeric_col_excluded_from_stats(self, profiler, df):
        snap = profiler.capture(df, "s", "after")
        assert "label" not in snap["profile"]["numeric_stats"]

    def test_dtypes_present(self, profiler, df):
        snap = profiler.capture(df, "s", "after")
        assert "dtypes" in snap["profile"]
        assert "value" in snap["profile"]["dtypes"]

    def test_schema_hash_present(self, profiler, df):
        snap = profiler.capture(df, "s", "after")
        assert "schema_hash" in snap["profile"]
        assert len(snap["profile"]["schema_hash"]) == 32

    def test_schema_hash_stable(self, profiler, df):
        h1 = profiler.capture(df, "s", "after")["profile"]["schema_hash"]
        h2 = profiler.capture(df, "s", "after")["profile"]["schema_hash"]
        assert h1 == h2

    def test_schema_hash_changes_on_schema_change(self, profiler, df):
        df2 = df.with_columns(pl.lit("x").alias("extra"))
        h1 = profiler.capture(df, "s", "after")["profile"]["schema_hash"]
        h2 = profiler.capture(df2, "s", "after")["profile"]["schema_hash"]
        assert h1 != h2

    def test_empty_dataframe(self, profiler):
        empty = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)})
        snap = profiler.capture(empty, "s", "after")
        assert snap["profile"]["rows"] == 0
        assert snap["profile"]["null_counts"]["a"] == 0


class TestPolarsLazyFrame:

    def test_lazyframe_profiled_as_dataframe(self, profiler, df):
        snap = profiler.capture(df.lazy(), "s", "after")
        assert snap["dtype"] == "dataframe"

    def test_lazyframe_rows(self, profiler, df):
        snap = profiler.capture(df.lazy(), "s", "after")
        assert snap["profile"]["rows"] == 4

    def test_lazyframe_null_counts(self, profiler, df):
        snap = profiler.capture(df.lazy(), "s", "after")
        assert snap["profile"]["null_counts"]["value"] == 1

    def test_lazyframe_with_filter(self, profiler, df):
        filtered = df.lazy().filter(pl.col("value") > 15)
        snap = profiler.capture(filtered, "s", "after")
        assert snap["profile"]["rows"] == 2


class TestPolarsEndToEnd:

    def test_pipeline_with_polars_dataframe(self):
        pipeline = DataPipeline(name="polars-e2e", db_path=":memory:")
        pipeline.add_step(lambda df: df.drop_nulls(), "clean")
        pipeline.add_step(
            lambda df: df.with_columns((pl.col("value") * 2).alias("doubled")),
            "transform",
        )
        df = pl.DataFrame({"id": [1, 2, None], "value": [10.0, None, 30.0]})
        result = pipeline.execute(df)
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (1, 3)

    def test_pipeline_with_lazyframe(self):
        pipeline = DataPipeline(name="polars-lazy-e2e", db_path=":memory:")
        pipeline.add_step(lambda lf: lf.filter(pl.col("value") > 10), "filter")
        lf = pl.DataFrame({"id": [1, 2, 3], "value": [5.0, 15.0, 25.0]}).lazy()
        result = pipeline.execute(lf)
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape[0] == 2

    def test_anomaly_detected_on_row_drop(self):
        # Row-drop detection compares step N output vs step N-1 output.
        # Need 2 steps: passthrough produces 10 rows, clean drops to 1 → >50% → anomaly.
        pipeline = DataPipeline(name="polars-anomaly", db_path=":memory:")
        pipeline.add_step(lambda df: df, "passthrough")
        pipeline.add_step(lambda df: df.drop_nulls(), "clean")

        df = pl.DataFrame({"v": [1.0, None, None, None, None, None, None, None, None, None]})
        import io, sys
        captured = io.StringIO()
        sys.stdout = captured
        pipeline.execute(df)
        sys.stdout = sys.__stdout__
        assert "⚠" in captured.getvalue()
