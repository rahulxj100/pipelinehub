"""Tests for add_step decorator forms."""
import pytest
from pipelinehub import DataPipeline


def test_bare_decorator():
    p = DataPipeline(name="test", db_path=":memory:")

    @p.add_step
    def step_a(df):
        return df

    assert step_a is not p
    assert callable(step_a)
    assert p.step_names == ["step_a"]


def test_parameterized_decorator():
    p = DataPipeline(name="test", db_path=":memory:")

    @p.add_step(name="custom-name")
    def step_b(df):
        return df

    assert callable(step_b)
    assert p.step_names == ["custom-name"]


def test_explicit_call_with_name():
    p = DataPipeline(name="test", db_path=":memory:")

    def step_c(df):
        return df

    p.add_step(step_c, name="override")
    assert p.step_names == ["override"]
    assert callable(step_c)


def test_explicit_call_without_name():
    p = DataPipeline(name="test", db_path=":memory:")

    def step_d(df):
        return df

    p.add_step(step_d)
    assert p.step_names == ["step_d"]


def test_reuse_across_pipelines():
    p1 = DataPipeline(name="p1", db_path=":memory:")
    p2 = DataPipeline(name="p2", db_path=":memory:")

    @p1.add_step
    def shared(df):
        return df

    p2.add_step(shared)

    assert p1.step_names == ["shared"]
    assert p2.step_names == ["shared"]
    assert callable(shared)


def test_stacked_decorators():
    p1 = DataPipeline(name="p1", db_path=":memory:")
    p2 = DataPipeline(name="p2", db_path=":memory:")

    @p1.add_step
    @p2.add_step
    def validate(df):
        return df

    assert callable(validate)
    assert p1.step_names == ["validate"]
    assert p2.step_names == ["validate"]


def test_parameterized_name_used_in_run():
    p = DataPipeline(name="test", db_path=":memory:")

    @p.add_step(name="reconcile")
    def reconcile_slow(df):
        return df

    assert p.step_names == ["reconcile"]
    result = p.execute([1, 2, 3])
    assert result == [1, 2, 3]
    run = p.last_run()
    assert run["steps"][0]["step_name"] == "reconcile"


def test_non_callable_raises():
    p = DataPipeline(name="test", db_path=":memory:")
    with pytest.raises(ValueError, match="callable"):
        p.add_step("not_a_function")


def test_non_callable_parameterized_raises():
    p = DataPipeline(name="test", db_path=":memory:")
    with pytest.raises(ValueError, match="callable"):
        p.add_step(name="x")("not_a_function")


def test_all_three_forms_execute_correctly():
    p = DataPipeline(name="test", db_path=":memory:")

    @p.add_step
    def double(x):
        return [i * 2 for i in x]

    @p.add_step(name="increment")
    def add_one(x):
        return [i + 1 for i in x]

    def negate(x):
        return [-i for i in x]

    p.add_step(negate, name="negate")

    result = p.execute([1, 2, 3])
    assert result == [-3, -5, -7]
    assert p.step_names == ["double", "increment", "negate"]
