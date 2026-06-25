import pytest
from pipelinehub import DataPipeline


class TestDataPipeline:

    def test_init(self):
        pipeline = DataPipeline(db_path=":memory:")
        assert len(pipeline) == 0
        assert pipeline.data is None

    def test_init_with_data(self):
        data = [1, 2, 3]
        pipeline = DataPipeline(db_path=":memory:")
        pipeline.set_data(data)
        assert pipeline.data == data

    def test_add_step(self):
        pipeline = DataPipeline(db_path=":memory:")
        pipeline.add_step(lambda x: x)
        assert len(pipeline) == 1

    def test_add_step_invalid(self):
        pipeline = DataPipeline(db_path=":memory:")
        with pytest.raises(ValueError):
            pipeline.add_step("not a function")

    def test_execute_basic(self):
        pipeline = DataPipeline(db_path=":memory:")
        pipeline.add_step(lambda x: [i * 2 for i in x])
        result = pipeline.execute([1, 2, 3])
        assert result == [2, 4, 6]

    def test_execute_no_data(self):
        pipeline = DataPipeline(db_path=":memory:")
        pipeline.add_step(lambda x: x)
        with pytest.raises(ValueError):
            pipeline.execute()

    def test_add_step_returns_func(self):
        pipeline = DataPipeline(db_path=":memory:")
        double = lambda x: [i * 2 for i in x]
        returned = pipeline.add_step(double)
        assert returned is double

    def test_clear_steps(self):
        pipeline = DataPipeline(db_path=":memory:")
        pipeline.add_step(lambda x: x)
        pipeline.clear_steps()
        assert len(pipeline) == 0

    def test_remove_step(self):
        pipeline = DataPipeline(db_path=":memory:")
        pipeline.add_step(lambda x: x, name="step1")
        pipeline.add_step(lambda x: x, name="step2")
        pipeline.remove_step(0)
        assert len(pipeline) == 1
        assert pipeline.get_steps() == ["step2"]

    def test_init_with_data_positional_warns(self):
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            pipeline = DataPipeline([1, 2, 3])
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
        assert pipeline.data == [1, 2, 3]
