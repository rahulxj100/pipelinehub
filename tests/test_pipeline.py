import pytest
from pipelinehub import DataPipeline


class TestDataPipeline:
    
    def test_init(self):
        pipeline = DataPipeline()
        assert len(pipeline) == 0
        assert pipeline.data is None
        
    def test_init_with_data(self):
        data = [1, 2, 3]
        pipeline = DataPipeline(data)
        assert pipeline.data == data
        
    def test_add_step(self):
        pipeline = DataPipeline()
        pipeline.add_step(lambda x: x)
        assert len(pipeline) == 1
        
    def test_add_step_invalid(self):
        pipeline = DataPipeline()
        with pytest.raises(ValueError):
            pipeline.add_step("not a function")
            
    def test_execute_basic(self):
        pipeline = DataPipeline()
        pipeline.add_step(lambda x: [i * 2 for i in x])
        result = pipeline.execute([1, 2, 3])
        assert result == [2, 4, 6]
        
    def test_execute_no_data(self):
        pipeline = DataPipeline()
        pipeline.add_step(lambda x: x)
        with pytest.raises(ValueError):
            pipeline.execute()
            
    def test_method_chaining(self):
        result = (DataPipeline()
                 .add_step(lambda x: [i * 2 for i in x])
                 .add_step(lambda x: [i + 1 for i in x])
                 .execute([1, 2, 3]))
        assert result == [3, 5, 7]
        
    def test_clear_steps(self):
        pipeline = DataPipeline()
        pipeline.add_step(lambda x: x)
        pipeline.clear_steps()
        assert len(pipeline) == 0
        
    def test_remove_step(self):
        pipeline = DataPipeline()
        pipeline.add_step(lambda x: x, "step1")
        pipeline.add_step(lambda x: x, "step2")
        pipeline.remove_step(0)
        assert len(pipeline) == 1
        assert pipeline.get_steps() == ["step2"]