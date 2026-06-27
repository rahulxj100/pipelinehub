import pytest
from pipelinehub.airflow_integration import PipelinehubCallback


class MockTI:
    def __init__(self, task_id="extract", dag_id="my_dag", duration=2.5, xcom_value=None):
        self.task_id = task_id
        self.dag_id = dag_id
        self.duration = duration
        self._xcom_value = xcom_value

    def xcom_pull(self, task_ids=None):
        return self._xcom_value


def make_success_context(xcom_value=None, duration=2.5, dag_id="my_dag", task_id="extract"):
    ti = MockTI(task_id=task_id, dag_id=dag_id, duration=duration, xcom_value=xcom_value)
    return {"task_instance": ti}


class TestOnSuccessXComPresent:
    def test_saves_run_with_success_status(self):
        context = make_success_context(xcom_value={"rows": 100})
        ph = PipelinehubCallback(pipeline_name="test_pipeline", db_path=":memory:")
        ph.on_success(context)
        runs = ph._store.list_runs(pipeline_name="test_pipeline")
        assert len(runs) == 1
        assert runs[0]["status"] == "success"

    def test_profiles_dict_xcom(self):
        context = make_success_context(xcom_value={"rows": 100, "cols": 5})
        ph = PipelinehubCallback(pipeline_name="test_pipeline", db_path=":memory:")
        ph.on_success(context)
        run = ph._store.get_last_run("test_pipeline")
        step = run["steps"][0]
        assert step["step_name"] == "extract"
        assert step["snapshot_after"]["dtype"] == "dict"

    def test_saves_duration(self):
        context = make_success_context(xcom_value={"x": 1}, duration=3.7)
        ph = PipelinehubCallback(pipeline_name="test_pipeline", db_path=":memory:")
        ph.on_success(context)
        run = ph._store.get_last_run("test_pipeline")
        assert run["steps"][0]["duration_seconds"] == pytest.approx(3.7)

    def test_profiles_list_xcom(self):
        context = make_success_context(xcom_value=[1, 2, 3, 4, 5])
        ph = PipelinehubCallback(pipeline_name="test_pipeline", db_path=":memory:")
        ph.on_success(context)
        run = ph._store.get_last_run("test_pipeline")
        step = run["steps"][0]
        assert step["snapshot_after"]["dtype"] == "sequence"
        assert step["snapshot_after"]["profile"]["length"] == 5
