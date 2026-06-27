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


class TestOnSuccessEdgeCases:
    def test_none_xcom_saves_empty_snapshot(self):
        context = make_success_context(xcom_value=None)
        ph = PipelinehubCallback(pipeline_name="pipe", db_path=":memory:")
        ph.on_success(context)
        run = ph._store.get_last_run("pipe")
        assert run is not None
        assert run["steps"][0]["snapshot_after"] == {}

    def test_none_xcom_still_saves_success_run(self):
        context = make_success_context(xcom_value=None)
        ph = PipelinehubCallback(pipeline_name="pipe", db_path=":memory:")
        ph.on_success(context)
        runs = ph._store.list_runs(pipeline_name="pipe")
        assert len(runs) == 1
        assert runs[0]["status"] == "success"

    def test_pipeline_name_falls_back_to_dag_id(self):
        context = make_success_context(dag_id="etl_dag", xcom_value=[10, 20])
        ph = PipelinehubCallback(db_path=":memory:")  # no pipeline_name
        ph.on_success(context)
        runs = ph._store.list_runs(pipeline_name="etl_dag")
        assert len(runs) == 1

    def test_explicit_pipeline_name_overrides_dag_id(self):
        context = make_success_context(dag_id="etl_dag", xcom_value=[10, 20])
        ph = PipelinehubCallback(pipeline_name="custom_name", db_path=":memory:")
        ph.on_success(context)
        assert len(ph._store.list_runs(pipeline_name="custom_name")) == 1
        assert len(ph._store.list_runs(pipeline_name="etl_dag")) == 0

    def test_duration_none_saves_zero(self):
        ti = MockTI(task_id="load", dag_id="dag", duration=None, xcom_value={"x": 1})
        context = {"task_instance": ti}
        ph = PipelinehubCallback(pipeline_name="pipe", db_path=":memory:")
        ph.on_success(context)
        run = ph._store.get_last_run("pipe")
        assert run["steps"][0]["duration_seconds"] == 0.0
