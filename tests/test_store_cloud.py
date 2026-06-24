import threading
import json
from unittest.mock import patch, MagicMock, call
import pytest
from pipelinehub.store import RunStore


def make_sync_thread(**kwargs):
    """Makes threading.Thread run synchronously so mock calls are captured."""
    target = kwargs.get("target")
    class _T:
        def start(self):
            if target:
                target()
    return _T()


class TestRunStoreCloudInit:

    def test_no_api_key_by_default(self):
        store = RunStore(db_path=":memory:")
        assert store._api_key is None

    def test_api_key_stored(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_abc123")
        assert store._api_key == "ph_live_abc123"

    def test_api_url_default(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_x")
        assert store._api_url == "https://api.pipelinehub.cloud"

    def test_api_url_custom(self):
        store = RunStore(db_path=":memory:", api_key="ph_test_x", api_url="http://localhost:8000")
        assert store._api_url == "http://localhost:8000"

    def test_env_var_used_when_no_explicit_key(self, monkeypatch):
        monkeypatch.setenv("PIPELINEHUB_API_KEY", "ph_live_fromenv")
        store = RunStore(db_path=":memory:")
        assert store._api_key == "ph_live_fromenv"

    def test_explicit_key_overrides_env_var(self, monkeypatch):
        monkeypatch.setenv("PIPELINEHUB_API_KEY", "ph_live_fromenv")
        store = RunStore(db_path=":memory:", api_key="ph_live_explicit")
        assert store._api_key == "ph_live_explicit"


class TestCloudPost:

    def test_no_call_when_no_api_key(self):
        store = RunStore(db_path=":memory:")
        with patch("urllib.request.urlopen") as mock_open:
            store._cloud_post("/v1/runs", {"run_id": "abc"})
            mock_open.assert_not_called()

    def test_posts_to_correct_url(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_x", api_url="http://localhost:8000")
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                store._cloud_post("/v1/runs", {"run_id": "abc"})
                req = mock_open.call_args[0][0]
                assert req.full_url == "http://localhost:8000/v1/runs"

    def test_post_method_default(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_x", api_url="http://localhost:8000")
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                store._cloud_post("/v1/runs", {"run_id": "abc"})
                req = mock_open.call_args[0][0]
                assert req.get_method() == "POST"

    def test_patch_method(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_x", api_url="http://localhost:8000")
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                store._cloud_post("/v1/runs/abc", {"status": "success"}, method="PATCH")
                req = mock_open.call_args[0][0]
                assert req.get_method() == "PATCH"

    def test_auth_header_set(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_secret", api_url="http://localhost:8000")
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                store._cloud_post("/v1/runs", {"run_id": "abc"})
                req = mock_open.call_args[0][0]
                assert req.get_header("Authorization") == "Bearer ph_live_secret"

    def test_content_type_json(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_x", api_url="http://localhost:8000")
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                store._cloud_post("/v1/runs", {"run_id": "abc"})
                req = mock_open.call_args[0][0]
                assert req.get_header("Content-type") == "application/json"

    def test_payload_json_encoded(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_x", api_url="http://localhost:8000")
        payload = {"run_id": "abc", "pipeline_name": "test"}
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                store._cloud_post("/v1/runs", payload)
                req = mock_open.call_args[0][0]
                body = json.loads(req.data)
                assert body == payload

    def test_urlopen_error_swallowed(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_x", api_url="http://localhost:8000")
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen", side_effect=Exception("network down")):
                # Must not raise
                store._cloud_post("/v1/runs", {"run_id": "abc"})

    def test_runs_in_non_daemon_thread(self):
        store = RunStore(db_path=":memory:", api_key="ph_live_x", api_url="http://localhost:8000")
        created_threads = []
        original_thread = threading.Thread

        def capture_thread(**kwargs):
            t = original_thread(**kwargs)
            created_threads.append(t)
            return t

        with patch("pipelinehub.store.threading.Thread", side_effect=capture_thread):
            with patch("urllib.request.urlopen"):
                store._cloud_post("/v1/runs", {"run_id": "abc"})
                if created_threads:
                    created_threads[0].join(timeout=1.0)
                    assert created_threads[0].daemon is False


class TestCloudSync:

    def _store(self, url="http://localhost:8000"):
        return RunStore(db_path=":memory:", api_key="ph_live_x", api_url=url)

    def test_start_run_posts_to_v1_runs(self):
        store = self._store()
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                run_id = store.start_run("my_pipeline", 3)
                req = mock_open.call_args[0][0]
                assert req.full_url == "http://localhost:8000/v1/runs"
                assert req.get_method() == "POST"
                body = json.loads(req.data)
                assert body["run_id"] == run_id
                assert body["pipeline_name"] == "my_pipeline"
                assert body["total_steps"] == 3
                assert "started_at" in body
                assert body["status"] == "running"

    def test_save_step_posts_to_steps_endpoint(self):
        store = self._store()
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                run_id = store.start_run("p", 1)
                mock_open.reset_mock()
                snap = {"step_name": "s1", "stage": "before", "dtype": "sequence",
                        "timestamp": "2025-01-01T00:00:00", "profile": {}}
                store.save_step(run_id, "s1", 0, snap, snap, 0.5)
                req = mock_open.call_args[0][0]
                assert req.full_url == f"http://localhost:8000/v1/runs/{run_id}/steps"
                assert req.get_method() == "POST"
                body = json.loads(req.data)
                assert body["step_name"] == "s1"
                assert body["step_index"] == 0
                assert body["duration_seconds"] == pytest.approx(0.5)
                assert "snapshot_before" in body
                assert "snapshot_after" in body

    def test_save_failure_posts_to_failure_endpoint(self):
        store = self._store()
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                run_id = store.start_run("p", 1)
                mock_open.reset_mock()
                snap = {"step_name": "s1", "stage": "before", "dtype": "sequence",
                        "timestamp": "2025-01-01T00:00:00", "profile": {}}
                exc = ValueError("bad data")
                store.save_failure(run_id, "s1", 0, snap, exc)
                req = mock_open.call_args[0][0]
                assert req.full_url == f"http://localhost:8000/v1/runs/{run_id}/failure"
                assert req.get_method() == "POST"
                body = json.loads(req.data)
                assert body["step_name"] == "s1"
                assert body["exception_type"] == "ValueError"
                assert body["exception_message"] == "bad data"

    def test_finish_run_patches_run(self):
        store = self._store()
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                run_id = store.start_run("p", 1)
                mock_open.reset_mock()
                store.finish_run(run_id, "success", "2025-01-01T12:00:00")
                req = mock_open.call_args[0][0]
                assert req.full_url == f"http://localhost:8000/v1/runs/{run_id}"
                assert req.get_method() == "PATCH"
                body = json.loads(req.data)
                assert body["status"] == "success"
                assert body["finished_at"] == "2025-01-01T12:00:00"

    def test_no_cloud_call_without_api_key(self):
        store = RunStore(db_path=":memory:")
        with patch("urllib.request.urlopen") as mock_open:
            run_id = store.start_run("p", 1)
            snap = {"step_name": "s1", "stage": "before", "dtype": "sequence",
                    "timestamp": "2025-01-01T00:00:00", "profile": {}}
            store.save_step(run_id, "s1", 0, snap, snap, 0.1)
            store.save_failure(run_id, "s1", 0, snap, ValueError("x"))
            store.finish_run(run_id, "success", "2025-01-01T00:00:00")
            mock_open.assert_not_called()

    def test_cloud_failure_does_not_raise(self):
        store = self._store()
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen", side_effect=OSError("timeout")):
                # None of these should raise
                run_id = store.start_run("p", 1)
                snap = {"step_name": "s1", "stage": "before", "dtype": "sequence",
                        "timestamp": "2025-01-01T00:00:00", "profile": {}}
                store.save_step(run_id, "s1", 0, snap, snap, 0.1)
                store.finish_run(run_id, "success", "2025-01-01T00:00:00")

    def test_sqlite_write_succeeds_even_when_cloud_fails(self):
        store = self._store()
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen", side_effect=OSError("timeout")):
                run_id = store.start_run("p", 1)
                run = store.get_run(run_id)
                assert run is not None
                assert run["pipeline_name"] == "p"


class TestDataPipelineCloudKey:

    def test_api_key_passed_to_store(self):
        from pipelinehub.pipeline import DataPipeline
        pipeline = DataPipeline(name="p", api_key="ph_live_abc")
        assert pipeline._store._api_key == "ph_live_abc"

    def test_api_url_passed_to_store(self):
        from pipelinehub.pipeline import DataPipeline
        pipeline = DataPipeline(name="p", api_key="ph_live_x", api_url="http://localhost:8000")
        assert pipeline._store._api_url == "http://localhost:8000"

    def test_no_api_key_by_default(self):
        from pipelinehub.pipeline import DataPipeline
        pipeline = DataPipeline(name="p", db_path=":memory:")
        assert pipeline._store._api_key is None

    def test_env_var_picked_up_via_store(self, monkeypatch):
        from pipelinehub.pipeline import DataPipeline
        monkeypatch.setenv("PIPELINEHUB_API_KEY", "ph_live_env")
        pipeline = DataPipeline(name="p")
        assert pipeline._store._api_key == "ph_live_env"

    def test_execute_fires_cloud_calls(self):
        from pipelinehub.pipeline import DataPipeline
        pipeline = DataPipeline(
            name="p",
            db_path=":memory:",
            api_key="ph_live_x",
            api_url="http://localhost:8000",
        )
        pipeline.add_step(lambda x: x)
        with patch("pipelinehub.store.threading.Thread", side_effect=make_sync_thread):
            with patch("urllib.request.urlopen") as mock_open:
                pipeline.execute([1, 2, 3])
                urls_called = [c[0][0].full_url for c in mock_open.call_args_list]
                assert any("/v1/runs" == u.replace("http://localhost:8000", "") for u in urls_called)
