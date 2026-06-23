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

    def test_runs_in_daemon_thread(self):
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
                    assert created_threads[0].daemon is True
