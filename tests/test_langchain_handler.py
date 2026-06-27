import pytest


class TestTruncate:

    def test_short_string_unchanged(self):
        from pipelinehub.langchain.utils import truncate
        assert truncate("hello", 500) == "hello"

    def test_exact_length_unchanged(self):
        from pipelinehub.langchain.utils import truncate
        s = "a" * 500
        assert truncate(s, 500) == s

    def test_long_string_truncated(self):
        from pipelinehub.langchain.utils import truncate
        s = "a" * 600
        result = truncate(s, 500)
        assert len(result) == 503  # 500 + "..."
        assert result.endswith("...")

    def test_empty_string(self):
        from pipelinehub.langchain.utils import truncate
        assert truncate("", 500) == ""

    def test_none_returns_empty(self):
        from pipelinehub.langchain.utils import truncate
        assert truncate(None, 500) == ""

    def test_non_string_coerced(self):
        from pipelinehub.langchain.utils import truncate
        assert truncate(12345, 10) == "12345"
