"""PipelineHubCallbackHandler — LangChain callback integration."""
import time
from typing import Any, Dict, Optional

try:
    from langchain_core.callbacks import BaseCallbackHandler
except ImportError as exc:
    raise ImportError(
        "langchain-core is required for pipelinehub[langchain]. "
        "Install with: pip install pipelinehub[langchain]"
    ) from exc

from pipelinehub.langchain.utils import truncate


class PipelineHubCallbackHandler(BaseCallbackHandler):
    """
    LangChain callback handler that instruments chains, agents, and tools
    with PipelineHub observability — writing to local SQLite with zero
    changes to user code beyond adding this as a callback.
    """

    def __init__(self, pipeline: Any) -> None:
        super().__init__()
        self.pipeline = pipeline
        self._run_map: Dict[str, dict] = {}  # LangChain run_id -> metadata
        self._root_run_id: Optional[str] = None  # top-level chain run_id

    # ------------------------------------------------------------------ LLM

    def on_llm_start(
        self, serialized: dict, prompts: list, *, run_id: Any, **kwargs: Any
    ) -> None:
        # Extract actual model name — works for OpenAI, Anthropic, Google, etc.
        invocation_params = kwargs.get("invocation_params") or {}
        model = (
            invocation_params.get("model")
            or invocation_params.get("model_name")
            or (serialized.get("kwargs") or {}).get("model")
            or (serialized.get("kwargs") or {}).get("model_name")
            or serialized.get("name", "unknown")
        )
        self._run_map[str(run_id)] = {
            "type": "llm_call",
            "model": model,
            "start": time.time(),
            "prompt_preview": truncate(prompts[0], 200) if prompts else "",
        }

    def on_llm_end(self, response: Any, *, run_id: Any, **kwargs: Any) -> None:
        meta = self._run_map.pop(str(run_id), {})
        duration = time.time() - meta.get("start", time.time())
        generations = getattr(response, "generations", None) or []
        output_text = ""
        if generations and generations[0]:
            output_text = getattr(generations[0][0], "text", "") or str(
                getattr(generations[0][0], "message", "")
            )

        # Normalize token usage across providers:
        # OpenAI:    llm_output["token_usage"]   → prompt_tokens / completion_tokens
        # Anthropic: llm_output["usage"]          → input_tokens / output_tokens
        # Google:    generation_info["usage_metadata"] → prompt_token_count / candidates_token_count
        llm_output = getattr(response, "llm_output", None) or {}
        raw = {}
        if isinstance(llm_output, dict):
            raw = llm_output.get("token_usage") or llm_output.get("usage") or {}
        if not raw and generations and generations[0]:
            gen_info = getattr(generations[0][0], "generation_info", None) or {}
            raw = (
                gen_info.get("usage_metadata")
                or gen_info.get("usage")
                or gen_info.get("token_usage")
                or {}
            )
        prompt_tokens = (
            raw.get("prompt_tokens")
            or raw.get("input_tokens")
            or raw.get("prompt_token_count")
            or 0
        )
        completion_tokens = (
            raw.get("completion_tokens")
            or raw.get("output_tokens")
            or raw.get("candidates_token_count")
            or 0
        )

        self.pipeline.record_step(
            "llm_call",
            model=meta.get("model", "unknown"),
            duration=duration,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            output_preview=truncate(output_text, 200),
        )

    def on_llm_error(self, error: Any, *, run_id: Any, **kwargs: Any) -> None:
        meta = self._run_map.pop(str(run_id), {})
        self.pipeline.record_step_error(
            "llm_call",
            error=str(error),
            model=meta.get("model", "unknown"),
            duration=time.time() - meta.get("start", time.time()),
        )

    # ----------------------------------------------------------------- Tool

    def on_tool_start(
        self, serialized: dict, input_str: str, *, run_id: Any, **kwargs: Any
    ) -> None:
        self._run_map[str(run_id)] = {
            "type": "tool_call",
            "tool_name": serialized.get("name", "unknown"),
            "start": time.time(),
            "input_preview": truncate(input_str, 200),
        }

    def on_tool_end(self, output: Any, *, run_id: Any, **kwargs: Any) -> None:
        meta = self._run_map.pop(str(run_id), {})
        self.pipeline.record_step(
            "tool_call",
            tool_name=meta.get("tool_name", "unknown"),
            duration=time.time() - meta.get("start", time.time()),
            input_preview=meta.get("input_preview", ""),
            output_preview=truncate(output, 200),
        )

    def on_tool_error(self, error: Any, *, run_id: Any, **kwargs: Any) -> None:
        meta = self._run_map.pop(str(run_id), {})
        self.pipeline.record_step_error(
            "tool_call",
            error=str(error),
            tool_name=meta.get("tool_name", "unknown"),
            duration=time.time() - meta.get("start", time.time()),
        )

    # ---------------------------------------------------------------- Chain

    def on_chain_start(
        self, serialized: dict, inputs: Any, *, run_id: Any, **kwargs: Any
    ) -> None:
        run_id_str = str(run_id)
        if self._root_run_id is None:
            self._root_run_id = run_id_str
            self.pipeline.start()
        self._run_map[run_id_str] = {
            "type": "chain",
            "chain_type": serialized.get("name", "unknown"),
            "start": time.time(),
        }

    def on_chain_end(self, outputs: Any, *, run_id: Any, **kwargs: Any) -> None:
        run_id_str = str(run_id)
        meta = self._run_map.pop(run_id_str, {})
        self.pipeline.record_step(
            "chain",
            chain_type=meta.get("chain_type", "unknown"),
            duration=time.time() - meta.get("start", time.time()),
            output_preview=truncate(str(outputs), 200),
        )
        if run_id_str == self._root_run_id:
            self.pipeline.end(status="success")
            self._root_run_id = None

    def on_chain_error(self, error: Any, *, run_id: Any, **kwargs: Any) -> None:
        run_id_str = str(run_id)
        meta = self._run_map.pop(run_id_str, {})
        self.pipeline.record_step_error(
            "chain",
            error=str(error),
            chain_type=meta.get("chain_type", "unknown"),
            duration=time.time() - meta.get("start", time.time()),
        )
        if run_id_str == self._root_run_id:
            self.pipeline.end(status="failed")
            self._root_run_id = None
