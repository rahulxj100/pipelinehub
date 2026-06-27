__all__ = ["PipelineHubCallbackHandler"]


def __getattr__(name):
    if name == "PipelineHubCallbackHandler":
        try:
            from pipelinehub.langchain.handler import PipelineHubCallbackHandler
            return PipelineHubCallbackHandler
        except ImportError:
            raise ImportError(
                "pipelinehub[langchain] requires langchain-core. "
                "Install with: pip install pipelinehub[langchain]"
            )
    raise AttributeError(f"module 'pipelinehub.langchain' has no attribute '{name}'")
