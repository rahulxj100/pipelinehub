"""Rich pipeline step failure exception with snapshot context."""

from typing import Any, Dict


class PipelineStepError(Exception):
    """Exception raised when a pipeline step fails, with full data snapshot context."""

    def __init__(
        self,
        step_name: str,
        step_index: int,
        snapshot_before: Dict[str, Any],
        original_exception: Exception,
    ) -> None:
        self.step_name = step_name
        self.step_index = step_index
        self.snapshot_before = snapshot_before
        self.original_exception = original_exception
        super().__init__(str(self))

    def __str__(self) -> str:
        dtype = self.snapshot_before.get("dtype", "unknown")
        profile = self.snapshot_before.get("profile", {})

        lines = [
            f'PipelineStepError: Step "{self.step_name}" (step {self.step_index + 1}) failed',
            "",
            "Data entering this step:",
        ]

        if dtype == "dataframe":
            rows = profile.get("rows", "?")
            cols = profile.get("cols", "?")
            lines.append(f"  type:    dataframe")
            lines.append(f"  shape:   ({rows}, {cols})")
            null_counts = {k: v for k, v in profile.get("null_counts", {}).items() if v > 0}
            if null_counts:
                null_str = "  ".join(f"{k}: {v}" for k, v in null_counts.items())
                lines.append(f"  nulls:   {null_str}")
            dtypes = profile.get("dtypes", {})
            if dtypes:
                dtype_str = "  ".join(f"{k}: {v}" for k, v in dtypes.items())
                lines.append(f"  dtypes:  {dtype_str}")
        elif dtype == "sequence":
            length = profile.get("length", "?")
            lines.append(f"  type:    sequence")
            lines.append(f"  length:  {length}")
        elif dtype == "array":
            shape = profile.get("shape", "?")
            lines.append(f"  type:    array")
            lines.append(f"  shape:   {shape}")
        else:
            type_name = profile.get("type_name", dtype)
            lines.append(f"  type:    {type_name}")

        lines.extend([
            "",
            f"Original error: {type(self.original_exception).__name__}: {self.original_exception}",
            "",
            "To replay from this step:",
            f'  pipeline.replay_from("{self.step_name}", your_data)',
        ])

        return "\n".join(lines)
