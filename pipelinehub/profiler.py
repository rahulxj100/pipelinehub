"""DataProfiler: captures typed statistical snapshots of pipeline data."""

import datetime
import hashlib
from contextlib import suppress
from typing import Any, Dict, Optional


def _is_polars_numeric(dtype) -> bool:
    """Check if a Polars dtype is numeric, compatible across versions."""
    dtype_str = str(dtype).lower()
    return any(t in dtype_str for t in ("int", "uint", "float", "decimal"))


class DataProfiler:
    """Captures lightweight statistical fingerprints of data at each
    pipeline step."""

    def capture(self, data: Any, step_name: str, stage: str) -> Dict[str, Any]:
        """
        Capture a snapshot of data.

        Args:
            data: Any Python object
            step_name: Name of the pipeline step
            stage: "before" or "after"

        Returns:
            dict with keys: step_name, stage, dtype, timestamp, profile
        """
        dtype = self._detect_type(data)
        profile_fns = {
            "dataframe": self._profile_dataframe,
            "array": self._profile_array,
            "sequence": self._profile_sequence,
            "dict": self._profile_dict,
            "generic": self._profile_generic,
        }
        try:
            profile = profile_fns[dtype](data)
        except Exception:
            profile = self._profile_generic(data)

        return {
            "step_name": step_name,
            "stage": stage,
            "dtype": dtype,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "profile": profile,
        }

    def _detect_type(self, data: Any) -> str:
        """
        Returns one of: "dataframe", "array", "sequence", "dict", "generic".
        Imports pandas, polars, numpy lazily — never fails if not installed.
        """
        with suppress(ImportError):
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                return "dataframe"

        with suppress(ImportError):
            import polars as pl
            if isinstance(data, (pl.DataFrame, pl.LazyFrame)):
                return "dataframe"

        with suppress(ImportError):
            import numpy as np
            if isinstance(data, np.ndarray):
                return "array"

        if isinstance(data, (list, tuple)):
            return "sequence"

        if isinstance(data, dict):
            return "dict"

        return "generic"

    def _profile_dataframe(self, df: Any) -> Dict[str, Any]:
        """Capture for pandas or polars DataFrame."""
        with suppress(ImportError):
            import pandas as pd
            if isinstance(df, pd.DataFrame):
                return self._profile_pandas(df)

        with suppress(ImportError):
            import polars as pl
            if isinstance(df, pl.LazyFrame):
                return self._profile_polars(df.collect())
            if isinstance(df, pl.DataFrame):
                return self._profile_polars(df)

        return self._profile_generic(df)

    def _profile_pandas(self, df: Any) -> Dict[str, Any]:
        import pandas as pd
        rows = len(df)
        columns = list(df.columns)
        dtypes = {col: str(df[col].dtype) for col in columns}
        null_counts = {col: int(df[col].isnull().sum()) for col in columns}

        if rows > 10000:
            sample = df.sample(n=min(10000, rows), random_state=42)
        else:
            sample = df
        numeric_cols = [
            c for c in columns
            if pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_bool_dtype(df[c])
        ]
        numeric_stats: Dict[str, Any] = {}
        for col in numeric_cols:
            s = sample[col].dropna()
            if len(s) > 0:
                q = s.quantile([0.25, 0.5, 0.75])
                numeric_stats[col] = {
                    "mean": float(s.mean()),
                    "std": float(s.std()),
                    "min": float(s.min()),
                    "max": float(s.max()),
                    "p25": float(q[0.25]),
                    "p50": float(q[0.50]),
                    "p75": float(q[0.75]),
                }

        cardinality = {col: int(df[col].nunique()) for col in columns}

        correlation = None
        if len(numeric_cols) >= 2:
            corr_df = df[numeric_cols].corr()
            correlation = {
                c: {r: float(corr_df.loc[c, r]) for r in numeric_cols}
                for c in numeric_cols
            }

        schema_str = str(sorted((c, str(df[c].dtype)) for c in columns))
        schema_hash = hashlib.md5(schema_str.encode()).hexdigest()

        return {
            "rows": rows,
            "cols": len(columns),
            "columns": columns,
            "dtypes": dtypes,
            "null_counts": null_counts,
            "numeric_stats": numeric_stats,
            "cardinality": cardinality,
            "correlation": correlation,
            "schema_hash": schema_hash,
        }

    def _profile_polars(self, df: Any) -> Dict[str, Any]:
        import polars as pl
        rows = len(df)
        columns = list(df.columns)
        dtypes = {col: str(df[col].dtype) for col in columns}
        null_counts = {col: int(df[col].null_count()) for col in columns}

        sample = df.sample(n=min(10000, rows), seed=42) if rows > 10000 else df
        numeric_cols = [c for c in columns if _is_polars_numeric(df[c].dtype)]
        numeric_stats: Dict[str, Any] = {}
        for col in numeric_cols:
            s = sample[col].drop_nulls()
            if len(s) > 0:
                mean_v, std_v = s.mean(), s.std()
                min_v, max_v = s.min(), s.max()
                p25_v = s.quantile(0.25, interpolation="linear")
                p50_v = s.quantile(0.50, interpolation="linear")
                p75_v = s.quantile(0.75, interpolation="linear")
                numeric_stats[col] = {
                    "mean": float(mean_v) if mean_v is not None else None,
                    "std": float(std_v) if std_v is not None else None,
                    "min": float(min_v) if min_v is not None else None,
                    "max": float(max_v) if max_v is not None else None,
                    "p25": float(p25_v) if p25_v is not None else None,
                    "p50": float(p50_v) if p50_v is not None else None,
                    "p75": float(p75_v) if p75_v is not None else None,
                }

        cardinality = {col: int(df[col].n_unique()) for col in columns}

        correlation = None
        if len(numeric_cols) >= 2:
            correlation = {
                c: {
                    r: float(df.select(pl.corr(c, r)).item())
                    for r in numeric_cols
                }
                for c in numeric_cols
            }

        schema_str = str(sorted((c, str(df[c].dtype)) for c in columns))
        schema_hash = hashlib.md5(schema_str.encode()).hexdigest()

        return {
            "rows": rows,
            "cols": len(columns),
            "columns": columns,
            "dtypes": dtypes,
            "null_counts": null_counts,
            "numeric_stats": numeric_stats,
            "cardinality": cardinality,
            "correlation": correlation,
            "schema_hash": schema_hash,
        }

    def _profile_array(self, arr: Any) -> Dict[str, Any]:
        """Capture for numpy ndarray."""
        import numpy as np
        flat = arr.flatten()
        sample = flat[:100000] if flat.size > 100000 else flat

        is_float = np.issubdtype(arr.dtype, np.floating)
        valid = sample[~np.isnan(sample)] if is_float else sample
        null_count = int(np.sum(np.isnan(arr))) if is_float else 0

        p25, p50, p75 = (
            (
                float(np.percentile(valid, 25)),
                float(np.percentile(valid, 50)),
                float(np.percentile(valid, 75))
            )
            if len(valid) > 0 else (None, None, None)
        )

        return {
            "shape": list(arr.shape),
            "dtype": str(arr.dtype),
            "mean": float(np.mean(valid)) if len(valid) > 0 else None,
            "std": float(np.std(valid)) if len(valid) > 0 else None,
            "min": float(np.min(valid)) if len(valid) > 0 else None,
            "max": float(np.max(valid)) if len(valid) > 0 else None,
            "p25": p25,
            "p50": p50,
            "p75": p75,
            "null_count": null_count,
        }

    def _profile_sequence(self, data: Any) -> Dict[str, Any]:
        """Capture for list or tuple."""
        length = len(data)

        if length == 0:
            element_type = "empty"
        else:
            first_type = type(data[0]).__name__
            element_type = (
                first_type
                if all(type(x).__name__ == first_type for x in data)
                else "mixed"
            )

        sample_head = list(data[:5])
        sample_tail = list(data[-5:])

        numeric_stats: Optional[Dict[str, Any]] = None
        if length > 0 and all(isinstance(x, (int, float)) for x in data):
            mean = sum(data) / length
            variance = sum((x - mean) ** 2 for x in data) / length
            sorted_data = sorted(data)

            def _pct(pval):
                k = (length - 1) * pval
                f = int(k)
                c = min(f + 1, length - 1)
                delta = sorted_data[c] - sorted_data[f]
                return sorted_data[f] + (k - f) * delta

            numeric_stats = {
                "mean": mean,
                "std": variance ** 0.5,
                "min": min(data),
                "max": max(data),
                "p25": _pct(0.25),
                "p50": _pct(0.50),
                "p75": _pct(0.75),
            }

        return {
            "length": length,
            "element_type": element_type,
            "sample_head": sample_head,
            "sample_tail": sample_tail,
            "numeric_stats": numeric_stats,
        }

    def _profile_dict(self, data: dict) -> Dict[str, Any]:
        """Capture for dict."""
        return {
            "num_keys": len(data),
            "keys": list(data.keys()),
            "value_types": {k: type(v).__name__ for k, v in data.items()},
        }

    def _profile_generic(self, data: Any) -> Dict[str, Any]:
        """Fallback for unknown types. Never raises."""
        try:
            str_repr = str(data)[:200]
        except Exception:
            str_repr = "<unprintable>"
        return {
            "type_name": type(data).__name__,
            "str_repr": str_repr,
        }
