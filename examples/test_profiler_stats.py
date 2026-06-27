"""
Verify that the profiler captures percentiles, cardinality, and correlation
for pandas DataFrames, polars DataFrames, numpy arrays, and Python sequences.

Run with:
    python examples/test_profiler_stats.py
"""

import sys
import pathlib

# Ensure the project root is on sys.path when run directly as a script
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
failures = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  {PASS}  {label}")
    else:
        print(f"  {FAIL}  {label}" + (f" — {detail}" if detail else ""))
        failures.append(label)


# ---------------------------------------------------------------------------
# pandas
# ---------------------------------------------------------------------------
print("\n── pandas DataFrame ──")
try:
    import pandas as pd
    from pipelinehub.profiler import DataProfiler

    df = pd.DataFrame({
        "price":    [10.0, 20.0, 30.0, 40.0, 50.0],
        "qty":      [1,    2,    3,    4,    5],
        "category": ["A",  "A",  "B",  "B",  "C"],
    })

    p = DataProfiler()
    profile = p._profile_pandas(df)

    # Percentiles
    price_stats = profile["numeric_stats"]["price"]
    check("pandas p25 present",         "p25" in price_stats)
    check("pandas p50 present",         "p50" in price_stats)
    check("pandas p75 present",         "p75" in price_stats)
    check("pandas p25 value correct",   abs(price_stats["p25"] - 20.0) < 0.01,
          f"got {price_stats['p25']}")
    check("pandas p50 value correct",   abs(price_stats["p50"] - 30.0) < 0.01,
          f"got {price_stats['p50']}")
    check("pandas p75 value correct",   abs(price_stats["p75"] - 40.0) < 0.01,
          f"got {price_stats['p75']}")

    # Cardinality
    check("pandas cardinality present",           "cardinality" in profile)
    check("pandas cardinality price == 5",        profile["cardinality"]["price"] == 5)
    check("pandas cardinality category == 3",     profile["cardinality"]["category"] == 3)

    # Correlation
    check("pandas correlation present",           "correlation" in profile)
    check("pandas correlation is dict",           isinstance(profile["correlation"], dict))
    check("pandas correlation price-qty ~1.0",    abs(profile["correlation"]["price"]["qty"] - 1.0) < 0.01,
          f"got {profile['correlation']['price']['qty']}")
    check("pandas correlation excludes category", "category" not in profile["correlation"])

    # Single numeric col → correlation None
    df_one = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": ["x", "y", "z"]})
    profile_one = p._profile_pandas(df_one)
    check("pandas correlation None for single numeric col", profile_one["correlation"] is None)

except ImportError:
    print("  SKIP  pandas not installed")


# ---------------------------------------------------------------------------
# polars
# ---------------------------------------------------------------------------
print("\n── polars DataFrame ──")
try:
    import polars as pl
    from pipelinehub.profiler import DataProfiler

    df = pl.DataFrame({
        "price":    [10.0, 20.0, 30.0, 40.0, 50.0],
        "qty":      [1,    2,    3,    4,    5],
        "category": ["A",  "A",  "B",  "B",  "C"],
    })

    p = DataProfiler()
    profile = p._profile_polars(df)

    price_stats = profile["numeric_stats"]["price"]
    check("polars p25 present",       "p25" in price_stats)
    check("polars p50 present",       "p50" in price_stats)
    check("polars p75 present",       "p75" in price_stats)
    check("polars p25 value correct", abs(price_stats["p25"] - 20.0) < 0.01,
          f"got {price_stats['p25']}")
    check("polars p50 value correct", abs(price_stats["p50"] - 30.0) < 0.01,
          f"got {price_stats['p50']}")
    check("polars p75 value correct", abs(price_stats["p75"] - 40.0) < 0.01,
          f"got {price_stats['p75']}")

    check("polars cardinality present",       "cardinality" in profile)
    check("polars cardinality price == 5",    profile["cardinality"]["price"] == 5)
    check("polars cardinality category == 3", profile["cardinality"]["category"] == 3)

    check("polars correlation present",            "correlation" in profile)
    check("polars correlation price-qty ~1.0",     abs(profile["correlation"]["price"]["qty"] - 1.0) < 0.01,
          f"got {profile['correlation']['price']['qty']}")
    check("polars correlation excludes category",  "category" not in profile["correlation"])

except ImportError:
    print("  SKIP  polars not installed")


# ---------------------------------------------------------------------------
# numpy
# ---------------------------------------------------------------------------
print("\n── numpy array ──")
try:
    import numpy as np
    from pipelinehub.profiler import DataProfiler

    arr = np.array([10.0, 20.0, 30.0, 40.0, 50.0])
    p = DataProfiler()
    profile = p._profile_array(arr)

    check("numpy p25 present",       "p25" in profile)
    check("numpy p50 present",       "p50" in profile)
    check("numpy p75 present",       "p75" in profile)
    check("numpy p25 value correct", abs(profile["p25"] - 20.0) < 0.01, f"got {profile['p25']}")
    check("numpy p50 value correct", abs(profile["p50"] - 30.0) < 0.01, f"got {profile['p50']}")
    check("numpy p75 value correct", abs(profile["p75"] - 40.0) < 0.01, f"got {profile['p75']}")

    empty_arr = np.array([], dtype=float)
    profile_empty = p._profile_array(empty_arr)
    check("numpy empty array p25 is None", profile_empty["p25"] is None)
    check("numpy empty array p50 is None", profile_empty["p50"] is None)

except ImportError:
    print("  SKIP  numpy not installed")


# ---------------------------------------------------------------------------
# Python sequence
# ---------------------------------------------------------------------------
print("\n── Python sequence ──")
from pipelinehub.profiler import DataProfiler

p = DataProfiler()
profile = p._profile_sequence([10.0, 20.0, 30.0, 40.0, 50.0])
stats = profile["numeric_stats"]

check("sequence p25 present",       "p25" in stats)
check("sequence p50 present",       "p50" in stats)
check("sequence p75 present",       "p75" in stats)
check("sequence p25 value correct", abs(stats["p25"] - 20.0) < 0.01, f"got {stats['p25']}")
check("sequence p50 value correct", abs(stats["p50"] - 30.0) < 0.01, f"got {stats['p50']}")
check("sequence p75 value correct", abs(stats["p75"] - 40.0) < 0.01, f"got {stats['p75']}")

non_numeric = p._profile_sequence(["a", "b", "c"])
check("sequence non-numeric has no percentiles", non_numeric["numeric_stats"] is None)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print()
if failures:
    print(f"❌  {len(failures)} check(s) failed:")
    for f in failures:
        print(f"    • {f}")
    sys.exit(1)
else:
    print("✅  All checks passed.")
