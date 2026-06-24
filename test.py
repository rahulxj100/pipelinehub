import pandas as pd
from pipelinehub import DataPipeline

def clean(df):
    return df.dropna()

def feature_engineer(df):
    df = df.copy()
    df["price_doubled"] = df["price"] * 2
    df["category"] = None  # introduce nulls
    return df

def normalize(df):
    df = df.copy()
    df["price"] = df["price"] / df["price"].max()
    return df

data = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "price": [10.0, None, 30.0, 40.0, 50.0],
    "name": ["a", "b", "c", "d", "e"],
})

pipeline = DataPipeline(name="df-test", db_path=":memory:")
pipeline.add_step(clean, "clean")
pipeline.add_step(feature_engineer, "feature_engineer")
pipeline.add_step(normalize, "normalize")

result = pipeline.execute(data, verbose=True)
print("\nResult:\n", result)

print("\n--- Step snapshots ---")
last = pipeline.last_run()
for step in last["steps"]:
    print(f"\n[{step['step_name']}]")
    profile = step["snapshot_after"]["profile"]
    print(f"  rows: {profile['rows']}, cols: {profile['cols']}")
    print(f"  null_counts: {profile['null_counts']}")
    print(f"  numeric_stats: {profile['numeric_stats']}")
