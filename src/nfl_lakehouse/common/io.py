from pathlib import Path
import pandas as pd

def write_bronze_parquet(df: pd.DataFrame, dataset: str, partition: str | None = None) -> Path:
    base = Path("data") / "bronze" / dataset
    if partition:
        base = base / partition
    base.mkdir(parents=True, exist_ok=True)

    out = base / "data.parquet"
    df.to_parquet(out, index=False)
    return out
