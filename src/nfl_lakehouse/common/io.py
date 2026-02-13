from pathlib import Path
import pandas as pd

def write_bronze_parquet(df: pd.DataFrame, dataset: str, partition: str | None = None, *, as_dataset: bool = False, filename: str = "data.parquet", compression: str = "snappy") -> Path:
    base = Path("data") / "bronze" / dataset
    if partition:
        base = base / partition
    base.mkdir(parents=True, exist_ok=True)
    if not as_dataset:
        out = base / filename
        df.to_parquet(out, index=False, compression=compression)
    out_dir = base
    df.to_parquet(out_dir, index=False, compression=compression, engine="pyarrow")
    return out_dir
