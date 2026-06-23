from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame, functions as F

from nfl_lakehouse.common.spark_io import get_spark, write_silver_parquet_spark


def _check_null_rate(df: DataFrame, col: str, max_null_pct: float, dataset: str):
    total = df.count()
    if total == 0:
        return
    null_count = df.filter(F.col(col).isNull()).count()
    null_pct = null_count / total
    if null_pct > max_null_pct:
        raise ValueError(
            f"{dataset}: column '{col}' has {null_pct:.1%} nulls "
            f"(threshold {max_null_pct:.1%}, {null_count}/{total} rows)."
        )


def clean_silver(
    *,
    dataset: str,
    bronze_dataset: str,
    silver_dataset: str,
    season: int,
    transform: Callable[[DataFrame], DataFrame],
    null_rate_checks: Optional[List[Tuple[str, float]]] = None,
    partition_by: Iterable[str] = ("season",),
) -> str:
    """
    Shared Silver cleaning pattern:
      1. Read Bronze partition for the given season
      2. Apply dataset-specific transform (passed in as a callable)
      3. Assert row count > 0 (catches transforms that wipe all rows)
      4. Run optional null-rate checks on important columns
      5. Write to Silver
    """
    bronze_path = Path(f"data/bronze/{bronze_dataset}/season={season}")
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze path not found: {bronze_path}")

    spark = get_spark(app_name=f"nfl_lakehouse_clean_{dataset}")
    df = spark.read.parquet(str(bronze_path))

    cleaned = transform(df)

    row_count = cleaned.count()
    if row_count == 0:
        raise ValueError(f"{dataset}: Silver transform produced 0 rows for season={season}. Aborting write.")

    if null_rate_checks:
        for col, threshold in null_rate_checks:
            if col in cleaned.columns:
                _check_null_rate(cleaned, col, threshold, dataset)

    out_dir = write_silver_parquet_spark(
        cleaned,
        dataset=silver_dataset,
        partition_by=tuple(partition_by),
        add_loaded_at=True,
        loaded_at_col="silver_loaded_at",
    )

    print(f"Wrote {dataset} Silver to {out_dir} (season={season}, rows={row_count})")
    spark.stop()
    return out_dir
