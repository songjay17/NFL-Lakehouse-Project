import shutil
from pathlib import Path
from typing import Callable, Iterable, List

from pyspark.sql import DataFrame, functions as F, types as T

from nfl_lakehouse.common.spark_io import get_spark, write_bronze_parquet_spark


def ingest_bronze(
    *,
    dataset: str,
    season: int,
    loader: Callable,
    required_keys: List[str],
    min_rows: int = 1,
    partition_by: Iterable[str] = ("season",),
) -> str:
    """
    Shared Bronze ingestion pattern:
      1. Load via nflreadpy (returns Polars DataFrame)
      2. Stage as temp Parquet so Spark reads embedded schema
      3. Ensure season column exists with correct type
      4. Validate required key columns are present and non-null
      5. Assert row count >= min_rows (catches silent empty loads)
      6. Write to Bronze partitioned by season
      7. Clean up temp staging directory
    """
    pl_df = loader(season)

    if not hasattr(pl_df, "write_parquet"):
        raise TypeError(f"{dataset}: loader returned {type(pl_df)}; expected a Polars DataFrame.")

    tmp_dir = Path("data") / "_tmp" / f"{dataset}_extract" / f"season={season}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_parquet = tmp_dir / f"{dataset}.parquet"
    if tmp_parquet.exists():
        tmp_parquet.unlink()

    pl_df.write_parquet(tmp_parquet)

    spark = get_spark(app_name=f"nfl_lakehouse_ingest_{dataset}")
    sdf = spark.read.parquet(str(tmp_parquet))

    if "season" not in sdf.columns:
        sdf = sdf.withColumn("season", F.lit(int(season)).cast(T.IntegerType()))
    else:
        sdf = sdf.withColumn("season", F.col("season").cast(T.IntegerType()))

    missing = [c for c in required_keys if c not in sdf.columns]
    if missing:
        raise ValueError(f"{dataset}: missing required columns: {missing}")

    for key in required_keys:
        null_count = sdf.filter(F.col(key).isNull()).count()
        if null_count > 0:
            raise ValueError(f"{dataset}: found {null_count} rows with null {key}.")

    row_count = sdf.count()
    if row_count < min_rows:
        raise ValueError(f"{dataset}: expected at least {min_rows} rows, got {row_count}. Aborting to protect existing Bronze data.")

    out_dir = write_bronze_parquet_spark(
        sdf,
        dataset=dataset,
        mode="overwrite",
        partition_by=tuple(partition_by),
    )

    print(f"Wrote {dataset} Bronze to {out_dir} (season={season}, rows={row_count})")

    spark.stop()
    shutil.rmtree(tmp_dir, ignore_errors=True)

    return out_dir
