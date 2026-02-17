import argparse
from pathlib import Path
import shutil

from pyspark.sql import functions as F, types as T

from nfl_lakehouse.common.spark_io import get_spark, write_bronze_parquet_spark
from nfl_lakehouse.sources.nflreadpy_source import load_schedules


def main(season: int):
    pl_df = load_schedules(season)

    if not hasattr(pl_df, "write_parquet"):
        raise TypeError(f"load_schedules returned {type(pl_df)}; expected a Polars DataFrame.")

    tmp_dir = Path("data") / "_tmp" / "schedules_extract" / f"season={season}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_parquet = tmp_dir / "schedules.parquet"
    if tmp_parquet.exists():
        tmp_parquet.unlink()

    pl_df.write_parquet(tmp_parquet)

    spark = get_spark(app_name="nfl_lakehouse_ingest_schedules")
    sdf = spark.read.parquet(str(tmp_parquet))

    if "season" not in sdf.columns:
        sdf = sdf.withColumn("season", F.lit(int(season)).cast(T.IntegerType()))
    else:
        sdf = sdf.withColumn("season", F.col("season").cast(T.IntegerType()))

    # Minimal sanity check
    if "game_id" in sdf.columns:
        null_game_id = sdf.filter(F.col("game_id").isNull()).count()
        if null_game_id > 0:
            raise ValueError(f"Found {null_game_id} rows with null game_id in schedules.")

    out_dir = write_bronze_parquet_spark(
        sdf,
        dataset="schedules",
        mode="overwrite",
        partition_by=("season",),
    )

    print(f"Wrote schedules Bronze dataset to: {out_dir} (season={season})")

    spark.stop()
    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)
