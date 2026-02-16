import argparse
from pathlib import Path
import shutil

from pyspark.sql import functions as F, types as T

from nfl_lakehouse.common.spark_io import get_spark, write_bronze_parquet_spark
from nfl_lakehouse.sources.nflreadpy_source import load_pbp


def main(season: int):
    # 1) Load PBP via nflreadpy helper
    df = load_pbp(season)

    # nflreadpy often returns polars; if it's pandas, convert to polars (for safe parquet writing)
    try:
        import polars as pl
    except ImportError as e:
        raise ImportError(
            "polars is required for this ingestion approach. Install with: python3 -m pip install polars"
        ) from e

    if hasattr(df, "write_parquet"):  # already polars
        pl_df = df
    else:
        # assume pandas
        pl_df = pl.from_pandas(df)

    # 2) Write a TEMP parquet file using Polars (schema is embedded -> Spark won't infer types)
    tmp_dir = Path("data") / "_tmp" / "pbp_extract" / f"season={season}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_parquet = tmp_dir / "pbp.parquet"

    # overwrite if rerun
    if tmp_parquet.exists():
        tmp_parquet.unlink()

    pl_df.write_parquet(tmp_parquet)

    # 3) Spark reads the parquet (no schema inference pain)
    spark = get_spark(app_name="nfl_lakehouse_ingest_pbp")
    sdf = spark.read.parquet(str(tmp_parquet))

    # 4) Ensure season exists + correct type (for partitioning)
    if "season" not in sdf.columns:
        sdf = sdf.withColumn("season", F.lit(int(season)).cast(T.IntegerType()))
    else:
        sdf = sdf.withColumn("season", F.col("season").cast(T.IntegerType()))

    # 5) Minimal key checks (fail early if broken)
    required = ["game_id", "play_id"]
    missing = [c for c in required if c not in sdf.columns]
    if missing:
        raise ValueError(f"Missing required columns in pbp: {missing}")

    null_game_id = sdf.filter(F.col("game_id").isNull()).count()
    null_play_id = sdf.filter(F.col("play_id").isNull()).count()
    if null_game_id > 0 or null_play_id > 0:
        raise ValueError(
            f"Found null keys in pbp: null game_id={null_game_id}, null play_id={null_play_id}"
        )

    # 6) Write Bronze (Spark dataset partitioned by season)
    out_dir = write_bronze_parquet_spark(
        sdf,
        dataset="pbp",
        mode="overwrite",
        partition_by=("season",),
    )

    print(f"Wrote pbp Bronze dataset to: {out_dir} (season={season})")

    spark.stop()

    # 7) Clean up temp extract (optional)
    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)
