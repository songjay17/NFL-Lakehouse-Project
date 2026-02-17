import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

from nfl_lakehouse.common.spark_io import get_spark, write_silver_parquet_spark

def main(season: int):
    spark = get_spark(app_name="nfl_lakehouse_clean_schedules")

    bronze_path = Path(f"data/bronze/schedules/season={season}")
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze path not found: {bronze_path}")
    
    df = spark.read.parquet(str(bronze_path))

    # ---- Silver cleaning (minimal) ----
    # 1) enforce key + drop duplicates
    # 2) standardize team abbreviations
    # 3) cast types for predictable downstream joins
    # 4) add partition columns (season) for consistency

    cleaned = (
        df
        .filter(F.col("game_id").isNotNull())
        .dropDuplicates(["game_id"])
        .withColumn("season", F.lit(int(season)).cast(T.IntegerType()))
        .withColumn("week", F.col("week").cast(T.IntegerType()))
        .withColumn("home_team", F.upper(F.col("home_team")))
        .withColumn("away_team", F.upper(F.col("away_team")))
    )

    out_dir = write_silver_parquet_spark(
        cleaned,
        dataset="schedules_clean",
        partition_by=("season",),
        add_loaded_at=True,
    )

    print(f"Wrote Silver schedules to {out_dir} (season={season})")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)