import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

def main(season: int):
    spark = (
        SparkSession.builder
            .appName("nfl_lakehouse_clean_schedules")
            .master("local[*]")
            .getOrCreate()
    )

    bronze_path = Path(f"data/bronze/schedules/season={season}/data.parquet")
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")
    
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

    out_dir = Path(f"data/silver/schedules_clean/season={season}")
    out_dir.mkdir(parents=True, exist_ok=True)

    (
        cleaned
        .repartition(1)
        .write.mode("overwrite")
        .parquet(str(out_dir))
    )

    print(f"Wrote Silver schedules to {out_dir}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)