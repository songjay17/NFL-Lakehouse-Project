import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

def main(season: int):
    spark = (
        SparkSession.builder
            .appName("nfl_lakehouse_clean_pbp")
            .master("local[*]")
            .getOrCreate()
    )

    bronze_path = Path(f"data/bronze/pbp/season={season}/data.parquet")
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")
    
    #Creating the dataframe through PySpark
    df = spark.read.parquet(str(bronze_path))

    # ---- Silver cleaning (minimal) ----
    # 1) enforce key + drop duplicates
    # 2) standardize team abbreviations
    # 3) cast types for predictable downstream joins
    # 4) add partition columns (season) for consistency

    cleaned = (
        df
        .filter(F.col("game_id").isNotNull())
        .filter(F.col("play_id").isNotNull())
        .dropDuplicates(["game_id", "play_id"])
        .withColumn("season", F.lit(int(season)).cast(T.IntegerType()))
        .withColumn("play_id", F.col("play_id").cast(T.IntegerType()))
        .transform(lambda d: d.withColumn("week", F.col("week").cast(T.IntegerType())) if "week" in d.columns else d)
        .transform(lambda d: d.withColumn("qtr", F.col("qtr").cast(T.IntegerType())) if "qtr" in d.columns else d)
        .transform(lambda d: d.withColumn("down", F.col("down").cast(T.IntegerType())) if "down" in d.columns else d)
        .transform(lambda d: d.withColumn("ydstogo", F.col("ydstogo").cast(T.IntegerType())) if "ydstogo" in d.columns else d)
        .transform(lambda d: d.withColumn("yardline_100", F.col("yardline_100").cast(T.IntegerType())) if "yardline_100" in d.columns else d)
        .transform(lambda d: d.withColumn("posteam", F.upper(F.col("posteam"))) if "posteam" in d.columns else d)
        .transform(lambda d: d.withColumn("defteam", F.upper(F.col("defteam"))) if "defteam" in d.columns else d)
    )

    #Adding pipeline timestamp for lineage/debug
    cleaned = cleaned.withColumn("silver_loaded_at", F.current_timestamp())

    out_dir = Path(f"data/silver/pbp_clean/season={season}")
    out_dir.mkdir(parents=True, exist_ok=True)

    (
        cleaned
        .write.mode("overwrite")
        .parquet(str(out_dir))
    )

    print(f"Wrote Silver play-by-play to {out_dir}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)