import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T, Window

from nfl_lakehouse.common.spark_io import get_spark, write_silver_parquet_spark

def main(season: int):
    spark = get_spark(app_name="nfl_lakehouse_clean_rosters")

    bronze_path = Path(f"data/bronze/rosters/season={season}")
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze path not found: {bronze_path}")
    
    #Creating the dataframe through PySpark
    df = spark.read.parquet(str(bronze_path))

    df = (df
        .withColumnRenamed("gsis_id", "player_id")
        .withColumn("season", F.lit(int(season))))

    if "player_id" not in df.columns:
        raise ValueError("rosters missing key field player_id")
    
    key_cols = ["player_id", "season"]

    if "team" in df.columns:
        df = df.withColumn("team", F.upper(F.trim(F.col("team"))))
    if "team_abbr" in df.columns:
        df = df.withColumn("team_abbr", F.upper(F.trim(F.col("team_abbr"))))
    if "position" in df.columns:
        df = df.withColumn("position", F.upper(F.trim(F.col("position"))))

    for c in ["player_name", "first_name", "last_name", "full_name"]:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    
    df = df.filter(F.col("player_id").isNotNull())
    
    candidate_cols = [
        "team", "team_abbr", "position",
        "player_name", "full_name", "first_name", "last_name",
        "college", "height", "weight", "birth_date", "age",
    ]

    present = [c for c in candidate_cols if c in df.columns]

    if present:
        completeness = sum(F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for c in present) #This counts up the number of columns that are not NULL so that we can build a completeness score and dedupe rows this way.
        df = df.withColumn("_completeness", completeness)
    else:
        #catch_all
        df = df.withColumn("_completeness", F.lit(0)) #if none of those columns are even present in the first place, than just auto 0.
    
    w = Window.partitionBy(*key_cols).orderBy(F.col("_completeness").desc())

    cleaned = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_completeness")
    )

    out_dir = write_silver_parquet_spark(
        cleaned,
        dataset="schedules_clean",
        partition_by=("season",),
        add_loaded_at=True,
        loaded_at_col="silver_loaded_at"
    )

    print(f"Wrote Silver schedules to {out_dir} (season = {season})")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)