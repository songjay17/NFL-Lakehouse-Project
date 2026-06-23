import argparse
from pyspark.sql import DataFrame, functions as F, types as T

from nfl_lakehouse.common.silver_clean import clean_silver


def _transform(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("game_id").isNotNull())
        .filter(F.col("play_id").isNotNull())
        # cast keys before dedup so type mismatches don't create phantom duplicates
        .withColumn("play_id", F.col("play_id").cast(T.IntegerType()))
        .dropDuplicates(["game_id", "play_id"])
        .transform(lambda d: d.withColumn("season", d["season"].cast(T.IntegerType())))
        .transform(lambda d: d.withColumn("week", F.col("week").cast(T.IntegerType())) if "week" in d.columns else d)
        .transform(lambda d: d.withColumn("qtr", F.col("qtr").cast(T.IntegerType())) if "qtr" in d.columns else d)
        .transform(lambda d: d.withColumn("down", F.col("down").cast(T.IntegerType())) if "down" in d.columns else d)
        .transform(lambda d: d.withColumn("ydstogo", F.col("ydstogo").cast(T.IntegerType())) if "ydstogo" in d.columns else d)
        .transform(lambda d: d.withColumn("yardline_100", F.col("yardline_100").cast(T.IntegerType())) if "yardline_100" in d.columns else d)
        .transform(lambda d: d.withColumn("posteam", F.upper(F.col("posteam"))) if "posteam" in d.columns else d)
        .transform(lambda d: d.withColumn("defteam", F.upper(F.col("defteam"))) if "defteam" in d.columns else d)
    )


def main(season: int):
    clean_silver(
        dataset="pbp",
        bronze_dataset="pbp",
        silver_dataset="pbp_clean",
        season=season,
        transform=_transform,
        null_rate_checks=[
            ("posteam", 0.10),
            ("defteam", 0.10),
        ],
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)
