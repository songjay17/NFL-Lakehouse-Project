import argparse
from pyspark.sql import DataFrame, functions as F, types as T

from nfl_lakehouse.common.silver_clean import clean_silver


def _transform(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("game_id").isNotNull())
        .dropDuplicates(["game_id"])
        .withColumn("season", F.col("season").cast(T.IntegerType()))
        .transform(lambda d: d.withColumn("week", F.col("week").cast(T.IntegerType())) if "week" in d.columns else d)
        .transform(lambda d: d.withColumn("home_team", F.upper(F.col("home_team"))) if "home_team" in d.columns else d)
        .transform(lambda d: d.withColumn("away_team", F.upper(F.col("away_team"))) if "away_team" in d.columns else d)
    )


def main(season: int):
    clean_silver(
        dataset="schedules",
        bronze_dataset="schedules",
        silver_dataset="schedules_clean",
        season=season,
        transform=_transform,
        null_rate_checks=[
            ("home_team", 0.10),
            ("away_team", 0.10),
        ],
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)
