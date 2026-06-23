import argparse
from pyspark.sql import DataFrame, functions as F, types as T, Window

from nfl_lakehouse.common.silver_clean import clean_silver


def _transform(df: DataFrame) -> DataFrame:
    if "gsis_id" not in df.columns:
        raise ValueError("rosters missing key field gsis_id")

    df = (df
        .withColumnRenamed("gsis_id", "player_id")
        .withColumn("season", F.col("season").cast(T.IntegerType())))

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
        completeness = sum(F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for c in present)
        df = df.withColumn("_completeness", completeness)
    else:
        df = df.withColumn("_completeness", F.lit(0))

    w = Window.partitionBy("player_id", "season").orderBy(F.col("_completeness").desc())

    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_completeness")
    )


def main(season: int):
    clean_silver(
        dataset="rosters",
        bronze_dataset="rosters",
        silver_dataset="rosters_clean",
        season=season,
        transform=_transform,
        null_rate_checks=[
            ("position", 0.10),
            ("team", 0.10),
        ],
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)
