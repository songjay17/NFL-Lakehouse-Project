import argparse
from pathlib import Path
from typing import Iterable, Optional
from pyspark.sql import SparkSession, DataFrame

def get_spark(app_name:  str = "nfl_lakehouse") -> SparkSession:
    return (
        SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .getOrCreate()
    )

def bronze_path(dataset: str) -> str:
    path = Path("data") / "bronze" / dataset
    path.mkdir(parents=True, exist_ok=True)
    return str(path)

def write_bronze_parquet_spark(df: DataFrame, dataset: str, *, mode: str = "overwrite", partition_by: Optional[Iterable[str]] = ("season")) -> str:
    out_dir = bronze_path(dataset)

    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(*list(partition_by))

    writer.save(out_dir)
    return out_dir