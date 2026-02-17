import argparse
from pathlib import Path
from typing import Iterable, Optional
from pyspark.sql import SparkSession, DataFrame, functions as F

def get_spark(app_name:  str = "nfl_lakehouse") -> SparkSession:
    spark = (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .getOrCreate())
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")    
    return spark

def bronze_path(dataset: str) -> str:
    path = Path("data") / "bronze" / dataset
    path.mkdir(parents=True, exist_ok=True)
    return str(path)

def silver_path(dataset: str) -> str:
    path = Path("data") / "silver" / dataset
    path.mkdir(parents=True, exist_ok=True)
    return str(path)

def write_bronze_parquet_spark(df: DataFrame, dataset: str, *, mode: str = "overwrite", partition_by: Optional[Iterable[str]] = ("season",),) -> str:
    out_dir = bronze_path(dataset)

    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(*list(partition_by))

    writer.save(out_dir)
    return out_dir

def write_silver_parquet_spark(df: DataFrame, dataset: str, *, mode: str = "overwrite", partition_by: Optional[Iterable[str]] = ("season",), add_loaded_at: bool = True, loaded_at_col: str = "silver_loaded_at") -> str:
    out_dir = silver_path(dataset)

    if add_loaded_at and loaded_at_col:
        df = df.withColumn(loaded_at_col, F.current_timestamp())
    
    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(*list(partition_by))
    
    writer.save(out_dir)
    return out_dir