from dagster import Definitions, job, op, Field, Int

from nfl_lakehouse.ingest.ingest_schedules import main as ingest_schedules_main
from nfl_lakehouse.spark.clean_schedules import main as clean_schedules_main

@op(config_schema={"season": Field(Int, description="NFL season year (e.g. 2024)")})
def ingest_schedules(context):
    season = context.op_config["season"]
    ingest_schedules_main(season=season)

@op(config_schema={"season": Field(Int, description="NFL season year (e.g. 2024)")})
def clean_schedules(context):
    season = context.op_config["season"]
    clean_schedules_main(season=season)

@job
def schedules_bronze_to_silver():
    ingest_schedules()
    clean_schedules()

defs = Definitions(jobs=[schedules_bronze_to_silver])
