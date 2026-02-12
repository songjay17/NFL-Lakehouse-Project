from dagster import Definitions, job, op

from nfl_lakehouse.ingest.ingest_schedules import main as ingest_schedules_main
from nfl_lakehouse.spark.clean_schedules import main as clean_schedules_main

@op
def ingest_schedules():
    # keep this simple for now; later we’ll parameterize season via config
    ingest_schedules_main(season=2024)

@op
def clean_schedules():
    clean_schedules_main(season=2024)

@job
def schedules_bronze_to_silver():
    ingest_schedules()
    clean_schedules()

defs = Definitions(jobs=[schedules_bronze_to_silver])
