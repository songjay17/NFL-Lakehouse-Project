from dagster import Definitions, job, op, Field, Int

from nfl_lakehouse.ingest.ingest_schedules import main as ingest_schedules_main
from nfl_lakehouse.spark.clean_schedules import main as clean_schedules_main

from nfl_lakehouse.ingest.ingest_pbp import main as ingest_pbp_main
from nfl_lakehouse.spark.clean_pbp import main as clean_pbp_main

from nfl_lakehouse.ingest.ingest_rosters import main as ingest_rosters_main
from nfl_lakehouse.spark.clean_rosters import main as clean_rosters_main

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

@op(config_schema={"season": int})
def ingest_pbp(context):
    season = context.op_config["season"]
    ingest_pbp_main(season=season)

@op(config_schema={"season": int})
def clean_pbp(context):
    season = context.op_config["season"]
    clean_pbp_main(season=season)

@job
def pbp_bronze_to_silver():
    ingest_pbp()
    clean_pbp()

@op(config_schema={"season": Field(Int, description="NFL season year (e.g. 2024)")})
def ingest_rosters(context):
    season = context.op_config["season"]
    ingest_rosters_main(season=season)

@op(config_schema={"season": Field(Int, description="NFL season year (e.g. 2024)")})
def clean_rosters(context):
    season = context.op_config["season"]
    clean_rosters_main(season=season)

@job
def rosters_bronze_to_silver():
    ingest_rosters()
    clean_rosters()

defs = Definitions(jobs=[schedules_bronze_to_silver, pbp_bronze_to_silver, rosters_bronze_to_silver])
