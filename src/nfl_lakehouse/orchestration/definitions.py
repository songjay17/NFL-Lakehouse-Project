from dagster import Definitions, job, op, Field, Int, Nothing

from nfl_lakehouse.ingest.ingest_schedules import main as ingest_schedules_main
from nfl_lakehouse.spark.clean_schedules import main as clean_schedules_main

from nfl_lakehouse.ingest.ingest_pbp import main as ingest_pbp_main
from nfl_lakehouse.spark.clean_pbp import main as clean_pbp_main

from nfl_lakehouse.ingest.ingest_rosters import main as ingest_rosters_main
from nfl_lakehouse.spark.clean_rosters import main as clean_rosters_main

@op(config_schema={"season": Field(Int, description="NFL season year (e.g. 2024)")})
def ingest_schedules(context) -> str:
    season = context.op_config["season"]
    ingest_schedules_main(season=season)
    return f"season={season}"

@op(config_schema={"season": Field(Int, description="NFL season year (e.g. 2024)")})
def clean_schedules(context, _ingest_done: str):
    season = context.op_config["season"]
    clean_schedules_main(season=season)

@job
def schedules_bronze_to_silver():
    clean_schedules(ingest_schedules())

@op(config_schema={"season": int})
def ingest_pbp(context) -> str:
    season = context.op_config["season"]
    ingest_pbp_main(season=season)
    return f"season={season}"  # any small token is fine

@op(config_schema={"season": int})
def clean_pbp(context, _ingest_done: str):
    season = context.op_config["season"]
    clean_pbp_main(season=season)

@job
def pbp_bronze_to_silver():
    clean_pbp(ingest_pbp())

@op(config_schema={"season": Field(Int, description="NFL season year (e.g. 2024)")})
def ingest_rosters(context):
    season = context.op_config["season"]
    ingest_rosters_main(season=season)
    return f"season={season}"

@op(config_schema={"season": Field(Int, description="NFL season year (e.g. 2024)")})
def clean_rosters(context, _ingest_done: str):
    season = context.op_config["season"]
    clean_rosters_main(season=season)

@job
def rosters_bronze_to_silver():
    clean_rosters(ingest_rosters())

defs = Definitions(jobs=[schedules_bronze_to_silver, pbp_bronze_to_silver, rosters_bronze_to_silver])
