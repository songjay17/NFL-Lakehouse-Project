import argparse

from nfl_lakehouse.common.bronze_ingest import ingest_bronze
from nfl_lakehouse.sources.nflreadpy_source import load_schedules


def main(season: int):
    ingest_bronze(
        dataset="schedules",
        season=season,
        loader=load_schedules,
        required_keys=["game_id"],
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)
