import argparse

from nfl_lakehouse.common.bronze_ingest import ingest_bronze
from nfl_lakehouse.sources.nflreadpy_source import load_rosters


def main(season: int):
    ingest_bronze(
        dataset="rosters",
        season=season,
        loader=load_rosters,
        required_keys=["gsis_id"],
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)
