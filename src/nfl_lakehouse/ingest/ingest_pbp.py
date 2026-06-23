import argparse

from nfl_lakehouse.common.bronze_ingest import ingest_bronze
from nfl_lakehouse.sources.nflreadpy_source import load_pbp


def main(season: int):
    ingest_bronze(
        dataset="pbp",
        season=season,
        loader=load_pbp,
        required_keys=["game_id", "play_id"],
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)
