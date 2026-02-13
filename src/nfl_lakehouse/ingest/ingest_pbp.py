import argparse
import nfl_data_py as nfl
from nfl_lakehouse.common.io import write_bronze_parquet

def main(season: int):
    df = nfl.import_pbp_data([season])

    #basic sanity checks
    required = ["game_id", "play_id"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in pbp: {missing}")
    
    if df["game_id"].isna().any():
        raise ValueError("Found null game_id values in pbp")
    if df["play_id"].isna().any():
        raise ValueError("Found null play_id values in pbp")
    
    if df.duplicated(subset=["game_id", "play_id"]).any():
        print("WARNING: duplicate (game_id, play_id) rows found, will take care of it later in Silver")

    out = write_bronze_parquet(df, dataset="pbp", partition=f"season={season}")
    print(f"Wrote {len(df):,} rows to {out}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)