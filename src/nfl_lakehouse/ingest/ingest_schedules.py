import argparse
import nfl_data_py as nfl
from nfl_lakehouse.common.io import write_bronze_parquet

def main(season: int):
    df = nfl.import_schedules([season])

    if "game_id" not in df.columns:
        raise ValueError("Game_ID Column is missing from the Dataframe")
    if df["game_id"].isna().any():
        raise ValueError("Some game_id values are NULL")
    if df["game_id"].duplicated().any():
        raise ValueError("Some game_id values are duplicated")
    
    out = write_bronze_parquet(df, dataset = "schedules", partition=f"season={season}")
    print(f"Wrote {len(df):,} rows to {out}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    main(args.season)