import nfl_data_py as nfl

df = nfl.import_seasonal_rosters([2024])
df.dtypes
print(list(df.columns)[:40])