# import requests
import pandas as pd
import datetime
# import os

url = "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_1999.parquet"

year_range = list(range(1999,datetime.date.today().year+1))

print(year_range)
# df = pd.read_parquet(url)

# print(df.head()) 

def get_pbp_data(year):
    url = url = f"https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.parquet"
    df = pd.read_parquet(url)
    return df

for year in year_range:
    df = get_pbp_data(year)
    df_scoring_only = df[df['sp'] == 1]
    df.to_parquet(f"data/pbp/play_by_play_{year}.parquet")
    df_scoring_only.to_parquet(f"data/pbp/scoring_only/play_by_play_{year}_scoring_only.parquet")
    print(f"Successfully Imported Play by Play Data from {year}")
    print(f"size: {df.shape}\n")
    print(f"Scoring plays: {df_scoring_only.shape}\n")



