import pandas as pd
import os

# Specify the relative path to the directory from the current working directory
pbp_directory = 'data/pbp'  # Replace with your relative path

# Create the absolute path by joining the current working directory with the relative path
directory_path = os.path.join(os.getcwd(), pbp_directory)

# Get the directory name
directory_name = os.path.basename(directory_path)

# List the files in the directory
files = os.listdir(directory_path)

dfs = []

total_size = 0

for pbp_file in os.listdir(directory_path):
    if pbp_file.endswith(".parquet"):

        year_str = pbp_file.split('_')[-1].split('.')[0]
        if year_str.isdigit() and len(year_str) == 4:
            year = int(year_str)
            if year >= 2017:
                file_path = os.path.join(directory_path, pbp_file)
                
                
                # Read the Parquet file into a dataframe
                df = pd.read_parquet(file_path)
                total_size += df.shape[0]
                # Append the dataframe to the list
                dfs.append(df)


all_pbp = pd.concat(dfs, ignore_index=True)

print(f"All PBP Data: {all_pbp.shape[0]}")
print(f"supposed to be: {total_size}")

all_pbp.to_csv("data/processed/pbp_since_2017.csv")