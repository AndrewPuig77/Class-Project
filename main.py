import pandas as pd
import glob
import os

csv_files = glob.glob("source_data/*.csv")

df_list = [pd.read_csv(f) for f in csv_files]
df = pd.concat(df_list, ignore_index=True)
print(df.head())

# Quick check
print("Loaded files:", [os.path.basename(f) for f in csv_files])
print("Total rows:", len(df))
print(df.head())