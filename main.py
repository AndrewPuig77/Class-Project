import pandas as pd
import numpy as np
import pymongo
import glob
import os
from pymongo.errors import BulkWriteError


# -------- Load and Combine CSV Files --------
csv_files = glob.glob("source_data/*.csv")

df_list = [pd.read_csv(f) for f in csv_files]
df = pd.concat(df_list, ignore_index=True)


# print("Columns in dataset:")
# print(df.columns.tolist())

print("Loaded files:", [os.path.basename(f) for f in csv_files])
print("Total rows:", len(df))
print(df.head())


# columns to z-score
NUMERIC_COLS = ["Temperature (c)", "Salinity (ppt)", "ODO mg/L"]

# ensure numeric (non-numeric -> NaN)
df[NUMERIC_COLS] = df[NUMERIC_COLS].apply(pd.to_numeric, errors="coerce")

# compute z-scores across the whole combined dataset
means = df[NUMERIC_COLS].mean()
stds = df[NUMERIC_COLS].std(ddof=0) 
stds = stds.replace(0, np.nan) # avoid divide-by-zero
z = (df[NUMERIC_COLS] - means) / stds

THRESH = 3.0
is_outlier = (z.abs() > THRESH).any(axis=1)

# report
total_rows = len(df)
removed_rows = int(is_outlier.sum())
remaining_rows = total_rows - removed_rows

print("=== Cleaning Report ===")
print(f"Total rows originally:          {total_rows}")
print(f"Rows removed as outliers:       {removed_rows}")
print(f"Rows remaining after cleaning:  {remaining_rows}")

# drop outliers
df_clean = df.loc[~is_outlier].copy()
df_clean = df_clean.dropna(subset=NUMERIC_COLS)

df_clean.to_csv("cleaned.csv", index=False)
print("Cleaned data saved to cleaned.csv")



# -------- Save to MongoDB --------
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["water_quality_data"]
collection = db["asv_1"]

collection.delete_many({})
    
records = df_clean.to_dict("records")
if records:  
    collection.insert_many(records)


print("Total documents in collection:", collection.count_documents({}))
print("First document:")
print(collection.find_one())