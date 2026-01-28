import pandas as pd
from sqlalchemy import create_engine

# 1️⃣ Read the Parquet file
df = pd.read_parquet("green_tripdata_2025-11.parquet")
df2 = pd.read_csv("taxi_zone_lookup.csv")
# 2️⃣ Connect to Postgres
engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5432/ny_taxi_hw")

# 3️⃣ Load into table
df.to_sql("green_trips", engine, if_exists="replace", index=False)
df2.to_sql("taxi_zone_lookup", engine, if_exists="replace", index=False)

print(f"Loaded {len(df)} rows into 'green_trips' table!")
print(f"Loaded {len(df2)} rows into 'taxi_zone_lookup' table!")

