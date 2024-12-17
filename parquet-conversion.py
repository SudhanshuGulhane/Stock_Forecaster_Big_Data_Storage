import pandas as pd

ssv_file = 'complaints.ssv'
df = pd.read_csv(ssv_file, sep=None, engine='python')

parquet_file = 'complaints.parquet'
df.to_parquet(parquet_file, engine='pyarrow')

print(f"File successfully converted to {parquet_file}")

df.columns = df.columns.str.replace(' ', '_')
df.columns = df.columns.str.replace('-', '_')
df.columns = df.columns.str.replace('?', '', regex=False)

df.to_parquet('complaints.parquet')