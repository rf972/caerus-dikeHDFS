import pandas as pd
import pyarrow.parquet as pq

#df = pd.read_csv('lineitem.csv')

#df.to_parquet('lineitem_srg.parquet', row_group_size=7000000)
#df.to_parquet('lineitem.parquet')

parquet_file = pq.ParquetFile('lineitem.snappy.parquet')

print(parquet_file.metadata)
print(parquet_file.schema)
