import sys
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.orc as orc

'''
parquet_file = pq.ParquetFile(sys.argv[1])
print(parquet_file.schema)
'''

orc_name = os.path.splitext(sys.argv[1])[0] + ".orc"

table = pq.read_table(sys.argv[1])

print("Writing ", orc_name)
orc.write_table(table, orc_name)
