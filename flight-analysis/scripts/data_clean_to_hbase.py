from src.cleaning import start_and_read, clean_data, prepare_hbase_data, save_to_data, process_segments
from src.writing import save_to_hbase
import glob
from src.config import Paths
from src.test import parquet_to_hbase
import os

df = start_and_read()
df = clean_data(df)
df = process_segments(df)
df = prepare_hbase_data(df)
save_to_data(df,1)


files= glob.glob(os.path.join(Paths.DATA_CLEANED, '*.parquet'))

#
#
for file in files:
    parquet_to_hbase(file, "flights")
