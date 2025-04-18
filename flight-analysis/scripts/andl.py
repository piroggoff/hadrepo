from src.cleaning import start_and_read, clean_data, prepare_hbase_data, process_segments
from src.writing import save_to_hbase

df = start_and_read()
df = clean_data(df)
df = process_segments(df)
df = prepare_hbase_data(df)
save_to_hbase(df)
