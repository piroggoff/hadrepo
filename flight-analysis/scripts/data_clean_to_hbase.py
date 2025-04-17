from src.cleaning import CleanProcessor
from src.writing import WriteProcessor

CleanP = CleanProcessor()
WriteP = WriteProcessor(host="localhost", port=9090, table_name="flights")

df = CleanP.process_pipeline()

WriteP.write_dataframe_to_hbase(df)
