import pandas as pd
import os
from pyspark.sql import SparkSession, DataFrame


def get_data(spark: SparkSession, data_dir: str = None) -> Generator[DataFrame, any, any]:
    
    if not data_dir:
        data_path = f"{os.path.dirname(__file__)}/../data/raw/2"

    files_names = os.listdir(data_dir)

    for file_name in files_names:
        if not file_name.endswith('.csv'):
            raise ValueError('Unsupported format')
        df = spark.read.csv(f"{data_dir}/{file_name}")
        if df.empty:
            raise ValueError('Empty file')
        yield df

def main():
    
    sespark = SparkSession.builder\
        .appName('Sparker')\
        .getOrCreate()
    
    for frame in get_data(sespark):
        frame.show(5)

