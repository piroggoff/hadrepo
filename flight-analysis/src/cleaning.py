# src/cleaning.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()

def drop_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.dropna(subset=columns)

def standardize_column_names(df: DataFrame) -> DataFrame:
    # Преобразует названия столбцов в нижний регистр и заменяет пробелы на "_"
    renamed = [col_name.lower().replace(" ", "_") for col_name in df.columns]
    return df.toDF(*renamed)