from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from typing import Optional
import json


class FlightDataProcessor:

    def __init__(self, spark: Optional[SparkSession] = None):
        # Инициализация Spark
        self.spark = spark or self._create_spark_session()
        
    @staticmethod
    def _create_spark_session() -> SparkSession:
        """Создает и возвращает Spark сессию"""
        return SparkSession.builder \
            .appName("FlightDataToHBase") \
            .getOrCreate()
    
    def process_pipeline(self, input_path: Optional[str] = "../data/raw/itineraries.csv") -> 'pyspark.sql.DataFrame':
        df = self._load_data(input_path)
        df_cleaned = self._clean_data(df)
        df_processed = self._process_segments(df_cleaned)
        df_hbase = self._prepare_hbase_data(df_processed)
        return df_hbase

    def _load_data(self, path: str) -> 'pyspark.sql.DataFrame':
        df = self.spark.read.option('header', 'true').csv(path)
        return df

    def _clean_data(self, df) -> 'pyspark.sql.DataFrame':
        df = df.dropDuplicates(["legId"])
    
        # Удаление строк с критически важными null значениями
        critical_columns = ["legId", "flightDate", "startingAirport", "destinationAirport"]
        df = df.dropna(subset=critical_columns)
    
        # Триминг пробелов для всех строковых колонок
        for col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
    
        # Замена пустых строк на null
        for col_name in df.columns:
            df = df.withColumn(col_name,
                               F.when(F.col(col_name) == "", None).otherwise(F.col(col_name)))
    
        return df

    def _process_segments(self, df) -> 'pyspark.sql.DataFrame':
        # Автоматическое определение колонок с сегментами
        segment_columns = [c for c in df.columns if c.startswith("segments") or c.startswith("segments")]
    
        # Преобразование всех сегментных колонок в массивы
        for col_name in segment_columns:
            df = df.withColumn(col_name,
                               F.split(F.col(col_name), "\|\|"))
    
        # Добавление количества сегментов
        df = df.withColumn("num_segments",
                           F.size(F.col("segmentsDepartureAirportCode")))
    
        # Проверка согласованности сегментов
        for col_name in segment_columns:
            df = df.withColumn(f"{col_name}_count",
                               F.size(F.col(col_name)))
    
        # Фильтрация некорректных данных (исправленный оператор &)
        condition = (F.col("segmentsDepartureAirportCode_count") == F.col("num_segments")) & \
                    (F.col("segmentsArrivalAirportCode_count") == F.col("num_segments"))
    
        df = df.filter(condition)
    
        return df.drop(*[f"{c}_count" for c in segment_columns])
     
    def _prepare_hbase_data(self, df) -> 'pyspark.sql.DataFrame':
        # Создание rowkey
        df = df.withColumn("rowkey",
                           F.concat_ws("|", F.col("legId"), F.col("flightDate")))
    
        # Создание колонок HBase
        columns_mapping = {
            "cf1": ["startingAirport", "destinationAirport", "totalFare", "baseFare"],
            "cf2": ["segmentsDepartureAirportCode", "segmentsArrivalAirportCode"],
            "cf3": ["segmentsAirlineCode", "segmentsEquipmentDescription"]
        }
    
        # Преобразование в HBase-формат
        hbase_columns = [F.col("rowkey")]
    
        # Простые колонки
        for cf, cols in columns_mapping.items():
            for col_name in cols:
                if not col_name.startswith("segments"):
                    hbase_columns.append(
                        F.create_map(
                            F.lit(f"{cf}:{col_name}"),
                            F.col(col_name).cast("string")
                        ).alias(f"{cf}_{col_name}")
                    )
    
        # Сегментные колонки (преобразуем в JSON)
        segment_columns = [c for c in df.columns if c.startswith("segments")]
        for col_name in segment_columns:
            cf = "cf2" if "Airport" in col_name else "cf3"
            hbase_columns.append(
                F.create_map(
                    F.lit(f"{cf}:{col_name}"),
                    F.to_json(F.col(col_name))
                ).alias(f"{cf}_{col_name}")
            )
    
        return df.select(*hbase_columns)
    
    def save_to_data(df,mode):
        if mode == 1:
            df.write.mode('overwrite').parquet('../data/cleaned')
    
        if mode == 2:
            df.coalesce(1).write.mode('overwrite').parquet('../data/cleaned')