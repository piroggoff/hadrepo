from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import json

def start_and_read():
    # Инициализация Spark
    spark = SparkSession.builder.appName("FlightDataToHBase").getOrCreate()

    # 1. Загрузка данных
    df = spark.read.option('header', 'true').csv("../data/raw/itineraries.csv")

    return df

# 2. Очистка данных
def clean_data(df):
    # Удаление дубликатов
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


# df_cleaned = clean_data(df)


# 3. Обработка множественных сегментов
def process_segments(df):
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


#df_processed = process_segments(df_cleaned)


# 4. Подготовка для HBase
def prepare_hbase_data(df):
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


# hbase_df = prepare_hbase_data(df_processed)

# hbase_df.show(5, truncate=False)

def save_to_data(df,mode):
    if mode == 1:
        df.write.mode('overwrite').parquet('../data/cleaned')

    if mode == 2:
        df.coalesce(1).write.mode('overwrite').parquet('../data/cleaned')


# 6. Сохранение (раскомментируйте когда будете готовы)
def save_to_hbase(df):
    catalog = json.dumps({
        "table": {"namespace": "default", "name": "flight_data"},
        "rowkey": "rowkey",
        "columns": {
            "rowkey": {"cf": "rowkey", "col": "rowkey", "type": "string"},
            **{
                f"{col}": {"cf": col.split("_")[0], "col": "_".join(col.split("_")[1:]), "type": "string"}
                for col in df.columns
                if col != "rowkey"
            }
        }
    })
#sosi
    (df.write
      .format("org.apache.hadoop.hbase.spark")
      .option("catalog", catalog)
      .option("hbase.spark.use.hbasecontext", "false")
      .save())

