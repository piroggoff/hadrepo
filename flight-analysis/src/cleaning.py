from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import happybase
import json



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
# def prepare_hbase_data(df):
#     # Создание rowkey
#     df = df.withColumn("rowkey",
#                        F.concat_ws("|", F.col("legId"), F.col("flightDate")))
#
#     # Создание колонок HBase
#     # TODO: пересмоьтреть выбор колонок
#     columns_mapping = {
#         "cf1": ["startingAirport", "destinationAirport", "totalFare", "baseFare"],
#         "cf2": ["segmentsDepartureAirportCode", "segmentsArrivalAirportCode"],
#         "cf3": ["segmentsAirlineCode", "segmentsEquipmentDescription"]
#     }
#
#     # Преобразование в HBase-формат
#     hbase_columns = [F.col("rowkey")]
#
#     # Простые колонки
#     for cf, cols in columns_mapping.items():
#         for col_name in cols:
#             if not col_name.startswith("segments"):
#                 hbase_columns.append(
#                     F.create_map(
#                         F.lit(f"{cf}:{col_name}"),
#                         F.col(col_name).cast("string")
#                     ).alias(f"{cf}_{col_name}")
#                 )
#
#     # Сегментные колонки (преобразуем в JSON)
#     segment_columns = [c for c in df.columns if c.startswith("segments")]
#     for col_name in segment_columns:
#         cf = "cf2" if "Airport" in col_name else "cf3"
#         hbase_columns.append(
#             F.create_map(
#                 F.lit(f"{cf}:{col_name}"),
#                 F.to_json(F.col(col_name))
#             ).alias(f"{cf}_{col_name}")
#         )
#
#     return df.select(*hbase_columns)


from pyspark.sql import functions as F


def prepare_hbase_data(df):
    # Создание rowkey
    df = df.withColumn("rowkey", F.concat_ws("|", F.col("legId"), F.col("flightDate")))

    columns_mapping = {
        "cf1": ["startingAirport", "destinationAirport", "totalFare", "baseFare"],
        "cf2": ["segmentsDepartureAirportCode", "segmentsArrivalAirportCode"],
        "cf3": ["segmentsAirlineCode", "segmentsEquipmentDescription"]
    }

    # Создаем структуры для HBase колонок
    hbase_columns = []

    # Обработка простых колонок
    for cf, cols in columns_mapping.items():
        for col_name in cols:
            if not col_name.startswith("segments"):
                hbase_columns.append(
                    F.struct(
                        F.lit(cf).alias("family"),
                        F.lit(col_name).alias("qualifier"),
                        F.col(col_name).cast("string").alias("value")
                    )
                )

    # Обработка сегментных колонок
    segment_columns = [c for c in df.columns if c.startswith("segments")]
    for col_name in segment_columns:
        cf = "cf2" if "Airport" in col_name else "cf3"
        col_type = dict(df.dtypes)[col_name]

        if col_type.startswith("struct") or col_type.startswith("array") or col_type.startswith("map"):
            value_expr = F.to_json(F.col(col_name))
        else:
            value_expr = F.col(col_name).cast("string")

        hbase_columns.append(
            F.struct(
                F.lit(cf).alias("family"),
                F.lit(col_name).alias("qualifier"),
                value_expr.alias("value")
            )
        )

    # Собираем все колонки в массив и разбиваем на строки
    df = df.withColumn("hbase_columns", F.array(*hbase_columns))
    df = df.select("rowkey", F.explode("hbase_columns").alias("column"))

    # Разделение структуры на отдельные поля
    return df.select(
        "rowkey",
        F.col("column.family").alias("family"),
        F.col("column.qualifier").alias("qualifier"),
        F.col("column.value").alias("value")
    )



def save_to_data(df,mode):
    if mode == 1:
        df.write.mode('overwrite').parquet('../data/cleaned')

    if mode == 2:
        df.coalesce(1).write.mode('overwrite').parquet('../data/cleaned')


# 6. Сохранение
#
#   TODO: 1. сохранение в hdfs 2. порт в hbase (лучше сразу созранять в hbase)

def write_partition(partition):
    import happybase
    import socket
    from collections import defaultdict

    try:
        socket.setdefaulttimeout(10)  # ограничить соединение
        connection = happybase.Connection(host='localhost', port=9090, timeout=5000)
        table = connection.table('flight')

        rows = defaultdict(dict)

        for row in partition:
            rowkey = row['rowkey']
            family = row['family']
            qualifier = row['qualifier']
            value = row['value']
            column = f"{family}:{qualifier}"
            rows[rowkey][column] = value.encode('utf-8') if value is not None else b''

        with table.batch() as batch:
            for rowkey, columns in rows.items():
                batch.put(rowkey, columns)

        connection.close()

    except Exception as e:
        import traceback
        print("Exception in write_partition:", traceback.format_exc())
def save_to_hbase(df_hbase, table_name='flight', host='localhost', port=9090):
    # Собираем данные в драйвер

    df_hbase.rdd.foreachPartition(write_partition)

    # data = df_hbase.collect()
    #
    # # Подключение к HBase через Thrift
    # connection = happybase.Connection(host=host, port=port)
    # table = connection.table(table_name)
    #
    # # Сгруппировать данные по rowkey и по column family
    # from collections import defaultdict
    #
    # rows = defaultdict(dict)
    #
    # for row in data:
    #     rowkey = row['rowkey']
    #     family = row['family']
    #     qualifier = row['qualifier']
    #     value = row['value']
    #     column = f"{family}:{qualifier}"
    #     rows[rowkey][column] = value.encode('utf-8') if value is not None else b''
    #
    # # Пакетная вставка
    # with table.batch() as batch:
    #     for rowkey, columns in rows.items():
    #         batch.put(rowkey, columns)
    #
    # connection.close()