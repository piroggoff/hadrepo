from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()



# Чтение файла
df = spark.read.parquet("/home/femida/bigdata_project/hadrepo/flight-analysis/data/cleaned/part-00000-356e44bc-c0ea-4aee-87f4-a2796bf8f2cd-c000.snappy.parquet")

# Просмотр данных
df.show(truncate = True, vertical = True, n =5)  # Первые 5 строк
df.printSchema()  # Структура данных

