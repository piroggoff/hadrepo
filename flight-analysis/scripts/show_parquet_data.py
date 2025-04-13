from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()



# Чтение файла
df = spark.read.parquet("../data/cleaned")

# Просмотр данных
df.show(5)  # Первые 5 строк
df.printSchema()  # Структура данных

