import happybase
import pandas as pd
from itertools import chain
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_hbase_table(connection, table_name, column_families):
    """Создает таблицу в HBase если она не существует"""
    try:
        tables = connection.tables()
        if table_name.encode() not in tables:
            connection.create_table(
                table_name,
                {cf: {} for cf in column_families}
            )
            logger.info(f"Таблица {table_name} создана")
    except Exception as e:
        logger.error(f"Ошибка создания таблицы: {str(e)}")
        raise


def process_map_column(map_data):
    """Обрабатывает map-колонки и извлекает данные в формате (qualifier, value)"""
    if not map_data:
        return []

    # Пример данных: {'cf1:startingAirport': 'LAX'} -> ('startingAirport', 'LAX')
    return [
        (key.split(':')[1], str(value))
        for key, value in map_data.items()
        if ':' in key
    ]


def parquet_to_hbase(parquet_path, table_name, host='localhost'):
    # Подключение к HBase
    connection = happybase.Connection(host)

    # Определяем семейства колонок из структуры данных
    column_families = {'cf1', 'cf2', 'cf3'}

    # Создаем таблицу если нужно
    create_hbase_table(connection, table_name, column_families)

    # Получаем объект таблицы
    table = connection.table(table_name)

    # Читаем Parquet файл
    df = pd.read_parquet(parquet_path)

    # Конвертируем NaN в None
    df = df.where(pd.notnull(df), None)

    # Обрабатываем каждую строку
    for index, row in df.iterrows():
        try:
            row_key = str(row['rowkey'])

            # Подготовка данных для вставки
            data = {}

            # Обрабатываем каждую колонку
            for column in df.columns:
                if column == 'rowkey':
                    continue

                # Извлекаем семейство колонок
                cf = column.split('_')[0]

                # Обрабатываем map-данные
                map_items = process_map_column(row[column])

                # Добавляем в данные
                for qualifier, value in map_items:
                    column_name = f"{cf}:{qualifier}"
                    data[column_name.encode()] = str(value).encode()

            # Вставляем данные
            if data:
                table.put(row_key.encode(), data)
                logger.debug(f"Вставлена запись {row_key}")

        except Exception as e:
            logger.error(f"Ошибка обработки строки {index}: {str(e)}")
            continue

    connection.close()
    logger.info("Все данные успешно загружены в HBase")


if __name__ == "__main__":
    # Настройки
    PARQUET_PATH = "/home/femida/bigdata_project/hadrepo/flight-analysis/data/cleaned/part-00000-356e44bc-c0ea-4aee-87f4-a2796bf8f2cd-c000.snappy.parquet"
    HBASE_TABLE = "sosi"

    # Запуск импорта
    parquet_to_hbase(PARQUET_PATH, HBASE_TABLE)