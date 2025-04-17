# src/writing.py
import happybase
import logging
from pyspark.sql import DataFrame


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




class WriteProcessor:

    def __init__(self, host: str = 'localhost', port: int = 9090, table_name: str = "flights", column_families={'cf1', 'cf2', 'cf3'}):
        """Инициализация соединения с HBase через Thrift и создание таблицы, если необходимо"""
        self.table_name = table_name
        self.connection = self._create_connection(host, port)



    @staticmethod
    def _create_connection(host: str, port: int) -> happybase.Connection:
        return happybase.Connection(host=host, port=port)

    def _create_hbase_table(self,connection, table_name, column_families):
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

    def _process_map_column(self,map_data):
        """Обрабатывает map-колонки и извлекает данные в формате (qualifier, value)"""
        if not map_data:
            return []

        # Пример данных: {'cf1:startingAirport': 'LAX'} -> ('startingAirport', 'LAX')
        return [
            (key.split(':')[1], str(value))
            for key, value in map_data.items()
            if ':' in key
        ]

    def save_to_hbase(self,table_name, df: DataFrame, host = "localhost"):
        connection = self._create_connection(host)
        column_families = {'cf1', 'cf2', 'cf3'}
        self._create_hbase_table(connection,table_name,column_families)

        table = connection.tables(table_name)

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
                    map_items = self._process_map_column(row[column])

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