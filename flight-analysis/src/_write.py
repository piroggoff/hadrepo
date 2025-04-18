# src/writing.py
import happybase
import logging
from pyspark.sql import DataFrame


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _write_partition_to_hbase(rows):
    """
    Функция, выполняемая на каждой Spark-партиции.
    Подключается к HBase, собирает батч и отправляет его раз в партицию.
    """
    # Открываем соединение и получаем table-подключение — раз на партицию


    connection = happybase.Connection(host='localhost', port=9090)
    table = connection.table('flights')
    batch = table.batch()

    for row in rows:
        try:
            # Берём ключ
            row_key = str(row['rowkey'])

            # Сбор данных для одной строки
            data = {}
            for column, value in row.asDict().items():
                if column == 'rowkey' or value is None:
                    continue

                cf = column.split('_', 1)[0]
                # Ваша логика разбора map-колонки
                map_items = WriteProcessor._process_map_column(value)
                for qualifier, v in map_items:
                    col_name = f"{cf}:{qualifier}".encode()
                    data[col_name] = str(v).encode()

            # Кладём в batch, но не шлём сразу
            if data:
                batch.put(row_key.encode(), data)
                logger.debug(f"Added to batch: {row_key}")

        except Exception as e:
            logger.error(f"Ошибка обработки rowkey={row_key}: {e}")
            # продолжаем партицию, не ломая весь процесс
            continue

    # Отправляем все накопленные записи одним запросом
    batch.send()
    connection.close()


class WriteProcessor:

    def __init__(self, host: str = 'localhost', port: int = 9090, table_name: str = "flights"):
        """Инициализация соединения с HBase через Thrift и создание таблицы, если необходимо"""
        self.table_name = table_name
        #self.connection = self._create_connection(host, port)



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

    @staticmethod
    def _process_map_column(map_data):
        """Обрабатывает map-колонки и извлекает данные в формате (qualifier, value)"""
        if not map_data:
            return []

        # Пример данных: {'cf1:startingAirport': 'LAX'} -> ('startingAirport', 'LAX')
        return [
            (key.split(':')[1], str(value))
            for key, value in map_data.items()
            if ':' in key
        ]


    def save_to_hbase(self, df):
        df.foreachPartition(_write_partition_to_hbase)
