import happybase
import logging
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WriteProcessor:

    def __init__(self, host: str = 'localhost', port: int = 9090, table_name="flights"):
        """"Initialisation HBase connect throught the Thrift"""
        self.table_name = table_name
        self.connection = self._create_connection(host, port)
        self.table = self._get_table()

    @staticmethod
    def _create_connection(host: str, port: int) -> happybase.Connection:
        return happybase.Connection(host=host, port=port)

    def _get_table(self):
        """
        Получение объекта таблицы. Возбуждает исключение, если таблица отсутствует.
        """
        if self.table_name.encode() not in self.connection.tables():
            raise ValueError(f"Таблица '{self.table_name}' не найдена в HBase.")
        return self.connection.table(self.table_name)

    def write_dataframe_to_hbase(self, df: DataFrame):
        """
        Записывает DataFrame в таблицу HBase. Предполагается, что каждая строка имеет:
        - колонку 'rowkey'
        - остальные колонки соответствуют формату 'cf_column'
        """
        data = df.collect()

        for row in data:
            rowkey = row['rowkey']
            hbase_data = {}

            for key, value in row.asDict().items():
                if key == 'rowkey' or value is None:
                    continue
                hbase_data[key.encode()] = str(value).encode()

            self.table.put(rowkey.encode(), hbase_data)
