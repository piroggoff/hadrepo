# src/writing.py
import happybase
import logging
from pyspark.sql import DataFrame

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


class WriteProcessor:

    def __init__(self, host: str = 'localhost', port: int = 9090, table_name: str = "flights", column_families=None):
        """Инициализация соединения с HBase через Thrift и создание таблицы, если необходимо"""
        self.table_name = table_name
        self.connection = self._create_connection(host, port)
        # Создаем таблицу с заданными column_families
        if column_families is None:
            column_families = {'cf1','cf2','cf3'}
        create_hbase_table(self.connection, self.table_name, column_families)
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
        data = df.collect()
        for row in data:
            rowkey = row['rowkey']
            hbase_data = {}

            for key, value in row.asDict().items():
                if key == 'rowkey' or value is None:
                    continue

                # Если value — dict (MapType из Spark) — развернём по элементам
                if isinstance(value, dict):
                    for fam_and_qf, cell in value.items():
                        # fam_and_qf уже содержит "cfX:qualifier"
                        hbase_data[fam_and_qf.encode()] = str(cell).encode()
                else:
                    # Преобразуем "cfX_qualifier" → "cfX:qualifier"
                    cf, qualifier = key.split('_', 1)
                    fam_and_qf = f"{cf}:{qualifier}"
                    hbase_data[fam_and_qf.encode()] = str(value).encode()

            self.table.put(rowkey.encode(), hbase_data)
