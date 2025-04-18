# writing.py

import happybase
import logging
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _create_connection(host: str, port: int) -> happybase.Connection:
    return happybase.Connection(host=host, port=port)


def _create_hbase_table(connection, table_name, column_families):
    """Создает таблицу в HBase, если она не существует."""
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


def _process_map_column(map_data):
    if not isinstance(map_data, dict) or not map_data:
        return []
    return [
        (key.split(':', 1)[1], str(val))
        for key, val in map_data.items()
        if ':' in key
    ]


def _write_partition_to_hbase(rows):
    """Функция, выполняемая на каждой Spark-партиции."""
    connection = _create_connection('localhost', 9090)
    table = connection.table('flights')
    batch = table.batch()

    for row in rows:
        try:
            row_key = str(row['rowkey'])
            data = {}

            for column, value in row.asDict().items():
                if column == 'rowkey' or value is None:
                    continue

                cf = column.split('_', 1)[0]
                map_items = _process_map_column(value)

                for qualifier, v in map_items:
                    col_name = f"{cf}:{qualifier}".encode()
                    data[col_name] = str(v).encode()

            if data:
                batch.put(row_key.encode(), data)
                logger.debug(f"Added to batch: {row_key}")

        except Exception as e:
            logger.error(f"Ошибка обработки rowkey={row_key}: {e}")
            continue

    batch.send()
    connection.close()


def save_to_hbase(df: DataFrame):
    """Сохраняет DataFrame в таблицу HBase 'flights'."""
    df.foreachPartition(_write_partition_to_hbase)
