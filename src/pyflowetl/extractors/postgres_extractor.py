import pandas as pd
from sqlalchemy import create_engine
from pyflowetl.log import get_logger, log_memory_usage

class PostgresExtractor:
    def __init__(self, connection_string: str, table_name: str = None, query: str = None):
        """
        :param connection_string: es. 'postgresql://user:password@localhost:5432/dbname'
        :param table_name: nome della tabella da cui estrarre (usato se query è None)
        :param query: query SQL personalizzata
        """
        self.connection_string = connection_string
        self.table_name = table_name
        self.query = query
        self.logger = get_logger()

    def extract(self) -> pd.DataFrame:
        self.logger.info("[PostgresExtractor] Avvio estrazione")

        engine = create_engine(self.connection_string)
        sql = self.query or f"SELECT * FROM {self.table_name}"

        self.logger.info(f"[PostgresExtractor] Eseguo query: {sql}")

        df = pd.read_sql_query(sql, engine)

        self.logger.info(f"[PostgresExtractor] Estratte {len(df)} righe")
        log_memory_usage("[PostgresExtractor] post-extract")
        return df
