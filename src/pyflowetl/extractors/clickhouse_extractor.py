import pandas as pd
from clickhouse_driver import Client
from pyflowetl.log import get_logger, log_memory_usage


class ClickHouseExtractor:
    def __init__(self, host: str = 'localhost', port: int = 9000,
                 user: str = 'default', password: str = '',
                 database: str = 'default', table_name: str = None,
                 query: str = None, settings: dict = None):
        """
        :param host: Host del server ClickHouse
        :param port: Porta (default 9000 per protocollo nativo)
        :param user: Username
        :param password: Password
        :param database: Database di riferimento
        :param table_name: Nome della tabella (se query è None)
        :param query: Query SQL personalizzata
        :param settings: Dizionario opzionale di impostazioni ClickHouse (es. {'max_block_size': 100000})
        """
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'settings': settings or {}
        }
        self.table_name = table_name
        self.query = query
        self.logger = get_logger()

    def extract(self) -> pd.DataFrame:
        self.logger.info("[ClickHouseExtractor] Avvio estrazione")

        sql = self.query or f"SELECT * FROM {self.table_name}"
        self.logger.info(f"[ClickHouseExtractor] Eseguo query: {sql}")

        try:
            # Utilizzo del protocollo nativo tramite clickhouse-driver
            client = Client(**self.config)

            # query_dataframe restituisce direttamente un oggetto DataFrame
            # Nota: richiede che 'pandas' sia installato nell'ambiente
            df = client.query_dataframe(sql)

            self.logger.info(f"[ClickHouseExtractor] Estratte {len(df)} righe")
            log_memory_usage("[ClickHouseExtractor] post-extract")

            return df

        except Exception as e:
            self.logger.error(f"[ClickHouseExtractor] Errore durante l'estrazione: {e}")
            raise
        finally:
            # Il driver gestisce internamente il pool, ma è bene assicurarsi
            # che le risorse temporanee siano liberate se necessario.
            pass