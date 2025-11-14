import pandas as pd
from sqlalchemy import create_engine, text
from pyflowetl.log import get_logger, log_memory_usage


class PostgresLoader:
    """
    Loader generico per Postgres con supporto a insert, update, upsert.

    Parametri
    ---------
    connection_string : str
        Stringa di connessione al database Postgres
        (es. 'postgresql://user:password@localhost:5432/dbname').

    config : dict
        Configurazione tabella. Deve contenere:
          - "table": nome della tabella nel DB.
          - "unique_keys": lista delle colonne **DB** usate come chiave (solo per update/upsert).
          - "columns": mappatura {nome_colonna_dataset: nome_colonna_db}.

    mode : str, opzionale
        Modalità di caricamento: 'insert', 'update', 'upsert'. Default: 'insert'.

    chunksize : int, opzionale
        Righe per batch. Default: 500.

    Esempio di utilizzo
    -------------------
    config = {
        "table": "companies",
        "unique_keys": ["dbc_company_id"],
        "columns": {
            "id": "dbc_company_id",
            "company_name": "company_name",
            "bpg_autofub_enabled": "autofub_enabled"
        }
    }

    loader = PostgresLoader(
        connection_string="postgresql://user:pwd@localhost:5432/mydb",
        config=config,
        mode="upsert"
    )

    pipeline = EtlPipeline().extract(...).transform(...).load(loader)
    """

    def __init__(self, connection_string: str, config: dict, mode: str = "insert", chunksize: int = 500):
        self.engine = create_engine(connection_string)
        self.config = config
        self.table_name = config["table"]
        self.unique_keys = config.get("unique_keys", [])
        self.columns_mapping = config.get("columns", {})
        self.mode = mode.lower()
        self.chunksize = chunksize
        self.logger = get_logger()

    def load(self, df: pd.DataFrame):
        # Applica mapping DataFrame → DB
        df_db = self._apply_mapping(df)

        self.logger.info(f"[PostgresLoader] Modalità: {self.mode} su tabella {self.table_name}")
        if self.mode == "insert":
            self._insert(df_db)
        elif self.mode == "update":
            self._update(df_db)
        elif self.mode == "upsert":
            self._upsert(df_db)
        else:
            raise ValueError(f"[PostgresLoader] Modalità non supportata: {self.mode}")

        log_memory_usage("[PostgresLoader] post-load")

    def _apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rinomina le colonne del DataFrame in base alla mappatura dataset→DB
        e filtra solo quelle presenti nel mapping.
        """
        if not self.columns_mapping:
            return df

        # Prende solo le colonne del mapping presenti nel DataFrame
        mapped_cols = {src: dst for src, dst in self.columns_mapping.items() if src in df.columns}

        # Filtra il DF e rinomina
        return df[list(mapped_cols.keys())].rename(columns=mapped_cols)

    def _insert(self, df: pd.DataFrame):
        df.to_sql(self.table_name, con=self.engine, if_exists="append", index=False, chunksize=self.chunksize)
        self.logger.info(f"[PostgresLoader] Inserite {len(df)} righe")

    def _update(self, df: pd.DataFrame):
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                set_clause = ", ".join([f"{col} = :{col}" for col in df.columns if col not in self.unique_keys])
                where_clause = " AND ".join([f"{col} = :{col}" for col in self.unique_keys])
                sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause}"
                conn.execute(text(sql), row.to_dict())
        self.logger.info(f"[PostgresLoader] Aggiornate {len(df)} righe")

    def _upsert(self, df: pd.DataFrame):
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                columns = list(row.keys())
                values = [f":{col}" for col in columns]
                updates = [f"{col} = EXCLUDED.{col}" for col in columns if col not in self.unique_keys]
                conflict_cols = ", ".join(self.unique_keys)
                sql = f"""
                    INSERT INTO {self.table_name} ({', '.join(columns)})
                    VALUES ({', '.join(values)})
                    ON CONFLICT ({conflict_cols}) DO UPDATE SET {', '.join(updates)}
                """
                conn.execute(text(sql), row.to_dict())
        self.logger.info(f"[PostgresLoader] Upsert su {len(df)} righe")
