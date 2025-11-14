import uuid
import pandas as pd
import duckdb

from pyflowetl.log import get_logger, log_memory_usage


class DuckDbLoader:
    """
    Loader generico per DuckDB con supporto a insert, update, upsert.

    Parametri
    ---------
    connection : str o duckdb.DuckDBPyConnection
        - Stringa percorso DB DuckDB (es. ':memory:' oppure 'data/attivi.duckdb')
        - Oppure una connessione DuckDB già aperta.

    config : dict
        Configurazione tabella. Deve contenere:
          - "table": nome della tabella nel DB.
          - "unique_keys": lista delle colonne DB usate come chiave (solo per update/upsert).
          - "columns": mappatura {nome_colonna_dataset: nome_colonna_db}.

    mode : str, opzionale
        Modalità di caricamento: 'insert', 'update', 'upsert'. Default: 'insert'.

    chunksize : int, opzionale
        Righe per batch (usato per l'insert). Default: 500.

    Note
    ----
    - Per usare la modalità 'upsert' è necessario che sulla tabella target esista
      un UNIQUE o PRIMARY KEY sulle colonne in `unique_keys`.
    - Tutte le modalità sono pensate per funzionare con DataFrame ragionevoli;
      per carichi enormi puoi anche usare direttamente le funzioni di DuckDB
      (COPY, FROM parquet/csv, ecc.) in pipeline dedicate.

    Esempio di utilizzo
    -------------------
    config = {
        "table": "attivi_ml",
        "unique_keys": ["pod", "nome_file", "data_attivazione"],
        "columns": {
            "POD": "pod",
            "NOME_FILE": "nome_file",
            "DATA_ATT": "data_attivazione",
        }
    }

    loader = DuckDbLoader(
        connection="attivi.duckdb",
        config=config,
        mode="upsert",
        chunksize=1000,
    )

    pipeline = EtlPipeline().extract(...).transform(...).load(loader)
    """

    def __init__(self, connection, config: dict, mode: str = "insert", chunksize: int = 500):
        # Gestione connection: stringa → duckdb.connect, oppure connessione già pronta
        if hasattr(connection, "execute"):
            # Presumo sia una DuckDBPyConnection
            self.con = connection
            self._owned_connection = False
        else:
            # Percorso o ':memory:'
            self.con = duckdb.connect(connection)
            self._owned_connection = True

        self.config = config
        self.table_name = config["table"]
        self.unique_keys = config.get("unique_keys", [])
        self.columns_mapping = config.get("columns", {})
        self.mode = mode.lower()
        self.chunksize = chunksize
        self.logger = get_logger()

    def __del__(self):
        # Chiudi la connessione solo se l'abbiamo aperta noi
        try:
            if getattr(self, "_owned_connection", False) and self.con is not None:
                self.con.close()
        except Exception:
            # In distruttore meglio non far esplodere eccezioni
            pass

    def load(self, df: pd.DataFrame):
        """
        Carica il DataFrame nella tabella DuckDB secondo la modalità scelta.
        """
        if df is None or df.empty:
            self.logger.info(f"[DuckDbLoader] DataFrame vuoto, niente da caricare in {self.table_name}")
            return

        log_memory_usage("[DuckDbLoader] pre-load")

        # Applica mapping DataFrame → DB
        df_db = self._apply_mapping(df)

        self.logger.info(f"[DuckDbLoader] Modalità: {self.mode} su tabella {self.table_name} ({len(df_db)} righe)")

        if self.mode == "insert":
            self._insert(df_db)
        elif self.mode == "update":
            if not self.unique_keys:
                raise ValueError("[DuckDbLoader] Per la modalità 'update' sono richieste le 'unique_keys' nella config")
            self._update(df_db)
        elif self.mode == "upsert":
            if not self.unique_keys:
                raise ValueError("[DuckDbLoader] Per la modalità 'upsert' sono richieste le 'unique_keys' nella config")
            self._upsert(df_db)
        else:
            raise ValueError(f"[DuckDbLoader] Modalità non supportata: {self.mode}")

        log_memory_usage("[DuckDbLoader] post-load")

    def _apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rinomina le colonne del DataFrame in base alla mappatura dataset→DB
        e filtra solo quelle presenti nel mapping.
        """
        if not self.columns_mapping:
            return df

        # Prende solo le colonne del mapping presenti nel DataFrame
        mapped_cols = {src: dst for src, dst in self.columns_mapping.items() if src in df.columns}

        if not mapped_cols:
            self.logger.warning("[DuckDbLoader] Nessuna colonna del mapping trovata nel DataFrame in input")
            return pd.DataFrame(columns=[])

        return df[list(mapped_cols.keys())].rename(columns=mapped_cols)

    # --------- MODALITÀ INSERT ---------

    def _insert(self, df: pd.DataFrame):
        """
        Insert semplice (append) usando una vista temporanea registrata in DuckDB.
        """
        total_rows = len(df)
        inserted = 0

        for start in range(0, total_rows, self.chunksize):
            chunk = df.iloc[start:start + self.chunksize]

            tmp_view = f"_tmp_df_{uuid.uuid4().hex[:8]}"
            self.con.register(tmp_view, chunk)

            cols = ", ".join(chunk.columns)
            sql = f"INSERT INTO {self.table_name} ({cols}) SELECT {cols} FROM {tmp_view}"

            self.con.execute(sql)
            self.con.unregister(tmp_view)

            inserted += len(chunk)

        self.logger.info(f"[DuckDbLoader] Inserite {inserted} righe in {self.table_name}")

    # --------- MODALITÀ UPDATE ---------

    def _update(self, df: pd.DataFrame):
        """
        Update row-by-row basato sulle unique_keys.
        (Non è il massimo come performance, ma è generico e chiaro.)
        """
        non_key_cols = [c for c in df.columns if c not in self.unique_keys]

        if not non_key_cols:
            self.logger.warning("[DuckDbLoader] Nessuna colonna da aggiornare (tutte sono chiavi uniche?)")
            return

        set_clause = ", ".join([f"{col} = ?" for col in non_key_cols])
        where_clause = " AND ".join([f"{col} = ?" for col in self.unique_keys])
        sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause}"

        updated = 0
        for _, row in df.iterrows():
            params = [row[col] for col in non_key_cols] + [row[col] for col in self.unique_keys]
            self.con.execute(sql, params)
            updated += 1

        self.logger.info(f"[DuckDbLoader] Aggiornate {updated} righe in {self.table_name}")

    # --------- MODALITÀ UPSERT ---------

    def _upsert(self, df: pd.DataFrame):
        """
        Upsert row-by-row usando la sintassi INSERT ... ON CONFLICT ... DO UPDATE di DuckDB.
        Richiede un vincolo UNIQUE/PK già definito sulle colonne in `unique_keys`.
        """
        columns = list(df.columns)
        value_placeholders = ", ".join(["?"] * len(columns))
        conflict_cols = ", ".join(self.unique_keys)

        update_assignments = ", ".join([
            f"{col} = EXCLUDED.{col}" for col in columns if col not in self.unique_keys
        ])

        if not update_assignments:
            # Caso limite: tutte le colonne sono chiavi, niente da aggiornare.
            sql = (
                f"INSERT INTO {self.table_name} ({', '.join(columns)}) "
                f"VALUES ({value_placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO NOTHING"
            )
        else:
            sql = (
                f"INSERT INTO {self.table_name} ({', '.join(columns)}) "
                f"VALUES ({value_placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_assignments}"
            )

        upserted = 0
        for _, row in df.iterrows():
            params = [row[col] for col in columns]
            self.con.execute(sql, params)
            upserted += 1

        self.logger.info(f"[DuckDbLoader] Upsert eseguito su {upserted} righe in {self.table_name}")
