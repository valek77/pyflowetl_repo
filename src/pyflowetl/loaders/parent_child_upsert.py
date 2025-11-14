from pyflowetl.log import get_logger, log_memory_usage
import psycopg2


class ParentChildUpsertLoader:
    """
    Loader per eseguire upsert padre/figlio su tabelle Postgres.

    Parametri
    ---------
    connection_string : str
        Stringa di connessione al database Postgres
        (es. "postgresql://user:pwd@localhost:5432/mydb").

    parent_config : dict
        Dizionario di configurazione per la tabella padre. Deve contenere:
          - "table": nome della tabella padre nel DB.
          - "unique_keys": lista di colonne **del DB** che identificano
            un record univoco (usate in ON CONFLICT).
          - "columns": mappatura {nome_colonna_dataset: nome_colonna_db}.

    child_config : dict
        Dizionario di configurazione per la tabella figlio. Deve contenere:
          - "table": nome della tabella figlio nel DB.
          - "unique_keys": lista di colonne **del DB** che identificano
            un record univoco (usate in ON CONFLICT).
          - "columns": mappatura {nome_colonna_dataset: nome_colonna_db}.
          - "foreign_key": dict con:
              - "db_column": nome della colonna **del DB figlio** che
                rappresenta la FK verso il padre (es. "compy_id").
              - "parent_db_column": nome della colonna **del DB padre**
                da usare come valore per la FK (es. "id").

    batch_size : int, opzionale
        Numero di righe da processare per ogni batch (default: 1000).

    Ritorno
    -------
    Nessun ritorno. Esegue direttamente gli upsert nel DB.

    Note
    ----
    - `unique_keys` deve sempre riferirsi ai **nomi delle colonne del DB**.
    - Le colonne del DataFrame devono essere mappate tramite la chiave
      `columns` verso le colonne del DB.
    - La foreign key è completamente indipendente dai nomi presenti nel DataFrame.

    Esempio di utilizzo
    -------------------
    from pyflowetl.pipeline import EtlPipeline
    from my_loaders.parent_child_upsert_loader import ParentChildUpsertLoader

    parent_config = {
        "table": "companies",
        "unique_keys": ["company_code"],
        "columns": {
            "codice_compagnia": "company_code",
            "nome_compagnia": "name"
        }
    }

    child_config = {
        "table": "employees",
        "unique_keys": ["employee_code"],
        "columns": {
            "codice_dipendente": "employee_code",
            "nome": "first_name",
            "cognome": "last_name"
        },
        "foreign_key": {
            "db_column": "compy_id",         # colonna DB del figlio
            "parent_db_column": "id"         # colonna DB padre
        }
    }

    loader = ParentChildUpsertLoader(
        connection_string="postgresql://user:pwd@localhost:5432/mydb",
        parent_config=parent_config,
        child_config=child_config,
        batch_size=500
    )

    pipeline = EtlPipeline() \
        .extract(...) \
        .transform(...) \
        .load(loader)
    """

    def __init__(self, connection_string: str, parent_config: dict, child_config: dict, batch_size: int = 1000):
        self.connection_string = connection_string
        self.parent_config = parent_config
        self.child_config = child_config
        self.batch_size = batch_size
        self.logger = get_logger()

    def load(self, df):
        self.logger.info("[ParentChildUpsertLoader] Inizio upsert padre/figlio...")
        with psycopg2.connect(self.connection_string) as conn:
            for start in range(0, len(df), self.batch_size):
                batch = df.iloc[start:start + self.batch_size]
                for _, row in batch.iterrows():
                    parent_data = self._map_row(row, self.parent_config["columns"])
                    parent_keys = self._upsert(conn, self.parent_config, parent_data)

                    child_data = self._map_row(row, self.child_config["columns"])

                    # Foreign key handling
                    fk = self.child_config["foreign_key"]
                    child_data[fk["db_column"]] = parent_keys[fk["parent_db_column"]] \
                        if isinstance(parent_keys, dict) else parent_keys

                    self._upsert(conn, self.child_config, child_data)

                log_memory_usage(f"[ParentChildUpsertLoader] dopo batch {start}")
        self.logger.info("[ParentChildUpsertLoader] Fine upsert padre/figlio")

    def _map_row(self, row, mapping):
        return {db_col: row[src_col] for src_col, db_col in mapping.items() if src_col in row}

    def _upsert(self, conn, config, data):
        cols = data.keys()
        values = [data[c] for c in cols]
        table = config["table"]
        unique_keys = config["unique_keys"]

        insert_cols = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in unique_keys])
        conflict = ", ".join(unique_keys)

        # ritorniamo tutte le unique_keys + la PK 'id' se presente
        returning_cols = list(unique_keys)
        if "id" not in returning_cols:
            returning_cols.append("id")
        returning_cols_sql = ", ".join(returning_cols)

        sql = f"""
        INSERT INTO {table} ({insert_cols})
        VALUES ({placeholders})
        ON CONFLICT ({conflict})
        DO UPDATE SET {updates}
        RETURNING {returning_cols_sql};
        """
        with conn.cursor() as cur:
            cur.execute(sql, values)
            result = cur.fetchone()

            # Se c'è solo una colonna ritorna il valore, altrimenti un dict
            if len(cur.description) == 1:
                return result[0]
            else:
                return {desc[0]: val for desc, val in zip(cur.description, result)}
