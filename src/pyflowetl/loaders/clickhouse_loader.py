import math
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from clickhouse_driver import Client

from pyflowetl.log import get_logger, log_memory_usage


class ClickHouseLoader:
    """
    Loader per ClickHouse (clickhouse_driver.Client) con firma parametri stile Extractor.

    Parametri (stile extractor)
    ---------------------------
    host : str
    port : int (default 9000)
    user : str
    password : str
    database : str
    table_name : str
        Nome tabella target (obbligatorio)
    settings : dict
        Settings del client clickhouse_driver (opzionale)

    config : dict
        Config tabella. Deve contenere:
          - "unique_keys": lista colonne chiave (necessarie per update/upsert)
          - "columns": mapping {col_df: col_ch}

    mode : str
        'insert' | 'update' | 'upsert' (default 'insert')

    chunksize : int
        Batch size per insert (default 10_000)

    upsert_strategy : str
        'replacing_merge_tree' (default) | 'delete_insert'

    optimize_final_after_upsert : bool
        Se True fa OPTIMIZE FINAL dopo upsert (⚠️ costoso)

    Esempio
    -------
    config = {
        "unique_keys": ["dbc_company_id"],
        "columns": {
            "id": "dbc_company_id",
            "company_name": "company_name",
            "bpg_autofub_enabled": "autofub_enabled"
        }
    }

    loader = ClickHouseLoader(
        host="127.0.0.1",
        port=9000,
        user="default",
        password="",
        database="analytics",
        table_name="companies",
        config=config,
        mode="upsert",
        upsert_strategy="replacing_merge_tree",
        chunksize=20000,
        settings={"use_numpy": False},
    )
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        user: str = "default",
        password: str = "",
        database: str = "default",
        table_name: Optional[str] = None,
        settings: Optional[dict] = None,
        *,
        config: Optional[dict] = None,
        mode: str = "insert",
        chunksize: int = 10_000,
        upsert_strategy: str = "replacing_merge_tree",
        optimize_final_after_upsert: bool = False,
    ):
        if not table_name:
            raise ValueError("[ClickHouseLoader] 'table_name' è obbligatorio.")

        self.table_name = table_name
        self.config = config or {}
        self.unique_keys = self.config.get("unique_keys", [])
        self.columns_mapping = self.config.get("columns", {})

        self.mode = (mode or "insert").lower()
        self.chunksize = int(chunksize)
        self.upsert_strategy = (upsert_strategy or "replacing_merge_tree").lower()
        self.optimize_final_after_upsert = bool(optimize_final_after_upsert)

        self.logger = get_logger()

        # Client
        self.client = Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            settings=settings or {},
        )

        if self.mode in ("update", "upsert") and not self.unique_keys:
            raise ValueError("[ClickHouseLoader] 'unique_keys' richieste per update/upsert.")

        if self.mode == "upsert" and self.upsert_strategy not in ("replacing_merge_tree", "delete_insert"):
            raise ValueError(f"[ClickHouseLoader] upsert_strategy non supportata: {self.upsert_strategy}")

    # ------------------------------------------------------------------

    def load(self, df: pd.DataFrame):
        df_db = self._apply_mapping(df)
        df_db = self._normalize_df(df_db)

        self.logger.info(
            f"[ClickHouseLoader] mode={self.mode} table={self.table_name} rows={len(df_db)} chunksize={self.chunksize}"
        )

        if df_db.empty:
            self.logger.info("[ClickHouseLoader] DataFrame vuoto: nulla da caricare.")
            log_memory_usage("[ClickHouseLoader] post-load")
            return

        if self.mode == "insert":
            self._insert(df_db)
        elif self.mode == "update":
            self._update(df_db)
        elif self.mode == "upsert":
            self._upsert(df_db)
        else:
            raise ValueError(f"[ClickHouseLoader] Modalità non supportata: {self.mode}")

        log_memory_usage("[ClickHouseLoader] post-load")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rinomina le colonne del DataFrame in base alla mappatura dataset→ClickHouse
        e filtra solo quelle presenti nel mapping.
        """
        if not self.columns_mapping:
            return df

        mapped = {src: dst for src, dst in self.columns_mapping.items() if src in df.columns}
        return df[list(mapped.keys())].rename(columns=mapped)

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte NaN/NaT -> None (più compatibile con clickhouse_driver).
        """
        return df.where(pd.notnull(df), None)

    def _iter_chunks(self, df: pd.DataFrame):
        for start in range(0, len(df), self.chunksize):
            yield df.iloc[start : start + self.chunksize]

    def _df_to_tuples(self, df: pd.DataFrame) -> List[Tuple[Any, ...]]:
        return [tuple(r) for r in df.itertuples(index=False, name=None)]

    # ------------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------------

    def _insert(self, df: pd.DataFrame):
        cols = ", ".join(df.columns)
        sql = f"INSERT INTO {self.table_name} ({cols}) VALUES"

        inserted = 0
        for chunk in self._iter_chunks(df):
            self.client.execute(sql, self._df_to_tuples(chunk))
            inserted += len(chunk)

        self.logger.info(f"[ClickHouseLoader] Inserite {inserted} righe")

    # ------------------------------------------------------------------
    # UPDATE (mutation)
    # ------------------------------------------------------------------

    def _update(self, df: pd.DataFrame):
        """
        UPDATE in ClickHouse = mutation (ALTER TABLE ... UPDATE).
        È costoso; evita su grandi volumi.
        """
        non_key_cols = [c for c in df.columns if c not in self.unique_keys]
        if not non_key_cols:
            self.logger.warning("[ClickHouseLoader] Nessuna colonna da aggiornare (solo chiavi).")
            return

        updated = 0
        for _, row in df.iterrows():
            set_clause = ", ".join(f"{c} = {self._fmt(row[c])}" for c in non_key_cols)
            where_clause = " AND ".join(f"{k} = {self._fmt(row[k])}" for k in self.unique_keys)
            sql = f"ALTER TABLE {self.table_name} UPDATE {set_clause} WHERE {where_clause}"
            self.client.execute(sql)
            updated += 1

        self.logger.warning(f"[ClickHouseLoader] Richiesti {updated} UPDATE (mutation asincrona)")

    # ------------------------------------------------------------------
    # UPSERT
    # ------------------------------------------------------------------

    def _upsert(self, df: pd.DataFrame):
        if self.upsert_strategy == "replacing_merge_tree":
            # upsert logico: solo INSERT (deduplica demandata all'engine)
            self._insert(df)

            if self.optimize_final_after_upsert:
                self.logger.warning("[ClickHouseLoader] OPTIMIZE FINAL richiesto (molto costoso).")
                self.client.execute(f"OPTIMIZE TABLE {self.table_name} FINAL")

            self.logger.info("[ClickHouseLoader] Upsert via ReplacingMergeTree (INSERT-only)")
            return

        # delete + insert
        affected = 0
        for chunk in self._iter_chunks(df):
            self._delete_by_keys(chunk)
            self._insert(chunk)
            affected += len(chunk)

        self.logger.info(f"[ClickHouseLoader] Upsert delete+insert su {affected} righe")

    def _delete_by_keys(self, df: pd.DataFrame):
        """
        DELETE a batch usando IN su tuple di chiavi:
          ALTER TABLE t DELETE WHERE (k1, k2) IN ((v11, v12), (v21, v22), ...)
        """
        key_tuples = [
            tuple(row[k] for k in self.unique_keys)
            for _, row in df[self.unique_keys].iterrows()
        ]
        if not key_tuples:
            return

        keys_expr = "(" + ", ".join(self.unique_keys) + ")"
        tuples_expr = ", ".join(self._fmt_tuple(t) for t in key_tuples)

        sql = f"ALTER TABLE {self.table_name} DELETE WHERE {keys_expr} IN ({tuples_expr})"
        self.client.execute(sql)

    # ------------------------------------------------------------------
    # Formatting (solo per comporre mutation/delete)
    # ------------------------------------------------------------------

    def _fmt_tuple(self, t: Tuple[Any, ...]) -> str:
        return "(" + ", ".join(self._fmt(v) for v in t) + ")"

    def _fmt(self, v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, int):
            return str(v)
        if isinstance(v, float):
            return "NULL" if math.isnan(v) else repr(v)

        s = str(v).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{s}'"
