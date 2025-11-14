import duckdb
import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class CustomSqlFilterTransformer:
    """
    Filtra un pandas.DataFrame usando SQL DuckDB.

    Uso tipico:
      - Clausola WHERE:
          t = CustomSqlFilterTransformer("city = 'Napoli' AND cap = '80100'")
          out_df = t.transform(in_df)

      - Query SELECT completa:
          t = CustomSqlFilterTransformer("SELECT cap, provincia FROM df WHERE regione = 'Campania'")
          out_df = t.transform(in_df)

    Note:
      - L'alias del DataFrame in DuckDB è "df" (configurabile).
      - Se la stringa SQL non inizia con SELECT, viene interpretata come WHERE.
    """

    def __init__(self, sql: str, alias: str = "df"):
        self.sql = (sql or "").strip()
        self.alias = alias
        self.logger = get_logger()

        if not self.sql:
            raise ValueError("SQL mancante. Passa una clausola WHERE o una query SELECT completa.")

    def _to_select(self) -> str:
        # Se non parte con SELECT, costruisci una SELECT * ... WHERE <sql>
        head = self.sql.lstrip().lower()
        if head.startswith("select"):
            return self.sql
        return f"SELECT * FROM {self.alias} WHERE {self.sql}"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input non valido: atteso pandas.DataFrame.")

        log_memory_usage("[CustomSqlFilterTransformer] start")
        self.logger.info("CustomSqlFilterTransformer: alias=%s", self.alias)

        con = duckdb.connect(database=":memory:")
        try:
            con.register(self.alias, df)
            query = self._to_select()
            self.logger.debug("Esecuzione SQL su DuckDB: %s", query)

            out_df = con.execute(query).fetch_df()

            self.logger.info(
                "CustomSqlFilterTransformer: righe in=%d, out=%d, colonne out=%d",
                len(df),
                len(out_df),
                len(out_df.columns),
            )
            log_memory_usage("[CustomSqlFilterTransformer] end")
            return out_df
        except duckdb.Error as e:
            cols = list(df.columns)
            self.logger.error("Errore DuckDB: %s", e)
            self.logger.debug("Colonne disponibili: %s", cols)
            raise
        finally:
            con.close()

