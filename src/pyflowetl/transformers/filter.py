import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class FilterTransformer:
    def __init__(self, filter_expression: str, inplace: bool = False):
        """
        :param filter_expression: Espressione Pandas query (stringa)
        :param inplace:
            - True -> modifica il DataFrame corrente (filtra inplace)
            - False -> restituisce un DataFrame filtrato SENZA toccare l'originale
        """
        self.filter_expression = filter_expression
        self.inplace = inplace
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra il DataFrame. Se inplace=True modifica df direttamente,
        altrimenti restituisce una nuova copia filtrata.
        """
        self.logger.info(f"[FilterTransformer] Filtro attivo: {self.filter_expression} (inplace={self.inplace})")

        try:
            if self.inplace:
                # Filtraggio diretto sul DataFrame corrente
                mask = df.eval(self.filter_expression, engine="python")
                df.drop(df[~mask].index, inplace=True)
                filtered_df = df
            else:
                # Ritorna una nuova copia filtrata
                filtered_df = df.query(self.filter_expression, engine="python")

        except Exception as e:
            self.logger.error(f"[FilterTransformer] Errore nel filtro: {e}")
            raise

        self.logger.info(
            f"[FilterTransformer] Records filtrati: {len(filtered_df)} su {len(df)}"
        )
        log_memory_usage("[FilterTransformer] post-transform")

        return filtered_df
