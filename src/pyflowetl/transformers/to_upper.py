import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class ToUpperTransformer:
    def __init__(self, column: str):
        """
        Converte in maiuscolo tutti i valori di una colonna stringa.
        :param column: nome della colonna da trasformare
        """
        self.column = column
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.column not in df.columns:
            raise KeyError(f"[ToUpperTransformer] Colonna '{self.column}' non trovata")

        self.logger.info(f"[ToUpperTransformer] Converto '{self.column}' in maiuscolo")
        df[self.column] = df[self.column].apply(lambda x: str(x).upper() if pd.notna(x) else x)

        log_memory_usage(f"[ToUpperTransformer] post-transform {self.column}")
        return df
