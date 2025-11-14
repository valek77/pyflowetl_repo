import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class ToLowerTransformer:
    def __init__(self, column: str):
        """
        Converte in minuscolo tutti i valori di una colonna stringa.
        :param column: nome della colonna da trasformare
        """
        self.column = column
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.column not in df.columns:
            raise KeyError(f"[ToLowerTransformer] Colonna '{self.column}' non trovata")

        self.logger.info(f"[ToLowerTransformer] Converto '{self.column}' in minuscolo")
        df[self.column] = df[self.column].apply(lambda x: str(x).lower() if pd.notna(x) else x)

        log_memory_usage(f"[ToLowerTransformer] post-transform {self.column}")
        return df
