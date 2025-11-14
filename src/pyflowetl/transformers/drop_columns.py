import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class DropColumnsTransformer:
    def __init__(self, columns):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[DropColumnsTransformer] Dropping columns: {self.columns}")
        df = df.drop(columns=self.columns, errors='ignore')  # non solleva errore se mancano
        log_memory_usage("[DropColumnsTransformer] post-transform")
        return df
