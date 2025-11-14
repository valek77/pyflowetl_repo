import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class AddConstantColumnTransformer:
    def __init__(self, column_name: str, value):
        self.column_name = column_name
        self.value = value
        logger = get_logger()
        logger.info(f"[AddConstantColumnTransformer] Aggiungerà la colonna '{column_name}' con valore: {value!r}")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger = get_logger()
        df[self.column_name] = self.value
        log_memory_usage(f"[AddConstantColumnTransformer] post-transform ({self.column_name})")
        return df
