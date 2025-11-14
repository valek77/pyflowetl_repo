import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class LogHeadTransformer:
    def __init__(self, n: int = 10):
        self.n = n
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[LogHeadTransformer] Prime {self.n} righe del DataFrame:")
        self.logger.info("\n" + df.head(self.n).to_string(index=False))
        log_memory_usage(f"[LogHeadTransformer] after logging head({self.n})")
        return df
