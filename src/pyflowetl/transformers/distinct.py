import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class DistinctTransformer:
    def __init__(self, subset: list[str] | None = None, keep: str = "first"):
        """
        Rimuove righe duplicate dal DataFrame.

        :param subset: Lista di colonne su cui calcolare i duplicati. Se None, considera tutte le colonne.
        :param keep: 'first' (default), 'last', o False. Come pandas.drop_duplicates.
        """
        self.subset = subset
        self.keep = keep
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[DistinctTransformer] Rimozione duplicati - subset: {self.subset}, keep: {self.keep}")
        before = len(df)
        df = df.drop_duplicates(subset=self.subset, keep=self.keep).copy()
        after = len(df)
        self.logger.info(f"[DistinctTransformer] Righe prima: {before}, dopo: {after}, eliminate: {before - after}")
        log_memory_usage("[DistinctTransformer] post-transform")
        return df
