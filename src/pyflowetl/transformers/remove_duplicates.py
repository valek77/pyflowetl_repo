import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class RemoveDuplicatesTransformer:
    def __init__(self, subset_columns=None, keep='first'):
        """
        Rimuove righe duplicate sulla base di una o più colonne.

        :param subset_columns: Lista di colonne su cui basare il controllo duplicati.
                               Se None, considera tutte le colonne (default comportamento Excel).
        :param keep: 'first' (default) per mantenere la prima occorrenza, 'last' per l’ultima, False per rimuoverle tutte.
        """
        self.subset_columns = subset_columns
        self.keep = keep
        self.logger = get_logger()

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[RemoveDuplicatesTransformer] Rimuovo duplicati su: {self.subset_columns or 'tutte le colonne'} (keep={self.keep})")
        before = len(data)
        data = data.drop_duplicates(subset=self.subset_columns, keep=self.keep)
        after = len(data)
        self.logger.info(f"[RemoveDuplicatesTransformer] Righe rimosse: {before - after}")
        log_memory_usage("Dopo RemoveDuplicatesTransformer")
        return data
