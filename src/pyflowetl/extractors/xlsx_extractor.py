import os
import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class XlsxExtractor:
    def __init__(self, filepath: str, sheet_name=0):
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.logger = get_logger()


    def extract(self) -> pd.DataFrame:
        self.logger.info(f"[XlsxExtractor] Lettura file: {self.filepath} (foglio: {self.sheet_name})")

        if not os.path.exists(self.filepath):
            msg = f"[XlsxExtractor] File non trovato: {self.filepath}"
            self.logger.error(msg)
            raise FileNotFoundError(msg)

        try:
            df = pd.read_excel(self.filepath, sheet_name=self.sheet_name, engine="openpyxl" , dtype=str, keep_default_na=False, na_values=[])
            self.logger.info(f"[XlsxExtractor] Letti {len(df)} record")
            log_memory_usage("[XlsxExtractor] post-extract")
            return df
        except Exception as e:
            self.logger.exception(f"[XlsxExtractor] Errore durante la lettura del file: {e}")
            raise
