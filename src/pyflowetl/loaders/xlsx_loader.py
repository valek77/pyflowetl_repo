import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class XlsxLoader:
    def __init__(self, filepath: str, sheet_name="Sheet1"):
        self.filepath = filepath
        self.sheet_name = sheet_name
        self.logger = get_logger()

    def load(self, data: pd.DataFrame):
        self.logger.info(f"[XlsxLoader] Scrittura file: {self.filepath} (foglio: {self.sheet_name})")

        try:
            data.to_excel(self.filepath, index=False, sheet_name=self.sheet_name, engine="openpyxl")
            self.logger.info(f"[XlsxLoader] Scritti {len(data)} record")
            log_memory_usage("[XlsxLoader] post-load")
        except Exception as e:
            self.logger.exception(f"[XlsxLoader] Errore durante la scrittura del file: {e}")
            raise
