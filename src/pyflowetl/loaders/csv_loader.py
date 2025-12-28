import os
from email import header

from pyflowetl.log import get_logger, log_memory_usage



class CsvLoader:
    def __init__(self, output_path, encoding="utf-8", delimiter=",", header=True):
        self.header = header
        self.output_path = output_path
        self.encoding = encoding
        self.delimiter = delimiter

    def load(self, data):
        logger = get_logger()
        logger.info(f"[CsvLoader] Scrittura su file: {self.output_path}")

        try:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            data.to_csv(self.output_path, index=False, encoding=self.encoding, sep=self.delimiter, header=self.header)
            logger.info(f"[CsvLoader] Scrittura completata: {len(data)} record")
            log_memory_usage(f"Dopo Scrittura su file: {self.output_path}")
        except Exception as e:
            logger.exception(f"[CsvLoader] Errore durante la scrittura del file: {e}")
            raise
