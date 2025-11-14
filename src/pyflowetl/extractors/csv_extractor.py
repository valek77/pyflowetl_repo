import pandas as pd
import os
import chardet
from pyflowetl.log import get_logger, log_memory_usage

class CsvExtractor:
    def __init__(self, filepath, encoding=None, delimiter=",", low_memory=True):
        self.filepath = filepath
        self.encoding = encoding  # ← può essere None, verrà rilevato
        self.delimiter = delimiter
        self.low_memory = low_memory

    def detect_encoding(self, num_bytes: int = 100_000):
        with open(self.filepath, "rb") as f:
            raw = f.read(num_bytes)
        result = chardet.detect(raw)
        return result["encoding"], result["confidence"]

    def extract(self):
        logger = get_logger()
        logger.info(f"[CsvExtractor] Leggo file: {self.filepath}")

        if not os.path.exists(self.filepath):
            logger.error(f"[CsvExtractor] File non trovato: {self.filepath}")
            raise FileNotFoundError(f"File non trovato: {self.filepath}")

        try:
            # autodetect
            detected_encoding, confidence = self.detect_encoding()
            logger.info(f"[CsvExtractor] Encoding rilevato: {detected_encoding} (confidence={confidence:.2f})")

            encoding_to_use = self.encoding or detected_encoding or "utf-8"

            try:
                df = pd.read_csv(
                    self.filepath,
                    encoding=encoding_to_use,
                    delimiter=self.delimiter,
                    keep_default_na=False,
                    na_values=[],
                    dtype=str,
                low_memory=self.low_memory,
                )
            except UnicodeDecodeError:
                logger.warning(f"[CsvExtractor] Errore con encoding '{encoding_to_use}', provo fallback 'cp1252'")
                df = pd.read_csv(
                    self.filepath,
                    encoding="cp1252",
                    delimiter=self.delimiter,
                    keep_default_na=False,
                    low_memory=self.low_memory,
                )

            # Rimozione BOM dalla prima colonna, se presente

            # Rimuove BOM e virgolette dai nomi delle colonne
            df.columns = [col.strip().replace('\ufeff', '').replace('ï»¿', '').replace('"', '') for col in df.columns]

            logger.info(f"[CsvExtractor] Letti {len(df)} record")
            log_memory_usage(f"[CsvExtractor] Dopo lettura file: {self.filepath}")
            return df

        except Exception as e:
            logger.exception(f"[CsvExtractor] Errore durante la lettura del file: {e}")
            raise
