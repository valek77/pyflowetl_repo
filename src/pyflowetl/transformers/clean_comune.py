import pandas as pd
import os
import re
import unicodedata
from pyflowetl.log import get_logger, log_memory_usage


def clean_comune_name(text: str) -> str:
    if pd.isna(text):
        return ""

    # Uppercase
    text = str(text).strip().upper()

    # Apostrofo → spazio
    text = text.replace("'", " ")

    # Converti lettere accentate in lettere semplici
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode()

    # Rimuovi caratteri speciali tranne lettere, numeri e spazi
    text = re.sub(r"[^A-Z0-9\s]", "", text)

    # Normalizza spazi
    text = re.sub(r"\s+", " ", text).strip()

    return text


class CleanComuneNameTransformer:
    def __init__(self, input_column: str, output_column: str = None):
        self.input_column = input_column
        self.output_column = output_column or f"{input_column}_CLEAN"
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[CleanComuneNameTransformer] Pulizia '{self.input_column}' su '{self.output_column}'")

        if self.input_column not in df.columns:
            raise ValueError(f"Colonna '{self.input_column}' non trovata nel DataFrame")

        df[self.output_column] = df[self.input_column].apply(clean_comune_name)

        self.logger.info(f"[CleanComuneNameTransformer] Colonna '{self.output_column}' aggiunta")
        log_memory_usage("[CleanComuneNameTransformer] post-transform")
        return df
