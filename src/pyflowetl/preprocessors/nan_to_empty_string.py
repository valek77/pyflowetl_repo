import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()

class NanToEmptyStringPreprocessor:
    def __init__(self, column=None):
        """
        Converte valori NaN o None in stringa vuota.

        :param column: nome della colonna da normalizzare (necessario per .apply())
        """
        self.column = column

    def convert(self, value):
        if pd.isna(value):
            return ""
        return value

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.column:
            raise ValueError("La colonna deve essere specificata per apply() diretto.")
        logger.info(f"[NanToEmptyStringPreprocessor] Conversione NaN→'' sulla colonna '{self.column}'")
        df[self.column] = df[self.column].apply(self.convert)
        log_memory_usage("Dopo NanToEmptyStringPreprocessor")
        return df

    def apply_to_value(self, value):
        return self.convert(value)
