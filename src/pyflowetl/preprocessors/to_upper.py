import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()

class ToUpperPreProcessor:
    def __init__(self, column=None):
        """
        Converte il testo in MAIUSCOLO nella colonna specificata.

        :param column: nome della colonna (richiesto per .apply())

        Esempio uso in pipeline:
            preprocessing_rules = {
                "NOME": [ToUpperPreProcessor()],
                "COGNOME": [ToUpperPreProcessor()]
            }
        """
        self.column = column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.column:
            raise ValueError("Colonna non specificata per ToUpperPreProcessor")
        logger.info(f"[ToUpperPreProcessor] Applico MAIUSCOLO su '{self.column}'")
        df[self.column] = df[self.column].apply(self.apply_to_value)
        log_memory_usage("Dopo ToUpperPreProcessor")
        return df

    def apply_to_value(self, value):
        if pd.isna(value):
            return value
        return str(value).upper()


# ESEMPIO
if __name__ == "__main__":
    df = pd.DataFrame({"NOME": ["mario", "Luca", None]})
    pre = ToUpperPreProcessor("NOME")
    print(pre.apply(df))
