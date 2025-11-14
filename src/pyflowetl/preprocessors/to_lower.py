import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()

class ToLowerPreProcessor:
    def __init__(self, column=None):
        """
        Converte il testo in minuscolo nella colonna specificata.

        :param column: nome della colonna (richiesto per .apply())

        Esempio:
            preprocessing_rules = {
                "EMAIL": [ToLowerPreProcessor()]
            }
        """
        self.column = column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.column:
            raise ValueError("Colonna non specificata per ToLowerPreProcessor")
        logger.info(f"[ToLowerPreProcessor] Applico minuscolo su '{self.column}'")
        df[self.column] = df[self.column].apply(self.apply_to_value)
        log_memory_usage("Dopo ToLowerPreProcessor")
        return df

    def apply_to_value(self, value):
        if pd.isna(value):
            return value
        return str(value).lower()


# ESEMPIO
if __name__ == "__main__":
    df = pd.DataFrame({"EMAIL": ["Mario@EXAMPLE.com", "TEST@test.IT", None]})
    pre = ToLowerPreProcessor("EMAIL")
    print(pre.apply(df))
