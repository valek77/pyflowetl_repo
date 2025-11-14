import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()


class PadColumnPreProcessor:
    def __init__(self, column=None, total_length=10, pad_char="0", direction="left"):
        """
        Aggiunge padding ai valori di una colonna fino a raggiungere una lunghezza specificata.

        :param column: nome della colonna su cui applicare il padding (necessario per apply()).
        :param total_length: lunghezza totale desiderata dopo il padding.
        :param pad_char: carattere usato per il padding (default: '0').
        :param direction: direzione del padding ('left' o 'right').

        Esempio in preprocess():
            PadColumnPreProcessor("CODICE", total_length=8, pad_char="0", direction="left")

        Esempio in preprocessing_rules:
            preprocessing_rules = {
                "CODICE": [PadColumnPreProcessor(total_length=8, pad_char="0")]
            }
        """
        self.column = column
        self.total_length = total_length
        self.pad_char = pad_char
        self.direction = direction.lower()

    def pad_value(self, value):
        if pd.isna(value):
            return value
        s = str(value)
        if len(s) >= self.total_length:
            return s
        if self.direction == "left":
            return s.rjust(self.total_length, self.pad_char)
        elif self.direction == "right":
            return s.ljust(self.total_length, self.pad_char)
        else:
            raise ValueError("direction deve essere 'left' o 'right'")

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.column:
            raise ValueError("La colonna deve essere specificata per apply() diretto.")
        logger.info(f"[PadColumnPreProcessor] Applico padding su '{self.column}' (len={self.total_length}, char='{self.pad_char}', dir={self.direction})")
        df[self.column] = df[self.column].apply(self.pad_value)
        log_memory_usage("Dopo PadColumnPreProcessor")
        return df

    def apply_to_value(self, value):
        return self.pad_value(value)


# ESEMPIO D'USO
if __name__ == "__main__":
    df = pd.DataFrame({
        "CODICE": ["123", "45", "7890", None, "5"]
    })

    pre = PadColumnPreProcessor("CODICE", total_length=6, pad_char="0", direction="left")
    df_padded = pre.apply(df)
    print(df_padded)

    print(pre.apply_to_value("42"))  # Output: 000042
