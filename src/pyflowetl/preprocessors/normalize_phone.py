import re
import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()


class NormalizePhoneNumberPreProcessor:
    def __init__(self, column=None):
        """
        Normalizza numeri di telefono rimuovendo spazi, simboli e prefissi internazionali italiani (+39, 0039, 39).

        :param column: nome della colonna da normalizzare (necessario solo per uso in .preprocess())

        Esempio in preprocess():
            NormalizePhoneNumberPreProcessor("CELL")

        Esempio in preprocessing_rules:
            preprocessing_rules = {
                "CELL": [NormalizePhoneNumberPreProcessor()]
            }
        """
        self.column = column

    def normalize(self, number):
        if pd.isna(number) or str(number).strip() == "":
            return None
        cleaned = re.sub(r"[^0-9+]", "", str(number))
        if cleaned.startswith("+39"):
            return cleaned[3:]
        elif cleaned.startswith("0039"):
            return cleaned[4:]
        elif cleaned.startswith("39") and len(cleaned) >= 11:
            return cleaned[2:]
        return cleaned

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.column:
            raise ValueError("La colonna deve essere specificata per apply() diretto.")
        logger.info(f"[NormalizePhoneNumberPreProcessor] Normalizzo colonna '{self.column}'")
        df[self.column] = df[self.column].apply(self.normalize)
        log_memory_usage("Dopo NormalizePhoneNumberPreProcessor")
        return df

    def apply_to_value(self, value):
        return self.normalize(value)


# ESEMPIO D'USO
if __name__ == "__main__":
    df = pd.DataFrame({
        "CELL": ["+39 335 1234567", "00393351234567", "3471234567", None, "39 3481234567", "333-123-4567"]
    })

    # Esecuzione diretta
    pre = NormalizePhoneNumberPreProcessor("CELL")
    df_cleaned = pre.apply(df)
    print(df_cleaned)

    # Esecuzione su singolo valore
    print(pre.apply_to_value("+39 347 1234567"))  # Output: 3471234567
