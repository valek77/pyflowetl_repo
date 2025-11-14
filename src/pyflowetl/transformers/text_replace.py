import pandas as pd
import re
from pyflowetl.log import get_logger, log_memory_usage

class TextReplaceTransformer:
    def __init__(self, column: str, replacements: dict[str, str], regex: bool = False, case_sensitive: bool = True):
        """
        Sostituisce testo in una colonna di tipo stringa.

        :param column: nome della colonna su cui operare
        :param replacements: dizionario {pattern: valore_sostitutivo}
        :param regex: se True, usa espressioni regolari
        :param case_sensitive: se False, ignora maiuscole/minuscole
        Esempio:
            TextReplaceTransformer("CITTA", {"ROMA": "Roma", "MILANO": "Milano"})
            TextReplaceTransformer("TESTO", {r"\\d+": "#NUM#"}, regex=True)
        """
        self.column = column
        self.replacements = replacements
        self.regex = regex
        self.case_sensitive = case_sensitive
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.column not in df.columns:
            raise KeyError(f"[TextReplaceTransformer] Colonna '{self.column}' non trovata")

        self.logger.info(f"[TextReplaceTransformer] Applico sostituzioni su '{self.column}' "
                         f"(regex={self.regex}, case_sensitive={self.case_sensitive})")

        flags = 0 if self.case_sensitive else re.IGNORECASE

        def replace_value(val):
            if pd.isna(val):
                return val
            s = str(val)
            for pattern, repl in self.replacements.items():
                if self.regex:
                    s = re.sub(pattern, repl, s, flags=flags)
                else:
                    if not self.case_sensitive:
                        s = re.sub(re.escape(pattern), repl, s, flags=flags)
                    else:
                        s = s.replace(pattern, repl)
            return s

        df[self.column] = df[self.column].apply(replace_value)

        log_memory_usage(f"[TextReplaceTransformer] post-transform {self.column}")
        return df
