import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()


class TextReplacePreProcessor:
    def __init__(self, old_value, new_value, column=None, case_sensitive=True):
        """
        Sostituisce un testo con un altro all'interno di una colonna.

        :param old_value: valore da cercare
        :param new_value: valore con cui sostituire
        :param column: nome della colonna da applicare (necessario se usato con .apply)
        :param case_sensitive: se False, esegue una sostituzione insensibile al maiuscolo/minuscolo
        """
        self.old_value = old_value
        self.new_value = new_value
        self.column = column
        self.case_sensitive = case_sensitive

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.column:
            raise ValueError("La colonna deve essere specificata per l'uso con .apply(df)")

        logger.info(
            f"[TextReplacePreProcessor] Sostituisco '{self.old_value}' con '{self.new_value}' in colonna '{self.column}'")

        df[self.column] = df[self.column].apply(self.apply_to_value)
        log_memory_usage("Dopo TextReplacePreProcessor")
        return df

    def apply_to_value(self, value):
        if pd.isna(value):
            return value
        val_str = str(value)
        if self.case_sensitive:
            return val_str.replace(self.old_value, self.new_value)
        else:
            return self._replace_case_insensitive(val_str)

    def _replace_case_insensitive(self, text):
        import re
        return re.sub(re.escape(self.old_value), self.new_value, text, flags=re.IGNORECASE)
