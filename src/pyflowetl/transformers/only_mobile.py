import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class KeepOnlyMobilePhonesTransformer:
    def __init__(self, column: str):
        """
        Mantiene solo i numeri di telefono cellulari italiani (iniziano con '3').

        :param column: nome della colonna contenente i numeri di telefono
        """
        self.column = column
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[KeepOnlyMobilePhonesTransformer] Mantengo solo i numeri mobili in '{self.column}'")

        # Crea maschera: tiene solo i numeri che iniziano con '3'
        mask = df[self.column].astype(str).str.match(r"^3\d+", na=False)
        filtered_df = df[mask].copy()

        scarti = len(df)- len(filtered_df)
        self.logger.info(f"[KeepOnlyMobilePhonesTransformer] Record mantenuti: {len(filtered_df)} su {len(df)}, scarti {scarti}")
        log_memory_usage("[KeepOnlyMobilePhonesTransformer] post-transform")

        return filtered_df


# Esempio di utilizzo:
# pipeline.transform(KeepOnlyMobilePhonesTransformer("CELL"))
