import pandas as pd
import os
from pyflowetl.log import get_logger, log_memory_usage

class AddRegioneTransformer:
    def __init__(self, comune_column: str, output_column: str = "REGIONE", max_match_chars: int = None):
        self.comune_column = comune_column
        self.output_column = output_column
        self.max_match_chars = max_match_chars
        self.logger = get_logger()

        # Carica CSV
        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'gi_comuni_cap.csv')
        self.comuni_df = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig')

        # Normalizza e taglia
        self.comuni_df["comune"] = self.comuni_df["comune"].astype(str).str.upper()
        if self.max_match_chars:
            self.comuni_df["comune"] = self.comuni_df["comune"].str[:self.max_match_chars]

        self.comuni_map = self.comuni_df.set_index("comune")["denominazione_regione"].to_dict()
        self.logger.info(f"[AddRegioneTransformer] Mappa comuni su regione caricata ({len(self.comuni_map)} chiavi uniche)")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(
            f"[AddRegioneTransformer] Aggiunta colonna '{self.output_column}' da '{self.comune_column}' "
            f"(match sui primi {self.max_match_chars if self.max_match_chars else '∞'} caratteri)"
        )

        df[self.comune_column] = df[self.comune_column].astype(str).str.upper()
        if self.max_match_chars:
            df[self.comune_column] = df[self.comune_column].str[:self.max_match_chars]

        df[self.output_column] = df[self.comune_column].map(self.comuni_map)
        log_memory_usage("[AddRegioneTransformer] post-transform")
        return df
