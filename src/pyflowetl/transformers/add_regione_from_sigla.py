import pandas as pd
import os
from pyflowetl.log import get_logger, log_memory_usage

class AddRegioneFromSiglaProvinciaTransformer:
    def __init__(self, sigla_column: str = "sigla_provincia", output_column: str = "REGIONE"):
        self.sigla_column = sigla_column
        self.output_column = output_column
        self.logger = get_logger()

        # Carica dataset di riferimento
        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'gi_comuni_cap.csv')
        self.prov_df = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig')


        # Crea mappa sigla → regione
        self.sigla_to_regione = self.prov_df.drop_duplicates(subset=["sigla_provincia"]).set_index("sigla_provincia")["denominazione_regione"].to_dict()
        self.logger.info(f"[AddRegioneFromSiglaProvinciaTransformer] Mappa sigle vs regioni caricata con {len(self.sigla_to_regione)} voci")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[AddRegioneFromSiglaProvinciaTransformer] Aggiunta colonna '{self.output_column}' da '{self.sigla_column}'")



        df[self.output_column] = df[self.sigla_column].map(self.sigla_to_regione)

        #  Pezza manuale per 'NA' = Napoli → Campania
        df.loc[df[self.sigla_column] == "NA", self.output_column] = "Campania"

        log_memory_usage("[AddRegioneFromSiglaProvinciaTransformer] post-transform")
        return df
