import pandas as pd
import os
from pyflowetl.log import get_logger, log_memory_usage

class AddProvinciaRegioneFromCapTransformer:
    def __init__(
        self,
        cap_column: str,
        provincia_column: str = "Provincia",
        regione_column: str = "Regione",
        csv_filename: str = "gi_comuni_cap.csv",
    ):
        self.cap_column = cap_column
        self.provincia_column = provincia_column
        self.regione_column = regione_column
        self.logger = get_logger()

        # Percorso file CSV
        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", csv_filename)

        # Carica CSV evitando che "NA" venga trattato come null
        self.cap_df = pd.read_csv(
            csv_path,
            sep=";",
            encoding="utf-8-sig",
            keep_default_na=False,      # <--- blocca NA->NaN
            dtype={
                "cap": "string",
                "sigla_provincia": "string",
                "denominazione_regione": "string",
            },
        )

        # Normalizza CAP nel dizionario
        self.cap_df["cap"] = (
            self.cap_df["cap"]
            .astype("string")
            .str.replace(r"\D", "", regex=True)
            .str.zfill(5)
        )

        self.cap_map_prov = self.cap_df.set_index("cap")["sigla_provincia"].to_dict()
        self.cap_map_reg = self.cap_df.set_index("cap")["denominazione_regione"].to_dict()

        self.logger.info(
            f"[AddProvinciaRegioneFromCapTransformer] Mappa CAP caricata "
            f"({len(self.cap_map_prov)} chiavi uniche)"
        )

    def _normalize_cap_series(self, s: pd.Series) -> pd.Series:
        return (
            s.astype("string")
             .str.replace(r"\D", "", regex=True)
             .str.zfill(5)
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(
            f"[AddProvinciaRegioneFromCapTransformer] Aggiungo '{self.provincia_column}' e "
            f"'{self.regione_column}' da '{self.cap_column}'"
        )

        df = df.copy()
        df[self.cap_column] = self._normalize_cap_series(df[self.cap_column])

        df[self.provincia_column] = df[self.cap_column].map(self.cap_map_prov)
        df[self.regione_column]   = df[self.cap_column].map(self.cap_map_reg)

        # Log di diagnostica: quanti CAP non mappati
        missing = df[self.provincia_column].isna().sum() + df[self.regione_column].isna().sum()
        if missing:
            self.logger.warning(
                f"[AddProvinciaRegioneFromCapTransformer] Valori mancanti totali su mappe provincia/regione: {missing}"
            )

        log_memory_usage("[AddProvinciaRegioneFromCapTransformer] post-transform")
        return df


# Esempio rapido
if __name__ == "__main__":
    data = pd.DataFrame({"CAP": ["80121", "10100", "00184", "80027", "47", "19"]})
    transformer = AddProvinciaRegioneFromCapTransformer("CAP")
    result = transformer.transform(data)
    print(result)
