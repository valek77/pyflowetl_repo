import re
import os
import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class SplitAddressTransformer:
    def __init__(self, address_column: str,
                 comune_column: str = "COMUNE",
                 provincia_column: str = "PROVINCIA",
                 regione_column: str = "REGIONE"):
        self.address_column = address_column
        self.comune_column = comune_column
        self.provincia_column = provincia_column
        self.regione_column = regione_column
        self.logger = get_logger()

        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "gi_comuni_cap.csv")
        comuni_df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")

        # normalizza
        comuni_df["comune"] = comuni_df["comune"].astype(str).str.upper().str.strip()
        comuni_df["sigla_provincia"] = comuni_df["sigla_provincia"].astype(str).str.upper().str.strip()
        comuni_df["denominazione_regione"] = comuni_df["denominazione_regione"].astype(str).str.upper().str.strip()

        # indicizza per (COMUNE, PROVINCIA) per evitare duplicati
        comuni_df = comuni_df.dropna(subset=["comune", "sigla_provincia"])
        comuni_df = comuni_df.drop_duplicates(subset=["comune", "sigla_provincia"])
        self.comune_prov_to_regione = {
            (row["comune"], row["sigla_provincia"]): row["denominazione_regione"]
            for _, row in comuni_df.iterrows()
        }
        self.logger.info(f"[SplitAddressTransformer] Mappa caricata: {len(self.comune_prov_to_regione)} chiavi")

        # regex: '... - Roma(RM)' o '... - Sant'Angelo Lomellina(PV)'
        self.pattern = re.compile(r"-\s*([A-Za-zÀ-ÖØ-öø-ÿ' \-]+)\((\w{2})\)")

    def _parse(self, addr: str):
        m = self.pattern.search(addr or "")
        if not m:
            return None, None
        comune = m.group(1).strip().upper()
        prov = m.group(2).strip().upper()
        return comune, prov

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        comuni = []
        province = []
        regioni = []

        for addr in df[self.address_column].astype(str):
            c, p = self._parse(addr)
            comuni.append(c)
            province.append(p)
            regioni.append(self.comune_prov_to_regione.get((c, p)) if c and p else None)

        df[self.comune_column] = comuni
        df[self.provincia_column] = province
        df[self.regione_column] = regioni

        log_memory_usage("[SplitAddressTransformer] dopo transform")
        return df
