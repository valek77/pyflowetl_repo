import pandas as pd
import os
import unicodedata
from pyflowetl.log import get_logger, log_memory_usage


class AddCapFromComuneTransformer:
    def __init__(
        self,
        comune_column: str,
        cap_column: str = "CAP",
        csv_filename: str = "gi_comuni_cap.csv",
    ):
        self.comune_column = comune_column
        self.cap_column = cap_column
        self.logger = get_logger()

        # Percorso file CSV
        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", csv_filename)

        # Carica CSV evitando che "NA" venga trattato come null
        cap_df = pd.read_csv(
            csv_path,
            sep=";",
            encoding="utf-8-sig",
            keep_default_na=False,
            dtype={
                "cap": "string",
                "comune": "string",
            },
        )

        # Normalizza CAP
        cap_df["cap"] = (
            cap_df["cap"]
            .astype("string")
            .str.replace(r"\D", "", regex=True)
            .str.zfill(5)
        )

        # Fix encoding per casi di Mojibake (es. "ForlÃ¬" -> "Forlì")
        cap_df["comune"] = cap_df["comune"].apply(self._fix_encoding)

        # Chiave di lookup: comune normalizzato (lowercase, senza accenti)
        cap_df["comune_key"] = cap_df["comune"].apply(self._normalize_comune)

        # Gestione comuni duplicati: prendi il primo CAP
        cap_df.drop_duplicates(subset=["comune_key"], keep="first", inplace=True)

        self.comune_map_cap = cap_df.set_index("comune_key")["cap"].to_dict()

        self.logger.info(
            f"[AddCapFromComuneTransformer] Mappa Comune->CAP caricata "
            f"({len(self.comune_map_cap)} chiavi uniche)"
        )

    @staticmethod
    def _normalize_comune(text: str) -> str:
        if not isinstance(text, str):
            return text
        # Rimuovi accenti e converti in minuscolo
        text = unicodedata.normalize("NFD", text)
        text = "".join(c for c in text if unicodedata.category(c) != "Mn")
        return text.strip().lower()

    @staticmethod
    def _fix_encoding(text: str) -> str:
        if not isinstance(text, str):
            return text
        try:
            return text.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(
            f"[AddCapFromComuneTransformer] Aggiungo '{self.cap_column}' "
            f"da '{self.comune_column}'"
        )

        df = df.copy()

        # Normalizza i nomi dei comuni nel dataframe per il lookup
        comune_keys = df[self.comune_column].apply(self._normalize_comune)
        df[self.cap_column] = comune_keys.map(self.comune_map_cap)

        # Log di diagnostica: quanti comuni non mappati
        missing = df[self.cap_column].isna().sum()
        if missing:
            self.logger.warning(
                f"[AddCapFromComuneTransformer] Valori mancanti totali su mappa CAP: {missing}"
            )

        log_memory_usage("[AddCapFromComuneTransformer] post-transform")
        return df


# Esempio rapido
if __name__ == "__main__":
    data = pd.DataFrame({"Comune": ["Napoli", "Torino", "Roma", "forlì", "MILANO"]})
    transformer = AddCapFromComuneTransformer("Comune")
    result = transformer.transform(data)
    print(result)
