import pandas as pd
import os
from pyflowetl.log import get_logger, log_memory_usage

class AddComuneFromCapTransformer:
    def __init__(
        self,
        cap_column: str,
        comune_column: str = "Comune",
        csv_filename: str = "gi_comuni_cap.csv",
    ):
        self.cap_column = cap_column
        self.comune_column = comune_column
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
                "comune": "string",
            },
        )

        # Normalizza CAP nel dizionario
        self.cap_df["cap"] = (
            self.cap_df["cap"]
            .astype("string")
            .str.replace(r"\D", "", regex=True)
            .str.zfill(5)
        )
        
        # Gestione CAP duplicati: prendi il primo come richiesto
        self.cap_df.drop_duplicates(subset=["cap"], keep="first", inplace=True)

        # Fix encoding per casi di Mojibake (es. "ForlÃ¬" -> "Forlì")
        self.cap_df["comune"] = self.cap_df["comune"].apply(self._fix_encoding)

        self.cap_map_comune = self.cap_df.set_index("cap")["comune"].to_dict()

        self.logger.info(
            f"[AddComuneFromCapTransformer] Mappa CAP caricata "
            f"({len(self.cap_map_comune)} chiavi uniche)"
        )

    def _fix_encoding(self, text: str) -> str:
        if not isinstance(text, str):
            return text
        try:
            # Tenta di fixare il mojibake comune (UTF-8 letto come Latin-1 e salvato di nuovo come UTF-8)
            # Se la stringa contiene già caratteri corretti che non sono validi in latin1 questo potrebbe fallire,
            # ma "ForlÃ¬" -> encode latin1 -> bytes C3 AC -> decode utf8 -> ì
            return text.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            # Se fallisce, ritorna il testo originale (probabilmente era già corretto o irrecuperabile così)
            return text

    def _normalize_cap_series(self, s: pd.Series) -> pd.Series:
        return (
            s.astype("string")
             .str.replace(r"\D", "", regex=True)
             .str.zfill(5)
        )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(
            f"[AddComuneFromCapTransformer] Aggiungo '{self.comune_column}' "
            f"da '{self.cap_column}'"
        )

        df = df.copy()
        df[self.cap_column] = self._normalize_cap_series(df[self.cap_column])

        df[self.comune_column] = df[self.cap_column].map(self.cap_map_comune)

        # Log di diagnostica: quanti CAP non mappati
        missing = df[self.comune_column].isna().sum()
        if missing:
            self.logger.warning(
                f"[AddComuneFromCapTransformer] Valori mancanti totali su mappa comune: {missing}"
            )

        log_memory_usage("[AddComuneFromCapTransformer] post-transform")
        return df


# Esempio rapido
if __name__ == "__main__":
    data = pd.DataFrame({"CAP": ["80121", "10100", "00184", "80027", "47", "19", "10011"]})
    transformer = AddComuneFromCapTransformer("CAP")
    result = transformer.transform(data)
    print(result)
