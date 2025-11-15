import os
import pandas as pd
import numpy as np
from datetime import date, datetime

from pyflowetl.log import get_logger, log_memory_usage


class AddCodiceFiscaleDetailsTransformer:
    """
    Transformer che, data una colonna con il codice fiscale italiano,
    aggiunge le colonne derivate:
      - DATA_NASCITA (datetime64[ns])
      - ANNO_NASCITA (int)
      - MESE_NASCITA (int)
      - GIORNO_NASCITA (int)
      - SESSO ('M'/'F')
      - ETA (int, in anni)

    Assunzioni:
      - Il codice fiscale è quello standard da 16 caratteri per persone fisiche.
      - Il secolo è stimato rispetto a `reference_date`:
        se YY <= reference_date.year % 100 -> 2000+YY
        altrimenti                          -> 1900+YY
    """

    MONTH_MAP = {
        "A": 1,  "B": 2,  "C": 3,  "D": 4,
        "E": 5,  "H": 6,  "L": 7,  "M": 8,
        "P": 9,  "R": 10, "S": 11, "T": 12,
    }

    def __init__(
        self,
        cf_column: str,
        output_prefix: str = "",
        reference_date: date | None = None,
    ):
        """
        :param cf_column: nome della colonna che contiene il codice fiscale
        :param output_prefix: prefisso opzionale per tutte le colonne di output
                              (es: 'CF_' -> CF_DATA_NASCITA, CF_SESSO, ecc.)
        :param reference_date: data rispetto a cui calcolare l'età e il secolo.
                               Se None viene usata la data odierna.
        """
        self.cf_column = cf_column
        self.output_prefix = (output_prefix or "").upper()
        self.reference_date = reference_date or date.today()
        self.logger = get_logger()

    # ---------------------------------- #
    #             API principale         #
    # ---------------------------------- #
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(
            f"[AddCodiceFiscaleDetailsTransformer] Calcolo dati da CF "
            f"per colonna '{self.cf_column}' (reference_date={self.reference_date})"
        )

        if self.cf_column not in df.columns:
            self.logger.warning(
                f"[AddCodiceFiscaleDetailsTransformer] Colonna '{self.cf_column}' "
                f"non trovata: nessuna trasformazione eseguita."
            )
            log_memory_usage("[AddCodiceFiscaleDetailsTransformer] post-transform (no-op)")
            return df

        cf_series = (
            df[self.cf_column]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        # Validità di base: 16 caratteri
        mask_valid_len = cf_series.str.len() == 16
        valid_count = mask_valid_len.sum()
        total_count = len(cf_series)

        if valid_count == 0:
            self.logger.warning(
                "[AddCodiceFiscaleDetailsTransformer] Nessun CF valido (16 char); "
                "tutte le colonne derivate saranno NaN/None."
            )

        # inizializziamo le colonne di output a NaN/None
        col_data_nascita = self.output_prefix + "DATA_NASCITA"
        col_anno_nascita = self.output_prefix + "ANNO_NASCITA"
        col_mese_nascita = self.output_prefix + "MESE_NASCITA"
        col_giorno_nascita = self.output_prefix + "GIORNO_NASCITA"
        col_sesso = self.output_prefix + "SESSO"
        col_eta = self.output_prefix + "ETA"

        df[col_data_nascita] = pd.NaT
        df[col_anno_nascita] = np.nan
        df[col_mese_nascita] = np.nan
        df[col_giorno_nascita] = np.nan
        df[col_sesso] = pd.NA
        df[col_eta] = np.nan

        if valid_count > 0:
            # estrazione delle parti dal CF
            cf_valid = cf_series[mask_valid_len]

            # YY (posizioni 6-7, 0-based)
            yy = pd.to_numeric(cf_valid.str[6:8], errors="coerce")

            # M (posizione 8)
            month_char = cf_valid.str[8:9]
            month = month_char.map(self.MONTH_MAP).astype("float")

            # GG + 40 per le donne (posizioni 9-10)
            day_gender_raw = pd.to_numeric(cf_valid.str[9:11], errors="coerce")

            # sesso
            sex = pd.Series(pd.NA, index=cf_valid.index, dtype="object")
            sex.loc[day_gender_raw <= 40] = "M"
            sex.loc[day_gender_raw > 40] = "F"

            # giorno vero
            day = day_gender_raw.copy()
            day.loc[day_gender_raw > 40] = day_gender_raw.loc[day_gender_raw > 40] - 40

            # calcolo anno completo (secolo) rispetto a reference_date
            current_year = self.reference_date.year
            current_yy = current_year % 100
            base_century = current_year - current_yy  # es. 2025 -> 2000

            # se YY <= anno corrente % 100 -> stesso secolo
            # altrimenti secolo precedente (1900 invece di 2000)
            full_year = yy.copy()
            # stesso secolo
            mask_same_century = yy <= current_yy
            full_year.loc[mask_same_century] = base_century + yy.loc[mask_same_century]
            # secolo precedente
            full_year.loc[~mask_same_century] = (base_century - 100) + yy.loc[~mask_same_century]

            # costruiamo la data di nascita
            birth_date = pd.to_datetime(
                dict(
                    year=full_year,
                    month=month,
                    day=day,
                ),
                errors="coerce",
            )

            # età in anni interi
            ref_ts = pd.Timestamp(self.reference_date)
            age = ((ref_ts - birth_date).dt.days / 365.25).astype("float")
            age = np.floor(age)

            # scriviamo i risultati solo sulle righe valide
            df.loc[mask_valid_len, col_data_nascita] = birth_date
            df.loc[mask_valid_len, col_anno_nascita] = full_year
            df.loc[mask_valid_len, col_mese_nascita] = month
            df.loc[mask_valid_len, col_giorno_nascita] = day
            df.loc[mask_valid_len, col_sesso] = sex
            df.loc[mask_valid_len, col_eta] = age

            # qualche log di riepilogo
            invalid_len_count = total_count - valid_count
            self.logger.info(
                f"[AddCodiceFiscaleDetailsTransformer] Righe totali: {total_count}, "
                f"CF lunghi 16 caratteri: {valid_count}, non validi (len!=16): {invalid_len_count}"
            )
            self.logger.info(
                "[AddCodiceFiscaleDetailsTransformer] Distribuzione sesso (solo CF validi, dopo parsing): "
                f"M={ (sex == 'M').sum() }, F={ (sex == 'F').sum() }"
            )

        log_memory_usage("[AddCodiceFiscaleDetailsTransformer] post-transform")
        return df


# -----------------------------------------------------------
# Esempio di utilizzo
# -----------------------------------------------------------
if __name__ == "__main__":
    # Esempio minimale
    data = {
        "CODICE_FISCALE": [
            "RSSMRA85M01H501Z",  # Maria Rossi 01/??/1985 F (esempio fittizio)
            "BNCLGU03A15F205X",  # Luigi Bianchi 15/01/2003 M (esempio fittizio)
            "XXXXXXXXXXXXXXX",   # 15 char -> non valido
            None,                # None -> non valido
        ]
    }
    df_example = pd.DataFrame(data)

    transformer = AddCodiceFiscaleDetailsTransformer(
        cf_column="CODICE_FISCALE",
        output_prefix="CF_",
        # reference_date=date(2025, 1, 1),  # opzionale
    )

    df_out = transformer.transform(df_example)
    print(df_out)
