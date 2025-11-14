import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class ConcatColumnsTransformer:
    def __init__(self, columns, output_column, separator="_", drop_originals=False, skip_empty=True):
        self.columns = columns
        self.output_column = output_column
        self.separator = separator
        self.drop_originals = drop_originals
        self.skip_empty = skip_empty

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        logger = get_logger()
        logger.info(f"[ConcatColumnsTransformer] Creo colonna '{self.output_column}' da: {self.columns}")

        # Check colonne
        missing = [c for c in self.columns if c not in data.columns]
        if missing:
            raise KeyError(f"[ConcatColumnsTransformer] Colonne mancanti: {missing}")

        # Prepara pezzi come Series pulite
        parts = []
        for c in self.columns:
            s = data[c].astype("string").fillna("").str.strip()
            if self.skip_empty:
                parts.append(s.replace("", pd.NA))
            else:
                parts.append(s)

        # Concatena in modo vettoriale; risultato è SEMPRE una Series
        out = parts[0]
        for s in parts[1:]:
            out = out.str.cat(s, sep=self.separator, na_rep="")

        # Ripulisci separatori doppi che possono nascere con skip_empty
        if self.skip_empty and self.separator:
            sep = self.separator
            # rimuovi separatori all'inizio/fine e doppi
            out = out.str.replace(f"{sep}+", sep, regex=True)\
                     .str.removeprefix(sep).str.removesuffix(sep)

        data[self.output_column] = out.fillna("")
        if self.drop_originals:
            data.drop(columns=self.columns, inplace=True, errors="ignore")

        logger.info(f"[ConcatColumnsTransformer] Colonna '{self.output_column}' creata")
        log_memory_usage(f"[ConcatColumnsTransformer] post-transform '{self.output_column}'")
        return data
