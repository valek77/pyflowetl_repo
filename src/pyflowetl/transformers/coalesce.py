import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

class Fixed:
    """Wrapper per passare valori fissi nel coalesce."""
    def __init__(self, value):
        self.value = value

class CoalesceTransformer:
    def __init__(self, output_column: str, *inputs, treat_empty_as_null: bool = True):
        """
        COALESCE stile Postgres: primo valore non nullo tra colonne e/o letterali.

        Esempi:
            CoalesceTransformer("OUT", "col1", "col2", Fixed("Sconosciuto"))
            CoalesceTransformer("OUT", Fixed("N/A"), "col1")  # default prioritario

        :param output_column: nome colonna di output
        :param inputs: sequenza di nomi colonna (str) e/o Fixed(valore)
        :param treat_empty_as_null: se True, stringhe vuote -> null
        """
        self.output_column = output_column
        self.inputs = list(inputs)
        self.treat_empty_as_null = treat_empty_as_null
        self.logger = get_logger()

    def _nullify(self, v):
        if pd.isna(v):
            return None
        if self.treat_empty_as_null and isinstance(v, str) and v.strip() == "":
            return None
        return v

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.inputs:
            raise ValueError("[CoalesceTransformer] Nessun input specificato")

        # verifica solo le colonne realmente richieste
        requested_cols = [x for x in self.inputs if isinstance(x, str)]
        missing = [c for c in requested_cols if c not in df.columns]
        if missing:
            msg = f"[CoalesceTransformer] Colonne non trovate: {missing}"
            self.logger.error(msg)
            raise KeyError(msg)

        self.logger.info(f"[CoalesceTransformer] Creo '{self.output_column}' da {self.inputs}")

        candidates = []
        for x in self.inputs:
            if isinstance(x, str):  # colonna
                s = df[x]
            elif isinstance(x, Fixed):  # valore fisso
                s = pd.Series([x.value] * len(df), index=df.index)
            else:  # fallback: trattalo come letterale
                s = pd.Series([x] * len(df), index=df.index)

            s = s.map(self._nullify)
            candidates.append(s)

        out = pd.concat(candidates, axis=1).bfill(axis=1).iloc[:, 0]
        df[self.output_column] = out

        log_memory_usage(f"[CoalesceTransformer] after coalesce -> {self.output_column}")
        return df
